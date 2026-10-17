# frozen_string_literal: true

require "test_helper"
require "cgi"
require "json"
require "openssl"

# Rails-dispatch, end to end, against the real dummy application.
#
# Nothing is stubbed below the HTTP boundary: the requests go through
# `Rails.application.call`, so the Engine's mount, the application's middleware
# stack and reloader, the real `ApplicationCable::Connection`, a real
# `ActionCable::Channel::Base` subclass, real cookies, and a real ActiveRecord
# model all participate. The only thing missing is the Rust process, which is
# exactly the half `tests/rpc.rs` covers from the other side.
#
# ## Running it
#
#     BUNDLE_GEMFILE=gem/Gemfile bundle exec ruby -Igem/lib -Igem/test \
#       gem/test/rails_contract_test.rb
#
# `rake test:gem` runs plain `ruby` with no Bundler, so this file skips rather
# than half-booting a Rails application against whatever happens to be
# installed. It also skips when it is only *one* file of a larger run, because
# booting an application mid-suite changes `Rails.root` under every other test:
# run it on its own, or set ORBITCAST_RAILS_TESTS=1.
DUMMY_APP_AVAILABLE =
  if ENV["BUNDLE_GEMFILE"].to_s.empty? || !defined?(Bundler)
    false
  elsif ENV["ORBITCAST_RAILS_TESTS"] != "1" && $PROGRAM_NAME != __FILE__
    false
  else
    begin
      ENV["RAILS_ENV"] = "test"
      ENV["ORBITCAST_MODE"] = "dispatch"
      ENV["ORBITCAST_RPC_TOKEN"] ||= "dummy-rails-service-token"
      require_relative "dummy/config/environment"
      true
    rescue LoadError => e
      warn "rails_contract_test: dummy application unavailable (#{e.message})"
      false
    end
  end

if DUMMY_APP_AVAILABLE
  class RailsContractTest < Minitest::Test
    V1 = Orbitcast::Protocol::V1

    MOUNT = Orbitcast::Configuration::DEFAULT_RPC_MOUNT_PATH
    TOKEN = ENV.fetch("ORBITCAST_RPC_TOKEN")
    NAMESPACE = "dummy_test"
    SESSION = "2f4a6c8e-1b3d-4f5a-9c7e-0d1a2b3c4d5e"

    class << self
      def prepare_database
        return if @prepared

        ActiveRecord::Base.establish_connection(
          adapter: "sqlite3", database: ":memory:"
        )
        ActiveRecord::Schema.verbose = false
        ActiveRecord::Schema.define do
          create_table :widgets, force: true do |table|
            table.string :name
          end
        end
        @prepared = true
      end
    end

    def setup
      self.class.prepare_database
      Widget.delete_all
      @user = Widget.create!(name: "Marissa")
      @room = Widget.create!(name: "Room 42")
      ApplicationCable::Connection.journal = []
      ChatChannel.journal = []
      @sequence = 0
    end

    # --- the mount ------------------------------------------------------------

    def test_the_engine_mounts_the_endpoint_in_dispatch_mode
      assert_predicate Orbitcast.config, :dispatch?
      assert_equal MOUNT, Orbitcast.config.rpc_mount_path

      status, = post("describe", envelope("describe"))

      assert_equal 200, status
    end

    def test_the_endpoint_authenticates_the_service_credential
      status, = post("describe", envelope("describe"), authorization: "Bearer nope")

      assert_equal 401, status
    end

    def test_describe_reports_readiness_through_the_real_application
      reply = call("describe", envelope("describe"))

      assert_equal "accepted", reply["outcome"]
      assert_equal [1], reply.dig("describe", "supported_versions")
      assert reply.dig("describe", "ready"), "a configured application is ready"
    end

    # --- identify -------------------------------------------------------------

    def test_a_cookie_authenticated_connection_identifies_a_user
      reply = call("identify", envelope("identify"))

      assert_equal "accepted", reply["outcome"]
      assert_equal(
        {
          "current_user" => { "_gid" => @user.to_global_id.to_s },
          "identity_key" => @user.to_gid_param
        },
        reply["identity"],
        "identified_by values travel as tagged references, not as guesses"
      )
      assert_equal({ "locale" => "fr" }, reply["state"])
      assert_equal 1, reply["new_revision"]
    end

    def test_an_unauthorized_connection_fails_closed
      reply = call("identify", envelope("identify", cookie: nil))

      assert_equal "denied", reply["outcome"]
      refute reply.key?("identity")
      assert_empty reply["effects"]
      assert_equal 0, reply["new_revision"]
    end

    def test_a_forged_cookie_does_not_identify_anyone
      reply = call("identify", envelope("identify", cookie: "user_id=#{@user.id}"))

      assert_equal "denied", reply["outcome"], "an unsigned cookie value is not a signed one"
    end

    # --- the executor ---------------------------------------------------------

    def test_callbacks_run_inside_the_executor
      call("identify", envelope("identify"))

      assert_equal [[:connect, true, @user.id]], ApplicationCable::Connection.journal,
                   "connect must run inside Rails' executor"
    end

    def test_channel_callbacks_run_inside_the_executor_too
      call("open_channel", envelope("open_channel"))

      assert_equal(
        [[:before_subscribe, @room.id.to_s], [:subscribed, true, "Room 42"]], ChatChannel.journal
      )
    end

    # The reason the executor matters. `Current` is reset and Active Record
    # leases are returned when the executor completes; a WebSocket's lifetime
    # must never pin either.
    def test_the_executor_cleans_up_after_every_call
      Current.reset
      ActiveRecord::Base.connection_pool.release_connection

      call("open_channel", envelope("open_channel"))

      assert_nil Current.user,
                 "per-request state set by a callback must not survive the call"
      assert ActiveRecord::Base.connection_pool.connections.none?(&:in_use?),
             "the endpoint must not leak a database connection"
    end

    def test_a_faulting_call_still_cleans_up
      Current.reset
      # A malformed identifier: the dispatcher denies, the executor still
      # completes.
      call("open_channel", envelope("open_channel", identifier: "not json"))

      assert_nil Current.user
      assert ActiveRecord::Base.connection_pool.connections.none?(&:in_use?)
    end

    # --- open_channel ---------------------------------------------------------

    def test_stream_for_and_transmit_become_ordered_effects
      reply = call("open_channel", envelope("open_channel"))

      assert_equal "accepted", reply["outcome"]
      assert_equal(
        %w[add_stream add_stream transmit], reply["effects"].map { |effect| effect["kind"] }
      )
      assert_equal(
        ChatChannel.broadcasting_for(@room), reply["effects"][0]["stream"],
        "stream_for still derives ActionCable's own broadcasting name"
      )
      assert_equal "chat:all", reply["effects"][1]["stream"]
      assert_equal(
        { "type" => "backlog", "user" => "Marissa", "room" => "Room 42" },
        JSON.parse(reply["effects"][2]["json_payload"])
      )
    end

    def test_declared_state_round_trips_a_global_id_record
      reply = call("open_channel", envelope("open_channel"))

      assert_equal(
        { "room" => { "_gid" => @room.to_global_id.to_s }, "cursor" => 0 },
        reply["subscription_state"]
      )

      # Hand the state back the way the relay would, and the channel gets the
      # record, not a hash.
      reply = call(
        "run_action",
        envelope("run_action", json_data: '{"action":"speak","text":"hi"}',
                               subscription_state: reply["subscription_state"])
      )

      assert_equal(
        { "said" => "hi", "in" => "Room 42", "by" => "Marissa" },
        JSON.parse(reply["effects"].first["json_payload"])
      )
      assert_equal 1, reply.dig("subscription_state", "cursor")
    end

    def test_a_reference_to_a_deleted_record_restores_as_nil
      state = { "room" => { "_gid" => @room.to_global_id.to_s }, "cursor" => 3 }
      @room.destroy!

      reply = call(
        "run_action",
        envelope("run_action", json_data: '{"action":"speak","text":"still here"}',
                               subscription_state: state)
      )

      assert_equal "accepted", reply["outcome"]
      assert_nil JSON.parse(reply["effects"].first["json_payload"])["in"],
                 "a deleted record restores as nil, documented, rather than raising"
    end

    def test_reject_denies_the_subscription_and_carries_nothing
      identifier = identifier_for(room: 999_999)
      reply = call("open_channel", envelope("open_channel", identifier: identifier))

      assert_equal "denied", reply["outcome"]
      assert_empty reply["effects"]
      refute reply.key?("subscription_state")
      assert_equal [[:before_subscribe, "999999"], [:unsubscribed, identifier]], ChatChannel.journal
    end

    def test_a_channel_outside_the_registry_is_denied
      reply = call(
        "open_channel", envelope("open_channel", identifier: '{"channel":"Widget"}')
      )

      assert_equal "denied", reply["outcome"]
      assert_includes reply["reason"], "registry"
    end

    # --- run_action -----------------------------------------------------------

    def test_an_action_can_change_the_installed_streams
      reply = call(
        "run_action",
        envelope("run_action", json_data: '{"action":"leave"}', streams: ["chat:all"])
      )

      assert_equal(
        [{ "kind" => "remove_stream", "identifier" => identifier_for, "stream" => "chat:all" }],
        reply["effects"]
      )
    end

    def test_an_action_can_close_the_connection
      reply = call("run_action", envelope("run_action", json_data: '{"action":"close_everything"}'))

      assert_equal(
        [{ "kind" => "close", "reason" => "remote", "reconnect" => false }], reply["effects"]
      )
    end

    def test_only_allowlisted_actions_are_dispatched
      %w[subscribed cursor= destroy send].each do |action|
        reply = call("run_action", envelope("run_action", json_data: %({"action":"#{action}"})))

        next if action == "subscribed" # a public callback really is an action in ActionCable

        assert_equal "denied", reply["outcome"], action
      end
    end

    def test_stop_all_streams_becomes_one_remove_all_streams_effect
      reply = call(
        "run_action",
        envelope("run_action", json_data: '{"action":"leave_all"}',
                               streams: ["chat:all", ChatChannel.broadcasting_for(@room)])
      )

      assert_equal "accepted", reply["outcome"]
      assert_equal(
        [{ "kind" => "remove_all_streams", "identifier" => identifier_for }], reply["effects"],
        "one effect for the whole subscription, whatever streams the relay reported"
      )
    end

    def test_stop_all_streams_with_nothing_installed_emits_nothing
      reply = call("run_action", envelope("run_action", json_data: '{"action":"leave_all"}', streams: []))

      assert_equal "accepted", reply["outcome"]
      assert_empty reply["effects"]
    end

    # --- ActionCable filters -----------------------------------------------------

    def test_before_subscribe_filters_run_before_subscribed
      call("open_channel", envelope("open_channel"))

      assert_equal(
        [[:before_subscribe, @room.id.to_s], [:subscribed, true, "Room 42"]],
        ChatChannel.journal,
        "ActionCable's own :subscribe callback chain runs unmodified"
      )
    end

    def test_around_command_filters_wrap_every_dispatched_command
      call("open_channel", envelope("open_channel"))

      assert_equal(
        [[:around_command, :before], [:around_command, :after]],
        ApplicationCable::Connection.journal
      )

      ApplicationCable::Connection.journal = []
      call("run_action", envelope("run_action", json_data: '{"action":"speak","text":"hi"}'))

      assert_equal(
        [[:around_command, :before], [:around_command, :after]],
        ApplicationCable::Connection.journal,
        "an action is a command too"
      )
    end

    def test_identify_runs_no_command_filters
      call("identify", envelope("identify"))

      refute_includes ApplicationCable::Connection.journal.map(&:first), :around_command,
                      "connect is not a command; ActionCable does not wrap it either"
    end

    # --- unsupported constructs --------------------------------------------------

    def test_a_stream_transformation_block_is_refused_explicitly_as_a_fault
      reply = call("run_action", envelope("run_action", json_data: '{"action":"transform"}'))

      assert_equal "fault", reply["outcome"], "an unsupported construct is never silently ignored"
      assert_equal "Orbitcast::UnsupportedChannelFeature", reply["reason"]
      assert_empty reply["effects"], "nothing is installed on the way to the refusal"
    end

    def test_periodically_is_refused_at_class_definition
      error = assert_raises(Orbitcast::UnsupportedChannelFeature) do
        Class.new(ApplicationCable::Channel) do
          periodically :tick, every: 5
        end
      end

      assert_includes error.message, "periodically"
      assert_includes error.message, "recurring job"
    end

    def test_a_reserved_action_name_is_refused_at_class_definition
      error = assert_raises(Orbitcast::UnsupportedChannelFeature) do
        Class.new(ApplicationCable::Channel) do
          def orbitcast_presence; end
        end
      end

      assert_includes error.message, "reserved action namespace"
    end

    # --- the engine -------------------------------------------------------------------

    # The dummy application carries db/orbitcast_migrate/ (what
    # `orbitcast:install --overflow` produces); the engine must put it where
    # `bin/rails db:migrate` looks, exactly once.
    def test_the_engine_appends_db_orbitcast_migrate_to_the_migration_paths
      directory = Rails.root.join("db", "orbitcast_migrate").to_s
      expanded = Rails.application.paths["db/migrate"].expanded

      assert_equal 1, expanded.count(directory), "appended exactly once"
      assert_includes expanded, Rails.root.join("db", "migrate").to_s, "the application's own path is kept"
      assert_includes ActiveRecord::Tasks::DatabaseTasks.migrations_paths, directory

      names = ActiveRecord::MigrationContext.new(ActiveRecord::Tasks::DatabaseTasks.migrations_paths)
                                            .migrations.map(&:name)

      assert_includes names, "CreateOrbitcastPayloads"
    end

    def test_the_stream_key_is_derived_from_the_application_key_generator
      config = Orbitcast.config

      assert_equal Rails.application.key_generator, config.key_generator
      assert_predicate config, :streams_secret_derived?

      expected = Rails.application.key_generator.generate_key("orbitcast/streams/v1/#{NAMESPACE}", 32)

      assert_equal expected, config.streams_secret
      assert_equal 32, config.streams_secret.bytesize

      encoded, signature = Orbitcast.signed_stream_name("chat:42").split("--")

      assert_equal OpenSSL::HMAC.hexdigest("SHA256", expected, encoded), signature
      refute_equal Rails.application.secret_key_base, config.streams_secret
    end

    # --- teardown -------------------------------------------------------------

    def test_close_channel_runs_unsubscribed_without_emitting_effects
      reply = call("close_channel", envelope("close_channel"))

      assert_equal "accepted", reply["outcome"]
      assert_equal [[:unsubscribed, identifier_for]], ChatChannel.journal
      assert_empty reply["effects"]
    end

    def test_release_connection_runs_the_remaining_callbacks
      reply = call("release_connection", envelope("release_connection"))

      assert_equal "accepted", reply["outcome"]
      assert_equal [[:unsubscribed, identifier_for]], ChatChannel.journal
      assert_equal(
        [[:around_command, :before], [:around_command, :after], [:disconnect, @user.id]],
        ApplicationCable::Connection.journal,
        "the release is a command (wrapped); disconnect itself is not"
      )
    end

    # --- transport errors -----------------------------------------------------

    def test_a_body_replayed_against_another_route_is_refused
      status, = post("run_action", envelope("identify"))

      assert_equal 400, status
    end

    def test_another_deployments_namespace_is_refused
      status, = post("identify", envelope("identify", namespace: "someone_else"))

      assert_equal 403, status
    end

    private

    # --- driving the real application -----------------------------------------

    def call(operation, body, **options)
      status, _headers, response = post(operation, body, **options)

      assert_equal 200, status, response

      JSON.parse(response)
    end

    def post(operation, body, authorization: "Bearer #{TOKEN}")
      env = Rack::MockRequest.env_for(
        "http://www.example.com#{MOUNT}/#{operation}",
        method: "POST",
        input: body,
        "CONTENT_TYPE" => "application/json",
        "HTTP_AUTHORIZATION" => authorization
      )

      status, headers, body = Rails.application.call(env)
      collected = +""
      body.each { |chunk| collected << chunk }
      body.close if body.respond_to?(:close)

      [status, headers, collected]
    end

    # --- envelopes ------------------------------------------------------------

    def identifier_for(room: nil)
      JSON.generate("channel" => "ChatChannel", "room" => (room || @room.id).to_s)
    end

    def envelope(operation, identifier: nil, cookie: :signed, namespace: NAMESPACE,
                 json_data: nil, streams: [], subscription_state: {})
      identifier ||= identifier_for
      @sequence += 1

      hash = {
        "version" => 1,
        "operation" => operation,
        "namespace" => namespace,
        "node_id" => "node-test",
        "session_id" => operation == "describe" ? "00000000-0000-0000-0000-000000000000" : SESSION,
        "request_id" => SecureRandom.uuid,
        "sequence" => @sequence,
        "expected_revision" => 0,
        "deadline_ms" => 1000
      }

      unless operation == "describe"
        headers = {}
        cookie_header = cookie == :signed ? "user_id=#{CGI.escape(signed_cookie(@user.id))}" : cookie
        headers["cookie"] = cookie_header if cookie_header

        hash["connection"] = {
          "url" => "/cable?tab=1",
          "origin" => "http://localhost:3000",
          "client_address" => "203.0.113.7",
          "headers" => headers,
          # After `identify` the relay hands the identity back on every call.
          "identity" => operation == "identify" ? {} : identity_map,
          "state" => operation == "identify" ? {} : { "locale" => "fr" }
        }
      end

      subscription = {
        "identifier" => identifier, "streams" => streams, "state" => subscription_state
      }

      case operation
      when "open_channel", "close_channel"
        hash["subscription"] = subscription
      when "run_action"
        hash["subscription"] = subscription
        hash["action"] = {
          "identifier" => identifier, "json_data" => json_data || '{"action":"speak"}'
        }
      when "release_connection"
        hash["release"] = {
          "reason" => "remote", "reconnect" => false, "subscriptions" => [subscription]
        }
      end

      JSON.generate(hash)
    end

    def identity_map
      {
        "current_user" => { "_gid" => @user.to_global_id.to_s },
        "identity_key" => @user.to_gid_param
      }
    end

    # A real signed cookie, produced by the application's own cookie jar: the
    # test must not reimplement the verifier it is testing against.
    def signed_cookie(value)
      request = ActionDispatch::Request.new(
        Rails.application.env_config.merge(Rack::MockRequest.env_for("/"))
      )
      jar = ActionDispatch::Cookies::CookieJar.build(request, {})
      jar.signed[:user_id] = value
      jar["user_id"]
    end
  end
end
