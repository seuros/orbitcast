# frozen_string_literal: true

require "test_helper"
require "json"

begin
  require "orbitcast/dispatcher"
  DISPATCHER_AVAILABLE = true
rescue LoadError
  # Needs ActionCable. The plain unit suite skips this file on a checkout
  # without Rails; rails_contract_test.rb covers the same paths against a real
  # application.
  DISPATCHER_AVAILABLE = false
end

# The Rails execution model: one validated request in, one reply out.
#
# ActionCable is real here — these are actual `ActionCable::Channel::Base`
# subclasses running their actual callbacks — but no Rails application is
# booted, which is exactly the point: the dispatcher needs the framework, not
# the app server.
if DISPATCHER_AVAILABLE
  class DispatcherTest < Minitest::Test
    V1 = Orbitcast::Protocol::V1

    SESSION = "2f4a6c8e-1b3d-4f5a-9c7e-0d1a2b3c4d5e"
    REQUEST_ID = "9a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d"
    IDENTIFIER = '{"channel":"DispatcherTest::ChatChannel","room":"42"}'

    class TestConnection < Orbitcast::Connection
      identified_by :current_user
      orbitcast_state :locale
      orbitcast_identity_key :current_user

      class << self
        attr_accessor :behaviour
      end

      def connect
        case self.class.behaviour
        when :reject then reject_unauthorized_connection
        when :raise then raise "connect exploded"
        else
          reject_unauthorized_connection unless cookies_say_who_it_is
          self.current_user = cookies_say_who_it_is
          self.locale = "fr"
        end
      end

      def disconnect
        self.class.log << [:disconnect, current_user]
      end

      def self.log
        @log ||= []
      end

      private

      # Reads the forwarded cookie header, the way a real connection does.
      def cookies_say_who_it_is
        request.headers["Cookie"].to_s[/user=(\w+)/, 1]
      end
    end

    class ChatChannel < Orbitcast::Channel
      orbitcast_state :cursor

      def subscribed
        return reject if params[:room] == "forbidden"

        stream_from "chat:#{params[:room]}"
        stream_from "chat:all"
        transmit({ "type" => "backlog", "user" => current_user })
        self.cursor = 1
      end

      def unsubscribed
        self.class.log << [:unsubscribed, identifier]
      end

      def speak(data)
        self.cursor = cursor.to_i + 1
        transmit({ "said" => data["text"] })
      end

      def leave
        stop_stream_from("chat:all")
      end

      def explode
        raise ArgumentError, "boom"
      end

      def self.log
        @log ||= []
      end
    end

    class TimerChannel < Orbitcast::Channel; end

    def setup
      TestConnection.behaviour = nil
      TestConnection.log.clear
      ChatChannel.log.clear
    end

    # --- describe -------------------------------------------------------------

    def test_describe_reports_version_capabilities_and_readiness
      reply = dispatch(request("describe")).to_h

      assert_equal "accepted", reply["outcome"]
      assert_equal [1], reply.dig("describe", "supported_versions")
      assert_equal Orbitcast::VERSION, reply.dig("describe", "software_version")
      assert_includes reply.dig("describe", "capabilities"), "rails_dispatch"
      assert reply.dig("describe", "ready")
      refute reply.key?("identity"), "describe carries no session"
    end

    def test_describe_advertises_every_capability_the_relay_requires
      source = File.expand_path("../../src/rpc_client.rs", __dir__)
      skip "relay source is not in this checkout" unless File.file?(source)

      required = File.read(source)[/REQUIRED_CAPABILITIES: &\[&str\] = &\[([^\]]*)\]/, 1].to_s.scan(/"([^"]+)"/).flatten
      refute_empty required, "REQUIRED_CAPABILITIES not found in #{source}"
      assert_empty required - dispatch(request("describe")).to_h.dig("describe", "capabilities")
    end

    def test_describe_is_not_ready_without_a_channel_registry
      reply = dispatch(request("describe"), registry: Orbitcast::ChannelRegistry.new([])).to_h

      refute reply.dig("describe", "ready")
    end

    # --- identify -------------------------------------------------------------

    def test_identify_runs_connect_and_returns_the_identity_map
      reply = dispatch(request("identify")).to_h

      assert_equal "accepted", reply["outcome"]
      assert_equal({ "current_user" => "marissa", "identity_key" => "marissa" }, reply["identity"])
      assert_equal({ "locale" => "fr" }, reply["state"])
      assert_equal 1, reply["new_revision"]
    end

    def test_reject_unauthorized_connection_is_a_denial_not_a_fault
      TestConnection.behaviour = :reject
      reply = dispatch(request("identify")).to_h

      assert_equal "denied", reply["outcome"]
      refute reply.key?("identity")
      refute reply.key?("state")
      assert_equal 0, reply["new_revision"], "a refused connection never advances state"
    end

    def test_an_exception_in_connect_is_a_fault
      TestConnection.behaviour = :raise
      reply = dispatch(request("identify")).to_h

      assert_equal "fault", reply["outcome"]
      assert_equal "RuntimeError", reply["reason"]
      assert_empty reply["effects"]
    end

    def test_an_anonymous_connection_is_accepted_with_an_empty_identity_map
      reply = dispatch(request("identify", connection: connection_snapshot(cookie: "none"))).to_h

      # No cookie, no user: this application refuses. An application that does
      # not call reject_unauthorized_connection would get `identity: {}`.
      assert_equal "denied", reply["outcome"]

      anonymous = Class.new(Orbitcast::Connection) do
        def self.name = "DispatcherTest::AnonymousConnection"
      end
      reply = dispatch(request("identify"), connection_class: anonymous).to_h

      assert_equal "accepted", reply["outcome"]
      assert_empty reply["identity"]
    end

    # --- open_channel ---------------------------------------------------------

    def test_open_channel_collects_ordered_effects_and_channel_state
      reply = dispatch(request("open_channel")).to_h

      assert_equal "accepted", reply["outcome"]
      assert_equal(
        %w[add_stream add_stream transmit], reply["effects"].map { |effect| effect["kind"] }
      )
      assert_equal "chat:42", reply["effects"][0]["stream"]
      assert_equal "chat:all", reply["effects"][1]["stream"]
      assert_equal IDENTIFIER, reply["effects"][2]["identifier"]
      assert_equal(
        { "type" => "backlog", "user" => "marissa" },
        JSON.parse(reply["effects"][2]["json_payload"])
      )
      assert_equal({ "cursor" => 1 }, reply["subscription_state"])
      refute reply.key?("state"), "the connection state did not move"
    end

    def test_the_identity_is_restored_before_the_callbacks_run
      snapshot = connection_snapshot(identity: { "current_user" => "restored" }, cookie: "none")
      reply = dispatch(request("open_channel", connection: snapshot)).to_h

      assert_equal(
        "restored", JSON.parse(reply["effects"].last["json_payload"])["user"],
        "connect does not run again: the identity comes back from the relay"
      )
    end

    def test_a_rejected_subscription_is_a_denial_that_carries_nothing
      identifier = '{"channel":"DispatcherTest::ChatChannel","room":"forbidden"}'
      reply = dispatch(request("open_channel", identifier: identifier)).to_h

      assert_equal "denied", reply["outcome"]
      assert_empty reply["effects"], "whatever the callbacks collected is dropped"
      refute reply.key?("subscription_state")
      assert_equal [[:unsubscribed, identifier]], ChatChannel.log,
                   "a rejected subscription unwinds exactly as it does in ActionCable"
    end

    # The whole reason the registry exists.
    def test_a_channel_outside_the_registry_is_denied_without_being_constantized
      reply = dispatch(request("open_channel", identifier: '{"channel":"Kernel"}')).to_h

      assert_equal "denied", reply["outcome"]
      assert_includes reply["reason"], "registry"
    end

    def test_a_malformed_identifier_is_denied
      ['not json', '["array"]', '"string"'].each do |identifier|
        reply = dispatch(request("open_channel", identifier: identifier)).to_h

        assert_equal "denied", reply["outcome"], identifier
      end
    end

    # --- run_action -----------------------------------------------------------

    def test_run_action_dispatches_the_client_encoded_data
      reply = dispatch(
        request("run_action", json_data: '{"action":"speak","text":"héllo"}')
      ).to_h

      assert_equal "accepted", reply["outcome"]
      assert_equal(
        { "said" => "héllo" }, JSON.parse(reply["effects"].first["json_payload"])
      )
    end

    def test_an_action_can_change_the_installed_streams
      reply = dispatch(
        request("run_action", json_data: '{"action":"leave"}', streams: %w[chat:42 chat:all])
      ).to_h

      assert_equal(
        [{ "kind" => "remove_stream", "identifier" => IDENTIFIER, "stream" => "chat:all" }],
        reply["effects"]
      )
    end

    def test_declared_channel_state_survives_between_requests
      reply = dispatch(
        request("run_action", json_data: '{"action":"speak","text":"again"}',
                              subscription_state: { "cursor" => 4 })
      ).to_h

      assert_equal({ "cursor" => 5 }, reply["subscription_state"])
    end

    def test_an_action_outside_the_allowlist_is_denied
      %w[subscribe_to_channel instance_variable_get send cursor= nope].each do |action|
        reply = dispatch(request("run_action", json_data: %({"action":"#{action}"}))).to_h

        assert_equal "denied", reply["outcome"], action
        assert_equal "unprocessable action", reply["reason"], action
      end
    end

    def test_an_action_that_raises_is_a_fault
      reply = dispatch(request("run_action", json_data: '{"action":"explode"}')).to_h

      assert_equal "fault", reply["outcome"]
      assert_equal "ArgumentError", reply["reason"]
    end

    def test_action_data_that_is_not_an_object_is_denied
      reply = dispatch(request("run_action", json_data: '"speak"')).to_h

      assert_equal "denied", reply["outcome"]
    end

    # --- teardown -------------------------------------------------------------

    def test_close_channel_runs_unsubscribed_and_emits_nothing
      reply = dispatch(request("close_channel")).to_h

      assert_equal "accepted", reply["outcome"]
      assert_equal [[:unsubscribed, IDENTIFIER]], ChatChannel.log
      assert_empty reply["effects"],
                   "the relay already dropped the subscription these would address"
      refute reply.key?("state")
    end

    def test_release_connection_unwinds_every_subscription_then_disconnects
      reply = dispatch(request("release_connection")).to_h

      assert_equal "accepted", reply["outcome"]
      assert_equal [[:unsubscribed, IDENTIFIER]], ChatChannel.log
      assert_equal [[:disconnect, "marissa"]], TestConnection.log
      assert_empty reply["effects"]
    end

    def test_release_connection_runs_disconnect_even_when_a_channel_raises
      exploding = Class.new(Orbitcast::Channel) do
        def self.name = "DispatcherTest::ExplodingChannel"
        def unsubscribed = raise("cleanup failed")
      end
      Object.const_set(:ExplodingChannel, exploding) unless defined?(::ExplodingChannel)

      registry = Orbitcast::ChannelRegistry.new(["ExplodingChannel"])
      reply = dispatch(
        request("release_connection", identifier: '{"channel":"ExplodingChannel"}'),
        registry: registry
      ).to_h

      assert_equal "accepted", reply["outcome"]
      assert_equal [[:disconnect, "marissa"]], TestConnection.log,
                   "one failing unsubscribe must not swallow the disconnect callback"
    end

    # --- unsupported constructs ------------------------------------------------

    def test_a_periodic_timer_fails_clearly_at_declaration
      error = assert_raises(Orbitcast::UnsupportedChannelFeature) do
        TimerChannel.periodically(:tick, every: 5)
      end

      assert_includes error.message, "event loop"
    end

    def test_a_stream_transformation_block_fails_clearly
      channel = ChatChannel.new(
        build_connection, IDENTIFIER, { "channel" => "DispatcherTest::ChatChannel" }
      )

      assert_raises(Orbitcast::UnsupportedChannelFeature) do
        channel.stream_from("chat:42") { |message| message }
      end
      assert_raises(Orbitcast::UnsupportedChannelFeature) do
        channel.stream_from("chat:42", coder: ActiveSupport::JSON)
      end
    end

    private

    def dispatch(request, registry: nil, connection_class: nil)
      Orbitcast::Dispatcher.new(
        config: build_config(namespace: "example_production"),
        registry: registry || Orbitcast::ChannelRegistry.new(["DispatcherTest::ChatChannel"]),
        connection_class: connection_class || TestConnection
      ).call(request)
    end

    def build_connection
      TestConnection.new(
        V1::Request::ConnectionSnapshot.from(connection_snapshot),
        effects: V1::EffectCollector.new
      )
    end

    def connection_snapshot(identity: {}, state: {}, cookie: "user=marissa")
      {
        "url" => "/cable",
        "origin" => "https://app.example",
        "client_address" => "203.0.113.7",
        "headers" => { "cookie" => cookie },
        "identity" => identity,
        "state" => state
      }
    end

    def request(operation, identifier: IDENTIFIER, connection: nil, json_data: nil,
                streams: [], subscription_state: {})
      hash = {
        "version" => 1,
        "operation" => operation,
        "namespace" => "example_production",
        "node_id" => "node-7",
        "session_id" => operation == "describe" ? "00000000-0000-0000-0000-000000000000" : SESSION,
        "request_id" => REQUEST_ID,
        "sequence" => 3,
        "expected_revision" => 0,
        "deadline_ms" => 1000
      }
      unless operation == "describe"
        # Every operation after `identify` carries the identity the relay is
        # holding: `connect` does not run again.
        default = operation == "identify" ? {} : { "current_user" => "marissa" }
        hash["connection"] = connection || connection_snapshot(identity: default)
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

      V1::Request.from(hash).validate!
    end
  end
end
