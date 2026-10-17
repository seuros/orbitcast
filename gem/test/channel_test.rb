# frozen_string_literal: true

require "test_helper"
require "json"

begin
  # ActionCable::Channel::Base#initialize reaches for Concurrent::AtomicFixnum
  # without requiring it; in an application it is already loaded by the time a
  # channel is instantiated, and here nothing else pulls it in.
  require "concurrent/atomic/atomic_fixnum"
  require "orbitcast/channel"
  CHANNEL_AVAILABLE = true
rescue LoadError
  # Needs ActionCable. The plain unit suite skips this file on a checkout
  # without Rails; rails_contract_test.rb covers the same class against a real
  # application.
  CHANNEL_AVAILABLE = false
end

# The server-side presence DSL on `Orbitcast::Channel`.
#
# ActionCable is real here — these are actual `ActionCable::Channel::Base`
# subclasses — but there is no connection, no relay, and no Rails application:
# a grant is a hash appended to an `EffectCollector`, and that is exactly the
# surface these tests pin. What the *relay* then does with it is Rust's half,
# covered by tests/fixtures/rpc/reply_open_channel_effects.json from both sides.
if CHANNEL_AVAILABLE
  class ChannelTest < Minitest::Test
    V1 = Orbitcast::Protocol::V1

    IDENTIFIER = '{"channel":"ChannelTest::RoomChannel","room":"42"}'

    # MOCK: stands in for an Orbitcast::Connection. The channel touches exactly
    # two things on it — the identifier delegation list and the collector — so
    # those two are all this provides.
    class FakeConnection
      def initialize(effects)
        @orbitcast_effects = effects
      end

      attr_reader :orbitcast_effects

      def identifiers
        []
      end
    end

    class RoomChannel < Orbitcast::Channel
      def subscribed
        stream_from "chat:42"
        grant_presence "chat:42", subject_id: "user:7", metadata: { "name" => "Marissa" }
        stream_from "chat:all"
      end

      def speak; end
    end

    def setup
      @effects = V1::EffectCollector.new
      @channel = channel_with(@effects)
    end

    # --- grant ----------------------------------------------------------------

    def test_a_grant_must_follow_stream_from_for_the_same_stream
      channel = channel_with(V1::EffectCollector.new, streaming: [])

      error = assert_raises(ArgumentError) do
        channel.grant_presence("chat:42", subject_id: "user:7")
      end

      assert_match(/must follow stream_from/, error.message)
      assert_empty channel.orbitcast_effects.to_a, "nothing is recorded for a refused grant"

      # Streaming first is what makes the same call valid; so is a stream the
      # relay seeded for a later perform.
      channel.stream_from "chat:42"
      channel.grant_presence("chat:42", subject_id: "user:7")
      assert_equal %w[add_stream presence_grant], channel.orbitcast_effects.to_a.map { |e| e["kind"] }

      # A stream that was stopped is no longer grantable either.
      channel.stop_stream_from "chat:42"
      assert_raises(ArgumentError) { channel.grant_presence("chat:42", subject_id: "user:7") }
    end

    def test_grant_presence_emits_the_pinned_effect_shape
      effect = @channel.grant_presence(
        "chat:42",
        subject_id: "user:7",
        metadata: { "name" => "Marissa" },
        ttl: 45,
        operations: %w[join snapshot]
      )

      expected = {
        "kind" => "presence_grant",
        "identifier" => IDENTIFIER,
        "stream" => "chat:42",
        "subject_id" => "user:7",
        "metadata" => { "name" => "Marissa" },
        "ttl_ms" => 45_000,
        "operations" => %w[join snapshot]
      }

      assert_equal expected, effect
      assert_equal [expected], @effects.to_a
      # The identifier is the channel's own, never an argument: a channel can
      # only grant presence on the subscription it *is*.
      assert_equal IDENTIFIER, effect.fetch("identifier")
    end

    def test_a_grant_defaults_to_every_operation_and_the_default_lease
      effect = @channel.grant_presence("chat:42", subject_id: "user:7")

      assert_equal %w[join leave snapshot], effect.fetch("operations")
      assert_equal Orbitcast::Channel::PRESENCE_OPERATIONS, effect.fetch("operations")
      assert_equal Orbitcast::Channel::DEFAULT_PRESENCE_TTL * 1000, effect.fetch("ttl_ms")
      assert_empty effect.fetch("metadata")
    end

    def test_the_ttl_is_seconds_in_milliseconds_out
      {
        45 => 45_000,
        0.5 => 500,
        1.2345 => 1235, # rounded, never truncated to zero
        3600 => 3_600_000
      }.each do |ttl, expected_ms|
        effect = channel_with(V1::EffectCollector.new)
                 .grant_presence("chat:42", subject_id: "user:7", ttl: ttl)

        assert_equal expected_ms, effect.fetch("ttl_ms"), "ttl: #{ttl.inspect}"
        assert_kind_of Integer, effect.fetch("ttl_ms"), "ttl_ms is an integer on the wire"
      end
    end

    def test_a_symbol_stream_and_symbol_operations_are_accepted_as_strings
      effect = @channel.grant_presence(
        :"chat:42", subject_id: "user:7", operations: %i[join leave]
      )

      assert_equal "chat:42", effect.fetch("stream")
      assert_equal %w[join leave], effect.fetch("operations")
    end

    def test_metadata_keys_travel_as_strings
      effect = @channel.grant_presence(
        "chat:42", subject_id: "user:7", metadata: { name: "Marissa", tz: "UTC" }
      )

      assert_equal({ "name" => "Marissa", "tz" => "UTC" }, effect.fetch("metadata"))
      assert_equal(
        '{"name":"Marissa","tz":"UTC"}', JSON.generate(effect.fetch("metadata"))
      )
    end

    def test_duplicate_operations_are_collapsed
      effect = @channel.grant_presence(
        "chat:42", subject_id: "user:7", operations: %w[join join snapshot]
      )

      assert_equal %w[join snapshot], effect.fetch("operations")
    end

    # --- revoke ---------------------------------------------------------------

    def test_revoke_presence_emits_the_pinned_effect_shape
      effect = @channel.revoke_presence("chat:42", subject_id: "user:9")

      assert_equal(
        {
          "kind" => "presence_revoke",
          "identifier" => IDENTIFIER,
          "stream" => "chat:42",
          "subject_id" => "user:9"
        },
        effect
      )
      # A revoke names no lease and no metadata: the subject is going away.
      assert_equal %w[kind identifier stream subject_id], effect.keys
    end

    def test_revoke_rejects_the_same_unusable_arguments_a_grant_does
      assert_raises(ArgumentError) { @channel.revoke_presence("", subject_id: "user:7") }
      assert_raises(ArgumentError) { @channel.revoke_presence("chat:42", subject_id: "") }
      assert_raises(ArgumentError) { @channel.revoke_presence(:not_a_model, subject_id: 7) }
      assert_empty @effects.to_a, "a refused revoke records nothing"
    end

    # --- limits ---------------------------------------------------------------
    #
    # Each of these mirrors a check in `Effect::validate` (src/protocol/v1/rpc.rs).
    # Rails fails in the callback rather than emitting a reply the relay rejects.

    def test_an_oversized_subject_id_is_refused
      # The boundary itself is legal; one byte past it is not.
      at_limit = "u" * V1::MAX_SUBJECT_ID_LEN
      effect = @channel.grant_presence("chat:42", subject_id: at_limit)

      assert_equal at_limit, effect.fetch("subject_id")

      error = assert_raises(ArgumentError) do
        @channel.grant_presence("chat:42", subject_id: "u" * (V1::MAX_SUBJECT_ID_LEN + 1))
      end

      assert_match(/subject_id/, error.message)
      assert_equal 1, @effects.size, "the refused grant was not recorded"
    end

    def test_an_empty_or_non_string_subject_id_is_refused
      assert_raises(ArgumentError) { @channel.grant_presence("chat:42", subject_id: "") }
      assert_raises(ArgumentError) { @channel.grant_presence("chat:42", subject_id: 7) }
      assert_raises(ArgumentError) { @channel.grant_presence("chat:42", subject_id: nil) }
    end

    def test_metadata_is_bounded_by_its_encoded_size
      # One huge value is as expensive as many small ones, so the *encoded*
      # size is what is checked.
      error = assert_raises(ArgumentError) do
        @channel.grant_presence(
          "chat:42",
          subject_id: "user:7",
          metadata: { "bio" => "x" * V1::MAX_PRESENCE_METADATA_LEN }
        )
      end

      assert_match(/metadata/, error.message)

      # And just under the ceiling still passes.
      room = V1::MAX_PRESENCE_METADATA_LEN - JSON.generate({ "bio" => "" }).bytesize
      effect = @channel.grant_presence(
        "chat:42", subject_id: "user:7", metadata: { "bio" => "x" * room }
      )

      assert_operator JSON.generate(effect.fetch("metadata")).bytesize,
                      :<=, V1::MAX_PRESENCE_METADATA_LEN
    end

    def test_metadata_must_be_a_hash_with_string_keys
      assert_raises(ArgumentError) do
        @channel.grant_presence("chat:42", subject_id: "user:7", metadata: [1, 2])
      end
      assert_raises(ArgumentError) do
        @channel.grant_presence("chat:42", subject_id: "user:7", metadata: { 7 => "seven" })
      end
    end

    def test_a_non_positive_ttl_fails_closed
      [0, -1, 0.0004].each do |ttl|
        error = assert_raises(ArgumentError, "ttl: #{ttl.inspect}") do
          @channel.grant_presence("chat:42", subject_id: "user:7", ttl: ttl)
        end

        assert_match(/ttl/, error.message)
      end

      assert_raises(ArgumentError) do
        @channel.grant_presence("chat:42", subject_id: "user:7", ttl: "45")
      end
      assert_empty @effects.to_a
    end

    def test_operations_must_be_a_subset_the_server_understands
      error = assert_raises(ArgumentError) do
        @channel.grant_presence("chat:42", subject_id: "user:7", operations: %w[join administrate])
      end

      assert_match(/administrate/, error.message)

      assert_raises(ArgumentError) do
        @channel.grant_presence("chat:42", subject_id: "user:7", operations: [])
      end
      assert_raises(ArgumentError) do
        @channel.grant_presence("chat:42", subject_id: "user:7", operations: "join")
      end
      assert_empty @effects.to_a
    end

    def test_a_model_is_not_silently_stringified_into_a_stream_name
      model = Struct.new(:id).new(42)

      error = assert_raises(ArgumentError) do
        @channel.grant_presence(model, subject_id: "user:7")
      end

      assert_match(/broadcasting_for/, error.message)
    end

    # --- ordering and suppression ---------------------------------------------

    def test_effects_keep_the_order_the_callbacks_ran_in
      @channel.subscribed

      assert_equal(
        %w[add_stream presence_grant add_stream], @effects.to_a.map { |e| e["kind"] }
      )
      assert_equal %w[chat:42 chat:42 chat:all], @effects.to_a.map { |e| e["stream"] }
    end

    def test_a_grant_interleaves_with_stream_and_revoke_effects_in_order
      @channel.stream_from "chat:42"
      @channel.grant_presence "chat:42", subject_id: "user:7"
      @channel.revoke_presence "chat:42", subject_id: "user:9"
      @channel.stop_stream_from "chat:42"

      assert_equal(
        %w[add_stream presence_grant presence_revoke remove_stream],
        @effects.to_a.map { |effect| effect["kind"] }
      )
    end

    # `close_channel` and `release_connection` run after the relay has already
    # tombstoned the subscription, so their collector records nothing.
    def test_a_suppressed_collector_records_no_presence_effects
      suppressed = V1::EffectCollector.new(suppressed: true)
      channel = channel_with(suppressed)

      assert_nil channel.grant_presence("chat:42", subject_id: "user:7")
      assert_nil channel.revoke_presence("chat:42", subject_id: "user:7")
      assert_empty suppressed.to_a
    end

    # --- the DSL is not an action --------------------------------------------

    def test_the_presence_dsl_is_never_a_client_callable_action
      # The two methods that decide identity are exactly the two a browser must
      # not be able to name in data["action"].
      %w[grant_presence revoke_presence].each do |method|
        refute_includes RoomChannel.action_methods, method,
                        "#{method} must never be dispatchable from the wire"
      end

      assert_includes RoomChannel.action_methods, "speak", "ordinary actions still dispatch"
    end

    # --- the reserved action namespace ---------------------------------------
    #
    # `orbitcast_presence` is answered by the relay, never forwarded to Rails
    # (src/protocol/v1/presence.rs). A channel that defines it would be writing
    # a method that can never run, so the class body raises instead.

    def test_defining_a_reserved_action_raises_at_class_definition
      error = assert_raises(Orbitcast::UnsupportedChannelFeature) do
        Class.new(Orbitcast::Channel) do
          def orbitcast_presence(_data); end
        end
      end

      assert_match(/orbitcast_presence/, error.message)
      assert_match(/reserved action namespace/, error.message)
    end

    def test_the_whole_orbitcast_prefix_is_reserved_not_just_the_known_action
      assert_raises(Orbitcast::UnsupportedChannelFeature) do
        Class.new(Orbitcast::Channel) do
          def orbitcast_whatever_comes_next; end
        end
      end
    end

    def test_a_private_helper_with_the_prefix_is_allowed
      # Only public methods become actions, so a private helper shadows
      # nothing. (Visibility must be a section, not `private def`: Ruby runs
      # `method_added` before the inline form applies it.)
      klass = Class.new(Orbitcast::Channel) do
        private

        def orbitcast_internal_helper; end
      end

      refute_includes klass.action_methods, "orbitcast_internal_helper"
    end

    def test_the_base_class_plumbing_is_still_defined_and_still_not_an_action
      # The guard must not have made the base class unloadable, nor promoted
      # its own plumbing into the action list.
      %i[orbitcast_effects orbitcast_streams orbitcast_rejected?].each do |method|
        assert Orbitcast::Channel.public_method_defined?(method), method
        refute_includes RoomChannel.action_methods, method.to_s
      end
    end

    def test_ordinary_action_names_are_unaffected
      klass = Class.new(Orbitcast::Channel) do
        def speak; end

        def orbit_cast_is_not_the_prefix; end
      end

      assert_includes klass.action_methods, "speak"
      assert_includes klass.action_methods, "orbit_cast_is_not_the_prefix"
    end

    # --- the effect the relay parses -----------------------------------------

    def test_a_granted_effect_survives_the_reply_validator
      @channel.grant_presence(
        "chat:42", subject_id: "user:7", metadata: { "name" => "Marissa" }, ttl: 45
      )
      @channel.revoke_presence("chat:42", subject_id: "user:9")

      request = V1::Request.from(
        "version" => 1,
        "operation" => V1::OPERATION_RUN_ACTION,
        "namespace" => "example_production",
        "node_id" => "node-7",
        "session_id" => "2f4a6c8e-1b3d-4f5a-9c7e-0d1a2b3c4d5e",
        "request_id" => "7a8b9c0d-1e2f-4a3b-8c4d-5e6f7a8b9c0d",
        "sequence" => 4,
        "expected_revision" => 3,
        "deadline_ms" => 1000
      )
      reply = JSON.parse(V1::Reply.accepted(request, effects: @effects.to_a).to_json)

      assert_equal %w[presence_grant presence_revoke],
                   reply.fetch("effects").map { |effect| effect.fetch("kind") }
    end

    # The DSL against the shared golden bytes: tests/fixtures/rpc pins the
    # presence effects the relay parses, and `grant_presence` must produce them
    # from the application-facing arguments alone. contract_test.rb asserts the
    # same fixture at the collector level, where ActionCable is not needed.
    def test_the_dsl_reproduces_the_pinned_presence_effects
      reproduced = 0

      pinned_presence_effects.each do |effect|
        channel = channel_with(V1::EffectCollector.new, identifier: effect.fetch("identifier"),
                               streaming: [effect.fetch("stream")])

        produced =
          if effect.fetch("kind") == "presence_grant"
            channel.grant_presence(
              effect.fetch("stream"),
              subject_id: effect.fetch("subject_id"),
              metadata: effect.fetch("metadata"),
              # The wire field is ttl_ms; the DSL takes seconds.
              ttl: effect.fetch("ttl_ms") / 1000.0,
              operations: effect.fetch("operations")
            )
          else
            channel.revoke_presence(effect.fetch("stream"), subject_id: effect.fetch("subject_id"))
          end

        assert_equal effect, produced, "rpc fixture effect #{effect.fetch("kind")}"
        reproduced += 1
      end

      assert_operator reproduced, :>, 0, "the rpc fixtures must pin presence effects"
    end

    private

    FIXTURES = File.expand_path("../../tests/fixtures", __dir__)

    def pinned_presence_effects
      Dir[File.join(FIXTURES, "rpc", "*.json")].sort.flat_map do |path|
        JSON.parse(File.read(path)).fetch("cases").flat_map do |case_data|
          next [] unless case_data.dig("expect", "valid")

          JSON.parse(case_data.dig("input", "wire")).fetch("effects", []).select do |effect|
            %w[presence_grant presence_revoke].include?(effect["kind"])
          end
        end
      end
    end

    # `streaming` seeds the streams the relay reports as installed, so a grant
    # for one of them is in order without running `subscribed` first.
    def channel_with(effects, identifier: IDENTIFIER, streaming: %w[chat:42])
      channel = RoomChannel.new(FakeConnection.new(effects), identifier, { room: "42" })
      channel.orbitcast_installed_streams = streaming
      channel
    end
  end
end
