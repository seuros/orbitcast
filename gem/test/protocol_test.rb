# frozen_string_literal: true

require "test_helper"
require "json"

# The v1 RPC envelopes: tolerant reading, fail-closed validation, and the
# fail-closed reply builder.
#
# Plain Ruby: no Rails boot, no ActionCable, no PostgreSQL. The cross-language
# assertions against tests/fixtures/rpc/*.json live in contract_test.rb.
class ProtocolTest < Minitest::Test
  V1 = Orbitcast::Protocol::V1

  SESSION = "2f4a6c8e-1b3d-4f5a-9c7e-0d1a2b3c4d5e"
  REQUEST = "9a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d"
  IDENTIFIER = '{"channel":"ChatChannel","room":"42"}'

  # --- requests -------------------------------------------------------------

  def test_request_parses_and_validates_every_operation
    assert_equal V1::OPERATIONS.sort, %w[
      close_channel describe identify open_channel release_connection run_action
    ]

    V1::OPERATIONS.each do |operation|
      request = build_request(operation).validate!

      assert_equal operation, request.operation
      assert_equal 1, request.version
    end
  end

  def test_request_ignores_unknown_fields_and_defaults_an_absent_version
    wire = JSON.generate(
      build_request("identify").to_h.merge("trace_id" => "abc", "future" => { "a" => [1] })
                                    .except("version")
    )
    request = V1::Request.parse(wire).validate!

    assert_equal 1, request.version
    assert_equal "identify", request.operation
  end

  def test_request_rejects_an_unknown_operation
    error = assert_raises(Orbitcast::ProtocolError) do
      V1::Request.from({ "operation" => "teleport" })
    end

    assert_equal "operation", error.field
  end

  def test_request_rejects_unsupported_versions_and_bad_uuids
    assert_raises(Orbitcast::ProtocolError) do
      V1::Request.from(build_request("identify").to_h.merge("version" => 2)).validate!
    end

    error = assert_raises(Orbitcast::ProtocolError) do
      V1::Request.from(build_request("identify").to_h.merge("session_id" => "nope")).validate!
    end

    assert_equal "session_id", error.field
  end

  def test_request_rejects_missing_operation_payloads
    bare = build_request("identify").to_h.except("connection")
    error = assert_raises(Orbitcast::ProtocolError) { V1::Request.from(bare).validate! }

    assert_equal "connection", error.field

    without_subscription = build_request("open_channel").to_h.except("subscription")
    error = assert_raises(Orbitcast::ProtocolError) do
      V1::Request.from(without_subscription).validate!
    end

    assert_equal "subscription", error.field
  end

  def test_request_rejects_an_unusable_deadline
    [0, V1::MAX_DEADLINE_MS + 1].each do |deadline|
      error = assert_raises(Orbitcast::ProtocolError) do
        V1::Request.from(build_request("identify").to_h.merge("deadline_ms" => deadline)).validate!
      end

      assert_equal "deadline_ms", error.field
    end
  end

  def test_request_rejects_hop_by_hop_and_uppercase_headers
    %w[transfer-encoding host connection].each do |header|
      hash = build_request("identify").to_h
      hash["connection"]["headers"] = { header => "x" }

      assert_raises(Orbitcast::ProtocolError) { V1::Request.from(hash).validate! }
    end

    hash = build_request("identify").to_h
    hash["connection"]["headers"] = { "Cookie" => "x" }
    error = assert_raises(Orbitcast::ProtocolError) { V1::Request.from(hash).validate! }

    assert_equal "header_name", error.field
  end

  def test_run_action_carries_the_client_encoded_data_byte_for_byte
    json_data = '{"action":"speak","text":"héllo"}'
    request = V1::Request.from(
      build_request("run_action").to_h.merge(
        "action" => { "identifier" => IDENTIFIER, "json_data" => json_data }
      )
    ).validate!

    assert_equal json_data, request.action.json_data
    assert_equal({ "action" => "speak", "text" => "héllo" }, request.action.data)
  end

  def test_release_payload_requires_an_allowlisted_reason
    hash = build_request("release_connection").to_h
    hash["release"]["reason"] = "because"
    error = assert_raises(Orbitcast::ProtocolError) { V1::Request.from(hash).validate! }

    assert_equal "reason", error.field
  end

  # --- replies --------------------------------------------------------------

  def test_accepted_reply_advances_the_revision_and_omits_an_unchanged_state
    request = build_request("identify")
    reply = V1::Reply.accepted(request, identity: { "identity_key" => "user:7" })
    hash = reply.to_h

    assert_equal 1, hash["version"]
    assert_equal "accepted", hash["outcome"]
    assert_equal request.expected_revision + 1, hash["new_revision"]
    assert_equal({ "identity_key" => "user:7" }, hash["identity"])
    assert_empty hash["effects"]
    refute hash.key?("state"), "an unchanged state must be omitted, never emitted as {}"
    refute hash.key?("subscription_state")
    refute hash.key?("reason")
  end

  def test_a_cleared_state_is_emitted_and_an_unchanged_one_is_not
    request = build_request("identify")

    cleared = V1::Reply.accepted(request, state: {}).to_h

    assert_equal({}, cleared["state"], "an empty object is an explicit clear")

    replaced = V1::Reply.accepted(request, state: { "cart_id" => 19 }).to_h

    assert_equal({ "cart_id" => 19 }, replaced["state"])

    unchanged = V1::Reply.accepted(request, state: V1::Reply::UNCHANGED).to_h

    refute unchanged.key?("state")
  end

  # The whole point of the builder: whatever the callbacks collected before a
  # refusal, a denial changes nothing.
  def test_a_denied_reply_drops_effects_state_and_identity
    request = build_request("open_channel")
    effects = V1::EffectCollector.new
    effects.add_stream(IDENTIFIER, "chat:42")

    reply = V1::Reply.new(
      request: request,
      outcome: V1::OUTCOME_DENIED,
      effects: effects,
      state: { "cart_id" => 19 },
      subscription_state: { "seen" => 1 },
      identity: { "identity_key" => "user:7" },
      reason: "not your room"
    )
    hash = reply.to_h

    assert_equal "denied", hash["outcome"]
    assert_empty hash["effects"]
    refute hash.key?("state")
    refute hash.key?("subscription_state")
    refute hash.key?("identity")
    assert_equal request.expected_revision, hash["new_revision"], "a denial never advances state"
    assert_equal "not your room", hash["reason"]
  end

  def test_a_fault_reply_carries_nothing_but_its_reason
    request = build_request("open_channel")
    hash = V1::Reply.fault(request, "ActiveRecord::ConnectionTimeoutError").to_h

    assert_equal "fault", hash["outcome"]
    assert_empty hash["effects"]
    refute hash.key?("state")
    assert_equal "ActiveRecord::ConnectionTimeoutError", hash["reason"]
  end

  def test_identity_is_emitted_only_for_an_accepted_identify
    accepted_identify = V1::Reply.accepted(build_request("identify"), identity: {}).to_h

    assert_equal({}, accepted_identify["identity"],
                 "an empty identity map is a valid accepted anonymous connection")

    accepted_open = V1::Reply.accepted(build_request("open_channel")).to_h

    refute accepted_open.key?("identity")
  end

  def test_a_reason_longer_than_the_limit_is_truncated_rather_than_emitted_invalid
    reply = V1::Reply.fault(build_request("identify"), "x" * (V1::MAX_REASON_LEN + 50))

    assert_equal V1::MAX_REASON_LEN, reply.to_h["reason"].bytesize
  end

  def test_a_describe_reply_must_advertise_version_one
    request = build_request("describe")

    assert_equal(
      [1], V1::Reply.describe(request).to_h.dig("describe", "supported_versions")
    )
    assert_equal request.expected_revision, V1::Reply.describe(request).to_h["new_revision"],
                 "describe is not stateful"

    reply = V1::Reply.accepted(request, describe: { "supported_versions" => [2] })

    assert_raises(Orbitcast::ProtocolError) { reply.to_h }
  end

  def test_an_accepted_describe_without_a_payload_fails_closed
    assert_raises(Orbitcast::ProtocolError) do
      V1::Reply.accepted(build_request("describe")).to_h
    end
  end

  # --- effects --------------------------------------------------------------

  def test_effects_keep_the_order_they_were_collected_in
    effects = V1::EffectCollector.new
    effects.add_stream(IDENTIFIER, "chat:42")
    effects.transmit(IDENTIFIER, '{"a":1}')
    effects.add_stream(IDENTIFIER, "chat:all")
    effects.remove_stream(IDENTIFIER, "chat:42")
    effects.remove_all_streams(IDENTIFIER)
    effects.close(reason: "unauthorized", reconnect: false)

    assert_equal(
      %w[add_stream transmit add_stream remove_stream remove_all_streams close],
      effects.to_a.map { |effect| effect["kind"] }
    )
  end

  def test_a_suppressed_collector_records_nothing
    effects = V1::EffectCollector.new(suppressed: true)
    effects.add_stream(IDENTIFIER, "chat:42")
    effects.remove_all_streams(IDENTIFIER)
    effects.transmit(IDENTIFIER, "{}")

    assert_predicate effects, :suppressed?
    assert_empty effects.to_a
  end

  def test_effects_validate_identifiers_streams_payloads_and_reasons
    effects = V1::EffectCollector.new

    assert_raises(Orbitcast::ProtocolError) { effects.add_stream("", "chat:42") }
    assert_raises(Orbitcast::ProtocolError) { effects.add_stream(IDENTIFIER, "") }
    assert_raises(Orbitcast::ProtocolError) { effects.transmit(IDENTIFIER, "{oops") }
    assert_raises(Orbitcast::ProtocolError) { effects.close(reason: "you_are_fired") }
    assert_raises(Orbitcast::ProtocolError) do
      effects.presence_grant(IDENTIFIER, stream: "chat:42", subject_id: "user:7", ttl_ms: 0)
    end
    assert_empty effects.to_a
  end

  def test_more_than_the_maximum_number_of_effects_is_refused_at_the_boundary
    effects = V1::EffectCollector.new
    V1::MAX_EFFECTS.times { effects.remove_all_streams(IDENTIFIER) }

    assert_raises(Orbitcast::ProtocolError) { effects.remove_all_streams(IDENTIFIER) }
    assert_equal V1::MAX_EFFECTS, effects.size
  end

  def test_presence_effects_are_emitted_in_full
    effects = V1::EffectCollector.new
    effects.presence_grant(
      IDENTIFIER, stream: "chat:42", subject_id: "user:7",
                  ttl_ms: 45_000, metadata: { "name" => "Marissa" }, operations: %w[join snapshot]
    )
    effects.presence_revoke(IDENTIFIER, stream: "chat:42", subject_id: "user:9")
    grant, revoke = effects.to_a

    assert_equal "presence_grant", grant["kind"]
    assert_equal 45_000, grant["ttl_ms"]
    assert_equal %w[join snapshot], grant["operations"]
    assert_equal "user:9", revoke["subject_id"]
  end

  # --- state maps -----------------------------------------------------------

  def test_state_maps_are_bounded_by_entries_key_length_and_encoded_size
    too_many = (0..V1::MAX_STATE_ENTRIES).to_h { |index| ["k#{index}", 1] }

    assert_raises(Orbitcast::ProtocolError) { V1.validate_state_map!("state", too_many) }

    long_key = { "k" * (V1::MAX_STATE_KEY_LEN + 1) => 1 }

    assert_raises(Orbitcast::ProtocolError) { V1.validate_state_map!("state", long_key) }

    huge = { "big" => "x" * V1::MAX_STATE_LEN }

    assert_raises(Orbitcast::ProtocolError) { V1.validate_state_map!("state", huge) }
  end

  # --- control --------------------------------------------------------------

  def test_control_messages_default_reconnect_to_false
    message = V1::ControlMessage.disconnect(namespace: "app", identity_key: "user:7")

    assert_equal "disconnect", message.kind
    assert_equal "remote", message.reason
    refute_predicate message, :reconnect?
    assert_match(/\A\h{8}-/, message.control_id)
    message.validate!
  end

  def test_control_messages_reject_unknown_kinds_and_unlisted_reasons
    error = assert_raises(Orbitcast::ProtocolError) do
      V1::ControlMessage.from({ "kind" => "self_destruct", "namespace" => "app" })
    end

    assert_equal "kind", error.field

    message = V1::ControlMessage.disconnect(
      namespace: "app", identity_key: "user:7", reason: "vibes"
    )

    assert_raises(Orbitcast::ProtocolError) { message.validate! }
  end

  def test_a_control_message_applies_only_to_its_own_namespace
    message = V1::ControlMessage.disconnect(namespace: "app", identity_key: "user:7")

    assert message.applies_to?("app")
    refute message.applies_to?("other")
  end

  private

  def build_request(operation, overrides = {})
    hash = {
      "version" => 1,
      "operation" => operation,
      "namespace" => "example_production",
      "node_id" => "node-7",
      "session_id" => SESSION,
      "request_id" => REQUEST,
      "sequence" => 2,
      "expected_revision" => 1,
      "deadline_ms" => 1000
    }

    unless operation == "describe"
      hash["connection"] = {
        "url" => "/cable?tab=1",
        "origin" => "https://app.example",
        "client_address" => "203.0.113.7",
        "headers" => { "cookie" => "_session=abc" },
        "identity" => {},
        "state" => {}
      }
    end

    case operation
    when "open_channel", "close_channel"
      hash["subscription"] = subscription_hash
    when "run_action"
      hash["subscription"] = subscription_hash
      hash["action"] = { "identifier" => IDENTIFIER, "json_data" => '{"action":"speak"}' }
    when "release_connection"
      hash["release"] = {
        "reason" => "remote", "reconnect" => false, "subscriptions" => [subscription_hash]
      }
    end

    V1::Request.from(hash.merge(overrides))
  end

  def subscription_hash
    {
      "identifier" => IDENTIFIER,
      "params" => { "channel" => "ChatChannel", "room" => "42" },
      "streams" => ["chat:42"],
      "state" => {}
    }
  end
end
