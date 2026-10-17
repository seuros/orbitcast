# frozen_string_literal: true

require "test_helper"
require "json"

require "orbitcast/publisher/control"

# Rails-initiated remote disconnect over the PostgreSQL control channel.
class PublisherControlTest < Minitest::Test
  Control = Orbitcast::Publisher::Control
  V1 = Orbitcast::Protocol::V1

  # MOCK: stands in for a `PG::Connection`; captures pg_notify calls. No
  # database is started for these tests.
  class FakeConnection
    attr_reader :calls

    def initialize
      @calls = []
    end

    def exec_params(sql, params)
      @calls << [sql, params]
      [{ "pg_notify" => nil }]
    end

    def close; end
  end

  def setup
    @connection = FakeConnection.new
    @config = build_config
    @publisher = Control.new(@config, connection_factory: -> { @connection })
  end

  def teardown
    Orbitcast::Control.publisher = nil
  end

  def test_the_control_channel_is_derived_like_every_other_channel_name
    channel = Control.channel_for(Orbitcast::TestHelper::NAMESPACE)

    assert_equal(
      Orbitcast::Publisher::Postgres.channel_name(
        Orbitcast::TestHelper::NAMESPACE, Control::CONTROL_STREAM
      ),
      channel
    )
    assert_equal 58, channel.bytesize
    assert channel.start_with?("orbitcast_")
  end

  def test_a_control_channel_is_namespaced
    refute_equal Control.channel_for("one"), Control.channel_for("two"),
                 "one deployment's control messages must be invisible to another"
  end

  def test_publishing_a_disconnect_emits_one_notify_with_the_pinned_envelope
    message = V1::ControlMessage.disconnect(
      namespace: @config.namespace, identity_key: "user:7"
    )
    control_id = @publisher.publish_control(message)

    assert_equal 1, @connection.calls.size

    sql, (channel, wire) = @connection.calls.first

    assert_equal "SELECT pg_notify($1, $2)", sql
    assert_equal Control.channel_for(@config.namespace), channel
    assert_equal(
      {
        "version" => 1,
        "kind" => "disconnect",
        "namespace" => @config.namespace,
        "control_id" => control_id,
        "identity_key" => "user:7",
        "reason" => "remote",
        "reconnect" => false
      },
      JSON.parse(wire)
    )
  end

  def test_a_message_for_another_namespace_is_refused
    message = V1::ControlMessage.disconnect(namespace: "somewhere_else", identity_key: "user:7")

    assert_raises(ArgumentError) { @publisher.publish_control(message) }
    assert_empty @connection.calls
  end

  def test_an_invalid_message_never_reaches_the_database
    message = V1::ControlMessage.disconnect(
      namespace: @config.namespace, identity_key: "user:7", reason: "vibes"
    )

    assert_raises(Orbitcast::ProtocolError) { @publisher.publish_control(message) }
    assert_empty @connection.calls
  end

  def test_an_oversized_identity_key_is_refused
    message = V1::ControlMessage.disconnect(
      namespace: @config.namespace, identity_key: "k" * (V1::MAX_IDENTITY_KEY_LEN + 1)
    )

    assert_raises(Orbitcast::ProtocolError) { @publisher.publish_control(message) }
    assert_empty @connection.calls
  end

  # The application-facing entry point.
  def test_orbitcast_disconnect_publishes_through_the_control_publisher
    Orbitcast::Control.publisher = @publisher

    control_id = Orbitcast::Control.disconnect(
      identity_key: "gid://app/User/7", reason: "server_restart", reconnect: true, config: @config
    )

    _sql, (_channel, wire) = @connection.calls.first
    message = JSON.parse(wire)

    assert_equal control_id, message["control_id"]
    assert_equal "gid://app/User/7", message["identity_key"]
    assert_equal "server_restart", message["reason"]
    assert message["reconnect"]
  end

  def test_the_module_level_entry_point_uses_the_global_configuration
    Orbitcast::Control.publisher = @publisher

    with_global_config(
      namespace: Orbitcast::TestHelper::NAMESPACE,
      database_url: Orbitcast::TestHelper::DATABASE_URL
    ) do
      Orbitcast.disconnect(identity_key: "user:7")
    end

    _sql, (_channel, wire) = @connection.calls.first

    assert_equal "user:7", JSON.parse(wire)["identity_key"]
  end

  def test_a_disconnect_defaults_to_remote_without_a_reconnect
    Orbitcast::Control.publisher = @publisher
    Orbitcast::Control.disconnect(identity_key: "user:7", config: @config)

    _sql, (_channel, wire) = @connection.calls.first
    message = JSON.parse(wire)

    assert_equal "remote", message["reason"]
    refute message["reconnect"],
           "a sign-out must not invite an immediate reconnect with revoked credentials"
  end
end
