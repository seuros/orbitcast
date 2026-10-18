# frozen_string_literal: true

require "test_helper"

begin
  require "action_cable/subscription_adapter/orbitcast"
  ACTION_CABLE_AVAILABLE = true
rescue LoadError
  # The rest of the unit suite must run without Rails installed; this file is
  # the only one that needs ActionCable, so it skips instead of failing.
  ACTION_CABLE_AVAILABLE = false
end

class ActionCableAdapterTest < Minitest::Test
  # MOCK: the slice of ActionCable::Server::Base the adapter contract touches.
  FakeConfig = Struct.new(:cable)
  FakeServer = Struct.new(:config, :logger)

  # MOCK: stands in for Orbitcast::Publisher::Postgres.
  class FakePublisher
    attr_reader :published

    def initialize
      @published = []
      @shutdown = false
    end

    def publish(stream, payload)
      @published << [stream, payload]
      "publication-id"
    end

    def shutdown
      @shutdown = true
    end

    def shutdown? = @shutdown
  end

  def setup
    skip "actioncable is not installed" unless ACTION_CABLE_AVAILABLE

    @adapter = ActionCable::SubscriptionAdapter::Orbitcast.new(build_server)
    @publisher = FakePublisher.new
    @adapter.instance_variable_set(:@publisher, @publisher)
  end

  def test_broadcast_delegates_the_encoded_payload_unchanged
    @adapter.broadcast("chat:42", '{"message":"hi"}')

    assert_equal [["chat:42", '{"message":"hi"}']], @publisher.published
  end

  def test_subscribe_explains_that_the_ruby_endpoint_is_disabled
    error = assert_raises(NotImplementedError) { @adapter.subscribe("chat:42", ->(_) {}) }

    assert_includes error.message, "chat:42"
    assert_includes error.message, "mount_path is nil"
  end

  def test_unsubscribe_explains_that_the_ruby_endpoint_is_disabled
    error = assert_raises(NotImplementedError) { @adapter.unsubscribe("chat:42", ->(_) {}) }

    assert_includes error.message, "does not unsubscribe"
  end

  def test_shutdown_releases_the_publisher
    @adapter.shutdown

    assert @publisher.shutdown?
    assert_nil @adapter.instance_variable_get(:@publisher)
  end

  def test_cable_options_drop_action_cable_own_keys
    options = @adapter.send(:cable_options)

    refute options.key?(:adapter)
    refute options.key?(:id)
    assert_equal Orbitcast::TestHelper::NAMESPACE, options[:namespace]
  end

  def test_publish_via_http_builds_the_http_publisher
    cable = {
      adapter: "orbitcast",
      namespace: Orbitcast::TestHelper::NAMESPACE,
      publish_via: "http",
      ingress_url: "http://127.0.0.1:28081/v1/publications",
      ingress_token: "ingress-token"
    }
    adapter = ActionCable::SubscriptionAdapter::Orbitcast.new(FakeServer.new(FakeConfig.new(cable), nil))

    with_global_config(namespace: Orbitcast::TestHelper::NAMESPACE) do
      publisher = adapter.publisher

      assert_kind_of Orbitcast::Publisher::Http, publisher
      assert_equal :http, publisher.configuration.publish_via

      # The shutdown contract is the same for both publishers.
      adapter.shutdown

      assert_nil adapter.instance_variable_get(:@publisher)
      assert_raises(Orbitcast::Error) { publisher.publish("chat:42", "{}") }
    end
  end

  def test_publish_via_postgres_builds_the_postgres_publisher
    with_global_config(namespace: Orbitcast::TestHelper::NAMESPACE) do
      adapter = ActionCable::SubscriptionAdapter::Orbitcast.new(build_server)

      assert_kind_of Orbitcast::Publisher::Postgres, adapter.publisher
    end
  end

  # Rails main's SubscriptionAdapter::Base keeps no `server` reader.
  def test_cable_options_do_not_need_the_server_after_construction
    @adapter.define_singleton_method(:server) { raise NoMethodError, "no server on Rails main" }

    assert_equal Orbitcast::TestHelper::NAMESPACE, @adapter.send(:cable_options)[:namespace]
  end

  def test_identifier_still_follows_the_base_contract
    assert_equal "ActionCable-PID-#{Process.pid}", @adapter.identifier
  end

  private

  def build_server
    cable = {
      adapter: "orbitcast",
      id: "ActionCable-PID-#{Process.pid}",
      namespace: Orbitcast::TestHelper::NAMESPACE,
      database_url: Orbitcast::TestHelper::DATABASE_URL,
      publish_via: "postgres"
    }

    FakeServer.new(FakeConfig.new(cable), nil)
  end
end
