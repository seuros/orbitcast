# frozen_string_literal: true

require "action_cable/subscription_adapter/base"
require "orbitcast"

module ActionCable
  module SubscriptionAdapter
    # Publish-only ActionCable adapter.
    #
    #   # config/cable.yml
    #   production:
    #     adapter: orbitcast
    #     database_url: <%= ENV.fetch("ORBITCAST_DATABASE_URL") %>
    #     namespace: "example_production"
    #     publish_via: postgres
    #
    # `ActionCable.server.broadcast` and the Turbo broadcast helpers go through
    # `broadcast`, to `Orbitcast::Publisher::Postgres` (`publish_via: postgres`,
    # the default) or `Orbitcast::Publisher::Http` (`publish_via: http`).
    # `shutdown` releases whichever publisher was built, fork-safely.
    # Subscribing is the Rust relay's job: in signed mode the Ruby
    # WebSocket endpoint is disabled (`config.action_cable.mount_path = nil`), so
    # a local `subscribe` call is a configuration bug, not something to satisfy
    # by starting a second listener.
    #
    # Note the `::` prefixes below: inside this class, a bare `Orbitcast`
    # resolves to the adapter itself, not to the gem's top-level module.
    class Orbitcast < Base
      def initialize(server)
        super
        # Rails main's Base keeps no `server` reader; read the config now.
        @cable = server.config.cable
        @publisher = nil
        @mutex = Mutex.new
      end

      # @param channel [String] logical stream name
      # @param payload [String] JSON text ActionCable already encoded; it is
      #   embedded verbatim rather than encoded a second time
      def broadcast(channel, payload)
        publisher.publish(channel, payload)
      end

      def subscribe(channel, message_callback, success_callback = nil)
        raise NotImplementedError, unsupported_message("subscribe", channel)
      end

      def unsubscribe(channel, message_callback)
        raise NotImplementedError, unsupported_message("unsubscribe", channel)
      end

      def shutdown
        publisher = @mutex.synchronize do
          current = @publisher
          @publisher = nil
          current
        end

        publisher&.shutdown
        nil
      end

      # The publisher, built from `cable.yml` merged over `Orbitcast.config`.
      def publisher
        @publisher || @mutex.synchronize { @publisher ||= build_publisher }
      end

      private

      def build_publisher
        config = ::Orbitcast.config
        config.assign(cable_options)

        case config.publish_via
        when :postgres
          ::Orbitcast::Publisher::Postgres.new(config)
        when :http
          # Loaded here, not at require time: a PostgreSQL deployment never
          # needs net/http in the adapter's path.
          require "orbitcast/publisher/http"
          ::Orbitcast::Publisher::Http.new(config)
        else
          # Configuration#publish_via= already rejects anything else; this is the
          # fail-closed backstop if a new value is added there first.
          raise ::Orbitcast::ConfigurationError,
                "publish_via #{config.publish_via.inspect} has no publisher implementation"
        end
      end

      # `cable[:id]` is ActionCable's own connection tag and `adapter` selects
      # us; neither is an Orbitcast setting.
      def cable_options
        @cable.to_h.transform_keys(&:to_sym).except(:adapter, :id)
      end

      def unsupported_message(operation, channel)
        "Orbitcast does not #{operation} in this process (stream #{channel.inspect}). " \
          "The Rust relay owns WebSocket connections and PostgreSQL LISTEN; the Rails cable " \
          "endpoint is disabled in signed mode. Check that config.action_cable.mount_path is nil " \
          "and that nothing mounts ActionCable::Server in this application."
      end
    end
  end
end
