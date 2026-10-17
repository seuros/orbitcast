# frozen_string_literal: true

require_relative "postgres"
require_relative "../protocol"

module Orbitcast
  module Publisher
    # Publishes Rails-initiated control messages over the same PostgreSQL
    # transport as ordinary publications.
    #
    # A control message is not a publication: it never reaches a browser, it
    # carries no application payload, and every node consumes it rather than
    # forwarding it. It does share the transport, so this subclasses the
    # publisher to reuse its connection pool, fork safety, and channel-name
    # derivation instead of opening a second pool for one NOTIFY.
    #
    # ## Channel derivation
    #
    # `orbitcast_` + the first 48 hex characters of SHA-256 over the
    # length-prefixed `(namespace, CONTROL_STREAM)` tuple — the same function
    # every other channel name comes from, with a reserved stream name. A
    # control message for one namespace is therefore invisible to another
    # deployment's listener, and the reserved name cannot collide with an
    # application stream unless the application deliberately publishes to
    # `__orbitcast_control__`.
    #
    # Relay side: the listener session holds a permanent `LISTEN` on this
    # channel (`Backend::spawn_listener` in `src/server.rs`), and the same
    # messages are also accepted over `POST /v1/control` on the private ingress
    # (`src/transport/ingress.rs`). The envelope is pinned by
    # `tests/fixtures/control/*.json` for both languages.
    class Control < Postgres
      # Reserved stream name. Never a browser-visible stream.
      CONTROL_STREAM = "__orbitcast_control__"

      def self.channel_for(namespace)
        channel_name(namespace, CONTROL_STREAM)
      end

      # @param message [Orbitcast::Protocol::V1::ControlMessage]
      # @return [String] the control UUID, for deduplication in logs
      def publish_control(message)
        message.validate!

        unless message.namespace == configuration.namespace
          raise ArgumentError,
                "control message namespace #{message.namespace.inspect} does not match the " \
                "configured namespace #{configuration.namespace.inspect}"
        end

        wire = message.to_json
        if wire.bytesize >= MAX_NOTIFY_BYTES
          raise PayloadTooLarge,
                "control message is #{wire.bytesize} bytes; PostgreSQL NOTIFY payloads must be " \
                "under #{MAX_NOTIFY_BYTES} bytes"
        end

        channel = self.class.channel_for(configuration.namespace)
        # `@pool` is the parent's bounded, fork-safe pool. Reused deliberately:
        # one process, one set of publisher connections.
        @pool.with { |connection| connection.exec_params(NOTIFY_SQL, [channel, wire]) }

        message.control_id
      end
    end
  end
end
