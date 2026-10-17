# frozen_string_literal: true

require_relative "protocol"

module Orbitcast
  # Rails-initiated remote disconnect.
  #
  #   Orbitcast.disconnect(identity_key: user.to_gid_param)
  #
  # One published control message reaches every node; each looks the identity
  # key up in its *local* session index and closes only what matches. No node
  # scans all sessions, no control data is ever visible to a browser, and
  # `control_id` deduplicates a message that arrives over more than one path.
  #
  # The key is the `"identity_key"` entry of the identity map an accepted
  # `identify` returned — see `Orbitcast::Connection.orbitcast_identity_key`. A
  # connection that declared none is anonymous to control: it is a valid
  # session that simply cannot be addressed.
  module Control
    class << self
      # @param identity_key [String] as returned by `identify`
      # @param reason [String] one of Protocol::V1::CLOSE_REASONS
      # @param reconnect [Boolean] the policy handed to the ActionCable client
      # @return [String] the control UUID
      def disconnect(identity_key:, reason: Protocol::V1::CLOSE_REASON_REMOTE, reconnect: false,
                     config: nil)
        config ||= Orbitcast.config
        message = Protocol::V1::ControlMessage.disconnect(
          namespace: config.namespace,
          identity_key: identity_key.to_s,
          reason: reason,
          reconnect: reconnect
        )

        publisher(config).publish_control(message)
      end

      # The process-wide control publisher. Separate from the ActionCable
      # adapter's publisher on purpose: control messages must still go out when
      # the adapter was never instantiated (a console, a job runner).
      def publisher(config = nil)
        config ||= Orbitcast.config
        @publisher ||= build_publisher(config)
      end

      def publisher=(publisher)
        @publisher = publisher
      end

      def shutdown
        @publisher&.shutdown
        @publisher = nil
      end

      private

      def build_publisher(config)
        require_relative "publisher/control"
        Publisher::Control.new(config)
      end
    end
  end

  class << self
    # See Orbitcast::Control.disconnect.
    def disconnect(identity_key:, reason: Protocol::V1::CLOSE_REASON_REMOTE, reconnect: false)
      Control.disconnect(
        identity_key: identity_key, reason: reason, reconnect: reconnect, config: config
      )
    end
  end
end
