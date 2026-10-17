# frozen_string_literal: true

require "securerandom"

require_relative "../v1"

module Orbitcast
  module Protocol
    module V1
      # One Rails-initiated control message.
      #
      # This is how Rails reaches sessions it does not hold: a multi-device
      # sign-out publishes one disconnect message, every node looks the identity
      # key up in its local index, and only the matching sessions are closed. No
      # node scans every session, and no control data is ever visible to a
      # browser.
      #
      # `identity_key` is the value the application put under the
      # `"identity_key"` key of the accepted `identify` identity map. A
      # connection that returned no `identity_key` is anonymous as far as
      # control is concerned: it is a valid session, it is simply not
      # addressable.
      class ControlMessage
        def self.disconnect(namespace:, identity_key:, reason: CLOSE_REASON_REMOTE,
                            reconnect: false, control_id: nil)
          new(
            kind: CONTROL_KIND_DISCONNECT,
            namespace: namespace,
            control_id: control_id || SecureRandom.uuid,
            identity_key: identity_key,
            reason: reason,
            reconnect: reconnect
          )
        end

        # Tolerant decode, for tests and for a Ruby-side ingress consumer. An
        # unknown `kind` is a hard error: a node that cannot understand a
        # control message must not apply a guessed-at fraction of it.
        def self.from(hash)
          unless hash.is_a?(Hash)
            raise ProtocolError.new("control message must be a JSON object", field: "body")
          end

          kind = hash["kind"]
          unless CONTROL_KINDS.include?(kind)
            raise ProtocolError.new("unknown control kind #{kind.inspect}", field: "kind")
          end

          new(
            version: V1.version_of(hash),
            kind: kind,
            namespace: hash["namespace"].is_a?(String) ? hash["namespace"] : "",
            control_id: hash["control_id"].is_a?(String) ? hash["control_id"] : "",
            identity_key: hash["identity_key"].is_a?(String) ? hash["identity_key"] : "",
            reason: hash["reason"].is_a?(String) ? hash["reason"] : "",
            # An absent reconnect policy defaults to false: a control message
            # that fails to say otherwise ends the session rather than inviting
            # an immediate reconnect with credentials just revoked.
            reconnect: reconnect_value(hash)
          )
        end

        def self.parse(text)
          from(JSON.parse(text))
        end

        def self.reconnect_value(hash)
          return false unless hash.key?("reconnect")

          value = hash["reconnect"]
          return value if value == true || value == false

          raise ProtocolError.new("reconnect must be true or false", field: "reconnect")
        end
        private_class_method :reconnect_value

        def initialize(kind:, namespace:, control_id:, identity_key:, reason:, reconnect:,
                       version: VERSION)
          @version = version
          @kind = kind
          @namespace = namespace
          @control_id = control_id
          @identity_key = identity_key
          @reason = reason
          @reconnect = reconnect
        end

        attr_reader :version, :kind, :namespace, :control_id, :identity_key, :reason

        def reconnect?
          @reconnect
        end

        def validate!
          V1.validate_version!(version)
          V1.validate_namespace!(namespace)
          V1.validate_uuid!("control_id", control_id)
          V1.validate_bounded!("identity_key", identity_key, MAX_IDENTITY_KEY_LEN)
          V1.validate_close_reason!(reason)
          unless @reconnect == true || @reconnect == false
            raise ProtocolError.new("reconnect must be true or false", field: "reconnect")
          end

          self
        end

        # Validation runs first: a message is never applied because its
        # namespace happens to match.
        def applies_to?(other_namespace)
          validate!
          namespace == other_namespace
        rescue ProtocolError
          false
        end

        def to_h
          {
            "version" => version,
            "kind" => kind,
            "namespace" => namespace,
            "control_id" => control_id,
            "identity_key" => identity_key,
            "reason" => reason,
            "reconnect" => @reconnect
          }
        end

        def to_json(*args)
          JSON.generate(to_h, *args)
        end
      end
    end
  end
end
