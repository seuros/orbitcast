# frozen_string_literal: true

require_relative "../v1"

module Orbitcast
  module Protocol
    module V1
      # The ordered, request-scoped list of typed effects one operation
      # produced.
      #
      # Rails never returns raw WebSocket frames: `welcome`,
      # `confirm_subscription`, `reject_subscription`, and `disconnect` framing
      # belong to Rust. What the application *can* ask for is this fixed set,
      # and the relay applies it in the order it was collected — which is the
      # order the application's callbacks ran in.
      #
      # ## Suppression
      #
      # `close_channel` and `release_connection` run after the relay has already
      # tombstoned the subscription (a client leaving cannot be vetoed). A
      # collector built with `suppressed: true` therefore accepts every call and
      # records nothing: `stop_all_streams`, which ActionCable itself registers
      # as an `on_unsubscribe` callback, must not produce effects addressing a
      # subscription that no longer exists.
      class EffectCollector
        def initialize(suppressed: false)
          @effects = []
          @suppressed = suppressed
        end

        attr_reader :effects

        def suppressed?
          @suppressed
        end

        def add_stream(identifier, stream)
          push(
            "kind" => EFFECT_ADD_STREAM,
            "identifier" => V1.validate_identifier!(identifier),
            "stream" => V1.validate_stream!(stream)
          )
        end

        def remove_stream(identifier, stream)
          push(
            "kind" => EFFECT_REMOVE_STREAM,
            "identifier" => V1.validate_identifier!(identifier),
            "stream" => V1.validate_stream!(stream)
          )
        end

        def remove_all_streams(identifier)
          push(
            "kind" => EFFECT_REMOVE_ALL_STREAMS,
            "identifier" => V1.validate_identifier!(identifier)
          )
        end

        # @param json_payload [String] the exact Rails-encoded JSON text; it is
        #   embedded verbatim and never re-encoded on the way to the browser.
        def transmit(identifier, json_payload)
          V1.validate_identifier!(identifier)
          if json_payload.to_s.bytesize > MAX_TRANSMIT_LEN
            raise ProtocolError.new(
              "json_payload is #{json_payload.to_s.bytesize} bytes; the maximum is " \
              "#{MAX_TRANSMIT_LEN}", field: "json_payload"
            )
          end
          V1.validate_json_text!("json_payload", json_payload)

          push(
            "kind" => EFFECT_TRANSMIT,
            "identifier" => identifier,
            "json_payload" => json_payload
          )
        end

        def close(reason:, reconnect: false)
          push(
            "kind" => EFFECT_CLOSE,
            "reason" => V1.validate_close_reason!(reason),
            "reconnect" => boolean!(reconnect)
          )
        end

        # Grants an authorized presence membership.
        def presence_grant(identifier, stream:, subject_id:, ttl_ms:, metadata: {}, operations: [])
          V1.validate_identifier!(identifier)
          V1.validate_stream!(stream)
          V1.validate_bounded!("subject_id", subject_id, MAX_SUBJECT_ID_LEN)

          ttl = Integer(ttl_ms)
          unless ttl.positive?
            raise ProtocolError.new("ttl_ms must be greater than zero", field: "ttl_ms")
          end

          unless metadata.is_a?(Hash)
            raise ProtocolError.new("metadata must be an object", field: "metadata")
          end

          if JSON.generate(metadata).bytesize > MAX_PRESENCE_METADATA_LEN
            raise ProtocolError.new(
              "metadata exceeds #{MAX_PRESENCE_METADATA_LEN} bytes", field: "metadata"
            )
          end

          operations = Array(operations).map(&:to_s)
          if operations.size > MAX_PRESENCE_OPERATIONS
            raise ProtocolError.new(
              "operations has #{operations.size} entries; the maximum is " \
              "#{MAX_PRESENCE_OPERATIONS}", field: "operations"
            )
          end

          push(
            "kind" => EFFECT_PRESENCE_GRANT,
            "identifier" => identifier,
            "stream" => stream,
            "subject_id" => subject_id,
            "metadata" => metadata,
            "ttl_ms" => ttl,
            "operations" => operations
          )
        end

        def presence_revoke(identifier, stream:, subject_id:)
          push(
            "kind" => EFFECT_PRESENCE_REVOKE,
            "identifier" => V1.validate_identifier!(identifier),
            "stream" => V1.validate_stream!(stream),
            "subject_id" => V1.validate_bounded!("subject_id", subject_id, MAX_SUBJECT_ID_LEN)
          )
        end

        def empty?
          @effects.empty?
        end

        def size
          @effects.size
        end

        # The ordered effects, as plain JSON-ready hashes.
        def to_a
          @effects.dup
        end

        private

        def push(effect)
          return nil if @suppressed

          if @effects.size >= MAX_EFFECTS
            raise ProtocolError.new(
              "one reply may carry at most #{MAX_EFFECTS} effects", field: "effects"
            )
          end

          @effects << effect
          effect
        end

        def boolean!(value)
          return value if value == true || value == false

          raise ProtocolError.new("reconnect must be true or false", field: "reconnect")
        end
      end
    end
  end
end
