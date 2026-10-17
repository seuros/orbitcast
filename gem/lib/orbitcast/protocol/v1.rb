# frozen_string_literal: true

require "json"

require_relative "../error"

module Orbitcast
  module Protocol
    # Version 1 envelopes: limits, tagged values, and the shared semantic
    # checks.
    #
    # Every constant here mirrors one in `src/protocol/v1/`. Rust is the strict
    # validator of what crosses the wire; Ruby validates its own output before
    # emitting it, so an envelope Rust would reject never leaves this process.
    #
    # Tolerant reading, exactly as documented:
    #
    #   | wire condition        | result                |
    #   | --------------------- | --------------------- |
    #   | unknown field         | ignored               |
    #   | absent `version`      | reads as 1            |
    #   | unknown tagged value  | hard error            |
    #   | absent `state`/`null` | unchanged             |
    #   | `state: {}`           | clear                 |
    module V1
      VERSION = 1

      # --- operations (also the last path segment of the HTTP binding) -------

      OPERATION_DESCRIBE = "describe"
      OPERATION_IDENTIFY = "identify"
      OPERATION_OPEN_CHANNEL = "open_channel"
      OPERATION_CLOSE_CHANNEL = "close_channel"
      OPERATION_RUN_ACTION = "run_action"
      OPERATION_RELEASE_CONNECTION = "release_connection"

      OPERATIONS = [
        OPERATION_DESCRIBE,
        OPERATION_IDENTIFY,
        OPERATION_OPEN_CHANNEL,
        OPERATION_CLOSE_CHANNEL,
        OPERATION_RUN_ACTION,
        OPERATION_RELEASE_CONNECTION
      ].freeze

      # `describe` is a node-level handshake: no session, no revision.
      STATEFUL_OPERATIONS = (OPERATIONS - [OPERATION_DESCRIBE]).freeze

      # Operations that run *after* the thing they describe is already gone.
      # Their effects would address a subscription the relay has tombstoned.
      TEARDOWN_OPERATIONS = [OPERATION_CLOSE_CHANNEL, OPERATION_RELEASE_CONNECTION].freeze

      # --- outcomes ---------------------------------------------------------

      OUTCOME_ACCEPTED = "accepted"
      OUTCOME_DENIED = "denied"
      OUTCOME_FAULT = "fault"
      OUTCOMES = [OUTCOME_ACCEPTED, OUTCOME_DENIED, OUTCOME_FAULT].freeze

      # --- effects ----------------------------------------------------------

      EFFECT_ADD_STREAM = "add_stream"
      EFFECT_REMOVE_STREAM = "remove_stream"
      EFFECT_REMOVE_ALL_STREAMS = "remove_all_streams"
      EFFECT_TRANSMIT = "transmit"
      EFFECT_CLOSE = "close"
      EFFECT_PRESENCE_GRANT = "presence_grant"
      EFFECT_PRESENCE_REVOKE = "presence_revoke"

      EFFECT_KINDS = [
        EFFECT_ADD_STREAM, EFFECT_REMOVE_STREAM, EFFECT_REMOVE_ALL_STREAMS,
        EFFECT_TRANSMIT, EFFECT_CLOSE, EFFECT_PRESENCE_GRANT, EFFECT_PRESENCE_REVOKE
      ].freeze

      # --- presence (src/protocol/v1/presence.rs) ---------------------------
      #
      # Presence needs no transport extension: a client operation rides the
      # `data` of an ordinary `message`/`perform` command under the reserved
      # action below, and a server answer rides an ordinary addressed message.
      #
      # `PRESENCE_OPERATIONS` is one spelling used in two places: the
      # `operations` allowlist of a `presence_grant` effect, and the `op` a
      # client may ask for. The server-to-client `event` values are the same
      # three words.

      PRESENCE_ACTION = "orbitcast_presence"
      PRESENCE_MESSAGE_TYPE = "presence"

      PRESENCE_OP_JOIN = "join"
      PRESENCE_OP_LEAVE = "leave"
      PRESENCE_OP_SNAPSHOT = "snapshot"

      PRESENCE_OPERATIONS = [
        PRESENCE_OP_JOIN, PRESENCE_OP_LEAVE, PRESENCE_OP_SNAPSHOT
      ].freeze

      # Snapshot page window, as the server clamps it.
      DEFAULT_PRESENCE_PAGE_SIZE = 50
      MAX_PRESENCE_PAGE_SIZE = 100
      # Deep paging over a roster is a scan; the ceiling keeps one client from
      # walking an unbounded OFFSET across the table.
      MAX_PRESENCE_PAGE_OFFSET = 100_000

      # --- close reasons ----------------------------------------------------
      #
      # Rails does not get to invent wire-visible close reasons: the browser
      # facing `disconnect` frame is framed by Rust and the ActionCable client
      # only understands these four.

      CLOSE_REASON_UNAUTHORIZED = "unauthorized"
      CLOSE_REASON_INVALID_REQUEST = "invalid_request"
      CLOSE_REASON_SERVER_RESTART = "server_restart"
      CLOSE_REASON_REMOTE = "remote"

      CLOSE_REASONS = [
        CLOSE_REASON_UNAUTHORIZED, CLOSE_REASON_INVALID_REQUEST,
        CLOSE_REASON_SERVER_RESTART, CLOSE_REASON_REMOTE
      ].freeze

      # --- control ----------------------------------------------------------

      CONTROL_KIND_DISCONNECT = "disconnect"
      CONTROL_KINDS = [CONTROL_KIND_DISCONNECT].freeze

      # --- limits (src/protocol/v1/rpc.rs) ----------------------------------

      MAX_EFFECTS = 64
      MAX_IDENTIFIER_LEN = 4096
      MAX_NODE_ID_LEN = 128
      MAX_NAMESPACE_LEN = 128
      MAX_STREAM_LEN = 1024
      MAX_URL_LEN = 8192
      MAX_ADDRESS_LEN = 256
      MAX_HEADERS = 32
      MAX_HEADER_NAME_LEN = 128
      MAX_HEADER_VALUE_LEN = 8192
      MAX_STATE_ENTRIES = 64
      MAX_STATE_KEY_LEN = 128
      MAX_STATE_LEN = 64 * 1024
      MAX_PAYLOAD_LEN = 1024 * 1024
      MAX_TRANSMIT_LEN = MAX_PAYLOAD_LEN
      MAX_SUBSCRIPTION_STREAMS = 64
      MAX_RELEASE_SUBSCRIPTIONS = 256
      MAX_REASON_LEN = 256
      MAX_SUBJECT_ID_LEN = 256
      MAX_PRESENCE_METADATA_LEN = 4096
      MAX_PRESENCE_OPERATIONS = 16
      MAX_DEADLINE_MS = 60_000
      MAX_CAPABILITIES = 64
      MAX_LABEL_LEN = 128
      MAX_IDENTITY_KEY_LEN = 512

      # Headers that are never forwarded, whatever the allowlist says: they
      # describe the relay's own hop and reconstructing them here would be a
      # request-smuggling hazard.
      HOP_BY_HOP_HEADERS = %w[
        connection content-length host keep-alive proxy-authenticate
        proxy-authorization te trailer transfer-encoding upgrade
      ].freeze

      UUID_PATTERN = /\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/

      class << self
        # An absent version reads as 1; serializers always emit it.
        def version_of(hash)
          value = hash["version"]
          return VERSION if value.nil?

          value
        end

        def validate_version!(version, field: "version")
          return VERSION if version == VERSION

          raise ProtocolError.new("unsupported envelope version #{version.inspect}", field: field)
        end

        def validate_bounded!(field, value, max)
          unless value.is_a?(String)
            raise ProtocolError.new("#{field} must be a String, got #{value.class}", field: field)
          end
          raise ProtocolError.new("#{field} must not be empty", field: field) if value.empty?

          if value.bytesize > max
            raise ProtocolError.new(
              "#{field} is #{value.bytesize} bytes; the maximum is #{max}", field: field
            )
          end

          value
        end

        def validate_namespace!(namespace)
          validate_bounded!("namespace", namespace, MAX_NAMESPACE_LEN)
        end

        def validate_stream!(stream)
          validate_bounded!("stream", stream, MAX_STREAM_LEN)
        end

        def validate_identifier!(identifier)
          validate_bounded!("identifier", identifier, MAX_IDENTIFIER_LEN)
        end

        def validate_uuid!(field, value)
          validate_bounded!(field, value, 64)
          return value if value.match?(UUID_PATTERN)

          raise ProtocolError.new("#{field} #{value.inspect} is not a UUID", field: field)
        end

        def validate_close_reason!(reason)
          raise ProtocolError.new("reason must not be empty", field: "reason") if reason.to_s.empty?
          return reason if CLOSE_REASONS.include?(reason)

          raise ProtocolError.new(
            "reason #{reason.inspect} is not one of #{CLOSE_REASONS.join(", ")}", field: "reason"
          )
        end

        # Bounds the entry count, each key, and the *encoded* size: one huge
        # value is as expensive as many small ones.
        def validate_state_map!(field, map)
          unless map.is_a?(Hash)
            raise ProtocolError.new("#{field} must be an object, got #{map.class}", field: field)
          end

          if map.size > MAX_STATE_ENTRIES
            raise ProtocolError.new(
              "#{field} has #{map.size} entries; the maximum is #{MAX_STATE_ENTRIES}", field: field
            )
          end

          map.each_key do |key|
            unless key.is_a?(String)
              raise ProtocolError.new("#{field} keys must be Strings, got #{key.class}", field: field)
            end

            validate_bounded!("state_key", key, MAX_STATE_KEY_LEN)
          end

          encoded = JSON.generate(map)
          if encoded.bytesize > MAX_STATE_LEN
            raise ProtocolError.new(
              "#{field} encodes to #{encoded.bytesize} bytes; the maximum is #{MAX_STATE_LEN}",
              field: field
            )
          end

          map
        end

        # Check that an embedded application payload is valid JSON *without*
        # keeping the decoded value: the exact encoded text is what travels.
        def validate_json_text!(field, text)
          unless text.is_a?(String)
            raise ProtocolError.new("#{field} must be a String, got #{text.class}", field: field)
          end
          raise ProtocolError.new("#{field} must not be empty", field: field) if text.empty?

          if text.bytesize > MAX_PAYLOAD_LEN
            raise ProtocolError.new(
              "#{field} is #{text.bytesize} bytes; the maximum is #{MAX_PAYLOAD_LEN}", field: field
            )
          end

          begin
            JSON.parse(text)
          rescue JSON::ParserError => e
            raise ProtocolError.new("#{field} is not valid JSON: #{e.message}", field: field)
          end

          text
        end

        def stateful?(operation)
          STATEFUL_OPERATIONS.include?(operation)
        end

        def teardown?(operation)
          TEARDOWN_OPERATIONS.include?(operation)
        end
      end
    end
  end
end

require_relative "v1/effects"
require_relative "v1/request"
require_relative "v1/reply"
require_relative "v1/control_message"
