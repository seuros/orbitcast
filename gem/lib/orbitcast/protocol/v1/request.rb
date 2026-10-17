# frozen_string_literal: true

require_relative "../v1"

module Orbitcast
  module Protocol
    module V1
      # One request from a relay node to this Rails endpoint.
      #
      # `POST {mount}/{operation}`; the envelope's `operation` must match the
      # route, which is what stops a body being replayed against a different
      # operation.
      #
      # Parsing is tolerant in exactly the documented ways (unknown fields
      # ignored, absent fields defaulted); `validate!` is where a defaulted
      # envelope fails closed. The browser cannot submit any of this: the URL,
      # origin, client address, and allowlisted headers are reconstructed by the
      # relay from validated metadata.
      class Request
        # Everything Rails is allowed to know about the original WebSocket
        # request.
        class ConnectionSnapshot
          def self.from(hash)
            hash = {} unless hash.is_a?(Hash)
            new(
              url: hash["url"].is_a?(String) ? hash["url"] : "",
              origin: hash["origin"].is_a?(String) ? hash["origin"] : nil,
              client_address: hash["client_address"].is_a?(String) ? hash["client_address"] : "",
              headers: hash["headers"].is_a?(Hash) ? hash["headers"] : {},
              identity: hash["identity"].is_a?(Hash) ? hash["identity"] : {},
              state: hash["state"].is_a?(Hash) ? hash["state"] : {}
            )
          end

          def initialize(url:, origin:, client_address:, headers:, identity:, state:)
            @url = url
            @origin = origin
            @client_address = client_address
            @headers = headers.freeze
            @identity = identity.freeze
            @state = state.freeze
          end

          attr_reader :url, :origin, :client_address, :headers, :identity, :state

          def validate!
            V1.validate_bounded!("url", url, MAX_URL_LEN)
            V1.validate_bounded!("client_address", client_address, MAX_ADDRESS_LEN)
            V1.validate_bounded!("origin", origin, MAX_ADDRESS_LEN) unless origin.nil?

            if headers.size > MAX_HEADERS
              raise ProtocolError.new(
                "headers has #{headers.size} entries; the maximum is #{MAX_HEADERS}",
                field: "headers"
              )
            end

            headers.each do |name, value|
              V1.validate_bounded!("header_name", name, MAX_HEADER_NAME_LEN)
              if name.match?(/[[:upper:]\s]/)
                raise ProtocolError.new(
                  "header name #{name.inspect} must be lowercase and unpadded",
                  field: "header_name"
                )
              end
              if HOP_BY_HOP_HEADERS.include?(name)
                raise ProtocolError.new(
                  "header #{name.inspect} is hop-by-hop and is never forwarded",
                  field: "header_name"
                )
              end
              unless value.is_a?(String) && value.bytesize <= MAX_HEADER_VALUE_LEN
                raise ProtocolError.new("header #{name.inspect} has an unusable value",
                                        field: "header_value")
              end
            end

            V1.validate_state_map!("identity", identity)
            V1.validate_state_map!("state", state)
            self
          end

          def to_h
            {
              "url" => url,
              "origin" => origin,
              "client_address" => client_address,
              "headers" => headers,
              "identity" => identity,
              "state" => state
            }
          end
        end

        # One subscription as the relay currently holds it. `identifier` is the
        # client's original string, byte for byte; `params` is a convenience
        # decode of it and is never authoritative.
        class SubscriptionSnapshot
          def self.from(hash)
            hash = {} unless hash.is_a?(Hash)
            new(
              identifier: hash["identifier"].is_a?(String) ? hash["identifier"] : "",
              params: hash["params"].is_a?(Hash) ? hash["params"] : {},
              streams: hash["streams"].is_a?(Array) ? hash["streams"] : [],
              state: hash["state"].is_a?(Hash) ? hash["state"] : {}
            )
          end

          def initialize(identifier:, params:, streams:, state:)
            @identifier = identifier
            @params = params.freeze
            @streams = streams.freeze
            @state = state.freeze
          end

          attr_reader :identifier, :params, :streams, :state

          def validate!
            V1.validate_identifier!(identifier)

            if streams.size > MAX_SUBSCRIPTION_STREAMS
              raise ProtocolError.new(
                "streams has #{streams.size} entries; the maximum is " \
                "#{MAX_SUBSCRIPTION_STREAMS}", field: "streams"
              )
            end
            streams.each { |stream| V1.validate_stream!(stream) }

            V1.validate_state_map!("params", params)
            V1.validate_state_map!("state", state)
            self
          end

          def to_h
            {
              "identifier" => identifier,
              "params" => params,
              "streams" => streams,
              "state" => state
            }
          end
        end

        # The `message`/`perform` payload of a `run_action` request.
        class ActionPayload
          def self.from(hash)
            hash = {} unless hash.is_a?(Hash)
            new(
              identifier: hash["identifier"].is_a?(String) ? hash["identifier"] : "",
              json_data: hash["json_data"].is_a?(String) ? hash["json_data"] : ""
            )
          end

          def initialize(identifier:, json_data:)
            @identifier = identifier
            @json_data = json_data
          end

          attr_reader :identifier, :json_data

          def validate!
            V1.validate_identifier!(identifier)
            V1.validate_json_text!("json_data", json_data)
            self
          end

          # The decoded command data. Rails reads `data["action"]` out of the
          # bytes the client actually sent.
          def data
            JSON.parse(json_data)
          end

          def to_h
            { "identifier" => identifier, "json_data" => json_data }
          end
        end

        # The final snapshot handed to `release_connection`.
        class ReleasePayload
          def self.from(hash)
            hash = {} unless hash.is_a?(Hash)
            subscriptions = hash["subscriptions"].is_a?(Array) ? hash["subscriptions"] : []
            new(
              reason: hash["reason"].is_a?(String) ? hash["reason"] : "",
              reconnect: hash["reconnect"] == true,
              subscriptions: subscriptions.map { |entry| SubscriptionSnapshot.from(entry) }
            )
          end

          def initialize(reason:, reconnect:, subscriptions:)
            @reason = reason
            @reconnect = reconnect
            @subscriptions = subscriptions.freeze
          end

          attr_reader :reason, :reconnect, :subscriptions

          def reconnect?
            @reconnect
          end

          def validate!
            V1.validate_close_reason!(reason)

            if subscriptions.size > MAX_RELEASE_SUBSCRIPTIONS
              raise ProtocolError.new(
                "subscriptions has #{subscriptions.size} entries; the maximum is " \
                "#{MAX_RELEASE_SUBSCRIPTIONS}", field: "subscriptions"
              )
            end
            subscriptions.each(&:validate!)
            self
          end

          def to_h
            {
              "reason" => reason,
              "reconnect" => reconnect,
              "subscriptions" => subscriptions.map(&:to_h)
            }
          end
        end

        # Decode one request body. Raises ProtocolError when the body is not an
        # object or names an operation this release does not implement: an
        # unknown tagged value is a hard error, never a tolerated unknown.
        def self.from(hash)
          unless hash.is_a?(Hash)
            raise ProtocolError.new("request body must be a JSON object", field: "body")
          end

          operation = hash["operation"]
          unless OPERATIONS.include?(operation)
            raise ProtocolError.new("unknown operation #{operation.inspect}", field: "operation")
          end

          new(
            version: V1.version_of(hash),
            operation: operation,
            namespace: string_or_empty(hash["namespace"]),
            node_id: string_or_empty(hash["node_id"]),
            session_id: string_or_empty(hash["session_id"]),
            request_id: string_or_empty(hash["request_id"]),
            sequence: integer_or_zero(hash["sequence"]),
            expected_revision: integer_or_zero(hash["expected_revision"]),
            deadline_ms: integer_or_zero(hash["deadline_ms"]),
            connection: hash.key?("connection") ? ConnectionSnapshot.from(hash["connection"]) : nil,
            subscription: hash.key?("subscription") ? SubscriptionSnapshot.from(hash["subscription"]) : nil,
            action: hash.key?("action") ? ActionPayload.from(hash["action"]) : nil,
            release: hash.key?("release") ? ReleasePayload.from(hash["release"]) : nil
          )
        end

        def self.parse(text)
          from(JSON.parse(text))
        end

        def self.string_or_empty(value)
          value.is_a?(String) ? value : ""
        end

        def self.integer_or_zero(value)
          value.is_a?(Integer) ? value : 0
        end
        private_class_method :string_or_empty, :integer_or_zero

        def initialize(version:, operation:, namespace:, node_id:, session_id:, request_id:,
                       sequence:, expected_revision:, deadline_ms:,
                       connection: nil, subscription: nil, action: nil, release: nil)
          @version = version
          @operation = operation
          @namespace = namespace
          @node_id = node_id
          @session_id = session_id
          @request_id = request_id
          @sequence = sequence
          @expected_revision = expected_revision
          @deadline_ms = deadline_ms
          @connection = connection
          @subscription = subscription
          @action = action
          @release = release
        end

        attr_reader :version, :operation, :namespace, :node_id, :session_id, :request_id,
                    :sequence, :expected_revision, :deadline_ms,
                    :connection, :subscription, :action, :release

        def stateful?
          V1.stateful?(operation)
        end

        def teardown?
          V1.teardown?(operation)
        end

        # Semantic validation. Parsing defaults never bypass this.
        def validate!
          V1.validate_version!(version)
          V1.validate_namespace!(namespace)
          V1.validate_bounded!("node_id", node_id, MAX_NODE_ID_LEN)
          V1.validate_uuid!("session_id", session_id)
          V1.validate_uuid!("request_id", request_id)

          if deadline_ms <= 0 || deadline_ms > MAX_DEADLINE_MS
            raise ProtocolError.new(
              "deadline_ms must be 1..#{MAX_DEADLINE_MS}, got #{deadline_ms}", field: "deadline_ms"
            )
          end

          require_payload!(connection, "connection") unless operation == OPERATION_DESCRIBE
          case operation
          when OPERATION_OPEN_CHANNEL, OPERATION_CLOSE_CHANNEL
            require_payload!(subscription, "subscription")
          when OPERATION_RUN_ACTION
            require_payload!(subscription, "subscription")
            require_payload!(action, "action")
          when OPERATION_RELEASE_CONNECTION
            require_payload!(release, "release")
          end

          # A payload that is present but not required is still validated: a
          # tolerated field is never an unvalidated field.
          connection&.validate!
          subscription&.validate!
          action&.validate!
          release&.validate!
          self
        end

        def to_h
          hash = {
            "version" => version,
            "operation" => operation,
            "namespace" => namespace,
            "node_id" => node_id,
            "session_id" => session_id,
            "request_id" => request_id,
            "sequence" => sequence,
            "expected_revision" => expected_revision,
            "deadline_ms" => deadline_ms
          }
          hash["connection"] = connection.to_h if connection
          hash["subscription"] = subscription.to_h if subscription
          hash["action"] = action.to_h if action
          hash["release"] = release.to_h if release
          hash
        end

        def to_json(*args)
          JSON.generate(to_h, *args)
        end

        private

        def require_payload!(payload, field)
          return if payload

          raise ProtocolError.new("#{operation} requires a #{field} payload", field: field)
        end
      end
    end
  end
end
