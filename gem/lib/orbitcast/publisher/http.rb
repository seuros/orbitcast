# frozen_string_literal: true

require "json"
require "net/http"
require "securerandom"
require "uri"

require_relative "postgres"
require_relative "instrumentation"
require_relative "../error"
require_relative "../protocol"
require_relative "../streams"

module Orbitcast
  module Publisher
    # Private HTTP publication ingress: the alternative publication path to
    # PostgreSQL.
    #
    #   config.orbitcast.publish_via  = :http
    #   config.orbitcast.ingress_url  = "http://127.0.0.1:28081/v1/publications"
    #   config.orbitcast.ingress_token = Rails.application.credentials.dig(:orbitcast, :ingress_token)
    #
    # Chosen when the database credentials must stay on the Rust side, or in a
    # memory-only build where there is no PostgreSQL to publish through. Rails
    # publishes through *one* path: never both this and PostgreSQL for the same
    # broadcast.
    #
    # ## Wire contract
    #
    #   POST {ingress_url}
    #   Authorization: Bearer <ingress_token>
    #   Content-Type: application/json
    #
    #   {"version":1,"namespace":"...","id":"<uuid>","stream":"...",
    #    "json_payload":"<exact ActionCable-encoded JSON text>"}
    #
    # which is the v1 `Publication` envelope from `src/protocol/v1/publication.rs`,
    # byte for byte. A `202` means the backend accepted and committed the
    # publication, not that a browser saw it.
    #
    # Relay side: `POST /v1/publications` on the private ingress listener
    # (`src/transport/ingress.rs`), which answers `202` with the `IngressAck`
    # envelope from `src/protocol/v1/publication.rs`. The ack is pinned by
    # `tests/fixtures/ingress/ack.json` for both languages; this client treats
    # the status code as the contract and does not require the body.
    #
    # ## Failure policy
    #
    # No retries. A publication that timed out may or may not have been
    # accepted, and replaying it would duplicate a browser-visible message;
    # durable cross-node deduplication belongs to the broker phase.
    class Http
      DEFAULT_OPEN_TIMEOUT = 2.0
      DEFAULT_READ_TIMEOUT = 5.0

      ACCEPTED_STATUSES = %w[200 201 202].freeze

      # @param instrumenter [#instrument] defaults to
      #   `ActiveSupport::Notifications` when loaded; see
      #   `Orbitcast::Publisher::Instrumentation`
      def initialize(configuration = Orbitcast.config, connection_factory: nil, instrumenter: nil)
        @configuration = configuration.validate!
        @instrumenter = Instrumentation.resolve(instrumenter)
        @uri = URI.parse(configuration.ingress_url.to_s)
        @pool = Postgres::Pool.new(
          size: configuration.pool_size,
          timeout: configuration.pool_timeout,
          &(connection_factory || method(:connect))
        )
      end

      attr_reader :configuration

      # @param stream [String] logical stream name
      # @param payload [String] JSON text already encoded by ActionCable
      # @return [String] publication UUID
      def publish(stream, payload)
        # Both guards run before anything is instrumented or sent: an invalid
        # stream (empty, over 1024 UTF-8 bytes) or a non-String payload is a
        # caller error, not a publication that failed.
        stream = Streams.validated_stream(stream)
        payload = validated_payload(payload)

        Instrumentation.publish(@instrumenter, stream: stream, bytes: payload.bytesize, via: :http) do
          if payload.bytesize > configuration.max_publication_size
            raise PayloadTooLarge,
                  "publication payload is #{payload.bytesize} bytes; the configured maximum is " \
                  "#{configuration.max_publication_size} bytes (max_publication_size)"
          end

          publication_id = SecureRandom.uuid
          body = JSON.generate(
            version: Protocol::V1::VERSION,
            namespace: configuration.namespace,
            id: publication_id,
            stream: stream,
            json_payload: payload
          )

          # `Pool#with` returns the checkin, not the block: capture the response.
          response = nil
          @pool.with { |connection| response = connection.post(@uri, body, bearer) }
          verify!(response)

          publication_id
        end
      end

      def shutdown
        @pool.shutdown
      end

      private

      def bearer
        token = configuration.ingress_token
        raise ConfigurationError, "ingress_token is required when publish_via is :http" if token.nil?

        "Bearer #{token}"
      end

      def verify!(response)
        return if ACCEPTED_STATUSES.include?(response.code.to_s)

        case response.code.to_s
        when "401", "403"
          raise IngressError, "publication ingress refused the service credential (#{response.code})"
        when "413"
          raise PayloadTooLarge, "publication ingress refused the payload as too large (413)"
        else
          raise IngressError, "publication ingress returned HTTP #{response.code}"
        end
      end

      def validated_payload(payload)
        unless payload.is_a?(String)
          raise ArgumentError,
                "payload must be the JSON text encoded by ActionCable, got #{payload.class}"
        end
        raise ArgumentError, "payload is not valid #{payload.encoding}" unless payload.valid_encoding?

        payload.encoding == Encoding::UTF_8 ? payload : payload.encode(Encoding::UTF_8)
      end

      def connect
        Connection.new(@uri, configuration)
      end

      # One keep-alive HTTP connection, shaped like the pooled objects the
      # PostgreSQL publisher hands out (`#close`), so both publishers share the
      # same bounded, fork-safe pool implementation.
      class Connection
        def initialize(uri, _configuration)
          @http = Net::HTTP.new(uri.host, uri.port)
          @http.use_ssl = uri.scheme == "https"
          @http.open_timeout = DEFAULT_OPEN_TIMEOUT
          @http.read_timeout = DEFAULT_READ_TIMEOUT
          @http.keep_alive_timeout = 30
        end

        def post(uri, body, authorization)
          request = Net::HTTP::Post.new(uri.request_uri)
          request["content-type"] = "application/json"
          request["accept"] = "application/json"
          request["authorization"] = authorization
          request.body = body

          @http.start unless @http.started?
          @http.request(request)
        end

        def close
          @http.finish if @http.started?
        rescue IOError
          nil
        end
      end
    end
  end
end
