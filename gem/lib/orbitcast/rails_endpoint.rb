# frozen_string_literal: true

require "active_support/security_utils"
require "json"

require_relative "error"
require_relative "protocol"

module Orbitcast
  # The private Rack endpoint the relay calls.
  #
  #   POST {mount}/{operation}
  #   Authorization: Bearer <rpc_token>
  #   Content-Type: application/json
  #
  # Mounted by `Orbitcast::Engine` only in `mode: :dispatch`, at
  # `config.orbitcast.rpc_mount_path` (default `/_orbitcast/v1/rails`).
  #
  # !!! This endpoint speaks for every user of the application. Block its path
  # !!! at the public proxy *and* keep the service credential rotating: the
  # !!! bearer check here is the second lock, not the only one.
  #
  # ## Where an error goes
  #
  # Two different failures need two different answers, and conflating them is
  # how a transport bug turns into a silent authorization change:
  #
  # * **Transport and envelope problems answer with an HTTP status.** A missing
  #   or wrong credential, an unknown operation path, a body that is not JSON, a
  #   body whose `operation` disagrees with the route, a foreign namespace, an
  #   envelope version this release cannot read. None of these can produce a
  #   *correlated* reply — the correlation fields are in the very envelope that
  #   failed to parse — so there is nothing honest to put in a 200 body. The
  #   relay treats a non-2xx as a temporary backend failure and closes affected
  #   sessions with `reconnect: true`; it never downgrades to acceptance.
  #
  # * **Application problems answer `200` with `outcome: "fault"`.** The
  #   envelope was fine, Rails simply could not run it: an exception in a
  #   callback, a pool timeout, a missing record. The reply correlates, so the
  #   relay can tie the failure to the session and command that caused it.
  #
  # A refusal is neither: `denied` is a decision and travels as a `200`.
  #
  # Every non-2xx body is JSON: `{"version":1,"error":"<code>","message":"..."}`
  # with `content-type: application/json` — the same envelope the relay's
  # ingress uses for its own errors.
  class RailsEndpoint
    # Ceiling on one request body. Matches the relay's own reply cap: neither
    # side reads an unbounded body from the other.
    MAX_REQUEST_BYTES = 2 * 1024 * 1024

    JSON_CONTENT_TYPE = "application/json"
    BEARER_PREFIX = "Bearer "

    def initialize(config: nil, dispatcher: nil, executor: nil)
      @config = config
      @dispatcher = dispatcher
      @executor = executor
    end

    def config
      @config || Orbitcast.config
    end

    def dispatcher
      @dispatcher ||= begin
        require_relative "dispatcher"
        Dispatcher.new(config: config)
      end
    end

    # Every call runs inside the Rails executor: `CurrentAttributes` are reset
    # and Active Record connection leases are released when it completes, so a
    # WebSocket's lifetime never pins a database connection. Mounted in the
    # application's routes the request is already inside the reloader, and a
    # nested wrap is a no-op; driven directly (tests, a bare Rack server) this
    # is the only thing that cleans up.
    def executor
      @executor ||= if defined?(::Rails) && ::Rails.respond_to?(:application) && ::Rails.application
                      ::Rails.application.executor
                    else
                      NullExecutor
                    end
    end

    def call(env)
      executor.wrap { handle(env) }
    end

    private

    def handle(env)
      return error(405, "method_not_allowed", "method not allowed") unless env["REQUEST_METHOD"] == "POST"

      # Authentication first: an unauthenticated caller learns nothing about
      # which operations exist or what this endpoint thinks of its body.
      return error(401, "unauthorized", "unauthorized") unless authenticated?(env)

      operation = operation_from(env)
      return error(404, "not_found", "unknown operation") if operation.nil?

      content_type = env["CONTENT_TYPE"].to_s
      unless content_type.empty? || content_type.split(";").first.to_s.strip == JSON_CONTENT_TYPE
        return error(415, "unsupported_media_type", "request body must be application/json")
      end

      body = read_body(env)
      return error(413, "payload_too_large", "request body exceeds #{MAX_REQUEST_BYTES} bytes") if body == :too_large

      decoded =
        begin
          JSON.parse(body)
        rescue JSON::ParserError
          return error(400, "malformed_json", "malformed JSON")
        end

      unless decoded.is_a?(Hash)
        return error(400, "invalid_request", "request body must be a JSON object")
      end

      # The path segment and the envelope must agree, which is what stops a
      # captured body being replayed against a different operation.
      unless decoded["operation"] == operation
        return error(400, "operation_mismatch", "operation does not match the request path")
      end

      request =
        begin
          Protocol::V1::Request.from(decoded).validate!
        rescue ProtocolError => e
          return error(400, "invalid_request", "invalid request envelope: #{e.message}")
        end

      unless request.namespace == config.namespace
        return error(403, "namespace_mismatch", "namespace mismatch")
      end

      reply = dispatcher.call(request)
      json(200, reply.to_json)
    rescue ProtocolError => e
      # The dispatcher produced a reply this process would itself reject.
      # Fail closed rather than emit it.
      log_error("Orbitcast endpoint refused to emit an invalid reply: #{e.message}")
      error(500, "invalid_reply", "invalid reply envelope")
    rescue StandardError => e
      log_error("Orbitcast endpoint failed: #{e.class}: #{e.message}")
      error(500, "internal_error", "internal error")
    end

    # The operation is the last path segment, and must be one this release
    # implements. Anything else is a 404: unknown routes do not exist here.
    def operation_from(env)
      segment = env["PATH_INFO"].to_s.split("/").last
      Protocol::V1::OPERATIONS.include?(segment) ? segment : nil
    end

    def authenticated?(env)
      token = config.rpc_token
      return false if token.nil? || token.empty?

      header = env["HTTP_AUTHORIZATION"].to_s
      return false unless header.start_with?(BEARER_PREFIX)

      ActiveSupport::SecurityUtils.secure_compare(header[BEARER_PREFIX.length..].to_s, token)
    end

    # Bounded read: one byte over the ceiling and the body is refused unread.
    def read_body(env)
      input = env["rack.input"]
      return "" if input.nil?

      length = env["CONTENT_LENGTH"].to_s
      return :too_large if !length.empty? && length.to_i > MAX_REQUEST_BYTES

      body = input.read(MAX_REQUEST_BYTES + 1).to_s
      return :too_large if body.bytesize > MAX_REQUEST_BYTES

      body
    ensure
      input.rewind if input.respond_to?(:rewind)
    end

    def json(status, body)
      [status, { "content-type" => JSON_CONTENT_TYPE, "content-length" => body.bytesize.to_s }, [body]]
    end

    # Every non-2xx answer is the shared error envelope — `version`, a stable
    # machine-readable `error` code, and a human `message` — so the relay can
    # log one shape for an ingress refusal and a Rails-endpoint refusal alike.
    def error(status, code, message)
      json(status, JSON.generate(version: Protocol::V1::VERSION, error: code, message: message))
    end

    def log_error(message)
      logger = defined?(::Rails) && ::Rails.respond_to?(:logger) ? ::Rails.logger : nil
      logger&.error(message)
    end

    # Used only when the endpoint runs outside a booted application (the
    # plain-ruby unit suite). A real deployment always has Rails' executor.
    module NullExecutor
      def self.wrap
        yield
      end
    end
  end
end
