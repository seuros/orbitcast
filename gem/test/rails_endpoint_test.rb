# frozen_string_literal: true

require "test_helper"
require "json"
require "stringio"

begin
  require "orbitcast/rails_endpoint"
  RAILS_ENDPOINT_AVAILABLE = true
rescue LoadError
  # Needs ActiveSupport::SecurityUtils only; no Rails boot, no ActionCable.
  RAILS_ENDPOINT_AVAILABLE = false
end

# The private Rack endpoint: routing, service authentication, envelope
# validation, and the HTTP-versus-fault split.
#
# The dispatcher is stubbed here on purpose — this file is about the boundary.
# The real connection/channel dispatch runs against a real Rails application in
# rails_contract_test.rb.
if RAILS_ENDPOINT_AVAILABLE
  class RailsEndpointTest < Minitest::Test
    V1 = Orbitcast::Protocol::V1

    MOUNT = "/_orbitcast/v1/rails"
    TOKEN = "rails-service-token"
    SESSION = "2f4a6c8e-1b3d-4f5a-9c7e-0d1a2b3c4d5e"
    REQUEST_ID = "9a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d"
    IDENTIFIER = '{"channel":"ChatChannel","room":"42"}'

    # MOCK: stands in for Orbitcast::Dispatcher. Records what it was handed and
    # answers with whatever the test asked for.
    class FakeDispatcher
      attr_reader :calls

      def initialize(&answer)
        @calls = []
        @answer = answer
      end

      def call(request)
        @calls << request
        return @answer.call(request) if @answer
        return V1::Reply.describe(request) if request.operation == V1::OPERATION_DESCRIBE

        V1::Reply.accepted(request)
      end
    end

    # MOCK: an executor that records that it wrapped the call, the way
    # Rails.application.executor does in a booted application.
    class RecordingExecutor
      attr_reader :wraps

      def initialize
        @wraps = 0
        @active = false
      end

      def active? = @active

      def wrap
        @wraps += 1
        @active = true
        yield
      ensure
        @active = false
      end
    end

    def setup
      @executor = RecordingExecutor.new
      @dispatcher = FakeDispatcher.new
      @config = build_config(namespace: "example_production", rpc_token: TOKEN, mode: :dispatch)
    end

    # --- routing --------------------------------------------------------------

    def test_each_operation_is_posted_to_its_own_path
      V1::OPERATIONS.each do |operation|
        status, _headers, body = call(operation, envelope(operation))

        assert_equal 200, status, operation
        assert_equal operation, JSON.parse(body.first)["operation"]
      end

      assert_equal V1::OPERATIONS, @dispatcher.calls.map(&:operation)
    end

    def test_an_unknown_operation_path_is_not_found
      status, = call("teleport", envelope("identify"))

      assert_equal 404, status
      assert_empty @dispatcher.calls
    end

    def test_a_body_replayed_against_another_operation_is_refused
      status, _headers, body = call("run_action", envelope("identify"))

      assert_equal 400, status
      assert_includes body.first, "operation does not match"
      assert_empty @dispatcher.calls, "a mismatched body must never reach the application"
    end

    def test_only_post_is_accepted
      status, = call("identify", envelope("identify"), method: "GET")

      assert_equal 405, status
    end

    # --- authentication -------------------------------------------------------

    def test_a_missing_or_wrong_credential_is_unauthorized
      ["", "Bearer ", "Bearer wrong-token", "Basic #{TOKEN}", TOKEN].each do |header|
        status, = call("identify", envelope("identify"), authorization: header)

        assert_equal 401, status, header.inspect
      end

      assert_empty @dispatcher.calls, "an unauthenticated call must never reach the application"
    end

    def test_authentication_happens_before_anything_is_parsed
      status, = call("teleport", "{not json", authorization: "Bearer wrong")

      assert_equal 401, status, "an unauthenticated caller learns nothing about routes or bodies"
    end

    def test_an_endpoint_without_a_configured_token_refuses_everything
      @config = build_config(namespace: "example_production")
      status, = call("identify", envelope("identify"))

      assert_equal 401, status
    end

    # --- envelope validation --------------------------------------------------

    def test_a_malformed_body_is_a_transport_error_not_a_fault
      status, _headers, body = call("identify", "{oops")

      assert_equal 400, status
      assert_includes body.first, "malformed JSON"
    end

    def test_a_non_object_body_is_refused
      status, = call("identify", "[1,2]")

      assert_equal 400, status
    end

    def test_an_unsupported_envelope_version_is_refused
      status, _headers, body = call("identify", envelope("identify", "version" => 2))

      assert_equal 400, status
      assert_includes body.first, "invalid request envelope"
      assert_empty @dispatcher.calls
    end

    def test_a_semantically_invalid_envelope_is_refused
      status, = call("identify", envelope("identify", "deadline_ms" => 0))

      assert_equal 400, status

      without_connection = JSON.parse(envelope("identify")).except("connection")
      status, = call("identify", JSON.generate(without_connection))

      assert_equal 400, status
    end

    def test_another_deployments_namespace_is_refused
      status, = call("identify", envelope("identify", "namespace" => "someone_else"))

      assert_equal 403, status
      assert_empty @dispatcher.calls
    end

    def test_a_non_json_content_type_is_refused
      status, = call("identify", envelope("identify"), content_type: "text/plain")

      assert_equal 415, status
    end

    def test_a_body_over_the_ceiling_is_refused_unread
      status, = call(
        "identify", envelope("identify"),
        content_length: (Orbitcast::RailsEndpoint::MAX_REQUEST_BYTES + 1).to_s
      )

      assert_equal 413, status
      assert_empty @dispatcher.calls
    end

    # --- outcomes -------------------------------------------------------------

    def test_a_denial_is_a_two_hundred
      @dispatcher = FakeDispatcher.new { |request| V1::Reply.denied(request, "no signed-in user") }
      status, _headers, body = call("identify", envelope("identify"))
      reply = JSON.parse(body.first)

      assert_equal 200, status, "a refusal is a decision, not a transport failure"
      assert_equal "denied", reply["outcome"]
      assert_equal "no signed-in user", reply["reason"]
      assert_empty reply["effects"]
    end

    def test_an_application_failure_is_a_fault_reply_not_an_http_error
      @dispatcher = FakeDispatcher.new { |request| V1::Reply.fault(request, "RuntimeError") }
      status, _headers, body = call("identify", envelope("identify"))
      reply = JSON.parse(body.first)

      assert_equal 200, status
      assert_equal "fault", reply["outcome"]
      assert_equal REQUEST_ID, reply["request_id"], "a fault must still correlate"
    end

    def test_a_reply_this_process_would_itself_reject_is_never_emitted
      @dispatcher = FakeDispatcher.new do |request|
        # Effects on a denied reply: rejected by the builder, so the endpoint
        # fails closed rather than sending something the relay would refuse.
        V1::Reply.new(
          request: request, outcome: V1::OUTCOME_ACCEPTED,
          describe: { "supported_versions" => [2] }
        )
      end
      status, = call("describe", envelope("describe"))

      assert_equal 500, status
    end

    # Every refusal is the shared error envelope, not a text/plain line: the
    # relay logs one shape for ingress and Rails-endpoint errors alike.
    def test_every_non_2xx_response_is_the_json_error_envelope
      expectations = {
        [405, "method_not_allowed"] => -> { call("identify", envelope("identify"), method: "GET") },
        [401, "unauthorized"] => -> { call("identify", envelope("identify"), authorization: "Bearer wrong") },
        [404, "not_found"] => -> { call("teleport", envelope("identify")) },
        [415, "unsupported_media_type"] => -> { call("identify", envelope("identify"), content_type: "text/plain") },
        [413, "payload_too_large"] => lambda do
          call("identify", envelope("identify"),
               content_length: (Orbitcast::RailsEndpoint::MAX_REQUEST_BYTES + 1).to_s)
        end,
        [400, "malformed_json"] => -> { call("identify", "{oops") },
        [400, "invalid_request"] => -> { call("identify", "[1,2]") },
        [400, "operation_mismatch"] => -> { call("run_action", envelope("identify")) },
        [403, "namespace_mismatch"] => -> { call("identify", envelope("identify", "namespace" => "someone_else")) }
      }

      expectations.each do |(expected_status, expected_code), request|
        status, headers, body = request.call

        assert_equal expected_status, status, expected_code
        assert_equal "application/json", headers["content-type"], expected_code
        assert_equal body.first.bytesize.to_s, headers["content-length"], expected_code

        error = JSON.parse(body.first)

        assert_equal 1, error.fetch("version"), expected_code
        assert_equal expected_code, error.fetch("error")
        assert_kind_of String, error.fetch("message"), expected_code
        refute_empty error.fetch("message"), expected_code
      end
    end

    def test_internal_failures_are_the_json_error_envelope_too
      @dispatcher = FakeDispatcher.new { |_request| raise "boom" }
      status, headers, body = call("identify", envelope("identify"))

      assert_equal 500, status
      assert_equal "application/json", headers["content-type"]
      assert_equal({ "version" => 1, "error" => "internal_error", "message" => "internal error" }, JSON.parse(body.first))
    end

    def test_the_reply_is_json_with_a_content_length
      _status, headers, body = call("identify", envelope("identify"))

      assert_equal "application/json", headers["content-type"]
      assert_equal body.first.bytesize.to_s, headers["content-length"]
    end

    # --- executor -------------------------------------------------------------

    def test_every_call_runs_inside_the_executor
      call("identify", envelope("identify"))
      call("teleport", envelope("identify"))
      call("identify", "{oops")

      assert_equal 3, @executor.wraps, "even a refused call is wrapped: it still ran Ruby"
    end

    def test_the_dispatcher_runs_while_the_executor_is_active
      active = nil
      @dispatcher = FakeDispatcher.new do |request|
        active = @executor.active?
        V1::Reply.accepted(request)
      end
      call("identify", envelope("identify"))

      assert active, "callbacks must run inside the executor so leases are released after"
      refute_predicate @executor, :active?
    end

    private

    def endpoint
      Orbitcast::RailsEndpoint.new(
        config: @config, dispatcher: @dispatcher, executor: @executor
      )
    end

    def call(operation, body, method: "POST", authorization: "Bearer #{TOKEN}",
             content_type: "application/json", content_length: nil)
      env = {
        "REQUEST_METHOD" => method,
        "PATH_INFO" => "#{MOUNT}/#{operation}",
        "CONTENT_TYPE" => content_type,
        "CONTENT_LENGTH" => content_length || body.bytesize.to_s,
        "rack.input" => StringIO.new(body)
      }
      env["HTTP_AUTHORIZATION"] = authorization unless authorization.nil?

      endpoint.call(env)
    end

    def envelope(operation, overrides = {})
      hash = {
        "version" => 1,
        "operation" => operation,
        "namespace" => "example_production",
        "node_id" => "node-7",
        "session_id" => operation == "describe" ? "00000000-0000-0000-0000-000000000000" : SESSION,
        "request_id" => REQUEST_ID,
        "sequence" => 1,
        "expected_revision" => 0,
        "deadline_ms" => 1000
      }

      unless operation == "describe"
        hash["connection"] = {
          "url" => "/cable", "client_address" => "203.0.113.7", "headers" => { "cookie" => "a=b" }
        }
      end

      subscription = { "identifier" => IDENTIFIER, "streams" => [] }
      case operation
      when "open_channel", "close_channel"
        hash["subscription"] = subscription
      when "run_action"
        hash["subscription"] = subscription
        hash["action"] = { "identifier" => IDENTIFIER, "json_data" => '{"action":"speak"}' }
      when "release_connection"
        hash["release"] = { "reason" => "remote", "reconnect" => false, "subscriptions" => [] }
      end

      JSON.generate(hash.merge(overrides))
    end
  end
end
