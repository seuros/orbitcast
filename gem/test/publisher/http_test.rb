# frozen_string_literal: true

require "test_helper"
require "json"

require "orbitcast/publisher/http"

# The private HTTP publication ingress.
#
# The Rust listener lives in src/transport/ingress.rs (POST /v1/publications).
# These tests pin the Ruby half against the same wire shape; the shared ack
# fixture is tests/fixtures/ingress/ack.json.
class PublisherHttpTest < Minitest::Test
  Http = Orbitcast::Publisher::Http

  # MOCK: stands in for the pooled Net::HTTP connection. Captures what would
  # have been posted; nothing opens a socket.
  class FakeConnection
    Response = Struct.new(:code, :body)

    attr_reader :posts

    def initialize(code: "202")
      @posts = []
      @code = code
    end

    def post(uri, body, authorization)
      @posts << { uri: uri, body: body, authorization: authorization }
      Response.new(@code, '{"version":1}')
    end

    def close; end
  end

  INGRESS = "http://127.0.0.1:28081/v1/publications"

  def config(**overrides)
    build_config(
      publish_via: :http,
      ingress_url: INGRESS,
      ingress_token: "ingress-token",
      **overrides
    )
  end

  def test_a_publication_is_posted_as_the_v1_publication_envelope
    connection = FakeConnection.new
    publisher = Http.new(config, connection_factory: -> { connection })

    publication_id = publisher.publish("chat:42", '{"body":"hi"}')
    post = connection.posts.fetch(0)

    assert_match(/\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/, publication_id)
    assert_equal "Bearer ingress-token", post[:authorization]
    assert_equal "/v1/publications", post[:uri].request_uri
    assert_equal(
      {
        "version" => 1,
        "namespace" => Orbitcast::TestHelper::NAMESPACE,
        "id" => publication_id,
        "stream" => "chat:42",
        "json_payload" => '{"body":"hi"}'
      },
      JSON.parse(post[:body])
    )
  end

  def test_the_payload_is_embedded_verbatim
    connection = FakeConnection.new
    publisher = Http.new(config, connection_factory: -> { connection })
    payload = '{"html":"<p>café</p>","n":1.0000000000000002}'

    publisher.publish("chat:42", payload)

    assert_equal payload, JSON.parse(connection.posts.first[:body])["json_payload"],
                 "the ActionCable-encoded text is never re-encoded"
  end

  def test_a_refused_credential_is_an_ingress_error
    %w[401 403].each do |code|
      publisher = Http.new(config, connection_factory: -> { FakeConnection.new(code: code) })

      assert_raises(Orbitcast::IngressError) { publisher.publish("chat:42", "{}") }
    end
  end

  def test_a_413_is_a_payload_too_large
    publisher = Http.new(config, connection_factory: -> { FakeConnection.new(code: "413") })

    assert_raises(Orbitcast::PayloadTooLarge) { publisher.publish("chat:42", "{}") }
  end

  def test_any_other_status_is_an_ingress_error
    publisher = Http.new(config, connection_factory: -> { FakeConnection.new(code: "503") })

    assert_raises(Orbitcast::IngressError) { publisher.publish("chat:42", "{}") }
  end

  def test_an_oversized_payload_never_leaves_the_process
    connection = FakeConnection.new
    publisher = Http.new(config(max_publication_size: 8000), connection_factory: -> { connection })

    assert_raises(Orbitcast::PayloadTooLarge) { publisher.publish("chat:42", "x" * 9000) }
    assert_empty connection.posts
  end

  def test_the_stream_guard_is_the_one_the_postgresql_publisher_uses
    publisher = Http.new(config, connection_factory: -> { FakeConnection.new })

    assert_raises(ArgumentError) { publisher.publish("", "{}") }
    assert_raises(ArgumentError) { publisher.publish(:chat, "{}") }
    assert_raises(ArgumentError) { publisher.publish("chat:42", { body: "hi" }) }
  end

  # Configuration refuses the mode without an endpoint, so the publisher can
  # never be built pointing at nothing.
  def test_publish_via_http_requires_an_ingress_url
    error = assert_raises(Orbitcast::ConfigurationError) do
      build_config(publish_via: :http).validate!
    end

    assert_includes error.message, "ingress_url"
  end

  def test_streams_over_1024_bytes_never_leave_the_process
    connection = FakeConnection.new
    publisher = Http.new(config, connection_factory: -> { connection })

    assert_raises(ArgumentError) { publisher.publish("x" * 1025, "{}") }
    assert_empty connection.posts
  end

  # MOCK: see PublisherPostgresTest::RecordingInstrumenter.
  class RecordingInstrumenter
    attr_reader :events

    def initialize
      @events = []
    end

    def instrument(name, payload = {})
      yield payload
    rescue StandardError => e
      payload[:exception] = [e.class.name, e.message]
      raise
    ensure
      @events << [name, payload]
    end
  end

  def test_publishes_are_instrumented_with_via_http_including_failures
    instrumenter = RecordingInstrumenter.new
    publisher = Http.new(config, connection_factory: -> { FakeConnection.new }, instrumenter: instrumenter)

    publisher.publish("chat:42", '{"body":"hi"}')

    assert_equal [["publish.orbitcast", { stream: "chat:42", bytes: 13, via: :http }]], instrumenter.events

    refused = Http.new(config, connection_factory: -> { FakeConnection.new(code: "503") }, instrumenter: instrumenter)

    assert_raises(Orbitcast::IngressError) { refused.publish("chat:42", "{}") }
    assert_equal "Orbitcast::IngressError", instrumenter.events.fetch(1).last[:exception].first
  end
end
