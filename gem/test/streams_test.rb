# frozen_string_literal: true

require "test_helper"
require "base64"
require "json"
require "openssl"

class StreamsTest < Minitest::Test
  def test_signed_stream_name_shape
    config = build_config
    token = Orbitcast::Streams.signed_stream_name("chat:42", config: config)
    encoded, signature, extra = token.split("--")

    assert_nil extra, "token must contain exactly one separator"
    assert_match(/\A[0-9a-f]{64}\z/, signature)

    claims = JSON.parse(Base64.strict_decode64(encoded))

    assert_equal 1, claims["version"]
    assert_equal Orbitcast::TestHelper::NAMESPACE, claims["namespace"]
    assert_equal "chat:42", claims["stream"]
    assert_equal "subscribe", claims["purpose"]
    assert_kind_of Integer, claims["expires_at"]
    assert_operator claims["expires_at"], :>, Time.now.to_i
  end

  def test_signature_covers_the_encoded_bytes
    config = build_config
    token = Orbitcast::Streams.signed_stream_name("chat:42", config: config)
    encoded, signature = token.split("--")

    expected = OpenSSL::HMAC.hexdigest("SHA256", Orbitcast::TestHelper::STREAMS_SECRET, encoded)

    assert_equal expected, signature
    refute_equal(
      expected,
      OpenSSL::HMAC.hexdigest("SHA256", "other-secret", encoded),
      "a different key must not produce the same signature"
    )
  end

  def test_tampered_claims_break_the_signature
    config = build_config
    token = Orbitcast::Streams.signed_stream_name("chat:42", config: config)
    encoded, signature = token.split("--")

    claims = JSON.parse(Base64.strict_decode64(encoded))
    claims["stream"] = "chat:1"
    forged = Base64.strict_encode64(JSON.generate(claims))

    refute_equal signature, OpenSSL::HMAC.hexdigest("SHA256", Orbitcast::TestHelper::STREAMS_SECRET, forged)
  end

  def test_expires_in_overrides_the_configured_ttl
    config = build_config(stream_expires_in: 3600)
    now = Time.now

    claims = Orbitcast::Streams.claims("chat:42", expires_in: 60, config: config, now: now)

    assert_equal now.to_i + 60, claims[:expires_at]

    default_claims = Orbitcast::Streams.claims("chat:42", config: config, now: now)

    assert_equal now.to_i + 3600, default_claims[:expires_at]
  end

  def test_claim_key_order_is_stable
    claims = Orbitcast::Streams.claims("chat:42", config: build_config)

    assert_equal %i[version namespace stream purpose expires_at], claims.keys
  end

  def test_whisper_is_an_opt_in_claim_emitted_only_when_granted
    config = build_config

    refute Orbitcast::Streams.claims("chat:42", config: config).key?(:whisper),
           "a plain token carries no whisper claim at all (absent means false)"
    refute Orbitcast::Streams.claims("chat:42", whisper: false, config: config).key?(:whisper)

    granted = Orbitcast::Streams.claims("chat:42", whisper: true, config: config)

    assert_equal true, granted[:whisper]
    assert_equal %i[version namespace stream purpose expires_at whisper], granted.keys,
                 "the capability follows the authorization-bearing claims"

    token = Orbitcast::Streams.signed_stream_name("chat:42", whisper: true, config: config)
    encoded, signature = token.split("--")
    decoded = JSON.parse(Base64.strict_decode64(encoded))

    assert_equal true, decoded["whisper"]
    assert_equal OpenSSL::HMAC.hexdigest("SHA256", Orbitcast::TestHelper::STREAMS_SECRET, encoded), signature,
                 "the capability is covered by the signature"

    with_global_config(
      namespace: "global_ns",
      database_url: Orbitcast::TestHelper::DATABASE_URL,
      streams_secret: "global-secret"
    ) do
      granted = JSON.parse(Base64.strict_decode64(Orbitcast.signed_stream_name("chat:42", whisper: true).split("--").first))
      plain = JSON.parse(Base64.strict_decode64(Orbitcast.signed_stream_name("chat:42").split("--").first))

      assert_equal true, granted["whisper"]
      refute plain.key?("whisper")
    end
  end

  def test_whisper_must_be_a_boolean
    assert_raises(ArgumentError) do
      Orbitcast::Streams.claims("chat:42", whisper: "yes", config: build_config)
    end
    assert_raises(ArgumentError) do
      Orbitcast::Streams.claims("chat:42", whisper: nil, config: build_config)
    end
  end

  def test_rejects_invalid_stream
    config = build_config

    assert_raises(ArgumentError) { Orbitcast::Streams.signed_stream_name("", config: config) }
    assert_raises(ArgumentError) { Orbitcast::Streams.signed_stream_name(:chat, config: config) }
  end

  def test_rejects_non_positive_expiry
    assert_raises(Orbitcast::ConfigurationError) do
      Orbitcast::Streams.signed_stream_name("chat:42", expires_in: 0, config: build_config)
    end
  end

  def test_requires_a_streams_secret
    config = build_config(streams_secret: nil)

    error = assert_raises(Orbitcast::ConfigurationError) do
      Orbitcast::Streams.signed_stream_name("chat:42", config: config)
    end

    assert_includes error.message, "streams_secret"
  end

  def test_module_level_helper_uses_the_global_configuration
    with_global_config(
      namespace: "global_ns",
      database_url: Orbitcast::TestHelper::DATABASE_URL,
      streams_secret: "global-secret"
    ) do
      token = Orbitcast.signed_stream_name("chat:42")
      encoded, signature = token.split("--")
      claims = JSON.parse(Base64.strict_decode64(encoded))

      assert_equal "global_ns", claims["namespace"]
      assert_equal OpenSSL::HMAC.hexdigest("SHA256", "global-secret", encoded), signature
    end
  end

  def test_unicode_stream_round_trips_through_base64_json
    config = build_config
    stream = "chat:ünïcode/🚀"
    token = Orbitcast::Streams.signed_stream_name(stream, config: config)
    claims = JSON.parse(Base64.strict_decode64(token.split("--").first))

    assert_equal stream, claims["stream"]
  end

  def test_streams_are_bounded_at_1024_utf8_bytes
    assert_equal "x" * 1024, Orbitcast::Streams.validated_stream("x" * 1024)
    assert_raises(ArgumentError) { Orbitcast::Streams.validated_stream("x" * 1025) }
    assert_raises(ArgumentError) { Orbitcast::Streams.validated_stream("é" * 513) }
    assert_raises(ArgumentError) { Orbitcast::Streams.validated_stream("\xff".b) }
    assert_equal 1024, Orbitcast::Streams::MAX_STREAM_BYTES
  end
end
