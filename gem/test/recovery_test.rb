# frozen_string_literal: true

require "test_helper"

# `Orbitcast::Recovery` mints the one recovery envelope the gem produces: the
# token. The byte-for-byte contract with the relay is pinned by
# contract_test.rb against tests/fixtures/recovery/tokens.json; this file
# covers the boundary checks around minting, which the fixtures only imply.
class RecoveryTest < Minitest::Test
  include Orbitcast::TestHelper

  RECOVERY_SECRET = "recovery-only-secret-32-bytes-000"

  def recovery_config(**overrides)
    build_config(**overrides).tap { |config| config.recovery_secret = RECOVERY_SECRET }
  end

  def test_claims_carry_the_fixed_key_order_and_purpose
    now = Time.at(1_760_000_000).utc
    claims = Orbitcast::Recovery.claims(
      session_id: "session:7", streams: %w[chat:42 posts:1], expires_in: 60,
      config: recovery_config, now: now
    )

    assert_equal %i[version namespace session_id streams purpose expires_at], claims.keys,
                 "the relay's fixtures pin this order"
    assert_equal 1, claims[:version]
    assert_equal NAMESPACE, claims[:namespace]
    assert_equal "recover", claims[:purpose]
    assert_equal now.to_i + 60, claims[:expires_at]
  end

  def test_the_default_lifetime_is_short
    assert_equal 900, Orbitcast::Recovery::DEFAULT_EXPIRES_IN

    now = Time.at(1_760_000_000).utc
    claims = Orbitcast::Recovery.claims(session_id: "s", streams: ["chat:42"],
                                        config: recovery_config, now: now)

    assert_equal now.to_i + 900, claims[:expires_at]
  end

  def test_tokens_are_the_documented_shape_and_verify_with_the_recovery_key
    token = Orbitcast::Recovery.recovery_token(session_id: "session:7", streams: ["chat:42"],
                                               config: recovery_config)
    encoded, signature, extra = token.split("--")

    assert_nil extra
    assert_match(/\A[0-9a-f]{64}\z/, signature)
    assert_equal signature, Orbitcast::Recovery.signature(encoded, RECOVERY_SECRET)
    refute_equal signature, Orbitcast::Recovery.signature(encoded, STREAMS_SECRET),
                 "the streams key never signs a recovery token"

    payload = JSON.parse(Base64.strict_decode64(encoded))

    assert_equal "session:7", payload.fetch("session_id")
    assert_equal ["chat:42"], payload.fetch("streams")
  end

  def test_minting_refuses_what_the_relay_would_reject
    config = recovery_config

    assert_raises(ArgumentError) { Orbitcast::Recovery.claims(session_id: "", streams: ["a"], config: config) }
    assert_raises(ArgumentError) { Orbitcast::Recovery.claims(session_id: 7, streams: ["a"], config: config) }
    assert_raises(ArgumentError) { Orbitcast::Recovery.claims(session_id: "s", streams: [], config: config) }
    assert_raises(ArgumentError) { Orbitcast::Recovery.claims(session_id: "s", streams: "a", config: config) }
    assert_raises(ArgumentError) { Orbitcast::Recovery.claims(session_id: "s", streams: [""], config: config) }
    assert_raises(ArgumentError) do
      Orbitcast::Recovery.claims(session_id: "s", streams: Array.new(65) { |i| "s:#{i}" }, config: config)
    end
    assert_raises(Orbitcast::ConfigurationError) do
      Orbitcast::Recovery.claims(session_id: "s", streams: ["a"], expires_in: 0, config: config)
    end
  end

  def test_minting_needs_the_dedicated_key_and_a_namespace
    without_key = build_config

    assert_nil without_key.recovery_secret
    error = assert_raises(Orbitcast::ConfigurationError) do
      Orbitcast::Recovery.recovery_token(session_id: "s", streams: ["a"], config: without_key)
    end
    assert_match(/ORBITCAST_RECOVERY_SECRET_BASE64/, error.message)

    without_namespace = Orbitcast::Configuration.new
    without_namespace.recovery_secret = RECOVERY_SECRET

    assert_raises(Orbitcast::ConfigurationError) do
      Orbitcast::Recovery.claims(session_id: "s", streams: ["a"], config: without_namespace)
    end
  end

  def test_the_module_level_helper_uses_the_global_config
    with_global_config(namespace: NAMESPACE, database_url: DATABASE_URL,
                       recovery_secret: RECOVERY_SECRET) do
      token = Orbitcast.recovery_token(session_id: "session:7", streams: ["chat:42"])
      encoded, signature = token.split("--")

      assert_equal signature, Orbitcast::Recovery.signature(encoded, RECOVERY_SECRET)
    end
  end
end
