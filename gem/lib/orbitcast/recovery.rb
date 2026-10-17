# frozen_string_literal: true

require "base64"
require "json"
require "openssl"

require_relative "error"

module Orbitcast
  # Recovery tokens: the capability that lets one browser session replay
  # retained history on `Orbitcast::RecoveryChannel`.
  #
  #   base64(JSON(claims)) + "--" + hex(HMAC-SHA256(recovery_secret, base64_part))
  #
  # Same wire shape as `Orbitcast::Streams`, and deliberately **not** the same
  # key or the same purpose:
  #
  # * the key is `config.recovery_secret`, exported to the relay as
  #   `ORBITCAST_RECOVERY_SECRET_BASE64`. There is no fallback to
  #   `streams_secret`, in either direction.
  # * the purpose is `"recover"`, so a subscribe token can never be presented as
  #   a recovery capability (and vice versa).
  # * the claims name the exact streams that may be replayed, so a recovery
  #   token never widens access. Mint it from the same authorization decision
  #   that produced the subscription, and keep the lifetime short.
  #
  # Recovery is opt-in on both sides: nothing here is used unless the
  # application asks for a token *and* the relay runs with `broker_enabled`.
  # An unchanged ActionCable or Turbo client never sees any of it.
  #
  # Two things the token does **not** do:
  #
  # * It does not make history exist. Only publications that pass through the
  #   relay are retained, which means `config.publish_via = :http`. The
  #   default `:postgres` path emits `pg_notify` from this process and never
  #   reaches the broker, so nothing broadcast that way can be replayed - the
  #   relay has no Ruby-side switch for this, and this module cannot detect
  #   it; it is a deployment decision.
  # * It does not authorize by itself. The relay re-checks on every resume
  #   that the connection is identified, that the token is still valid, and
  #   that the connection holds a confirmed subscription to each stream it
  #   replays: `streams` must be the logical names the channels `stream_from`,
  #   and the browser must subscribe to those channels before it creates the
  #   recovery subscription.
  module Recovery
    CLAIMS_VERSION = 1
    PURPOSE = "recover"
    SEPARATOR = "--"
    DIGEST = "SHA256"

    # Same bound the server enforces (`MAX_RECOVERY_STREAMS`).
    MAX_STREAMS = 64

    # Default token lifetime, in seconds. Deliberately short: a recovery token
    # is used at reconnect time, not held for a session's lifetime.
    DEFAULT_EXPIRES_IN = 900

    class << self
      # Issue a recovery capability.
      #
      # @param session_id [String] an identity *reference* the application
      #   recognizes. Never a cookie, never sensitive connection state: it is
      #   stored in the relay's session row.
      # @param streams [Array<String>] the logical streams this token may
      #   replay. Exactly the streams the caller already authorized.
      # @param expires_in [Integer, nil] seconds; defaults to
      #   {DEFAULT_EXPIRES_IN}
      # @return [String] the token for a
      #   `{"channel":"Orbitcast::RecoveryChannel","token":"..."}` identifier
      def recovery_token(session_id:, streams:, expires_in: nil, config: Orbitcast.config)
        payload = claims(session_id: session_id, streams: streams,
                         expires_in: expires_in, config: config)
        encoded = Base64.strict_encode64(JSON.generate(payload))

        "#{encoded}#{SEPARATOR}#{signature(encoded, config.recovery_secret!)}"
      end

      # The claim set, in the fixed key order the server's fixtures pin:
      # version, namespace, session_id, streams, purpose, expires_at.
      def claims(session_id:, streams:, expires_in: nil, config: Orbitcast.config, now: Time.now)
        ttl = expires_in.nil? ? DEFAULT_EXPIRES_IN : Integer(expires_in)
        raise ConfigurationError, "expires_in must be greater than zero" unless ttl.positive?

        namespace = config.namespace
        raise ConfigurationError, "namespace is required to sign recovery tokens" if namespace.nil?

        {
          version: CLAIMS_VERSION,
          namespace: namespace,
          session_id: validated_session_id(session_id),
          streams: validated_streams(streams),
          purpose: PURPOSE,
          expires_at: now.to_i + ttl
        }
      end

      # Exposed for tests and for `orbitcast doctor`; the server owns the
      # authoritative verifier.
      def signature(encoded, key)
        OpenSSL::HMAC.hexdigest(DIGEST, key, encoded)
      end

      private

      def validated_session_id(session_id)
        unless session_id.is_a?(String)
          raise ArgumentError, "session_id must be a String, got #{session_id.class}"
        end
        raise ArgumentError, "session_id must not be empty" if session_id.empty?

        session_id
      end

      # A token grants exactly what it names, so an empty list is refused here
      # rather than minted into a token the server would reject anyway.
      def validated_streams(streams)
        unless streams.is_a?(Array)
          raise ArgumentError, "streams must be an Array of stream names, got #{streams.class}"
        end
        raise ArgumentError, "streams must not be empty" if streams.empty?

        if streams.length > MAX_STREAMS
          raise ArgumentError, "streams must name at most #{MAX_STREAMS} streams"
        end

        streams.map do |stream|
          unless stream.is_a?(String)
            raise ArgumentError, "stream must be a String, got #{stream.class}"
          end
          raise ArgumentError, "stream must not be empty" if stream.empty?

          stream
        end
      end
    end
  end

  # The dedicated recovery key.
  #
  # Added here rather than in `Configuration`'s attribute allowlist so that
  # recovery stays a separate, opt-in concern: set it explicitly
  # (`Orbitcast.config.recovery_secret = ...`), and export it to the relay as
  # `ORBITCAST_RECOVERY_SECRET_BASE64`.
  class Configuration
    # `recovery_secret` reader/writer live in configuration.rb with the other
    # secrets (shared normalizer); only the bang helper is recovery-specific.
    def recovery_secret!
      return recovery_secret if recovery_secret

      raise ConfigurationError,
            "recovery_secret is not configured; set config.orbitcast.recovery_secret " \
            "(exported to the relay as ORBITCAST_RECOVERY_SECRET_BASE64) before signing " \
            "recovery tokens"
    end
  end

  class << self
    # Issue a recovery capability for `streams`. The result goes into an
    # ordinary ActionCable identifier:
    # {"channel":"Orbitcast::RecoveryChannel","token":"..."}.
    def recovery_token(session_id:, streams:, expires_in: nil)
      Recovery.recovery_token(session_id: session_id, streams: streams,
                              expires_in: expires_in, config: config)
    end
  end
end
