# frozen_string_literal: true

require "base64"
require "json"
require "openssl"

require_relative "error"

module Orbitcast
  # Orbitcast signed-stream profile.
  #
  #   base64(JSON(claims)) + "--" + hex(HMAC-SHA256(key, base64_part))
  #
  # The HMAC covers the *encoded* bytes, so a verifier can authenticate the
  # token before it parses anything. Claims always carry every
  # authorization-bearing field; a verifier that finds one missing must fail
  # closed rather than widen access.
  #
  # A signed name is a *read* capability. Whisper (client-originated fanout
  # on the stream) is granted only by the optional `whisper: true` claim,
  # emitted only when asked for so plain tokens keep their exact bytes; the
  # relay never grants it to Turbo, CableReady, or public streams
  #
  # This is not Turbo's profile. `Turbo::StreamsChannel.signed_stream_name`
  # signs a bare stream string with Turbo's own verifier key and is parsed
  # separately by the server.
  module Streams
    CLAIMS_VERSION = 1
    PURPOSE = "subscribe"
    SEPARATOR = "--"
    DIGEST = "SHA256"

    # The relay refuses a stream longer than this (UTF-8 bytes) on every path:
    # signed-name claims, Turbo payloads, publications. Mirrored here so the
    # producer fails before signing or publishing something undeliverable.
    MAX_STREAM_BYTES = 1024

    class << self
      # @param stream [String] logical stream name
      # @param expires_in [Integer, nil] seconds; defaults to
      #   `config.stream_expires_in`
      # @param whisper [Boolean] also grant whisper on the stream
      # @return [String] signed stream name for a channel identifier
      def signed_stream_name(stream, expires_in: nil, whisper: false, config: Orbitcast.config)
        claims = claims(stream, expires_in: expires_in, whisper: whisper, config: config)
        encoded = Base64.strict_encode64(JSON.generate(claims))

        "#{encoded}#{SEPARATOR}#{signature(encoded, config.streams_secret!)}"
      end

      # The claims, in the fixed key order the fixtures pin: version,
      # namespace, stream, purpose, expires_at, then `whisper` only when true.
      def claims(stream, expires_in: nil, whisper: false, config: Orbitcast.config, now: Time.now)
        stream = validated_stream(stream)
        ttl = expires_in.nil? ? config.stream_expires_in : Integer(expires_in)
        raise ConfigurationError, "expires_in must be greater than zero" unless ttl.positive?

        namespace = config.namespace
        raise ConfigurationError, "namespace is required to sign stream names" if namespace.nil?

        unless [true, false].include?(whisper)
          raise ArgumentError, "whisper must be true or false, got #{whisper.inspect}"
        end

        claims = {
          version: CLAIMS_VERSION,
          namespace: namespace,
          stream: stream,
          purpose: PURPOSE,
          expires_at: now.to_i + ttl
        }
        claims[:whisper] = true if whisper
        claims
      end

      # Exposed for tests and for `orbitcast doctor`; the server owns the
      # authoritative verifier.
      def signature(encoded, key)
        OpenSSL::HMAC.hexdigest(DIGEST, key, encoded)
      end

      # The one stream-name guard, shared with the publisher
      # (`Orbitcast::Publisher::Postgres`): a stream name is the routing key of
      # both a signed token and a notice envelope, so the two paths must reject
      # exactly the same values.
      #
      # @param stream [String] logical stream name
      # @return [String] the validated name
      # @raise [ArgumentError] when it is not a String, is empty, is not valid
      #   UTF-8, or exceeds {MAX_STREAM_BYTES}
      def validated_stream(stream)
        unless stream.is_a?(String)
          raise ArgumentError, "stream must be a String, got #{stream.class}"
        end

        stream = stream.to_s
        raise ArgumentError, "stream must not be empty" if stream.empty?

        unless stream.valid_encoding?
          raise ArgumentError, "stream is not valid #{stream.encoding}"
        end

        unless stream.encoding == Encoding::UTF_8
          begin
            stream = stream.encode(Encoding::UTF_8)
          rescue EncodingError => e
            raise ArgumentError, "stream must be UTF-8 text (#{e.message})"
          end
        end
        if stream.bytesize > MAX_STREAM_BYTES
          raise ArgumentError,
                "stream is #{stream.bytesize} bytes; the maximum is #{MAX_STREAM_BYTES} " \
                "bytes of UTF-8 (the relay refuses longer streams)"
        end

        stream
      end
    end
  end

  class << self
    # Issue a read capability for `stream` (plus whisper when `whisper:
    # true`). The result goes into an ordinary ActionCable identifier:
    # {"channel":"Orbitcast::StreamsChannel","signed_stream_name":"..."}.
    def signed_stream_name(stream, expires_in: nil, whisper: false)
      Streams.signed_stream_name(stream, expires_in: expires_in, whisper: whisper, config: config)
    end
  end
end
