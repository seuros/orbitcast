# frozen_string_literal: true

require "test_helper"
require "base64"
require "json"
require "openssl"

require "orbitcast/publisher/http"

# Cross-language contract tests over the shared JSON golden fixtures in
# tests/fixtures/ at the repository root.
#
# The exact same files are consumed by tests/contract.rs; neither suite
# regenerates them during test execution. Signing fixtures are produced by
# gem/test/dummy/script/export_signed_stream_fixtures.rb (Ruby is the
# producer). Rust is the authoritative validator of inbound envelopes; the
# Ruby suite asserts the producer side — tokens it generates, notices it
# emits, and channel names it derives match the fixtures byte for byte.
#
# Plain Ruby: no Rails boot, no ActionCable, no PostgreSQL.
class ContractTest < Minitest::Test
  FIXTURES = File.expand_path("../../tests/fixtures", __dir__)

  CONTRACT_SECRET = "test-secret-32-bytes-padded-000000"
  CONTRACT_NAMESPACE = "example_production"

  # MOCK: stands in for a PG::Connection; captures pg_notify calls. Only
  # `exec_params` and `close` are exercised — the publisher's whole surface.
  class FakeConnection
    attr_reader :calls

    def initialize
      @calls = []
    end

    def exec_params(sql, params)
      @calls << [sql, params]
      [{ "pg_notify" => nil }]
    end

    def close; end
  end

  def self.fixture(relative_path)
    JSON.parse(File.read(File.join(FIXTURES, relative_path)))
  end

  def self.fixture_files(group)
    Dir[File.join(FIXTURES, group, "*.json")].sort.map do |path|
      [File.basename(path), JSON.parse(File.read(path))]
    end
  end

  # --- channel names --------------------------------------------------------

  def test_channel_name_derivation_matches_the_golden_vectors
    vectors = self.class.fixture("channel_names/vectors.json").fetch("cases")

    refute_empty vectors

    vectors.each do |case_data|
      input = case_data.fetch("input")
      derived = Orbitcast::Publisher::Postgres.channel_name(
        input.fetch("namespace"), input.fetch("stream")
      )

      assert_equal case_data.dig("expect", "channel"), derived, case_data.fetch("name")
      assert_equal 58, derived.bytesize, case_data.fetch("name")
    end
  end

  # --- signed streams -------------------------------------------------------

  def test_signed_stream_fixtures_verify_with_plain_hmac
    each_signed_stream_case do |context, input, expect|
      encoded, signature, extra = input.fetch("token").split("--")

      assert_nil extra, "#{context}: token must contain exactly one separator"
      assert_match(/\A[0-9a-f]{64}\z/, signature, context)

      recomputed = Orbitcast::Streams.signature(encoded, fixture_secret(input))

      assert_equal expect.fetch("signature_valid"), recomputed == signature, context
    end
  end

  def test_signed_stream_fixture_claims_parse_and_default_as_expected
    each_signed_stream_case do |context, input, expect|
      claims = JSON.parse(Base64.strict_decode64(input.fetch("token").split("--").first))
      expected = expect.fetch("claims")

      # An absent version reads as 1 (serializers always emit it).
      assert_equal expected.fetch("version"), claims.fetch("version", 1), context
      %w[namespace stream purpose expires_at].each do |field|
        # An absent claim is absent in expect.claims too — never defaulted.
        assert_equal expected.key?(field), claims.key?(field), "#{context}: #{field} presence"
        if expected.key?(field)
          assert_equal expected.fetch(field), claims.fetch(field), "#{context}: #{field}"
        else
          assert_nil claims[field], "#{context}: #{field}"
        end
      end

      if expect.fetch("claims_valid")
        assert_operator claims.fetch("expires_at"), :>, input.fetch("now"), context
        assert_equal "subscribe", claims.fetch("purpose"), context
        assert_equal verifier_namespace(input), claims.fetch("namespace"), context
      end

      case expect["error_kind"]
      when "expired"
        assert_operator claims.fetch("expires_at"), :<=, input.fetch("now"), context
      when "wrong_purpose"
        refute_equal "subscribe", claims.fetch("purpose"), context
      when "wrong_namespace"
        refute_equal verifier_namespace(input), claims.fetch("namespace"), context
      when "missing_claim"
        missing = expect.fetch("missing")

        assert_includes %w[namespace stream purpose expires_at], missing, context
        refute claims.key?(missing), "#{context}: the token must really lack #{missing}"
        refute expected.key?(missing), "#{context}: expect.claims must not invent #{missing}"
      end
    end
  end

  # A token missing an authorization-bearing claim is one the gem cannot
  # mint: `Orbitcast::Streams.claims` emits all five, always. The fixture's
  # absent-claim tokens therefore come from a hand-built payload, and the
  # producer side of the contract is that the gem's output for the same
  # inputs carries the field the fixture dropped.
  def test_the_gem_always_emits_every_authorization_bearing_claim
    checked = 0

    self.class.fixture("signed_streams/absent_claims.json").fetch("cases").each do |case_data|
      expect = case_data.fetch("expect")
      missing = expect.fetch("missing")
      pinned = expect.fetch("claims")
      now = case_data.dig("input", "now")

      minted = Orbitcast::Streams.claims(
        pinned.fetch("stream", "chat:42"),
        expires_in: pinned.fetch("expires_at", now + 60) - now,
        config: contract_config,
        now: Time.at(now).utc
      ).transform_keys(&:to_s)

      %w[version namespace stream purpose expires_at].each do |field|
        assert minted.key?(field), "#{case_data.fetch("name")}: the gem emits #{field}"
      end
      assert minted.key?(missing)
      checked += 1
    end

    assert_equal 4, checked, "one case per authorization-bearing claim"
  end

  # Namespace isolation from the producer side: a token the gem signs carries
  # the gem's configured namespace, so a verifier in another namespace sees a
  # mismatch, shared key or not.
  def test_a_token_never_crosses_namespaces
    doc = self.class.fixture("signed_streams/namespace_mismatch.json")
    seen = 0

    doc.fetch("cases").each do |case_data|
      input = case_data.fetch("input")
      expect = case_data.fetch("expect")
      token_namespace = expect.dig("claims", "namespace")

      assert input.key?("namespace"), "#{case_data.fetch("name")}: a mismatch case names the verifier's namespace"

      if expect.fetch("valid")
        assert_equal input.fetch("namespace"), token_namespace
      else
        assert_equal "wrong_namespace", expect.fetch("error_kind")
        refute_equal input.fetch("namespace"), token_namespace
      end

      # The gem, configured for the verifier's namespace, cannot produce this
      # token's claims unless the namespaces agree.
      minted = Orbitcast::Streams.claims(
        expect.dig("claims", "stream"),
        expires_in: expect.dig("claims", "expires_at") - input.fetch("now"),
        config: contract_config(namespace: input.fetch("namespace")),
        now: Time.at(input.fetch("now")).utc
      ).transform_keys(&:to_s)

      assert_equal expect.fetch("valid"), minted == expect.fetch("claims"), case_data.fetch("name")
      seen += 1
    end

    assert_equal 2, seen
  end

  def test_gem_regenerates_the_valid_tokens_byte_for_byte
    # Claim construction, key order, base64 and HMAC all come from the gem;
    # pinned inputs must reproduce the checked-in tokens exactly — with a text
    # key and with raw binary key bytes alike.
    reproduced = 0

    %w[signed_streams/valid_claims.json signed_streams/binary_secret.json].each do |file|
      self.class.fixture(file).fetch("cases").each do |case_data|
        next unless case_data.dig("expect", "valid")

        input = case_data.fetch("input")
        expected_claims = case_data.dig("expect", "claims")
        now = input.fetch("now")
        secret = fixture_secret(input)
        config = contract_config
        config.streams_secret = secret

        claims = Orbitcast::Streams.claims(
          expected_claims.fetch("stream"),
          expires_in: expected_claims.fetch("expires_at") - now,
          config: config,
          now: Time.at(now).utc
        )

        assert_equal expected_claims, claims.transform_keys(&:to_s), case_data.fetch("name")

        encoded = Base64.strict_encode64(JSON.generate(claims))
        token = "#{encoded}--#{Orbitcast::Streams.signature(encoded, secret)}"

        assert_equal input.fetch("token"), token, case_data.fetch("name")
        reproduced += 1
      end
    end

    assert_operator reproduced, :>=, 4
  end

  # The binary key is exported to the relay exactly as the fixture carries
  # it: strict base64 of the raw bytes. A UTF-8 reinterpretation of those
  # bytes is not even representable, which is the point.
  def test_binary_stream_secrets_travel_as_strict_base64_of_raw_bytes
    doc = self.class.fixture("signed_streams/binary_secret.json")

    doc.fetch("cases").each do |case_data|
      input = case_data.fetch("input")

      refute input.key?("secret"), "#{case_data.fetch("name")}: a binary key has no text form"

      key = Base64.strict_decode64(input.fetch("secret_base64"))

      assert_equal 32, key.bytesize
      refute key.dup.force_encoding(Encoding::UTF_8).valid_encoding?, "the key is deliberately not UTF-8"

      config = contract_config
      config.streams_secret = key

      assert_equal input.fetch("secret_base64"), config.exported_env.fetch("ORBITCAST_STREAMS_SECRET_BASE64")
    end
  end

  # --- Turbo profile --------------------------------------------------------
  #
  # tests/fixtures/signed_streams/turbo/turbo_real.json holds REAL
  # turbo-rails output, produced by the dummy Rails application
  # (gem/test/dummy/script/export_turbo_stream_fixtures.rb). It lives in a
  # subdirectory because the flat signed_streams/*.json files are the Orbitcast
  # claims profile, which both suites parse with a different shape.
  #
  # Nothing here re-implements Turbo's format: the assertions state what the
  # gem and the Rust verifier must agree on, against bytes Turbo actually
  # emitted.

  def test_turbo_fixture_is_real_turbo_rails_output
    doc = turbo_fixture
    producer = doc.fetch("producer")

    assert_equal(
      'ActiveSupport::MessageVerifier.new(key, digest: "SHA256", serializer: JSON)',
      producer.fetch("verifier")
    )
    refute_empty producer.fetch("turbo_rails_version")
    refute_empty doc.fetch("cases")

    # The page the dummy app renders must carry the same token the exporter
    # signed: fixtures pin what `turbo_stream_from` really puts in the HTML.
    rendered = doc.fetch("rendered_page")

    assert_equal "Turbo::StreamsChannel", rendered.fetch("channel")
    assert_equal(
      doc.fetch("cases").first.dig("input", "signed_stream_name"),
      rendered.fetch("signed_stream_name")
    )
  end

  def test_turbo_tokens_are_hex_hmac_sha256_over_standard_base64_json_strings
    each_turbo_case do |context, input, expect|
      token = input.fetch("signed_stream_name")
      key = Base64.strict_decode64(input.fetch("key_base64"))
      encoded, signature, extra = token.split("--")

      assert_nil extra, "#{context}: exactly one -- separator"
      assert_equal expect.fetch("encoded_payload"), encoded, context
      assert_equal expect.fetch("signature"), signature, context

      # 1. The HMAC covers the ENCODED bytes, keyed with the raw key bytes.
      assert_equal(
        OpenSSL::HMAC.hexdigest("SHA256", key, encoded), signature,
        "#{context}: signature must be hex HMAC-SHA256 over the encoded part"
      )

      # 2. Standard (non-URL-safe) base64, padding included. A URL-safe decoder
      #    would reject these bytes.
      assert_match(/\A[A-Za-z0-9+\/]+={0,2}\z/, encoded, context)
      assert_equal expect.fetch("base64_padded"), encoded.include?("="), context
      assert_equal expect.fetch("encoded_payload"), Base64.strict_encode64(expect.fetch("payload_json")), context

      # 3. The payload is a JSON *string*, not a claims object: Rust parses it
      #    with `serde_json::from_str::<String>`.
      payload = Base64.strict_decode64(encoded).force_encoding(Encoding::UTF_8)

      assert_equal expect.fetch("payload_json"), payload, context
      decoded = JSON.parse(payload)

      assert_kind_of String, decoded, "#{context}: Turbo signs a bare stream string"
      assert_equal expect.fetch("stream"), decoded, context
    end
  end

  def test_turbo_fixture_records_no_rust_verifier_mismatch
    each_turbo_case do |context, _input, _expect, case_data|
      assert_equal(
        "match", case_data.fetch("rust_status"),
        "#{context}: real turbo-rails output the Rust verifier does not accept " \
        "(#{case_data["rust_note"]}). Fix src/streams.rs; never re-shape the fixture " \
        "to match the verifier."
      )
    end
  end

  def test_turbo_key_is_exported_to_rust_as_strict_base64_of_raw_bytes
    # Export the initialized
    # Turbo.signed_stream_verifier_key with
    # strict base64 and decode it to bytes in Rust; never guess that it is
    # secret_key_base or UTF-8 text.
    each_turbo_case do |context, input, _expect|
      key = Base64.strict_decode64(input.fetch("key_base64"))
      config = build_config(turbo_secret: key)
      exported = config.exported_env

      assert_equal input.fetch("key_base64"), exported.fetch("ORBITCAST_TURBO_SECRET_BASE64"), context
      refute exported.key?("SECRET_KEY_BASE"), context
      refute exported.key?("RAILS_MASTER_KEY"), context
      break # the key is the same in every case
    end
  end

  def test_turbo_and_orbitcast_profiles_share_no_secret
    # There is no fallback between signing profiles: the Orbitcast streams
    # secret must not authenticate a Turbo token.
    each_turbo_case do |context, input, expect|
      wrong = Orbitcast::Streams.signature(expect.fetch("encoded_payload"), CONTRACT_SECRET)

      refute_equal expect.fetch("signature"), wrong, context

      # And a one-character signature flip must not verify either.
      tampered = expect.fetch("signature").dup
      tampered[-1] = tampered[-1] == "0" ? "1" : "0"
      key = Base64.strict_decode64(input.fetch("key_base64"))

      refute_equal OpenSSL::HMAC.hexdigest("SHA256", key, expect.fetch("encoded_payload")), tampered, context
    end
  end

  # tests/fixtures/signed_streams/turbo/turbo_metadata_envelope.json: real
  # MessageVerifier output with a `_rails` metadata envelope, signed with the
  # Turbo key. Genuine HMAC, wrong shape: the Turbo profile must reject it
  # and turbo-rails itself does not accept it as a stream name.
  def test_turbo_metadata_envelopes_are_signed_but_are_not_stream_names
    doc = self.class.fixture("signed_streams/turbo/turbo_metadata_envelope.json")
    cases = doc.fetch("cases")

    refute_empty cases

    cases.each do |case_data|
      context = "turbo/turbo_metadata_envelope.json case #{case_data.fetch("name")}"
      input = case_data.fetch("input")
      expect = case_data.fetch("expect")
      key = Base64.strict_decode64(input.fetch("key_base64"))
      encoded, signature = input.fetch("signed_stream_name").split("--")

      assert expect.fetch("rejected"), context
      assert_nil expect.fetch("stream"), context
      assert_equal "metadata_envelope", expect.fetch("error_kind"), context
      # What turbo-rails' own verifier says is recorded, not asserted: it
      # refuses a purposed envelope (nil) but unwraps an unexpired purpose-less
      # one — the relay rejects both because it implements no metadata at all.
      assert expect.key?("turbo_rails_verified"), context

      # The signature is real: a verifier rejecting on the HMAC is rejecting
      # for the wrong reason.
      assert_equal OpenSSL::HMAC.hexdigest("SHA256", key, encoded), signature, context
      assert_equal expect.fetch("signature"), signature, context

      payload = JSON.parse(Base64.strict_decode64(encoded).force_encoding(Encoding::UTF_8))

      assert_kind_of Hash, payload, "#{context}: the payload is an envelope object, not a stream string"
      assert payload.key?("_rails"), context
      assert_equal "match", case_data.fetch("rust_status"),
                   "#{context}: the Rust verifier must reject this (#{case_data["rust_note"]})"
    end
  end

  def test_turbo_identifiers_are_ordinary_actioncable_identifiers
    each_turbo_case do |context, input, _expect|
      identifier = JSON.parse(input.fetch("identifier"))

      assert_equal "Turbo::StreamsChannel", identifier.fetch("channel"), context
      assert_equal input.fetch("signed_stream_name"), identifier.fetch("signed_stream_name"), context
      assert_equal 2, identifier.size, "#{context}: no extra client-supplied fields"
    end
  end

  # --- publications ---------------------------------------------------------

  def test_published_notices_match_the_valid_inline_fixture_shape
    self.class.fixture("publications/valid_inline.json").fetch("cases").each do |case_data|
      expected_notice = JSON.parse(case_data.dig("input", "wire"))
      assert_emitted_notice_matches(case_data, expected_notice)
    end
  end

  def test_unicode_and_escaped_payloads_are_preserved_byte_for_byte
    self.class.fixture("publications/unicode_escaping.json").fetch("cases").each do |case_data|
      expected_notice = JSON.parse(case_data.dig("input", "wire"))
      namespace = case_data.dig("expect", "namespace")

      # "café" is a legal wire namespace but not a legal Orbitcast
      # configuration namespace, so the publisher cannot emit it; the channel
      # derivation is still asserted below.
      if namespace.match?(Orbitcast::Configuration::NAMESPACE_PATTERN)
        assert_emitted_notice_matches(case_data, expected_notice)
      end

      payload = case_data.dig("expect", "json_payload")

      assert_equal payload, expected_notice.dig("publication", "json_payload"), case_data.fetch("name")
      if (values = case_data.dig("expect", "payload_values"))
        assert_equal values, JSON.parse(payload), case_data.fetch("name")
      end

      assert_equal(
        case_data.dig("expect", "channel"),
        Orbitcast::Publisher::Postgres.channel_name(namespace, case_data.dig("expect", "stream")),
        case_data.fetch("name")
      )
    end
  end

  def test_malformed_wire_fixtures_do_not_parse
    self.class.fixture("publications/invalid_malformed_json.json").fetch("cases").each do |case_data|
      assert_equal "malformed_json", case_data.dig("expect", "error_kind")
      assert_raises(JSON::ParserError, case_data.fetch("name")) do
        JSON.parse(case_data.dig("input", "wire"))
      end
    end
  end

  def test_publisher_refuses_the_empty_stream_fixture_cases
    publisher, = build_publisher

    self.class.fixture("publications/invalid_empty_stream.json").fetch("cases").each do |case_data|
      stream = JSON.parse(case_data.dig("input", "wire")).dig("publication", "stream") ||
               JSON.parse(case_data.dig("input", "wire")).fetch("stream")

      assert_raises(ArgumentError, case_data.fetch("name")) { publisher.publish(stream, "{}") }
    end
  end

  def test_oversized_payloads_raise_payload_too_large
    publisher, connection = build_publisher
    payload = JSON.generate(html: "x" * Orbitcast::Publisher::Postgres::MAX_NOTIFY_BYTES)

    assert_raises(Orbitcast::PayloadTooLarge) { publisher.publish("chat:42", payload) }
    assert_empty connection.calls, "oversized payloads must never reach the database"
  end

  # With the relay enabled, the emitted reference must match the shared
  # overflow fixture exactly (the publication id is the only variable field).
  def test_overflow_references_match_the_valid_overflow_ref_fixture_shape
    self.class.fixture("publications/valid_overflow_ref.json").fetch("cases").each do |case_data|
      expected = JSON.parse(case_data.dig("input", "wire"))
      namespace = expected.fetch("namespace")
      stream = expected.fetch("stream")

      connection = FakeConnection.new
      publisher = Orbitcast::Publisher::Postgres.new(
        contract_config(namespace: namespace).tap { |config| config.overflow_enabled = true },
        connection_factory: -> { connection }
      )

      payload = JSON.generate(html: "x" * Orbitcast::Publisher::Postgres::MAX_NOTIFY_BYTES)
      publication_id = publisher.publish(stream, payload)

      statements = connection.calls.map(&:first)

      assert_equal "BEGIN", statements.first, case_data.fetch("name")
      assert_equal "COMMIT", statements.last, case_data.fetch("name")

      channel, notice = connection.calls[2][1]

      assert_equal case_data.dig("expect", "channel"), channel, case_data.fetch("name")
      assert_match(/\A[0-9a-f-]{36}\z/, publication_id, case_data.fetch("name"))
      assert_equal(
        expected.merge("id" => publication_id),
        JSON.parse(notice),
        case_data.fetch("name")
      )
    end
  end

  # --- RPC envelopes --------------------------------------------------------
  #
  # tests/fixtures/rpc/*.json pins both directions of the Rails dispatch
  # contract. Rust is the authoritative validator; what Ruby asserts here is
  # that it *reads* every pinned request the same way and *writes* replies the
  # relay will accept — field for field, against the same bytes.

  def test_pinned_rpc_requests_parse_into_the_documented_fields
    seen = 0

    each_rpc_case("request") do |context, wire, expect|
      if expect.fetch("valid")
        request = V1::Request.parse(wire).validate!
        seen += 1

        %w[version operation namespace node_id session_id request_id sequence
           expected_revision deadline_ms].each do |field|
          next unless expect.key?(field)

          assert_equal expect.fetch(field), request.public_send(field), "#{context}: #{field}"
        end

        assert_pinned_connection(context, request, expect["connection"])
        assert_pinned_subscription(context, request, expect["subscription"])
        assert_pinned_action(context, request, expect["action"])
        assert_pinned_release(context, request, expect["release"])
      else
        error = assert_raises(Orbitcast::ProtocolError, context) do
          V1::Request.parse(wire).validate!
        end
        assert_rejection_kind(context, expect.fetch("error_kind"), error)
      end
    end

    assert_operator seen, :>, 0, "the request fixtures must carry valid cases"
  end

  # A request Ruby re-emits from what it parsed must be the same envelope: the
  # gem is a consumer here, and a consumer that silently normalizes is a
  # consumer that disagrees with the relay about what was sent.
  def test_pinned_rpc_requests_round_trip_without_normalization
    each_rpc_case("request") do |context, wire, expect|
      next unless expect.fetch("valid")

      original = JSON.parse(wire)
      reemitted = JSON.parse(V1::Request.parse(wire).to_json)

      original.each do |field, value|
        next if %w[connection subscription action release].include?(field)
        # Unknown fields are ignored on both ends, by rule: a newer peer's
        # extra field is not carried, it is dropped.
        next unless reemitted.key?(field)

        assert_equal value, reemitted[field], "#{context}: #{field}"
      end
      assert_equal 1, reemitted.fetch("version"), "#{context}: an absent version reads as 1"
      next unless original.key?("action")

      assert_equal original.dig("action", "json_data"), reemitted.dig("action", "json_data"),
                   "#{context}: the client-encoded data is carried byte for byte"
    end
  end

  # The reply half: Ruby builds each pinned reply from the request it answers
  # and must produce exactly the pinned envelope.
  def test_the_reply_builder_reproduces_the_pinned_replies
    built = 0

    each_rpc_case("reply") do |context, wire, expect|
      next unless expect.fetch("valid")

      # Absent and explicit null both mean unchanged, and Ruby emits absence.
      pinned = JSON.parse(wire).reject { |_field, value| value.nil? }
      # Absent fields read as documented defaults; Ruby always emits them, the
      # same way the canonical Rust serializer does. An absent outcome reads as
      # `fault` — never as an acceptance.
      expected = pinned.merge(
        # An absent version reads as 1; serializers always emit it.
        "version" => pinned.fetch("version", 1),
        "outcome" => pinned.fetch("outcome", V1::OUTCOME_FAULT),
        "effects" => pinned.fetch("effects", [])
      )
      # An accepted identify always states its identity map, empty or not: an
      # empty map is a valid accepted anonymous connection, not an omission.
      if expected["operation"] == V1::OPERATION_IDENTIFY &&
         expected["outcome"] == V1::OUTCOME_ACCEPTED
        expected["identity"] = pinned.fetch("identity", {})
      end

      # A fixture may carry a newer peer's extra fields; this release emits the
      # v1 fields, so the comparison is over exactly those.
      assert_equal expected.slice(*REPLY_FIELDS),
                   JSON.parse(build_pinned_reply(pinned, expect).to_json), context
      built += 1
    end

    assert_operator built, :>, 0, "the reply fixtures must carry valid cases"
  end

  # The other half of fail-closed: a reply the relay rejects must be one this
  # builder cannot produce — either it raises, or it emits something else. What
  # it must never do is hand the relay the pinned invalid envelope.
  def test_the_reply_builder_cannot_reproduce_a_rejected_reply
    checked = 0

    each_rpc_case("reply") do |context, wire, expect|
      next if expect.fetch("valid")

      pinned = begin
        JSON.parse(wire)
      rescue JSON::ParserError
        next # not JSON at all: nothing for a builder to reproduce
      end
      # Unknown operations and outcomes cannot even be expressed.
      next unless V1::OPERATIONS.include?(pinned["operation"])
      next unless V1::OUTCOMES.include?(pinned["outcome"])

      emitted =
        begin
          JSON.parse(build_pinned_reply(pinned, expect).to_json)
        rescue Orbitcast::ProtocolError
          checked += 1
          next
        end

      refute_equal pinned, emitted, "#{context}: the builder emitted a reply Rust rejects"
      checked += 1
    end

    assert_operator checked, :>, 0
  end

  def test_the_state_trichotomy_is_emitted_the_way_the_fixtures_pin_it
    self.class.fixture("rpc/reply_state_trichotomy.json").fetch("cases").each do |case_data|
      expect = case_data.fetch("expect")
      next unless expect.fetch("valid")

      context = "rpc/reply_state_trichotomy.json case #{case_data.fetch("name")}"
      pinned = JSON.parse(case_data.dig("input", "wire"))
      emitted = JSON.parse(build_pinned_reply(pinned.compact, expect).to_json)

      case expect.fetch("state")
      when "unchanged"
        refute emitted.key?("state"), "#{context}: unchanged is absence, never {}"
      when "clear"
        assert_equal({}, emitted["state"], "#{context}: {} is an explicit clear")
      when "replace"
        assert_equal expect.fetch("state_value"), emitted["state"], context
      end
    end
  end

  # --- presence -------------------------------------------------------------
  #
  # tests/fixtures/presence/*.json pins both directions of the presence
  # contract.
  # There is no Ruby presence *envelope*: a command is decoded by the relay and
  # a message is framed by the relay, so neither is something this gem writes.
  # What Ruby owns is the other end of the same contract — the `presence_grant`
  # effect that decides the subject, its metadata, its lease and its allowed
  # operations — and that is what these assert against the pinned bytes.
  #
  # Not assertable from Ruby, deliberately left to tests/presence.rs: decode
  # tolerance (unknown fields ignored, absent `op` a hard error), page
  # clamping, and roster ordering.

  def test_presence_operations_share_one_spelling_with_the_client_commands
    ops = presence_cases("command").filter_map do |_context, wire, expect|
      JSON.parse(wire)["op"] if expect.fetch("valid")
    rescue JSON::ParserError
      nil
    end

    refute_empty ops

    # Every operation a client may ask for is one a grant can authorize: the
    # allowlist and the wire share one vocabulary, or a grant could never be
    # exercised.
    ops.uniq.each do |op|
      assert_includes V1::PRESENCE_OPERATIONS, op
    end
    assert_equal V1::PRESENCE_OPERATIONS.sort, ops.uniq.sort,
                 "every operation the gem can grant is exercised by the command fixtures"
  end

  # The fixture's whole point, from the producing side: the four fields a
  # hostile client would most like to control are exactly the four that come
  # from a `presence_grant` effect and from nowhere else.
  def test_the_fields_a_client_cannot_forge_are_the_fields_a_grant_decides
    doc = self.class.fixture("presence/client_cannot_forge_identity.json")
    granted = granted_effect(
      stream: "chat:42", subject_id: "user:7", metadata: { "name" => "Marissa" },
      ttl_ms: 45_000, operations: %w[join snapshot]
    )
    checked = 0

    doc.fetch("cases").each do |case_data|
      context = "presence/client_cannot_forge_identity.json case #{case_data.fetch("name")}"
      forged = JSON.parse(case_data.dig("input", "wire"))
      ignored = case_data.dig("expect", "ignored_fields")

      refute_empty ignored, context

      ignored.each do |field|
        next unless granted.key?(field)

        assert forged.key?(field), "#{context}: the fixture forges #{field}"
        refute_equal forged.fetch(field), granted.fetch(field),
                     "#{context}: #{field} comes from the grant, never from the client"
        checked += 1
      end
    end

    # Every identity-bearing field the fixture forges must be a grant field.
    %w[subject_id metadata stream ttl_ms].each do |field|
      assert granted.key?(field), "a grant supplies #{field}"
    end
    assert_operator checked, :>, 0
  end

  def test_the_pinned_page_windows_agree_with_the_gems_limits
    presence_cases("command").each do |context, wire, expect|
      command = begin
        JSON.parse(wire)
      rescue JSON::ParserError
        next
      end

      if expect.fetch("valid")
        page = expect.fetch("page")

        assert_equal command.fetch("limit", V1::DEFAULT_PRESENCE_PAGE_SIZE), page.fetch("limit"), context
        assert_equal command.fetch("offset", 0), page.fetch("offset"), context
        assert_operator page.fetch("limit"), :<=, V1::MAX_PRESENCE_PAGE_SIZE, context
        assert_operator page.fetch("offset"), :<=, V1::MAX_PRESENCE_PAGE_OFFSET, context
      elsif expect.fetch("error_kind") == "out_of_range"
        out_of_range =
          (command.key?("limit") &&
            !command.fetch("limit").between?(1, V1::MAX_PRESENCE_PAGE_SIZE)) ||
          (command.key?("offset") && command.fetch("offset") > V1::MAX_PRESENCE_PAGE_OFFSET)

        assert out_of_range, "#{context}: the gem's page limits must reject it too"
      end
    end
  end

  def test_pinned_presence_messages_stay_inside_the_limits_the_gem_enforces
    seen = 0

    presence_cases("message").each do |context, wire, expect|
      next unless expect.fetch("valid")

      message = JSON.parse(wire)
      seen += 1

      assert_equal V1::VERSION, message.fetch("version", 1), context
      assert_equal V1::PRESENCE_MESSAGE_TYPE, message.fetch("type"), context
      # Events and grant operations are one vocabulary.
      assert_includes V1::PRESENCE_OPERATIONS, message.fetch("event"), context

      if (subject = message["subject_id"])
        assert_equal subject, V1.validate_bounded!("subject_id", subject, V1::MAX_SUBJECT_ID_LEN), context
      end
      if (metadata = message["metadata"])
        assert_operator JSON.generate(metadata).bytesize, :<=, V1::MAX_PRESENCE_METADATA_LEN, context
      end

      members = message.fetch("members", [])
      next if (page = message["page"]).nil?

      assert_operator page.fetch("limit"), :<=, V1::MAX_PRESENCE_PAGE_SIZE, context
      assert_operator members.size, :<=, page.fetch("limit"), context
    end

    assert_operator seen, :>, 0
  end

  # The other half of fail-closed: where a pinned rejection maps onto a check
  # the gem actually has, the gem must reject the same bytes.
  def test_the_gem_refuses_the_values_the_invalid_presence_fixtures_pin
    checked = 0

    (presence_cases("command") + presence_cases("message")).each do |context, wire, expect|
      next if expect.fetch("valid")

      kind = expect.fetch("error_kind")
      if kind == "malformed_json"
        assert_raises(JSON::ParserError, context) { JSON.parse(wire) }
        checked += 1
        next
      end

      envelope = JSON.parse(wire)

      case kind
      when "unsupported_version"
        assert_raises(Orbitcast::ProtocolError, context) do
          V1.validate_version!(envelope.fetch("version"))
        end
      when "empty_stream"
        assert_raises(Orbitcast::ProtocolError, context) do
          V1.validate_stream!(envelope["stream"].to_s)
        end
      when "empty_field"
        subject = envelope["subject_id"] || envelope.dig("members", 0, "subject_id")
        assert_raises(Orbitcast::ProtocolError, context) do
          V1.validate_bounded!("subject_id", subject.to_s, V1::MAX_SUBJECT_ID_LEN)
        end
      when "disallowed_value"
        refute_equal V1::PRESENCE_ACTION, envelope["action"], context
        refute_equal V1::PRESENCE_MESSAGE_TYPE, envelope["type"], context if envelope.key?("type")
      when "unknown_op"
        refute_includes V1::PRESENCE_OPERATIONS, envelope.fetch("op"), context
      when "unknown_event"
        refute_includes V1::PRESENCE_OPERATIONS, envelope.fetch("event"), context
      when "too_many"
        assert_operator envelope.fetch("members").size, :>, envelope.dig("page", "limit"), context
      else
        next # wrong_type / missing_payload: decoder-level, tests/presence.rs owns them
      end

      checked += 1
    end

    assert_operator checked, :>, 0
  end

  # The grant side, byte for byte: the effects the gem collects are the effects
  # the relay is pinned to parse.
  def test_the_collector_reproduces_the_pinned_presence_effects
    reproduced = 0

    each_rpc_case("reply") do |context, wire, expect|
      next unless expect.fetch("valid")

      pinned = JSON.parse(wire).fetch("effects", []).select do |effect|
        [V1::EFFECT_PRESENCE_GRANT, V1::EFFECT_PRESENCE_REVOKE].include?(effect["kind"])
      end
      next if pinned.empty?

      collector = V1::EffectCollector.new
      pinned.each do |effect|
        if effect.fetch("kind") == V1::EFFECT_PRESENCE_GRANT
          collector.presence_grant(
            effect.fetch("identifier"),
            stream: effect.fetch("stream"),
            subject_id: effect.fetch("subject_id"),
            metadata: effect.fetch("metadata"),
            ttl_ms: effect.fetch("ttl_ms"),
            operations: effect.fetch("operations")
          )
        else
          collector.presence_revoke(
            effect.fetch("identifier"),
            stream: effect.fetch("stream"),
            subject_id: effect.fetch("subject_id")
          )
        end
      end

      assert_equal pinned, collector.to_a, "#{context}: ordered, field for field"
      pinned.each do |effect|
        next unless effect.fetch("kind") == V1::EFFECT_PRESENCE_GRANT

        assert_operator effect.fetch("ttl_ms"), :>, 0, context
        effect.fetch("operations").each do |operation|
          assert_includes V1::PRESENCE_OPERATIONS, operation, context
        end
      end
      reproduced += 1
    end

    assert_operator reproduced, :>, 0, "the rpc fixtures must pin presence effects"
  end

  # --- recovery -------------------------------------------------------------
  #
  # tests/fixtures/recovery/*.json pins the recovery contract
  # The gem is the
  # authoritative *producer* of one of those envelopes — the token — and of
  # nothing else: cursors, delivery envelopes, and the resume/ack negotiation
  # are written by the relay and read by the browser helper
  # (gem/app/assets/javascripts/orbitcast/recovery.js), so Ruby asserts the
  # token equivalence and, for the rest, only what it can read.

  def test_recovery_token_fixtures_verify_with_plain_hmac
    seen = 0

    each_recovery_token_case do |context, input, expect|
      encoded, signature, extra = input.fetch("token").split("--")

      assert_nil extra, "#{context}: token must contain exactly one separator"
      assert_match(/\A[0-9a-f]{64}\z/, signature, context)

      recomputed = Orbitcast::Recovery.signature(encoded, input.fetch("secret"))

      assert_equal expect.fetch("signature_valid"), recomputed == signature, context
      seen += 1
    end

    assert_operator seen, :>, 0
  end

  # Claim construction, key order, base64 and HMAC all come from the gem; the
  # pinned inputs must reproduce the checked-in bytes exactly. Mirrors
  # test_gem_regenerates_the_valid_tokens_byte_for_byte on the subscribe side.
  def test_the_gem_regenerates_the_canonical_recovery_tokens_byte_for_byte
    reproduced = 0

    each_recovery_token_case do |context, input, expect|
      next unless expect.fetch("claims_valid")

      pinned_claims = expect.fetch("claims")
      now = input.fetch("now")
      claims = recovery_claims(pinned_claims, now)

      assert_equal pinned_claims, claims, "#{context}: claims, key for key"

      encoded = Base64.strict_encode64(JSON.generate(claims))
      token = "#{encoded}--#{Orbitcast::Recovery.signature(encoded, input.fetch("secret"))}"
      pinned_encoded, pinned_signature = input.fetch("token").split("--")
      pinned_payload = JSON.parse(Base64.strict_decode64(pinned_encoded))

      if pinned_payload != claims
        # A token minted by an older or newer peer: the claims *read* the same
        # (an absent version is 1, unknown claims are ignored) but the bytes
        # are not ours to reproduce.
        refute_equal input.fetch("token"), token, context
        assert_equal pinned_claims.fetch("version"), pinned_payload.fetch("version", 1), context
        next
      end

      if expect.fetch("signature_valid")
        assert_equal input.fetch("token"), token, "#{context}: byte for byte"
        assert_equal expect.fetch("canonical", true), true, context
        reproduced += 1
      else
        # Same claims, another key (or a flipped signature): the gem produces
        # the same encoded half and a different signature. Never the token.
        assert_equal pinned_encoded, encoded, context
        refute_equal pinned_signature, token.split("--").last, context
        refute_equal input.fetch("token"), token, context
      end
    end

    assert_operator reproduced, :>, 0, "the recovery fixtures must carry a canonical token"
  end

  # The refusals: a token the relay rejects on its claims is one the gem either
  # cannot mint at all, or would mint differing only in the field the fixture
  # pins as wrong.
  def test_the_gem_cannot_mint_the_refused_recovery_tokens
    checked = 0

    each_recovery_token_case do |context, input, expect|
      next if expect.fetch("claims_valid")

      pinned_claims = expect.fetch("claims")
      now = input.fetch("now")

      case expect.fetch("error_kind")
      when "empty_field"
        # A token grants exactly what it names, so an empty stream set is
        # refused here rather than minted into something the relay discards.
        assert_empty pinned_claims.fetch("streams"), context
        assert_raises(ArgumentError, context) { recovery_claims(pinned_claims, now) }
      when "expired"
        assert_operator pinned_claims.fetch("expires_at"), :<=, now, context
        # `expires_in` would have to be zero or negative, which the gem refuses.
        assert_raises(Orbitcast::ConfigurationError, context) do
          recovery_claims(pinned_claims, now)
        end

        # What it *would* produce differs only in the expiry.
        live = recovery_claims(pinned_claims.merge("expires_at" => now + 60), now)

        assert_equal pinned_claims.except("expires_at"), live.except("expires_at"), context
        assert_operator live.fetch("expires_at"), :>, now, context
      when "wrong_purpose"
        refute_equal Orbitcast::Recovery::PURPOSE, pinned_claims.fetch("purpose"), context
        minted = recovery_claims(pinned_claims, now)

        assert_equal Orbitcast::Recovery::PURPOSE, minted.fetch("purpose"),
                     "#{context}: the gem signs one purpose and cannot be asked for another"
        assert_equal pinned_claims.except("purpose"), minted.except("purpose"), context
      else
        next
      end

      checked += 1
    end

    assert_operator checked, :>=, 3, "the refusal cases must all be exercised"
  end

  # The recovery key is its own key and the recovery purpose is its own
  # purpose. Neither profile can stand in for the other — not by fallback,
  # not by accident.
  def test_recovery_is_isolated_from_the_subscribe_profile
    refute_equal Orbitcast::Streams::PURPOSE, Orbitcast::Recovery::PURPOSE
    assert_equal "recover", Orbitcast::Recovery::PURPOSE
    assert_equal "subscribe", Orbitcast::Streams::PURPOSE

    # A configured streams secret is not a recovery secret: there is no
    # fallback in either direction.
    streams_only = contract_config

    assert_equal CONTRACT_SECRET, streams_only.streams_secret
    assert_nil streams_only.recovery_secret
    assert_raises(Orbitcast::ConfigurationError) { streams_only.recovery_secret! }
    assert_raises(Orbitcast::ConfigurationError) do
      Orbitcast::Recovery.recovery_token(
        session_id: "session:7", streams: ["chat:42"], config: streams_only
      )
    end

    # And the two keys are exported under their own names.
    both = contract_config
    both.recovery_secret = "recovery-only-secret-32-bytes-000"
    exported = both.exported_env

    assert_equal Base64.strict_encode64(CONTRACT_SECRET), exported.fetch("ORBITCAST_STREAMS_SECRET_BASE64")
    assert_equal(
      Base64.strict_encode64("recovery-only-secret-32-bytes-000"),
      exported.fetch("ORBITCAST_RECOVERY_SECRET_BASE64")
    )
    refute_equal exported.fetch("ORBITCAST_STREAMS_SECRET_BASE64"),
                 exported.fetch("ORBITCAST_RECOVERY_SECRET_BASE64")
  end

  # Cursors are opaque to a client, but the gem must read them the way the
  # relay writes them: a `(generation, sequence)` pair whose numbers travel as
  # decimal strings, because a 64-bit sequence exceeds Number.MAX_SAFE_INTEGER.
  def test_pinned_cursors_decode_to_js_safe_decimal_strings
    seen = 0

    self.class.fixture("recovery/cursors.json").fetch("cases").each do |case_data|
      context = "recovery/cursors.json case #{case_data.fetch("name")}"
      expect = case_data.fetch("expect")
      encoded = case_data.dig("input", "cursor")

      unless expect.fetch("valid")
        assert_cursor_is_unusable(context, expect.fetch("error_kind"), encoded)
        next
      end

      decoded = Base64.strict_decode64(encoded)

      assert_equal expect.fetch("json"), decoded, context if expect.key?("json")

      payload = JSON.parse(decoded)

      assert_equal 1, payload.fetch("v", 1), context
      %w[g s].each do |field|
        assert_kind_of String, payload.fetch(field), "#{context}: #{field} is a decimal string"
      end
      assert_equal expect.fetch("generation"), payload.fetch("g"), context
      assert_equal expect.fetch("sequence"), payload.fetch("s"), context
      # The reason for the strings: the value survives a round trip no JSON
      # number would.
      assert_equal payload.fetch("s"), payload.fetch("s").to_i.to_s, context
      seen += 1
    end

    assert_operator seen, :>, 0
  end

  # --- control envelopes ----------------------------------------------------

  def test_pinned_control_messages_parse_and_validate
    seen = 0

    each_control_case do |context, wire, expect|
      if expect.fetch("valid")
        message = V1::ControlMessage.parse(wire).validate!
        seen += 1

        assert_equal expect.fetch("version"), message.version, context
        assert_equal expect.fetch("kind"), message.kind, context
        assert_equal expect.fetch("namespace"), message.namespace, context
        assert_equal expect.fetch("control_id"), message.control_id, context
        assert_equal expect.fetch("identity_key"), message.identity_key, context
        assert_equal expect.fetch("reason"), message.reason, context
        assert_equal expect.fetch("reconnect"), message.reconnect?, context
        assert message.applies_to?(expect.fetch("applies_to")), context
        refute message.applies_to?("another_deployment"), context
      else
        assert_raises(Orbitcast::ProtocolError, JSON::ParserError, context) do
          V1::ControlMessage.parse(wire).validate!
        end
      end
    end

    assert_operator seen, :>, 0
  end

  def test_the_gem_regenerates_the_pinned_control_messages
    self.class.fixture("control/valid_disconnect.json").fetch("cases").each do |case_data|
      expect = case_data.fetch("expect")
      message = V1::ControlMessage.disconnect(
        namespace: expect.fetch("namespace"),
        identity_key: expect.fetch("identity_key"),
        reason: expect.fetch("reason"),
        reconnect: expect.fetch("reconnect"),
        control_id: expect.fetch("control_id")
      )

      assert_equal(
        JSON.parse(case_data.dig("input", "wire")),
        JSON.parse(message.to_json),
        case_data.fetch("name")
      )
    end
  end

  # --- private ingress acknowledgements ---------------------------------------
  #
  # tests/fixtures/ingress/ack.json pins the `202` body of
  # `POST /v1/publications` (`src/transport/ingress.rs`). Rust is the
  # authoritative validator — tests/contract.rs decodes every case through
  # `IngressAck` — so what the Ruby side owes the contract is narrower and
  # stated here: the acks the relay emits are ones `Publisher::Http` accepts,
  # they echo the id this publisher generated, and the *status code* is the
  # acceptance rule.

  def test_the_ingress_ack_fixture_is_shaped_like_what_the_http_publisher_accepts
    seen = 0

    each_ingress_ack_case do |context, wire, expect|
      next unless expect.fetch("valid")

      seen += 1
      ack = JSON.parse(wire)

      # The two fields the publisher's contract depends on: the version every
      # Orbitcast envelope carries (absent reads as 1), and the echoed id.
      assert_equal expect.fetch("version"), ack.fetch("version", 1), context
      assert_equal expect.fetch("publication_id"), ack.fetch("publication_id"), context
      assert_match UUID_PATTERN, ack.fetch("publication_id"), context

      # And an ack in that shape is accepted, with the id the publisher
      # generated coming back out of `publish`.
      connection = FakeIngressConnection.new(body: wire)
      publication_id = ingress_publisher(connection).publish("chat:42", '{"body":"hi"}')

      assert_match UUID_PATTERN, publication_id, context
      assert_equal(
        publication_id,
        JSON.parse(connection.posts.fetch(0).fetch(:body)).fetch("id"),
        "#{context}: the ack echoes the `id` the publisher sent as `publication_id`"
      )
    end

    assert_operator seen, :>, 0
  end

  def test_the_ingress_status_code_is_the_acceptance_rule_not_the_ack_body
    # A body Rust would reject still rides on whatever status the relay chose,
    # and the publisher deliberately does not re-validate it: re-deriving the
    # decision from the body would let a mangled ack turn an accepted
    # publication into a raised error, and there is no safe retry for that.
    rejected = each_ingress_ack_case.reject { |_, _, expect| expect.fetch("valid") }

    refute_empty rejected

    rejected.each do |context, wire, _expect|
      connection = FakeIngressConnection.new(body: wire)

      assert_match(
        UUID_PATTERN,
        ingress_publisher(connection).publish("chat:42", "{}"),
        "#{context}: a 202 is accepted whatever the body says"
      )
    end

    # The converse: a non-accepted status is an error even with a perfectly
    # valid ack body.
    valid_wire = self.class.fixture("ingress/ack.json")
                     .fetch("cases").first.fetch("input").fetch("wire")

    assert_raises(Orbitcast::IngressError) do
      ingress_publisher(FakeIngressConnection.new(code: "500", body: valid_wire))
        .publish("chat:42", "{}")
    end
  end

  private

  V1 = Orbitcast::Protocol::V1

  UUID_PATTERN = /\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/

  # MOCK: stands in for the pooled Net::HTTP connection in
  # `Orbitcast::Publisher::Http`. Captures the request and replays a canned
  # status plus one of the fixture ack bodies; nothing opens a socket.
  class FakeIngressConnection
    Response = Struct.new(:code, :body)

    attr_reader :posts

    def initialize(code: "202", body: "{}")
      @posts = []
      @code = code
      @body = body
    end

    def post(uri, body, authorization)
      @posts << { uri: uri, body: body, authorization: authorization }
      Response.new(@code, @body)
    end

    def close; end
  end

  def ingress_publisher(connection)
    config = build_config(
      namespace: CONTRACT_NAMESPACE,
      publish_via: :http,
      ingress_url: "http://127.0.0.1:28081/v1/publications",
      ingress_token: "ingress-token"
    )
    Orbitcast::Publisher::Http.new(config, connection_factory: -> { connection })
  end

  # Yields `(context, wire, expect)` per fixture case, or returns them all when
  # called without a block.
  def each_ingress_ack_case
    files = self.class.fixture_files("ingress")

    refute_empty files

    cases = files.flat_map do |file, doc|
      doc.fetch("cases").map do |case_data|
        ["ingress/#{file} case #{case_data.fetch("name")}",
         case_data.fetch("input").fetch("wire"),
         case_data.fetch("expect")]
      end
    end

    return cases unless block_given?

    cases.each { |entry| yield(*entry) }
  end

  # Every field a v1 reply may carry. Anything else in a fixture belongs to a
  # newer peer and is ignored, by rule.
  REPLY_FIELDS = %w[
    version operation outcome request_id sequence new_revision
    state subscription_state identity effects reason describe
  ].freeze

  def each_rpc_case(envelope)
    files = self.class.fixture_files("rpc")

    refute_empty files

    files.each do |file, doc|
      doc.fetch("cases").each do |case_data|
        next unless case_data.fetch("envelope") == envelope

        yield "rpc/#{file} case #{case_data.fetch("name")}",
              case_data.dig("input", "wire"),
              case_data.fetch("expect")
      end
    end
  end

  # Presence and recovery live in their own fixture directories; both carry
  # `envelope`-tagged cases, the same way the rpc group does.
  def presence_cases(envelope)
    files = self.class.fixture_files("presence")

    refute_empty files

    files.flat_map do |file, doc|
      doc.fetch("cases").filter_map do |case_data|
        next unless case_data.fetch("envelope") == envelope

        ["presence/#{file} case #{case_data.fetch("name")}",
         case_data.dig("input", "wire"),
         case_data.fetch("expect")]
      end
    end
  end

  def each_recovery_token_case
    files = self.class.fixture_files("recovery")

    refute_empty files

    files.each do |file, doc|
      doc.fetch("cases").each do |case_data|
        next unless case_data.fetch("envelope") == "token"

        yield "recovery/#{file} case #{case_data.fetch("name")}",
              case_data.fetch("input"),
              case_data.fetch("expect")
      end
    end
  end

  # One `presence_grant` effect, built by the gem's own collector.
  def granted_effect(stream:, subject_id:, metadata:, ttl_ms:, operations:)
    V1::EffectCollector.new.presence_grant(
      '{"channel":"ChatChannel","room":"42"}',
      stream: stream, subject_id: subject_id, metadata: metadata,
      ttl_ms: ttl_ms, operations: operations
    )
  end

  # The claims the gem would sign for a pinned claim set, with string keys.
  def recovery_claims(pinned, now)
    Orbitcast::Recovery.claims(
      session_id: pinned.fetch("session_id"),
      streams: pinned.fetch("streams"),
      expires_in: pinned.fetch("expires_at") - now,
      config: recovery_config(namespace: pinned.fetch("namespace")),
      now: Time.at(now).utc
    ).transform_keys(&:to_s)
  end

  def recovery_config(namespace: CONTRACT_NAMESPACE)
    config = contract_config(namespace: namespace)
    config.recovery_secret = CONTRACT_SECRET
    config
  end

  # A cursor the relay would refuse is one Ruby cannot read either: strict
  # base64, then JSON, then decimal strings, then version 1.
  def assert_cursor_is_unusable(context, kind, encoded)
    case kind
    when "empty_field"
      assert_empty encoded, context
    when "disallowed_value"
      decoded =
        begin
          Base64.strict_decode64(encoded)
        rescue ArgumentError
          return # not base64 at all
        end

      payload =
        begin
          JSON.parse(decoded)
        rescue JSON::ParserError
          return # base64 of something that is not JSON
        end

      # Numbers instead of decimal strings, or a sequence that is not a u64.
      unusable = %w[g s].any? do |field|
        value = payload[field]
        !value.is_a?(String) || !value.match?(/\A\d+\z/)
      end

      assert unusable, "#{context}: the gem must not read this as a position"
    when "unsupported_version"
      payload = JSON.parse(Base64.strict_decode64(encoded))

      assert_raises(Orbitcast::ProtocolError, context) { V1.validate_version!(payload.fetch("v")) }
    end
  end

  def each_control_case
    files = self.class.fixture_files("control")

    refute_empty files

    files.each do |file, doc|
      doc.fetch("cases").each do |case_data|
        yield "control/#{file} case #{case_data.fetch("name")}",
              case_data.dig("input", "wire"),
              case_data.fetch("expect")
      end
    end
  end

  # Build the request a pinned reply answers, then rebuild the reply from it.
  # An accepted stateful operation advances the revision by one, so the request
  # it answers expected one less.
  def build_pinned_reply(pinned, expect)
    operation = pinned.fetch("operation")
    accepted = pinned["outcome"] == V1::OUTCOME_ACCEPTED
    new_revision = pinned.fetch("new_revision", 0)
    stateful = V1::STATEFUL_OPERATIONS.include?(operation)
    expected_revision = accepted && stateful ? new_revision - 1 : new_revision

    request = V1::Request.from(
      "version" => 1,
      "operation" => operation,
      "namespace" => CONTRACT_NAMESPACE,
      "node_id" => "node-7",
      "session_id" => "2f4a6c8e-1b3d-4f5a-9c7e-0d1a2b3c4d5e",
      "request_id" => pinned.fetch("request_id"),
      "sequence" => pinned.fetch("sequence", 0),
      "expected_revision" => expected_revision,
      "deadline_ms" => 1000
    )

    V1::Reply.new(
      request: request,
      outcome: pinned.fetch("outcome", V1::OUTCOME_FAULT),
      effects: pinned.fetch("effects", []),
      state: pinned.key?("state") ? pinned["state"] : V1::Reply::UNCHANGED,
      subscription_state: pinned.key?("subscription_state") ? pinned["subscription_state"] : V1::Reply::UNCHANGED,
      identity: pinned["identity"],
      reason: pinned["reason"],
      describe: pinned["describe"]
    )
  end

  def assert_pinned_connection(context, request, expected)
    return if expected.nil?

    assert_equal expected.fetch("url"), request.connection.url, "#{context}: url"
    if expected["origin"].nil?
      assert_nil request.connection.origin, "#{context}: origin"
    else
      assert_equal expected["origin"], request.connection.origin, "#{context}: origin"
    end
    assert_equal expected.fetch("client_address"), request.connection.client_address, context
    return unless expected.key?("headers")

    assert_equal expected.fetch("headers"), request.connection.headers, "#{context}: headers"
  end

  def assert_pinned_subscription(context, request, expected)
    return if expected.nil?

    assert_equal expected.fetch("identifier"), request.subscription.identifier, context
    assert_equal expected.fetch("params"), request.subscription.params, "#{context}: params"
    assert_equal expected.fetch("streams"), request.subscription.streams, "#{context}: streams"
  end

  def assert_pinned_action(context, request, expected)
    return if expected.nil?

    assert_equal expected.fetch("identifier"), request.action.identifier, context
    assert_equal expected.fetch("json_data"), request.action.json_data,
                 "#{context}: the client's encoded data, byte for byte"
  end

  def assert_pinned_release(context, request, expected)
    return if expected.nil?

    assert_equal expected.fetch("reason"), request.release.reason, context
    assert_equal expected.fetch("reconnect"), request.release.reconnect?, context
    assert_equal expected.fetch("subscription_count"), request.release.subscriptions.size, context
  end

  # The fixtures name a rejection kind; Ruby's errors carry the field that
  # failed. This maps the ones a Ruby consumer can be held to.
  def assert_rejection_kind(context, kind, error)
    expected_field =
      case kind
      when "missing_payload" then %w[connection subscription action release]
      when "disallowed_value" then %w[header_name reason]
      when "out_of_range" then %w[deadline_ms ttl_ms]
      when "unsupported_version" then %w[version]
      when "invalid_uuid" then %w[session_id request_id control_id]
      end

    return if expected_field.nil?

    assert_includes expected_field, error.field, "#{context}: #{kind} (#{error.message})"
  end

  def contract_config(namespace: CONTRACT_NAMESPACE)
    build_config(
      namespace: namespace,
      streams_secret: CONTRACT_SECRET,
      database_url: "postgres://cable@localhost/orbitcast_contract"
    )
  end

  def build_publisher(namespace: CONTRACT_NAMESPACE)
    connection = FakeConnection.new
    publisher = Orbitcast::Publisher::Postgres.new(
      contract_config(namespace: namespace), connection_factory: -> { connection }
    )
    [publisher, connection]
  end

  def turbo_fixture
    self.class.fixture("signed_streams/turbo/turbo_real.json")
  end

  def each_turbo_case
    cases = turbo_fixture.fetch("cases")

    refute_empty cases

    cases.each do |case_data|
      yield "turbo/turbo_real.json case #{case_data.fetch("name")}",
            case_data.fetch("input"),
            case_data.fetch("expect"),
            case_data
    end
  end

  # The verifier's key, whichever encoding the case carries it in.
  def fixture_secret(input)
    return Base64.strict_decode64(input.fetch("secret_base64")) if input.key?("secret_base64")

    input.fetch("secret")
  end

  # The verifier's namespace: explicit in the newer files, CONTRACT_NAMESPACE
  # by convention in the older ones.
  def verifier_namespace(input)
    input.fetch("namespace", CONTRACT_NAMESPACE)
  end

  def each_signed_stream_case
    files = self.class.fixture_files("signed_streams")

    refute_empty files

    files.each do |file, doc|
      doc.fetch("cases").each do |case_data|
        yield "signed_streams/#{file} case #{case_data.fetch("name")}",
              case_data.fetch("input"),
              case_data.fetch("expect")
      end
    end
  end

  # Publish the fixture's (namespace, stream, payload) through the real
  # publisher and compare the emitted notice with the fixture wire. The
  # publication id is the only nondeterministic field; it is asserted to be a
  # UUID and normalized before comparison.
  def assert_emitted_notice_matches(case_data, expected_notice)
    name = case_data.fetch("name")
    expect = case_data.fetch("expect")
    publisher, connection = build_publisher(namespace: expect.fetch("namespace"))

    publication_id = publisher.publish(
      expect.fetch("stream"), expected_notice.dig("publication", "json_payload")
    )

    assert_match(/\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/, publication_id, name)
    assert_equal 1, connection.calls.size, name

    sql, (channel, wire) = connection.calls.first

    assert_equal "SELECT pg_notify($1, $2)", sql, name
    assert_equal expect.fetch("channel"), channel, name

    emitted = JSON.parse(wire)

    assert_equal publication_id, emitted.dig("publication", "id"), name
    emitted["publication"]["id"] = expected_notice.dig("publication", "id")

    assert_equal expected_notice, emitted, name
  end
end
