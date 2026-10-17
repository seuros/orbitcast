#!/usr/bin/env ruby
# frozen_string_literal: true

# Exports the signed-stream golden fixtures in tests/fixtures/signed_streams/,
# consumed by BOTH tests/contract.rs and gem/test/contract_test.rb: Ruby is
# the producer of signing fixtures.
#
# Tokens are built with the actual gem code — claim construction and key
# order (Orbitcast::Streams.claims), strict base64, and HMAC-SHA256
# (Orbitcast::Streams.signature) — pinned to fixed inputs so the output is
# deterministic. Re-running this script must produce a zero diff.
#
#   ruby gem/test/dummy/script/export_signed_stream_fixtures.rb
#
# Plain Ruby: no Rails boot, no ActionCable, no PostgreSQL.
#
# ## Fixture shape (every file in tests/fixtures/signed_streams/*.json)
#
#   {
#     "description": "...",
#     "cases": [
#       {
#         "name": "snake_case_case_name",
#         "input": {
#           "token": "<base64(JSON(claims))>--<hex HMAC-SHA256>",
#           "secret": "<verifier key as UTF-8 text>",            // OR
#           "secret_base64": "<verifier key as strict base64>",  // raw bytes; decode, never UTF-8
#           "now": <unix seconds the verifier's clock is pinned to>,
#           "namespace": "<verifier's configured namespace>"     // optional; absent = "example_production"
#         },
#         "expect": {
#           "valid": bool,            // the whole token is accepted
#           "signature_valid": bool,  // HMAC over the encoded half, with input's key
#           "claims_valid": bool,     // every authorization-bearing claim passes
#           "error_kind": "expired" | "wrong_purpose" | "bad_signature" |
#                         "wrong_namespace" | "missing_claim",   // only when !valid
#           "missing": "namespace" | "stream" | "purpose" | "expires_at",  // only for missing_claim
#           "claims": { ...the known claims as the token carries them... },
#                                     // an absent claim is absent here too; an absent
#                                     // version reads as 1; "whisper": true only when
#                                     // the token grants whisper (absent = false)
#           "stream": "<the stream a valid token grants>"        // only when valid
#         }
#       }
#     ]
#   }
#
# Files written before the shape grew `secret_base64`, `namespace`,
# `wrong_namespace`, `missing_claim`, `whisper` keep their exact bytes; the
# additions are only ever used by newer files, so a consumer of the old files
# is unaffected.

require "base64"
require "fileutils"
require "json"

GEM_LIB = File.expand_path("../../../lib", __dir__)
$LOAD_PATH.unshift(GEM_LIB) unless $LOAD_PATH.include?(GEM_LIB)

require "orbitcast"

FIXTURES_DIR = File.expand_path("../../../../tests/fixtures/signed_streams", __dir__)

# Pinned inputs. Changing any of these changes checked-in golden files, which
# is a contract change, not a refactor.
SECRET = "test-secret-32-bytes-padded-000000"
WRONG_SECRET = "wrong-secret-32-bytes-padded-00000"
NAMESPACE = "example_production"
STREAM = "chat:42"
UNICODE_STREAM = "stream:ünïcode/🚀"
NOW = 1_760_000_000        # pinned verification clock: 2025-10-09T08:53:20Z
FUTURE = 4_102_444_800     # 2100-01-01T00:00:00Z
EXPIRED = 1_000_000_000    # 2001-09-09T01:46:40Z

def config
  @config ||= begin
    config = Orbitcast::Configuration.new
    config.assign(
      namespace: NAMESPACE,
      database_url: "postgres://cable@localhost/orbitcast_contract",
      streams_secret: SECRET
    )
    config
  end
end

# Claims exactly as the gem emits them (fixed key order:
# version, namespace, stream, purpose, expires_at[, whisper]).
def gem_claims(stream, expires_at:, whisper: false)
  Orbitcast::Streams.claims(
    stream,
    expires_in: expires_at - NOW,
    whisper: whisper,
    config: config,
    now: Time.at(NOW).utc
  )
end

# `Orbitcast::Streams.signed_stream_name` reads the wall clock, so fixtures
# assemble the token from the same primitives with a pinned clock instead.
def token_for(claims, secret = SECRET)
  encoded = Base64.strict_encode64(JSON.generate(claims))
  "#{encoded}--#{Orbitcast::Streams.signature(encoded, secret)}"
end

def build_case(name, token:, expect:, secret: SECRET, now: NOW, namespace: nil, secret_base64: nil)
  input = { token: token }
  if secret_base64
    input[:secret_base64] = secret_base64
  else
    input[:secret] = secret
  end
  input[:now] = now
  input[:namespace] = namespace if namespace
  { name: name, input: input, expect: expect }
end

def expected_claims(claims)
  { version: 1, purpose: "subscribe" }.merge(claims)
end

def write_fixture(file, description, cases)
  path = File.join(FIXTURES_DIR, file)
  json = JSON.pretty_generate(description: description, cases: cases)
  File.write(path, "#{json}\n")
  puts "wrote #{path}"
end

FileUtils.mkdir_p(FIXTURES_DIR)

valid = gem_claims(STREAM, expires_at: FUTURE)
valid_unicode = gem_claims(UNICODE_STREAM, expires_at: FUTURE)

write_fixture(
  "valid_claims.json",
  "Well-formed Orbitcast-profile signed stream tokens: base64(JSON(claims)) " \
  "+ \"--\" + hex(HMAC-SHA256(secret, encoded)). Signature and claims are " \
  "both valid at input.now.",
  [
    build_case(
      "valid_subscribe_token",
      token: token_for(valid),
      expect: {
        valid: true,
        signature_valid: true,
        claims_valid: true,
        claims: valid,
        stream: STREAM
      }
    ),
    build_case(
      "valid_unicode_stream",
      token: token_for(valid_unicode),
      expect: {
        valid: true,
        signature_valid: true,
        claims_valid: true,
        claims: valid_unicode,
        stream: UNICODE_STREAM
      }
    )
  ]
)

expired = { version: 1, namespace: NAMESPACE, stream: STREAM,
            purpose: "subscribe", expires_at: EXPIRED }

write_fixture(
  "expired_claims.json",
  "Correctly signed token whose claims expired before input.now. The " \
  "signature authenticates, but semantic validation must fail closed.",
  [
    build_case(
      "expired_token",
      token: token_for(expired),
      expect: {
        valid: false,
        signature_valid: true,
        claims_valid: false,
        error_kind: "expired",
        claims: expired
      }
    )
  ]
)

wrong_purpose = { version: 1, namespace: NAMESPACE, stream: STREAM,
                  purpose: "publish", expires_at: FUTURE }

write_fixture(
  "wrong_purpose.json",
  "Correctly signed token whose purpose is not \"subscribe\". A subscribe " \
  "verifier must reject it; signed read access never widens into another " \
  "purpose.",
  [
    build_case(
      "purpose_publish",
      token: token_for(wrong_purpose),
      expect: {
        valid: false,
        signature_valid: true,
        claims_valid: false,
        error_kind: "wrong_purpose",
        claims: wrong_purpose
      }
    )
  ]
)

# Valid encoded claims, but the appended signature was computed over different
# claims (stream chat:1), so it does not authenticate the encoded bytes.
forged_source = { version: 1, namespace: NAMESPACE, stream: "chat:1",
                  purpose: "subscribe", expires_at: FUTURE }
forged_signature = token_for(forged_source).split("--").last
tampered = "#{Base64.strict_encode64(JSON.generate(valid))}--#{forged_signature}"

write_fixture(
  "tampered_signature.json",
  "The encoded claims are semantically valid, but the signature was taken " \
  "from a different token. HMAC verification over the encoded bytes must " \
  "fail before the claims are ever interpreted.",
  [
    build_case(
      "signature_from_other_claims",
      token: tampered,
      expect: {
        valid: false,
        signature_valid: false,
        claims_valid: true,
        error_kind: "bad_signature",
        claims: valid
      }
    )
  ]
)

write_fixture(
  "wrong_secret.json",
  "Token signed with a different secret than the verifier's (input.secret). " \
  "The claims themselves are valid; only the key is wrong.",
  [
    build_case(
      "signed_with_wrong_secret",
      token: token_for(valid, WRONG_SECRET),
      expect: {
        valid: false,
        signature_valid: false,
        claims_valid: true,
        error_kind: "bad_signature",
        claims: valid
      }
    )
  ]
)

extra = valid.merge(issued_at: NOW, region: "eu-west-1")

write_fixture(
  "unknown_extra_claim_fields.json",
  "Claims carrying unknown additive fields. Verifiers must ignore them " \
  "(normal tolerant parsing) while still validating every known " \
  "authorization-bearing claim. expect.claims lists only the known fields.",
  [
    build_case(
      "extra_fields_tolerated",
      token: token_for(extra),
      expect: {
        valid: true,
        signature_valid: true,
        claims_valid: true,
        claims: valid,
        stream: STREAM
      }
    )
  ]
)

no_version = { namespace: NAMESPACE, stream: STREAM,
               purpose: "subscribe", expires_at: FUTURE }

write_fixture(
  "absent_version_defaults.json",
  "Claims without a version field. Tolerant readers default it to 1 " \
  "(serializers always emit it); everything else validates normally.",
  [
    build_case(
      "absent_version_reads_as_v1",
      token: token_for(no_version),
      expect: {
        valid: true,
        signature_valid: true,
        claims_valid: true,
        claims: expected_claims(no_version),
        stream: STREAM
      }
    )
  ]
)

# --- namespace isolation ----------------------------------------------------------

OTHER_NAMESPACE = "other_tenant"
other_tenant = { version: 1, namespace: OTHER_NAMESPACE, stream: STREAM,
                 purpose: "subscribe", expires_at: FUTURE }

write_fixture(
  "namespace_mismatch.json",
  "Correctly signed token issued for another namespace (claims.namespace) " \
  "than the verifier's (input.namespace). Even with a shared key, a token " \
  "never crosses namespaces: claims_valid is false, error_kind " \
  "wrong_namespace.",
  [
    build_case(
      "token_for_other_namespace",
      token: token_for(other_tenant),
      namespace: NAMESPACE,
      expect: {
        valid: false,
        signature_valid: true,
        claims_valid: false,
        error_kind: "wrong_namespace",
        claims: other_tenant
      }
    ),
    build_case(
      "same_token_under_its_own_namespace",
      token: token_for(other_tenant),
      namespace: OTHER_NAMESPACE,
      expect: {
        valid: true,
        signature_valid: true,
        claims_valid: true,
        claims: other_tenant,
        stream: STREAM
      }
    )
  ]
)

# --- absent claims fail closed ---------------------------------------------------------

full = { version: 1, namespace: NAMESPACE, stream: STREAM, purpose: "subscribe", expires_at: FUTURE }

write_fixture(
  "absent_claims.json",
  "Correctly signed tokens each missing ONE authorization-bearing claim. " \
  "A verifier must fail closed on the absence (error_kind missing_claim, " \
  "expect.missing names the field) rather than default it to something " \
  "permissive: no namespace is not every namespace, no expiry is not never.",
  %i[namespace stream purpose expires_at].map do |field|
    claims = full.reject { |key, _| key == field }
    build_case(
      "absent_#{field}",
      token: token_for(claims),
      expect: {
        valid: false,
        signature_valid: true,
        claims_valid: false,
        error_kind: "missing_claim",
        missing: field.to_s,
        claims: claims
      }
    )
  end
)

# --- binary key material ---------------------------------------------------------------

# 32 fixed, random-looking bytes that are NOT valid UTF-8 (0xc3 0x28, 0xa0 0xa1,
# 0xf0 0x28 ...): the only correct way to hand this key to a verifier is as
# strict base64 decoded back to bytes (ORBITCAST_STREAMS_SECRET_BASE64).
BINARY_SECRET = [
  0x00, 0xff, 0x10, 0x80, 0xc3, 0x28, 0xa0, 0xa1,
  0xe2, 0x28, 0xa1, 0xf0, 0x90, 0x28, 0xbc, 0x7f,
  0x01, 0x02, 0xfe, 0xfd, 0x9d, 0x6b, 0x33, 0x4c,
  0xd5, 0x8e, 0x77, 0x2a, 0xb9, 0x40, 0x5c, 0x11
].pack("C*")
BINARY_SECRET_BASE64 = Base64.strict_encode64(BINARY_SECRET)

write_fixture(
  "binary_secret.json",
  "Orbitcast-profile tokens keyed with 32 raw bytes that are not valid " \
  "UTF-8, carried as input.secret_base64 (there is no input.secret). The " \
  "verifier must HMAC with the decoded bytes; keying with any text " \
  "re-interpretation of them, or with the text secret the other fixtures " \
  "use, must fail.",
  [
    build_case(
      "valid_with_binary_secret",
      token: token_for(valid, BINARY_SECRET),
      secret_base64: BINARY_SECRET_BASE64,
      expect: {
        valid: true,
        signature_valid: true,
        claims_valid: true,
        claims: valid,
        stream: STREAM
      }
    ),
    build_case(
      "valid_unicode_stream_with_binary_secret",
      token: token_for(valid_unicode, BINARY_SECRET),
      secret_base64: BINARY_SECRET_BASE64,
      expect: {
        valid: true,
        signature_valid: true,
        claims_valid: true,
        claims: valid_unicode,
        stream: UNICODE_STREAM
      }
    ),
    build_case(
      "text_secret_token_under_binary_secret",
      token: token_for(valid),
      secret_base64: BINARY_SECRET_BASE64,
      expect: {
        valid: false,
        signature_valid: false,
        claims_valid: true,
        error_kind: "bad_signature",
        claims: valid
      }
    )
  ]
)

# --- whisper capability ----------------------------------------------------------

# `whisper: true` is the one additive claim the gem emits, and only on request.
# A verifier grants whisper from this claim alone; splicing it into another
# token's encoded half invalidates that token's signature.
whisper_granted = gem_claims(STREAM, expires_at: FUTURE, whisper: true)
whisper_signature = token_for(whisper_granted).split("--").last
spliced_whisper = "#{Base64.strict_encode64(JSON.generate(whisper_granted))}--#{token_for(valid).split("--").last}"

write_fixture(
  "whisper_claim.json",
  "The optional whisper capability: signed read access does not grant " \
  "publication. A token carrying whisper: true grants whisper on its " \
  "stream and nothing else; a plain token (no whisper claim) grants none. " \
  "Copying the claim onto a token signed without it breaks that token's " \
  "signature.",
  [
    build_case(
      "whisper_granted",
      token: token_for(whisper_granted),
      expect: {
        valid: true,
        signature_valid: true,
        claims_valid: true,
        claims: whisper_granted,
        stream: STREAM
      }
    ),
    build_case(
      "plain_token_grants_no_whisper",
      token: token_for(valid),
      expect: {
        valid: true,
        signature_valid: true,
        claims_valid: true,
        claims: valid,
        stream: STREAM
      }
    ),
    build_case(
      "whisper_spliced_onto_a_read_only_signature",
      token: spliced_whisper,
      expect: {
        valid: false,
        signature_valid: false,
        claims_valid: true,
        error_kind: "bad_signature",
        claims: whisper_granted
      }
    )
  ]
)
raise "whisper token must differ from the plain one" if whisper_signature == token_for(valid).split("--").last
