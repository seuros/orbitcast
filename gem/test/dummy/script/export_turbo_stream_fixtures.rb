#!/usr/bin/env ruby
# frozen_string_literal: true

# Exports the REAL turbo-rails signing fixtures consumed by both language test
# suites.
#
#   cd gem/test/dummy && bin/rails runner script/export_turbo_stream_fixtures.rb
#
# Unlike the Orbitcast-profile exporter next to it, this script boots the dummy
# Rails application and calls turbo-rails' own code:
#
#   Turbo::StreamsChannel.signed_stream_name(streamables)
#     -> Turbo.signed_stream_verifier.generate(stream_name_from(streamables))
#     -> ActiveSupport::MessageVerifier.new(key, digest: "SHA256", serializer: JSON)
#
# Nothing here re-implements the format. The fixture pins whatever real
# turbo-rails produces; if the Rust verifier disagrees, the case records
# "rust_status": "mismatch" and Rust is NOT changed to match a guess.
#
# Determinism: the verifier key is pinned (see PINNED_KEY below) and
# MessageVerifier.generate embeds no timestamp when no purpose/expiry is given,
# so re-running the script produces a zero diff. The metadata-envelope file
# pins its expiry to a fixed instant for the same reason.
#
# ## Fixture shape
#
# Two files, one document shape (turbo_real.json additionally carries
# `rendered_page`):
#
#   {
#     "description": "...", "producer": {...}, "rust_contract": {...},
#     "cases": [
#       {
#         "name": "...", "description": "...",
#         "input": {
#           "key_base64": "<Turbo verifier key, raw bytes as strict base64>",
#           "stream_arg": { "kind": "array"|"string", "value": ... },   // what Ruby signed
#           "signed_stream_name": "<encoded>--<hex HMAC-SHA256>",
#           "identifier": "<ActionCable identifier JSON carrying it>"
#         },
#         "expect": {
#           "stream": "<stream string>" | null,  // null: the Turbo profile must REJECT
#           "rejected": true,                    // only on rejection cases
#           "error_kind": "metadata_envelope",   // only on rejection cases
#           "encoded_payload": "...", "payload_json": "<decoded UTF-8 text>",
#           "signature": "...", "signature_digest": "SHA256",
#           "base64_alphabet": "standard", "base64_padded": bool, "serializer": "JSON"
#         },
#         "rust_status": "match" | "mismatch",  // whether src/streams.rs agrees with expect
#         "rust_note": "..."                     // only on mismatch
#       }
#     ]
#   }

require "base64"
require "fileutils"
require "json"

abort "run me through bin/rails runner from gem/test/dummy" unless defined?(Rails) && Rails.application

FIXTURES_DIR = File.expand_path("../../../../tests/fixtures/signed_streams/turbo", __dir__)
FIXTURE_FILE = "turbo_real.json"
METADATA_FIXTURE_FILE = "turbo_metadata_envelope.json"

# Pinned Turbo verifier key. In a real application this is
# `Rails.application.key_generator.generate_key("turbo/signed_stream_verifier_key")`
# — raw binary key material, NOT secret_key_base — which is why it is exported
# to Rust as strict base64 (ORBITCAST_TURBO_SECRET_BASE64) and decoded back to
# bytes there. This fixture deliberately includes non-UTF-8 bytes in the key to
# prove the byte path.
PINNED_KEY = ([0x00, 0xff, 0x10, 0x80] + "turbo-fixture-key-32-bytes-0".bytes).pack("C*")

Turbo.signed_stream_verifier_key = PINNED_KEY
Turbo.instance_variable_set(:@signed_stream_verifier, nil)

KEY_BASE64 = Base64.strict_encode64(PINNED_KEY)

# What the Rust verifier does with a Turbo token, read off src/streams.rs
# (MessageVerifier::verify):
#
#   1. reject tokens longer than MAX_TOKEN_LEN (8 KiB)
#   2. split once on "--"; both halves must be non-empty
#   3. HMAC-SHA256 over the ENCODED bytes, compared against lowercase hex
#   4. base64::engine::general_purpose::STANDARD decode (padded, non-URL-safe)
#   5. String::from_utf8
#   6. serde_json::from_str::<String> — the payload must be a JSON *string*
#   7. reject an empty stream
#
# This mirror exists only to classify each case; it never produces the token.
MAX_TOKEN_LEN = 8 * 1024
STANDARD_BASE64 = /\A[A-Za-z0-9+\/]+={0,2}\z/

def rust_verdict(token, expected_stream)
  return ["mismatch", "token exceeds the Rust MAX_TOKEN_LEN of #{MAX_TOKEN_LEN} bytes"] if token.bytesize > MAX_TOKEN_LEN

  encoded, signature, extra = token.split("--")
  return ["mismatch", "token does not split into exactly two halves on '--'"] unless extra.nil?
  return ["mismatch", "empty half in payload--signature"] if encoded.to_s.empty? || signature.to_s.empty?
  unless signature.match?(/\A[0-9a-f]{64}\z/)
    return ["mismatch", "signature is not 64 lowercase hex characters: #{signature.inspect}"]
  end
  unless encoded.match?(STANDARD_BASE64)
    return ["mismatch", "payload is not standard (non-URL-safe) base64: #{encoded.inspect}"]
  end

  decoded = begin
    Base64.strict_decode64(encoded)
  rescue ArgumentError => e
    return ["mismatch", "strict base64 decode failed: #{e.message}"]
  end

  unless decoded.dup.force_encoding(Encoding::UTF_8).valid_encoding?
    return ["mismatch", "decoded payload is not valid UTF-8"]
  end

  parsed = begin
    JSON.parse(decoded)
  rescue JSON::ParserError => e
    return ["mismatch", "decoded payload is not JSON: #{e.message}"]
  end

  unless parsed.is_a?(String)
    return ["mismatch", "decoded payload is #{parsed.class}, Rust expects a JSON string"]
  end
  return ["mismatch", "Rust rejects an empty stream"] if parsed.empty?
  unless parsed == expected_stream
    return ["mismatch", "decoded stream #{parsed.inspect} != expected #{expected_stream.inspect}"]
  end

  ["match", nil]
end

def build_case(name, description:, streamables:, stream_arg:)
  signed = Turbo::StreamsChannel.signed_stream_name(streamables)
  expected = Turbo.signed_stream_verifier.verified(signed)
  encoded, signature = signed.split("--")
  status, note = rust_verdict(signed, expected)

  {
    name: name,
    description: description,
    input: {
      key_base64: KEY_BASE64,
      stream_arg: stream_arg,
      signed_stream_name: signed,
      identifier: JSON.generate(channel: "Turbo::StreamsChannel", signed_stream_name: signed)
    },
    expect: {
      stream: expected,
      encoded_payload: encoded,
      # Base64 decoding yields ASCII-8BIT; the payload is UTF-8 JSON text and
      # must be written to the fixture as such.
      payload_json: Base64.strict_decode64(encoded).force_encoding(Encoding::UTF_8),
      signature: signature,
      signature_digest: "SHA256",
      base64_alphabet: "standard",
      base64_padded: encoded.include?("="),
      serializer: "JSON"
    },
    rust_status: status,
    rust_note: note
  }.compact
end

cases = [
  build_case(
    "turbo_stream_from_demo",
    description: 'Exactly what `turbo_stream_from "demo"` puts in the page: ' \
                 "Turbo::StreamsChannel.signed_stream_name([\"demo\"]).",
    streamables: ["demo"],
    stream_arg: { kind: "array", value: ["demo"] }
  ),
  build_case(
    "plain_string_streamable",
    description: "The single-string form, signed_stream_name(\"demo\"). Turbo " \
                 "joins arrays with ':' but a bare string is passed through, so " \
                 "this must equal the one-element array case.",
    streamables: "demo",
    stream_arg: { kind: "string", value: "demo" }
  ),
  build_case(
    "joined_streamables_padded_base64",
    description: "Multiple streamables are joined with ':'. The payload length " \
                 "forces base64 '=' padding, which a URL-safe/unpadded decoder " \
                 "would reject — Rust decodes with the padded standard alphabet.",
    streamables: ["account", 5, :entries],
    stream_arg: { kind: "array", value: ["account", 5, "entries"] }
  ),
  build_case(
    "unicode_streamable",
    description: "Non-ASCII stream name. turbo-rails' JSON serializer emits raw " \
                 "UTF-8 (no \\u escapes); Rust decodes base64 -> UTF-8 -> JSON string.",
    streamables: ["room:ünïcode/🚀"],
    stream_arg: { kind: "array", value: ["room:ünïcode/🚀"] }
  ),
  build_case(
    "quote_and_backslash_streamable",
    description: "Stream name containing characters the JSON serializer must " \
                 "escape. The escaped bytes are what the HMAC covers.",
    streamables: ['say "hi"\\now'],
    stream_arg: { kind: "array", value: ['say "hi"\\now'] }
  )
]

# The same token, as produced by rendering the real page through the real view
# helper. Proves the exporter is not signing something the application never
# emits.
rendered = ApplicationController.render(template: "demo/show", layout: false)
rendered_name = rendered[/signed-stream-name="([^"]+)"/, 1]
rendered_channel = rendered[/channel="([^"]+)"/, 1]

raise "demo page rendered no <turbo-cable-stream-source>" if rendered_name.nil?

demo_case = cases.fetch(0)
unless rendered_name == demo_case.dig(:input, :signed_stream_name)
  raise "rendered signed-stream-name does not match signed_stream_name([\"demo\"])"
end

document = {
  description: "REAL turbo-rails signed stream names, produced by the dummy " \
               "Rails application (gem/test/dummy) through " \
               "Turbo::StreamsChannel.signed_stream_name. Format: " \
               "base64_standard(JSON(stream_string)) + \"--\" + " \
               "hex(HMAC-SHA256(key, encoded)). The key is raw bytes exported " \
               "as strict base64 in input.key_base64; Rust must decode it to " \
               "bytes (ORBITCAST_TURBO_SECRET_BASE64), never treat it as text. " \
               "`rust_status` records whether src/streams.rs MessageVerifier " \
               "accepts the case as-is.",
  producer: {
    script: "gem/test/dummy/script/export_turbo_stream_fixtures.rb",
    turbo_rails_version: Gem.loaded_specs.fetch("turbo-rails").version.to_s,
    rails_version: Rails.version,
    verifier: "ActiveSupport::MessageVerifier.new(key, digest: \"SHA256\", serializer: JSON)",
    key_source: "Rails.application.key_generator.generate_key(\"turbo/signed_stream_verifier_key\") " \
                "in a real application; pinned raw bytes here"
  },
  # What the Rust contract test (tests/contract.rs, owned by the Rust side)
  # must assert for every case in this file. Everything it needs is in the
  # fixture: the key as base64, the token, and the expected stream.
  rust_contract: {
    key: "base64-decode input.key_base64 to raw bytes; key it into SecretBytes/MessageVerifier",
    accept: "MessageVerifier::new(key).verify(input.signed_stream_name) == Ok(expect.stream)",
    controller:
      "StreamsController::new(StreamsConfig { turbo: true, turbo_secret: Some(key), .. }, namespace) " \
      ".handles(input.identifier) is true and .resolve(input.identifier) yields " \
      "StreamResult { stream: expect.stream, whisper: false, presence: false }",
    negative:
      "flipping one hex character of expect.signature must yield VerifyError::InvalidSignature; " \
      "verifying with a different key must not fall back to the Orbitcast claims secret",
    mismatch_rule:
      "a case with \"rust_status\": \"mismatch\" documents real turbo-rails output the current " \
      "verifier does NOT accept; assert the documented rejection, do not silently loosen the verifier"
  },
  rendered_page: {
    template: "demo/show",
    channel: rendered_channel,
    signed_stream_name: rendered_name,
    element: rendered[/<turbo-cable-stream-source[^>]*>/]
  },
  cases: cases
}

FileUtils.mkdir_p(FIXTURES_DIR)
path = File.join(FIXTURES_DIR, FIXTURE_FILE)
File.write(path, "#{JSON.pretty_generate(document)}\n")

mismatches = cases.select { |c| c.fetch(:rust_status) != "match" }
puts "wrote #{path}"
puts "cases: #{cases.size}, rust mismatches: #{mismatches.size}"
mismatches.each { |c| puts "  MISMATCH #{c.fetch(:name)}: #{c[:rust_note]}" }

# --- `_rails` metadata envelopes: real verifier output the Turbo profile must REJECT -------
#
# `ActiveSupport::MessageVerifier#generate` with a purpose or expiry wraps the
# serialized value in `{"_rails":{"data":...,"exp":...,"pur":...}}`. Turbo
# never does this for stream names (it calls `generate` bare), so a token whose
# payload is that envelope is not a Turbo stream name even though its HMAC is
# genuine. Unsupported Turbo verifier metadata is rejected rather than
# treating it as a valid stream string; never Marshal, never interpret the
# envelope. `expect.turbo_rails_verified` records what turbo-rails' own
# verifier returns for the token (nil for a purposed envelope; the unwrapped
# string for an unexpired purpose-less one) — the relay rejects both.
PINNED_EXPIRY = Time.utc(2100, 1, 1)

def build_rejection_case(name, description:, token:, stream_arg:)
  encoded, signature = token.split("--")
  payload = Base64.strict_decode64(encoded).force_encoding(Encoding::UTF_8)
  # What turbo-rails' own verifier (no purpose) makes of it; recorded, so the
  # fixture states where the relay is deliberately stricter than Rails.
  turbo_verdict = Turbo.signed_stream_verifier.verified(token)
  # The Rust mirror must classify it as "not a JSON string" => rejection.
  status, note = rust_verdict(token, "")
  rejection_predicted = status == "mismatch" && note.to_s.include?("Rust expects a JSON string")

  {
    name: name,
    description: description,
    input: {
      key_base64: KEY_BASE64,
      stream_arg: stream_arg,
      signed_stream_name: token,
      identifier: JSON.generate(channel: "Turbo::StreamsChannel", signed_stream_name: token)
    },
    expect: {
      stream: nil,
      rejected: true,
      error_kind: "metadata_envelope",
      turbo_rails_verified: turbo_verdict,
      encoded_payload: encoded,
      payload_json: payload,
      signature: signature,
      signature_digest: "SHA256",
      base64_alphabet: "standard",
      base64_padded: encoded.include?("="),
      serializer: "JSON"
    },
    rust_status: rejection_predicted ? "match" : "mismatch",
    rust_note: rejection_predicted ? nil : "the Rust mirror did not classify this as a rejected envelope: #{note}"
  }.compact
end

verifier = Turbo.signed_stream_verifier
rejection_cases = [
  build_rejection_case(
    "purpose_metadata_envelope",
    description: "MessageVerifier#generate(\"demo\", purpose: :orbitcast_fixture): the payload is " \
                 "a {\"_rails\":{...}} object, not a JSON string. Correctly signed with the " \
                 "Turbo key, and still not a stream name.",
    token: verifier.generate("demo", purpose: :orbitcast_fixture),
    stream_arg: { kind: "string", value: "demo", purpose: "orbitcast_fixture" }
  ),
  build_rejection_case(
    "expiry_metadata_envelope",
    description: "MessageVerifier#generate(\"demo\", expires_at: 2100-01-01T00:00:00Z): an " \
                 "unexpired envelope with no purpose. turbo-rails' verifier unwraps it to " \
                 "\"demo\" (turbo_rails_verified), but Turbo::StreamsChannel never emits it and " \
                 "the relay implements no metadata: still an object, still rejected — the Turbo " \
                 "profile carries no expiry claim and must not grow one by accident.",
    token: verifier.generate("demo", expires_at: PINNED_EXPIRY),
    stream_arg: { kind: "string", value: "demo", expires_at: PINNED_EXPIRY.iso8601 }
  )
]

metadata_document = {
  description: "REAL ActiveSupport::MessageVerifier output signed with the Turbo verifier key " \
               "but wrapped in the `_rails` metadata envelope (purpose and/or expiry). " \
               "Turbo::StreamsChannel never produces these, and the relay's Turbo profile " \
               "implements no metadata, so it must reject them: expect.stream is null and " \
               "expect.rejected is true (expect.turbo_rails_verified records what Rails' own " \
               "verifier would say). The signature IS valid — the rejection is about the " \
               "payload shape, not the HMAC.",
  producer: document.fetch(:producer).merge(
    envelope: "ActiveSupport::Messages::Metadata — {\"_rails\":{\"data\":...,\"exp\":...,\"pur\":...}}"
  ),
  rust_contract: {
    key: document.dig(:rust_contract, :key),
    reject: "MessageVerifier::new(key).verify(input.signed_stream_name) must be Err (the payload is " \
            "not a JSON string); StreamsController.resolve(input.identifier) must not yield a stream",
    signature: "HMAC-SHA256(key, expect.encoded_payload) == expect.signature still holds: a verifier " \
               "that rejects on signature here is rejecting for the wrong reason",
    mismatch_rule: document.dig(:rust_contract, :mismatch_rule)
  },
  cases: rejection_cases
}

metadata_path = File.join(FIXTURES_DIR, METADATA_FIXTURE_FILE)
File.write(metadata_path, "#{JSON.pretty_generate(metadata_document)}\n")
puts "wrote #{metadata_path}"
rejection_cases.each do |c|
  puts "  #{c.fetch(:name)}: rust_status=#{c.fetch(:rust_status)}#{" (#{c[:rust_note]})" if c[:rust_note]}"
end
