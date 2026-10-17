# frozen_string_literal: true

# Turbo derives its signing key from the application's key generator
# (`Rails.application.key_generator.generate_key("turbo/signed_stream_verifier_key")`),
# which is raw binary — not `secret_key_base`. The fixture exporter overrides it
# with a pinned key so the golden files are deterministic.
#
# ORBITCAST_TURBO_SECRET_BASE64 exports these exact bytes to the Rust relay.
if (pinned = ENV["DUMMY_TURBO_SIGNING_KEY_BASE64"])
  require "base64"
  Turbo.signed_stream_verifier_key = Base64.strict_decode64(pinned)
end
