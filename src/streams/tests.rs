use std::assert_matches;
use std::sync::Arc;

use super::*;
use crate::protocol::v1::MAX_STREAM_LEN;

// Pins from tests/fixtures/signed_streams/valid_claims.json
// (case "valid_subscribe_token"). The gem is the authoritative producer;
// this key + inputs must reproduce the fixture token byte for byte.
const FIXTURE_SECRET: &[u8] = b"test-secret-32-bytes-padded-000000";
const FIXTURE_NAMESPACE: &str = "example_production";
const FIXTURE_STREAM: &str = "chat:42";
const FIXTURE_EXPIRES_AT: i64 = 4_102_444_800;
const FIXTURE_NOW: i64 = 1_760_000_000;
const FIXTURE_TOKEN: &str = concat!(
    "eyJ2ZXJzaW9uIjoxLCJuYW1lc3BhY2UiOiJleGFtcGxlX3Byb2R1Y3Rpb24iLCJzdHJlYW0iOi",
    "JjaGF0OjQyIiwicHVycG9zZSI6InN1YnNjcmliZSIsImV4cGlyZXNfYXQiOjQxMDI0NDQ4MDB9",
    "--ba7637287a948eea5865b506a7601ec94c8f08b7473dd80785d70f479db227b7"
);

const OLD_SECRET: &[u8] = b"retired-key-from-the-previous-rotation";

fn secret(bytes: &[u8]) -> SecretBytes {
    SecretBytes::new(bytes.to_vec())
}

fn claims_verifier() -> ClaimsVerifier {
    ClaimsVerifier::new(secret(FIXTURE_SECRET))
}

fn claims_config() -> StreamsConfig {
    StreamsConfig { secret: Some(secret(FIXTURE_SECRET)), ..Default::default() }
}

fn orbitcast_identifier(token: &str) -> String {
    format!(r#"{{"channel":"Orbitcast::StreamsChannel","signed_stream_name":"{token}"}}"#)
}

fn turbo_identifier(signed: &str) -> String {
    format!(r#"{{"channel":"Turbo::StreamsChannel","signed_stream_name":"{signed}"}}"#)
}

/// Producer-equivalence proof: the Rust generator, given the fixture's
/// inputs and the fixed claim key order `{version, namespace, stream,
/// purpose, expires_at}`, reproduces the gem-produced fixture token byte
/// for byte. This is what licenses the generator for edge-case tests, and it
/// also pins that an unset `whisper`/`kid` adds no bytes.
#[test]
fn generator_matches_fixture_token_byte_for_byte() {
    let verifier = claims_verifier();
    let claims = StreamClaims::subscribe(FIXTURE_NAMESPACE, FIXTURE_STREAM, FIXTURE_EXPIRES_AT);
    assert_eq!(verifier.sign(&claims), FIXTURE_TOKEN);
}

#[test]
fn claims_verifier_accepts_the_fixture_token() {
    let verifier = claims_verifier();
    let outcome = verifier.verify_keyed(FIXTURE_TOKEN, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap();
    assert_eq!(outcome.key, KeyMatch::Current);
    let claims = outcome.value;
    assert_eq!(claims.stream, FIXTURE_STREAM);
    assert_eq!(claims.namespace, FIXTURE_NAMESPACE);
    assert_eq!(claims.purpose, "subscribe");
    assert!(!claims.whisper, "a token without the claim grants no whisper");
    assert_eq!(claims.kid, None);
}

#[test]
fn claims_verifier_rejects_namespace_mismatch() {
    // A correctly signed, unexpired subscribe token for one namespace must
    // be rejected under a different one.
    let verifier = claims_verifier();
    let err = verifier.verify(FIXTURE_TOKEN, "some_other_namespace", FIXTURE_NOW).unwrap_err();
    assert_matches!(
        err,
        VerifyError::NamespaceMismatch { ref actual, .. } if actual == FIXTURE_NAMESPACE);
}

#[test]
fn claims_verifier_rejects_tampered_signature() {
    let verifier = claims_verifier();
    let (encoded, _) = FIXTURE_TOKEN.split_once("--").unwrap();
    let tampered = format!("{encoded}--{}", "0".repeat(64));
    let err = verifier.verify(&tampered, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap_err();
    assert_matches!(err, VerifyError::InvalidSignature);
}

#[test]
fn claims_verifier_rejects_wrong_secret() {
    let verifier = ClaimsVerifier::new(secret(b"a-completely-different-key"));
    let err = verifier.verify(FIXTURE_TOKEN, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap_err();
    assert_matches!(err, VerifyError::InvalidSignature);
}

#[test]
fn claims_verifier_rejects_expired_token() {
    // Sign a fresh token that has already expired at `now`.
    let verifier = claims_verifier();
    let claims = StreamClaims::subscribe(FIXTURE_NAMESPACE, FIXTURE_STREAM, FIXTURE_NOW - 1);
    let token = verifier.sign(&claims);
    let err = verifier.verify(&token, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap_err();
    assert_matches!(err, VerifyError::Claims(ProtocolError::Expired { .. }));
}

#[test]
fn claims_verifier_rejects_wrong_purpose() {
    let verifier = claims_verifier();
    let mut claims = StreamClaims::subscribe(FIXTURE_NAMESPACE, FIXTURE_STREAM, FIXTURE_EXPIRES_AT);
    claims.purpose = "publish".to_string();
    let token = verifier.sign(&claims);
    let err = verifier.verify(&token, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap_err();
    assert_matches!(err, VerifyError::Claims(ProtocolError::InvalidPurpose { .. }));
}

#[test]
fn claims_verifier_rejects_malformed_tokens() {
    // Every shape no key could have produced is a format error, decided
    // before any HMAC runs.
    let verifier = claims_verifier();
    let (encoded, signature) = FIXTURE_TOKEN.split_once("--").unwrap();
    let cases: &[(&str, String)] = &[
        ("no separator", "no-separator".to_string()),
        ("empty payload", format!("--{signature}")),
        ("empty signature", format!("{encoded}--")),
        ("short signature", format!("{encoded}--{}", &signature[..62])),
        ("long signature", format!("{encoded}--{signature}00")),
        ("non-hex signature", format!("{encoded}--{}", "zz".repeat(32))),
    ];
    for (name, token) in cases {
        assert_matches!(
            verifier.verify(token, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap_err(),
            VerifyError::InvalidFormat,
            "{name}"
        );
    }

    let oversized = "a".repeat(MAX_TOKEN_LEN + 1);
    assert_matches!(
        verifier.verify(&oversized, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap_err(),
        VerifyError::TokenTooLong { .. }
    );
}

#[test]
fn claims_verifier_accepts_uppercase_hex_signatures() {
    // Hex is compared as decoded bytes, so case is not part of the signature.
    let verifier = claims_verifier();
    let (encoded, signature) = FIXTURE_TOKEN.split_once("--").unwrap();
    let upper = format!("{encoded}--{}", signature.to_ascii_uppercase());
    assert!(verifier.verify(&upper, FIXTURE_NAMESPACE, FIXTURE_NOW).is_ok());
}

// --- key ring --------------------------------------------------------------

#[test]
fn previous_keys_are_bounded() {
    let keys = |n: usize| (0..n).map(|i| secret(&[u8::try_from(i).unwrap()])).collect::<Vec<_>>();
    assert_eq!(PreviousKeys::new(keys(MAX_PREVIOUS_KEYS)).unwrap().len(), MAX_PREVIOUS_KEYS);
    assert_matches!(
        PreviousKeys::new(keys(MAX_PREVIOUS_KEYS + 1)),
        Err(KeyRingError::TooManyPreviousKeys { count: 9, max: 8 })
    );
}

#[test]
fn ring_verifies_with_a_previous_key_after_rotation() {
    // The fixture token was signed with what is now the *previous* key.
    let ring = KeyRing::with_previous(
        secret(b"the-new-current-key"),
        PreviousKeys::new(vec![secret(OLD_SECRET), secret(FIXTURE_SECRET)]).unwrap(),
    );
    let verifier = ClaimsVerifier::with_ring(ring);
    let outcome = verifier.verify_keyed(FIXTURE_TOKEN, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap();
    assert_eq!(outcome.key, KeyMatch::Previous(1), "matched the second previous key");
    assert_eq!(outcome.value.stream, FIXTURE_STREAM);

    // Signing is always the current key, which the ring then matches first.
    let claims = StreamClaims::subscribe(FIXTURE_NAMESPACE, FIXTURE_STREAM, FIXTURE_EXPIRES_AT);
    let fresh = verifier.sign(&claims);
    assert_ne!(fresh, FIXTURE_TOKEN);
    assert_eq!(
        verifier.verify_keyed(&fresh, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap().key,
        KeyMatch::Current
    );
}

#[test]
fn ring_rejects_a_retired_key() {
    // Removing the old key from the ring is what finally invalidates tokens
    // minted with it.
    let ring = KeyRing::with_previous(
        secret(b"the-new-current-key"),
        PreviousKeys::new(vec![secret(OLD_SECRET)]).unwrap(),
    );
    let err = ClaimsVerifier::with_ring(ring)
        .verify(FIXTURE_TOKEN, FIXTURE_NAMESPACE, FIXTURE_NOW)
        .unwrap_err();
    assert_matches!(err, VerifyError::InvalidSignature);
}

#[test]
fn kid_claim_is_carried_but_never_selects_a_key() {
    // A `kid` naming a key that is not in the ring changes nothing: the ring
    // is tried in order and the signature decides.
    let verifier = claims_verifier();
    let mut claims = StreamClaims::subscribe(FIXTURE_NAMESPACE, FIXTURE_STREAM, FIXTURE_EXPIRES_AT);
    claims.kid = Some("2026-q4".to_string());
    let token = verifier.sign(&claims);
    let outcome = verifier.verify_keyed(&token, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap();
    assert_eq!(outcome.key, KeyMatch::Current);
    assert_eq!(outcome.value.kid.as_deref(), Some("2026-q4"));

    // The same `kid`, signed with a key the ring does not hold, still fails.
    let foreign = ClaimsVerifier::new(secret(OLD_SECRET)).sign(&claims);
    let err = verifier.verify(&foreign, FIXTURE_NAMESPACE, FIXTURE_NOW).unwrap_err();
    assert_matches!(err, VerifyError::InvalidSignature);
}

#[test]
fn string_profiles_verify_through_the_ring_too() {
    let signed = MessageVerifier::new(OLD_SECRET).sign("posts:1").unwrap();
    let ring = KeyRing::with_previous(
        secret(b"new-turbo-key"),
        PreviousKeys::new(vec![secret(OLD_SECRET)]).unwrap(),
    );
    let outcome = MessageVerifier::with_ring(ring).verify_keyed(&signed).unwrap();
    assert_eq!(outcome.value, "posts:1");
    assert_eq!(outcome.key, KeyMatch::Previous(0));
}

#[test]
fn controller_counts_which_key_verified() {
    use crate::metrics::{Metrics, SignatureKey, SignatureProfile};

    let metrics = Arc::new(Metrics::new());
    let config = StreamsConfig {
        secret: Some(secret(b"new-current-key")),
        previous_secrets: PreviousKeys::new(vec![secret(FIXTURE_SECRET)]).unwrap(),
        turbo: true,
        turbo_secret: Some(secret(b"turbo-secret")),
        ..Default::default()
    };
    let controller = StreamsController::new(config, FIXTURE_NAMESPACE.to_string())
        .with_metrics(Arc::clone(&metrics));

    // Previous key (the fixture token) on the Orbitcast profile.
    controller.resolve_at(&orbitcast_identifier(FIXTURE_TOKEN), FIXTURE_NOW).unwrap();
    // Current key on the Turbo profile.
    let signed = MessageVerifier::new(b"turbo-secret").sign("posts:1").unwrap();
    controller.resolve(&turbo_identifier(&signed)).unwrap();
    // No key at all.
    let (encoded, _) = FIXTURE_TOKEN.split_once("--").unwrap();
    let forged = format!("{encoded}--{}", "0".repeat(64));
    controller.resolve_at(&orbitcast_identifier(&forged), FIXTURE_NOW).unwrap_err();
    // A format error involves no key and is not counted.
    controller.resolve_at(&orbitcast_identifier("garbage"), FIXTURE_NOW).unwrap_err();

    let count = |profile, key| metrics.signature_verifications(profile, key);
    assert_eq!(count(SignatureProfile::Orbitcast, SignatureKey::Previous), 1);
    assert_eq!(count(SignatureProfile::Orbitcast, SignatureKey::Current), 0);
    assert_eq!(count(SignatureProfile::Orbitcast, SignatureKey::Failed), 1);
    assert_eq!(count(SignatureProfile::Turbo, SignatureKey::Current), 1);
    assert_eq!(count(SignatureProfile::Turbo, SignatureKey::Failed), 0);
    assert_eq!(count(SignatureProfile::CableReady, SignatureKey::Current), 0);
}

// --- controller: Orbitcast profile -----------------------------------------

#[test]
fn streams_controller_pubsub_claims_profile() {
    let controller = StreamsController::new(claims_config(), FIXTURE_NAMESPACE.to_string());

    let identifier = orbitcast_identifier(FIXTURE_TOKEN);
    assert!(controller.handles(&identifier));

    let result = controller.resolve_at(&identifier, FIXTURE_NOW).unwrap();
    assert_eq!(result.stream, FIXTURE_STREAM);
    assert!(!result.whisper, "a plain subscribe token is read-only");
}

#[test]
fn whisper_is_granted_only_by_the_whisper_claim() {
    let controller = StreamsController::new(claims_config(), FIXTURE_NAMESPACE.to_string());
    let verifier = claims_verifier();

    let granted = StreamClaims::subscribe(FIXTURE_NAMESPACE, FIXTURE_STREAM, FIXTURE_EXPIRES_AT)
        .with_whisper(true);
    let token = verifier.sign(&granted);
    assert!(token.len() > FIXTURE_TOKEN.len(), "the claim is emitted only when set");
    let result = controller.resolve_at(&orbitcast_identifier(&token), FIXTURE_NOW).unwrap();
    assert!(result.whisper);

    // The capability is inside the signature: flipping the claim after
    // signing is a different token.
    let (encoded, signature) = FIXTURE_TOKEN.split_once("--").unwrap();
    let (granted_encoded, _) = token.split_once("--").unwrap();
    assert_ne!(encoded, granted_encoded);
    let spliced = format!("{granted_encoded}--{signature}");
    assert_matches!(
        controller.resolve_at(&orbitcast_identifier(&spliced), FIXTURE_NOW).unwrap_err(),
        StreamError::Verify(VerifyError::InvalidSignature)
    );
}

#[test]
fn streams_controller_pubsub_rejects_other_namespace() {
    let controller = StreamsController::new(claims_config(), "different".to_string());
    let err = controller.resolve_at(&orbitcast_identifier(FIXTURE_TOKEN), FIXTURE_NOW).unwrap_err();
    assert_matches!(err, StreamError::Verify(VerifyError::NamespaceMismatch { .. }));
}

#[test]
fn streams_controller_pubsub_rejects_expired_at_clock() {
    let controller = StreamsController::new(claims_config(), FIXTURE_NAMESPACE.to_string());
    // The token expires at 4102444800; a clock past that must reject it.
    let err = controller
        .resolve_at(&orbitcast_identifier(FIXTURE_TOKEN), FIXTURE_EXPIRES_AT)
        .unwrap_err();
    assert_matches!(err, StreamError::Verify(VerifyError::Claims(ProtocolError::Expired { .. })));
}

#[test]
fn streams_controller_reports_a_missing_stream_field_per_channel() {
    let config = StreamsConfig {
        turbo: true,
        turbo_secret: Some(secret(b"t")),
        cable_ready: true,
        cable_ready_secret: Some(secret(b"c")),
        ..claims_config()
    };
    let controller = StreamsController::new(config, FIXTURE_NAMESPACE.to_string());
    for identifier in [
        r#"{"channel":"Orbitcast::StreamsChannel"}"#,
        // Each channel reads its own field; another channel's field is ignored.
        r#"{"channel":"Turbo::StreamsChannel","identifier":"x--y"}"#,
        r#"{"channel":"CableReady::Stream","signed_stream_name":"x--y"}"#,
    ] {
        assert_matches!(
            controller.resolve_at(identifier, FIXTURE_NOW),
            Err(StreamError::MissingStream),
            "{identifier}"
        );
    }
    assert_matches!(
        controller.resolve_at(r#"{"x":1}"#, FIXTURE_NOW),
        Err(StreamError::MissingChannel)
    );
    assert_matches!(
        controller.resolve_at(r#"{"channel":"ChatChannel"}"#, FIXTURE_NOW),
        Err(StreamError::UnknownChannel(_))
    );
    assert!(!controller.handles(r#"{"channel":"ChatChannel"}"#));
    assert!(!controller.handles("not json"));
}

// --- controller: public streams ----------------------------------------

#[test]
fn streams_controller_public() {
    let config = StreamsConfig { public: true, public_insecure: true, ..Default::default() };
    let controller = StreamsController::new(config, FIXTURE_NAMESPACE.to_string());

    let identifier = r#"{"channel":"Orbitcast::StreamsChannel","stream_name":"public:stream"}"#;

    assert!(controller.handles(identifier));

    let result = controller.resolve(identifier).unwrap();
    assert_eq!(result.stream, "public:stream");
    assert!(!result.whisper, "an unsigned name is no capability; it never grants whisper");
}

fn config_public() -> StreamsConfig {
    StreamsConfig { public: true, public_insecure: true, ..Default::default() }
}

#[test]
fn public_streams_apply_the_stream_name_bounds() {
    let controller = StreamsController::new(config_public(), FIXTURE_NAMESPACE.to_string());
    let identifier = |stream: &str| {
        format!(r#"{{"channel":"Orbitcast::StreamsChannel","stream_name":"{stream}"}}"#)
    };

    assert_matches!(
        controller.resolve(&identifier("")).unwrap_err(),
        StreamError::InvalidStream(ProtocolError::EmptyField { field: "stream" })
    );
    assert_matches!(
        controller.resolve(&identifier(&"x".repeat(MAX_STREAM_LEN + 1))).unwrap_err(),
        StreamError::InvalidStream(ProtocolError::FieldTooLong { field: "stream", .. })
    );
    assert_eq!(
        controller.resolve(&identifier(&"x".repeat(MAX_STREAM_LEN))).unwrap().stream.len(),
        MAX_STREAM_LEN
    );
}

#[test]
fn streams_controller_public_not_allowed() {
    let controller = StreamsController::new(claims_config(), FIXTURE_NAMESPACE.to_string());

    let identifier = r#"{"channel":"Orbitcast::StreamsChannel","stream_name":"public:stream"}"#;
    let result = controller.resolve(identifier);

    assert_matches!(result, Err(StreamError::PublicNotAllowed));
}

// --- controller: Turbo / CableReady --------------------------------------

#[test]
fn turbo_profile_roundtrip_is_string_signing() {
    let config = StreamsConfig {
        turbo: true,
        turbo_secret: Some(secret(b"turbo-secret")),
        ..Default::default()
    };
    let controller = StreamsController::new(config, FIXTURE_NAMESPACE.to_string());
    let verifier = MessageVerifier::new(b"turbo-secret");

    let signed = verifier.sign("posts:1").unwrap();
    let identifier = turbo_identifier(&signed);

    assert!(controller.handles(&identifier));

    let result = controller.resolve(&identifier).unwrap();
    assert_eq!(result.stream, "posts:1");
    assert!(!result.whisper, "Turbo names never carry a whisper capability");
}

#[test]
fn turbo_profile_never_falls_back_to_the_claims_secret() {
    // A misconfigured controller (turbo on, no turbo key) must not verify
    // Turbo names with the Orbitcast claims secret, even when that secret
    // could produce a matching signature.
    let config = StreamsConfig {
        secret: Some(secret(b"shared-secret")),
        turbo: true,
        turbo_secret: None,
        ..Default::default()
    };
    let controller = StreamsController::new(config, FIXTURE_NAMESPACE.to_string());

    // Signed with the claims secret: would verify under the old fallback.
    let signed = MessageVerifier::new(b"shared-secret").sign("posts:1").unwrap();
    let err = controller.resolve(&turbo_identifier(&signed)).unwrap_err();
    assert_matches!(err, StreamError::NoSecret);
}

#[test]
fn cable_ready_profile_never_falls_back_to_the_claims_secret() {
    let config = StreamsConfig {
        secret: Some(secret(b"shared-secret")),
        cable_ready: true,
        cable_ready_secret: None,
        ..Default::default()
    };
    let controller = StreamsController::new(config, FIXTURE_NAMESPACE.to_string());

    let signed = MessageVerifier::new(b"shared-secret").sign("posts:1").unwrap();
    let identifier = format!(r#"{{"channel":"CableReady::Stream","identifier":"{signed}"}}"#);

    let err = controller.resolve(&identifier).unwrap_err();
    assert_matches!(err, StreamError::NoSecret);
}

#[test]
fn message_verifier_rejects_bad_signature_and_format() {
    let verifier = MessageVerifier::new(b"test-secret");
    assert_matches!(verifier.verify("ImNoYXQ6NDIi--invalid"), Err(VerifyError::InvalidFormat));
    assert_matches!(verifier.verify("no-separator"), Err(VerifyError::InvalidFormat));
    let forged = format!("ImNoYXQ6NDIi--{}", "0".repeat(64));
    assert_matches!(verifier.verify(&forged), Err(VerifyError::InvalidSignature));
}

#[test]
fn message_verifier_applies_the_stream_bounds() {
    let verifier = MessageVerifier::new(b"test-secret");
    assert_matches!(
        verifier.verify(&verifier.sign("").unwrap()),
        Err(VerifyError::Stream(ProtocolError::EmptyField { field: "stream" }))
    );
    let oversized = "s".repeat(MAX_STREAM_LEN + 1);
    assert_matches!(
        verifier.verify(&verifier.sign(&oversized).unwrap()),
        Err(VerifyError::Stream(ProtocolError::FieldTooLong { field: "stream", .. }))
    );
    let at_limit = "s".repeat(MAX_STREAM_LEN);
    assert_eq!(verifier.verify(&verifier.sign(&at_limit).unwrap()).unwrap(), at_limit);
}

#[test]
fn message_verifier_rejects_non_string_payloads() {
    // Correctly signed, but not a bare JSON string: a Rails metadata
    // envelope is named explicitly; anything else is "not a string".
    let key = secret(b"turbo-secret");
    let signed = |json: &str| sign_encoded(&key, &base64_encode(json.as_bytes()));
    let verifier = MessageVerifier::new(key.as_bytes());

    assert_matches!(
        verifier.verify(&signed(r#"{"_rails":{"data":"demo","pur":"x"}}"#)),
        Err(VerifyError::MetadataEnvelope)
    );
    for json in [r#"{"data":"demo"}"#, r#"["demo"]"#, "42", "null"] {
        assert_matches!(
            verifier.verify(&signed(json)),
            Err(VerifyError::PayloadNotAString),
            "{json}"
        );
    }
}

#[test]
fn secret_bytes_debug_is_redacted() {
    let secret = SecretBytes::new(b"super-secret".to_vec());
    let rendered = format!("{secret:?}");
    assert!(!rendered.contains("super-secret"));
    assert!(rendered.contains("REDACTED"));

    let ring = KeyRing::with_previous(
        secret,
        PreviousKeys::new(vec![SecretBytes::new(b"older-secret".to_vec())]).unwrap(),
    );
    let rendered = format!("{ring:?}");
    assert!(!rendered.contains("super-secret") && !rendered.contains("older-secret"));
}
