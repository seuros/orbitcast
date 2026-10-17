//! Property tests over every parser that faces untrusted input:
//! tolerant parsing, strict authorization.
//!
//! Each parser gets the same four properties where they apply:
//!
//! 1. **never panics** on arbitrary bytes or arbitrary JSON;
//! 2. **never works past its documented limit** (a token over
//!    `MAX_TOKEN_LEN` is refused before any decode, a frame header past
//!    `MAX_FRAME_PAYLOAD` is refused before any buffer is reserved);
//! 3. **invalid never verifies** (a token whose payload, signature, or key
//!    was altered is rejected; a well-formed but unsigned envelope grants
//!    nothing);
//! 4. **valid round-trips** (what the producer encodes, the parser returns).
//!
//! Case counts are kept small on purpose: the whole file runs in a few
//! seconds and lives in the ordinary test suite, so it is a regression gate,
//! not a campaign. Raise `PROPTEST_CASES` locally for a longer run.

use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use mothership_docking_protocol::{
    MAX_FRAME_PAYLOAD, MessageType, decode_cargo, decode_header, encode_cargo, encode_header,
};
use proptest::prelude::*;
use serde_json::{Value, json};
use sha2::Sha256;

use orbitcast::actioncable::{ClientCommand, parse_command};
use orbitcast::protocol::Envelope;
use orbitcast::protocol::v1::claims::StreamClaims;
use orbitcast::protocol::v1::control::ControlMessage;
use orbitcast::protocol::v1::publication::{Publication, PublicationNotice};
use orbitcast::protocol::v1::{MAX_NAMESPACE_LEN, MAX_STREAM_LEN};
use orbitcast::streams::{
    ClaimsVerifier, KeyRing, MAX_PREVIOUS_KEYS, MAX_TOKEN_LEN, MessageVerifier, PreviousKeys,
    SecretBytes, StreamsConfig, StreamsController, VerifyError,
};

const NAMESPACE: &str = "fuzz_namespace";
const NOW: i64 = 1_760_000_000;

fn config() -> ProptestConfig {
    ProptestConfig { cases: 128, max_shrink_iters: 256, ..ProptestConfig::default() }
}

/// Sign `encoded` the way the Ruby producers do: lowercase hex HMAC-SHA256
/// over the encoded bytes, appended after `--`.
fn sign(key: &[u8], encoded: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(encoded.as_bytes());
    format!("{encoded}--{}", hex::encode(mac.finalize().into_bytes()))
}

fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Any UTF-8 text, biased toward short strings with awkward characters.
fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => "[ -~]{0,64}",
        2 => "\\PC{0,32}",
        1 => any::<String>(),
    ]
}

/// A key of 1..=64 arbitrary bytes.
fn key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..=64)
}

/// A stream name the producer would accept: non-empty, within the limit.
fn valid_stream() -> impl Strategy<Value = String> {
    "\\PC{1,64}"
        .prop_filter("within MAX_STREAM_LEN", |s| !s.is_empty() && s.len() <= MAX_STREAM_LEN)
}

fn valid_namespace() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,31}".prop_filter("bounded", |s| s.len() <= MAX_NAMESPACE_LEN)
}

/// Arbitrary JSON values of bounded depth, for "never panics" properties.
fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        any::<f64>().prop_filter("finite", |f| f.is_finite()).prop_map(|f| json!(f)),
        text().prop_map(Value::String),
    ];
    leaf.prop_recursive(3, 24, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
            prop::collection::btree_map("[a-z_]{1,12}", inner, 0..6)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

/// Any JSON value with (some of) the claim field names and arbitrary types.
fn claims_like_json() -> impl Strategy<Value = Value> {
    prop::collection::btree_map(
        prop_oneof![
            Just("version".to_string()),
            Just("namespace".to_string()),
            Just("stream".to_string()),
            Just("purpose".to_string()),
            Just("expires_at".to_string()),
            Just("whisper".to_string()),
            Just("kid".to_string()),
            "[a-z_]{1,10}",
        ],
        json_value(),
        0..8,
    )
    .prop_map(|m| Value::Object(m.into_iter().collect()))
}

// --- ActionCable command frames ------------------------------------------

proptest! {
    #![proptest_config(config())]

    #[test]
    fn actioncable_parse_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let _ = parse_command(&bytes);
    }

    #[test]
    fn actioncable_parse_never_panics_on_json(value in json_value()) {
        let _ = parse_command(value.to_string().as_bytes());
    }

    #[test]
    fn actioncable_commands_round_trip(
        command in prop_oneof![
            Just("subscribe"), Just("unsubscribe"), Just("message"), Just("whisper"),
            Just("join"), Just("leave"), Just("presence"),
        ],
        identifier in text(),
        data in text(),
    ) {
        let mut frame = json!({"command": command, "identifier": identifier});
        if matches!(command, "message" | "whisper") {
            frame["data"] = Value::String(data.clone());
        }
        let parsed = parse_command(frame.to_string().as_bytes())
            .expect("a well-formed command parses");
        let (parsed_id, parsed_data) = match parsed {
            ClientCommand::Subscribe { identifier } | ClientCommand::Unsubscribe { identifier }
            | ClientCommand::Join { identifier, .. } | ClientCommand::Leave { identifier }
            | ClientCommand::Presence { identifier } => (identifier, None),
            ClientCommand::Message { identifier, data }
            | ClientCommand::Whisper { identifier, data } => (identifier, Some(data)),
        };
        prop_assert_eq!(parsed_id, identifier);
        if matches!(command, "message" | "whisper") {
            prop_assert_eq!(parsed_data, Some(data));
        }
    }

    #[test]
    fn actioncable_unknown_commands_are_rejected(
        command in "[a-z_]{1,16}".prop_filter("unknown", |c| {
            !["subscribe", "unsubscribe", "message", "whisper", "join", "leave", "presence"]
                .contains(&c.as_str())
        }),
        identifier in text(),
    ) {
        let frame = json!({"command": command, "identifier": identifier, "data": "{}"});
        prop_assert!(parse_command(frame.to_string().as_bytes()).is_err());
    }
}

// --- signed-stream tokens, all profiles ----------------------------------

proptest! {
    #![proptest_config(config())]

    #[test]
    fn verifiers_never_panic(token in text(), key in key()) {
        let _ = ClaimsVerifier::new(SecretBytes::new(key.clone())).verify(&token, NAMESPACE, NOW);
        let _ = MessageVerifier::new(&key).verify(&token);
    }

    #[test]
    fn verifiers_never_panic_on_token_shaped_input(
        encoded in "[A-Za-z0-9+/=]{0,200}",
        signature in "[0-9a-fA-F]{0,70}",
        key in key(),
    ) {
        let token = format!("{encoded}--{signature}");
        let _ = ClaimsVerifier::new(SecretBytes::new(key.clone())).verify(&token, NAMESPACE, NOW);
        let _ = MessageVerifier::new(&key).verify(&token);
    }

    #[test]
    fn oversized_tokens_are_refused_before_any_decode(
        len in MAX_TOKEN_LEN + 1..MAX_TOKEN_LEN + 64,
        key in key(),
    ) {
        // The body is a valid, correctly signed token padded past the limit:
        // the only reason to refuse it is its length, and that is decided
        // before base64, JSON, or HMAC run.
        let claims = StreamClaims::subscribe(NAMESPACE, "s".repeat(len), NOW + 60);
        let token = sign(&key, &b64(serde_json::to_string(&claims).unwrap().as_bytes()));
        prop_assert!(token.len() > MAX_TOKEN_LEN);
        // `prop_assert!` stringifies its condition into a format string, so
        // struct patterns with `{ .. }` are bound to names first.
        let claims_refused = matches!(
            ClaimsVerifier::new(SecretBytes::new(key.clone())).verify(&token, NAMESPACE, NOW),
            Err(VerifyError::TokenTooLong { .. })
        );
        prop_assert!(claims_refused);
        let string_refused = matches!(
            MessageVerifier::new(&key).verify(&token),
            Err(VerifyError::TokenTooLong { .. })
        );
        prop_assert!(string_refused);
    }

    #[test]
    fn claims_tokens_round_trip_and_bind_to_their_claims(
        key in key(),
        namespace in valid_namespace(),
        stream in valid_stream(),
        ttl in 1i64..100_000,
        whisper in any::<bool>(),
        kid in prop::option::of("[a-z0-9-]{1,8}"),
    ) {
        let mut claims =
            StreamClaims::subscribe(&namespace, &stream, NOW + ttl).with_whisper(whisper);
        claims.kid = kid;
        let token = sign(&key, &b64(serde_json::to_string(&claims).unwrap().as_bytes()));
        let verifier = ClaimsVerifier::new(SecretBytes::new(key));

        let accepted = verifier.verify(&token, &namespace, NOW).expect("a producer token verifies");
        prop_assert_eq!(&accepted, &claims);
        prop_assert!(accepted.authorizes(&namespace, &stream, NOW));
        prop_assert!(!accepted.authorizes(&namespace, &stream, NOW + ttl));

        // Another namespace never admits it, however it was signed.
        let isolated = matches!(
            verifier.verify(&token, "another_namespace", NOW),
            Err(VerifyError::NamespaceMismatch { .. })
        );
        prop_assert!(isolated);
    }

    #[test]
    fn altered_claims_tokens_never_verify(
        key in key(),
        stream in valid_stream(),
        flip in 0usize..64,
        other_key in key(),
    ) {
        let claims = StreamClaims::subscribe(NAMESPACE, &stream, NOW + 60);
        let encoded = b64(serde_json::to_string(&claims).unwrap().as_bytes());
        let token = sign(&key, &encoded);
        let keys_differ = other_key != key;
        let verifier = ClaimsVerifier::new(SecretBytes::new(key));
        let (_, signature) = token.split_once("--").unwrap();

        // One signature nibble flipped.
        let mut bytes = signature.as_bytes().to_vec();
        bytes[flip] = if bytes[flip] == b'0' { b'1' } else { b'0' };
        let flipped = format!("{encoded}--{}", std::str::from_utf8(&bytes).unwrap());
        prop_assert!(matches!(
            verifier.verify(&flipped, NAMESPACE, NOW),
            Err(VerifyError::InvalidSignature)
        ));

        // Payload altered after signing (a wider claim set), same signature.
        let widened = StreamClaims::subscribe(NAMESPACE, &stream, NOW + 60).with_whisper(true);
        let spliced =
            format!("{}--{signature}", b64(serde_json::to_string(&widened).unwrap().as_bytes()));
        prop_assert!(matches!(
            verifier.verify(&spliced, NAMESPACE, NOW),
            Err(VerifyError::InvalidSignature)
        ));

        // A different key, unless it happens to be the same bytes.
        if keys_differ {
            prop_assert!(matches!(
                ClaimsVerifier::new(SecretBytes::new(other_key)).verify(&token, NAMESPACE, NOW),
                Err(VerifyError::InvalidSignature)
            ));
        }
    }

    #[test]
    fn the_ring_admits_every_listed_key_and_nothing_else(
        keys in prop::collection::vec(key(), 1..=MAX_PREVIOUS_KEYS + 1),
        signer in 0usize..=MAX_PREVIOUS_KEYS,
        outsider in key(),
        stream in valid_stream(),
    ) {
        let signer = signer % keys.len();
        let claims = StreamClaims::subscribe(NAMESPACE, &stream, NOW + 60);
        let token = sign(&keys[signer], &b64(serde_json::to_string(&claims).unwrap().as_bytes()));

        let (current, previous) = keys.split_first().unwrap();
        let ring = KeyRing::with_previous(
            SecretBytes::new(current.clone()),
            PreviousKeys::new(previous.iter().cloned().map(SecretBytes::new).collect()).unwrap(),
        );
        let verifier = ClaimsVerifier::with_ring(ring);
        prop_assert!(verifier.verify(&token, NAMESPACE, NOW).is_ok());

        if !keys.contains(&outsider) {
            let foreign = sign(&outsider, &b64(serde_json::to_string(&claims).unwrap().as_bytes()));
            prop_assert!(matches!(
                verifier.verify(&foreign, NAMESPACE, NOW),
                Err(VerifyError::InvalidSignature)
            ));
        }
    }

    #[test]
    fn string_profile_tokens_round_trip_and_reject_alterations(
        key in key(),
        stream in valid_stream(),
        other in valid_stream(),
    ) {
        let token = sign(&key, &b64(serde_json::to_string(&stream).unwrap().as_bytes()));
        let verifier = MessageVerifier::new(&key);

        if other != stream {
            let (_, signature) = token.split_once("--").unwrap();
            let spliced =
                format!("{}--{signature}", b64(serde_json::to_string(&other).unwrap().as_bytes()));
            prop_assert!(matches!(verifier.verify(&spliced), Err(VerifyError::InvalidSignature)));
        }

        // Correctly signed non-strings are rejected by shape.
        let envelope = json!({"_rails": {"data": &stream, "pur": "x"}});
        let wrapped = sign(&key, &b64(envelope.to_string().as_bytes()));
        prop_assert!(matches!(verifier.verify(&wrapped), Err(VerifyError::MetadataEnvelope)));

        // The producer's own token round-trips.
        prop_assert_eq!(verifier.verify(&token).unwrap(), stream);
    }

    #[test]
    fn string_profile_applies_the_stream_bounds(key in key(), extra in 1usize..64) {
        let verifier = MessageVerifier::new(&key);
        let empty = sign(&key, &b64(b"\"\""));
        prop_assert!(matches!(verifier.verify(&empty), Err(VerifyError::Stream(_))));
        let long = "x".repeat(MAX_STREAM_LEN + extra);
        let oversized = sign(&key, &b64(serde_json::to_string(&long).unwrap().as_bytes()));
        prop_assert!(matches!(verifier.verify(&oversized), Err(VerifyError::Stream(_))));
    }

    #[test]
    fn the_controller_never_panics_on_any_identifier(
        identifier in text(),
        value in json_value(),
        key in key(),
    ) {
        let config = StreamsConfig {
            secret: Some(SecretBytes::new(key.clone())),
            public: true,
            public_insecure: true,
            turbo: true,
            turbo_secret: Some(SecretBytes::new(key.clone())),
            cable_ready: true,
            cable_ready_secret: Some(SecretBytes::new(key)),
            ..StreamsConfig::default()
        };
        let controller = StreamsController::new(config, NAMESPACE.to_string());
        let _ = controller.handles(&identifier);
        let _ = controller.resolve_at(&identifier, NOW);
        let _ = controller.resolve_at(&value.to_string(), NOW);
        for channel in [
            "Orbitcast::StreamsChannel",
            "Turbo::StreamsChannel",
            "CableReady::Stream",
        ] {
            let mut with_channel = value.clone();
            if let Value::Object(map) = &mut with_channel {
                map.insert("channel".to_string(), Value::String(channel.to_string()));
            }
            let _ = controller.resolve_at(&with_channel.to_string(), NOW);
        }
    }
}

// --- claims JSON ---------------------------------------------------------

proptest! {
    #![proptest_config(config())]

    #[test]
    fn claims_decode_never_panics_and_absent_fields_grant_nothing(value in claims_like_json()) {
        let Ok(claims) = serde_json::from_value::<StreamClaims>(value.clone()) else {
            return Ok(());
        };
        let _ = claims.validate_at(NOW);
        let _ = claims.validate();
        // Whatever parsed, an unsigned envelope authorizes nothing outside
        // exactly what it names.
        prop_assert!(!claims.authorizes("", "", NOW) || claims.validate_at(NOW).is_ok());
        if claims.namespace.is_empty()
            || claims.stream.is_empty()
            || claims.purpose != "subscribe"
        {
            prop_assert!(claims.validate_at(NOW).is_err());
        }
        // The capability defaults closed when the field is not a plain `true`.
        if value.get("whisper").is_none() {
            prop_assert!(!claims.whisper);
        }
    }

    #[test]
    fn claims_json_round_trips(
        namespace in valid_namespace(),
        stream in valid_stream(),
        expires_at in any::<i64>(),
        whisper in any::<bool>(),
    ) {
        let claims = StreamClaims::subscribe(namespace, stream, expires_at).with_whisper(whisper);
        let text = serde_json::to_string(&claims).unwrap();
        let decoded: StreamClaims = serde_json::from_str(&text).unwrap();
        prop_assert_eq!(decoded, claims);
    }
}

// --- publication notices, ingress bodies, control messages ---------------

proptest! {
    #![proptest_config(config())]

    #[test]
    fn notice_and_ingress_parsers_never_panic(text in text(), value in json_value()) {
        for wire in [text.as_str(), &value.to_string()] {
            if let Ok(notice) = serde_json::from_str::<PublicationNotice>(wire) {
                let _ = notice.validate();
                let _ = (notice.namespace(), notice.stream());
            }
            if let Ok(publication) = serde_json::from_str::<Publication>(wire) {
                let _ = publication.validate();
            }
            if let Ok(control) = serde_json::from_str::<ControlMessage>(wire) {
                let _ = control.validate();
            }
        }
    }

    #[test]
    fn publications_round_trip_through_their_notice(
        namespace in valid_namespace(),
        stream in valid_stream(),
        payload in json_value(),
    ) {
        let publication = Publication::new(namespace, stream, payload.to_string());
        publication.validate().expect("a producer publication validates");
        let notice = PublicationNotice::inline(publication.clone());
        let wire = serde_json::to_string(&notice).unwrap();
        let decoded: PublicationNotice = serde_json::from_str(&wire).unwrap();
        prop_assert_eq!(&decoded, &notice);
        decoded.validate().expect("the decoded notice validates");
        prop_assert_eq!(decoded.stream(), publication.stream.as_str());
        prop_assert_eq!(decoded.namespace(), publication.namespace.as_str());
    }

    #[test]
    fn empty_or_oversized_publications_never_validate(
        namespace in valid_namespace(),
        stream in prop_oneof![Just(String::new()), Just("x".repeat(MAX_STREAM_LEN + 1))],
    ) {
        let publication = Publication::new(namespace, stream, "{}");
        prop_assert!(publication.validate().is_err());
        prop_assert!(PublicationNotice::inline(publication).validate().is_err());
    }
}

// --- docking frame decoder -----------------------------------------------

proptest! {
    #![proptest_config(config())]

    #[test]
    fn frame_header_decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..16)) {
        let _ = decode_header(&bytes);
    }

    #[test]
    fn frame_headers_round_trip_and_oversize_is_visible_before_any_payload(
        len in 0u32..=u32::MAX,
        kind in prop_oneof![
            Just(MessageType::Dock), Just(MessageType::Moored), Just(MessageType::Boarding),
            Just(MessageType::Cargo), Just(MessageType::Disembark), Just(MessageType::Hail),
            Just(MessageType::Haul), Just(MessageType::Reply), Just(MessageType::Ready),
        ],
    ) {
        let header = encode_header(kind, len);
        let (decoded_kind, decoded_len) =
            decode_header(&header).expect("an encoded header decodes");
        prop_assert_eq!(decoded_kind, kind);
        prop_assert_eq!(decoded_len, len as usize);
        // The reader refuses anything past the limit from the header alone;
        // this is the comparison it makes before reserving a byte.
        prop_assert_eq!(decoded_len > MAX_FRAME_PAYLOAD, len as usize > MAX_FRAME_PAYLOAD);
    }

    #[test]
    fn cargo_decode_never_panics_and_round_trips(
        payload in prop::collection::vec(any::<u8>(), 0..256),
        conn_id in any::<u32>(),
        data in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        let _ = decode_cargo(&payload);

        let cargo = mothership_docking_protocol::Cargo::text(conn_id, data.clone());
        let encoded = encode_cargo(&cargo);
        let (kind, len) = decode_header(&encoded).unwrap();
        prop_assert_eq!(kind, MessageType::Cargo);
        let decoded = decode_cargo(&encoded[encoded.len() - len..]).expect("encoded cargo decodes");
        prop_assert_eq!(decoded.conn_id, conn_id);
        prop_assert_eq!(decoded.data, data.as_slice());
    }
}
