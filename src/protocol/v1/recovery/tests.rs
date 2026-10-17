use super::*;

use std::collections::BTreeMap;

use std::assert_matches;

const NOW: i64 = 1_800_000_000;
const PUBLICATION_ID: &str = "8f14e45f-ce0a-4f2e-9b2b-2a1f3a4b5c6d";

fn claims() -> RecoveryClaims {
    RecoveryClaims::recover(
        "app",
        "session:7",
        vec!["chat:42".to_string(), "posts:1".to_string()],
        NOW + 60,
    )
}

// --- cursors ------------------------------------------------------------

#[test]
fn cursor_encoding_is_deterministic_and_documented() {
    const CASES: &[(u64, u64, &str)] = &[
        (1, 7, r#"{"v":1,"g":"1","s":"7"}"#),
        (0, 0, r#"{"v":1,"g":"0","s":"0"}"#),
        (u64::MAX, u64::MAX, r#"{"v":1,"g":"18446744073709551615","s":"18446744073709551615"}"#),
    ];

    for &(generation, sequence, expected) in CASES {
        let cursor = Cursor::new(generation, sequence);
        let encoded = cursor.encode();
        assert_eq!(encoded, cursor.encode(), "the same position encodes identically");

        let json = String::from_utf8(BASE64.decode(&encoded).unwrap()).unwrap();
        assert_eq!(json, expected, "{cursor:?}");
        assert_eq!(Cursor::decode(&encoded).unwrap(), cursor);
    }
    // The documented example in the module docs, byte for byte.
    assert_eq!(Cursor::new(1, 7).encode(), "eyJ2IjoxLCJnIjoiMSIsInMiOiI3In0=");
}

#[test]
fn cursor_survives_sequences_javascript_cannot_hold_as_a_number() {
    // 2^53 + 1: exactly the first integer a JS Number cannot represent.
    let cursor = Cursor::new(u64::MAX, 9_007_199_254_740_993);
    let encoded = cursor.encode();
    let json = String::from_utf8(BASE64.decode(&encoded).unwrap()).unwrap();
    assert!(json.contains(r#""s":"9007199254740993""#), "{json}");
    assert_eq!(Cursor::decode(&encoded).unwrap(), cursor);
}

#[test]
fn cursor_rejects_junk_without_guessing_a_position() {
    assert_matches!(Cursor::decode("").unwrap_err(), ProtocolError::EmptyField { field: "cursor" });
    assert_matches!(
        Cursor::decode(&"a".repeat(MAX_CURSOR_LEN + 1)).unwrap_err(),
        ProtocolError::FieldTooLong { field: "cursor", .. }
    );
    assert_matches!(
        Cursor::decode("!!!not base64!!!").unwrap_err(),
        ProtocolError::DisallowedValue { field: "cursor", .. }
    );
    assert_matches!(
        Cursor::decode(&BASE64.encode("not json")).unwrap_err(),
        ProtocolError::DisallowedValue { field: "cursor", .. }
    );
    // Numbers, not strings: rejected rather than silently coerced.
    assert_matches!(
        Cursor::decode(&BASE64.encode(r#"{"v":1,"g":1,"s":7}"#)).unwrap_err(),
        ProtocolError::DisallowedValue { field: "cursor", .. }
    );
    // A negative or overflowing sequence is not a position.
    assert_matches!(
        Cursor::decode(&BASE64.encode(r#"{"v":1,"g":"1","s":"-1"}"#)).unwrap_err(),
        ProtocolError::DisallowedValue { field: "cursor", .. }
    );
    assert_matches!(
        Cursor::decode(&BASE64.encode(r#"{"v":2,"g":"1","s":"7"}"#)).unwrap_err(),
        ProtocolError::UnsupportedVersion(2)
    );
}

#[test]
fn cursor_ignores_unknown_fields_and_defaults_its_version() {
    let cursor = Cursor::decode(&BASE64.encode(r#"{"g":"3","s":"9","node":"a"}"#)).unwrap();
    assert_eq!(cursor, Cursor::new(3, 9));
}

// --- claims -------------------------------------------------------------

#[test]
fn claims_roundtrip_emits_version_one() {
    let claims = claims();
    let json = serde_json::to_value(&claims).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["namespace"], "app");
    assert_eq!(json["session_id"], "session:7");
    assert_eq!(json["purpose"], "recover");
    assert_eq!(json["streams"][1], "posts:1");

    let decoded: RecoveryClaims = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, claims);
    decoded.validate_at(NOW).unwrap();
}

#[test]
fn claims_missing_version_defaults_to_one() {
    let json = r#"{"namespace":"app","session_id":"s","streams":["chat:42"],
                       "purpose":"recover","expires_at":1800000060}"#;
    let decoded: RecoveryClaims = serde_json::from_str(json).unwrap();
    assert_eq!(decoded.version, 1);
    decoded.validate_at(NOW).unwrap();
}

#[test]
fn absent_claims_default_to_invalid() {
    let decoded: RecoveryClaims = serde_json::from_str("{}").unwrap();
    assert_eq!(decoded.version, 1);
    assert_eq!(decoded.streams, Vec::<String>::new());
    assert!(decoded.validate_at(NOW).is_err());
    assert!(!decoded.authorizes("", "", NOW));
}

#[test]
fn claims_reject_a_subscribe_purpose() {
    // The exact confusion the dedicated purpose exists to prevent.
    let mut claims = claims();
    claims.purpose = crate::protocol::v1::claims::PURPOSE_SUBSCRIBE.to_string();
    assert_matches!(
        claims.validate_at(NOW).unwrap_err(),
        ProtocolError::InvalidPurpose { expected: PURPOSE_RECOVER, .. }
    );
}

#[test]
fn claims_reject_empty_or_oversized_stream_sets() {
    let mut claims = claims();
    claims.streams.clear();
    assert_matches!(
        claims.validate_at(NOW).unwrap_err(),
        ProtocolError::EmptyField { field: "streams" }
    );

    let mut claims = super::RecoveryClaims::recover(
        "app",
        "s",
        (0..=MAX_RECOVERY_STREAMS).map(|i| format!("chat:{i}")).collect(),
        NOW + 60,
    );
    assert_matches!(
        claims.validate_at(NOW).unwrap_err(),
        ProtocolError::TooMany { field: "streams", .. }
    );

    claims.streams = vec![String::new()];
    assert_matches!(
        claims.validate_at(NOW).unwrap_err(),
        ProtocolError::EmptyField { field: "stream" }
    );
}

#[test]
fn claims_expire_and_scope_to_their_own_streams() {
    let claims = claims();
    assert!(claims.authorizes("app", "chat:42", NOW));
    assert!(claims.authorizes("app", "posts:1", NOW));
    assert!(!claims.authorizes("app", "chat:43", NOW), "a token grants only what it names");
    assert!(!claims.authorizes("other", "chat:42", NOW), "tokens are namespace-scoped");
    assert!(!claims.authorizes("app", "chat:42", NOW + 61), "expiry is checked first");

    let expired = RecoveryClaims::recover("app", "s", vec!["chat:42".into()], NOW - 1);
    assert_matches!(
        expired.validate_at(NOW).unwrap_err(),
        ProtocolError::Expired { expires_at, now } if expires_at == NOW - 1 && now == NOW);
}

#[test]
fn claims_reject_unsupported_version_and_wrong_types() {
    let mut claims = claims();
    claims.version = 99;
    assert_matches!(claims.validate_at(NOW).unwrap_err(), ProtocolError::UnsupportedVersion(99));

    let err = serde_json::from_str::<RecoveryClaims>(r#"{"expires_at":"soon"}"#).unwrap_err();
    assert!(err.is_data());
}

// --- commands -----------------------------------------------------------

#[test]
fn command_roundtrip_emits_version_one() {
    let command =
        RecoveryCommand::new(RecoveryOp::Resume).with_cursor("chat:42", &Cursor::new(1, 7));
    let json = serde_json::to_value(&command).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["action"], RECOVERY_ACTION);
    assert_eq!(json["op"], "resume");
    assert_eq!(json["cursors"]["chat:42"], Cursor::new(1, 7).encode());

    let decoded: RecoveryCommand = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, command);
    decoded.validate().unwrap();
    assert_eq!(decoded.decoded_cursors().unwrap()["chat:42"], Cursor::new(1, 7));
}

#[test]
fn command_without_cursors_omits_the_map() {
    let command = RecoveryCommand::new(RecoveryOp::Ack);
    let json = serde_json::to_value(&command).unwrap();
    assert!(json.get("cursors").is_none(), "an empty map is omitted, never emitted as {{}}");

    let decoded =
        RecoveryCommand::decode(r#"{"action":"orbitcast_recovery","op":"resume"}"#).unwrap();
    assert_eq!(decoded.version, 1);
    assert!(decoded.cursors.is_empty());
}

#[test]
fn command_rejects_unknown_ops_actions_and_cursors() {
    let err =
        RecoveryCommand::decode(r#"{"action":"orbitcast_recovery","op":"rewind"}"#).unwrap_err();
    assert_matches!(err, ProtocolError::Malformed(_));

    let err = RecoveryCommand::decode(r#"{"action":"orbitcast_recovery"}"#).unwrap_err();
    assert_matches!(err, ProtocolError::Malformed(_), "an absent op is not defaulted");

    let err = RecoveryCommand::decode(r#"{"action":"speak","op":"resume"}"#).unwrap_err();
    assert_matches!(err, ProtocolError::DisallowedValue { field: "action", .. });

    let err = RecoveryCommand::decode(
        r#"{"action":"orbitcast_recovery","op":"resume","cursors":{"chat:42":"nope"}}"#,
    )
    .unwrap_err();
    assert_matches!(err, ProtocolError::DisallowedValue { field: "cursor", .. });

    let err = RecoveryCommand::decode(
        r#"{"action":"orbitcast_recovery","op":"resume","cursors":{"":"x"}}"#,
    )
    .unwrap_err();
    assert_matches!(err, ProtocolError::EmptyField { field: "stream" });
}

#[test]
fn command_rejects_more_streams_than_a_token_could_ever_grant() {
    let mut command = RecoveryCommand::new(RecoveryOp::Resume);
    for index in 0..=MAX_RECOVERY_STREAMS {
        command.cursors.insert(format!("chat:{index}"), Cursor::new(1, 1).encode());
    }
    assert_matches!(
        command.validate().unwrap_err(),
        ProtocolError::TooMany { field: "cursors", .. }
    );
}

#[test]
fn the_action_probe_routes_only_our_namespace() {
    assert!(is_recovery_action(r#"{"action":"orbitcast_recovery","op":"ack"}"#));
    assert!(!is_recovery_action(r#"{"action":"orbitcast_presence","op":"join"}"#));
    assert!(!is_recovery_action("{oops"), "unparseable data is never routed to recovery");
    assert!(!is_recovery_action(r#"{"action":123}"#), "a non-string action is not ours");
}

// --- control messages ---------------------------------------------------

#[test]
fn replay_messages_roundtrip() {
    let start = RecoveryMessage::replay_start(vec!["chat:42".to_string()]);
    start.validate().unwrap();
    let json = serde_json::to_value(&start).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["type"], "orbitcast_recovery");
    assert_eq!(json["event"], "replay_start");
    assert!(json.get("reason").is_none());

    let cursors = BTreeMap::from([("chat:42".to_string(), Cursor::new(1, 9).encode())]);
    let done = RecoveryMessage::replay_done(cursors);
    done.validate().unwrap();
    let decoded: RecoveryMessage = serde_json::from_str(&done.to_json().unwrap()).unwrap();
    assert_eq!(decoded, done);
}

#[test]
fn gap_messages_carry_a_stream_and_a_reason() {
    let gap = RecoveryMessage::gap("chat:42", GapReason::Expired);
    gap.validate().unwrap();
    let json = serde_json::to_value(&gap).unwrap();
    assert_eq!(json["event"], "gap");
    assert_eq!(json["stream"], "chat:42");
    assert_eq!(json["reason"], "expired");

    let mut broken = gap.clone();
    broken.reason = None;
    assert_matches!(
        broken.validate().unwrap_err(),
        ProtocolError::MissingPayload { operation: "gap", field: "reason" }
    );

    let mut broken = gap.clone();
    broken.stream = None;
    assert_matches!(broken.validate().unwrap_err(), ProtocolError::EmptyField { field: "stream" });

    let mut broken = gap;
    broken.message_type = "presence".to_string();
    assert_matches!(
        broken.validate().unwrap_err(),
        ProtocolError::DisallowedValue { field: "type", .. }
    );
}

#[test]
fn unknown_event_or_reason_is_a_hard_error() {
    let err = serde_json::from_str::<RecoveryMessage>(
        r#"{"type":"orbitcast_recovery","event":"rewound"}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");

    let err = serde_json::from_str::<RecoveryMessage>(
        r#"{"type":"orbitcast_recovery","event":"gap","stream":"chat:42","reason":"vibes"}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");
}

#[test]
fn replay_start_requires_at_least_one_stream() {
    let empty = RecoveryMessage::replay_start(Vec::new());
    assert_matches!(empty.validate().unwrap_err(), ProtocolError::EmptyField { field: "streams" });
}

// --- delivery envelopes -------------------------------------------------

#[test]
fn delivery_envelope_roundtrips_with_a_js_safe_position() {
    let envelope = DeliveryEnvelope::new(
        "chat:42",
        PUBLICATION_ID,
        Cursor::new(1, 7),
        r#"{"body":"hi","pi":1.0000000000000002}"#,
    );
    envelope.validate().unwrap();

    let json = serde_json::to_value(&envelope).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["type"], "orbitcast_delivery");
    assert_eq!(json["stream"], "chat:42");
    assert_eq!(json["publication_id"], PUBLICATION_ID);
    assert_eq!(json["generation"], "1", "numbers travel as strings");
    assert_eq!(json["sequence"], "7");
    assert_eq!(json["cursor"], Cursor::new(1, 7).encode());
    assert_eq!(
        json["json_payload"], r#"{"body":"hi","pi":1.0000000000000002}"#,
        "the payload text is forwarded byte for byte"
    );

    let decoded: DeliveryEnvelope = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, envelope);
    assert_eq!(decoded.position().unwrap(), Cursor::new(1, 7));
}

#[test]
fn delivery_envelope_fails_closed_on_every_identity_field() {
    let valid = DeliveryEnvelope::new("chat:42", PUBLICATION_ID, Cursor::new(1, 7), r#"{"a":1}"#);

    let mut broken = valid.clone();
    broken.stream = String::new();
    assert_matches!(broken.validate().unwrap_err(), ProtocolError::EmptyField { field: "stream" });

    let mut broken = valid.clone();
    broken.publication_id = "not-a-uuid".to_string();
    assert_matches!(
        broken.validate().unwrap_err(),
        ProtocolError::InvalidUuid { field: "publication_id", .. }
    );

    let mut broken = valid.clone();
    broken.json_payload = "{oops".to_string();
    assert_matches!(
        broken.validate().unwrap_err(),
        ProtocolError::InvalidJson { field: "json_payload", .. }
    );

    let mut broken = valid.clone();
    broken.sequence = "seven".to_string();
    assert_matches!(
        broken.validate().unwrap_err(),
        ProtocolError::DisallowedValue { field: "sequence", .. }
    );

    // A cursor that disagrees with the position it accompanies is a
    // contradiction, not a hint.
    let mut broken = valid.clone();
    broken.cursor = Cursor::new(1, 8).encode();
    assert_matches!(
        broken.validate().unwrap_err(),
        ProtocolError::Mismatch { field: "cursor", .. }
    );

    let mut broken = valid;
    broken.message_type = "orbitcast_recovery".to_string();
    assert_matches!(
        broken.validate().unwrap_err(),
        ProtocolError::DisallowedValue { field: "type", .. }
    );
}

#[test]
fn delivery_envelope_defaults_its_version_and_ignores_unknown_fields() {
    let wire = format!(
        r#"{{"type":"orbitcast_delivery","stream":"chat:42",
                 "publication_id":"{PUBLICATION_ID}","generation":"1","sequence":"7",
                 "cursor":"{}","json_payload":"{{}}","node":"a-1"}}"#,
        Cursor::new(1, 7).encode()
    );
    let decoded: DeliveryEnvelope = serde_json::from_str(&wire).unwrap();
    assert_eq!(decoded.version, 1);
    decoded.validate().unwrap();
}

// --- session snapshots ----------------------------------------------------

#[test]
fn session_snapshot_is_versioned_json_that_round_trips() {
    let cursors = BTreeMap::from([("chat:42".to_string(), Cursor::new(1, 7).encode())]);
    let snapshot = SessionSnapshot::new("session:7", cursors);
    let json = snapshot.to_json().unwrap();
    assert!(json.starts_with(r#"{"version":1"#), "{json}");

    let decoded = SessionSnapshot::decode(&json).unwrap();
    assert_eq!(decoded, snapshot);
    assert_eq!(decoded.decoded_cursors().unwrap()["chat:42"], Cursor::new(1, 7));

    // Tolerant read: an absent version is v1, unknown fields are ignored.
    let decoded = SessionSnapshot::decode(r#"{"session_id":"s","cursors":{},"node":"a"}"#).unwrap();
    assert_eq!(decoded.version, 1);
    assert!(decoded.cursors.is_empty());
}

#[test]
fn session_snapshot_fails_closed_like_an_inbound_envelope() {
    assert_matches!(SessionSnapshot::decode("{oops").unwrap_err(), ProtocolError::Malformed(_));
    assert_matches!(
        SessionSnapshot::decode("{}").unwrap_err(),
        ProtocolError::EmptyField { field: "session_id" },
        "a snapshot without an identity names no session"
    );
    assert_matches!(
        SessionSnapshot::decode(r#"{"version":2,"session_id":"s"}"#).unwrap_err(),
        ProtocolError::UnsupportedVersion(2)
    );
    assert_matches!(
        SessionSnapshot::decode(r#"{"session_id":"s","cursors":{"chat:42":"nope"}}"#).unwrap_err(),
        ProtocolError::DisallowedValue { field: "cursor", .. }
    );
    let long = SessionSnapshot::new("s".repeat(MAX_SESSION_ID_LEN + 1), BTreeMap::new());
    assert_matches!(long.validate().unwrap_err(), ProtocolError::FieldTooLong { .. });
}

/// The name a snapshot fixture uses for a rejection.
fn snapshot_error_kind(err: &ProtocolError) -> &'static str {
    match err {
        ProtocolError::Malformed(_) => "malformed_json",
        ProtocolError::UnsupportedVersion(_) => "unsupported_version",
        ProtocolError::EmptyField { field: "stream" } => "empty_stream",
        ProtocolError::EmptyField { .. } => "empty_field",
        ProtocolError::DisallowedValue { .. } => "disallowed_value",
        other => panic!("fixture names no kind for {other:?}"),
    }
}

/// `tests/fixtures/broker/session_snapshots.json`: the stored shape, pinned.
#[test]
fn session_snapshot_fixtures() {
    let doc: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/broker/session_snapshots.json"
    )))
    .unwrap();

    let cases = doc["cases"].as_array().unwrap();
    assert!(!cases.is_empty(), "the snapshot fixture must carry cases");
    for case in cases {
        let name = case["name"].as_str().unwrap();
        assert_eq!(case["envelope"], "snapshot", "{name}");
        let wire = case["input"]["wire"].as_str().unwrap();
        let expect = &case["expect"];

        if !expect["valid"].as_bool().unwrap() {
            let err = SessionSnapshot::decode(wire).expect_err(name);
            assert_eq!(snapshot_error_kind(&err), expect["error_kind"].as_str().unwrap(), "{name}");
            continue;
        }

        let snapshot = SessionSnapshot::decode(wire).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(u64::from(snapshot.version), expect["version"].as_u64().unwrap(), "{name}");
        assert_eq!(snapshot.session_id, expect["session_id"].as_str().unwrap(), "{name}");

        let streams: Vec<serde_json::Value> =
            snapshot.cursors.keys().map(|s| serde_json::Value::from(s.as_str())).collect();
        assert_eq!(&streams, expect["cursor_streams"].as_array().unwrap(), "{name}");
        if let Some(sequences) = expect.get("cursor_sequences").and_then(|v| v.as_object()) {
            let decoded = snapshot.decoded_cursors().unwrap();
            for (stream, sequence) in sequences {
                assert_eq!(
                    decoded[stream].sequence.to_string(),
                    sequence.as_str().unwrap(),
                    "{name}"
                );
            }
        }

        // A canonical case re-encodes to its own bytes: what the broker
        // stores is exactly what the fixture pins.
        if expect["canonical"].as_bool().unwrap_or(false) {
            assert_eq!(snapshot.to_json().unwrap(), wire, "{name}: storage encoding is pinned");
        }
    }
}
