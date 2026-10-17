use super::*;

use std::assert_matches;

const NOW: i64 = 1_800_000_000;

fn valid() -> StreamClaims {
    StreamClaims::subscribe("app", "chat:42", NOW + 60)
}

#[test]
fn roundtrip_emits_version_one() {
    let claims = valid();
    let json = serde_json::to_value(&claims).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["namespace"], "app");
    assert_eq!(json["stream"], "chat:42");
    assert_eq!(json["purpose"], "subscribe");
    assert_eq!(json["expires_at"], NOW + 60);

    let decoded: StreamClaims = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, claims);
    decoded.validate_at(NOW).unwrap();
}

#[test]
fn missing_version_defaults_to_one() {
    let json = r#"{"namespace":"app","stream":"chat:42",
                       "purpose":"subscribe","expires_at":1800000060}"#;
    let decoded: StreamClaims = serde_json::from_str(json).unwrap();
    assert_eq!(decoded.version, 1);
    decoded.validate_at(NOW).unwrap();
}

#[test]
fn unknown_fields_are_ignored() {
    let json = r#"{"version":1,"namespace":"app","stream":"chat:42",
                       "purpose":"subscribe","expires_at":1800000060,
                       "issued_at":1800000000,"future":{"a":[1]}}"#;
    let decoded: StreamClaims = serde_json::from_str(json).unwrap();
    decoded.validate_at(NOW).unwrap();
}

#[test]
fn unsupported_version_is_rejected() {
    let mut claims = valid();
    claims.version = 99;
    assert_matches!(claims.validate_at(NOW).unwrap_err(), ProtocolError::UnsupportedVersion(99));
}

#[test]
fn wrong_field_type_is_rejected() {
    let json = r#"{"version":1,"expires_at":"soon"}"#;
    let err = serde_json::from_str::<StreamClaims>(json).unwrap_err();
    assert!(err.is_data());
}

#[test]
fn empty_namespace_or_stream_is_rejected() {
    let mut claims = valid();
    claims.namespace = String::new();
    assert_matches!(
        claims.validate_at(NOW).unwrap_err(),
        ProtocolError::EmptyField { field: "namespace" }
    );

    let mut claims = valid();
    claims.stream = String::new();
    assert_matches!(
        claims.validate_at(NOW).unwrap_err(),
        ProtocolError::EmptyField { field: "stream" }
    );
}

#[test]
fn wrong_purpose_is_rejected() {
    let mut claims = valid();
    claims.purpose = "publish".to_string();
    assert_matches!(
        claims.validate_at(NOW).unwrap_err(),
        ProtocolError::InvalidPurpose { expected: PURPOSE_SUBSCRIBE, .. }
    );
}

#[test]
fn expired_claims_are_rejected() {
    let claims = StreamClaims::subscribe("app", "chat:42", NOW - 1);
    assert_matches!(
        claims.validate_at(NOW).unwrap_err(),
        ProtocolError::Expired { expires_at, now } if expires_at == NOW - 1 && now == NOW);
}

#[test]
fn absent_claims_default_to_invalid() {
    // Nothing but a version: parses, but grants nothing.
    let decoded: StreamClaims = serde_json::from_str("{}").unwrap();
    assert_eq!(decoded.version, 1);
    assert_eq!(decoded.expires_at, 0);
    assert_eq!(decoded.purpose, "");
    assert!(decoded.validate_at(NOW).is_err());
    assert!(!decoded.authorizes("", "", NOW));
}

#[test]
fn whisper_and_kid_are_optional_and_default_to_nothing() {
    // Absent on the wire: no capability, no key hint, and nothing emitted.
    let claims = valid();
    assert!(!claims.whisper);
    assert_eq!(claims.kid, None);
    let json = serde_json::to_value(&claims).unwrap();
    assert!(json.get("whisper").is_none(), "an unset capability adds no bytes");
    assert!(json.get("kid").is_none());

    // Present: carried through, emitted after the authorization claims so
    // the producer's fixed key order is untouched.
    let mut claims = valid().with_whisper(true);
    claims.kid = Some("k2".to_string());
    let text = serde_json::to_string(&claims).unwrap();
    assert!(text.ends_with(r#""whisper":true,"kid":"k2"}"#), "{text}");
    let decoded: StreamClaims = serde_json::from_str(&text).unwrap();
    assert_eq!(decoded, claims);
    decoded.validate_at(NOW).unwrap();

    // `whisper: false` on the wire is the same as absent.
    let explicit: StreamClaims = serde_json::from_str(
        r#"{"version":1,"namespace":"app","stream":"chat:42","purpose":"subscribe",
            "expires_at":1800000060,"whisper":false}"#,
    )
    .unwrap();
    assert_eq!(explicit, valid());

    // A capability claim with the wrong type is a decode error, not `true`.
    assert!(
        serde_json::from_str::<StreamClaims>(r#"{"version":1,"whisper":"yes"}"#)
            .unwrap_err()
            .is_data()
    );
}

#[test]
fn whisper_never_widens_the_stream_or_namespace() {
    let claims = valid().with_whisper(true);
    assert!(claims.authorizes("app", "chat:42", NOW));
    assert!(!claims.authorizes("app", "chat:43", NOW));
    assert!(!claims.authorizes("other", "chat:42", NOW));
}

#[test]
fn authorizes_requires_matching_namespace_and_stream() {
    let claims = valid();
    assert!(claims.authorizes("app", "chat:42", NOW));
    assert!(!claims.authorizes("other", "chat:42", NOW));
    assert!(!claims.authorizes("app", "chat:43", NOW));
    // Valid grant, but the clock says it is over.
    assert!(!claims.authorizes("app", "chat:42", NOW + 61));
}
