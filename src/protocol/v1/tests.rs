use super::*;

use std::assert_matches;

#[test]
fn default_version_is_one() {
    assert_eq!(default_version(), 1);
    assert!(validate_version(1).is_ok());
}

#[test]
fn validate_version_rejects_unsupported() {
    let err = validate_version(99).unwrap_err();
    assert_matches!(err, ProtocolError::UnsupportedVersion(99));
}

#[test]
fn validate_namespace_rejects_empty() {
    let err = validate_namespace("").unwrap_err();
    assert_matches!(err, ProtocolError::EmptyField { field: "namespace" });
}

#[test]
fn validate_stream_rejects_empty() {
    let err = validate_stream("").unwrap_err();
    assert_matches!(err, ProtocolError::EmptyField { field: "stream" });
}

#[test]
fn validate_bounded_rejects_oversized() {
    let too_long = "n".repeat(MAX_NAMESPACE_LEN + 1);
    let err = validate_namespace(&too_long).unwrap_err();
    assert_matches!(err, ProtocolError::FieldTooLong { field: "namespace", .. });
}

#[test]
fn validate_uuid_accepts_v4_and_rejects_junk() {
    let id = uuid::Uuid::new_v4().to_string();
    assert!(validate_uuid("id", &id).is_ok());

    let err = validate_uuid("id", "not-a-uuid").unwrap_err();
    assert_matches!(err, ProtocolError::InvalidUuid { field: "id", .. });

    let err = validate_uuid("id", "").unwrap_err();
    assert_matches!(err, ProtocolError::EmptyField { field: "id" });
}

#[test]
fn validate_json_text_checks_without_reencoding() {
    assert!(validate_json_text("json_payload", r#"{"a":1.0000000000000002}"#).is_ok());

    let err = validate_json_text("json_payload", "{oops").unwrap_err();
    assert_matches!(err, ProtocolError::InvalidJson { field: "json_payload", .. });

    let err = validate_json_text("json_payload", "").unwrap_err();
    assert_matches!(err, ProtocolError::EmptyField { field: "json_payload" });
}

#[test]
fn validate_not_expired_is_exclusive() {
    assert!(validate_not_expired(101, 100).is_ok());
    assert!(validate_not_expired(100, 100).is_err());
    // An absent expiry defaults to 0, which is always expired.
    assert!(validate_not_expired(0, 100).is_err());
}

#[test]
fn now_unix_is_after_2020() {
    assert!(now_unix() > 1_577_836_800);
}
