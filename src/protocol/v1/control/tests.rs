use super::*;

use crate::protocol::v1::rpc::{ALLOWED_CLOSE_REASONS, CLOSE_REASON_REMOTE};
use std::assert_matches;

fn valid() -> ControlMessage {
    ControlMessage::disconnect("app", "user:7", CLOSE_REASON_REMOTE, false)
}

#[test]
fn roundtrip_emits_version_one() {
    let control = valid();
    let json = serde_json::to_value(&control).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["kind"], "disconnect");
    assert_eq!(json["namespace"], "app");
    assert_eq!(json["identity_key"], "user:7");
    assert_eq!(json["reason"], "remote");
    assert_eq!(json["reconnect"], false);
    assert!(json["control_id"].is_string());

    let decoded: ControlMessage = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, control);
    decoded.validate().unwrap();
    assert!(decoded.applies_to("app"));
    assert!(!decoded.applies_to("other"));
}

#[test]
fn missing_version_defaults_to_one() {
    let json = format!(
        r#"{{"kind":"disconnect","namespace":"app","control_id":"{}",
                 "identity_key":"user:7","reason":"remote"}}"#,
        uuid::Uuid::new_v4()
    );
    let decoded: ControlMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.version, 1);
    assert!(!decoded.reconnect, "an absent reconnect policy must not invite a reconnect");
    decoded.validate().unwrap();
}

#[test]
fn unknown_fields_are_ignored() {
    let json = format!(
        r#"{{"version":1,"kind":"disconnect","namespace":"app","control_id":"{}",
                 "identity_key":"user:7","reason":"remote","issued_at":123,
                 "future":{{"a":[1]}}}}"#,
        uuid::Uuid::new_v4()
    );
    let decoded: ControlMessage = serde_json::from_str(&json).unwrap();
    decoded.validate().unwrap();
}

#[test]
fn unknown_kind_is_a_hard_error() {
    let json = r#"{"version":1,"kind":"self_destruct","namespace":"app"}"#;
    let err = serde_json::from_str::<ControlMessage>(json).unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");
}

#[test]
fn missing_kind_is_a_hard_error() {
    let err = serde_json::from_str::<ControlMessage>(r#"{"version":1}"#).unwrap_err();
    assert!(err.is_data());
}

#[test]
fn unsupported_version_is_rejected() {
    let mut control = valid();
    control.version = 99;
    assert_matches!(control.validate().unwrap_err(), ProtocolError::UnsupportedVersion(99));
}

#[test]
fn wrong_field_type_is_rejected() {
    let json = r#"{"version":1,"kind":"disconnect","reconnect":"yes"}"#;
    let err = serde_json::from_str::<ControlMessage>(json).unwrap_err();
    assert!(err.is_data());
}

#[test]
fn empty_namespace_identity_key_or_control_id_is_rejected() {
    let mut control = valid();
    control.namespace = String::new();
    assert_matches!(
        control.validate().unwrap_err(),
        ProtocolError::EmptyField { field: "namespace" }
    );

    let mut control = valid();
    control.identity_key = String::new();
    assert_matches!(
        control.validate().unwrap_err(),
        ProtocolError::EmptyField { field: "identity_key" }
    );

    let mut control = valid();
    control.control_id = "42".to_string();
    assert_matches!(
        control.validate().unwrap_err(),
        ProtocolError::InvalidUuid { field: "control_id", .. }
    );
}

#[test]
fn reason_must_be_allowlisted() {
    for reason in ALLOWED_CLOSE_REASONS {
        let mut control = valid();
        control.reason = (*reason).to_string();
        control.validate().unwrap();
    }

    let mut control = valid();
    control.reason = "vibes".to_string();
    assert_matches!(
        control.validate().unwrap_err(),
        ProtocolError::DisallowedValue { field: "reason", .. }
    );
}

#[test]
fn oversized_identity_key_is_rejected() {
    let mut control = valid();
    control.identity_key = "k".repeat(MAX_IDENTITY_KEY_LEN + 1);
    assert_matches!(
        control.validate().unwrap_err(),
        ProtocolError::FieldTooLong { field: "identity_key", .. }
    );
}

#[test]
fn defaults_are_still_semantically_invalid() {
    // Only the required `kind`: parses, but authorizes nothing.
    let decoded: ControlMessage = serde_json::from_str(r#"{"kind":"disconnect"}"#).unwrap();
    assert_eq!(decoded.version, 1);
    assert!(decoded.validate().is_err());
    assert!(!decoded.applies_to(""));
}
