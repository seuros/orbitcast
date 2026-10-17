use super::*;

use std::assert_matches;

fn sample() -> Publication {
    Publication::new("app", "chat:42", r#"{"body":"hi"}"#)
}

#[test]
fn publication_roundtrip_emits_version_one() {
    let publication = sample();
    let json = serde_json::to_value(&publication).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["namespace"], "app");
    assert_eq!(json["stream"], "chat:42");
    assert_eq!(json["json_payload"], r#"{"body":"hi"}"#);

    let decoded: Publication = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, publication);
    decoded.validate().unwrap();
}

#[test]
fn publication_preserves_exact_payload_text() {
    // Key order, spacing, and float formatting must survive untouched.
    let text = r#"{ "b":1.0000000000000002,  "a":"é" }"#;
    let publication = Publication::new("app", "chat:42", text);
    let encoded = serde_json::to_string(&publication).unwrap();
    let decoded: Publication = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.json_payload, text);
    decoded.validate().unwrap();
}

#[test]
fn ingress_ack_roundtrips_with_version_one() {
    let ack = IngressAck::new(uuid::Uuid::new_v4().to_string());
    ack.validate().unwrap();

    let json = serde_json::to_value(&ack).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["publication_id"], ack.publication_id);
    let mut keys: Vec<&str> = json.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["publication_id", "version"], "the ack stays minimal");

    let decoded: IngressAck = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, ack);
}

#[test]
fn ingress_ack_requires_a_publication_uuid_and_a_known_version() {
    let missing: IngressAck = serde_json::from_str("{}").unwrap();
    assert_eq!(missing.version, 1, "absent version reads as v1");
    assert_matches!(
        missing.validate().unwrap_err(),
        ProtocolError::EmptyField { field: "publication_id" }
    );

    let mut junk = IngressAck::new("42");
    junk.version = 1;
    assert_matches!(
        junk.validate().unwrap_err(),
        ProtocolError::InvalidUuid { field: "publication_id", .. }
    );

    let mut ack = IngressAck::new(uuid::Uuid::new_v4().to_string());
    ack.version = 99;
    assert_matches!(ack.validate().unwrap_err(), ProtocolError::UnsupportedVersion(99));
}

#[test]
fn publication_missing_version_defaults_to_one() {
    let json = format!(
        r#"{{"namespace":"app","id":"{}","stream":"chat:42","json_payload":"{{}}"}}"#,
        uuid::Uuid::new_v4()
    );
    let decoded: Publication = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.version, 1);
    decoded.validate().unwrap();
}

#[test]
fn publication_ignores_unknown_fields() {
    let json = format!(
        r#"{{"version":1,"namespace":"app","id":"{}","stream":"chat:42",
                 "json_payload":"{{}}","published_from":"node-7","future":[1,2]}}"#,
        uuid::Uuid::new_v4()
    );
    let decoded: Publication = serde_json::from_str(&json).unwrap();
    decoded.validate().unwrap();
    assert_eq!(decoded.namespace, "app");
}

#[test]
fn publication_rejects_unsupported_version() {
    let mut publication = sample();
    publication.version = 99;
    let err = publication.validate().unwrap_err();
    assert_matches!(err, ProtocolError::UnsupportedVersion(99));
}

#[test]
fn publication_rejects_wrong_field_type() {
    let err = serde_json::from_str::<Publication>(r#"{"version":1,"stream":42}"#).unwrap_err();
    assert!(err.is_data());
}

#[test]
fn publication_rejects_empty_namespace_and_stream() {
    let mut publication = sample();
    publication.namespace = String::new();
    assert_matches!(
        publication.validate().unwrap_err(),
        ProtocolError::EmptyField { field: "namespace" }
    );

    let mut publication = sample();
    publication.stream = String::new();
    assert_matches!(
        publication.validate().unwrap_err(),
        ProtocolError::EmptyField { field: "stream" }
    );
}

#[test]
fn publication_rejects_non_uuid_id_and_invalid_payload() {
    let mut publication = sample();
    publication.id = "42".to_string();
    assert_matches!(
        publication.validate().unwrap_err(),
        ProtocolError::InvalidUuid { field: "id", .. }
    );

    let mut publication = sample();
    publication.json_payload = "{not json".to_string();
    assert_matches!(
        publication.validate().unwrap_err(),
        ProtocolError::InvalidJson { field: "json_payload", .. }
    );
}

#[test]
fn publication_defaults_are_still_semantically_invalid() {
    // Every field absent: parsing succeeds, validation fails closed.
    let decoded: Publication = serde_json::from_str("{}").unwrap();
    assert_eq!(decoded.version, 1);
    assert!(decoded.validate().is_err());
}

#[test]
fn inline_notice_roundtrip() {
    let notice = PublicationNotice::inline(sample());
    let json = serde_json::to_value(&notice).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["kind"], "inline");
    assert_eq!(json["publication"]["stream"], "chat:42");

    let decoded: PublicationNotice = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, notice);
    decoded.validate().unwrap();
    assert_eq!(decoded.namespace(), "app");
    assert_eq!(decoded.stream(), "chat:42");
}

#[test]
fn overflow_notice_roundtrip() {
    let publication = sample();
    let notice = PublicationNotice::overflow(OverflowRef::for_publication(&publication));
    let json = serde_json::to_value(&notice).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["kind"], "overflow");
    assert_eq!(json["id"], publication.id);
    assert_eq!(json["namespace"], "app");
    assert_eq!(json["stream"], "chat:42");

    let decoded: PublicationNotice = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, notice);
    decoded.validate().unwrap();
    assert_eq!(decoded.stream(), "chat:42");
}

#[test]
fn notice_missing_version_defaults_to_one() {
    let json = format!(
        r#"{{"kind":"overflow","id":"{}","namespace":"app","stream":"chat:42"}}"#,
        uuid::Uuid::new_v4()
    );
    let decoded: PublicationNotice = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.version, 1);
    decoded.validate().unwrap();
}

#[test]
fn notice_ignores_unknown_fields() {
    let json = format!(
        r#"{{"version":1,"kind":"overflow","id":"{}","namespace":"app",
                 "stream":"chat:42","hint":"warm","future":{{"a":1}}}}"#,
        uuid::Uuid::new_v4()
    );
    let decoded: PublicationNotice = serde_json::from_str(&json).unwrap();
    decoded.validate().unwrap();
}

#[test]
fn notice_rejects_unsupported_version() {
    let mut notice = PublicationNotice::inline(sample());
    notice.version = 99;
    assert_matches!(notice.validate().unwrap_err(), ProtocolError::UnsupportedVersion(99));
}

#[test]
fn notice_rejects_unknown_kind() {
    let json = r#"{"version":1,"kind":"telepathy","id":"x"}"#;
    let err = serde_json::from_str::<PublicationNotice>(json).unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");
}

#[test]
fn notice_rejects_missing_kind() {
    let err = serde_json::from_str::<PublicationNotice>(r#"{"version":1}"#).unwrap_err();
    assert!(err.is_data());
}

#[test]
fn notice_rejects_empty_namespace_and_stream() {
    let json = format!(
        r#"{{"version":1,"kind":"overflow","id":"{}","stream":"chat:42"}}"#,
        uuid::Uuid::new_v4()
    );
    let decoded: PublicationNotice = serde_json::from_str(&json).unwrap();
    assert_matches!(
        decoded.validate().unwrap_err(),
        ProtocolError::EmptyField { field: "namespace" }
    );

    let json = format!(
        r#"{{"version":1,"kind":"overflow","id":"{}","namespace":"app"}}"#,
        uuid::Uuid::new_v4()
    );
    let decoded: PublicationNotice = serde_json::from_str(&json).unwrap();
    assert_matches!(decoded.validate().unwrap_err(), ProtocolError::EmptyField { field: "stream" });
}

#[test]
fn notify_payload_limit_is_exclusive() {
    let small = serde_json::to_string(&PublicationNotice::inline(sample())).unwrap();
    assert!(PublicationNotice::fits_notify_payload(&small));

    let big = "x".repeat(MAX_NOTIFY_PAYLOAD_LEN);
    assert!(!PublicationNotice::fits_notify_payload(&big));
}
