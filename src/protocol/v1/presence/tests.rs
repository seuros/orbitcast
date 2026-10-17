use super::*;

use std::assert_matches;

fn metadata(pairs: &[(&str, &str)]) -> Map<String, Value> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), Value::from(*v))).collect()
}

// --- commands -----------------------------------------------------------

#[test]
fn command_roundtrip_emits_version_one() {
    let command = PresenceCommand::new(PresenceOp::Join);
    let json = serde_json::to_value(&command).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["action"], PRESENCE_ACTION);
    assert_eq!(json["op"], "join");
    // An absent page window is omitted, never emitted as null.
    assert!(json.get("limit").is_none());
    assert!(json.get("offset").is_none());

    let decoded: PresenceCommand = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, command);
    decoded.validate().unwrap();
}

#[test]
fn command_missing_version_defaults_to_one() {
    let decoded =
        PresenceCommand::decode(r#"{"action":"orbitcast_presence","op":"leave"}"#).unwrap();
    assert_eq!(decoded.version, 1);
    assert_eq!(decoded.op, PresenceOp::Leave);
}

#[test]
fn command_ignores_unknown_fields_including_a_forged_identity() {
    // The two fields a hostile client would most like to control are
    // exactly the two this envelope does not have.
    let decoded = PresenceCommand::decode(
        r#"{"version":1,"action":"orbitcast_presence","op":"join",
                "subject_id":"user:999","metadata":{"role":"admin"},"trace":1}"#,
    )
    .unwrap();
    assert_eq!(decoded.op, PresenceOp::Join);

    let reencoded = serde_json::to_value(&decoded).unwrap();
    assert!(reencoded.get("subject_id").is_none(), "identity is never carried from a client");
    assert!(reencoded.get("metadata").is_none(), "metadata is never carried from a client");
}

#[test]
fn unknown_op_is_a_hard_error() {
    let err = serde_json::from_str::<PresenceOp>(r#""lurk""#).unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");

    let err =
        PresenceCommand::decode(r#"{"action":"orbitcast_presence","op":"lurk"}"#).unwrap_err();
    assert_matches!(err, ProtocolError::Malformed(_));
}

#[test]
fn absent_op_is_a_hard_error() {
    let err = PresenceCommand::decode(r#"{"action":"orbitcast_presence"}"#).unwrap_err();
    assert_matches!(err, ProtocolError::Malformed(_));
}

#[test]
fn a_foreign_action_is_rejected_semantically() {
    let err = PresenceCommand::decode(r#"{"action":"speak","op":"join"}"#).unwrap_err();
    assert_matches!(err, ProtocolError::DisallowedValue { field: "action", .. });
}

#[test]
fn command_rejects_unsupported_version() {
    let mut command = PresenceCommand::new(PresenceOp::Snapshot);
    command.version = 99;
    assert_matches!(command.validate().unwrap_err(), ProtocolError::UnsupportedVersion(99));
}

#[test]
fn page_windows_are_bounded_and_defaulted() {
    let default = PresenceCommand::new(PresenceOp::Snapshot);
    assert_eq!(default.page(), PresencePage { limit: DEFAULT_PAGE_SIZE, offset: 0 });

    let paged = PresenceCommand::new(PresenceOp::Snapshot).with_page(10, 20);
    paged.validate().unwrap();
    assert_eq!(paged.page(), PresencePage { limit: 10, offset: 20 });

    let zero = PresenceCommand::new(PresenceOp::Snapshot).with_page(0, 0);
    assert_matches!(zero.validate().unwrap_err(), ProtocolError::OutOfRange { field: "limit", .. });

    let huge = PresenceCommand::new(PresenceOp::Snapshot).with_page(MAX_PAGE_SIZE + 1, 0);
    assert_matches!(huge.validate().unwrap_err(), ProtocolError::OutOfRange { field: "limit", .. });

    let deep = PresenceCommand::new(PresenceOp::Snapshot).with_page(10, MAX_PAGE_OFFSET + 1);
    assert_matches!(
        deep.validate().unwrap_err(),
        ProtocolError::OutOfRange { field: "offset", .. }
    );
}

#[test]
fn the_action_probe_routes_only_our_namespace() {
    assert!(is_presence_action(r#"{"action":"orbitcast_presence","op":"join"}"#));
    assert!(!is_presence_action(r#"{"action":"speak","text":"hi"}"#));
    assert!(!is_presence_action(r#"{"no_action":true}"#));
    assert!(!is_presence_action("{oops"), "unparseable data is never routed to presence");
    assert!(!is_presence_action(r#"{"action":123}"#), "a non-string action is not ours");
}

#[test]
fn operations_share_one_spelling_with_the_grant_allowlist() {
    for op in [PresenceOp::Join, PresenceOp::Leave, PresenceOp::Snapshot] {
        assert_eq!(PresenceOp::from_wire(op.as_str()), Some(op));
        assert_eq!(serde_json::to_value(op).unwrap(), op.as_str());
    }
    assert_eq!(PresenceOp::from_wire("administrate"), None);
}

// --- messages -----------------------------------------------------------

#[test]
fn join_message_roundtrips_with_its_subject_and_metadata() {
    let message = PresenceMessage::join("chat:42", "user:7", metadata(&[("name", "Marissa")]), 3);
    message.validate().unwrap();

    let json = serde_json::to_value(&message).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["type"], "presence");
    assert_eq!(json["event"], "join");
    assert_eq!(json["stream"], "chat:42");
    assert_eq!(json["subject_id"], "user:7");
    assert_eq!(json["metadata"]["name"], "Marissa");
    assert_eq!(json["total"], 3);
    assert!(json.get("members").is_none(), "a transition carries no roster");
    assert!(json.get("page").is_none());

    let decoded: PresenceMessage = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, message);
}

#[test]
fn leave_message_carries_no_metadata() {
    let message = PresenceMessage::leave("chat:42", "user:7", 0);
    message.validate().unwrap();

    let json = serde_json::to_value(&message).unwrap();
    assert_eq!(json["event"], "leave");
    assert_eq!(json["total"], 0);
    assert!(json.get("metadata").is_none());
}

#[test]
fn snapshot_message_carries_its_page_window() {
    let members = vec![
        PresenceMember { subject_id: "user:1".into(), metadata: Map::new() },
        PresenceMember { subject_id: "user:2".into(), metadata: metadata(&[("tz", "UTC")]) },
    ];
    let page = PresencePageInfo { limit: 2, offset: 0, has_more: true };
    let message = PresenceMessage::snapshot("chat:42", members, 7, page);
    message.validate().unwrap();

    let json = serde_json::to_value(&message).unwrap();
    assert_eq!(json["event"], "snapshot");
    assert_eq!(json["total"], 7);
    assert_eq!(json["members"][1]["subject_id"], "user:2");
    assert_eq!(json["page"]["has_more"], true);
    assert!(json.get("subject_id").is_none());

    let decoded: PresenceMessage = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, message);
    assert_eq!(decoded.to_json().unwrap(), message.to_json().unwrap());
}

#[test]
fn messages_fail_closed_on_missing_or_oversized_parts() {
    let mut message = PresenceMessage::join("chat:42", "user:7", Map::new(), 1);
    message.subject_id = None;
    assert_matches!(
        message.validate().unwrap_err(),
        ProtocolError::EmptyField { field: "subject_id" }
    );

    let mut message = PresenceMessage::join("", "user:7", Map::new(), 1);
    assert_matches!(message.validate().unwrap_err(), ProtocolError::EmptyField { field: "stream" });
    message.stream = "chat:42".to_string();
    message.message_type = "chat".to_string();
    assert_matches!(
        message.validate().unwrap_err(),
        ProtocolError::DisallowedValue { field: "type", .. }
    );

    let oversized =
        Map::from_iter([("bio".to_string(), Value::from("x".repeat(MAX_METADATA_LEN)))]);
    let message = PresenceMessage::join("chat:42", "user:7", oversized, 1);
    assert_matches!(
        message.validate().unwrap_err(),
        ProtocolError::FieldTooLong { field: "metadata", .. }
    );

    let mut message = PresenceMessage::snapshot(
        "chat:42",
        vec![PresenceMember { subject_id: "user:1".into(), metadata: Map::new() }],
        1,
        PresencePageInfo { limit: 1, offset: 0, has_more: false },
    );
    message.page = None;
    assert_matches!(
        message.validate().unwrap_err(),
        ProtocolError::MissingPayload { operation: "snapshot", field: "page" }
    );
}

#[test]
fn a_snapshot_page_may_not_exceed_its_own_limit() {
    let members = (0..3)
        .map(|index| PresenceMember { subject_id: format!("user:{index}"), metadata: Map::new() })
        .collect();
    let message = PresenceMessage::snapshot(
        "chat:42",
        members,
        3,
        PresencePageInfo { limit: 2, offset: 0, has_more: false },
    );
    assert_matches!(
        message.validate().unwrap_err(),
        ProtocolError::TooMany { field: "members", max: 2, .. }
    );
}

#[test]
fn message_ignores_unknown_fields_and_defaults_its_version() {
    let decoded: PresenceMessage = serde_json::from_str(
        r#"{"type":"presence","event":"leave","stream":"chat:42",
                "subject_id":"user:7","total":0,"server_time":123}"#,
    )
    .unwrap();
    assert_eq!(decoded.version, 1);
    decoded.validate().unwrap();
}

#[test]
fn error_messages_carry_a_reason_and_only_what_they_know() {
    // A refusal that resolved to one roster and one operation.
    let message = PresenceMessage::error(
        "operation_not_allowed",
        Some(PresenceOp::Snapshot),
        Some("chat:42"),
    );
    message.validate().unwrap();
    let json = serde_json::to_value(&message).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["type"], "presence");
    assert_eq!(json["event"], "error");
    assert_eq!(json["reason"], "operation_not_allowed");
    assert_eq!(json["op"], "snapshot");
    assert_eq!(json["stream"], "chat:42");
    assert_eq!(json["total"], 0);
    assert!(json.get("subject_id").is_none());
    assert!(json.get("members").is_none(), "a refusal carries no roster");
    assert!(json.get("page").is_none());
    let decoded: PresenceMessage = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, message);

    // A refusal before any roster or operation was known omits both.
    let bare = PresenceMessage::error("invalid_command", None, None);
    bare.validate().unwrap();
    let json = serde_json::to_value(&bare).unwrap();
    assert!(json.get("stream").is_none(), "no roster to name");
    assert!(json.get("op").is_none(), "no operation to name");
    let decoded: PresenceMessage = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, bare);
    assert_eq!(decoded.to_json().unwrap(), bare.to_json().unwrap());

    // Roster events still need their stream; only an error may omit it.
    let mut no_stream = PresenceMessage::leave("", "user:7", 0);
    assert_matches!(
        no_stream.validate().unwrap_err(),
        ProtocolError::EmptyField { field: "stream" }
    );
    no_stream.event = PresenceEvent::Error;
    no_stream.reason = Some("not_granted".to_string());
    no_stream.validate().unwrap();
}

#[test]
fn error_messages_fail_closed_without_a_usable_reason() {
    let mut message = PresenceMessage::error("backend", None, Some("chat:42"));
    message.reason = None;
    assert_matches!(message.validate().unwrap_err(), ProtocolError::EmptyField { field: "reason" });

    message.reason = Some(String::new());
    assert_matches!(message.validate().unwrap_err(), ProtocolError::EmptyField { field: "reason" });

    message.reason = Some("x".repeat(MAX_REASON_LEN + 1));
    assert_matches!(
        message.validate().unwrap_err(),
        ProtocolError::FieldTooLong { field: "reason", .. }
    );

    // An error with a roster attached is not an error message.
    let mut with_roster = PresenceMessage::error("backend", None, Some("chat:42"));
    with_roster.members.push(PresenceMember { subject_id: "user:1".into(), metadata: Map::new() });
    assert_matches!(
        with_roster.validate().unwrap_err(),
        ProtocolError::NotAllowedForOutcome { field: "members", outcome: "error" }
    );

    // The stream, when given, is still a stream.
    let oversized =
        PresenceMessage::error("backend", None, Some(&"s".repeat(v1::MAX_STREAM_LEN + 1)));
    assert_matches!(
        oversized.validate().unwrap_err(),
        ProtocolError::FieldTooLong { field: "stream", .. }
    );
}

#[test]
fn unknown_event_is_a_hard_error() {
    let err = serde_json::from_str::<PresenceMessage>(
        r#"{"type":"presence","event":"vanish","stream":"chat:42"}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");
}
