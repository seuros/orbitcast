use super::*;

use std::assert_matches;

const SESSION: &str = "5b1f1c8e-8c2e-4a1d-9e5b-1a2b3c4d5e6f";
const IDENTIFIER: &str = r#"{"channel":"ChatChannel","room":"42"}"#;

fn connection() -> ConnectionSnapshot {
    ConnectionSnapshot {
        url: "/cable?tab=1".to_string(),
        origin: Some("https://app.example".to_string()),
        client_address: "203.0.113.7".to_string(),
        headers: BTreeMap::from([("cookie".to_string(), "_session=abc".to_string())]),
        identity: Map::new(),
        state: Map::new(),
    }
}

fn request(operation: Operation) -> RpcRequest {
    RpcRequest::new(operation, "app", "node-7", SESSION, 1, 0, 1000)
}

fn identify() -> RpcRequest {
    request(Operation::Identify).with_connection(connection())
}

fn open_channel() -> RpcRequest {
    request(Operation::OpenChannel)
        .with_connection(connection())
        .with_subscription(SubscriptionSnapshot::new(IDENTIFIER))
}

// --- operations and outcomes -------------------------------------------

#[test]
fn operations_use_snake_case_wire_names() {
    for (operation, expected) in [
        (Operation::Describe, "describe"),
        (Operation::Identify, "identify"),
        (Operation::OpenChannel, "open_channel"),
        (Operation::CloseChannel, "close_channel"),
        (Operation::RunAction, "run_action"),
        (Operation::ReleaseConnection, "release_connection"),
    ] {
        assert_eq!(operation.as_str(), expected);
        assert_eq!(serde_json::to_value(operation).unwrap(), expected);
        let decoded: Operation = serde_json::from_value(expected.into()).unwrap();
        assert_eq!(decoded, operation);
    }

    assert!(!Operation::Describe.is_stateful());
    assert!(Operation::Identify.is_stateful());
}

#[test]
fn unknown_operation_is_a_hard_error() {
    let err = serde_json::from_str::<Operation>(r#""teleport""#).unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");
}

#[test]
fn absent_outcome_defaults_to_fault() {
    let json =
        format!(r#"{{"version":1,"operation":"identify","request_id":"{SESSION}","sequence":1}}"#);
    let reply: RpcReply = serde_json::from_str(&json).unwrap();
    assert_eq!(reply.outcome, Outcome::Fault);
    assert!(!reply.is_accepted());
    reply.validate().unwrap();
}

#[test]
fn unknown_outcome_is_a_hard_error() {
    let json = format!(
        r#"{{"version":1,"operation":"identify","outcome":"maybe","request_id":"{SESSION}"}}"#
    );
    let err = serde_json::from_str::<RpcReply>(&json).unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");
}

// --- requests ----------------------------------------------------------

#[test]
fn request_roundtrip_emits_version_one() {
    let request = identify();
    let json = serde_json::to_value(&request).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["operation"], "identify");
    assert_eq!(json["namespace"], "app");
    assert_eq!(json["node_id"], "node-7");
    assert_eq!(json["session_id"], SESSION);
    assert_eq!(json["sequence"], 1);
    assert_eq!(json["expected_revision"], 0);
    assert_eq!(json["deadline_ms"], 1000);
    assert_eq!(json["connection"]["url"], "/cable?tab=1");
    assert_eq!(json["connection"]["headers"]["cookie"], "_session=abc");
    // Payloads the operation does not use are omitted, not emitted as null.
    assert!(json.get("subscription").is_none());
    assert!(json.get("action").is_none());

    let decoded: RpcRequest = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, request);
    decoded.validate().unwrap();
}

#[test]
fn request_missing_version_defaults_to_one() {
    let mut json = serde_json::to_value(identify()).unwrap();
    json.as_object_mut().unwrap().remove("version");
    let decoded: RpcRequest = serde_json::from_value(json).unwrap();
    assert_eq!(decoded.version, 1);
    decoded.validate().unwrap();
}

#[test]
fn request_ignores_unknown_fields() {
    let mut json = serde_json::to_value(identify()).unwrap();
    let object = json.as_object_mut().unwrap();
    object.insert("trace_id".to_string(), "abc".into());
    object.insert("future".to_string(), serde_json::json!({ "a": [1, 2] }));
    let decoded: RpcRequest = serde_json::from_value(json).unwrap();
    decoded.validate().unwrap();
    assert_eq!(decoded.operation, Operation::Identify);
}

#[test]
fn request_rejects_unsupported_version_and_bad_uuids() {
    let mut request = identify();
    request.version = 99;
    assert_matches!(request.validate().unwrap_err(), ProtocolError::UnsupportedVersion(99));

    let mut request = identify();
    request.session_id = "not-a-uuid".to_string();
    assert_matches!(
        request.validate().unwrap_err(),
        ProtocolError::InvalidUuid { field: "session_id", .. }
    );
}

#[test]
fn request_rejects_missing_operation_payloads() {
    let bare = request(Operation::Identify);
    assert_matches!(
        bare.validate().unwrap_err(),
        ProtocolError::MissingPayload { operation: "identify", field: "connection" }
    );

    let missing_subscription = request(Operation::OpenChannel).with_connection(connection());
    assert_matches!(
        missing_subscription.validate().unwrap_err(),
        ProtocolError::MissingPayload { operation: "open_channel", field: "subscription" }
    );

    let missing_action = open_channel();
    let mut missing_action = missing_action;
    missing_action.operation = Operation::RunAction;
    assert_matches!(
        missing_action.validate().unwrap_err(),
        ProtocolError::MissingPayload { operation: "run_action", field: "action" }
    );
}

#[test]
fn request_rejects_unusable_deadline() {
    let mut request = identify();
    request.deadline_ms = 0;
    assert_matches!(
        request.validate().unwrap_err(),
        ProtocolError::OutOfRange { field: "deadline_ms", value: 0, .. }
    );

    request.deadline_ms = MAX_DEADLINE_MS + 1;
    assert_matches!(
        request.validate().unwrap_err(),
        ProtocolError::OutOfRange { field: "deadline_ms", .. }
    );
}

#[test]
fn request_rejects_hop_by_hop_and_uppercase_headers() {
    let mut request = identify();
    let connection = request.connection.as_mut().unwrap();
    connection.headers.insert("transfer-encoding".to_string(), "chunked".to_string());
    assert_matches!(
        request.validate().unwrap_err(),
        ProtocolError::DisallowedValue { field: "header_name", .. }
    );

    let mut request = identify();
    let connection = request.connection.as_mut().unwrap();
    connection.headers.clear();
    connection.headers.insert("Cookie".to_string(), "x".to_string());
    assert_matches!(
        request.validate().unwrap_err(),
        ProtocolError::DisallowedValue { field: "header_name", .. }
    );
}

#[test]
fn request_defaults_are_still_semantically_invalid() {
    let decoded: RpcRequest =
        serde_json::from_str(r#"{"operation":"identify"}"#).expect("operation is required");
    assert_eq!(decoded.version, 1);
    assert!(decoded.validate().is_err(), "an empty namespace must fail closed");
}

#[test]
fn subscription_snapshot_decodes_params_from_the_identifier() {
    let snapshot = SubscriptionSnapshot::new(IDENTIFIER);
    assert_eq!(snapshot.params["channel"], "ChatChannel");
    assert_eq!(snapshot.params["room"], "42");
    snapshot.validate().unwrap();

    // A non-object identifier is still a usable snapshot: params stay empty.
    let snapshot = SubscriptionSnapshot::new("not-json");
    assert!(snapshot.params.is_empty());
    snapshot.validate().unwrap();
}

#[test]
fn action_payload_requires_valid_json_text() {
    let mut request = open_channel();
    request.operation = Operation::RunAction;
    request.action = Some(ActionPayload {
        identifier: IDENTIFIER.to_string(),
        json_data: r#"{"action":"speak","text":"hi"}"#.to_string(),
    });
    request.validate().unwrap();

    request.action.as_mut().unwrap().json_data = "{oops".to_string();
    assert_matches!(
        request.validate().unwrap_err(),
        ProtocolError::InvalidJson { field: "json_data", .. }
    );
}

#[test]
fn release_payload_requires_an_allowlisted_reason() {
    let release = ReleasePayload {
        reason: CLOSE_REASON_REMOTE.to_string(),
        reconnect: false,
        subscriptions: vec![SubscriptionSnapshot::new(IDENTIFIER)],
    };
    let request =
        request(Operation::ReleaseConnection).with_connection(connection()).with_release(release);
    request.validate().unwrap();

    let mut request = request;
    request.release.as_mut().unwrap().reason = "because".to_string();
    assert_matches!(
        request.validate().unwrap_err(),
        ProtocolError::DisallowedValue { field: "reason", .. }
    );
}

// --- replies -----------------------------------------------------------

#[test]
fn reply_roundtrip_emits_version_one() {
    let request = open_channel();
    let mut reply = RpcReply::accepted(&request);
    reply.effects = vec![
        Effect::AddStream { identifier: IDENTIFIER.to_string(), stream: "chat:42".to_string() },
        Effect::Transmit {
            identifier: IDENTIFIER.to_string(),
            json_payload: r#"{"welcome":true}"#.to_string(),
        },
    ];

    let json = serde_json::to_value(&reply).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["operation"], "open_channel");
    assert_eq!(json["outcome"], "accepted");
    assert_eq!(json["effects"][0]["kind"], "add_stream");
    assert_eq!(json["effects"][1]["kind"], "transmit");
    // An unchanged state is omitted entirely, never emitted as `{}`.
    assert!(json.get("state").is_none());

    let decoded: RpcReply = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, reply);
    decoded.validate().unwrap();
    decoded.correlates_with(&request).unwrap();
}

#[test]
fn reply_missing_version_defaults_to_one() {
    let json = format!(
        r#"{{"operation":"identify","outcome":"accepted","request_id":"{SESSION}",
                 "sequence":1,"new_revision":1}}"#
    );
    let decoded: RpcReply = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.version, 1);
    decoded.validate().unwrap();
}

#[test]
fn reply_ignores_unknown_fields() {
    let json = format!(
        r#"{{"version":1,"operation":"identify","outcome":"accepted",
                 "request_id":"{SESSION}","sequence":1,"new_revision":1,
                 "server_time":123,"future":{{"a":1}}}}"#
    );
    let decoded: RpcReply = serde_json::from_str(&json).unwrap();
    decoded.validate().unwrap();
    assert!(decoded.is_accepted());
}

#[test]
fn reply_rejects_unsupported_version() {
    let mut reply = RpcReply::accepted(&identify());
    reply.version = 99;
    assert_matches!(reply.validate().unwrap_err(), ProtocolError::UnsupportedVersion(99));
}

#[test]
fn reply_rejects_wrong_field_type() {
    let json = r#"{"operation":"identify","sequence":"soon"}"#;
    let err = serde_json::from_str::<RpcReply>(json).unwrap_err();
    assert!(err.is_data());
}

#[test]
fn reply_rejects_missing_operation() {
    let err = serde_json::from_str::<RpcReply>(r#"{"version":1}"#).unwrap_err();
    assert!(err.is_data(), "operation is required: {err}");
}

// --- state trichotomy ---------------------------------------------------

#[test]
fn state_absent_null_and_empty_are_three_different_things() {
    let base = |state: &str| {
        format!(
            r#"{{"version":1,"operation":"identify","outcome":"accepted",
                     "request_id":"{SESSION}","sequence":1,"new_revision":1{state}}}"#
        )
    };

    let absent: RpcReply = serde_json::from_str(&base("")).unwrap();
    assert_eq!(absent.state, StateReplacement::Unchanged);
    assert!(absent.state.is_unchanged());
    assert!(!absent.state.is_clear());
    assert!(absent.state.map().is_none());

    let null: RpcReply = serde_json::from_str(&base(r#","state":null"#)).unwrap();
    assert_eq!(null.state, StateReplacement::Unchanged, "explicit null means unchanged");

    let empty: RpcReply = serde_json::from_str(&base(r#","state":{}"#)).unwrap();
    assert_eq!(empty.state, StateReplacement::clear());
    assert!(!empty.state.is_unchanged());
    assert!(empty.state.is_clear(), "an empty object clears the state");
    assert_eq!(empty.state.map(), Some(&Map::new()));

    let replaced: RpcReply = serde_json::from_str(&base(r#","state":{"cart_id":7}"#)).unwrap();
    assert!(!replaced.state.is_clear());
    assert_eq!(replaced.state.map().unwrap()["cart_id"], 7);

    for reply in [&absent, &null, &empty, &replaced] {
        reply.validate().unwrap();
    }
}

#[test]
fn state_of_a_wrong_type_is_a_hard_error() {
    let json = format!(
        r#"{{"version":1,"operation":"identify","outcome":"accepted",
                 "request_id":"{SESSION}","state":[1,2]}}"#
    );
    let err = serde_json::from_str::<RpcReply>(&json).unwrap_err();
    assert!(err.is_data(), "a list is not a state map: {err}");
}

#[test]
fn unchanged_state_is_omitted_but_a_cleared_state_is_emitted() {
    let mut reply = RpcReply::accepted(&identify());
    assert!(serde_json::to_value(&reply).unwrap().get("state").is_none());

    reply.state = StateReplacement::clear();
    let json = serde_json::to_value(&reply).unwrap();
    assert_eq!(json["state"], serde_json::json!({}), "a cleared state is an empty object");

    let decoded: RpcReply = serde_json::from_value(json).unwrap();
    assert!(decoded.state.is_clear());
}

// --- effects ------------------------------------------------------------

#[test]
fn every_effect_kind_roundtrips() {
    let effects = vec![
        Effect::AddStream { identifier: IDENTIFIER.into(), stream: "chat:42".into() },
        Effect::RemoveStream { identifier: IDENTIFIER.into(), stream: "chat:42".into() },
        Effect::RemoveAllStreams { identifier: IDENTIFIER.into() },
        Effect::Transmit { identifier: IDENTIFIER.into(), json_payload: "{}".into() },
        Effect::Close { reason: CLOSE_REASON_UNAUTHORIZED.into(), reconnect: false },
        Effect::PresenceGrant {
            identifier: IDENTIFIER.into(),
            stream: "chat:42".into(),
            subject_id: "user:7".into(),
            metadata: Map::new(),
            ttl_ms: 45_000,
            operations: vec!["join".into()],
        },
        Effect::PresenceRevoke {
            identifier: IDENTIFIER.into(),
            stream: "chat:42".into(),
            subject_id: "user:7".into(),
        },
    ];

    for effect in &effects {
        effect.validate().unwrap();
        let json = serde_json::to_value(effect).unwrap();
        assert_eq!(json["kind"], effect.kind());
        let decoded: Effect = serde_json::from_value(json).unwrap();
        assert_eq!(&decoded, effect);
    }

    assert_eq!(effects[0].identifier(), Some(IDENTIFIER));
    assert_eq!(effects[4].identifier(), None, "close addresses the connection");
}

#[test]
fn unknown_effect_kind_is_a_hard_error() {
    let json = r#"{"kind":"execute_ruby","source":"exit"}"#;
    let err = serde_json::from_str::<Effect>(json).unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");

    // And through the reply, so a single hostile effect fails the envelope.
    let json = format!(
        r#"{{"version":1,"operation":"open_channel","outcome":"accepted",
                 "request_id":"{SESSION}","effects":[{{"kind":"execute_ruby"}}]}}"#
    );
    let err = serde_json::from_str::<RpcReply>(&json).unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");
}

#[test]
fn effect_missing_kind_is_a_hard_error() {
    let err = serde_json::from_str::<Effect>(r#"{"identifier":"x"}"#).unwrap_err();
    assert!(err.is_data());
}

#[test]
fn effects_validate_their_identifiers_streams_and_payloads() {
    let empty_identifier = Effect::AddStream { identifier: String::new(), stream: "s".into() };
    assert_matches!(
        empty_identifier.validate().unwrap_err(),
        ProtocolError::EmptyField { field: "identifier" }
    );

    let empty_stream = Effect::AddStream { identifier: IDENTIFIER.into(), stream: String::new() };
    assert_matches!(
        empty_stream.validate().unwrap_err(),
        ProtocolError::EmptyField { field: "stream" }
    );

    let bad_payload =
        Effect::Transmit { identifier: IDENTIFIER.into(), json_payload: "{oops".into() };
    assert_matches!(
        bad_payload.validate().unwrap_err(),
        ProtocolError::InvalidJson { field: "json_payload", .. }
    );

    let oversize = Effect::Transmit {
        identifier: IDENTIFIER.into(),
        json_payload: format!("\"{}\"", "x".repeat(MAX_TRANSMIT_LEN)),
    };
    assert_matches!(
        oversize.validate().unwrap_err(),
        ProtocolError::FieldTooLong { field: "json_payload", .. }
    );
}

#[test]
fn close_effect_reason_must_be_allowlisted() {
    for reason in ALLOWED_CLOSE_REASONS {
        Effect::Close { reason: (*reason).to_string(), reconnect: true }.validate().unwrap();
    }

    let err =
        Effect::Close { reason: "you_are_fired".into(), reconnect: false }.validate().unwrap_err();
    assert_matches!(err, ProtocolError::DisallowedValue { field: "reason", .. });

    let err = Effect::Close { reason: String::new(), reconnect: false }.validate().unwrap_err();
    assert_matches!(err, ProtocolError::EmptyField { field: "reason" });
}

#[test]
fn presence_grant_without_a_ttl_fails_closed() {
    let effect = Effect::PresenceGrant {
        identifier: IDENTIFIER.into(),
        stream: "chat:42".into(),
        subject_id: "user:7".into(),
        metadata: Map::new(),
        ttl_ms: 0,
        operations: Vec::new(),
    };
    assert_matches!(
        effect.validate().unwrap_err(),
        ProtocolError::OutOfRange { field: "ttl_ms", .. }
    );
}

#[test]
fn too_many_effects_are_rejected() {
    let mut reply = RpcReply::accepted(&open_channel());
    reply.effects = (0..=MAX_EFFECTS)
        .map(|_| Effect::RemoveAllStreams { identifier: IDENTIFIER.to_string() })
        .collect();
    assert_matches!(
        reply.validate().unwrap_err(),
        ProtocolError::TooMany { field: "effects", max: MAX_EFFECTS, .. }
    );
}

// --- outcome scope and correlation --------------------------------------

#[test]
fn a_denied_reply_may_not_carry_effects_state_or_identity() {
    let request = open_channel();

    let mut reply = RpcReply::denied(&request, "not your room");
    reply.effects =
        vec![Effect::AddStream { identifier: IDENTIFIER.into(), stream: "chat:42".into() }];
    assert_matches!(
        reply.validate().unwrap_err(),
        ProtocolError::NotAllowedForOutcome { field: "effects", outcome: "denied" }
    );

    let mut reply = RpcReply::denied(&request, "no");
    reply.state = StateReplacement::clear();
    assert_matches!(
        reply.validate().unwrap_err(),
        ProtocolError::NotAllowedForOutcome { field: "state", outcome: "denied" }
    );

    let mut reply = RpcReply::denied(&request, "no");
    reply.identity.insert("user_id".to_string(), 7.into());
    assert_matches!(
        reply.validate().unwrap_err(),
        ProtocolError::NotAllowedForOutcome { field: "identity", outcome: "denied" }
    );

    // A plain denial is valid, and is a decision rather than an error.
    let reply = RpcReply::denied(&request, "not your room");
    reply.validate().unwrap();
    reply.correlates_with(&request).unwrap();
    assert!(!reply.is_accepted());
}

#[test]
fn correlation_rejects_a_reply_for_another_request() {
    let request = identify();
    let other = identify();

    let mut reply = RpcReply::accepted(&request);
    reply.request_id = other.request_id;
    assert_matches!(
        reply.correlates_with(&request).unwrap_err(),
        ProtocolError::Mismatch { field: "request_id", .. }
    );

    let mut reply = RpcReply::accepted(&request);
    reply.operation = Operation::RunAction;
    assert_matches!(
        reply.correlates_with(&request).unwrap_err(),
        ProtocolError::Mismatch { field: "operation", .. }
    );

    let mut reply = RpcReply::accepted(&request);
    reply.sequence += 1;
    assert_matches!(
        reply.correlates_with(&request).unwrap_err(),
        ProtocolError::Mismatch { field: "sequence", .. }
    );
}

#[test]
fn correlation_rejects_a_revision_that_moved_backwards() {
    let mut request = identify();
    request.expected_revision = 5;

    let mut reply = RpcReply::accepted(&request);
    reply.new_revision = 4;
    assert_matches!(
        reply.correlates_with(&request).unwrap_err(),
        ProtocolError::Mismatch { field: "new_revision", .. }
    );

    // Equal is fine: an operation that changed nothing keeps the revision.
    reply.new_revision = 5;
    reply.correlates_with(&request).unwrap();
}

// --- describe ------------------------------------------------------------

#[test]
fn describe_reply_must_advertise_a_version_we_speak() {
    let request = request(Operation::Describe);
    let mut reply = RpcReply::accepted(&request);
    reply.describe = Some(DescribeInfo {
        supported_versions: vec![1],
        software_version: "0.2.0".to_string(),
        capabilities: vec!["rails_dispatch".to_string()],
        ready: true,
    });
    reply.validate().unwrap();
    assert!(reply.describe.as_ref().unwrap().has_capability("rails_dispatch"));

    reply.describe.as_mut().unwrap().supported_versions = vec![2];
    assert_matches!(reply.validate().unwrap_err(), ProtocolError::UnsupportedVersion(2));

    let mut reply = RpcReply::accepted(&request);
    reply.describe = None;
    assert_matches!(
        reply.validate().unwrap_err(),
        ProtocolError::MissingPayload { operation: "describe", field: "describe" }
    );
}

// --- limits --------------------------------------------------------------

#[test]
fn state_maps_are_bounded_by_entries_key_length_and_encoded_size() {
    let mut map = Map::new();
    for index in 0..=MAX_STATE_ENTRIES {
        map.insert(format!("k{index}"), Value::from(1));
    }
    assert_matches!(
        validate_state_map("state", &map).unwrap_err(),
        ProtocolError::TooMany { field: "state", .. }
    );

    let mut map = Map::new();
    map.insert("k".repeat(MAX_STATE_KEY_LEN + 1), Value::from(1));
    assert_matches!(
        validate_state_map("state", &map).unwrap_err(),
        ProtocolError::FieldTooLong { field: "state_key", .. }
    );

    let mut map = Map::new();
    map.insert("big".to_string(), Value::from("x".repeat(MAX_STATE_LEN)));
    assert_matches!(
        validate_state_map("state", &map).unwrap_err(),
        ProtocolError::FieldTooLong { field: "state", .. }
    );
}
