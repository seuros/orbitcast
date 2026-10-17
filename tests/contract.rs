//! Cross-language contract tests over the shared JSON golden fixtures in
//! `tests/fixtures/`.
//!
//! The exact same files are consumed by `gem/test/contract_test.rb`; neither
//! suite regenerates them during test execution. Signing fixtures are produced
//! by `gem/test/dummy/script/export_signed_stream_fixtures.rb` (Ruby is the
//! producer), publication/channel fixtures are reviewed checked-in documents.

// The canonical protocol types and the *real* signed-stream verifier come from
// the library target, so this test exercises exactly the code the server runs.
use std::assert_matches;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use base64::Engine as _;
use serde_json::Value;

use orbitcast::protocol::v1::claims::StreamClaims;
use orbitcast::protocol::v1::control::ControlMessage;
use orbitcast::protocol::v1::presence::{PresenceCommand, PresenceMessage};
use orbitcast::protocol::v1::publication::{IngressAck, NoticeKind, PublicationNotice};
use orbitcast::protocol::v1::recovery::{
    Cursor, DeliveryEnvelope, GapReason, RecoveryClaims, RecoveryCommand, RecoveryMessage,
};
use orbitcast::protocol::v1::rpc::{RpcReply, RpcRequest, StateReplacement};
use orbitcast::protocol::{Envelope, ProtocolError};
use orbitcast::recovery::RecoveryVerifier;
use orbitcast::streams::{
    ClaimsVerifier, MessageVerifier, SecretBytes, StreamError, StreamsConfig, StreamsController,
    VerifyError,
};

fn fixtures_dir(group: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(group)
}

/// Load every fixture file in a group as `(file_name, parsed_document)`.
fn fixture_files(group: &str) -> Vec<(String, Value)> {
    let dir = fixtures_dir(group);
    let mut files: Vec<_> = fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no fixtures in {}", dir.display());

    files
        .into_iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let text = fs::read_to_string(&path).unwrap();
            let doc: Value = serde_json::from_str(&text)
                .unwrap_or_else(|err| panic!("{name} is not valid JSON: {err}"));
            (name, doc)
        })
        .collect()
}

fn cases(doc: &Value) -> &Vec<Value> {
    doc["cases"].as_array().expect("fixture document must have a cases array")
}

fn str_field<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field].as_str().unwrap_or_else(|| panic!("expected string field {field:?}"))
}

/// The stable, cross-language `error_kind` names used in fixture expectations.
fn error_kind(err: &ProtocolError) -> &'static str {
    match err {
        ProtocolError::UnsupportedVersion(_) => "unsupported_version",
        ProtocolError::EmptyField { field: "stream" } => "empty_stream",
        ProtocolError::EmptyField { field: "namespace" } => "empty_namespace",
        ProtocolError::EmptyField { .. } => "empty_field",
        ProtocolError::FieldTooLong { .. } => "field_too_long",
        ProtocolError::InvalidUuid { .. } => "invalid_uuid",
        ProtocolError::InvalidJson { .. } => "invalid_json_payload",
        ProtocolError::InvalidPurpose { .. } => "wrong_purpose",
        ProtocolError::Expired { .. } => "expired",
        ProtocolError::DisallowedValue { .. } => "disallowed_value",
        ProtocolError::TooMany { .. } => "too_many",
        ProtocolError::OutOfRange { .. } => "out_of_range",
        ProtocolError::MissingPayload { .. } => "missing_payload",
        ProtocolError::NotAllowedForOutcome { .. } => "not_allowed_for_outcome",
        ProtocolError::Mismatch { .. } => "mismatch",
        ProtocolError::Malformed(_) => "malformed_json",
    }
}

/// Channel-name derivation: `orbitcast_` + the first 48
/// lowercase hex characters of SHA-256 over the length-prefixed UTF-8
/// `(namespace, stream)` tuple (4-byte big-endian byte-length prefix before
/// each part). Reimplemented here at protocol level against the golden
/// vectors.
// TODO: replace with `pubsub::postgres::channel_name` once the PostgreSQL
// NOTIFY rework lands that function; the golden vectors must not change.
fn channel_name(namespace: &str, stream: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(length_prefix(namespace));
    hasher.update(namespace.as_bytes());
    hasher.update(length_prefix(stream));
    hasher.update(stream.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("orbitcast_{}", &digest[..48])
}

/// The 4-byte big-endian byte length that precedes each hashed part.
fn length_prefix(part: &str) -> [u8; 4] {
    u32::try_from(part.len()).expect("a golden-vector part fits a 4-byte length").to_be_bytes()
}

/// Decode `wire`, failing the case with its context when it does not.
fn decode<T: serde::de::DeserializeOwned>(context: &str, wire: &str) -> T {
    serde_json::from_str(wire)
        .unwrap_or_else(|err| panic!("{context}: expected wire to decode: {err}"))
}

/// Whether `field` holds a string outside `allowed`.
///
/// serde reports every unknown variant the same way, so which tagged value
/// was unknown is decided from the wire rather than from the message.
fn unknown_tag(document: &Value, field: &str, allowed: &[&str]) -> bool {
    document.get(field).and_then(Value::as_str).is_some_and(|value| !allowed.contains(&value))
}

/// Every field the fixture lists as client-supplied identity must be absent
/// after decode and re-encode.
fn assert_ignored_fields_dropped(context: &str, reencoded: &Value, expect: &Value) {
    for field in expect["ignored_fields"].as_array().unwrap_or(&Vec::new()) {
        let name = field.as_str().unwrap();
        assert!(
            reencoded.get(name).is_none(),
            "{context}: a client-supplied {name:?} must never survive decoding"
        );
    }
}

// --- publications ----------------------------------------------------------

fn check_valid_notice(context: &str, wire: &str, expect: &Value) {
    let notice: PublicationNotice = decode(context, wire);
    notice.validate().unwrap_or_else(|err| panic!("{context}: expected valid notice: {err}"));

    assert_eq!(u64::from(notice.version()), expect["version"].as_u64().unwrap(), "{context}");

    let kind = match &notice.kind {
        NoticeKind::Inline { .. } => "inline",
        NoticeKind::Overflow { .. } => "overflow",
    };
    assert_eq!(kind, str_field(expect, "kind"), "{context}");
    assert_eq!(notice.namespace(), str_field(expect, "namespace"), "{context}");
    assert_eq!(notice.stream(), str_field(expect, "stream"), "{context}");

    match &notice.kind {
        NoticeKind::Inline { publication } => {
            assert_eq!(publication.id, str_field(expect, "id"), "{context}");
            if let Some(expected_version) = expect.get("publication_version") {
                assert_eq!(
                    u64::from(publication.version),
                    expected_version.as_u64().unwrap(),
                    "{context}"
                );
            }
            if let Some(expected_payload) = expect.get("json_payload") {
                // Byte-for-byte: the envelope must not re-encode the payload.
                assert_eq!(
                    publication.json_payload,
                    expected_payload.as_str().unwrap(),
                    "{context}: json_payload must be preserved exactly"
                );
            }
            if let Some(expected_values) = expect.get("payload_values") {
                let values: Value = serde_json::from_str(&publication.json_payload).unwrap();
                assert_eq!(&values, expected_values, "{context}");
            }
        }
        NoticeKind::Overflow { id, .. } => {
            assert_eq!(id, str_field(expect, "id"), "{context}");
        }
    }

    if let Some(expected_channel) = expect.get("channel") {
        assert_eq!(
            channel_name(notice.namespace(), notice.stream()),
            expected_channel.as_str().unwrap(),
            "{context}"
        );
    }

    // Serialization: version is always emitted, and the notice round-trips.
    let reencoded = serde_json::to_value(&notice).unwrap();
    assert_eq!(reencoded["version"], 1, "{context}: serializers always emit version");
    let roundtripped: PublicationNotice = serde_json::from_value(reencoded).unwrap();
    assert_eq!(roundtripped, notice, "{context}: notice must round-trip");
}

fn check_invalid_notice(context: &str, wire: &str, expect: &Value) {
    match str_field(expect, "error_kind") {
        "malformed_json" => {
            assert!(
                serde_json::from_str::<Value>(wire).is_err(),
                "{context}: wire must not parse as JSON"
            );
        }
        "unknown_kind" => {
            let err = serde_json::from_str::<PublicationNotice>(wire)
                .expect_err("an unknown kind is a hard decode error");
            assert!(err.to_string().contains("unknown variant"), "{context}: {err}");
        }
        expected_kind => {
            // Structurally valid: decodes fine, fails semantic validation.
            let notice: PublicationNotice = decode(context, wire);
            let err = notice.validate().expect_err("validation must fail");
            assert_eq!(error_kind(&err), expected_kind, "{context}: {err}");
        }
    }
}

#[test]
fn publication_fixtures() {
    for (file, doc) in fixture_files("publications") {
        for case in cases(&doc) {
            let context = format!("publications/{file} case {}", str_field(case, "name"));
            let wire = str_field(&case["input"], "wire");
            let expect = &case["expect"];

            if expect["valid"].as_bool().unwrap() {
                check_valid_notice(&context, wire, expect);
            } else {
                check_invalid_notice(&context, wire, expect);
            }
        }
    }
}

// --- RPC requests, replies, and effects ------------------------------------

/// The stable name a fixture uses for a decode-level rejection.
///
/// Unknown tagged values and wrong field types are *hard* errors, so they never
/// reach `validate`; the fixture distinguishes them by name so the Ruby suite
/// can assert the same boundary.
fn decode_error_kind(_err: &serde_json::Error, wire: &str) -> &'static str {
    const OPERATIONS: &[&str] = &[
        "describe",
        "identify",
        "open_channel",
        "close_channel",
        "run_action",
        "release_connection",
    ];
    const OUTCOMES: &[&str] = &["accepted", "denied", "fault"];
    const EFFECTS: &[&str] = &[
        "add_stream",
        "remove_stream",
        "remove_all_streams",
        "transmit",
        "close",
        "presence_grant",
        "presence_revoke",
    ];
    const CONTROL_KINDS: &[&str] = &["disconnect"];

    let Ok(document) = serde_json::from_str::<Value>(wire) else {
        return "malformed_json";
    };

    if unknown_tag(&document, "operation", OPERATIONS) {
        "unknown_operation"
    } else if unknown_tag(&document, "outcome", OUTCOMES) {
        "unknown_outcome"
    } else if unknown_tag(&document, "kind", CONTROL_KINDS) {
        "unknown_kind"
    } else if document["effects"]
        .as_array()
        .is_some_and(|effects| effects.iter().any(|effect| unknown_effect(effect, EFFECTS)))
    {
        "unknown_effect"
    } else {
        "wrong_type"
    }
}

/// Whether one encoded effect names a `kind` this build does not know.
///
/// An effect with *no* `kind` is a missing discriminant (a wrong-type error),
/// not an unknown one.
fn unknown_effect(effect: &Value, allowed: &[&str]) -> bool {
    effect.get("kind").and_then(Value::as_str).is_some_and(|kind| !allowed.contains(&kind))
}

fn check_valid_rpc_request(context: &str, wire: &str, expect: &Value) {
    let request: RpcRequest = decode(context, wire);
    request.validate().unwrap_or_else(|err| panic!("{context}: expected valid request: {err}"));

    assert_eq!(u64::from(request.version), expect["version"].as_u64().unwrap(), "{context}");
    assert_eq!(request.operation.as_str(), str_field(expect, "operation"), "{context}");
    assert_eq!(request.namespace, str_field(expect, "namespace"), "{context}");
    assert_eq!(request.node_id, str_field(expect, "node_id"), "{context}");
    assert_eq!(request.session_id, str_field(expect, "session_id"), "{context}");
    assert_eq!(request.request_id, str_field(expect, "request_id"), "{context}");
    assert_eq!(request.sequence, expect["sequence"].as_u64().unwrap(), "{context}");
    assert_eq!(
        request.expected_revision,
        expect["expected_revision"].as_u64().unwrap(),
        "{context}"
    );
    assert_eq!(request.deadline_ms, expect["deadline_ms"].as_u64().unwrap(), "{context}");

    if let Some(expected) = expect.get("connection") {
        let connection = request.connection.as_ref().expect("connection snapshot");
        assert_eq!(connection.url, str_field(expected, "url"), "{context}");
        assert_eq!(
            connection.origin.as_deref(),
            expected["origin"].as_str(),
            "{context}: absent and null origin are both `no Origin header`"
        );
        assert_eq!(connection.client_address, str_field(expected, "client_address"), "{context}");
        if let Some(headers) = expected.get("headers").and_then(Value::as_object) {
            assert_eq!(connection.headers.len(), headers.len(), "{context}");
            for (name, value) in headers {
                assert_eq!(
                    connection.headers.get(name).map(String::as_str),
                    value.as_str(),
                    "{context}: header {name}"
                );
            }
        }
    }

    if let Some(expected) = expect.get("subscription") {
        let subscription = request.subscription.as_ref().expect("subscription snapshot");
        // Byte for byte: the identifier is what addresses every frame.
        assert_eq!(subscription.identifier, str_field(expected, "identifier"), "{context}");
        if let Some(params) = expected.get("params") {
            assert_eq!(&Value::Object(subscription.params.clone()), params, "{context}");
        }
        if let Some(streams) = expected.get("streams").and_then(Value::as_array) {
            let actual: Vec<Value> =
                subscription.streams.iter().map(|s| Value::from(s.as_str())).collect();
            assert_eq!(&actual, streams, "{context}");
        }
    }

    if let Some(expected) = expect.get("action") {
        let action = request.action.as_ref().expect("action payload");
        assert_eq!(action.identifier, str_field(expected, "identifier"), "{context}");
        assert_eq!(
            action.json_data,
            str_field(expected, "json_data"),
            "{context}: the client's encoded data is forwarded exactly"
        );
    }

    if let Some(expected) = expect.get("release") {
        let release = request.release.as_ref().expect("release payload");
        assert_eq!(release.reason, str_field(expected, "reason"), "{context}");
        assert_eq!(release.reconnect, expected["reconnect"].as_bool().unwrap(), "{context}");
        assert_eq!(
            release.subscriptions.len() as u64,
            expected["subscription_count"].as_u64().unwrap(),
            "{context}"
        );
    }

    let reencoded = serde_json::to_value(&request).unwrap();
    assert_eq!(reencoded["version"], 1, "{context}: serializers always emit version");
    let roundtripped: RpcRequest = serde_json::from_value(reencoded).unwrap();
    assert_eq!(roundtripped, request, "{context}: request must round-trip");
}

/// Assert the absent / null / empty trichotomy of one state field.
fn check_state(context: &str, field: &str, state: &StateReplacement, expect: &Value) {
    let Some(expected) = expect.get(field).and_then(Value::as_str) else {
        return;
    };

    match expected {
        "unchanged" => {
            assert!(state.is_unchanged(), "{context}: {field} must be unchanged");
            assert!(state.map().is_none(), "{context}");
        }
        "clear" => {
            assert!(!state.is_unchanged(), "{context}: {field} must not be unchanged");
            assert!(state.is_clear(), "{context}: {field} must clear the state");
            assert_eq!(state.map().unwrap().len(), 0, "{context}");
        }
        "replace" => {
            assert!(!state.is_unchanged(), "{context}: {field} must replace the state");
            assert!(!state.is_clear(), "{context}");
        }
        other => panic!("{context}: unknown {field} expectation {other:?}"),
    }

    if let Some(value) = expect.get("state_value")
        && field == "state"
    {
        assert_eq!(&Value::Object(state.map().unwrap().clone()), value, "{context}");
    }
}

fn check_valid_rpc_reply(context: &str, wire: &str, expect: &Value) {
    let reply: RpcReply = decode(context, wire);
    reply.validate().unwrap_or_else(|err| panic!("{context}: expected valid reply: {err}"));

    assert_eq!(u64::from(reply.version), expect["version"].as_u64().unwrap(), "{context}");
    assert_eq!(reply.operation.as_str(), str_field(expect, "operation"), "{context}");
    assert_eq!(reply.outcome.as_str(), str_field(expect, "outcome"), "{context}");
    assert_eq!(reply.is_accepted(), expect["accepted"].as_bool().unwrap(), "{context}");
    assert_eq!(reply.request_id, str_field(expect, "request_id"), "{context}");
    assert_eq!(reply.sequence, expect["sequence"].as_u64().unwrap(), "{context}");
    assert_eq!(reply.new_revision, expect["new_revision"].as_u64().unwrap(), "{context}");

    check_state(context, "state", &reply.state, expect);
    check_state(context, "subscription_state", &reply.subscription_state, expect);

    if let Some(identity) = expect.get("identity") {
        assert_eq!(&Value::Object(reply.identity.clone()), identity, "{context}");
    }
    if let Some(reason) = expect.get("reason") {
        assert_eq!(reply.reason.as_deref(), reason.as_str(), "{context}");
    }

    if let Some(kinds) = expect.get("effect_kinds").and_then(Value::as_array) {
        let actual: Vec<Value> =
            reply.effects.iter().map(|effect| Value::from(effect.kind())).collect();
        assert_eq!(&actual, kinds, "{context}: effects are an ordered list");
    }

    // Effects are compared as encoded values, so unknown extra keys sent by a
    // newer peer are visibly dropped rather than silently carried along.
    if let Some(effects) = expect.get("effects").and_then(Value::as_array) {
        let actual = serde_json::to_value(&reply.effects).unwrap();
        assert_eq!(actual.as_array().unwrap(), effects, "{context}");
    }

    if let Some(expected) = expect.get("describe") {
        let describe = reply.describe.as_ref().expect("describe payload");
        assert_eq!(&serde_json::to_value(describe).unwrap(), expected, "{context}");
    }

    let reencoded = serde_json::to_value(&reply).unwrap();
    assert_eq!(reencoded["version"], 1, "{context}: serializers always emit version");
    assert_eq!(
        reencoded.get("state").is_none(),
        reply.state.is_unchanged(),
        "{context}: an unchanged state is omitted, never emitted as {{}}"
    );
    let roundtripped: RpcReply = serde_json::from_value(reencoded).unwrap();
    assert_eq!(roundtripped, reply, "{context}: reply must round-trip");
}

fn check_invalid_rpc(context: &str, envelope: &str, wire: &str, expect: &Value) {
    let expected_kind = str_field(expect, "error_kind");

    // A decode failure and a semantic rejection are different boundaries; the
    // fixture says which one it expects.
    let decode_error = match envelope {
        "request" => serde_json::from_str::<RpcRequest>(wire).err(),
        _ => serde_json::from_str::<RpcReply>(wire).err(),
    };

    if let Some(err) = decode_error {
        assert_eq!(decode_error_kind(&err, wire), expected_kind, "{context}: {err}");
        return;
    }

    let err = match envelope {
        "request" => serde_json::from_str::<RpcRequest>(wire)
            .unwrap()
            .validate()
            .expect_err("validation must fail"),
        _ => serde_json::from_str::<RpcReply>(wire)
            .unwrap()
            .validate()
            .expect_err("validation must fail"),
    };
    assert_eq!(error_kind(&err), expected_kind, "{context}: {err}");
}

#[test]
fn rpc_fixtures() {
    for (file, doc) in fixture_files("rpc") {
        for case in cases(&doc) {
            let context = format!("rpc/{file} case {}", str_field(case, "name"));
            let wire = str_field(&case["input"], "wire");
            let expect = &case["expect"];
            let envelope = case.get("envelope").and_then(Value::as_str).unwrap_or("reply");

            match (expect["valid"].as_bool().unwrap(), envelope) {
                (true, "request") => check_valid_rpc_request(&context, wire, expect),
                (true, _) => check_valid_rpc_reply(&context, wire, expect),
                (false, envelope) => check_invalid_rpc(&context, envelope, wire, expect),
            }
        }
    }
}

// --- presence --------------------------------------------------------------

/// The stable name a presence fixture uses for a decode-level rejection.
///
/// An unknown `op` or `event` is a *hard* error - never a defaulted operation -
/// so it never reaches `validate`; the fixture names the boundary it expects so
/// the Ruby suite can assert the same one.
fn presence_decode_error_kind(wire: &str) -> &'static str {
    const OPS: &[&str] = &["join", "leave", "snapshot"];
    const EVENTS: &[&str] = &["join", "leave", "snapshot"];

    let Ok(document) = serde_json::from_str::<Value>(wire) else {
        return "malformed_json";
    };

    if unknown_tag(&document, "op", OPS) {
        "unknown_op"
    } else if unknown_tag(&document, "event", EVENTS) {
        "unknown_event"
    } else {
        "wrong_type"
    }
}

fn check_valid_presence_command(context: &str, wire: &str, expect: &Value) {
    let command: PresenceCommand = decode(context, wire);
    command.validate().unwrap_or_else(|err| panic!("{context}: expected valid command: {err}"));

    assert_eq!(u64::from(command.version), expect["version"].as_u64().unwrap(), "{context}");
    assert_eq!(command.op.as_str(), str_field(expect, "op"), "{context}");

    if let Some(page) = expect.get("page") {
        let resolved = command.page();
        assert_eq!(u64::from(resolved.limit), page["limit"].as_u64().unwrap(), "{context}");
        assert_eq!(resolved.offset, page["offset"].as_u64().unwrap(), "{context}");
    }

    // Identity is not a field of this envelope: anything a client sent that
    // looks like one was ignored, and re-encoding proves it was not carried.
    let reencoded = serde_json::to_value(&command).unwrap();
    assert_eq!(reencoded["version"], 1, "{context}: serializers always emit version");
    assert_ignored_fields_dropped(context, &reencoded, expect);

    let roundtripped: PresenceCommand = serde_json::from_value(reencoded).unwrap();
    assert_eq!(roundtripped, command, "{context}: command must round-trip");
}

fn check_valid_presence_message(context: &str, wire: &str, expect: &Value) {
    let message: PresenceMessage = decode(context, wire);
    message.validate().unwrap_or_else(|err| panic!("{context}: expected valid message: {err}"));

    assert_eq!(u64::from(message.version), expect["version"].as_u64().unwrap(), "{context}");
    assert_eq!(message.message_type, "presence", "{context}");
    assert_eq!(message.event.as_str(), str_field(expect, "event"), "{context}");
    assert_eq!(message.stream, str_field(expect, "stream"), "{context}");
    assert_eq!(message.total, expect["total"].as_u64().unwrap(), "{context}");

    assert_eq!(
        message.subject_id.as_deref(),
        expect.get("subject_id").and_then(Value::as_str),
        "{context}"
    );
    if let Some(metadata) = expect.get("metadata") {
        assert_eq!(
            &Value::Object(message.metadata.clone().unwrap_or_default()),
            metadata,
            "{context}"
        );
    }

    if let Some(subjects) = expect.get("member_subjects").and_then(Value::as_array) {
        let actual: Vec<Value> =
            message.members.iter().map(|member| Value::from(member.subject_id.as_str())).collect();
        assert_eq!(&actual, subjects, "{context}: members keep their page order");
    }

    if let Some(page) = expect.get("page") {
        let actual = message.page.expect("a snapshot carries its page window");
        assert_eq!(u64::from(actual.limit), page["limit"].as_u64().unwrap(), "{context}");
        assert_eq!(actual.offset, page["offset"].as_u64().unwrap(), "{context}");
        assert_eq!(actual.has_more, page["has_more"].as_bool().unwrap(), "{context}");
    }

    let reencoded = serde_json::to_value(&message).unwrap();
    assert_eq!(reencoded["version"], 1, "{context}: serializers always emit version");
    assert_eq!(reencoded["type"], "presence", "{context}");
    let roundtripped: PresenceMessage = serde_json::from_value(reencoded).unwrap();
    assert_eq!(roundtripped, message, "{context}: message must round-trip");
}

fn check_invalid_presence(context: &str, envelope: &str, wire: &str, expect: &Value) {
    let expected_kind = str_field(expect, "error_kind");

    let decode_error = match envelope {
        "command" => serde_json::from_str::<PresenceCommand>(wire).err(),
        _ => serde_json::from_str::<PresenceMessage>(wire).err(),
    };

    if decode_error.is_some() {
        assert_eq!(presence_decode_error_kind(wire), expected_kind, "{context}");
        return;
    }

    let err = match envelope {
        "command" => serde_json::from_str::<PresenceCommand>(wire)
            .unwrap()
            .validate()
            .expect_err("validation must fail"),
        _ => serde_json::from_str::<PresenceMessage>(wire)
            .unwrap()
            .validate()
            .expect_err("validation must fail"),
    };
    assert_eq!(error_kind(&err), expected_kind, "{context}: {err}");
}

#[test]
fn presence_fixtures() {
    for (file, doc) in fixture_files("presence") {
        for case in cases(&doc) {
            let context = format!("presence/{file} case {}", str_field(case, "name"));
            let wire = str_field(&case["input"], "wire");
            let expect = &case["expect"];
            let envelope = case.get("envelope").and_then(Value::as_str).unwrap_or("command");

            match (expect["valid"].as_bool().unwrap(), envelope) {
                (true, "command") => check_valid_presence_command(&context, wire, expect),
                (true, _) => check_valid_presence_message(&context, wire, expect),
                (false, envelope) => check_invalid_presence(&context, envelope, wire, expect),
            }
        }
    }
}

// --- recovery ---------------------------------------------------------------

/// The stable name a recovery fixture uses for a decode-level rejection.
///
/// An unknown `op`, `event`, or gap `reason` is a *hard* error, so it never
/// reaches `validate`; the fixture names the boundary it expects.
fn recovery_decode_error_kind(wire: &str) -> &'static str {
    const OPS: &[&str] = &["resume", "ack"];
    const EVENTS: &[&str] = &["replay_start", "replay_done", "gap"];
    const REASONS: &[&str] = &[
        "expired",
        "unknown_generation",
        "buffer_overflow",
        "fenced",
        "unauthorized",
        "unavailable",
    ];

    let Ok(document) = serde_json::from_str::<Value>(wire) else {
        return "malformed_json";
    };

    if unknown_tag(&document, "op", OPS) {
        "unknown_op"
    } else if unknown_tag(&document, "event", EVENTS) {
        "unknown_event"
    } else if unknown_tag(&document, "reason", REASONS) {
        "unknown_reason"
    } else {
        "wrong_type"
    }
}

/// Cursors: opaque to a client, exact for the server.
fn check_cursor(context: &str, input: &Value, expect: &Value) {
    let encoded = str_field(input, "cursor");

    if !expect["valid"].as_bool().unwrap() {
        let err = Cursor::decode(encoded).expect_err("an unusable cursor must be rejected");
        assert_eq!(error_kind(&err), str_field(expect, "error_kind"), "{context}: {err}");
        return;
    }

    let cursor = Cursor::decode(encoded)
        .unwrap_or_else(|err| panic!("{context}: expected the cursor to decode: {err}"));

    // The positions are compared as decimal *strings*: a 64-bit sequence does
    // not survive a JSON number in every language that reads this fixture.
    assert_eq!(cursor.generation.to_string(), str_field(expect, "generation"), "{context}");
    assert_eq!(cursor.sequence.to_string(), str_field(expect, "sequence"), "{context}");

    if let Some(json) = expect.get("json").and_then(Value::as_str) {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap_or_else(|err| panic!("{context}: cursor must be strict base64: {err}"));
        assert_eq!(
            String::from_utf8(decoded).unwrap(),
            json,
            "{context}: the encoded payload is the documented compact JSON"
        );
        // Re-encoding a canonical cursor reproduces it byte for byte.
        assert_eq!(cursor.encode(), encoded, "{context}: encoding is deterministic");
    }
}

fn check_delivery_envelope(context: &str, wire: &str, expect: &Value) {
    if !expect["valid"].as_bool().unwrap() {
        let expected_kind = str_field(expect, "error_kind");
        match serde_json::from_str::<DeliveryEnvelope>(wire) {
            Err(_) => assert_eq!(recovery_decode_error_kind(wire), expected_kind, "{context}"),
            Ok(envelope) => {
                let err = envelope.validate().expect_err("validation must fail");
                assert_eq!(error_kind(&err), expected_kind, "{context}: {err}");
            }
        }
        return;
    }

    let envelope: DeliveryEnvelope = decode(context, wire);
    envelope.validate().unwrap_or_else(|err| panic!("{context}: expected valid envelope: {err}"));

    assert_eq!(u64::from(envelope.version), expect["version"].as_u64().unwrap(), "{context}");
    assert_eq!(envelope.stream, str_field(expect, "stream"), "{context}");
    assert_eq!(envelope.sequence, str_field(expect, "sequence"), "{context}");
    if let Some(generation) = expect.get("generation").and_then(Value::as_str) {
        assert_eq!(envelope.generation, generation, "{context}");
    }
    if let Some(id) = expect.get("publication_id").and_then(Value::as_str) {
        assert_eq!(envelope.publication_id, id, "{context}");
    }
    if let Some(payload) = expect.get("json_payload").and_then(Value::as_str) {
        assert_eq!(
            envelope.json_payload, payload,
            "{context}: the payload text is forwarded byte for byte"
        );
    }

    // The cursor names exactly the position the envelope reports, so a client
    // that resumes from it asks for the place it just received.
    let position = envelope.position().expect("a valid envelope carries a decodable cursor");
    assert_eq!(position.sequence.to_string(), envelope.sequence, "{context}");
    assert_eq!(position.generation.to_string(), envelope.generation, "{context}");

    let reencoded = serde_json::to_value(&envelope).unwrap();
    assert_eq!(reencoded["version"], 1, "{context}: serializers always emit version");
    assert_eq!(reencoded["type"], "orbitcast_delivery", "{context}");
    let roundtripped: DeliveryEnvelope = serde_json::from_value(reencoded).unwrap();
    assert_eq!(roundtripped, envelope, "{context}: envelope must round-trip");
}

/// Assert the `stream -> cursor` map of a command or message.
fn check_cursors(context: &str, cursors: &BTreeMap<String, String>, expect: &Value) {
    if let Some(streams) = expect.get("cursor_streams").and_then(Value::as_array) {
        let actual: Vec<Value> = cursors.keys().map(|s| Value::from(s.as_str())).collect();
        assert_eq!(&actual, streams, "{context}: cursors are keyed by stream");
    }

    if let Some(sequences) = expect.get("cursor_sequences").and_then(Value::as_object) {
        for (stream, sequence) in sequences {
            let cursor = Cursor::decode(&cursors[stream]).expect("cursor decodes");
            assert_eq!(cursor.sequence.to_string(), sequence.as_str().unwrap(), "{context}");
        }
    }
}

fn check_recovery_command(context: &str, wire: &str, expect: &Value) {
    if !expect["valid"].as_bool().unwrap() {
        let expected_kind = str_field(expect, "error_kind");
        match serde_json::from_str::<RecoveryCommand>(wire) {
            Err(_) => assert_eq!(recovery_decode_error_kind(wire), expected_kind, "{context}"),
            Ok(command) => {
                let err = command.validate().expect_err("validation must fail");
                assert_eq!(error_kind(&err), expected_kind, "{context}: {err}");
            }
        }
        return;
    }

    let command = RecoveryCommand::decode(wire)
        .unwrap_or_else(|err| panic!("{context}: expected a valid command: {err}"));

    assert_eq!(u64::from(command.version), expect["version"].as_u64().unwrap(), "{context}");
    assert_eq!(command.op.as_str(), str_field(expect, "op"), "{context}");
    check_cursors(context, &command.cursors, expect);

    let reencoded = serde_json::to_value(&command).unwrap();
    assert_eq!(reencoded["version"], 1, "{context}: serializers always emit version");
    assert_ignored_fields_dropped(context, &reencoded, expect);

    let roundtripped: RecoveryCommand = serde_json::from_value(reencoded).unwrap();
    assert_eq!(roundtripped, command, "{context}: command must round-trip");
}

fn check_recovery_message(context: &str, wire: &str, expect: &Value) {
    if !expect["valid"].as_bool().unwrap() {
        let expected_kind = str_field(expect, "error_kind");
        match serde_json::from_str::<RecoveryMessage>(wire) {
            Err(_) => assert_eq!(recovery_decode_error_kind(wire), expected_kind, "{context}"),
            Ok(message) => {
                let err = message.validate().expect_err("validation must fail");
                assert_eq!(error_kind(&err), expected_kind, "{context}: {err}");
            }
        }
        return;
    }

    let message: RecoveryMessage = decode(context, wire);
    message.validate().unwrap_or_else(|err| panic!("{context}: expected valid message: {err}"));

    assert_eq!(u64::from(message.version), expect["version"].as_u64().unwrap(), "{context}");
    assert_eq!(message.event.as_str(), str_field(expect, "event"), "{context}");
    assert_eq!(
        message.stream.as_deref(),
        expect.get("stream").and_then(Value::as_str),
        "{context}"
    );
    assert_eq!(
        message.reason.map(GapReason::as_str),
        expect.get("reason").and_then(Value::as_str),
        "{context}"
    );
    if let Some(streams) = expect.get("streams").and_then(Value::as_array) {
        let actual: Vec<Value> = message.streams.iter().map(|s| Value::from(s.as_str())).collect();
        assert_eq!(&actual, streams, "{context}");
    }
    check_cursors(context, &message.cursors, expect);

    let reencoded = serde_json::to_value(&message).unwrap();
    assert_eq!(reencoded["version"], 1, "{context}: serializers always emit version");
    assert_eq!(reencoded["type"], "orbitcast_recovery", "{context}");
    let roundtripped: RecoveryMessage = serde_json::from_value(reencoded).unwrap();
    assert_eq!(roundtripped, message, "{context}: message must round-trip");
}

/// Recovery tokens, through the *real* verifier keyed with the fixture's
/// dedicated recovery secret.
fn check_recovery_token(context: &str, input: &Value, expect: &Value) {
    let token = str_field(input, "token");
    let secret = str_field(input, "secret");
    let now = input["now"].as_i64().unwrap();

    let (encoded, signature) =
        token.split_once("--").unwrap_or_else(|| panic!("{context}: token has no --"));
    assert!(
        signature.len() == 64 && signature.bytes().all(|b| b.is_ascii_hexdigit()),
        "{context}: signature must be 64 hex chars"
    );

    // The claims decode independently of the signature.
    let claims_bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .unwrap_or_else(|err| panic!("{context}: encoded part is not strict base64: {err}"));
    let claims: RecoveryClaims = serde_json::from_slice(&claims_bytes)
        .unwrap_or_else(|err| panic!("{context}: claims must decode: {err}"));

    let expected_claims = &expect["claims"];
    assert_eq!(
        u64::from(claims.version),
        expected_claims["version"].as_u64().unwrap(),
        "{context}"
    );
    assert_eq!(claims.namespace, str_field(expected_claims, "namespace"), "{context}");
    assert_eq!(claims.session_id, str_field(expected_claims, "session_id"), "{context}");
    assert_eq!(claims.purpose, str_field(expected_claims, "purpose"), "{context}");
    assert_eq!(claims.expires_at, expected_claims["expires_at"].as_i64().unwrap(), "{context}");
    let expected_streams: Vec<Value> =
        claims.streams.iter().map(|s| Value::from(s.as_str())).collect();
    assert_eq!(&expected_streams, expected_claims["streams"].as_array().unwrap(), "{context}");

    let signature_valid = expect["signature_valid"].as_bool().unwrap();
    let claims_valid = expect["claims_valid"].as_bool().unwrap();
    let valid = expect["valid"].as_bool().unwrap();

    let verifier = RecoveryVerifier::new(SecretBytes::new(secret.as_bytes().to_vec()));
    let namespace = str_field(expected_claims, "namespace");

    match verifier.verify(token, namespace, now) {
        Ok(accepted) => {
            assert!(signature_valid && claims_valid && valid, "{context}: unexpectedly valid");
            assert_eq!(accepted, claims, "{context}");

            // A valid token authorizes exactly the streams it names.
            for stream in expect["authorizes"].as_array().unwrap_or(&Vec::new()) {
                assert!(
                    accepted.authorizes(namespace, stream.as_str().unwrap(), now),
                    "{context}: must authorize {stream}"
                );
            }
            for stream in expect["denies"].as_array().unwrap_or(&Vec::new()) {
                assert!(
                    !accepted.authorizes(namespace, stream.as_str().unwrap(), now),
                    "{context}: must not authorize {stream}"
                );
            }
            assert!(
                !accepted.authorizes("some_other_namespace", &accepted.streams[0], now),
                "{context}: a token is namespace-scoped"
            );

            // Producer equivalence, for the canonically encoded cases only:
            // signing the decoded claims with the same key reproduces the
            // fixture token byte for byte, which is what licenses
            // gem/lib/orbitcast/recovery.rb to produce these. A tolerant-read
            // case (absent version, extra claims) deliberately does not
            // re-encode to its own input.
            if expect["canonical"].as_bool().unwrap_or(false) {
                assert_eq!(verifier.sign(&accepted), token, "{context}");
            }
        }
        Err(VerifyError::InvalidSignature) => {
            assert!(!signature_valid, "{context}: signature unexpectedly invalid");
            assert!(!valid, "{context}");
            assert_eq!(str_field(expect, "error_kind"), "bad_signature", "{context}");
        }
        Err(VerifyError::Claims(err)) => {
            assert!(signature_valid, "{context}: signature unexpectedly invalid");
            assert!(!claims_valid && !valid, "{context}: claims unexpectedly valid");
            assert_eq!(error_kind(&err), str_field(expect, "error_kind"), "{context}");
        }
        Err(other) => panic!("{context}: unexpected verify error: {other}"),
    }
}

#[test]
fn recovery_fixtures() {
    for (file, doc) in fixture_files("recovery") {
        for case in cases(&doc) {
            let context = format!("recovery/{file} case {}", str_field(case, "name"));
            let input = &case["input"];
            let expect = &case["expect"];

            match str_field(case, "envelope") {
                "cursor" => check_cursor(&context, input, expect),
                "delivery" => check_delivery_envelope(&context, str_field(input, "wire"), expect),
                "command" => check_recovery_command(&context, str_field(input, "wire"), expect),
                "message" => check_recovery_message(&context, str_field(input, "wire"), expect),
                "token" => check_recovery_token(&context, input, expect),
                other => panic!("{context}: unknown envelope {other:?}"),
            }
        }
    }
}

// --- control messages ------------------------------------------------------

#[test]
fn control_fixtures() {
    for (file, doc) in fixture_files("control") {
        for case in cases(&doc) {
            let context = format!("control/{file} case {}", str_field(case, "name"));
            let wire = str_field(&case["input"], "wire");
            let expect = &case["expect"];

            if !expect["valid"].as_bool().unwrap() {
                let expected_kind = str_field(expect, "error_kind");
                match serde_json::from_str::<ControlMessage>(wire) {
                    Err(err) => {
                        assert_eq!(
                            decode_error_kind(&err, wire),
                            expected_kind,
                            "{context}: {err}"
                        );
                    }
                    Ok(control) => {
                        let err = control.validate().expect_err("validation must fail");
                        assert_eq!(error_kind(&err), expected_kind, "{context}: {err}");
                        assert!(
                            !control.applies_to(&control.namespace),
                            "{context}: an invalid control message applies to nothing"
                        );
                    }
                }
                continue;
            }

            let control: ControlMessage = decode(&context, wire);
            control.validate().unwrap_or_else(|err| panic!("{context}: expected valid: {err}"));

            assert_eq!(
                u64::from(control.version),
                expect["version"].as_u64().unwrap(),
                "{context}"
            );
            assert_eq!(control.kind.as_str(), str_field(expect, "kind"), "{context}");
            assert_eq!(control.namespace, str_field(expect, "namespace"), "{context}");
            assert_eq!(control.control_id, str_field(expect, "control_id"), "{context}");
            assert_eq!(control.identity_key, str_field(expect, "identity_key"), "{context}");
            assert_eq!(control.reason, str_field(expect, "reason"), "{context}");
            assert_eq!(control.reconnect, expect["reconnect"].as_bool().unwrap(), "{context}");

            assert!(control.applies_to(str_field(expect, "applies_to")), "{context}");
            assert!(
                !control.applies_to("some_other_namespace"),
                "{context}: control is namespace-scoped"
            );

            let reencoded = serde_json::to_value(&control).unwrap();
            assert_eq!(reencoded["version"], 1, "{context}: serializers always emit version");
            let roundtripped: ControlMessage = serde_json::from_value(reencoded).unwrap();
            assert_eq!(roundtripped, control, "{context}: control must round-trip");
        }
    }
}

// --- ingress acknowledgements ----------------------------------------------

#[test]
fn ingress_ack_fixtures() {
    for (file, doc) in fixture_files("ingress") {
        for case in cases(&doc) {
            let context = format!("ingress/{file} case {}", str_field(case, "name"));
            let wire = str_field(&case["input"], "wire");
            let expect = &case["expect"];

            let ack: IngressAck = decode(&context, wire);

            if !expect["valid"].as_bool().unwrap() {
                let err = ack.validate().expect_err("validation must fail");
                assert_eq!(error_kind(&err), str_field(expect, "error_kind"), "{context}: {err}");
                continue;
            }

            ack.validate().unwrap_or_else(|err| panic!("{context}: expected valid: {err}"));
            assert_eq!(u64::from(ack.version), expect["version"].as_u64().unwrap(), "{context}");
            assert_eq!(ack.publication_id, str_field(expect, "publication_id"), "{context}");

            let reencoded = serde_json::to_value(&ack).unwrap();
            assert_eq!(reencoded["version"], 1, "{context}: serializers always emit version");
            let roundtripped: IngressAck = serde_json::from_value(reencoded).unwrap();
            assert_eq!(roundtripped, ack, "{context}: the ack must round-trip");
        }
    }
}

// --- channel names ---------------------------------------------------------

#[test]
fn channel_name_golden_vectors() {
    for (file, doc) in fixture_files("channel_names") {
        for case in cases(&doc) {
            let context = format!("channel_names/{file} case {}", str_field(case, "name"));
            let input = &case["input"];
            let derived = channel_name(str_field(input, "namespace"), str_field(input, "stream"));

            assert_eq!(derived, str_field(&case["expect"], "channel"), "{context}");
            assert_eq!(derived.len(), 58, "{context}: 58 ASCII bytes, under PG's 63-byte limit");
        }
    }
}

// --- Turbo real fixtures ---------------------------------------------------

/// Flip one hex character of the token's signature half.
///
/// Produces a token that is structurally identical to the original but can
/// never carry a valid HMAC, which is exactly the negative the fixture's
/// `rust_contract.negative` rule asks for.
fn flip_signature_hex(token: &str) -> String {
    let (encoded, signature) = token.split_once("--").expect("token has no --");
    let mut chars: Vec<char> = signature.chars().collect();
    chars[0] = if chars[0] == '0' { '1' } else { '0' };
    let flipped: String = chars.into_iter().collect();
    format!("{encoded}--{flipped}")
}

/// Consume `tests/fixtures/signed_streams/turbo/turbo_real.json` per its
/// `rust_contract` block: real turbo-rails output must verify with the
/// base64-decoded key, resolve through `StreamsController` with the Turbo
/// profile enabled, reject a flipped signature, and never fall back to the
/// Orbitcast claims secret. The Turbo profile carries no expiry claim, so the
/// pinned clock below only proves resolution is clock-independent.
#[test]
fn turbo_real_fixtures() {
    const PINNED_NOW: i64 = 1_760_000_000;

    let path = fixtures_dir("signed_streams").join("turbo/turbo_real.json");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));
    let doc: Value = serde_json::from_str(&text).expect("turbo_real.json is valid JSON");

    for case in cases(&doc) {
        let context =
            format!("signed_streams/turbo/turbo_real.json case {}", str_field(case, "name"));
        let input = &case["input"];
        let expect = &case["expect"];

        // The key is raw bytes transported as strict base64; decoding it is
        // part of the contract (ORBITCAST_TURBO_SECRET_BASE64 semantics).
        let key = base64::engine::general_purpose::STANDARD
            .decode(str_field(input, "key_base64"))
            .unwrap_or_else(|err| panic!("{context}: key_base64 must decode: {err}"));

        let signed = str_field(input, "signed_stream_name");
        let identifier = str_field(input, "identifier");
        let expected_stream = str_field(expect, "stream");
        let verifier = MessageVerifier::new(&key);

        match str_field(case, "rust_status") {
            "match" => {
                // Bare verifier acceptance.
                let stream = verifier
                    .verify(signed)
                    .unwrap_or_else(|err| panic!("{context}: expected acceptance: {err}"));
                assert_eq!(stream, expected_stream, "{context}");

                // Controller path: turbo enabled + its own key handles and
                // resolves the identifier; string profiles never authorize
                // whisper or presence.
                let config = StreamsConfig {
                    turbo: true,
                    turbo_secret: Some(SecretBytes::new(key.clone())),
                    ..StreamsConfig::default()
                };
                let controller = StreamsController::new(config, "example_production".to_string());
                assert!(controller.handles(identifier), "{context}: controller must handle");

                let result = controller
                    .resolve_at(identifier, PINNED_NOW)
                    .unwrap_or_else(|err| panic!("{context}: expected resolution: {err}"));
                assert_eq!(result.stream, expected_stream, "{context}");
                assert!(!result.whisper, "{context}: Turbo names carry no whisper capability");

                // Negative: one flipped signature hex character must reject.
                let flipped = flip_signature_hex(signed);
                assert_matches!(
                    verifier.verify(&flipped),
                    Err(VerifyError::InvalidSignature),
                    "{context}: flipped signature must be InvalidSignature"
                );

                // Negative: a controller keyed with a *different* Turbo secret
                // must reject even when the Orbitcast claims secret holds the
                // right key — there is no cross-profile fallback.
                let config = StreamsConfig {
                    secret: Some(SecretBytes::new(key.clone())),
                    turbo: true,
                    turbo_secret: Some(SecretBytes::new(b"a-different-turbo-key".to_vec())),
                    ..StreamsConfig::default()
                };
                let controller = StreamsController::new(config, "example_production".to_string());
                let err = controller
                    .resolve_at(identifier, PINNED_NOW)
                    .expect_err("a wrong Turbo key must not verify");
                assert_matches!(
                    err,
                    StreamError::Verify(VerifyError::InvalidSignature),
                    "{context}: wrong key must fail the signature, got {err}"
                );
            }
            "mismatch" => {
                // Documented real turbo-rails output the current verifier does
                // NOT accept: assert the rejection rather than loosening the
                // verifier (fixture `mismatch_rule`).
                assert!(
                    verifier.verify(signed).is_err(),
                    "{context}: a mismatch case must stay rejected; if the verifier \
                     now accepts it, promote the case to rust_status \"match\""
                );
            }
            other => panic!("{context}: unknown rust_status {other:?}"),
        }
    }
}

// --- signed streams --------------------------------------------------------

/// The verifier's key, whichever encoding the case carries it in: text in
/// `input.secret`, raw bytes as strict base64 in `input.secret_base64`.
fn fixture_secret(context: &str, input: &Value) -> SecretBytes {
    match (input.get("secret"), input.get("secret_base64")) {
        (Some(text), None) => SecretBytes::new(text.as_str().unwrap().as_bytes().to_vec()),
        (None, Some(encoded)) => SecretBytes::new(
            base64::engine::general_purpose::STANDARD
                .decode(encoded.as_str().unwrap())
                .unwrap_or_else(|err| panic!("{context}: secret_base64 must decode: {err}")),
        ),
        _ => panic!("{context}: exactly one of input.secret / input.secret_base64"),
    }
}

/// The verifier's namespace: explicit in the newer files, `example_production`
/// by convention in the older ones. Never the token's own namespace, or the
/// isolation check would be vacuous.
const CONTRACT_NAMESPACE: &str = "example_production";

fn verifier_namespace(input: &Value) -> &str {
    input.get("namespace").map_or(CONTRACT_NAMESPACE, |v| v.as_str().unwrap())
}

/// Whether the decoded claims carry exactly the fields `expect.claims` lists:
/// a listed field matches, an unlisted one holds its fail-closed default.
fn assert_claims_match_expectation(context: &str, claims: &StreamClaims, expected: &Value) {
    assert_eq!(
        u64::from(claims.version),
        expected["version"].as_u64().unwrap(),
        "{context}: version"
    );
    for (field, actual) in [
        ("namespace", claims.namespace.as_str()),
        ("stream", claims.stream.as_str()),
        ("purpose", claims.purpose.as_str()),
    ] {
        match expected.get(field) {
            Some(value) => assert_eq!(actual, value.as_str().unwrap(), "{context}: {field}"),
            None => assert_eq!(actual, "", "{context}: absent {field} defaults to empty"),
        }
    }
    match expected.get("expires_at") {
        Some(value) => assert_eq!(claims.expires_at, value.as_i64().unwrap(), "{context}"),
        None => assert_eq!(claims.expires_at, 0, "{context}: absent expires_at is already expired"),
    }
    // The optional capability: absent means false, never anything wider.
    let expected_whisper = expected.get("whisper").is_some_and(|v| v.as_bool().unwrap());
    assert_eq!(claims.whisper, expected_whisper, "{context}: whisper");
}

/// The exact rejection a token missing `field` must produce: each absent
/// authorization-bearing claim defaults to a value that fails validation.
fn assert_missing_claim_rejection(context: &str, err: &ProtocolError, field: &str) {
    match field {
        "namespace" => {
            assert_matches!(err, ProtocolError::EmptyField { field: "namespace" }, "{context}");
        }
        "stream" => {
            assert_matches!(err, ProtocolError::EmptyField { field: "stream" }, "{context}");
        }
        "purpose" => assert_matches!(
            err,
            ProtocolError::InvalidPurpose { actual, .. } if actual.is_empty(),
            "{context}"
        ),
        "expires_at" => {
            assert_matches!(err, ProtocolError::Expired { expires_at: 0, .. }, "{context}");
        }
        other => panic!("{context}: unknown missing claim {other:?}"),
    }
}

#[test]
fn signed_stream_fixtures() {
    let mut error_kinds_seen = std::collections::BTreeSet::new();

    for (file, doc) in fixture_files("signed_streams") {
        for case in cases(&doc) {
            let context = format!("signed_streams/{file} case {}", str_field(case, "name"));
            let input = &case["input"];
            let expect = &case["expect"];

            let token = str_field(input, "token");
            let now = input["now"].as_i64().unwrap();
            let secret = fixture_secret(&context, input);
            let namespace = verifier_namespace(input);

            let (encoded, signature) =
                token.split_once("--").unwrap_or_else(|| panic!("{context}: token has no --"));
            assert!(
                signature.len() == 64 && signature.bytes().all(|b| b.is_ascii_hexdigit()),
                "{context}: signature must be 64 hex chars"
            );

            // The claims decode independently of the signature: assert the
            // fixture claim fields regardless of whether the token verifies.
            let claims_bytes =
                base64::engine::general_purpose::STANDARD.decode(encoded).unwrap_or_else(|err| {
                    panic!("{context}: encoded part is not strict base64: {err}")
                });
            let claims: StreamClaims = serde_json::from_slice(&claims_bytes)
                .unwrap_or_else(|err| panic!("{context}: claims must decode: {err}"));
            let expected_claims = &expect["claims"];
            assert_claims_match_expectation(&context, &claims, expected_claims);

            let signature_valid = expect["signature_valid"].as_bool().unwrap();
            let claims_valid = expect["claims_valid"].as_bool().unwrap();
            let valid = expect["valid"].as_bool().unwrap();

            // Run the token through the *real* verifier: HMAC over the encoded
            // bytes with the fixture key, then tolerant claims decode +
            // semantic validation, scoped to the *verifier's* namespace. The
            // injected `now` pins the expiry clock.
            let verifier = ClaimsVerifier::new(secret);
            let outcome = verifier.verify(token, namespace, now);
            if !valid {
                error_kinds_seen.insert(str_field(expect, "error_kind").to_string());
            }

            match outcome {
                Ok(accepted) => {
                    assert!(signature_valid, "{context}: signature unexpectedly valid");
                    assert!(claims_valid, "{context}: claims unexpectedly valid");
                    assert!(valid, "{context}: token unexpectedly valid");
                    assert_eq!(
                        accepted, claims,
                        "{context}: the verifier returns the decoded claims"
                    );
                    assert_eq!(accepted.stream, str_field(expect, "stream"), "{context}");
                    // A valid token authorizes its own stream, nothing else,
                    // and only in the namespace it was verified under.
                    assert!(
                        accepted.authorizes(namespace, &accepted.stream, now),
                        "{context}: valid claims must authorize their own stream"
                    );
                    assert!(
                        !accepted.authorizes(namespace, "some-other-stream", now),
                        "{context}: claims must not authorize a different stream"
                    );
                    assert!(
                        !accepted.authorizes("some_other_namespace", &accepted.stream, now),
                        "{context}: claims must not authorize another namespace"
                    );
                }
                Err(VerifyError::InvalidSignature) => {
                    assert!(!signature_valid, "{context}: signature unexpectedly invalid");
                    assert!(!valid, "{context}");
                    assert_eq!(str_field(expect, "error_kind"), "bad_signature", "{context}");
                }
                Err(VerifyError::Claims(err)) => {
                    // Signature passed; a structurally valid but semantically
                    // rejected claim carries a specific error kind.
                    assert!(signature_valid, "{context}: signature unexpectedly invalid");
                    assert!(!claims_valid, "{context}: claims unexpectedly valid");
                    assert!(!valid, "{context}");
                    match str_field(expect, "error_kind") {
                        "missing_claim" => {
                            let missing = str_field(expect, "missing");
                            assert!(
                                expected_claims.get(missing).is_none(),
                                "{context}: expect.claims must not invent {missing}"
                            );
                            assert_missing_claim_rejection(&context, &err, missing);
                        }
                        kind => assert_eq!(error_kind(&err), kind, "{context}"),
                    }
                }
                Err(VerifyError::NamespaceMismatch { expected, actual }) => {
                    assert!(signature_valid, "{context}: signature unexpectedly invalid");
                    assert!(!claims_valid, "{context}: claims unexpectedly valid");
                    assert!(!valid, "{context}");
                    assert_eq!(str_field(expect, "error_kind"), "wrong_namespace", "{context}");
                    assert_eq!(expected, namespace, "{context}");
                    assert_eq!(actual, str_field(expected_claims, "namespace"), "{context}");
                }
                Err(other) => panic!("{context}: unexpected verify error: {other}"),
            }
        }
    }

    // Every documented rejection kind is exercised by at least one fixture;
    // a new kind in the producer script must land here too.
    for kind in ["bad_signature", "expired", "wrong_purpose", "wrong_namespace", "missing_claim"] {
        assert!(error_kinds_seen.contains(kind), "no signed_streams fixture exercises {kind}");
    }
}

/// Consume `tests/fixtures/signed_streams/turbo/turbo_metadata_envelope.json`
/// per its `rust_contract` block: real `ActiveSupport::MessageVerifier`
/// output, correctly signed with the Turbo key but wrapped in the `_rails`
/// metadata envelope. The HMAC holds; the rejection is about the payload
/// shape, so a verifier failing on the signature here fails for the wrong
/// reason.
#[test]
fn turbo_metadata_envelope_fixtures() {
    use hmac::{Hmac, KeyInit, Mac};

    let path = fixtures_dir("signed_streams").join("turbo/turbo_metadata_envelope.json");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));
    let doc: Value =
        serde_json::from_str(&text).expect("turbo_metadata_envelope.json is valid JSON");

    let cases = cases(&doc);
    assert_ne!(cases.len(), 0);
    for case in cases {
        let context = format!(
            "signed_streams/turbo/turbo_metadata_envelope.json case {}",
            str_field(case, "name")
        );
        let input = &case["input"];
        let expect = &case["expect"];
        assert_eq!(str_field(case, "rust_status"), "match", "{context}");
        assert!(expect["rejected"].as_bool().unwrap(), "{context}");
        assert!(expect["stream"].is_null(), "{context}: a rejected case names no stream");
        assert_eq!(str_field(expect, "error_kind"), "metadata_envelope", "{context}");

        let key = base64::engine::general_purpose::STANDARD
            .decode(str_field(input, "key_base64"))
            .unwrap_or_else(|err| panic!("{context}: key_base64 must decode: {err}"));
        let signed = str_field(input, "signed_stream_name");

        // The signature is genuinely valid for the Turbo key.
        let (encoded, signature) = signed.split_once("--").unwrap();
        assert_eq!(encoded, str_field(expect, "encoded_payload"), "{context}");
        assert_eq!(signature, str_field(expect, "signature"), "{context}");
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
        mac.update(encoded.as_bytes());
        assert_eq!(hex::encode(mac.finalize().into_bytes()), signature, "{context}: HMAC holds");
        let payload = base64::engine::general_purpose::STANDARD.decode(encoded).unwrap();
        assert_eq!(payload, str_field(expect, "payload_json").as_bytes(), "{context}");

        // ... and the verifier rejects it for its shape, not its signature.
        assert_matches!(
            MessageVerifier::new(&key).verify(signed),
            Err(VerifyError::MetadataEnvelope),
            "{context}"
        );

        let config = StreamsConfig {
            turbo: true,
            turbo_secret: Some(SecretBytes::new(key.clone())),
            ..StreamsConfig::default()
        };
        let controller = StreamsController::new(config, CONTRACT_NAMESPACE.to_string());
        let identifier = str_field(input, "identifier");
        assert!(controller.handles(identifier), "{context}: it is a Turbo identifier");
        assert_matches!(
            controller.resolve(identifier),
            Err(StreamError::Verify(VerifyError::MetadataEnvelope)),
            "{context}"
        );
    }
}

/// Every file under `signed_streams/turbo/` is consumed by a dedicated test
/// above; a new fixture there must get one before it is checked in.
#[test]
fn every_turbo_fixture_file_is_consumed() {
    const CONSUMED: &[&str] = &["turbo_metadata_envelope.json", "turbo_real.json"];
    let mut files: Vec<String> = fs::read_dir(fixtures_dir("signed_streams/turbo"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    files.sort();
    assert_eq!(files, CONSUMED, "add a test for every new signed_streams/turbo fixture");
}
