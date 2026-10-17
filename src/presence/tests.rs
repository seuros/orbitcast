use std::assert_matches;

use super::*;

fn metadata(pairs: &[(&str, &str)]) -> Map<String, Value> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), Value::from(*v))).collect()
}

fn grant() -> PresenceGrant {
    PresenceGrant::from_effect(
        "chat:42",
        "user:7",
        &metadata(&[("name", "Marissa")]),
        45_000,
        &["join".to_string(), "leave".to_string()],
    )
    .expect("a well-formed grant")
}

const IDENTIFIER: &str = r#"{"channel":"ChatChannel","room":"42"}"#;
const OTHER: &str = r#"{"channel":"ChatChannel","room":"43"}"#;

// --- keys and derivation ------------------------------------------------

#[test]
fn presence_stream_keys_round_trip_and_stay_namespaced_by_derivation() {
    let key = presence_stream_key("chat:42");
    assert_eq!(key, "presence:chat:42");
    assert_eq!(presence_stream_target(&key), Some("chat:42"));
    assert_eq!(presence_stream_target("chat:42"), None);

    // The channel is derived from (namespace, key), so two deployments
    // never share a roster channel.
    let a = crate::pubsub::channel_for("app_a", &key);
    let b = crate::pubsub::channel_for("app_b", &key);
    assert_ne!(a, b);
    assert_ne!(a, crate::pubsub::channel_for("app_a", "chat:42"));
}

#[test]
fn lock_keys_are_stable_and_length_prefixed() {
    let key = MembershipKey::new("app", "chat:42", "user:7");
    assert_eq!(key.lock_key(), key.lock_key(), "derivation is pure");

    // Boundary shifting must not collide.
    assert_ne!(
        MembershipKey::new("ab", "c", "d").lock_key(),
        MembershipKey::new("a", "bc", "d").lock_key()
    );
    assert_ne!(
        MembershipKey::new("a", "bc", "d").lock_key(),
        MembershipKey::new("a", "b", "cd").lock_key()
    );

    // Different subjects on one stream lock independently.
    assert_ne!(key.lock_key(), MembershipKey::new("app", "chat:42", "user:8").lock_key());
}

#[test]
fn membership_keys_validate_their_parts() {
    MembershipKey::new("app", "chat:42", "user:7").validate().unwrap();
    assert_matches!(
        MembershipKey::new("", "chat:42", "user:7").validate().unwrap_err(),
        ProtocolError::EmptyField { field: "namespace" }
    );
    assert_matches!(
        MembershipKey::new("app", "", "user:7").validate().unwrap_err(),
        ProtocolError::EmptyField { field: "stream" }
    );
    assert_matches!(
        MembershipKey::new("app", "chat:42", "").validate().unwrap_err(),
        ProtocolError::EmptyField { field: "subject_id" }
    );
}

// --- grants -------------------------------------------------------------

#[test]
fn a_grant_keeps_the_exact_granted_identity_and_metadata() {
    let grant = grant();
    assert_eq!(grant.subject_id, "user:7");
    assert_eq!(grant.metadata_json, r#"{"name":"Marissa"}"#);
    assert_eq!(grant.ttl, Duration::from_secs(45));
    assert!(grant.allows(PresenceOp::Join));
    assert!(grant.allows(PresenceOp::Leave));
    assert!(!grant.allows(PresenceOp::Snapshot));

    let lease = grant.lease("app", "session-1", "node-7");
    assert_eq!(lease.key, MembershipKey::new("app", "chat:42", "user:7"));
    assert_eq!(lease.metadata_json, grant.metadata_json);
    assert_eq!(lease.session_id, "session-1");
    assert_eq!(lease.node_id, "node-7");
}

#[test]
fn a_grant_fails_closed_on_unusable_identity_ttl_or_metadata() {
    let err = PresenceGrant::from_effect("chat:42", "", &Map::new(), 1, &[]).unwrap_err();
    assert_matches!(
        err,
        PresenceError::InvalidGrant(ProtocolError::EmptyField { field: "subject_id" })
    );

    let err = PresenceGrant::from_effect("", "user:7", &Map::new(), 1, &[]).unwrap_err();
    assert_matches!(
        err,
        PresenceError::InvalidGrant(ProtocolError::EmptyField { field: "stream" })
    );

    let err = PresenceGrant::from_effect("chat:42", "user:7", &Map::new(), 0, &[]).unwrap_err();
    assert_matches!(
        err,
        PresenceError::InvalidGrant(ProtocolError::OutOfRange { field: "ttl_ms", .. })
    );

    let oversized = metadata(&[("bio", &"x".repeat(8192))]);
    let err = PresenceGrant::from_effect("chat:42", "user:7", &oversized, 1000, &[]).unwrap_err();
    assert_matches!(
        err,
        PresenceError::InvalidGrant(ProtocolError::FieldTooLong { field: "metadata", .. })
    );
}

#[test]
fn a_ttl_above_the_ceiling_is_clamped_not_honoured() {
    let grant =
        PresenceGrant::from_effect("chat:42", "user:7", &Map::new(), u64::MAX, &[]).unwrap();
    assert_eq!(grant.ttl, Duration::from_millis(MAX_PRESENCE_TTL_MS));
}

#[test]
fn unknown_granted_operations_are_dropped_not_fatal() {
    let grant = PresenceGrant::from_effect(
        "chat:42",
        "user:7",
        &Map::new(),
        1000,
        &["join".to_string(), "teleport".to_string()],
    )
    .expect("a future operation name does not invalidate the grant");
    assert_eq!(grant.operations, vec![PresenceOp::Join]);
}

#[test]
fn an_ungranted_client_can_do_nothing() {
    let grants = PresenceGrants::new();
    assert!(grants.is_empty());
    for op in [PresenceOp::Join, PresenceOp::Leave, PresenceOp::Snapshot] {
        assert_matches!(grants.authorize(IDENTIFIER, op).unwrap_err(), PresenceError::NotGranted);
    }
}

#[test]
fn a_grant_authorizes_only_its_own_subscription_and_operations() {
    let mut grants = PresenceGrants::new();
    grants.insert(IDENTIFIER, grant());
    assert_eq!(grants.len(), 1);

    let authorized = grants.authorize(IDENTIFIER, PresenceOp::Join).unwrap();
    assert_eq!(authorized.len(), 1);
    assert_eq!(authorized[0].subject_id, "user:7", "identity comes from the grant");

    // The snapshot operation was not granted.
    assert_matches!(
        grants.authorize(IDENTIFIER, PresenceOp::Snapshot).unwrap_err(),
        PresenceError::OperationNotAllowed { op: PresenceOp::Snapshot }
    );

    // Another subscription on the same connection is not covered.
    assert_matches!(
        grants.authorize(OTHER, PresenceOp::Join).unwrap_err(),
        PresenceError::NotGranted
    );
}

#[test]
fn a_snapshot_grant_is_separate_from_a_join_grant() {
    let mut grants = PresenceGrants::new();
    grants.insert(
        IDENTIFIER,
        PresenceGrant::from_effect(
            "chat:42",
            "user:7",
            &Map::new(),
            1000,
            &["snapshot".to_string()],
        )
        .unwrap(),
    );

    grants.authorize(IDENTIFIER, PresenceOp::Snapshot).unwrap();
    assert_matches!(
        grants.authorize(IDENTIFIER, PresenceOp::Join).unwrap_err(),
        PresenceError::OperationNotAllowed { op: PresenceOp::Join }
    );
}

#[test]
fn revoke_removes_only_the_named_subject() {
    let mut grants = PresenceGrants::new();
    grants.insert(IDENTIFIER, grant());

    // Wrong subject: not a way to drop a grant.
    assert!(grants.revoke(IDENTIFIER, "chat:42", "user:8").is_none());
    assert_eq!(grants.len(), 1);

    // Wrong stream, wrong identifier: likewise.
    assert!(grants.revoke(IDENTIFIER, "chat:43", "user:7").is_none());
    assert!(grants.revoke(OTHER, "chat:42", "user:7").is_none());
    assert_eq!(grants.len(), 1);

    let revoked = grants.revoke(IDENTIFIER, "chat:42", "user:7").expect("revoked");
    assert_eq!(revoked.grant.subject_id, "user:7");
    assert!(!revoked.joined, "nothing joined under it");
    assert!(grants.is_empty());
    assert_matches!(
        grants.authorize(IDENTIFIER, PresenceOp::Join).unwrap_err(),
        PresenceError::NotGranted
    );
}

#[test]
fn a_replacement_grant_wins_and_hands_back_what_it_displaced() {
    let mut grants = PresenceGrants::new();
    assert!(grants.insert(IDENTIFIER, grant()).is_none(), "nothing to displace");
    assert!(grants.set_joined(IDENTIFIER, "chat:42", true));
    assert!(grants.is_joined(IDENTIFIER, "chat:42"));

    let updated = PresenceGrant::from_effect(
        "chat:42",
        "user:7",
        &metadata(&[("name", "Marissa"), ("status", "away")]),
        1000,
        &["join".to_string(), "snapshot".to_string()],
    )
    .unwrap();
    let replaced = grants.insert(IDENTIFIER, updated).expect("the first grant is displaced");
    assert_eq!(replaced.grant.subject_id, "user:7");
    assert!(replaced.joined, "the caller learns a lease was live under it");

    assert_eq!(grants.len(), 1, "one grant per (identifier, stream)");
    assert!(!grants.is_joined(IDENTIFIER, "chat:42"), "a fresh entry starts not joined");
    let authorized = grants.authorize(IDENTIFIER, PresenceOp::Snapshot).unwrap();
    assert_eq!(authorized[0].metadata["status"], "away");

    // The joined flag follows the grant it was set on.
    assert!(!grants.set_joined(OTHER, "chat:42", true), "no such grant");
    assert!(!grants.is_joined(OTHER, "chat:42"));
}

#[test]
fn grants_are_dropped_with_the_streams_they_belong_to() {
    let mut grants = PresenceGrants::new();
    grants.insert(IDENTIFIER, grant());
    grants.insert(
        IDENTIFIER,
        PresenceGrant::from_effect("chat:43", "user:7", &Map::new(), 1000, &[]).unwrap(),
    );
    grants.set_joined(IDENTIFIER, "chat:43", true);

    // The subscription stopped streaming from chat:43 only.
    let dropped = grants.remove_where(IDENTIFIER, |grant| grant.stream == "chat:43");
    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].grant.stream, "chat:43");
    assert!(dropped[0].joined);
    assert_eq!(grants.len(), 1, "the other stream's grant survives");
    assert_eq!(grants.all()[0].1.stream, "chat:42");
}

#[test]
fn unsubscribe_and_close_take_their_grants_with_them() {
    let mut grants = PresenceGrants::new();
    grants.insert(IDENTIFIER, grant());
    grants.insert(
        OTHER,
        PresenceGrant::from_effect("chat:43", "user:7", &Map::new(), 1000, &[]).unwrap(),
    );

    let removed = grants.remove_identifier(IDENTIFIER);
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].grant.stream, "chat:42");
    assert_eq!(grants.len(), 1, "the other subscription keeps its grant");

    let drained = grants.drain();
    assert_eq!(drained.len(), 1);
    assert!(grants.is_empty());
}

#[test]
fn transitions_announce_only_the_subject_level_edges() {
    assert!(Transition::FirstJoin.is_announced());
    assert!(Transition::LastLeave.is_announced());
    assert!(!Transition::Refreshed.is_announced());
    assert!(!Transition::Reduced.is_announced());
    assert!(!Transition::Unchanged.is_announced());
}

#[test]
fn every_refusal_has_a_stable_wire_code() {
    const CASES: &[(PresenceError, &str)] = &[
        (PresenceError::NotGranted, "not_granted"),
        (PresenceError::OperationNotAllowed { op: PresenceOp::Join }, "operation_not_allowed"),
        (PresenceError::Unavailable, "unavailable"),
    ];
    for (error, code) in CASES {
        assert_eq!(error.code(), *code, "{error}");
    }
    assert_eq!(
        PresenceError::StreamNotSubscribed { stream: "chat:42".to_string() }.code(),
        "stream_not_subscribed"
    );
    assert_eq!(
        PresenceError::InvalidGrant(ProtocolError::EmptyField { field: "stream" }).code(),
        "invalid_grant"
    );
    assert_eq!(
        PresenceError::InvalidCommand(ProtocolError::EmptyField { field: "op" }).code(),
        "invalid_command"
    );
    assert_eq!(PresenceError::Backend(anyhow::anyhow!("down")).code(), "backend");
}

// --- sessions -----------------------------------------------------------

#[test]
fn sessions_track_leases_per_connection_with_their_own_ttls() {
    let sessions = PresenceSessions::new();
    assert!(sessions.is_empty());
    assert_eq!(sessions.lease_count(), 0);

    let short = grant().lease("app", "session-1", "node-7");
    let mut long = PresenceGrant::from_effect("chat:43", "user:7", &Map::new(), 90_000, &[])
        .unwrap()
        .lease("app", "session-1", "node-7");
    long.session_id = "session-1".to_string();

    sessions.hold(1, &short);
    sessions.hold(1, &long);
    sessions.hold(2, &grant().lease("app", "session-2", "node-7"));
    assert_eq!(sessions.len(), 2, "two connections hold leases");
    assert_eq!(sessions.lease_count(), 3, "three leases between them");

    let mut live = sessions.live_leases(Duration::from_mins(1));
    live.sort_by(|a, b| (&a.session_id, &a.key).cmp(&(&b.session_id, &b.key)));
    assert_eq!(live.len(), 3);
    assert_eq!(live[0].key.stream, "chat:42");
    assert_eq!(live[0].ttl, Duration::from_secs(45), "each lease keeps the TTL it was granted");
    assert_eq!(live[1].key.stream, "chat:43");
    assert_eq!(live[1].ttl, Duration::from_secs(90));
    assert_eq!(live[2].session_id, "session-2");

    // A silent socket is left out of the refresh batch without losing its
    // bookkeeping; its leases expire on their own.
    assert_eq!(sessions.live_leases(Duration::ZERO), Vec::<HeldLease>::new());
    assert_eq!(sessions.lease_count(), 3);

    // An explicit leave releases one lease; the last one releases the
    // connection; a release of something never held is reported as such.
    assert!(sessions.release(1, &short.key));
    assert!(!sessions.release(1, &short.key));
    assert_eq!(sessions.lease_count(), 2);
    assert!(sessions.release(1, &long.key));
    assert_eq!(sessions.len(), 1, "a connection with no lease is not tracked");

    sessions.forget(2);
    assert!(sessions.is_empty());
}

#[test]
fn liveness_is_recorded_only_for_connections_holding_a_lease() {
    let sessions = PresenceSessions::new();
    sessions.record_liveness(9);
    assert!(sessions.is_empty(), "a socket without a lease is not tracked");

    sessions.hold(9, &grant().lease("app", "session-9", "node-7"));
    sessions.record_liveness(9);
    assert_eq!(sessions.live_leases(Duration::from_secs(1)).len(), 1);
}
