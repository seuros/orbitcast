//! presence: grants, refusals, and the no-spoofing rule
//!
//! Two stores drive these. `FakePresence` records what it was asked to do and
//! answers every call the same way: it isolates the *authorization* reducer -
//! which subject a lease carries, who may ask for one, and what is released
//! when. `MemoryPresence` is the real single-process store: it carries the
//! aggregation tests (two tabs, one join; replacement grants) that need a
//! store that actually counts. The PostgreSQL behaviour (expiry, locking) is
//! asserted against a live database in `presence::postgres`.

use std::assert_matches;

use super::*;
use crate::presence::memory::MemoryPresence;
use crate::presence::test_support::RecordingPublisher;
use crate::presence::{HeldLease, Lease, MembershipKey, PresencePublisher};
use crate::protocol::v1::presence::{PresenceEvent, PresencePage};

/// A presence store that records what it was asked to do.
#[derive(Debug, Default)]
struct FakePresence {
    joins: Mutex<Vec<Lease>>,
    leaves: Mutex<Vec<(MembershipKey, String)>>,
    snapshots: Mutex<Vec<(String, PresencePage)>>,
    invalidated: Mutex<Vec<String>>,
}

impl FakePresence {
    fn joins(&self) -> Vec<Lease> {
        self.joins.lock().expect("fake presence poisoned").clone()
    }

    fn leaves(&self) -> Vec<(MembershipKey, String)> {
        self.leaves.lock().expect("fake presence poisoned").clone()
    }

    fn snapshots(&self) -> Vec<(String, PresencePage)> {
        self.snapshots.lock().expect("fake presence poisoned").clone()
    }
}

#[async_trait::async_trait]
impl PresenceBackend for FakePresence {
    async fn join(
        &self,
        lease: &Lease,
    ) -> Result<crate::presence::TransitionOutcome, PresenceError> {
        self.joins.lock().expect("fake presence poisoned").push(lease.clone());
        Ok(crate::presence::TransitionOutcome {
            transition: crate::presence::Transition::FirstJoin,
            total: 1,
        })
    }

    async fn leave(
        &self,
        key: &MembershipKey,
        session_id: &str,
    ) -> Result<crate::presence::TransitionOutcome, PresenceError> {
        self.leaves
            .lock()
            .expect("fake presence poisoned")
            .push((key.clone(), session_id.to_string()));
        Ok(crate::presence::TransitionOutcome {
            transition: crate::presence::Transition::LastLeave,
            total: 0,
        })
    }

    async fn snapshot(
        &self,
        stream: &str,
        page: PresencePage,
    ) -> Result<crate::presence::PresenceSnapshot, PresenceError> {
        self.snapshots.lock().expect("fake presence poisoned").push((stream.to_string(), page));
        Ok(crate::presence::PresenceSnapshot {
            members: vec![crate::protocol::v1::presence::PresenceMember {
                subject_id: "user:7".to_string(),
                metadata: Map::new(),
            }],
            total: 1,
            has_more: false,
        })
    }

    async fn refresh(&self, leases: &[HeldLease]) -> Result<u64, PresenceError> {
        Ok(leases.len() as u64)
    }

    fn invalidate(&self, stream: &str) {
        self.invalidated.lock().expect("fake presence poisoned").push(stream.to_string());
    }

    fn invalidate_all(&self) {
        self.invalidated.lock().expect("fake presence poisoned").push("*".to_string());
    }
}

/// Install `stream` on the session's subscription, as an `add_stream` effect
/// would: in Rails' view of the subscription and as a hub membership.
async fn stream_from(session: &mut SessionDispatch, stream: &str) {
    session
        .subscriptions
        .get_mut(IDENTIFIER)
        .expect("the test subscription exists")
        .streams
        .insert(stream.to_string());
    session
        .context
        .hub
        .join(session.conn_id, IDENTIFIER, stream, async { true })
        .await
        .expect("membership installed");
}

/// A session on `conn_id` whose node has `presence`, already streaming
/// from `chat:42` - the stream the test grants are for.
async fn tab(
    conn_id: u32,
    hub: &Arc<Hub>,
    backend: &Arc<Backend>,
    presence: Arc<dyn PresenceBackend>,
) -> SessionDispatch {
    if conn_id != 1 {
        hub.add_session(Session::new(conn_id, "/cable".to_string()));
    }
    let mut session = SessionDispatch::new(
        context(Arc::clone(backend), Arc::clone(hub), Some(presence)),
        conn_id,
        Arc::new(SessionGate::new(u64::from(conn_id))),
        format!("/cable?tab={conn_id}"),
        "127.0.0.1".to_string(),
        &HashMap::new(),
    );
    session.identified = true;
    session.sequence = 1;
    session.subscriptions.insert(IDENTIFIER.to_string(), SubscriptionRecord::default());
    hub.ensure_subscription(conn_id, IDENTIFIER).expect("session exists");
    stream_from(&mut session, "chat:42").await;
    session
}

/// A session whose node has a (fake) presence store.
async fn presence_session(
    hub: &Arc<Hub>,
    backend: Arc<Backend>,
    presence: Arc<FakePresence>,
) -> SessionDispatch {
    tab(1, hub, &backend, presence as Arc<dyn PresenceBackend>).await
}

/// The real single-process store, announcing into a recorder.
fn memory_store() -> (Arc<MemoryPresence>, Arc<RecordingPublisher>) {
    let recorder = Arc::new(RecordingPublisher::default());
    let store = MemoryPresence::start(
        "test",
        Arc::clone(&recorder) as Arc<dyn PresencePublisher>,
        Duration::from_hours(1),
    );
    (store, recorder)
}

fn grant_effect(operations: &[&str]) -> Effect {
    grant_effect_for("chat:42", "user:7", operations)
}

fn grant_effect_for(stream: &str, subject_id: &str, operations: &[&str]) -> Effect {
    Effect::PresenceGrant {
        identifier: IDENTIFIER.to_string(),
        stream: stream.to_string(),
        subject_id: subject_id.to_string(),
        metadata: Map::from_iter([("name".to_string(), Value::from("Marissa"))]),
        ttl_ms: 45_000,
        operations: operations.iter().map(|op| (*op).to_string()).collect(),
    }
}

/// Apply one reply carrying `effects` as the session's next command.
async fn apply_effects(session: &mut SessionDispatch, effects: Vec<Effect>) {
    session.sequence += 1;
    let reply = reply(session, effects);
    session.apply(&reply, Some(IDENTIFIER)).await.expect("applied");
}

/// The exact frame a stock ActionCable client sends for a presence op.
fn presence_data(op: &str) -> String {
    format!(r#"{{"action":"orbitcast_presence","op":"{op}"}}"#)
}

/// The presence messages among the frames queued so far.
fn presence_frames(rx: &mut mpsc::Receiver<Outgoing>) -> Vec<serde_json::Value> {
    frames(rx).into_iter().filter(|frame| frame["message"]["type"] == "presence").collect()
}

#[tokio::test]
async fn a_presence_grant_registers_the_grant_and_its_roster_membership() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let reply = reply(&session, vec![grant_effect(&["join", "leave"])]);
    session.apply(&reply, Some(IDENTIFIER)).await.expect("applied");

    assert_eq!(session.grants.len(), 1);
    assert_eq!(session.revision, 1, "the reply still advances the revision");

    // The roster arrives as an ordinary publication on the derived key,
    // addressed with this subscription's own identifier.
    let key = presence_stream_key("chat:42");
    assert_eq!(hub.stream_member_count(&key), 1);
    assert_eq!(
        session.subscriptions[IDENTIFIER].streams.iter().collect::<Vec<_>>(),
        ["chat:42"],
        "the roster key is not part of Rails' own view of installed streams"
    );
    assert!(!session.grants.is_joined(IDENTIFIER, "chat:42"), "a grant is not a join");
    assert_eq!(presence.joins(), Vec::<Lease>::new());
}

#[tokio::test]
async fn a_roster_event_reaches_only_granted_subscriptions() {
    let (hub, backend, mut rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    // A second subscription on the same connection, without a grant.
    session.subscriptions.insert(OTHER.to_string(), SubscriptionRecord::default());
    hub.ensure_subscription(1, OTHER).expect("session 1 exists");

    let granted = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    hub.activate(1, IDENTIFIER);
    let _ = frames(&mut rx);

    // What another node's transition looks like when it lands here.
    let event = r#"{"version":1,"type":"presence","event":"join","stream":"chat:42",
                            "subject_id":"user:9","metadata":{},"total":2}"#;
    hub.deliver(&presence_stream_key("chat:42"), event.as_bytes());

    let sent = frames(&mut rx);
    assert_eq!(sent.len(), 1, "only the granted subscription is a member of the roster");
    assert_eq!(
        sent[0]["identifier"], IDENTIFIER,
        "and it is addressed with its own identifier, not the roster key"
    );
    assert_eq!(sent[0]["message"]["type"], "presence");
    assert_eq!(sent[0]["message"]["event"], "join");
    assert_eq!(sent[0]["message"]["subject_id"], "user:9");
}

#[tokio::test]
async fn a_grant_is_refused_when_this_node_has_no_presence_store() {
    let (hub, backend, _rx) = parts();
    let mut session = session(&hub, backend);
    stream_from(&mut session, "chat:42").await;

    let reply = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&reply, Some(IDENTIFIER)).await.expect("applied");

    assert!(session.grants.is_empty(), "a grant this node cannot honour is not kept");
    assert_eq!(hub.stream_count(), 1, "no roster membership is installed beside the stream");
    assert_eq!(session.revision, 1, "but the rest of the reply still applies");
}

#[tokio::test]
async fn an_unusable_grant_is_dropped_without_failing_the_reply() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let reply = reply(
        &session,
        vec![Effect::PresenceGrant {
            identifier: IDENTIFIER.to_string(),
            stream: "chat:42".to_string(),
            subject_id: String::new(), // no subject: nothing to lease
            metadata: Map::new(),
            ttl_ms: 45_000,
            operations: vec!["join".to_string()],
        }],
    );
    session.apply(&reply, Some(IDENTIFIER)).await.expect("applied");

    assert!(session.grants.is_empty());
    assert_eq!(hub.stream_count(), 1);
}

#[tokio::test]
async fn a_grant_for_a_stream_the_subscription_does_not_hold_is_refused() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    // The typed refusal, from the reducer's own entry point.
    let refused = session
        .install_grant(IDENTIFIER, "chat:43", "user:7", &Map::new(), 45_000, &["join".to_string()])
        .await
        .expect_err("chat:43 is not one of this subscription's streams");
    assert_matches!(
        refused,
        PresenceError::StreamNotSubscribed { ref stream } if stream == "chat:43"
    );
    assert_eq!(refused.code(), "stream_not_subscribed");

    // Through a reply: the grant is dropped, the reply still applies, and
    // no roster interest was taken for a stream nobody streams from.
    let reply = reply(&session, vec![grant_effect_for("chat:43", "user:7", &["join"])]);
    session.apply(&reply, Some(IDENTIFIER)).await.expect("applied");
    assert!(session.grants.is_empty());
    assert_eq!(hub.stream_member_count(&presence_stream_key("chat:43")), 0);
    assert_eq!(session.revision, 1);

    // A grant that arrives *before* its stream in the same reply is refused
    // too: the order of effects is the order Rails ran its callbacks in.
    apply_effects(
        &mut session,
        vec![
            grant_effect_for("chat:43", "user:7", &["join"]),
            Effect::AddStream { identifier: IDENTIFIER.to_string(), stream: "chat:43".to_string() },
        ],
    )
    .await;
    assert!(session.grants.is_empty(), "stream_from must precede grant_presence");
    assert_eq!(hub.stream_member_count("chat:43"), 1, "the stream itself was installed");
}

#[tokio::test]
async fn a_presence_revoke_drops_the_grant_the_lease_and_the_roster() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let granted = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    session.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();
    assert_eq!(session.context.presence_sessions.lease_count(), 1);

    apply_effects(
        &mut session,
        vec![Effect::PresenceRevoke {
            identifier: IDENTIFIER.to_string(),
            stream: "chat:42".to_string(),
            subject_id: "user:7".to_string(),
        }],
    )
    .await;

    assert!(session.grants.is_empty());
    assert_eq!(presence.leaves().len(), 1, "the lease is released, not left to expire");
    assert_eq!(session.context.presence_sessions.lease_count(), 0, "and no longer refreshed");
    assert_eq!(
        hub.stream_member_count(&presence_stream_key("chat:42")),
        0,
        "and the roster membership goes with it"
    );
    assert_eq!(hub.stream_member_count("chat:42"), 1, "the application stream stays");

    // The client can no longer do anything with presence.
    let refused = session.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap_err();
    assert_matches!(refused.error, PresenceError::NotGranted);
}

#[tokio::test]
async fn a_revoke_for_another_subject_changes_nothing() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let granted = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");

    apply_effects(
        &mut session,
        vec![Effect::PresenceRevoke {
            identifier: IDENTIFIER.to_string(),
            stream: "chat:42".to_string(),
            subject_id: "user:8".to_string(),
        }],
    )
    .await;

    assert_eq!(session.grants.len(), 1);
    let leaves = presence.leaves();
    assert!(leaves.is_empty(), "another subject's revoke releases nothing: {leaves:?}");
}

#[tokio::test]
async fn an_ungranted_client_cannot_join() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    for op in ["join", "leave", "snapshot"] {
        let refused = session
            .apply_presence(IDENTIFIER, &presence_data(op))
            .await
            .expect_err("an ungranted subscription has no presence identity");
        assert_matches!(refused.error, PresenceError::NotGranted, "{op}");
        assert_eq!(refused.op.map(PresenceOp::as_str), Some(op), "the refusal names the operation");
        assert!(refused.stream.is_none(), "but no roster: none was resolved");
    }

    assert!(presence.joins().is_empty(), "nothing reached the store");
    assert!(session.context.presence_sessions.is_empty());
}

#[tokio::test]
async fn a_client_cannot_choose_its_own_subject_or_metadata() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let granted = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");

    // A client doing its very best to be somebody else.
    let forged = r#"{"action":"orbitcast_presence","op":"join",
                             "subject_id":"user:999","metadata":{"role":"admin"},
                             "ttl_ms":86400000,"stream":"chat:other"}"#;
    assert_eq!(session.apply_presence(IDENTIFIER, forged).await.unwrap(), 1);

    let joins = presence.joins();
    assert_eq!(joins.len(), 1);
    assert_eq!(joins[0].key.subject_id, "user:7", "identity comes from the grant");
    assert_eq!(joins[0].key.stream, "chat:42", "so does the stream");
    assert_eq!(joins[0].metadata_json, r#"{"name":"Marissa"}"#, "and so does the metadata");
    assert_eq!(joins[0].session_id, session.session_id, "the lease is per session");
    assert_eq!(joins[0].node_id, "node-test");
    assert_eq!(joins[0].ttl, Duration::from_secs(45));
    assert!(session.grants.is_joined(IDENTIFIER, "chat:42"));

    // Joining also makes this connection refreshable, with the lease's TTL.
    let sessions = &session.context.presence_sessions;
    assert_eq!(sessions.len(), 1);
    let live = sessions.live_leases(Duration::from_mins(1));
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].key, joins[0].key);
    assert_eq!(live[0].ttl, Duration::from_secs(45));
}

#[tokio::test]
async fn a_grant_cannot_lengthen_a_lease_past_this_nodes_ttl() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let mut effect = grant_effect(&["join"]);
    if let Effect::PresenceGrant { ref mut ttl_ms, .. } = effect {
        *ttl_ms = 24 * 60 * 60 * 1000;
    }
    let granted = reply(&session, vec![effect]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");

    session.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();
    assert_eq!(
        presence.joins()[0].ttl,
        session.context.presence_ttl,
        "a grant may shorten a lease, never widen it"
    );
}

#[tokio::test]
async fn a_shorter_granted_ttl_is_what_the_refresh_batch_carries() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let mut effect = grant_effect(&["join"]);
    if let Effect::PresenceGrant { ref mut ttl_ms, .. } = effect {
        *ttl_ms = 5_000;
    }
    let granted = reply(&session, vec![effect]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    session.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();

    let live = session.context.presence_sessions.live_leases(Duration::from_mins(1));
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].ttl, Duration::from_secs(5), "the grant's TTL, for the lease's whole life");
}

#[tokio::test]
async fn a_snapshot_needs_the_snapshot_operation() {
    let (hub, backend, mut rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    // Join-only grant: a snapshot is refused.
    let granted = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    let refused = session.apply_presence(IDENTIFIER, &presence_data("snapshot")).await.unwrap_err();
    assert_matches!(refused.error, PresenceError::OperationNotAllowed { op: PresenceOp::Snapshot });
    let requested = presence.snapshots();
    assert!(requested.is_empty(), "a refused snapshot never reaches the store: {requested:?}");

    // Re-granted with the operation: one addressed page comes back.
    apply_effects(&mut session, vec![grant_effect(&["join", "snapshot"])]).await;
    let _ = frames(&mut rx);

    let paged = r#"{"action":"orbitcast_presence","op":"snapshot","limit":25,"offset":50}"#;
    assert_eq!(session.apply_presence(IDENTIFIER, paged).await.unwrap(), 1);

    let requested = presence.snapshots();
    assert_eq!(requested.len(), 1);
    assert_eq!(requested[0].0, "chat:42");
    assert_eq!(requested[0].1.limit, 25);
    assert_eq!(requested[0].1.offset, 50);

    let sent = frames(&mut rx);
    assert_eq!(sent.len(), 1, "a snapshot answers the asking subscription only");
    assert_eq!(sent[0]["identifier"], IDENTIFIER);
    assert_eq!(sent[0]["message"]["type"], "presence");
    assert_eq!(sent[0]["message"]["event"], "snapshot");
    assert_eq!(sent[0]["message"]["members"][0]["subject_id"], "user:7");
    assert_eq!(sent[0]["message"]["page"]["limit"], 25);
    assert_eq!(sent[0]["message"]["page"]["offset"], 50);
    assert_eq!(sent[0]["message"]["total"], 1);
}

#[tokio::test]
async fn a_malformed_presence_command_is_a_refusal_not_a_rails_action() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let granted = reply(&session, vec![grant_effect(&["join", "snapshot"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");

    for data in [
        r#"{"action":"orbitcast_presence","op":"lurk"}"#,
        r#"{"action":"orbitcast_presence"}"#,
        r#"{"action":"orbitcast_presence","op":"snapshot","limit":5000}"#,
    ] {
        let refused = session.apply_presence(IDENTIFIER, data).await.unwrap_err();
        assert_matches!(refused.error, PresenceError::InvalidCommand(_), "{data}");
        assert!(refused.op.is_none(), "{data}: nothing decoded far enough to name an op");
    }
    let (joins, snapshots) = (presence.joins(), presence.snapshots());
    assert!(joins.is_empty(), "a malformed command never joins: {joins:?}");
    assert!(snapshots.is_empty(), "a malformed command never pages: {snapshots:?}");
}

#[tokio::test]
async fn a_refused_operation_is_answered_with_a_typed_error_on_the_subscription() {
    let (hub, backend, mut rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;
    hub.activate(1, IDENTIFIER);

    // No grant at all: the refusal names the operation, no roster.
    session.run_presence(IDENTIFIER, &presence_data("join")).await;
    let sent = presence_frames(&mut rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["identifier"], IDENTIFIER, "answered on the asking subscription");
    assert_eq!(sent[0]["message"]["event"], "error");
    assert_eq!(sent[0]["message"]["reason"], "not_granted");
    assert_eq!(sent[0]["message"]["op"], "join");
    assert!(sent[0]["message"].get("stream").is_none());
    assert_eq!(sent[0]["message"]["version"], 1);

    // A grant without the operation: the refusal names the roster too.
    apply_effects(&mut session, vec![grant_effect(&["join"])]).await;
    let _ = frames(&mut rx);
    session.run_presence(IDENTIFIER, &presence_data("snapshot")).await;
    let sent = presence_frames(&mut rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["message"]["reason"], "operation_not_allowed");
    assert_eq!(sent[0]["message"]["op"], "snapshot");
    assert_eq!(sent[0]["message"]["stream"], "chat:42");

    // A command that did not decode: the reason, and nothing else.
    session.run_presence(IDENTIFIER, r#"{"action":"orbitcast_presence","op":"lurk"}"#).await;
    let sent = presence_frames(&mut rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["message"]["reason"], "invalid_command");
    assert!(sent[0]["message"].get("op").is_none());
    assert!(sent[0]["message"].get("stream").is_none());

    assert!(!session.closed, "a refusal never costs the client its connection");
}

#[tokio::test]
async fn leaving_releases_the_lease_from_the_refresh_registry() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let granted = reply(&session, vec![grant_effect(&["join", "leave"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");

    session.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();
    assert_eq!(session.context.presence_sessions.lease_count(), 1);

    session.apply_presence(IDENTIFIER, &presence_data("leave")).await.unwrap();
    assert_eq!(presence.leaves().len(), 1);
    assert_eq!(
        session.context.presence_sessions.lease_count(),
        0,
        "a client that left is neither refreshed nor counted while its socket lives on"
    );
    assert!(!session.grants.is_joined(IDENTIFIER, "chat:42"));
    assert_eq!(session.grants.len(), 1, "the grant itself stays until Rails revokes it");

    // Leaving twice is a second leave at the store (idempotent there), not a
    // refusal here.
    session.apply_presence(IDENTIFIER, &presence_data("leave")).await.unwrap();
    assert_eq!(presence.leaves().len(), 2);
}

#[tokio::test]
async fn unsubscribe_and_close_release_this_sessions_leases() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let granted = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    session.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();

    session.unsubscribe(IDENTIFIER.to_string()).await;
    assert!(session.grants.is_empty());
    assert_eq!(presence.leaves().len(), 1);
    assert_eq!(presence.leaves()[0].0.subject_id, "user:7");
    assert_eq!(presence.leaves()[0].1, session.session_id, "leaves are per session");
    assert_eq!(hub.stream_count(), 0, "the roster membership went with it");
    assert_eq!(session.context.presence_sessions.lease_count(), 0);

    // A close has nothing left to release, and says so silently.
    session.release_presence().await;
    assert_eq!(presence.leaves().len(), 1);
}

#[tokio::test]
async fn a_socket_close_releases_every_remaining_lease_but_not_an_unjoined_grant() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    // Two grants on one subscription: one joined, one only granted.
    stream_from(&mut session, "chat:43").await;
    let granted = reply(
        &session,
        vec![grant_effect(&["join"]), grant_effect_for("chat:43", "user:7", &["join"])],
    );
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    session
        .join_lease(IDENTIFIER, &session.grants.for_identifier(IDENTIFIER)[0].clone())
        .await
        .unwrap();

    session.release_presence().await;
    assert!(session.grants.is_empty());
    let leaves = presence.leaves();
    assert_eq!(leaves.len(), 1, "only the joined grant had a lease to release: {leaves:?}");
    assert_eq!(leaves[0].0.stream, "chat:42");
    assert_eq!(hub.stream_member_count(&presence_stream_key("chat:42")), 0);
    assert_eq!(hub.stream_member_count(&presence_stream_key("chat:43")), 0);
}

#[tokio::test]
async fn stop_all_streams_and_stop_stream_release_the_lease_and_the_grant() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let granted = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    session.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();

    // `stop_all_streams`: the membership that carried the grant is gone.
    apply_effects(
        &mut session,
        vec![Effect::RemoveAllStreams { identifier: IDENTIFIER.to_string() }],
    )
    .await;
    assert!(session.grants.is_empty(), "the grant goes with its stream");
    assert_eq!(presence.leaves().len(), 1, "and the lease is released, not dropped on the floor");
    assert_eq!(session.context.presence_sessions.lease_count(), 0);
    assert_eq!(hub.stream_count(), 0);
    assert!(hub.has_subscription(1, IDENTIFIER), "the subscription stays addressable");

    // `stop_stream_from` of the granted stream: same path, same outcome.
    stream_from(&mut session, "chat:42").await;
    apply_effects(&mut session, vec![grant_effect(&["join"])]).await;
    session.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();
    apply_effects(
        &mut session,
        vec![Effect::RemoveStream {
            identifier: IDENTIFIER.to_string(),
            stream: "chat:42".to_string(),
        }],
    )
    .await;
    assert!(session.grants.is_empty());
    assert_eq!(presence.leaves().len(), 2);
    assert_eq!(hub.stream_count(), 0);

    // Stopping an *unrelated* stream leaves the grant alone.
    stream_from(&mut session, "chat:42").await;
    stream_from(&mut session, "chat:all").await;
    apply_effects(&mut session, vec![grant_effect(&["join"])]).await;
    apply_effects(
        &mut session,
        vec![Effect::RemoveStream {
            identifier: IDENTIFIER.to_string(),
            stream: "chat:all".to_string(),
        }],
    )
    .await;
    assert_eq!(session.grants.len(), 1, "the grant is bound to chat:42, not chat:all");
}

#[tokio::test]
async fn a_presence_action_is_never_dispatched_to_rails() {
    let (hub, backend, _rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;

    let granted = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    let sequence_before = session.sequence;

    // The Rails endpoint is unreachable in these tests, so a dispatched
    // action would close the session and burn a sequence number.
    session.run_action(IDENTIFIER.to_string(), presence_data("join")).await;

    assert!(!session.closed, "presence is answered locally");
    assert_eq!(session.sequence, sequence_before, "no RPC was issued");
    assert_eq!(presence.joins().len(), 1);
}

#[tokio::test]
async fn a_presence_notification_invalidates_the_local_snapshot_cache() {
    let presence = Arc::new(FakePresence::default());
    let backend = Arc::new(
        Backend::with_pubsub(ScriptedPubSub::new())
            .with_presence(Arc::clone(&presence) as Arc<dyn PresenceBackend>),
    );
    let (tx, _rx) = mpsc::channel(16);
    let hub = Arc::new(Hub::new(tx));
    backend.register_hub(&hub);

    backend.deliver(&presence_stream_key("chat:42"), br#"{"type":"presence"}"#);
    backend.deliver("chat:42", br#"{"body":"hi"}"#);

    let invalidated = presence.invalidated.lock().expect("fake presence poisoned").clone();
    assert_eq!(
        invalidated,
        vec!["chat:42".to_string()],
        "only a roster publication drops the cache, and it names the application stream"
    );
}

#[tokio::test]
async fn a_pubsub_reconnect_reannounces_a_roster_page_to_snapshot_grants_only() {
    let (hub, backend, mut rx) = parts();
    let presence = Arc::new(FakePresence::default());
    let mut session = presence_session(&hub, backend, Arc::clone(&presence)).await;
    hub.activate(1, IDENTIFIER);

    // One grant that may read the roster, one that may not.
    stream_from(&mut session, "chat:43").await;
    let granted = reply(
        &session,
        vec![grant_effect(&["join", "snapshot"]), grant_effect_for("chat:43", "user:7", &["join"])],
    );
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    let _ = frames(&mut rx);

    // What the driver sends every session once the listener is back.
    session.resync_presence().await;

    let sent = presence_frames(&mut rx);
    assert_eq!(sent.len(), 1, "one page per grant that may read a roster: {sent:?}");
    assert_eq!(sent[0]["identifier"], IDENTIFIER);
    assert_eq!(sent[0]["message"]["event"], "snapshot");
    assert_eq!(sent[0]["message"]["stream"], "chat:42");
    assert_eq!(sent[0]["message"]["page"]["limit"], 50);
    let requested = presence.snapshots();
    assert_eq!(requested.len(), 1);
    assert_eq!(requested[0].0, "chat:42", "the join-only grant on chat:43 reads nothing");
}

// --- aggregation, on the real single-process store --------------------------

#[tokio::test]
async fn two_tabs_produce_one_join_and_a_last_session_leave() {
    let (hub, backend, _rx) = parts();
    let (store, recorder) = memory_store();
    let presence = Arc::clone(&store) as Arc<dyn PresenceBackend>;
    let mut first = tab(1, &hub, &backend, Arc::clone(&presence)).await;
    let mut second = tab(2, &hub, &backend, Arc::clone(&presence)).await;

    for session in [&mut first, &mut second] {
        let granted = reply(session, vec![grant_effect(&["join", "leave", "snapshot"])]);
        session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    }
    assert_eq!(hub.stream_member_count(&presence_stream_key("chat:42")), 2);

    // Two sessions of one subject: one subject-level join.
    first.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();
    second.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();
    assert_eq!(recorder.event_kinds(), vec![PresenceEvent::Join]);
    assert_eq!(recorder.events(PresenceEvent::Join)[0].subject_id.as_deref(), Some("user:7"));
    let roster = store.snapshot("chat:42", PresencePage::default()).await.unwrap();
    assert_eq!(roster.total, 1, "two tabs are one subject");

    // Each tab's registry entry is its own.
    assert_eq!(first.context.presence_sessions.lease_count(), 1);
    assert_eq!(second.context.presence_sessions.lease_count(), 1);

    // One tab leaving changes nothing for the stream.
    second.apply_presence(IDENTIFIER, &presence_data("leave")).await.unwrap();
    assert_eq!(recorder.event_kinds(), vec![PresenceEvent::Join]);
    assert_eq!(second.context.presence_sessions.lease_count(), 0);

    // The last tab's socket closing is the leave.
    first.release_presence().await;
    assert_eq!(recorder.event_kinds(), vec![PresenceEvent::Join, PresenceEvent::Leave]);
    assert_eq!(recorder.events(PresenceEvent::Leave)[0].total, 0);
    assert_eq!(first.context.presence_sessions.lease_count(), 0);
}

#[tokio::test]
async fn a_replacement_grant_for_another_subject_leaves_the_old_subject_first() {
    let (hub, backend, _rx) = parts();
    let (store, recorder) = memory_store();
    let mut session = tab(1, &hub, &backend, Arc::clone(&store) as Arc<dyn PresenceBackend>).await;

    let granted = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    session.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();
    assert_eq!(recorder.event_kinds(), vec![PresenceEvent::Join]);

    // Rails now vouches for a different subject on the same subscription.
    apply_effects(&mut session, vec![grant_effect_for("chat:42", "user:8", &["join"])]).await;

    let events: Vec<(PresenceEvent, Option<String>)> = recorder
        .messages()
        .into_iter()
        .map(|(_, message)| (message.event, message.subject_id))
        .collect();
    assert_eq!(
        events,
        vec![
            (PresenceEvent::Join, Some("user:7".to_string())),
            (PresenceEvent::Leave, Some("user:7".to_string())),
            (PresenceEvent::Join, Some("user:8".to_string())),
        ],
        "the old subject leaves before the new one joins"
    );
    assert_eq!(session.grants.len(), 1);
    assert!(session.grants.is_joined(IDENTIFIER, "chat:42"), "the live lease carried over");

    let roster = store.snapshot("chat:42", PresencePage::default()).await.unwrap();
    assert_eq!(
        roster.members.iter().map(|m| m.subject_id.as_str()).collect::<Vec<_>>(),
        ["user:8"]
    );

    // The refresh registry follows: one lease, the new subject's.
    let live = session.context.presence_sessions.live_leases(Duration::from_mins(1));
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].key.subject_id, "user:8");
}

#[tokio::test]
async fn a_same_subject_replacement_updates_metadata_without_a_transition() {
    let (hub, backend, _rx) = parts();
    let (store, recorder) = memory_store();
    let mut session = tab(1, &hub, &backend, Arc::clone(&store) as Arc<dyn PresenceBackend>).await;

    let granted = reply(&session, vec![grant_effect(&["join"])]);
    session.apply(&granted, Some(IDENTIFIER)).await.expect("applied");
    session.apply_presence(IDENTIFIER, &presence_data("join")).await.unwrap();

    let mut updated = grant_effect(&["join"]);
    if let Effect::PresenceGrant { ref mut metadata, ref mut ttl_ms, .. } = updated {
        metadata.insert("status".to_string(), Value::from("away"));
        *ttl_ms = 20_000;
    }
    apply_effects(&mut session, vec![updated]).await;

    assert_eq!(recorder.event_kinds(), vec![PresenceEvent::Join], "the subject never left");
    let roster = store.snapshot("chat:42", PresencePage::default()).await.unwrap();
    assert_eq!(roster.members[0].metadata["status"], "away", "the lease carries the new metadata");
    let live = session.context.presence_sessions.live_leases(Duration::from_mins(1));
    assert_eq!(live[0].ttl, Duration::from_secs(20), "and the new TTL");

    // A replacement of a grant the session never joined under joins nothing.
    stream_from(&mut session, "chat:43").await;
    apply_effects(&mut session, vec![grant_effect_for("chat:43", "user:7", &["join"])]).await;
    apply_effects(&mut session, vec![grant_effect_for("chat:43", "user:9", &["join"])]).await;
    assert_eq!(recorder.event_kinds(), vec![PresenceEvent::Join]);
    assert!(!session.grants.is_joined(IDENTIFIER, "chat:43"));
}
