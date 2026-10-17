use std::assert_matches;
use std::time::Duration;

use super::*;
use crate::presence::test_support::RecordingPublisher;
use crate::protocol::v1::presence::PresenceEvent;

const NAMESPACE: &str = "example_production";

fn store() -> (Arc<MemoryPresence>, Arc<RecordingPublisher>) {
    let publisher = Arc::new(RecordingPublisher::default());
    // A sweep period far beyond any test: sweeps here are explicit.
    let store = MemoryPresence::start(
        NAMESPACE,
        Arc::clone(&publisher) as Arc<dyn PresencePublisher>,
        Duration::from_hours(1),
    );
    (store, publisher)
}

fn lease(session: &str, subject: &str, ttl: Duration) -> Lease {
    Lease {
        key: MembershipKey::new(NAMESPACE, "chat:42", subject),
        session_id: session.to_string(),
        node_id: "node-a".to_string(),
        metadata_json: r#"{"name":"Marissa"}"#.to_string(),
        ttl,
    }
}

#[tokio::test]
async fn two_sessions_of_one_subject_produce_one_join_and_one_last_leave() {
    let (store, recorder) = store();
    let ttl = Duration::from_mins(1);
    let key = MembershipKey::new(NAMESPACE, "chat:42", "user:7");

    let first = store.join(&lease("session-1", "user:7", ttl)).await.unwrap();
    assert_eq!(first, TransitionOutcome { transition: Transition::FirstJoin, total: 1 });
    let joined = recorder.events(PresenceEvent::Join);
    assert_eq!(joined.len(), 1);
    assert_eq!(joined[0].metadata.as_ref().unwrap()["name"], "Marissa");
    assert_eq!(recorder.messages()[0].0, presence_stream_key("chat:42"));

    let second = store.join(&lease("session-2", "user:7", ttl)).await.unwrap();
    assert_eq!(second, TransitionOutcome { transition: Transition::Refreshed, total: 1 });
    assert_eq!(recorder.messages().len(), 1, "a second tab announces nothing");

    let reduced = store.leave(&key, "session-2").await.unwrap();
    assert_eq!(reduced.transition, Transition::Reduced);
    let last = store.leave(&key, "session-1").await.unwrap();
    assert_eq!(last, TransitionOutcome { transition: Transition::LastLeave, total: 0 });
    assert_eq!(recorder.event_kinds(), vec![PresenceEvent::Join, PresenceEvent::Leave]);

    let again = store.leave(&key, "session-1").await.unwrap();
    assert_eq!(again.transition, Transition::Unchanged);
    assert_eq!(recorder.messages().len(), 2, "a repeated leave announces nothing");
}

#[tokio::test]
async fn an_expired_lease_is_invisible_then_swept_and_announced() {
    let (store, recorder) = store();
    store.join(&lease("session-dead", "user:7", Duration::from_millis(20))).await.unwrap();
    store.join(&lease("session-live", "user:8", Duration::from_mins(1))).await.unwrap();

    tokio::time::sleep(Duration::from_millis(60)).await;

    let roster = store.snapshot("chat:42", PresencePage::default()).await.unwrap();
    assert_eq!(roster.total, 1, "an expired lease is nobody's presence");
    assert_eq!(roster.members[0].subject_id, "user:8");

    store.sweep_once().await;
    let left = recorder.events(PresenceEvent::Leave);
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].subject_id.as_deref(), Some("user:7"));
    assert_eq!(left[0].total, 1);
    assert_eq!(store.metrics().swept, 1);

    store.sweep_once().await;
    assert_eq!(recorder.events(PresenceEvent::Leave).len(), 1, "nothing left to sweep");
}

#[tokio::test]
async fn refresh_extends_only_the_named_leases_by_their_own_ttl() {
    let (store, _recorder) = store();
    let short = Duration::from_millis(40);
    store.join(&lease("session-live", "user:7", short)).await.unwrap();
    store.join(&lease("session-dead", "user:7", short)).await.unwrap();

    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let held = lease("session-live", "user:7", short).held();
        assert_eq!(store.refresh(&[held]).await.unwrap(), 1);
    }

    let roster = store.snapshot("chat:42", PresencePage::default()).await.unwrap();
    assert_eq!(roster.total, 1, "the live tab kept the subject present");
    let table = lock(&store.table);
    let sessions = &table["chat:42"]["user:7"];
    let (live_until, dead_until) =
        (sessions["session-live"].expires_at, sessions["session-dead"].expires_at);
    drop(table);
    assert!(live_until > Instant::now());
    assert!(dead_until <= Instant::now(), "not kept alive by the other tab");
}

#[tokio::test]
async fn snapshots_are_paged_and_stably_ordered() {
    let (store, _recorder) = store();
    for index in 0..5 {
        store
            .join(&Lease {
                key: MembershipKey::new(NAMESPACE, "chat:42", format!("user:{index:02}")),
                session_id: format!("session-{index}"),
                node_id: "node-a".to_string(),
                metadata_json: format!(r#"{{"seat":{index}}}"#),
                ttl: Duration::from_mins(1),
            })
            .await
            .unwrap();
    }

    let first = store.snapshot("chat:42", PresencePage { limit: 2, offset: 0 }).await.unwrap();
    assert_eq!(first.total, 5);
    assert!(first.has_more);
    assert_eq!(
        first.members.iter().map(|m| m.subject_id.as_str()).collect::<Vec<_>>(),
        ["user:00", "user:01"]
    );
    assert_eq!(first.members[1].metadata["seat"], 1);

    let last = store.snapshot("chat:42", PresencePage { limit: 2, offset: 4 }).await.unwrap();
    assert_eq!(last.members.len(), 1);
    assert!(!last.has_more);

    let empty = store.snapshot("chat:none", PresencePage::default()).await.unwrap();
    assert_eq!(empty, PresenceSnapshot::default());
}

#[tokio::test]
async fn a_lease_from_another_namespace_is_refused() {
    let (store, _recorder) = store();
    let mut foreign = lease("session-1", "user:7", Duration::from_mins(1));
    foreign.key.namespace = "other".to_string();
    assert_matches!(store.join(&foreign).await, Err(PresenceError::Backend(_)));
}
