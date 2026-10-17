use super::*;
use crate::pubsub::postgres::test_support::{
    integration_client, integration_dsn, integration_namespace, spawn_listener,
};

const NAMESPACE: &str = "example_production";

fn config(namespace: &str) -> PresenceConfig {
    PresenceConfig {
        database_url: "postgres://localhost/orbitcast".to_string(),
        namespace: namespace.to_string(),
        node_id: "node-test".to_string(),
        ttl: Duration::from_millis(crate::presence::DEFAULT_PRESENCE_TTL_MS),
        refresh_interval: Duration::from_millis(
            crate::presence::DEFAULT_PRESENCE_REFRESH_INTERVAL_MS,
        ),
    }
}

#[test]
fn config_accepts_the_documented_defaults() {
    let config = config(NAMESPACE);
    config.validate().unwrap();
    assert_eq!(ttl_millis(config.ttl), 45_000);
}

#[test]
fn config_rejects_unusable_values() {
    let mut empty_namespace = config("");
    assert!(empty_namespace.validate().is_err());
    empty_namespace.namespace = NAMESPACE.to_string();
    empty_namespace.validate().unwrap();

    let mut no_url = config(NAMESPACE);
    no_url.database_url = "  ".to_string();
    assert!(no_url.validate().is_err());

    let mut no_node = config(NAMESPACE);
    no_node.node_id = String::new();
    assert!(no_node.validate().is_err());

    let mut zero_ttl = config(NAMESPACE);
    zero_ttl.ttl = Duration::ZERO;
    assert!(zero_ttl.validate().is_err());

    // A refresh period at or above the TTL cannot keep a lease alive.
    let mut slow_refresh = config(NAMESPACE);
    slow_refresh.refresh_interval = slow_refresh.ttl;
    assert!(slow_refresh.validate().is_err());

    let mut zero_refresh = config(NAMESPACE);
    zero_refresh.refresh_interval = Duration::ZERO;
    assert!(zero_refresh.validate().is_err());
}

#[test]
fn stored_metadata_is_decoded_tolerantly() {
    assert_eq!(decode_metadata(r#"{"name":"Marissa"}"#)["name"], "Marissa");
    assert!(decode_metadata("not json").is_empty(), "an unreadable row is not a failed roster");
    assert!(decode_metadata("[1,2]").is_empty(), "a non-object row is not a roster entry");
}

#[test]
fn the_missing_table_hint_names_the_migration() {
    let hint = missing_table_hint();
    assert!(hint.contains(PRESENCE_TABLE));
    assert!(hint.contains("002_create_orbitcast_presence.rb"));
}

// ---- Integration tests: require a live PostgreSQL ----------------------
//
// Run with: DATABASE_URL=postgres://... cargo test -p orbitcast -- --ignored
//
// Every test works in its own `(namespace)` derived from its label and this
// process's pid, and every statement this module issues is namespace-scoped
// - the sweeper included - so the whole file is safe to run in parallel
// against one shared database.

use crate::presence::test_support::RecordingPublisher;
use crate::protocol::Envelope;
use crate::protocol::v1::presence::{
    PresenceEvent as MessageEvent, PresenceMessage as WireMessage,
};
use crate::pubsub::PubSub as _;

/// Create the presence table exactly as the gem migration does.
async fn ensure_presence_table() {
    let client = integration_client().await;

    // Parallel `CREATE TABLE IF NOT EXISTS` can still collide in pg_type,
    // so the postcondition - not this statement's result - is the check.
    let attempted = client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS orbitcast_presence (
                     namespace text NOT NULL,
                     stream text NOT NULL,
                     subject_id text NOT NULL,
                     session_id text NOT NULL,
                     node_id text NOT NULL,
                     metadata text NOT NULL,
                     expires_at timestamptz NOT NULL,
                     PRIMARY KEY (namespace, stream, subject_id, session_id));
                 CREATE INDEX IF NOT EXISTS index_orbitcast_presence_on_expires_at
                     ON orbitcast_presence (expires_at);
                 CREATE INDEX IF NOT EXISTS index_orbitcast_presence_on_namespace_and_stream
                     ON orbitcast_presence (namespace, stream);",
        )
        .await;

    let present: Option<String> = client
        .query_one("SELECT to_regclass('orbitcast_presence')::text", &[])
        .await
        .expect("query to_regclass")
        .get(0);
    assert!(present.is_some(), "presence table must exist for these tests: {attempted:?}");
}

/// Remove every lease of one test namespace.
///
/// Also the point where the table is created, so a test that starts by
/// clearing its namespace works against a fresh database.
async fn clear_namespace(namespace: &str) {
    ensure_presence_table().await;
    integration_client()
        .await
        .execute("DELETE FROM orbitcast_presence WHERE namespace = $1", &[&namespace])
        .await
        .expect("namespace cleanup");
}

/// Live leases of one namespace, whatever their expiry.
async fn row_count(namespace: &str) -> i64 {
    integration_client()
        .await
        .query_one("SELECT count(*) FROM orbitcast_presence WHERE namespace = $1", &[&namespace])
        .await
        .expect("row count")
        .get(0)
}

/// A publisher that keeps what it was asked to announce.
/// A store in `namespace`, on `node_id`, with its own recorder.
async fn store(
    namespace: &str,
    node_id: &str,
    ttl: Duration,
) -> (Arc<PostgresPresence>, Arc<RecordingPublisher>) {
    ensure_presence_table().await;
    let publisher = Arc::new(RecordingPublisher::default());
    let config = PresenceConfig {
        database_url: integration_dsn(),
        namespace: namespace.to_string(),
        node_id: node_id.to_string(),
        ttl,
        refresh_interval: ttl / 4,
    };
    let store = PostgresPresence::connect(config, Arc::clone(&publisher) as Arc<_>)
        .await
        .expect("connect to DATABASE_URL");
    (store, publisher)
}

fn lease(namespace: &str, session: &str, node: &str, ttl: Duration) -> Lease {
    Lease {
        key: MembershipKey::new(namespace, "chat:42", "user:7"),
        session_id: session.to_string(),
        node_id: node.to_string(),
        metadata_json: r#"{"name":"Marissa"}"#.to_string(),
        ttl,
    }
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn two_nodes_and_two_sessions_produce_one_join_and_one_last_leave() {
    let namespace = integration_namespace("presence_two_nodes");
    let ttl = Duration::from_mins(1);
    clear_namespace(&namespace).await;

    let (node_a, recorder_a) = store(&namespace, "node-a", ttl).await;
    let (node_b, recorder_b) = store(&namespace, "node-b", ttl).await;
    let key = MembershipKey::new(&namespace, "chat:42", "user:7");

    // First tab, on node A: one subject-level join.
    let first = node_a.join(&lease(&namespace, "session-1", "node-a", ttl)).await.unwrap();
    assert_eq!(first.transition, Transition::FirstJoin);
    assert_eq!(first.total, 1);

    let announced = recorder_a.events(MessageEvent::Join);
    assert_eq!(announced.len(), 1, "the first live lease announces exactly one join");
    assert_eq!(announced[0].subject_id.as_deref(), Some("user:7"));
    assert_eq!(announced[0].metadata.as_ref().unwrap()["name"], "Marissa");
    assert_eq!(
        recorder_a.messages()[0].0,
        presence_stream_key("chat:42"),
        "transitions travel on the derived presence key"
    );

    // Second tab of the same subject, on node B: no second join.
    let second = node_b.join(&lease(&namespace, "session-2", "node-b", ttl)).await.unwrap();
    assert_eq!(second.transition, Transition::Refreshed);
    assert_eq!(second.total, 1, "two sessions are still one subject");
    assert!(recorder_b.messages().is_empty(), "a second tab announces nothing");

    // Either tab leaving while the other lives announces nothing.
    let reduced = node_b.leave(&key, "session-2").await.unwrap();
    assert_eq!(reduced.transition, Transition::Reduced);
    assert_eq!(reduced.total, 1);
    assert_eq!(recorder_b.messages(), []);

    // The last one to go announces the leave.
    let last = node_a.leave(&key, "session-1").await.unwrap();
    assert_eq!(last.transition, Transition::LastLeave);
    assert_eq!(last.total, 0);
    let left = recorder_a.events(MessageEvent::Leave);
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].subject_id.as_deref(), Some("user:7"));
    assert_eq!(left[0].total, 0);

    // A repeated leave is a no-op, not a second announcement.
    let again = node_a.leave(&key, "session-1").await.unwrap();
    assert_eq!(again.transition, Transition::Unchanged);
    assert_eq!(recorder_a.events(MessageEvent::Leave).len(), 1);

    assert_eq!(row_count(&namespace).await, 0);
    clear_namespace(&namespace).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn one_tabs_refresh_never_preserves_another_tabs_lease() {
    let namespace = integration_namespace("presence_refresh_scope");
    let ttl = Duration::from_millis(600);
    clear_namespace(&namespace).await;

    let (node, _recorder) = store(&namespace, "node-a", Duration::from_mins(1)).await;

    // Two tabs of one subject, both short-lived leases.
    node.join(&lease(&namespace, "session-live", "node-a", ttl)).await.unwrap();
    node.join(&lease(&namespace, "session-dead", "node-a", ttl)).await.unwrap();
    assert_eq!(row_count(&namespace).await, 2);

    // Only the live tab is refreshed, twice, past the original expiry.
    for _ in 0..2 {
        tokio::time::sleep(Duration::from_millis(350)).await;
        let held = lease(&namespace, "session-live", "node-a", ttl).held();
        let refreshed = node.refresh(&[held]).await.unwrap();
        assert_eq!(refreshed, 1, "exactly one lease is extended");
    }

    // The dead tab's lease is gone from every live query; the live one is not.
    let snapshot = node.snapshot("chat:42", PresencePage { limit: 50, offset: 0 }).await.unwrap();
    assert_eq!(snapshot.total, 1, "the subject survives on its live tab");

    let client = integration_client().await;
    let live: i64 = client
        .query_one(
            "SELECT count(*) FROM orbitcast_presence \
                 WHERE namespace = $1 AND expires_at > now()",
            &[&namespace],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(live, 1, "the dead tab was not kept alive by the live tab's refresh");

    clear_namespace(&namespace).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_hard_killed_node_disappears_within_the_expiry_bound() {
    let namespace = integration_namespace("presence_expiry");
    clear_namespace(&namespace).await;

    // A node that dies refreshes nothing, so its lease simply runs out.
    let (killed, _recorder) = store(&namespace, "node-killed", Duration::from_mins(1)).await;
    killed
        .join(&lease(&namespace, "session-killed", "node-killed", Duration::from_millis(200)))
        .await
        .unwrap();
    assert_eq!(row_count(&namespace).await, 1);

    tokio::time::sleep(Duration::from_millis(400)).await;

    // Expired leases are invisible to every live query, before any sweep.
    let snapshot = killed.snapshot("chat:42", PresencePage { limit: 50, offset: 0 }).await.unwrap();
    assert_eq!(snapshot.total, 0, "an expired lease is nobody's presence");
    assert_eq!(snapshot.members, []);

    // The sweeper then removes the row and announces the departure.
    let session = PgPresenceSession::connect(integration_dsn()).await.unwrap();
    let config = PresenceConfig {
        database_url: integration_dsn(),
        namespace: namespace.clone(),
        node_id: "node-sweeper".to_string(),
        ttl: Duration::from_mins(1),
        refresh_interval: Duration::from_secs(10),
    };
    let publisher = RecordingPublisher::default();
    let stats = PresenceStats::default();
    sweep_once(&session, &config, &publisher, &stats).await;

    assert_eq!(row_count(&namespace).await, 0, "the expired lease is reclaimed");
    let left = publisher.events(MessageEvent::Leave);
    assert_eq!(left.len(), 1, "a subject that expired away is announced as gone");
    assert_eq!(left[0].subject_id.as_deref(), Some("user:7"));
    assert_eq!(left[0].total, 0);

    // A second pass has nothing to do and announces nothing.
    sweep_once(&session, &config, &publisher, &stats).await;
    assert_eq!(publisher.events(MessageEvent::Leave).len(), 1);

    clear_namespace(&namespace).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_sweep_keeps_a_subject_that_still_has_a_live_tab() {
    let namespace = integration_namespace("presence_sweep_partial");
    clear_namespace(&namespace).await;

    let (node, _recorder) = store(&namespace, "node-a", Duration::from_mins(1)).await;
    node.join(&lease(&namespace, "session-live", "node-a", Duration::from_mins(1))).await.unwrap();
    node.join(&lease(&namespace, "session-gone", "node-b", Duration::from_millis(150)))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(350)).await;

    let session = PgPresenceSession::connect(integration_dsn()).await.unwrap();
    let config = PresenceConfig {
        database_url: integration_dsn(),
        namespace: namespace.clone(),
        node_id: "node-sweeper".to_string(),
        ttl: Duration::from_mins(1),
        refresh_interval: Duration::from_secs(10),
    };
    let publisher = RecordingPublisher::default();
    sweep_once(&session, &config, &publisher, &PresenceStats::default()).await;

    assert_eq!(row_count(&namespace).await, 1, "only the expired lease was removed");
    assert!(
        publisher.messages().is_empty(),
        "a subject with another live tab is not announced as gone"
    );

    clear_namespace(&namespace).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn snapshots_are_paged_bounded_and_stably_ordered() {
    let namespace = integration_namespace("presence_pagination");
    let ttl = Duration::from_mins(1);
    clear_namespace(&namespace).await;

    let (node, _recorder) = store(&namespace, "node-a", ttl).await;

    for index in 0..5 {
        let subject = format!("user:{index:02}");
        node.join(&Lease {
            key: MembershipKey::new(&namespace, "chat:42", &subject),
            session_id: format!("session-{index}"),
            node_id: "node-a".to_string(),
            metadata_json: format!(r#"{{"seat":{index}}}"#),
            ttl,
        })
        .await
        .unwrap();
    }

    let first = node.snapshot("chat:42", PresencePage { limit: 2, offset: 0 }).await.unwrap();
    assert_eq!(first.total, 5);
    assert!(first.has_more);
    assert_eq!(
        first.members.iter().map(|m| m.subject_id.as_str()).collect::<Vec<_>>(),
        vec!["user:00", "user:01"],
        "pages are ordered by subject id, not by physical row order"
    );
    assert_eq!(first.members[1].metadata["seat"], 1, "granted metadata comes back intact");

    let middle = node.snapshot("chat:42", PresencePage { limit: 2, offset: 2 }).await.unwrap();
    assert_eq!(
        middle.members.iter().map(|m| m.subject_id.as_str()).collect::<Vec<_>>(),
        vec!["user:02", "user:03"]
    );
    assert!(middle.has_more);

    let last = node.snapshot("chat:42", PresencePage { limit: 2, offset: 4 }).await.unwrap();
    assert_eq!(last.members.len(), 1);
    assert!(!last.has_more, "the final page says so");

    let past_the_end =
        node.snapshot("chat:42", PresencePage { limit: 2, offset: 10 }).await.unwrap();
    assert_eq!(past_the_end.members, []);
    assert_eq!(past_the_end.total, 5, "a page past the end still reports the roster size");

    // A page size above the ceiling is clamped, never honoured.
    let clamped =
        node.snapshot("chat:42", PresencePage { limit: 10_000, offset: 0 }).await.unwrap();
    assert_eq!(clamped.members.len(), 5);

    // The second identical read is served from the local cache; a join
    // invalidates it again.
    let before = node.stats().cache_hits;
    node.snapshot("chat:42", PresencePage { limit: 2, offset: 0 }).await.unwrap();
    assert_eq!(node.stats().cache_hits, before + 1);

    node.join(&lease(&namespace, "session-new", "node-a", ttl)).await.unwrap();
    let after_join = node.stats().cache_hits;
    let refreshed = node.snapshot("chat:42", PresencePage { limit: 2, offset: 0 }).await.unwrap();
    assert_eq!(node.stats().cache_hits, after_join, "a transition drops the cached page");
    assert_eq!(refreshed.total, 6);

    clear_namespace(&namespace).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires DATABASE_URL"]
async fn racing_nodes_announce_exactly_one_join_and_one_leave() {
    const RACERS: i64 = 4;

    let namespace = integration_namespace("presence_race");
    let ttl = Duration::from_mins(1);
    clear_namespace(&namespace).await;

    // Separate stores mean separate PostgreSQL connections: the race is
    // real, and only the advisory lock orders it. A single store would
    // serialize on its own session mutex and prove nothing.
    let mut stores = Vec::new();
    for index in 0..RACERS {
        stores.push(store(&namespace, &format!("node-{index}"), ttl).await);
    }

    let mut joining = Vec::new();
    for (index, (node, _)) in stores.iter().enumerate() {
        let node = Arc::clone(node);
        let namespace = namespace.clone();
        joining.push(tokio::spawn(async move {
            node.join(&lease(&namespace, &format!("session-{index}"), "node-x", ttl)).await
        }));
    }

    let mut first_joins = 0;
    for task in joining {
        let outcome = task.await.expect("no racer panicked").expect("join committed");
        if outcome.transition == Transition::FirstJoin {
            first_joins += 1;
        }
    }
    assert_eq!(first_joins, 1, "exactly one racer observes the first join");

    let announced: usize =
        stores.iter().map(|(_, recorder)| recorder.events(MessageEvent::Join).len()).sum();
    assert_eq!(announced, 1, "and exactly one join is announced to the stream");
    assert_eq!(row_count(&namespace).await, RACERS, "every session still has a lease");

    // The same race in the other direction.
    let key = MembershipKey::new(&namespace, "chat:42", "user:7");
    let mut leaving = Vec::new();
    for (index, (node, _)) in stores.iter().enumerate() {
        let node = Arc::clone(node);
        let key = key.clone();
        leaving
            .push(tokio::spawn(async move { node.leave(&key, &format!("session-{index}")).await }));
    }

    let mut last_leaves = 0;
    for task in leaving {
        let outcome = task.await.expect("no racer panicked").expect("leave committed");
        if outcome.transition == Transition::LastLeave {
            last_leaves += 1;
        }
    }
    assert_eq!(last_leaves, 1, "exactly one racer observes the last leave");

    let announced: usize =
        stores.iter().map(|(_, recorder)| recorder.events(MessageEvent::Leave).len()).sum();
    assert_eq!(announced, 1, "and exactly one leave is announced");
    assert_eq!(row_count(&namespace).await, 0);

    clear_namespace(&namespace).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn transitions_reach_other_nodes_over_the_derived_presence_channel() {
    use crate::pubsub::{PostgresConfig, PostgresPubSub};

    let namespace = integration_namespace("presence_notify");
    let ttl = Duration::from_mins(1);
    clear_namespace(&namespace).await;
    ensure_presence_table().await;

    // A real pub/sub backend as the publisher, and a listener standing in
    // for another node: this is the whole notification path, channel
    // derivation included.
    let backend = Arc::new(
        PostgresPubSub::connect(PostgresConfig::new(integration_dsn(), namespace.clone()))
            .await
            .expect("connect to DATABASE_URL"),
    );

    let (mut rx, pump) = spawn_listener(Arc::clone(&backend));

    let roster_key = presence_stream_key("chat:42");
    backend.subscribe(&roster_key).await.expect("LISTEN acknowledged");

    let config = PresenceConfig {
        database_url: integration_dsn(),
        namespace: namespace.clone(),
        node_id: "node-a".to_string(),
        ttl,
        refresh_interval: ttl / 4,
    };
    let store = PostgresPresence::connect(config, Arc::clone(&backend) as Arc<_>)
        .await
        .expect("presence store");

    store.join(&lease(&namespace, "session-1", "node-a", ttl)).await.unwrap();

    let (stream, payload) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the transition is delivered within 5s")
        .expect("delivery channel open");

    assert_eq!(stream, roster_key, "roster changes arrive on the derived presence key");
    let message: WireMessage =
        serde_json::from_slice(&payload).expect("the payload is a v1 presence message");
    message.validate().unwrap();
    assert_eq!(message.event, MessageEvent::Join);
    assert_eq!(message.stream, "chat:42", "the message names the application stream");
    assert_eq!(message.subject_id.as_deref(), Some("user:7"));

    pump.abort();
    clear_namespace(&namespace).await;
}

/// Milliseconds left on `session`'s lease, by the database clock.
async fn remaining_ms(namespace: &str, session: &str) -> f64 {
    integration_client()
        .await
        .query_one(
            "SELECT (extract(epoch FROM (expires_at - now())) * 1000)::float8 \
                 FROM orbitcast_presence WHERE namespace = $1 AND session_id = $2",
            &[&namespace, &session],
        )
        .await
        .expect("lease row")
        .get(0)
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_refresh_keeps_each_leases_own_ttl() {
    let namespace = integration_namespace("presence_refresh_ttl");
    clear_namespace(&namespace).await;

    // The node's bound is a minute; the grant asked for 4 s. Generous
    // windows: the assertions open their own connection, which can take a
    // few hundred milliseconds against a slow database.
    let (node, _recorder) = store(&namespace, "node-a", Duration::from_mins(1)).await;
    let granted = Duration::from_secs(4);
    node.join(&lease(&namespace, "session-1", "node-a", granted)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let refreshed = node.refresh(&[lease(&namespace, "session-1", "node-a", granted).held()]).await;
    assert_eq!(refreshed.unwrap(), 1);

    let left = remaining_ms(&namespace, "session-1").await;
    assert!(
        left > 3_000.0,
        "the refresh extended the lease past its original expiry: {left} ms left"
    );
    assert!(
        left <= 4_000.0,
        "the refresh honoured the grant's TTL, not the node's: {left} ms left"
    );

    // A lease of a session this node does not hold is never extended.
    let other = lease(&namespace, "session-elsewhere", "node-a", granted).held();
    assert_eq!(node.refresh(&[other]).await.unwrap(), 0);

    clear_namespace(&namespace).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires DATABASE_URL"]
async fn an_expiry_sweep_is_serialized_with_a_concurrent_join_under_the_membership_lock() {
    let namespace = integration_namespace("presence_sweep_order");
    clear_namespace(&namespace).await;

    let (node, recorder) = store(&namespace, "node-a", Duration::from_mins(1)).await;
    let key = MembershipKey::new(&namespace, "chat:42", "user:7");

    // A lease that has already run out: the next sweep wants to reclaim it,
    // and the client (same session) is about to re-join.
    node.join(&lease(&namespace, "session-1", "node-a", Duration::from_millis(100))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(recorder.event_kinds(), vec![MessageEvent::Join]);

    // Somebody else holds the membership lock: both the sweep and the join
    // must wait for it rather than proceed.
    let holder = integration_client().await;
    holder.batch_execute("BEGIN").await.unwrap();
    holder.execute("SELECT pg_advisory_xact_lock($1::bigint)", &[&key.lock_key()]).await.unwrap();

    let sweeper_session = PgPresenceSession::connect(integration_dsn()).await.unwrap();
    let sweeper_config = PresenceConfig {
        database_url: integration_dsn(),
        namespace: namespace.clone(),
        node_id: "node-sweeper".to_string(),
        ttl: Duration::from_mins(1),
        refresh_interval: Duration::from_secs(10),
    };
    let sweep_recorder = Arc::clone(&recorder);
    let stats = Arc::new(PresenceStats::default());
    let sweep_stats = Arc::clone(&stats);
    let sweep = tokio::spawn(async move {
        sweep_once(&sweeper_session, &sweeper_config, &*sweep_recorder, &sweep_stats).await;
    });

    let joiner = Arc::clone(&node);
    let join_namespace = namespace.clone();
    let join = tokio::spawn(async move {
        joiner
            .join(&lease(&join_namespace, "session-1", "node-a", Duration::from_mins(1)))
            .await
            .expect("join committed")
    });

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!sweep.is_finished(), "the sweep waits for the membership lock");
    assert!(!join.is_finished(), "so does the join");
    assert_eq!(row_count(&namespace).await, 1, "nothing was removed behind the lock");
    assert_eq!(recorder.messages().len(), 1, "and nothing was announced");

    holder.batch_execute("COMMIT").await.unwrap();
    sweep.await.expect("sweep task");
    let outcome = join.await.expect("join task");

    // Whichever side the lock let through first, the decision was made on a
    // consistent state: the fresh lease is live, exactly one subject-level
    // join happened for it, and a leave was announced only if the sweep
    // reclaimed the stale lease first (in which case it removed that row and
    // nothing else).
    assert_eq!(outcome.transition, Transition::FirstJoin);
    assert!(remaining_ms(&namespace, "session-1").await > 0.0, "the lease is live");
    let leaves = recorder.events(MessageEvent::Leave).len();
    assert!(leaves <= 1, "at most one leave: {:?}", recorder.event_kinds());
    assert_eq!(stats.swept.load(Ordering::Relaxed), u64::from(leaves == 1));
    assert_eq!(recorder.events(MessageEvent::Join).len(), 2);

    // The fresh lease is not a sweep candidate: a further pass is a no-op.
    let before = recorder.messages().len();
    sweep_once(
        &PgPresenceSession::connect(integration_dsn()).await.unwrap(),
        &PresenceConfig {
            database_url: integration_dsn(),
            namespace: namespace.clone(),
            node_id: "node-sweeper".to_string(),
            ttl: Duration::from_mins(1),
            refresh_interval: Duration::from_secs(10),
        },
        &*recorder,
        &PresenceStats::default(),
    )
    .await;
    assert_eq!(recorder.messages().len(), before);
    assert_eq!(row_count(&namespace).await, 1);

    clear_namespace(&namespace).await;
}
