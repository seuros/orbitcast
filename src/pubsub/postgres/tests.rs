use std::assert_matches;

use super::test_support::{
    integration_client, integration_dsn, integration_namespace, spawn_event_listener,
    spawn_listener,
};
use super::*;
use crate::protocol::v1::publication::MAX_NOTIFY_PAYLOAD_LEN;
use crate::pubsub::DEFAULT_OVERFLOW_TTL;
use crate::pubsub::PubSubEvent;

const NAMESPACE: &str = "example_production";

async fn integration_backend(label: &str) -> PostgresPubSub {
    PostgresPubSub::connect(PostgresConfig::new(integration_dsn(), integration_namespace(label)))
        .await
        .expect("connect to DATABASE_URL")
}

/// A backend with the overflow relay enabled, in this test's namespace.
async fn integration_overflow_backend(label: &str, ttl: Duration) -> PostgresPubSub {
    ensure_overflow_table().await;

    let mut config = PostgresConfig::new(integration_dsn(), integration_namespace(label));
    config.overflow = OverflowConfig { enabled: true, ttl, ..OverflowConfig::default() };

    PostgresPubSub::connect(config).await.expect("connect to DATABASE_URL")
}

/// Create the relay table exactly as the gem migration does.
///
/// Shared by every integration test in this process; rows stay isolated
/// because each test publishes in its own namespace.
async fn ensure_overflow_table() {
    let client = integration_client().await;

    // Parallel `CREATE TABLE IF NOT EXISTS` can still collide in pg_type,
    // so the postcondition - not this statement's result - is the check.
    let attempted = client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS orbitcast_payloads (
                     id uuid PRIMARY KEY,
                     namespace text NOT NULL,
                     stream text NOT NULL,
                     json_payload text NOT NULL,
                     expires_at timestamptz NOT NULL);
                 CREATE INDEX IF NOT EXISTS index_orbitcast_payloads_on_expires_at
                     ON orbitcast_payloads (expires_at);",
        )
        .await;

    let present: Option<String> = client
        .query_one("SELECT to_regclass('orbitcast_payloads')::text", &[])
        .await
        .expect("query to_regclass")
        .get(0);
    assert!(present.is_some(), "relay table must exist for overflow tests: {attempted:?}");
}

/// A payload that cannot travel inline but stays well under 1 MiB.
fn relay_sized_payload(marker: &str) -> String {
    format!(r#"{{"marker":"{marker}","html":"{}"}}"#, "y".repeat(16 * 1024))
}

#[test]
fn config_rejects_empty_namespace() {
    let config = PostgresConfig::new("postgres://localhost/orbitcast", "");
    let err = config.validate().unwrap_err();
    assert_matches!(err, PubSubError::Config(_));
    assert!(err.to_string().contains("namespace"), "{err}");
}

#[test]
fn config_rejects_empty_url_and_zero_bounds() {
    assert!(PostgresConfig::new("  ", NAMESPACE).validate().is_err());

    let mut config = PostgresConfig::new("postgres://localhost/orbitcast", NAMESPACE);
    config.queue_capacity = 0;
    assert!(config.validate().is_err());

    let mut config = PostgresConfig::new("postgres://localhost/orbitcast", NAMESPACE);
    config.ack_timeout = Duration::ZERO;
    assert!(config.validate().is_err());

    let mut config = PostgresConfig::new("postgres://localhost/orbitcast", NAMESPACE);
    config.keepalive_interval = Duration::ZERO;
    assert!(config.validate().is_err());
}

#[test]
fn config_accepts_defaults() {
    let config = PostgresConfig::new("postgres://localhost/orbitcast", NAMESPACE);
    config.validate().unwrap();
    assert_eq!(config.ack_timeout, DEFAULT_ACK_TIMEOUT);
    assert_eq!(config.queue_capacity, DEFAULT_QUEUE_CAPACITY);
    assert_eq!(config.keepalive_interval, DEFAULT_KEEPALIVE_INTERVAL);
}

#[test]
fn channels_are_quoted_identifiers() {
    let channel = channel_for(NAMESPACE, "chat:42");
    assert_eq!(quote_channel(&channel), format!("\"{channel}\""));
    assert_eq!(format!("LISTEN {}", quote_channel(&channel)), format!("LISTEN \"{channel}\""));
}

#[tokio::test]
async fn connect_rejects_invalid_config_before_touching_the_network() {
    let result = PostgresPubSub::connect(PostgresConfig::new("postgres://h/db", "")).await;
    let Err(err) = result else {
        panic!("an empty namespace must not yield a usable backend");
    };
    assert_matches!(err, PubSubError::Config(_));
}

// ---- Integration tests: require a live PostgreSQL ----------------------
//
// Run with: DATABASE_URL=postgres://... cargo test -p orbitcast -- --ignored

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn subscribe_is_acknowledged_before_returning() {
    let backend = integration_backend("subscribe_ack").await;
    assert!(backend.is_ready());

    backend.subscribe("chat:42").await.expect("LISTEN acknowledged");
    // A second subscribe is idempotent and still acknowledged.
    backend.subscribe("chat:42").await.expect("repeat LISTEN acknowledged");
    backend.unsubscribe("chat:42").await.expect("UNLISTEN acknowledged");
    // Unsubscribing an unknown stream is a no-op, not an error.
    backend.unsubscribe("chat:42").await.expect("repeat UNLISTEN acknowledged");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn publish_after_subscribe_is_delivered_through_the_listener() {
    let backend = Arc::new(integration_backend("publish_delivery").await);
    let (mut rx, handle) = spawn_listener(backend.clone());

    backend.subscribe("chat:42").await.unwrap();
    backend.publish("chat:42", r#"{"body":"hi"}"#).await.unwrap();

    let (stream, payload) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("publication delivered within 5s")
        .expect("delivery channel open");
    assert_eq!(stream, "chat:42");
    assert_eq!(String::from_utf8(payload).unwrap(), r#"{"body":"hi"}"#);

    handle.abort();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_second_listener_fails_loudly_instead_of_going_silent() {
    let backend = Arc::new(integration_backend("second_listener").await);
    let (_first_rx, first) = spawn_listener(backend.clone());
    let (mut rx, second) = spawn_listener(backend.clone());

    // The current-thread runtime polls `first` before `second`, so the
    // second `listen()` is the one refused with `ListenerAlreadyStarted`.
    let err = tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("a refused listener ends at once")
        .expect_err("a refused listener must panic, not end quietly");
    assert!(err.is_panic(), "{err:?}");
    assert!(rx.recv().await.is_none(), "its delivery channel is closed");

    first.abort();
}

/// Regression: 0.2.0 awaited `LISTEN` and the keepalive `SELECT 1` on the
/// task that alone polled the connection, so the first query never left the
/// socket and the listener hung for good - initially, or after one idle
/// keepalive period. Here the keepalive fires several times on an idle
/// listener, and a subscription made afterwards must still be acknowledged and
/// delivered, with no reconnect masking a stall.
#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn subscribe_after_idle_keepalives_is_acknowledged_and_delivered() {
    const KEEPALIVE: Duration = Duration::from_millis(100);

    let namespace = integration_namespace("idle_keepalive");
    // Tag this backend's sessions so pg_stat_activity can single them out
    // from parallel tests sharing the database.
    let dsn = integration_dsn();
    let separator = if dsn.contains('?') { '&' } else { '?' };
    let mut config =
        PostgresConfig::new(format!("{dsn}{separator}application_name={namespace}"), &namespace);
    config.keepalive_interval = KEEPALIVE;
    let backend = Arc::new(PostgresPubSub::connect(config).await.expect("connect to DATABASE_URL"));
    let (mut rx, handle) = spawn_listener(backend.clone());

    // Only the listener's keepalive issues `SELECT 1` (the publisher has run
    // nothing yet), so seeing it as a tagged session's last statement proves
    // a probe completed - however early the first one fired.
    let observer = integration_client().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let probed: i64 = observer
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity \
                         WHERE application_name = $1 AND query = 'SELECT 1'",
                    &[&namespace],
                )
                .await
                .expect("query pg_stat_activity")
                .get(0);
            if probed > 0 {
                break;
            }
            tokio::time::sleep(KEEPALIVE).await;
        }
    })
    .await
    .expect("keepalive ran on the listener session within 5s");

    // Stay idle across several more periods: every probe must complete on
    // the live session.
    tokio::time::sleep(KEEPALIVE * 5).await;
    assert!(backend.is_ready());
    assert_eq!(backend.stats().reconnects, 0, "keepalive must not stall into a reconnect");

    tokio::time::timeout(Duration::from_secs(5), backend.subscribe("chat:42"))
        .await
        .expect("LISTEN after idle acknowledged within 5s")
        .expect("LISTEN after idle succeeds");
    backend.publish("chat:42", r#"{"body":"after idle"}"#).await.unwrap();

    let (stream, payload) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("publication delivered after idle within 5s")
        .expect("delivery channel open");
    assert_eq!(stream, "chat:42");
    assert_eq!(String::from_utf8(payload).unwrap(), r#"{"body":"after idle"}"#);

    handle.abort();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn unsubscribed_streams_are_not_delivered() {
    let backend = Arc::new(integration_backend("unsubscribed").await);
    let (mut rx, handle) = spawn_listener(backend.clone());

    backend.subscribe("chat:42").await.unwrap();
    backend.unsubscribe("chat:42").await.unwrap();
    backend.publish("chat:42", r#"{"body":"hi"}"#).await.unwrap();

    let idle = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;
    assert!(idle.is_err(), "unsubscribed stream must not be delivered");

    handle.abort();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn oversized_publication_is_rejected_before_sql() {
    let backend = integration_backend("oversized").await;
    let payload = format!(r#"{{"body":"{}"}}"#, "x".repeat(MAX_NOTIFY_PAYLOAD_LEN));
    let err = backend.publish("chat:42", &payload).await.unwrap_err();
    assert_matches!(err, PubSubError::PayloadTooLarge { .. });
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn foreign_notices_are_counted_not_delivered() {
    let backend = Arc::new(integration_backend("foreign_notices").await);
    let (mut rx, handle) = spawn_listener(backend.clone());

    backend.subscribe("chat:42").await.unwrap();

    // Raw NOTIFY on our channel carrying a malformed notice.
    let channel = channel_for(&integration_namespace("foreign_notices"), "chat:42");
    backend.publisher.notify(&channel, "{\"version\":1}").await.unwrap();

    let idle = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;
    assert!(idle.is_err(), "malformed notice must not be delivered");
    assert_eq!(backend.stats().rejected, 1);

    handle.abort();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn reconnect_reestablishes_every_interest() {
    let namespace = integration_namespace("reconnect");
    let backend = Arc::new(integration_backend("reconnect").await);
    let (mut rx, handle) = spawn_listener(backend.clone());

    backend.subscribe("chat:42").await.unwrap();
    backend.subscribe("chat:43").await.unwrap();

    // Kill the listener session from the publisher session. The listener's
    // last statement is its LISTEN, which identifies it in pg_stat_activity.
    // Match only THIS test's channels so a parallel test's listener survives.
    let kill_sql = format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE pid <> pg_backend_pid() AND datname = current_database() \
             AND (query = 'LISTEN {}' OR query = 'LISTEN {}')",
        quote_channel(&channel_for(&namespace, "chat:42")),
        quote_channel(&channel_for(&namespace, "chat:43")),
    );
    backend
        .publisher
        .slot
        .acquire()
        .await
        .expect("publisher session")
        .client
        .execute(kill_sql.as_str(), &[])
        .await
        .unwrap();

    // Readiness drops, then returns once every interest is re-LISTENed.
    let mut ready = backend.ready.clone();
    tokio::time::timeout(Duration::from_secs(10), async {
        ready.wait_for(|r| !*r).await.unwrap();
        ready.wait_for(|r| *r).await.unwrap();
    })
    .await
    .expect("listener reconnects within 10s");

    assert!(backend.stats().reconnects >= 1);

    backend.publish("chat:43", r#"{"body":"after"}"#).await.unwrap();
    let (stream, payload) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("publication delivered after reconnect")
        .expect("delivery channel open");
    assert_eq!(stream, "chat:43");
    assert_eq!(String::from_utf8(payload).unwrap(), r#"{"body":"after"}"#);

    handle.abort();
}

// ---- Integration tests: overflow relay ---------------------------------

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn overflow_publication_round_trips_through_the_relay_table() {
    let backend =
        Arc::new(integration_overflow_backend("overflow_roundtrip", DEFAULT_OVERFLOW_TTL).await);
    let (mut rx, handle) = spawn_listener(backend.clone());

    backend.subscribe("chat:42").await.unwrap();

    let payload = relay_sized_payload("roundtrip");
    assert!(payload.len() > MAX_NOTIFY_PAYLOAD_LEN, "the test payload must need the relay");
    backend.publish("chat:42", &payload).await.expect("overflow publish commits");

    let (stream, delivered) = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("relayed publication delivered within 10s")
        .expect("delivery channel open");

    assert_eq!(stream, "chat:42");
    assert_eq!(
        String::from_utf8(delivered).unwrap(),
        payload,
        "the relayed payload must survive byte for byte"
    );

    let stats = backend.stats();
    assert_eq!(stats.overflow, 1, "one overflow notice was received");
    assert_eq!(stats.overflow_gaps, 0, "a live row is not a gap");

    handle.abort();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn overflow_publish_stores_one_row_matching_the_table_contract() {
    let backend =
        Arc::new(integration_overflow_backend("overflow_row", DEFAULT_OVERFLOW_TTL).await);
    let namespace = integration_namespace("overflow_row");

    backend.subscribe("chat:42").await.unwrap();
    let payload = relay_sized_payload("stored");
    backend.publish("chat:42", &payload).await.unwrap();

    let client = integration_client().await;
    let row = client
        .query_one(
            "SELECT stream, json_payload, expires_at > now() AS live \
                 FROM orbitcast_payloads WHERE namespace = $1",
            &[&namespace],
        )
        .await
        .expect("exactly one relay row for this namespace");

    assert_eq!(row.get::<_, String>("stream"), "chat:42");
    assert_eq!(row.get::<_, String>("json_payload"), payload);
    assert!(row.get::<_, bool>("live"), "expires_at comes from the database clock");

    client
        .execute("DELETE FROM orbitcast_payloads WHERE namespace = $1", &[&namespace])
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn inline_and_overflow_publications_preserve_stream_order() {
    let backend =
        Arc::new(integration_overflow_backend("overflow_order", DEFAULT_OVERFLOW_TTL).await);
    let (mut rx, handle) = spawn_listener(backend.clone());

    backend.subscribe("chat:42").await.unwrap();

    // Alternating inline and relayed publications on one stream: the
    // relayed ones need a round trip the inline ones do not, so a naive
    // implementation would deliver 2 and 4 before 1 and 3.
    let published = vec![
        r#"{"marker":"1-inline"}"#.to_string(),
        relay_sized_payload("2-overflow"),
        r#"{"marker":"3-inline"}"#.to_string(),
        relay_sized_payload("4-overflow"),
    ];

    for payload in &published {
        backend.publish("chat:42", payload).await.expect("publish accepted");
    }

    let mut delivered = Vec::new();
    for _ in 0..published.len() {
        let (_, payload) = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("every publication is delivered within 10s")
            .expect("delivery channel open");
        delivered.push(String::from_utf8(payload).unwrap());
    }

    assert_eq!(delivered, published, "per-stream order must survive the relay");
    assert_eq!(backend.stats().overflow_gaps, 0);

    handle.abort();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn expired_relay_row_is_a_counted_gap_not_a_silent_skip() {
    // A one-millisecond TTL guarantees the row is expired by the time the
    // notification completes its round trip.
    let backend =
        Arc::new(integration_overflow_backend("overflow_expired", Duration::from_millis(1)).await);
    let (mut rx, handle) = spawn_listener(backend.clone());

    backend.subscribe("chat:42").await.unwrap();
    backend.publish("chat:42", &relay_sized_payload("expired")).await.unwrap();

    let idle = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    assert!(idle.is_err(), "an expired relay row must not be delivered");

    let stats = backend.stats();
    assert_eq!(stats.overflow, 1, "the overflow notice was received");
    assert_eq!(stats.overflow_gaps, 1, "and counted as a delivery gap");

    // A gap must not wedge the queue: the next inline publication arrives.
    backend.publish("chat:42", r#"{"marker":"after-gap"}"#).await.unwrap();
    let (_, payload) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("delivery continues after a gap")
        .expect("delivery channel open");
    assert_eq!(String::from_utf8(payload).unwrap(), r#"{"marker":"after-gap"}"#);

    handle.abort();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn overflow_notice_on_a_relay_disabled_node_is_a_counted_gap() {
    let namespace = integration_namespace("overflow_disabled");
    ensure_overflow_table().await;

    // Publisher relays; this node cannot fetch. Same namespace, so the
    // notice is addressed to it and is not merely foreign traffic.
    let mut publisher_config = PostgresConfig::new(integration_dsn(), namespace.clone());
    publisher_config.overflow = OverflowConfig { enabled: true, ..OverflowConfig::default() };
    let publisher = PostgresPubSub::connect(publisher_config).await.unwrap();

    let receiver = Arc::new(
        PostgresPubSub::connect(PostgresConfig::new(integration_dsn(), namespace.clone()))
            .await
            .unwrap(),
    );
    let (mut rx, handle) = spawn_listener(receiver.clone());

    receiver.subscribe("chat:42").await.unwrap();
    publisher.publish("chat:42", &relay_sized_payload("unfetchable")).await.unwrap();

    let idle = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    assert!(idle.is_err(), "a node without the relay cannot deliver a relayed payload");

    let stats = receiver.stats();
    assert_eq!(stats.overflow, 1);
    assert_eq!(stats.overflow_gaps, 1, "an unfetchable notice is a gap, not a drop");
    assert_eq!(stats.rejected, 0, "the notice itself is well-formed");

    handle.abort();
}

/// Expiry cleanup racing a fetch: the publisher's transaction committed the
/// row and its notice together, and a cleanup (any node's) removed the row
/// before this node's fetch ran. Reproduced deterministically by committing
/// the delete *in the same transaction* as the notify - from the listener's
/// point of view that is indistinguishable from losing the race.
#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn cleanup_racing_a_fetch_is_a_gap_that_names_the_stream() {
    let namespace = integration_namespace("overflow_race");
    ensure_overflow_table().await;

    let mut config = PostgresConfig::new(integration_dsn(), namespace.clone());
    config.overflow = OverflowConfig { enabled: true, ..OverflowConfig::default() };
    let backend = Arc::new(PostgresPubSub::connect(config).await.expect("connect"));
    let (mut events, handle) = spawn_event_listener(Arc::clone(&backend));
    backend.subscribe("chat:42").await.unwrap();

    // What the publisher would have committed, minus the row.
    let publication = Publication::new(&namespace, "chat:42", relay_sized_payload("raced"));
    let reference = OverflowRef::for_publication(&publication);
    let notice = serde_json::to_string(
        &crate::protocol::v1::publication::PublicationNotice::overflow(reference),
    )
    .unwrap();
    let channel = channel_for(&namespace, "chat:42");

    let mut client = integration_client().await;
    let transaction = client.transaction().await.unwrap();
    transaction
        .execute(
            OVERFLOW_INSERT_SQL,
            &[
                &publication.id,
                &publication.namespace,
                &publication.stream,
                &publication.json_payload,
                &300_000_i64,
            ],
        )
        .await
        .unwrap();
    transaction.execute(NOTIFY_SQL, &[&channel, &notice]).await.unwrap();
    transaction
        .execute("DELETE FROM orbitcast_payloads WHERE id = $1::text::uuid", &[&publication.id])
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("the gap is reported within 10s")
        .expect("event channel open");
    assert_matches!(
        event,
        PubSubEvent::Gap { ref stream, ref reason }
            if stream == "chat:42" && reason.contains("could not be fetched")
    );
    let stats = backend.stats();
    assert_eq!(stats.overflow, 1);
    assert_eq!(stats.overflow_gaps, 1, "a lost race is a counted gap, in order");

    // The gap did not wedge the stream: the next inline publication arrives.
    backend.publish("chat:42", r#"{"marker":"after-race"}"#).await.unwrap();
    let next = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("delivery continues")
        .expect("event channel open");
    assert_matches!(
        next,
        PubSubEvent::Publication { ref stream, ref payload }
            if stream == "chat:42" && payload.as_ref() == br#"{"marker":"after-race"}"#
    );

    handle.abort();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_dedicated_listener_dsn_is_used_for_the_listen_session() {
    // The same database twice over: a listener DSN that is a (differently
    // spelled) address of the same cable database must work exactly like
    // the single-DSN shape, and the publisher must still reach the listener.
    let mut config = PostgresConfig::new(integration_dsn(), integration_namespace("listener_dsn"));
    config.listener_database_url = Some(format!(
        "{}{}application_name=orbitcast_listener",
        integration_dsn(),
        if integration_dsn().contains('?') { "&" } else { "?" }
    ));
    let backend = Arc::new(PostgresPubSub::connect(config).await.expect("connect"));
    let (mut rx, handle) = spawn_listener(Arc::clone(&backend));

    backend.subscribe("chat:42").await.unwrap();
    backend.publish("chat:42", r#"{"body":"via listener dsn"}"#).await.unwrap();

    let (stream, payload) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("delivered")
        .expect("channel open");
    assert_eq!(stream, "chat:42");
    assert_eq!(String::from_utf8(payload).unwrap(), r#"{"body":"via listener dsn"}"#);

    // A blank listener DSN is refused at the boundary, not at connect time.
    let mut blank = PostgresConfig::new(integration_dsn(), "ns");
    blank.listener_database_url = Some("  ".to_string());
    assert_matches!(blank.validate(), Err(PubSubError::Config(_)));

    handle.abort();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn publication_above_the_configured_ceiling_is_rejected_before_sql() {
    let mut config =
        PostgresConfig::new(integration_dsn(), integration_namespace("overflow_ceiling"));
    config.overflow =
        OverflowConfig { enabled: true, max_publication_size: 16_000, ..OverflowConfig::default() };
    ensure_overflow_table().await;
    let backend = PostgresPubSub::connect(config).await.unwrap();

    let payload = format!(r#"{{"html":"{}"}}"#, "z".repeat(16_000));
    let err = backend.publish("chat:42", &payload).await.unwrap_err();
    assert_matches!(err, PubSubError::PublicationTooLarge { max: 16_000, .. });
}
