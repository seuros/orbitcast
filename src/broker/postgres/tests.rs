use std::assert_matches;
use std::collections::BTreeMap;

use super::test_support::{integration_broker, integration_broker_with_relay};
use super::*;
use crate::protocol::v1::recovery::SessionSnapshot;
use crate::pubsub::postgres::test_support::{
    integration_client, integration_dsn, integration_namespace,
};

const NAMESPACE: &str = "example_production";

fn broker_config() -> BrokerConfig {
    BrokerConfig { enabled: true, ..BrokerConfig::default() }
}

#[test]
fn config_rejects_empty_namespace_and_url() {
    let err =
        PostgresBrokerConfig::new("postgres://h/db", "", broker_config()).validate().unwrap_err();
    assert!(err.to_string().contains("namespace"), "{err}");

    let err = PostgresBrokerConfig::new("   ", NAMESPACE, broker_config()).validate().unwrap_err();
    assert!(err.to_string().contains("database_url"), "{err}");
}

#[test]
fn config_rejects_an_unusable_pool_size_or_relay() {
    let mut config = PostgresBrokerConfig::new("postgres://h/db", NAMESPACE, broker_config());
    config.pool_size = 0;
    assert!(config.validate().is_err());

    config.pool_size = MAX_POOL_SIZE + 1;
    assert!(config.validate().is_err());

    config.pool_size = DEFAULT_POOL_SIZE;
    config.validate().unwrap();

    // The relay settings are validated here too: the broker writes relay rows.
    config.overflow = OverflowConfig { enabled: true, ttl: Duration::ZERO, ..config.overflow };
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("overflow"), "{err}");
}

#[tokio::test]
async fn connect_rejects_invalid_config_before_touching_the_network() {
    let config = PostgresBrokerConfig::new("postgres://h/db", "", broker_config());
    let Err(err) = PostgresBroker::connect(config).await else {
        panic!("an empty namespace must not yield a usable broker");
    };
    assert_matches!(err, BrokerError::Config(_));
}

#[test]
fn server_errors_name_the_migration_they_point_at() {
    assert!(missing_table_hint().contains("003_create_orbitcast_broker"));
    assert!(missing_table_hint().contains("004_rekey_orbitcast_sessions_by_session_id"));
    assert!(stale_schema_hint().contains("004_rekey_orbitcast_sessions_by_session_id"));
}

// ---- Integration tests: require a live PostgreSQL ----------------------
//
// Run with:
//   DATABASE_URL=postgres://... cargo test -p orbitcast -- --ignored

fn publication(stream: &str, payload: &str) -> Publication {
    Publication::new(NAMESPACE, stream, payload)
}

/// Retain one fresh publication; the namespace in the envelope is the
/// contract namespace, the broker's own namespace is what keys the row.
async fn retain(broker: &PostgresBroker, stream: &str, payload: &str) -> Retained {
    broker.retain(&publication(stream, payload)).await.expect("retain commits")
}

async fn sweeper() -> PgSlot {
    PgSlot::connect(integration_dsn(), "broker sweeper").await.unwrap()
}

/// Rows of the broker table for one namespace, expired or not.
async fn raw_message_count(namespace: &str) -> i64 {
    integration_client()
        .await
        .query_one("SELECT count(*) FROM orbitcast_messages WHERE namespace = $1", &[&namespace])
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn retain_assigns_contiguous_sequences_and_reports_the_range() {
    let broker = integration_broker("broker_range", broker_config()).await;

    let empty = broker.range("chat:42").await.unwrap();
    assert_eq!(empty.watermark, Cursor::new(1, 0), "an untouched stream starts before 1");
    assert_eq!(empty.oldest, None);

    for expected in 1..=3u64 {
        let retained = retain(&broker, "chat:42", r#"{"body":"hi"}"#).await;
        assert_eq!(retained.duplicate, None);
        assert!(!retained.relayed);
        assert_eq!(retained.position, Cursor::new(1, expected));
    }

    let range = broker.range("chat:42").await.unwrap();
    assert_eq!(range.watermark, Cursor::new(1, 3));
    assert_eq!(range.oldest, Some(1));
    assert!(range.covers(Cursor::new(1, 0)), "everything is still retained");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn concurrent_publishers_to_one_stream_commit_strictly_ordered_sequences() {
    // The property the per-stream lock exists to protect: racing publishers may
    // not skip a sequence, because a reader treats "> last_delivered" as
    // "everything I have not seen".
    const PUBLISHERS: u64 = 8;
    const PER_PUBLISHER: u64 = 6;

    let broker = integration_broker("broker_race", broker_config()).await;

    let mut tasks = Vec::new();
    for _ in 0..PUBLISHERS {
        let broker = Arc::clone(&broker);
        tasks.push(tokio::spawn(async move {
            let mut positions = Vec::new();
            for _ in 0..PER_PUBLISHER {
                positions.push(retain(&broker, "chat:race", r#"{"n":1}"#).await.position.sequence);
            }
            positions
        }));
    }

    let mut sequences = Vec::new();
    for task in tasks {
        sequences.extend(task.await.expect("publisher task"));
    }
    sequences.sort_unstable();

    let expected: Vec<u64> = (1..=PUBLISHERS * PER_PUBLISHER).collect();
    assert_eq!(sequences, expected, "sequences must be contiguous with no skips");

    // And the store agrees: reading from the start yields every message in
    // order, exactly once.
    let rows = broker.fetch_after("chat:race", Cursor::new(1, 0), REPLAY_BATCH).await.unwrap();
    let read: Vec<u64> = rows.iter().map(|row| row.position.sequence).collect();
    assert_eq!(read, expected);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn duplicate_publication_ids_are_told_apart_and_never_stored_twice() {
    let broker = integration_broker("broker_dedup", broker_config()).await;
    let first = publication("chat:42", r#"{"body":"one"}"#);

    let retained = broker.retain(&first).await.unwrap();
    assert_eq!(retained.duplicate, None);

    // A retry: same id, same bytes. Expected, counted as identical.
    let retry = broker.retain(&first).await.unwrap();
    assert_eq!(retry.duplicate, Some(DuplicateKind::Identical));
    assert_eq!(retry.position, retained.position, "the existing position is returned");

    // Same id, *different* payload: first write wins, nothing is stored, and
    // the conflict is counted on its own.
    let conflict = Publication { json_payload: r#"{"body":"two"}"#.to_string(), ..first };
    let second = broker.retain(&conflict).await.unwrap();
    assert_eq!(second.duplicate, Some(DuplicateKind::Conflicting));
    assert_eq!(second.position, retained.position);

    let rows = broker.fetch_after("chat:42", Cursor::new(1, 0), REPLAY_BATCH).await.unwrap();
    assert_eq!(rows.len(), 1, "retention holds one row for one id");
    assert_eq!(rows[0].json_payload, r#"{"body":"one"}"#, "the first payload is kept");

    let stats = broker.stats();
    assert_eq!(stats.retained, 1);
    assert_eq!(stats.duplicates_identical, 1, "the retry is counted, not swallowed");
    assert_eq!(stats.duplicates_conflicting, 1, "the conflict is counted separately");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn an_oversized_publication_is_retained_in_full_and_announced_by_reference() {
    let payload = format!(r#"{{"html":"{}"}}"#, "x".repeat(20_000));

    // Relay off: refused, exactly as on an ephemeral stream, and not retained.
    let strict = integration_broker("broker_relay_off", broker_config()).await;
    assert_matches!(
        strict.retain(&publication("chat:big", &payload)).await.unwrap_err(),
        BrokerError::Publication(crate::pubsub::PubSubError::PayloadTooLarge { .. })
    );
    assert_eq!(strict.range("chat:big").await.unwrap().watermark, Cursor::new(1, 0));

    // Relay on: the broker row holds the full payload, the relay row holds it
    // for the live listeners, and both commit together.
    let overflow = OverflowConfig { enabled: true, ..OverflowConfig::default() };
    let relayed = integration_broker_with_relay("broker_relay_on", broker_config(), overflow).await;
    let big = publication("chat:big", &payload);
    let retained = relayed.retain(&big).await.unwrap();
    assert!(retained.relayed, "announced through the overflow relay");
    assert_eq!(retained.position, Cursor::new(1, 1));

    let rows = relayed.fetch_after("chat:big", Cursor::new(1, 0), REPLAY_BATCH).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].json_payload, payload, "replay reads the full payload from the broker row");

    let relay_rows: i64 = integration_client()
        .await
        .query_one(
            "SELECT count(*) FROM orbitcast_payloads WHERE id = $1::text::uuid \
             AND namespace = $2 AND stream = $3 AND expires_at > now()",
            &[&big.id, &big.namespace, &big.stream],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(relay_rows, 1, "the live listeners find the relay row the notice references");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_missed_notification_is_repaired_by_the_reconciliation_read() {
    // No listener at all: the notification is lost by construction. The
    // durable rows are still there, which is the whole point of treating
    // NOTIFY as a wakeup rather than as the message source.
    let broker = integration_broker("broker_reconcile", broker_config()).await;

    for _ in 0..3 {
        retain(&broker, "chat:missed", r#"{"n":1}"#).await;
    }

    let rows = broker.fetch_after("chat:missed", Cursor::new(1, 0), REPLAY_BATCH).await.unwrap();
    assert_eq!(rows.len(), 3, "a reader that never got a notification still finds the rows");
    assert_eq!(rows[2].position, Cursor::new(1, 3));

    // A reader already at the watermark reads nothing and is not confused.
    let none = broker.fetch_after("chat:missed", Cursor::new(1, 3), REPLAY_BATCH).await.unwrap();
    assert_eq!(none, []);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn expired_history_becomes_an_explicit_gap_and_bumps_the_generation() {
    // A 1 ms retention window expires every row immediately.
    let config = BrokerConfig { retention: Duration::from_millis(1), ..broker_config() };
    let broker = integration_broker("broker_expiry", config.clone()).await;
    let namespace = integration_namespace("broker_expiry");

    for _ in 0..3 {
        retain(&broker, "chat:old", r#"{"n":1}"#).await;
    }
    let cursor = Cursor::new(1, 1);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Expired rows are unreadable even before the sweeper runs.
    let rows = broker.fetch_after("chat:old", cursor, REPLAY_BATCH).await.unwrap();
    assert!(rows.is_empty(), "an expired row is never replayed");

    let range = broker.range("chat:old").await.unwrap();
    assert_eq!(range.oldest, None);
    assert!(!range.covers(cursor), "a cursor into expired history is a gap");

    // The sweeper reclaims the rows and starts a new generation, so the old
    // cursor is now unmistakably stale rather than matching new sequences.
    let link = sweeper().await;
    let stats = BrokerStats::default();
    sweep_once(&link, &namespace, &config, &stats).await.unwrap();
    sweep_once(&link, &namespace, &config, &stats).await.unwrap();

    let range = broker.range("chat:old").await.unwrap();
    assert_eq!(range.watermark, Cursor::new(2, 0), "the wiped stream restarted at generation 2");
    assert!(!range.covers(cursor), "the previous generation can never be replayed");
    assert!(stats.snapshot().swept >= 3);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_generation_bump_is_checked_under_the_stream_lock() {
    // A stream with live history is never bumped, however the candidate scan
    // was produced: the check runs under the same lock a publish holds.
    let broker = integration_broker("broker_bump", broker_config()).await;
    let namespace = integration_namespace("broker_bump");
    retain(&broker, "chat:live", r#"{"n":1}"#).await;

    let link = sweeper().await;
    let mut guard = link.acquire().await.unwrap();
    assert!(
        !bump_generation_tx(&mut guard.client, &namespace, "chat:live", 1).await.unwrap(),
        "a stream with retained rows keeps its generation"
    );
    assert!(
        !bump_generation_tx(&mut guard.client, &namespace, "chat:never", 1).await.unwrap(),
        "a stream never published to has nothing to bump"
    );
    drop(guard);

    // The race the lock closes: a sweeper that found the stream wiped while a
    // publisher is committing into it. Whatever interleaving PostgreSQL picks,
    // no row may end up stranded in a generation the position row has moved
    // past - such a row would be history a resuming client can never reach.
    let short = BrokerConfig { retention: Duration::from_millis(1), ..broker_config() };
    let seed =
        integration_broker_with_relay("broker_bump_race", short.clone(), OverflowConfig::default())
            .await;
    let writer = integration_broker("broker_bump_race", broker_config()).await;
    let race_namespace = integration_namespace("broker_bump_race");
    retain(&seed, "chat:race", r#"{"seed":true}"#).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let publisher = tokio::spawn({
        let writer = Arc::clone(&writer);
        async move {
            for n in 0..40 {
                retain(&writer, "chat:race", &format!(r#"{{"n":{n}}}"#)).await;
            }
        }
    });
    let stats = BrokerStats::default();
    for _ in 0..20 {
        sweep_once(&link, &race_namespace, &short, &stats).await.unwrap();
    }
    publisher.await.unwrap();

    let position = writer.range("chat:race").await.unwrap();
    let stranded: i64 = integration_client()
        .await
        .query_one(
            "SELECT count(*) FROM orbitcast_messages WHERE namespace = $1 AND stream = $2 \
             AND generation <> $3::bigint AND expires_at > now()",
            &[
                &race_namespace,
                &"chat:race",
                &i64::try_from(position.watermark.generation).unwrap(),
            ],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stranded, 0, "no live row may sit in a generation the stream has moved past");

    // Whether the sweeper won (generation 2 from sequence 1) or the writer
    // kept the stream alive (generation 1, the expired seed swept out from
    // under sequence 2 onwards), the live rows of the current generation are
    // contiguous: no hole a reader would misread as a skipped sequence.
    let rows = writer
        .fetch_after("chat:race", Cursor::new(position.watermark.generation, 0), REPLAY_BATCH)
        .await
        .unwrap();
    let sequences: Vec<u64> = rows.iter().map(|row| row.position.sequence).collect();
    let first = sequences.first().copied().expect("the writer's rows are live");
    let expected: Vec<u64> = (first..first + sequences.len() as u64).collect();
    assert_eq!(sequences, expected, "the current generation is contiguous");
    assert_eq!(sequences.len(), 40, "every writer row sits in the current generation");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn the_count_bound_trims_the_oldest_retained_messages() {
    let config = BrokerConfig { max_messages_per_stream: 3, ..broker_config() };
    let broker = integration_broker("broker_trim", config).await;

    for _ in 0..6 {
        retain(&broker, "chat:trim", r#"{"n":1}"#).await;
    }

    let range = broker.range("chat:trim").await.unwrap();
    assert_eq!(range.watermark, Cursor::new(1, 6));
    assert_eq!(range.oldest, Some(4), "only the newest three survive");
    assert!(!range.covers(Cursor::new(1, 2)), "trimmed history is a gap, not a silent skip");
    assert!(range.covers(Cursor::new(1, 3)), "the next message (4) is still retained");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn sweeps_are_scoped_to_the_namespace() {
    let config = BrokerConfig { retention: Duration::from_millis(1), ..broker_config() };
    let mine = integration_broker("broker_scope_a", config.clone()).await;
    let theirs = integration_broker("broker_scope_b", config.clone()).await;
    let my_namespace = integration_namespace("broker_scope_a");
    let their_namespace = integration_namespace("broker_scope_b");

    retain(&mine, "chat:1", r#"{"n":1}"#).await;
    retain(&theirs, "chat:1", r#"{"n":1}"#).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let link = sweeper().await;
    let stats = BrokerStats::default();
    sweep_once(&link, &my_namespace, &config, &stats).await.unwrap();

    assert_eq!(raw_message_count(&my_namespace).await, 0, "my expired row is gone");
    assert_eq!(raw_message_count(&their_namespace).await, 1, "another namespace's rows are theirs");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn the_growth_refresh_exports_gauges_and_enforces_the_namespace_quota() {
    let config = BrokerConfig {
        max_messages_per_stream: 3,
        max_messages_per_namespace: 4,
        ..broker_config()
    };
    let broker = integration_broker("broker_quota", config.clone()).await;
    let namespace = integration_namespace("broker_quota");

    for stream in ["chat:a", "chat:b"] {
        for _ in 0..3 {
            retain(&broker, stream, r#"{"n":1}"#).await;
        }
    }

    // The broker's own growth task may already have trimmed on its reconcile
    // tick, so the end state - not this call's count - is the invariant: six
    // rows against a quota of four leaves exactly the four newest.
    let link = sweeper().await;
    let stats = BrokerStats::default();
    let removed = refresh_growth(&link, &namespace, &config, &stats).await.unwrap();
    assert!(removed <= 2, "at most the two oldest go: {removed}");

    let growth = stats.snapshot();
    assert_eq!(growth.messages_current, 4);
    assert!(growth.bytes_current > 0, "the byte gauge follows the surviving rows");
    assert_eq!(growth.swept, removed, "every trimmed row is counted as swept");

    // A second pass finds nothing to do and reports the live footprint.
    assert_eq!(refresh_growth(&link, &namespace, &config, &stats).await.unwrap(), 0);
    assert_eq!(stats.snapshot().messages_current, 4);
    assert_eq!(raw_message_count(&namespace).await, 4);

    // The trimmed stream reports the hole honestly.
    let range = broker.range("chat:a").await.unwrap();
    assert!(!range.covers(Cursor::new(1, 0)), "trimmed history is an explicit gap");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn two_concurrent_fencing_claims_produce_exactly_one_winner() {
    let broker = integration_broker("broker_fence", broker_config()).await;
    let session = format!("session:{}", uuid::Uuid::new_v4());

    let (first, second) = {
        let a = Arc::clone(&broker);
        let b = Arc::clone(&broker);
        let session_a = session.clone();
        let session_b = session.clone();
        tokio::join!(
            tokio::spawn(async move { a.fence(&session_a).await.unwrap() }),
            tokio::spawn(async move { b.fence(&session_b).await.unwrap() }),
        )
    };
    let first = first.unwrap();
    let second = second.unwrap();

    assert_ne!(first, second, "two claims can never hold the same lease");
    let winner = first.max(second);
    assert_eq!(
        broker.fencing_generation(&session).await.unwrap(),
        winner,
        "exactly one claim holds the lease; the other one has been fenced out"
    );
    assert_eq!(winner.min(first.min(second)) + 1, winner, "leases are strictly increasing");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_session_row_lives_from_its_last_activity() {
    // Seconds of slack, not milliseconds: the database clock decides expiry,
    // and a containerized PostgreSQL shared by the whole ignored suite can
    // stall a statement for most of a second.
    let config = BrokerConfig { session_ttl: Duration::from_secs(2), ..broker_config() };
    let broker = integration_broker("broker_touch", config).await;
    let session = format!("session:{}", uuid::Uuid::new_v4());

    let lease = broker.fence(&session).await.unwrap();

    // Kept alive by touches across more than a full TTL (3 s of 500 ms touches).
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        broker.touch_session(&session).await.unwrap();
        assert_eq!(broker.fencing_generation(&session).await.unwrap(), lease, "still live");
    }

    // Left alone, it expires and the next read sees no live row.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(broker.fencing_generation(&session).await.unwrap(), 0, "expired without activity");
    assert_eq!(broker.load_snapshot(&session).await.unwrap(), None);

    // Touching an expired row - swept or not - never resurrects it: a lapsed
    // lease comes back only through a fence.
    broker.touch_session(&session).await.unwrap();
    assert_eq!(broker.fencing_generation(&session).await.unwrap(), 0, "a touch is not a fence");
    assert_eq!(broker.fence(&session).await.unwrap(), lease + 1, "the next resume re-fences");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_session_snapshot_round_trips_without_touching_the_lease() {
    let broker = integration_broker("broker_snapshot", broker_config()).await;
    let session = format!("session:{}", uuid::Uuid::new_v4());

    let lease = broker.fence(&session).await.unwrap();
    assert_eq!(
        SessionSnapshot::decode(&broker.load_snapshot(&session).await.unwrap().unwrap())
            .unwrap()
            .cursors
            .len(),
        0,
        "a fresh lease carries an empty snapshot"
    );

    let cursors = BTreeMap::from([("chat:42".to_string(), Cursor::new(1, 7).encode())]);
    let snapshot = SessionSnapshot::new(&session, cursors);
    broker.save_snapshot(&session, &snapshot.to_json().unwrap()).await.unwrap();

    assert_eq!(
        broker.fencing_generation(&session).await.unwrap(),
        lease,
        "an acknowledgement never moves the fencing generation"
    );

    let stored = broker.load_snapshot(&session).await.unwrap().expect("the snapshot is stored");
    assert_eq!(
        SessionSnapshot::decode(&stored).unwrap(),
        snapshot,
        "only versioned JSON cursors are stored - never cookies or Marshal data"
    );
}
