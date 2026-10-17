use std::assert_matches;

use super::*;

fn enabled() -> BrokerConfig {
    BrokerConfig { enabled: true, ..BrokerConfig::default() }
}

#[test]
fn broker_is_off_by_default_with_the_documented_bounds() {
    let config = BrokerConfig::default();
    assert!(!config.enabled, "retained history needs a migration, so it is opt-in");
    assert_eq!(config.retention, Duration::from_mins(5));
    assert_eq!(config.max_messages_per_stream, 1000);
    assert_eq!(config.max_bytes_per_stream, 4 * 1024 * 1024);
    assert_eq!(config.reconcile_interval, Duration::from_secs(5));
    assert_eq!(config.replay_buffer, 256);
    assert_eq!(config.session_ttl, Duration::from_hours(1));
    assert_eq!(config.max_messages_per_namespace, 1_000_000);
    assert_eq!(config.max_bytes_per_namespace, 1024 * 1024 * 1024);
    config.validate().unwrap();
}

#[test]
fn a_disabled_broker_handles_nothing() {
    let config = BrokerConfig::default();
    assert!(!config.handles("chat:42"));
    assert!(!config.handles(""));
}

#[test]
fn an_empty_allowlist_retains_every_stream() {
    let config = enabled();
    assert!(config.handles("chat:42"));
    assert!(config.handles("anything"));
}

#[test]
fn an_allowlist_is_exact_prefix_matching() {
    let config = BrokerConfig { streams: vec!["chat:".into(), "orders:".into()], ..enabled() };
    assert!(config.handles("chat:42"));
    assert!(config.handles("orders:1"));
    assert!(!config.handles("presence:chat:42"), "a prefix is a prefix, not a substring");
    assert!(!config.handles("telemetry:cpu"));
    assert!(!config.handles("chat"), "a shorter name does not match a longer prefix");
}

#[test]
fn validate_rejects_unusable_bounds() {
    let zero_retention = BrokerConfig { retention: Duration::ZERO, ..enabled() };
    assert!(zero_retention.validate().is_err());

    let forever = BrokerConfig { retention: MAX_RETENTION + Duration::from_secs(1), ..enabled() };
    assert!(forever.validate().is_err());

    let no_messages = BrokerConfig { max_messages_per_stream: 0, ..enabled() };
    assert!(no_messages.validate().is_err());

    let too_many =
        BrokerConfig { max_messages_per_stream: MAX_MESSAGES_PER_STREAM_LIMIT + 1, ..enabled() };
    assert!(too_many.validate().is_err());

    let no_bytes = BrokerConfig { max_bytes_per_stream: 0, ..enabled() };
    assert!(no_bytes.validate().is_err());

    let never_reconciles = BrokerConfig { reconcile_interval: Duration::ZERO, ..enabled() };
    assert!(never_reconciles.validate().is_err());

    let no_buffer = BrokerConfig { replay_buffer: 0, ..enabled() };
    assert!(no_buffer.validate().is_err());

    let huge_buffer = BrokerConfig { replay_buffer: MAX_REPLAY_BUFFER + 1, ..enabled() };
    assert!(huge_buffer.validate().is_err());

    let no_sessions = BrokerConfig { session_ttl: Duration::ZERO, ..enabled() };
    assert!(no_sessions.validate().is_err());

    let empty_prefix = BrokerConfig { streams: vec![String::new()], ..enabled() };
    assert!(empty_prefix.validate().is_err());

    // Namespace quotas: zero, above the ceiling, or below the per-stream
    // bound they are supposed to contain.
    for (messages, bytes) in [
        (0, DEFAULT_MAX_BYTES_PER_NAMESPACE),
        (MAX_MESSAGES_PER_NAMESPACE_LIMIT + 1, DEFAULT_MAX_BYTES_PER_NAMESPACE),
        (DEFAULT_MAX_MESSAGES_PER_STREAM - 1, DEFAULT_MAX_BYTES_PER_NAMESPACE),
        (DEFAULT_MAX_MESSAGES_PER_NAMESPACE, 0),
        (DEFAULT_MAX_MESSAGES_PER_NAMESPACE, MAX_BYTES_PER_NAMESPACE_LIMIT + 1),
        (DEFAULT_MAX_MESSAGES_PER_NAMESPACE, DEFAULT_MAX_BYTES_PER_STREAM - 1),
    ] {
        let quota = BrokerConfig {
            max_messages_per_namespace: messages,
            max_bytes_per_namespace: bytes,
            ..enabled()
        };
        assert!(quota.validate().is_err(), "messages {messages}, bytes {bytes}");
    }

    // A namespace quota equal to the per-stream bound is the smallest
    // namespace that still holds one full stream.
    let one_stream = BrokerConfig {
        max_messages_per_namespace: DEFAULT_MAX_MESSAGES_PER_STREAM,
        max_bytes_per_namespace: DEFAULT_MAX_BYTES_PER_STREAM,
        ..enabled()
    };
    #[cfg(not(feature = "memory"))]
    one_stream.validate().unwrap();
    #[cfg(feature = "memory")]
    assert!(one_stream.validate().is_err(), "a memory build refuses every enabled broker");
}

#[test]
fn a_disabled_broker_never_fails_validation() {
    // Nonsense bounds on a disabled broker are inert, not a boot failure.
    let config = BrokerConfig { retention: Duration::ZERO, replay_buffer: 0, ..Default::default() };
    config.validate().unwrap();
}

#[cfg(feature = "memory")]
#[test]
fn a_memory_build_refuses_to_enable_the_broker() {
    use std::assert_matches;

    let err = enabled().validate().unwrap_err();
    assert_matches!(err, BrokerError::Config(_));
    assert!(err.to_string().contains("postgres"), "{err}");
}

#[test]
fn stream_range_covers_only_fully_retained_history() {
    let range = StreamRange { watermark: Cursor::new(1, 10), oldest: Some(5) };

    assert!(range.covers(Cursor::new(1, 10)), "the live edge needs no history");
    assert!(range.covers(Cursor::new(1, 12)), "a client ahead of us is not a gap");
    assert!(range.covers(Cursor::new(1, 9)), "the next message (10) is retained");
    assert!(range.covers(Cursor::new(1, 4)), "the next message (5) is the oldest retained");
    assert!(!range.covers(Cursor::new(1, 3)), "message 4 was already swept: a gap");
    assert!(!range.covers(Cursor::new(2, 9)), "a foreign generation is never replayable");

    let wiped = StreamRange { watermark: Cursor::new(2, 0), oldest: None };
    assert!(wiped.covers(Cursor::new(2, 0)), "an empty stream at the edge is covered");
    assert!(!wiped.covers(Cursor::new(1, 3)), "the old generation is gone");
}

#[test]
fn stats_snapshot_counts_every_category() {
    let stats = BrokerStats::default();
    stats.record_retained();
    stats.record_duplicate(DuplicateKind::Identical);
    stats.record_duplicate(DuplicateKind::Conflicting);
    stats.record_duplicate(DuplicateKind::Conflicting);
    stats.record_replayed(3);
    stats.record_gap();
    stats.record_reconciled();
    stats.record_swept(12);
    stats.record_fenced();
    stats.set_growth(40, 4096);

    assert_eq!(
        stats.snapshot(),
        BrokerStatsSnapshot {
            retained: 1,
            duplicates_identical: 1,
            duplicates_conflicting: 2,
            replayed: 3,
            gaps: 1,
            reconciled: 1,
            swept: 12,
            fenced: 1,
            messages_current: 40,
            bytes_current: 4096,
        }
    );

    // Gauges are replaced, never accumulated.
    stats.set_growth(2, 20);
    assert_eq!(stats.snapshot().messages_current, 2);
    assert_eq!(stats.snapshot().bytes_current, 20);

    assert_eq!(DuplicateKind::Identical.as_str(), "identical");
    assert_eq!(DuplicateKind::Conflicting.as_str(), "conflicting");
}

#[tokio::test]
async fn the_fake_broker_deduplicates_by_publication_id_like_the_real_one() {
    use crate::protocol::v1::publication::Publication;
    use test_support::FakeBroker;

    let broker = FakeBroker::new();
    let first = Publication::new("app", "chat:42", r#"{"n":1}"#);
    let retained = broker.retain(&first).await.unwrap();
    assert_eq!(retained, Retained { position: Cursor::new(1, 1), duplicate: None, relayed: false });

    // A retry: same id, same bytes.
    let retry = broker.retain(&first).await.unwrap();
    assert_eq!(retry.duplicate, Some(DuplicateKind::Identical));
    assert_eq!(retry.position, retained.position, "the existing position is returned");

    // A publisher bug: same id, different bytes. Not stored either.
    let conflict = Publication { json_payload: r#"{"n":2}"#.to_string(), ..first.clone() };
    assert_eq!(broker.retain(&conflict).await.unwrap().duplicate, Some(DuplicateKind::Conflicting));
    assert_eq!(broker.payloads("chat:42"), vec![r#"{"n":1}"#.to_string()]);

    let stats = broker.stats();
    assert_eq!(
        (stats.retained, stats.duplicates_identical, stats.duplicates_conflicting),
        (1, 1, 1)
    );

    // Oversized: refused without the relay, retained in full with it.
    let big = Publication::new("app", "chat:42", format!(r#"{{"html":"{}"}}"#, "x".repeat(9000)));
    assert_matches!(
        broker.retain(&big).await.unwrap_err(),
        BrokerError::Publication(crate::pubsub::PubSubError::PayloadTooLarge { .. })
    );
    broker.relay_enabled.store(true, std::sync::atomic::Ordering::Relaxed);
    let relayed = broker.retain(&big).await.unwrap();
    assert!(relayed.relayed, "an oversized retained publication is announced by reference");
    assert_eq!(broker.payloads("chat:42").len(), 2, "the broker row holds the full payload");
}
