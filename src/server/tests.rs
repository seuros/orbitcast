// `clippy::significant_drop_tightening` cannot tell these fixtures from lock
// guards. `RpcClient` owns rama's connection pool, whose slots are tokio
// semaphore permits (`#[clippy::has_significant_drop]`), and the lint walks
// field types transitively: `DispatchContext`, `SessionDispatch`,
// `Connections`, and `Backend` (through its weak driver registry) all reach
// one. The dispatch submodules share these fixtures, so one expectation here
// covers them too.
#![expect(
    clippy::significant_drop_tightening,
    reason = "the fixtures reach RpcClient's pool permits; none of them is a lock guard"
)]

use std::assert_matches;

use super::*;
use crate::metrics::LagReason;
use crate::protocol::v1::rpc::CLOSE_REASON_REMOTE;
use crate::pubsub::MemoryPubSub;
use crate::pubsub::test_support::{ScriptedPubSub, SubscribeAnswer};
use crate::transport::docking::Disembark;

/// Deadline for every frame a test waits on.
const WAIT: Duration = Duration::from_secs(5);

const CHAT_42: &str = r#"{"channel":"Orbitcast::StreamsChannel","stream_name":"chat:42"}"#;
const CHAT_43: &str = r#"{"channel":"Orbitcast::StreamsChannel","stream_name":"chat:43"}"#;

fn test_config() -> Config {
    let mut config = Config {
        docking: None,
        http: crate::config::HttpConfig::default(),
        shutdown_timeout: crate::config::DEFAULT_SHUTDOWN_TIMEOUT,
        database_url: None,
        listener_database_url: None,
        namespace: "test".to_string(),
        pubsub_ack_timeout: crate::pubsub::DEFAULT_ACK_TIMEOUT,
        pubsub_queue_capacity: crate::pubsub::DEFAULT_QUEUE_CAPACITY,
        overflow: crate::pubsub::OverflowConfig::default(),
        ping_interval: 3,
        presence_ttl: Duration::from_millis(crate::presence::DEFAULT_PRESENCE_TTL_MS),
        presence_refresh_interval: Duration::from_millis(
            crate::presence::DEFAULT_PRESENCE_REFRESH_INTERVAL_MS,
        ),
        broker: crate::broker::BrokerConfig::default(),
        recovery_secret: None,
        streams: crate::streams::StreamsConfig::default(),
        rpc: crate::config::RpcSettings::default(),
        ingress: crate::config::IngressSettings::default(),
        explicit: crate::config::ExplicitKeys::default(),
    };
    config.streams.public = true;
    config
}

/// A signed-only driver over a scripted backend that acknowledges everything.
type Driver = (Arc<Connections>, mpsc::Receiver<Outgoing>, Arc<ScriptedPubSub>);

fn driver() -> Driver {
    driver_with(&test_config())
}

fn driver_with(config: &Config) -> Driver {
    let (tx, rx) = mpsc::channel(64);
    let pubsub = ScriptedPubSub::new();
    let backend = Arc::new(Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn PubSub>));
    (Connections::new(backend, config, tx).expect("signed-only driver"), rx, pubsub)
}

/// Drain whatever is queued right now, decoded as JSON.
fn frames(rx: &mut mpsc::Receiver<Outgoing>) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    while let Ok(Outgoing::Cargo(cargo)) = rx.try_recv() {
        out.push(serde_json::from_slice(&cargo.data).expect("frame is JSON"));
    }
    out
}

/// Wait for the next data frame (closes are skipped), decoded as JSON.
async fn next_frame(rx: &mut mpsc::Receiver<Outgoing>) -> serde_json::Value {
    tokio::time::timeout(WAIT, async {
        loop {
            match rx.recv().await.expect("outbound queue open") {
                Outgoing::Cargo(cargo) => {
                    return serde_json::from_slice(&cargo.data).expect("frame is JSON");
                }
                Outgoing::Disembark(_) => {}
            }
        }
    })
    .await
    .expect("a frame within the deadline")
}

/// Wait for the next outbound message of any kind.
async fn next_outgoing(rx: &mut mpsc::Receiver<Outgoing>) -> Outgoing {
    tokio::time::timeout(WAIT, rx.recv()).await.expect("outbound within deadline").expect("open")
}

fn subscribe_frame(identifier: &str) -> Vec<u8> {
    format!(r#"{{"command":"subscribe","identifier":{identifier:?}}}"#).into_bytes()
}

fn unsubscribe_frame(identifier: &str) -> Vec<u8> {
    format!(r#"{{"command":"unsubscribe","identifier":{identifier:?}}}"#).into_bytes()
}

/// Wait until `predicate` holds, polling, or fail with `what`.
async fn wait_until(what: &str, predicate: impl Fn() -> bool + Send + Sync) {
    tokio::time::timeout(WAIT, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// Accept `conn_id` and consume its welcome.
async fn accept(connections: &Connections, rx: &mut mpsc::Receiver<Outgoing>, conn_id: u32) {
    connections.accept(conn_id, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;
    assert_eq!(next_frame(rx).await["type"], "welcome");
}

#[tokio::test]
async fn accept_sends_exactly_one_welcome() {
    let (connections, mut rx, _) = driver();
    connections.accept(1, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;

    let sent = frames(&mut rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["type"], "welcome");
    assert!(sent[0]["sid"].is_string());
    assert_eq!(connections.session_count(), 1);
    assert_eq!(connections.task_count(), 1, "every session owns a task and a mailbox");
}

#[tokio::test]
async fn subscribe_confirm_publish_unsubscribe_round_trip() {
    let (connections, mut rx, pubsub) = driver();
    accept(&connections, &mut rx, 1).await;

    connections.handle_frame(1, &subscribe_frame(CHAT_42)).await;

    let confirm = next_frame(&mut rx).await;
    assert_eq!(confirm["type"], "confirm_subscription");
    assert_eq!(confirm["identifier"], CHAT_42);
    assert_eq!(pubsub.commands(), vec![("LISTEN", "chat:42".to_string())]);

    // A publication is addressed with the subscriber's own identifier.
    connections.hub().deliver("chat:42", br#"{"body":"hi"}"#);
    let sent = next_frame(&mut rx).await;
    assert_eq!(sent["identifier"], CHAT_42);
    assert_eq!(sent["message"]["body"], "hi");

    connections.handle_frame(1, &unsubscribe_frame(CHAT_42)).await;
    wait_until("the backend to be released", || pubsub.commands().len() == 2).await;
    assert_eq!(pubsub.commands()[1], ("UNLISTEN", "chat:42".to_string()));
    connections.hub().deliver("chat:42", br#"{"body":"gone"}"#);
    assert!(frames(&mut rx).is_empty(), "unsubscribed member must not receive");
}

#[tokio::test]
async fn unsigned_channel_is_rejected() {
    let (connections, mut rx, _) = driver();
    accept(&connections, &mut rx, 1).await;

    connections
        .handle_frame(1, br#"{"command":"subscribe","identifier":"{\"channel\":\"Chat\"}"}"#)
        .await;

    assert_eq!(next_frame(&mut rx).await["type"], "reject_subscription");
}

#[tokio::test]
async fn the_recovery_channel_does_not_exist_unless_it_is_configured() {
    // Default configuration: no broker, no recovery key. The reserved
    // channel is then just another unknown channel - it is never a
    // half-configured surface a client can reach.
    let (connections, mut rx, _) = driver();
    assert!(connections.recovery.is_none());
    accept(&connections, &mut rx, 1).await;

    connections
        .handle_frame(
            1,
            br#"{"command":"subscribe",
                     "identifier":"{\"channel\":\"Orbitcast::RecoveryChannel\",\"token\":\"x\"}"}"#,
        )
        .await;

    assert_eq!(next_frame(&mut rx).await["type"], "reject_subscription");
}

#[tokio::test]
async fn publishes_are_retained_only_on_broker_streams() {
    let broker = crate::broker::test_support::FakeBroker::new();
    let backend = Backend::with_pubsub(ScriptedPubSub::unready()).with_broker(
        "test",
        Arc::clone(&broker) as Arc<dyn Broker>,
        BrokerConfig {
            enabled: true,
            streams: vec!["chat:".to_string()],
            ..BrokerConfig::default()
        },
    );

    backend.publish("chat:42", r#"{"body":"retained"}"#).await.expect("retained publish");
    assert_eq!(broker.payloads("chat:42"), vec![r#"{"body":"retained"}"#.to_string()]);

    // A stream outside the allowlist takes the ephemeral path unchanged -
    // which, with an unready pub/sub, fails rather than silently retaining
    // it somewhere else.
    assert_matches!(
        backend.publish("telemetry:cpu", r#"{"v":1}"#).await,
        Err(PubSubError::NotReady)
    );
    let retained = broker.payloads("telemetry:cpu");
    assert!(retained.is_empty(), "nothing is retained off the allowlist: {retained:?}");
}

#[tokio::test]
async fn a_publication_keeps_the_publishers_id_through_the_broker() {
    let broker = crate::broker::test_support::FakeBroker::new();
    let backend = Backend::with_pubsub(ScriptedPubSub::new()).with_broker(
        "test",
        Arc::clone(&broker) as Arc<dyn Broker>,
        BrokerConfig { enabled: true, ..BrokerConfig::default() },
    );

    let publication = Publication::new("test", "chat:42", r#"{"body":"once"}"#);
    let id = publication.id.clone();
    backend.publish_publication(publication.clone()).await.expect("retained");
    // A retry with the same id is a duplicate for the broker, not a second
    // message.
    backend.publish_publication(publication).await.expect("retained again");

    let retained = broker
        .fetch_after("chat:42", crate::protocol::v1::recovery::Cursor::new(1, 0), 8)
        .await
        .expect("fake store reads");
    assert_eq!(retained.len(), 1, "the broker deduplicated the retry");
    assert_eq!(retained[0].publication_id, id, "the id reaches the broker unchanged");
    assert_eq!(broker.stats().duplicates_identical, 1, "the retry is counted as identical");
}

#[tokio::test]
async fn a_retained_publication_must_fit_one_notification() {
    let broker = crate::broker::test_support::FakeBroker::new();
    let backend = Backend::with_pubsub(ScriptedPubSub::new()).with_broker(
        "test",
        Arc::clone(&broker) as Arc<dyn Broker>,
        BrokerConfig { enabled: true, ..BrokerConfig::default() },
    );

    let payload = format!(
        r#"{{"html":"{}"}}"#,
        "x".repeat(crate::protocol::v1::publication::MAX_NOTIFY_PAYLOAD_LEN)
    );
    assert_matches!(
        backend.publish("chat:42", &payload).await,
        Err(PubSubError::PayloadTooLarge { .. })
    );
    assert!(broker.payloads("chat:42").is_empty(), "a refused publication is never half-retained");
}

#[tokio::test]
async fn duplicate_subscribe_reconfirms_without_second_membership() {
    let (connections, mut rx, pubsub) = driver();
    accept(&connections, &mut rx, 1).await;

    connections.handle_frame(1, &subscribe_frame(CHAT_42)).await;
    connections.handle_frame(1, &subscribe_frame(CHAT_42)).await;

    assert_eq!(next_frame(&mut rx).await["type"], "confirm_subscription");
    assert_eq!(next_frame(&mut rx).await["type"], "confirm_subscription");
    assert_eq!(connections.hub().stream_member_count("chat:42"), 1);
    assert_eq!(pubsub.commands().len(), 1, "one LISTEN for one membership");
}

#[tokio::test]
async fn teardown_and_drain_use_the_same_path() {
    let (connections, mut rx, pubsub) = driver();
    for conn_id in [1, 2] {
        accept(&connections, &mut rx, conn_id).await;
        connections.handle_frame(conn_id, &subscribe_frame(CHAT_42)).await;
        assert_eq!(next_frame(&mut rx).await["type"], "confirm_subscription");
    }

    connections.teardown(1).await;
    assert_eq!(connections.session_count(), 1);
    assert_eq!(connections.task_count(), 1, "the torn-down session's task is gone");
    assert_eq!(connections.hub().stream_member_count("chat:42"), 1);

    connections.drain().await;
    assert_eq!(connections.session_count(), 0);
    assert_eq!(connections.task_count(), 0);
    assert_eq!(connections.hub().stream_count(), 0);
    assert_eq!(pubsub.commands().last(), Some(&("UNLISTEN", "chat:42".to_string())));
}

#[tokio::test]
async fn teardown_stops_the_session_task_and_refuses_late_results() {
    let (tx, mut rx) = mpsc::channel(64);
    let pubsub = ScriptedPubSub::new();
    let backend = Arc::new(Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn PubSub>));
    let connections = Connections::new(Arc::clone(&backend), &test_config(), tx).expect("driver");
    accept(&connections, &mut rx, 1).await;
    let gate = connections.gate(1).expect("live session");

    // Queue two subscribes, then tear the session down: whatever had not
    // started by then must never run, and whatever had must undo itself.
    connections.handle_frame(1, &subscribe_frame(CHAT_42)).await;
    connections.handle_frame(1, &subscribe_frame(CHAT_43)).await;
    connections.teardown(1).await;
    assert!(gate.is_closed(), "teardown closes the gate first");
    assert!(connections.gate(1).is_none());

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(connections.hub().stream_count(), 0);
    assert_eq!(backend.interest_count("chat:42"), 0);
    assert_eq!(backend.interest_count("chat:43"), 0);
    let commands = pubsub.commands();
    let listens = commands.iter().filter(|(verb, _)| *verb == "LISTEN").count();
    let unlistens = commands.iter().filter(|(verb, _)| *verb == "UNLISTEN").count();
    assert_eq!(listens, unlistens, "every LISTEN that ran was undone: {commands:?}");

    // The hub refuses to resurrect the session for anything still in flight.
    assert_matches!(
        connections.hub().ensure_subscription(1, CHAT_42),
        Err(HubError::UnknownSession(1))
    );
    assert_matches!(
        connections.hub().set_identifier(1, "user:7".to_string()),
        Err(HubError::UnknownSession(1))
    );
    assert_matches!(
        connections.hub().join(1, CHAT_42, "chat:42", async { true }).await,
        Err(HubError::UnknownSession(1))
    );
}

#[tokio::test]
async fn a_reused_connection_id_starts_from_a_clean_state() {
    let (connections, mut rx, pubsub) = driver();
    accept(&connections, &mut rx, 7).await;
    connections.handle_frame(7, &subscribe_frame(CHAT_42)).await;
    assert_eq!(next_frame(&mut rx).await["type"], "confirm_subscription");
    let first_gate = connections.gate(7).expect("live session");

    // The transport reuses the id without a teardown (a supervisor bug): the
    // stale membership is cleared and counted, the old task is closed, and
    // the backend interest it held is released.
    accept(&connections, &mut rx, 7).await;
    assert!(first_gate.is_closed(), "the older session under the reused id is closed");
    assert_ne!(connections.gate(7).expect("new session").generation(), first_gate.generation());
    assert_eq!(connections.hub().stream_count(), 0, "no stale forward-index entry survives");
    assert_eq!(pubsub.commands().last(), Some(&("UNLISTEN", "chat:42".to_string())));
    let scrape = connections.backend().render_metrics();
    assert!(
        scrape.contains("orbitcast_session_reuse_stale_entries_total 2"),
        "the stale membership and session are counted:\n{scrape}"
    );
}

#[tokio::test]
async fn deliveries_during_registration_are_buffered_behind_the_confirmation() {
    let (connections, mut rx, _) = driver();
    accept(&connections, &mut rx, 1).await;

    // Install the membership under an acquisition the test acknowledges,
    // exactly as the session task does, and publish while it is pending.
    let hub = Arc::clone(connections.hub());
    let (ack, acked) = tokio::sync::oneshot::channel::<bool>();
    let join = tokio::spawn({
        let hub = Arc::clone(&hub);
        async move { hub.join(1, CHAT_42, "chat:42", async { acked.await.expect("acked") }).await }
    });
    wait_until("the pending membership", || hub.stream_member_count("chat:42") == 1).await;
    hub.deliver("chat:42", br#"{"n":1}"#);
    hub.deliver("chat:42", br#"{"n":2}"#);
    assert!(frames(&mut rx).is_empty(), "nothing is delivered before the confirmation");

    ack.send(true).expect("join pending");
    join.await.expect("join task").expect("joined");
    hub.send(
        1,
        &actioncable::encode(&ServerMessage::ConfirmSubscription {
            identifier: CHAT_42.to_string(),
        }),
    )
    .await;
    assert_eq!(hub.activate(1, CHAT_42), 2);

    let sent = frames(&mut rx);
    let kinds: Vec<String> = sent
        .iter()
        .map(|frame| {
            frame["type"].as_str().map_or_else(|| frame["message"]["n"].to_string(), str::to_string)
        })
        .collect();
    assert_eq!(kinds, vec!["confirm_subscription", "1", "2"], "confirmation first, then in order");
}

#[tokio::test]
async fn a_pending_buffer_that_overflows_closes_the_connection() {
    let (connections, mut rx, _) = driver();
    accept(&connections, &mut rx, 1).await;

    let hub = Arc::clone(connections.hub());
    let join = tokio::spawn({
        let hub = Arc::clone(&hub);
        async move { hub.join(1, CHAT_42, "chat:42", std::future::pending()).await }
    });
    wait_until("the pending membership", || hub.stream_member_count("chat:42") == 1).await;

    for n in 0..=crate::hub::PENDING_BUFFER_FRAMES {
        hub.deliver("chat:42", format!(r#"{{"n":{n}}}"#).as_bytes());
    }

    let disconnect = next_frame(&mut rx).await;
    assert_eq!(disconnect["type"], "disconnect");
    assert_eq!(disconnect["reconnect"], true, "lag invites a reconnect");
    assert_matches!(
        next_outgoing(&mut rx).await,
        Outgoing::Disembark(Disembark { conn_id: 1, .. })
    );
    assert_eq!(connections.backend().metrics().socket_lag_disconnects(LagReason::PendingBuffer), 1);
    join.abort();
}

#[tokio::test]
async fn disconnect_all_sends_a_reconnectable_disconnect() {
    let (connections, mut rx, _) = driver();
    accept(&connections, &mut rx, 1).await;

    connections.disconnect_all(DISCONNECT_REASON_SERVER_RESTART, true);

    let sent = frames(&mut rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["type"], "disconnect");
    assert_eq!(sent[0]["reason"], DISCONNECT_REASON_SERVER_RESTART);
    assert_eq!(sent[0]["reconnect"], true);
}

// --- rate limits and unsupported commands --------------------------------

#[tokio::test]
async fn subscribe_and_action_commands_spend_separate_budgets() {
    let mut config = test_config();
    config.http.rate_subscribes_per_conn_per_sec = 1;
    config.http.rate_commands_per_conn_per_sec = 2;
    let (connections, mut rx, _) = driver_with(&config);
    accept(&connections, &mut rx, 1).await;

    // Two actions fit the action budget without touching the subscribe one.
    for _ in 0..2 {
        connections
            .handle_command(
                1,
                ClientCommand::Message { identifier: CHAT_42.into(), data: "{}".into() },
            )
            .await;
    }
    assert!(frames(&mut rx).is_empty(), "actions inside the budget close nothing");

    // One subscribe fits; the second trips the subscribe class.
    connections.handle_frame(1, &subscribe_frame(CHAT_42)).await;
    assert_eq!(next_frame(&mut rx).await["type"], "confirm_subscription");
    connections.handle_frame(1, &subscribe_frame(CHAT_42)).await;
    assert_eq!(next_frame(&mut rx).await["type"], "disconnect");

    let scrape = connections.backend().render_metrics();
    assert!(
        scrape.contains("orbitcast_commands_rate_limited_total{class=\"subscribe\"} 1"),
        "{scrape}"
    );
    assert!(
        scrape.contains("orbitcast_commands_rate_limited_total{class=\"action\"} 0"),
        "{scrape}"
    );
}

#[tokio::test]
async fn an_unsupported_perform_is_counted_in_signed_mode() {
    let (connections, mut rx, _) = driver();
    accept(&connections, &mut rx, 1).await;

    for _ in 0..3 {
        connections
            .handle_frame(
                1,
                br#"{"command":"message","identifier":"{}","data":"{\"action\":\"speak\"}"}"#,
            )
            .await;
    }
    wait_until("the performs to be counted", || {
        connections
            .backend()
            .render_metrics()
            .contains("orbitcast_unsupported_commands_total{command=\"message\"} 3")
    })
    .await;
    assert!(frames(&mut rx).is_empty(), "a perform is dropped, never answered");
    assert_eq!(connections.session_count(), 1, "and never closes the session");
}

// --- Rails-initiated control --------------------------------------------

/// Frames and closes drained from the raw outbound queue.
type Drained = (Vec<(u32, Value)>, Vec<(u32, u16, String)>);

/// Drain the raw outbound queue: `(conn_id, frame)` and disembark events.
fn outgoing(rx: &mut mpsc::Receiver<Outgoing>) -> Drained {
    let mut frames = Vec::new();
    let mut closes = Vec::new();
    while let Ok(message) = rx.try_recv() {
        match message {
            Outgoing::Cargo(cargo) => {
                frames.push((cargo.conn_id, serde_json::from_slice(&cargo.data).unwrap()));
            }
            Outgoing::Disembark(disembark) => {
                closes.push((disembark.conn_id, disembark.code, disembark.reason));
            }
        }
    }
    (frames, closes)
}

#[test]
fn the_control_deduper_is_bounded_and_expiring() {
    let mut deduper = ControlDeduper::new(2, Duration::from_mins(1));
    let t0 = Instant::now();

    assert!(deduper.first_sighting("a", t0));
    assert!(!deduper.first_sighting("a", t0), "second sighting is a duplicate");

    // Capacity eviction drops the oldest entry, so it can be seen again.
    assert!(deduper.first_sighting("b", t0));
    assert!(deduper.first_sighting("c", t0));
    assert!(deduper.first_sighting("a", t0), "evicted entries are forgotten");
    assert!(deduper.seen.len() <= 2 && deduper.order.len() <= 2, "the set stays bounded");

    // TTL expiry: after the window, the same id is fresh again.
    let mut deduper = ControlDeduper::new(8, Duration::from_millis(10));
    assert!(deduper.first_sighting("x", t0));
    assert!(!deduper.first_sighting("x", t0 + Duration::from_millis(9)));
    assert!(deduper.first_sighting("x", t0 + Duration::from_millis(10)));
}

#[test]
fn control_decoding_fails_closed() {
    let backend = Backend::with_pubsub(ScriptedPubSub::new()).with_namespace("test");

    assert_matches!(backend.decode_control("{not json"), Err(ControlRejection::Malformed(_)));
    assert_matches!(
        backend.decode_control(r#"{"version":1,"kind":"self_destruct","namespace":"test"}"#),
        Err(ControlRejection::Malformed(_))
    );

    let mut unlisted = ControlMessage::disconnect("test", "user:7", CLOSE_REASON_REMOTE, false);
    unlisted.reason = "vibes".to_string();
    assert_matches!(
        backend.decode_control(&serde_json::to_string(&unlisted).unwrap()),
        Err(ControlRejection::Invalid(ProtocolError::DisallowedValue { field: "reason", .. }))
    );

    let foreign = ControlMessage::disconnect("other", "user:7", CLOSE_REASON_REMOTE, false);
    assert_matches!(
        backend.decode_control(&serde_json::to_string(&foreign).unwrap()),
        Err(ControlRejection::ForeignNamespace { .. })
    );

    let ok = ControlMessage::disconnect("test", "user:7", CLOSE_REASON_REMOTE, true);
    let decoded = backend.decode_control(&serde_json::to_string(&ok).unwrap()).unwrap();
    assert_eq!(decoded, ok);
}

#[tokio::test]
async fn a_control_message_disconnects_only_the_matching_identity() {
    let (tx, mut rx) = mpsc::channel(64);
    let backend = Arc::new(Backend::with_pubsub(ScriptedPubSub::new()).with_namespace("test"));
    let connections = Connections::new(Arc::clone(&backend), &test_config(), tx).expect("driver");

    for conn_id in [1, 2, 3] {
        connections.accept(conn_id, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;
    }
    // Two devices of user:7, one of user:8 - the identity index the
    // Rails-dispatch `identify` path populates.
    connections.hub().set_identifier(1, "user:7".to_string()).expect("live");
    connections.hub().set_identifier(2, "user:8".to_string()).expect("live");
    connections.hub().set_identifier(3, "user:7".to_string()).expect("live");
    let _ = outgoing(&mut rx);

    let control = ControlMessage::disconnect("test", "user:7", CLOSE_REASON_REMOTE, false);
    let outcome = backend
        .apply_control(&backend.decode_control(&serde_json::to_string(&control).unwrap()).unwrap())
        .await;
    assert_eq!(outcome, ControlOutcome::Applied { sessions: 2 });

    let (frames, closes) = outgoing(&mut rx);

    // Each matching session gets the standard ActionCable disconnect frame
    // carrying the message's reason and reconnect policy...
    let mut told_ids: Vec<u32> = frames
        .iter()
        .filter(|(_, frame)| {
            frame["type"] == "disconnect"
                && frame["reason"] == CLOSE_REASON_REMOTE
                && frame["reconnect"] == false
        })
        .map(|(conn_id, _)| *conn_id)
        .collect();
    told_ids.sort_unstable();
    assert_eq!(told_ids, vec![1, 3], "user:8 is untouched");

    // ...followed by the socket close on the same ordered queue.
    let mut closed_ids: Vec<u32> = closes.iter().map(|(conn_id, _, _)| *conn_id).collect();
    closed_ids.sort_unstable();
    assert_eq!(closed_ids, vec![1, 3]);
    assert!(
        closes
            .iter()
            .all(|(_, code, reason)| { *code == CLOSE_NORMAL && reason == CLOSE_REASON_REMOTE })
    );

    // A duplicate control_id is consumed without acting again.
    assert_eq!(backend.apply_control(&control).await, ControlOutcome::Duplicate);
    let (frames, closes) = outgoing(&mut rx);
    assert!(frames.is_empty() && closes.is_empty());
}

#[tokio::test]
async fn a_control_message_for_an_unknown_identity_is_a_no_op() {
    let (tx, mut rx) = mpsc::channel(64);
    let backend = Arc::new(Backend::with_pubsub(ScriptedPubSub::new()).with_namespace("test"));
    let connections = Connections::new(Arc::clone(&backend), &test_config(), tx).expect("driver");
    connections.accept(1, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;
    let _ = outgoing(&mut rx);

    let control = ControlMessage::disconnect("test", "nobody:9", CLOSE_REASON_REMOTE, true);
    assert_eq!(
        backend.apply_control(&control).await,
        ControlOutcome::Applied { sessions: 0 },
        "another node may hold the sessions; this one has nothing to do"
    );
    let (frames, closes) = outgoing(&mut rx);
    assert!(frames.is_empty() && closes.is_empty());
    assert_eq!(connections.session_count(), 1, "unrelated sessions stay connected");
}

#[tokio::test]
async fn a_rejected_control_wire_changes_nothing() {
    let (tx, mut rx) = mpsc::channel(64);
    let backend = Arc::new(Backend::with_pubsub(ScriptedPubSub::new()).with_namespace("test"));
    let connections = Connections::new(Arc::clone(&backend), &test_config(), tx).expect("driver");
    connections.accept(1, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;
    connections.hub().set_identifier(1, "user:7".to_string()).expect("live");
    let _ = outgoing(&mut rx);

    // Foreign namespace, unknown kind, and garbage all fall out of
    // `handle_control_wire` before any session is touched.
    let foreign = ControlMessage::disconnect("other", "user:7", CLOSE_REASON_REMOTE, false);
    backend.handle_control_wire(&serde_json::to_string(&foreign).unwrap()).await;
    backend.handle_control_wire(r#"{"version":1,"kind":"self_destruct"}"#).await;
    backend.handle_control_wire("{not json").await;

    let (frames, closes) = outgoing(&mut rx);
    assert!(frames.is_empty() && closes.is_empty());
    assert_eq!(connections.session_count(), 1);
}

#[tokio::test]
async fn whitespace_and_garbage_frames_are_ignored() {
    let (connections, mut rx, _) = driver();
    accept(&connections, &mut rx, 1).await;

    connections.handle_frame(1, b"  \n\t ").await;
    connections.handle_frame(1, b"{not json").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let sent = frames(&mut rx);
    assert!(sent.is_empty(), "neither frame is answered: {sent:?}");
}

#[tokio::test]
async fn shutdown_signal_wakes_every_receiver() {
    let shutdown = Shutdown::new();
    let mut rx = shutdown.subscribe();
    assert!(!rx.is_triggered());

    // Triggering through a clone, with no receiver of its own, must still
    // latch the state (`send` alone would silently do nothing).
    shutdown.clone().trigger();
    assert!(shutdown.is_triggered());
    tokio::time::timeout(Duration::from_secs(1), rx.wait()).await.expect("signal observed");

    // A receiver created after the fact resolves immediately.
    let mut late = shutdown.subscribe();
    tokio::time::timeout(Duration::from_secs(1), late.wait()).await.expect("signal observed");
}

// --- backend interest, readiness, and gaps --------------------------------

#[tokio::test]
async fn backend_interest_is_refcounted_across_hubs() {
    let pubsub = Arc::new(MemoryPubSub::new());
    let backend = Arc::new(Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn PubSub>));

    assert!(backend.acquire("chat:42").await);
    assert!(backend.acquire("chat:42").await);
    assert!(pubsub.is_subscribed("chat:42"));
    assert_eq!(backend.interest_count("chat:42"), 2);

    // First release is a decrement only: another hub still holds interest.
    backend.release(vec!["chat:42".to_string()]).await;
    assert!(pubsub.is_subscribed("chat:42"));

    backend.release(vec!["chat:42".to_string()]).await;
    assert!(!pubsub.is_subscribed("chat:42"));
    assert_eq!(backend.gauges().streams_with_interest, 0, "an idle entry is pruned");
}

#[tokio::test]
async fn interest_entries_are_per_stream_not_one_global_lock() {
    // One stream's acquisition hangs; another stream's must still complete.
    let pubsub = ScriptedPubSub::new();
    let backend = Arc::new(Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn PubSub>));

    // Hold `chat:slow`'s entry lock from outside, as a stuck LISTEN would.
    let slow = Arc::clone(&backend.interest.entry("chat:slow".to_string()).or_default());
    let held = slow.lock().await;

    let fast = tokio::time::timeout(Duration::from_secs(1), backend.acquire("chat:fast")).await;
    assert_eq!(fast, Ok(true), "an unrelated stream never waits for another's LISTEN");
    drop(held);
}

#[tokio::test]
async fn a_timed_out_listen_is_followed_by_an_unlisten() {
    let pubsub = ScriptedPubSub::new();
    pubsub.answer_subscribes_with(SubscribeAnswer::AckTimeout);
    let backend = Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn PubSub>);

    assert!(!backend.acquire("chat:42").await, "a timed-out LISTEN is a refusal");
    assert_eq!(
        pubsub.commands(),
        vec![("LISTEN", "chat:42".to_string()), ("UNLISTEN", "chat:42".to_string())],
        "PostgreSQL may still execute the LISTEN, so it is undone"
    );
    assert_eq!(backend.interest_count("chat:42"), 0);
    assert_eq!(backend.gauges().streams_with_interest, 0);

    // A plain refusal (not a timeout) issues no UNLISTEN: nothing could have
    // been executed.
    pubsub.answer_subscribes_with(SubscribeAnswer::NotReady);
    assert!(!backend.acquire("chat:43").await);
    assert_eq!(pubsub.commands().last(), Some(&("LISTEN", "chat:43".to_string())));
}

#[tokio::test]
async fn readiness_reports_each_component() {
    let pubsub = ScriptedPubSub::new();
    let backend = Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn PubSub>);

    let ready = backend.readiness(false);
    assert_eq!(ready, Readiness { ready: true, pubsub: "ok", rails: None, draining: false });
    assert_eq!(
        serde_json::to_value(&ready).unwrap(),
        serde_json::json!({"ready": true, "pubsub": "ok"})
    );

    // A reconnecting listener is unready, by name.
    pubsub.set_ready(false);
    let reconnecting = backend.readiness(false);
    assert!(!reconnecting.ready);
    assert_eq!(reconnecting.pubsub, "reconnecting");
    pubsub.set_ready(true);

    // Dispatch mode: Rails must pass describe.
    backend.set_rails_status(RailsStatus::Probing);
    assert_eq!(backend.readiness(false).rails, Some("probing"));
    assert!(!backend.is_ready());
    backend.set_rails_status(RailsStatus::Refused(DescribeRefusal::UnsupportedVersion));
    let refused = backend.readiness(false);
    assert_eq!(
        serde_json::to_value(&refused).unwrap(),
        serde_json::json!({"ready": false, "pubsub": "ok", "rails": "unsupported_version"})
    );
    backend.set_rails_status(RailsStatus::Admitted);
    assert!(backend.is_ready());
    assert_eq!(backend.gauges().rails_ready, Some(true));

    // Draining is unready whatever the components say.
    let draining = backend.readiness(true);
    assert!(!draining.ready && draining.draining);
}

#[tokio::test]
async fn a_delivery_gap_disconnects_the_streams_local_members() {
    let (tx, mut rx) = mpsc::channel(64);
    let pubsub = ScriptedPubSub::new();
    let backend = Arc::new(Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn PubSub>));
    let connections = Connections::new(Arc::clone(&backend), &test_config(), tx).expect("driver");
    let listener = backend.spawn_listener().expect("listener starts once");

    for conn_id in [1, 2] {
        accept(&connections, &mut rx, conn_id).await;
    }
    connections.handle_frame(1, &subscribe_frame(CHAT_42)).await;
    assert_eq!(next_frame(&mut rx).await["type"], "confirm_subscription");

    // A publication still flows...
    pubsub.emit(PubSubEvent::Publication { stream: "chat:42".into(), payload: "{\"n\":1}".into() });
    assert_eq!(next_frame(&mut rx).await["message"]["n"], 1);

    // ...and a gap closes exactly the stream's members, with reconnect.
    pubsub.emit(PubSubEvent::Gap { stream: "chat:42".into(), reason: "row expired".into() });
    let (frames, closes) = tokio::time::timeout(WAIT, async {
        let mut all = (Vec::new(), Vec::new());
        loop {
            let (frames, closes) = outgoing(&mut rx);
            all.0.extend(frames);
            all.1.extend(closes);
            if !all.1.is_empty() {
                return all;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the gap close arrives");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].0, 1);
    assert_eq!(frames[0].1["type"], "disconnect");
    assert_eq!(frames[0].1["reconnect"], true);
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].0, 1, "connection 2 holds no membership and is untouched");
    assert!(
        connections.backend().render_metrics().contains("orbitcast_pubsub_gap_disconnects_total 1")
    );

    listener.abort();
}

#[tokio::test]
async fn backend_delivers_one_publication_to_every_registered_hub() {
    let pubsub = Arc::new(MemoryPubSub::new());
    let backend = Arc::new(Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn PubSub>));
    let config = test_config();

    let (tx_a, mut rx_a) = mpsc::channel(16);
    let (tx_b, mut rx_b) = mpsc::channel(16);
    let a = Connections::new(Arc::clone(&backend), &config, tx_a).expect("driver");
    let b = Connections::new(Arc::clone(&backend), &config, tx_b).expect("driver");

    for (connections, rx) in [(&a, &mut rx_a), (&b, &mut rx_b)] {
        accept(connections, rx, 1).await;
        connections.handle_frame(1, &subscribe_frame(CHAT_42)).await;
        assert_eq!(next_frame(rx).await["type"], "confirm_subscription");
    }

    let listener = backend.spawn_listener().expect("listener starts once");
    assert!(backend.spawn_listener().is_none(), "listener starts only once");

    pubsub.publish("chat:42", r#"{"body":"hi"}"#).await.unwrap();

    for rx in [&mut rx_a, &mut rx_b] {
        assert_eq!(
            next_frame(rx).await["message"]["body"],
            "hi",
            "every hub receives the publication"
        );
    }

    listener.abort();
}

mod dispatch;
