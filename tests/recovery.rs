//! End-to-end recovery tests.
//!
//! The real server - rama listener, real WebSocket handshake, real
//! `ActionCable` frames - with the in-process memory pub/sub backend and an
//! in-memory broker attached to it, so the whole path from `POST`-equivalent
//! publication through retention, the recovery channel, replay, and the live
//! tail runs the production code. The store is the only fake.
//!
//! Requires the `memory` feature, like `tests/websocket.rs`. The one thing a
//! memory store cannot show - history surviving a relay restart - is the
//! `#[ignore]` PostgreSQL test in `tests/recovery_postgres.rs`, built with
//! the default feature set.
#![cfg(feature = "memory")]
#![expect(
    clippy::significant_drop_tightening,
    reason = "TestServer is a fixture of Arcs and JoinHandles reaching pool permits, \
              not lock guards"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};

#[path = "support/ws.rs"]
mod ws;

use orbitcast::MemoryPubSub;
use orbitcast::broker::{
    Broker, BrokerConfig, BrokerError, BrokerStats, DuplicateKind, Retained, RetainedMessage,
    StreamRange,
};
use orbitcast::config::{Config, HttpConfig};
use orbitcast::protocol::Envelope as _;
use orbitcast::protocol::v1::publication::Publication;
use orbitcast::protocol::v1::recovery::{Cursor, RECOVERY_CHANNEL, RecoveryClaims};
use orbitcast::pubsub::PubSub as _;
use orbitcast::recovery::RecoveryVerifier;
use orbitcast::server::{Backend, Shutdown};
use orbitcast::streams::SecretBytes;
use orbitcast::transport::http::{HttpTransport, bind};
use rama::http::ws::handshake::client::ClientWebSocket;
use ws::{next_frame, send_command};

const NAMESPACE: &str = "orbitcast_test";
const SECRET: &[u8] = b"recovery-test-secret-32-bytes-000";

/// MOCKED: the retained store. REAL: everything that calls it - the server's
/// routed publish, the recovery controller, the tails.
///
/// Retains like the PostgreSQL broker (contiguous per-stream sequences,
/// dedup by id, per-session fencing, snapshots) and announces each retained
/// publication through the memory pub/sub so the node's own listener delivers
/// it and wakes the tails, exactly as `pg_notify` would.
struct MemoryBroker {
    pubsub: Arc<MemoryPubSub>,
    streams: Mutex<HashMap<String, (u64, Vec<RetainedMessage>)>>,
    sessions: Mutex<HashMap<String, (u64, Option<String>)>>,
    stats: BrokerStats,
}

impl MemoryBroker {
    fn new(pubsub: Arc<MemoryPubSub>) -> Arc<Self> {
        Arc::new(Self {
            pubsub,
            streams: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            stats: BrokerStats::default(),
        })
    }
}

#[async_trait]
impl Broker for MemoryBroker {
    async fn retain(&self, publication: &Publication) -> Result<Retained, BrokerError> {
        publication.validate().map_err(|e| BrokerError::Publication(e.into()))?;

        let position = {
            let mut streams = self.streams.lock().unwrap();
            let (next_seq, messages) =
                streams.entry(publication.stream.clone()).or_insert((1, Vec::new()));
            if let Some(existing) = messages.iter().find(|m| m.publication_id == publication.id) {
                let kind = if existing.json_payload == publication.json_payload {
                    DuplicateKind::Identical
                } else {
                    DuplicateKind::Conflicting
                };
                self.stats.record_duplicate(kind);
                return Ok(Retained {
                    position: existing.position,
                    duplicate: Some(kind),
                    relayed: false,
                });
            }
            let position = Cursor::new(1, *next_seq);
            *next_seq += 1;
            messages.push(RetainedMessage {
                stream: publication.stream.clone(),
                publication_id: publication.id.clone(),
                position,
                json_payload: publication.json_payload.clone(),
            });
            position
        };
        self.stats.record_retained();

        // The announcement: visible to this node's listener, like a commit.
        self.pubsub
            .publish_publication(publication)
            .await
            .map_err(|e| BrokerError::Backend(anyhow::anyhow!("announce: {e}")))?;
        Ok(Retained { position, duplicate: None, relayed: false })
    }

    async fn range(&self, stream: &str) -> Result<StreamRange, BrokerError> {
        Ok(self.streams.lock().unwrap().get(stream).map_or(
            StreamRange { watermark: Cursor::new(1, 0), oldest: None },
            |(next_seq, messages)| StreamRange {
                watermark: Cursor::new(1, next_seq - 1),
                oldest: messages.first().map(|m| m.position.sequence),
            },
        ))
    }

    async fn fetch_after(
        &self,
        stream: &str,
        after: Cursor,
        limit: usize,
    ) -> Result<Vec<RetainedMessage>, BrokerError> {
        Ok(self.streams.lock().unwrap().get(stream).map_or_else(Vec::new, |(_, messages)| {
            messages
                .iter()
                .filter(|m| {
                    m.position.generation == after.generation
                        && m.position.sequence > after.sequence
                })
                .take(limit)
                .cloned()
                .collect()
        }))
    }

    async fn fence(&self, session_id: &str) -> Result<u64, BrokerError> {
        let mut sessions = self.sessions.lock().unwrap();
        let entry = sessions.entry(session_id.to_string()).or_insert((0, None));
        entry.0 += 1;
        self.stats.record_fenced();
        Ok(entry.0)
    }

    async fn fencing_generation(&self, session_id: &str) -> Result<u64, BrokerError> {
        Ok(self.sessions.lock().unwrap().get(session_id).map_or(0, |(lease, _)| *lease))
    }

    async fn touch_session(&self, _session_id: &str) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn save_snapshot(&self, session_id: &str, snapshot: &str) -> Result<(), BrokerError> {
        self.sessions.lock().unwrap().entry(session_id.to_string()).or_insert((0, None)).1 =
            Some(snapshot.to_string());
        Ok(())
    }

    async fn load_snapshot(&self, session_id: &str) -> Result<Option<String>, BrokerError> {
        Ok(self.sessions.lock().unwrap().get(session_id).and_then(|(_, s)| s.clone()))
    }

    fn counters(&self) -> &BrokerStats {
        &self.stats
    }
}

struct TestServer {
    addr: std::net::SocketAddr,
    backend: Arc<Backend>,
    broker: Arc<MemoryBroker>,
    _transport: Arc<HttpTransport>,
    _shutdown: Shutdown,
    _listener_task: tokio::task::JoinHandle<()>,
    _serve_task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    fn ws_url(&self) -> String {
        format!("ws://{}/cable", self.addr)
    }

    /// Publish through the server's routed path: retained, announced,
    /// delivered - the id preserved end to end.
    async fn publish(&self, stream: &str, payload: &str) -> String {
        let publication = Publication::new(NAMESPACE, stream, payload);
        let id = publication.id.clone();
        self.backend.publish_publication(publication).await.expect("published");
        id
    }
}

fn broker_config() -> BrokerConfig {
    BrokerConfig {
        enabled: true,
        reconcile_interval: Duration::from_millis(25),
        ..BrokerConfig::default()
    }
}

/// Signed mode with public streams, plus the recovery key. The broker is
/// attached to the backend directly (a memory build cannot *configure* one,
/// which `Config::validate` would say; the server wiring does not care where
/// the store came from).
fn config() -> Config {
    let mut config = Config {
        docking: None,
        http: HttpConfig { listen_addr: Some("127.0.0.1:0".to_string()), ..Default::default() },
        shutdown_timeout: Duration::from_secs(5),
        database_url: None,
        listener_database_url: None,
        namespace: NAMESPACE.to_string(),
        pubsub_ack_timeout: Duration::from_secs(5),
        pubsub_queue_capacity: 64,
        overflow: orbitcast::pubsub::OverflowConfig::default(),
        ping_interval: 3,
        presence_ttl: Duration::from_millis(orbitcast::presence::DEFAULT_PRESENCE_TTL_MS),
        presence_refresh_interval: Duration::from_millis(
            orbitcast::presence::DEFAULT_PRESENCE_REFRESH_INTERVAL_MS,
        ),
        broker: broker_config(),
        recovery_secret: Some(SecretBytes::new(SECRET.to_vec())),
        streams: orbitcast::streams::StreamsConfig::default(),
        rpc: orbitcast::config::RpcSettings::default(),
        ingress: orbitcast::config::IngressSettings::default(),
        explicit: orbitcast::config::ExplicitKeys::default(),
    };
    config.streams.public = true;
    config.streams.public_insecure = true;
    config
}

async fn start() -> TestServer {
    let config = config();
    let pubsub = Arc::new(MemoryPubSub::new());
    let broker = MemoryBroker::new(Arc::clone(&pubsub));
    let backend = Arc::new(
        Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn orbitcast::pubsub::PubSub>)
            .with_broker(NAMESPACE, Arc::clone(&broker) as Arc<dyn Broker>, broker_config()),
    );
    let listener_task = backend.spawn_listener().expect("pub/sub listener starts");

    let shutdown = Shutdown::new();
    let transport = HttpTransport::new(Arc::clone(&backend), &config, shutdown.clone())
        .expect("transport builds");
    let listener = bind(config.http.listen_addr.as_deref().unwrap()).await.expect("bind");
    let addr = listener.local_addr();
    let serve_task = tokio::spawn(Arc::clone(&transport).serve(listener));

    TestServer {
        addr,
        backend,
        broker,
        _transport: transport,
        _shutdown: shutdown,
        _listener_task: listener_task,
        _serve_task: serve_task,
    }
}

fn token(session_id: &str, streams: &[&str], expires_at: i64) -> String {
    let claims = RecoveryClaims::recover(
        NAMESPACE,
        session_id,
        streams.iter().map(|s| (*s).to_string()).collect(),
        expires_at,
    );
    RecoveryVerifier::new(SecretBytes::new(SECRET.to_vec())).sign(&claims)
}

fn live_token(session_id: &str, streams: &[&str]) -> String {
    token(session_id, streams, orbitcast::protocol::v1::now_unix() + 600)
}

fn stream_identifier(stream: &str) -> String {
    json!({ "channel": "Orbitcast::StreamsChannel", "stream_name": stream }).to_string()
}

fn recovery_identifier(token: &str) -> String {
    json!({ "channel": RECOVERY_CHANNEL, "token": token }).to_string()
}

/// Connect, take the welcome, subscribe to `streams` (confirmed), and
/// subscribe to the recovery channel with `token`, returning the socket and
/// the recovery identifier. Application streams first, recovery last - the
/// order the relay requires.
async fn connect_recovering(
    server: &TestServer,
    streams: &[&str],
    token: &str,
) -> (ClientWebSocket, String) {
    let mut socket = ws::connect(server.ws_url()).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    for stream in streams {
        let id = stream_identifier(stream);
        send_command(&mut socket, json!({ "command": "subscribe", "identifier": id })).await;
        let confirm = next_frame(&mut socket).await;
        assert_eq!(confirm["type"], "confirm_subscription", "{confirm}");
    }

    let identifier = recovery_identifier(token);
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": identifier })).await;
    let confirm = next_frame(&mut socket).await;
    assert_eq!(confirm["type"], "confirm_subscription", "{confirm}");
    assert_eq!(confirm["identifier"], identifier);
    (socket, identifier)
}

/// `perform("orbitcast_recovery", {op: "resume", cursors})`, as the helper
/// sends it.
async fn resume(socket: &mut ClientWebSocket, identifier: &str, cursors: &BTreeMap<&str, Cursor>) {
    let cursors: BTreeMap<&str, String> =
        cursors.iter().map(|(stream, cursor)| (*stream, cursor.encode())).collect();
    let data =
        json!({ "action": "orbitcast_recovery", "version": 1, "op": "resume", "cursors": cursors });
    send_command(
        socket,
        json!({ "command": "message", "identifier": identifier, "data": data.to_string() }),
    )
    .await;
}

/// Frames addressed to the recovery subscription, up to and including the
/// first frame `done` accepts.
async fn recovery_frames_until(
    socket: &mut ClientWebSocket,
    identifier: &str,
    mut done: impl FnMut(&Value) -> bool,
) -> Vec<Value> {
    let mut frames = Vec::new();
    loop {
        let frame = next_frame(socket).await;
        if frame["identifier"] != identifier {
            continue; // an ordinary live delivery on an application identifier
        }
        let message = frame["message"].clone();
        let finished = done(&message);
        frames.push(message);
        if finished {
            return frames;
        }
    }
}

fn is_event(message: &Value, event: &str) -> bool {
    message["type"] == "orbitcast_recovery" && message["event"] == event
}

fn deliveries(frames: &[Value], stream: &str) -> Vec<(u64, String)> {
    frames
        .iter()
        .filter(|m| m["type"] == "orbitcast_delivery" && m["stream"] == stream)
        .map(|m| {
            (
                m["sequence"].as_str().unwrap().parse().unwrap(),
                m["publication_id"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn gaps(frames: &[Value]) -> Vec<(String, String)> {
    frames
        .iter()
        .filter(|m| is_event(m, "gap"))
        .map(|m| {
            (m["stream"].as_str().unwrap().to_string(), m["reason"].as_str().unwrap().to_string())
        })
        .collect()
}

#[tokio::test]
async fn replay_hands_off_to_live_delivery_with_no_missing_sequence() {
    let server = start().await;
    let stream = "chat:handoff";
    for n in 1..=5 {
        server.publish(stream, &format!(r#"{{"n":{n}}}"#)).await;
    }

    let (mut socket, identifier) =
        connect_recovering(&server, &[stream], &live_token("session:1", &[stream])).await;

    // Publish *while* the resume is in flight: the tail buffers these behind
    // the replay and releases them after it, deduplicated.
    let writer = tokio::spawn({
        let backend = Arc::clone(&server.backend);
        async move {
            for n in 6..=12 {
                backend
                    .publish_publication(Publication::new(
                        NAMESPACE,
                        "chat:handoff",
                        format!(r#"{{"n":{n}}}"#),
                    ))
                    .await
                    .expect("published");
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
        }
    });
    resume(&mut socket, &identifier, &BTreeMap::from([(stream, Cursor::new(1, 2))])).await;
    writer.await.unwrap();

    let frames = recovery_frames_until(&mut socket, &identifier, |m| {
        m["type"] == "orbitcast_delivery" && m["sequence"] == "12"
    })
    .await;

    let delivered = deliveries(&frames, stream);
    let sequences: Vec<u64> = delivered.iter().map(|(s, _)| *s).collect();
    assert_eq!(sequences, (3..=12).collect::<Vec<u64>>(), "replay then live, contiguous");
    let ids: HashSet<&String> = delivered.iter().map(|(_, id)| id).collect();
    assert_eq!(ids.len(), delivered.len(), "no publication delivered twice");
    assert!(gaps(&frames).is_empty(), "a clean handoff has no gaps: {frames:?}");
    assert!(frames.iter().any(|m| is_event(m, "replay_start")));
    assert!(frames.iter().any(|m| is_event(m, "replay_done")));

    // At least 3, 4 and 5 came from history; whatever the concurrent writer
    // committed before the watermark was captured was replayed too, the rest
    // arrived live through the buffered tail. The contiguous sequence above
    // is the gate; the counter only has to agree with it.
    let stats = server.broker.stats();
    assert!((3..=10).contains(&stats.replayed), "replayed {}", stats.replayed);
    assert_eq!(stats.gaps, 0);
}

#[tokio::test]
async fn duplicate_publications_keep_one_stable_id_end_to_end() {
    let server = start().await;
    let stream = "chat:dup";

    // A publisher retry: same id, same payload, published twice through the
    // routed path. The broker deduplicates it; the id is the one the
    // publisher chose.
    let publication = Publication::new(NAMESPACE, stream, r#"{"body":"once"}"#);
    let id = publication.id.clone();
    server.backend.publish_publication(publication.clone()).await.expect("published");
    server.backend.publish_publication(publication).await.expect("retried");
    assert_eq!(server.broker.stats().duplicates_identical, 1);

    let (mut socket, identifier) =
        connect_recovering(&server, &[stream], &live_token("session:2", &[stream])).await;
    // Two resumes from the same cursor: the second replays the same row and
    // the session's dedup window drops it.
    for _ in 0..2 {
        resume(&mut socket, &identifier, &BTreeMap::from([(stream, Cursor::new(1, 0))])).await;
    }
    let mut done = 0;
    let frames = recovery_frames_until(&mut socket, &identifier, |m| {
        if is_event(m, "replay_done") {
            done += 1;
        }
        done == 2
    })
    .await;

    let delivered = deliveries(&frames, stream);
    assert_eq!(delivered, vec![(1, id)], "one delivery, carrying the publisher's id");
}

#[tokio::test]
async fn a_stale_token_is_rejected_and_an_expiring_one_stops_replaying() {
    let server = start().await;
    let stream = "chat:stale";
    server.publish(stream, r#"{"n":1}"#).await;

    // Already expired: the subscription itself is rejected.
    let mut socket = ws::connect(server.ws_url()).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");
    let expired = recovery_identifier(&token("session:3", &[stream], 1));
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": expired })).await;
    let reject = next_frame(&mut socket).await;
    assert_eq!(reject["type"], "reject_subscription");
    assert_eq!(reject["identifier"], expired);

    // Valid at subscribe time, expired by resume time: every stream is
    // refused as unauthorized, nothing is replayed, no lease is taken.
    let short = token("session:3b", &[stream], orbitcast::protocol::v1::now_unix() + 1);
    let (mut socket, identifier) = connect_recovering(&server, &[stream], &short).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    resume(&mut socket, &identifier, &BTreeMap::from([(stream, Cursor::new(1, 0))])).await;
    let frames = recovery_frames_until(&mut socket, &identifier, |m| is_event(m, "gap")).await;
    assert_eq!(gaps(&frames), vec![(stream.to_string(), "unauthorized".to_string())]);
    assert!(deliveries(&frames, stream).is_empty(), "nothing replayed: {frames:?}");
    assert_eq!(server.broker.fencing_generation("session:3b").await.unwrap(), 0, "no lease");
}

#[tokio::test]
async fn a_stream_the_connection_does_not_subscribe_to_is_not_replayed() {
    let server = start().await;
    server.publish("chat:a", r#"{"n":1}"#).await;
    server.publish("chat:b", r#"{"n":1}"#).await;

    // The token grants both; the socket subscribes to one.
    let (mut socket, identifier) =
        connect_recovering(&server, &["chat:a"], &live_token("session:4", &["chat:a", "chat:b"]))
            .await;
    resume(
        &mut socket,
        &identifier,
        &BTreeMap::from([("chat:a", Cursor::new(1, 0)), ("chat:b", Cursor::new(1, 0))]),
    )
    .await;
    let frames =
        recovery_frames_until(&mut socket, &identifier, |m| is_event(m, "replay_done")).await;

    assert_eq!(gaps(&frames), vec![("chat:b".to_string(), "unauthorized".to_string())]);
    assert_eq!(deliveries(&frames, "chat:a").len(), 1);
    assert!(deliveries(&frames, "chat:b").is_empty(), "the unsubscribed stream is never replayed");
}

#[tokio::test]
async fn concurrent_restores_of_one_session_leave_exactly_one_winner() {
    let server = start().await;
    let stream = "chat:fence";
    server.publish(stream, r#"{"n":1}"#).await;

    // Two sockets, two *different* tokens for the same session: fencing is
    // per session, so the later resume wins whichever token it carries.
    let first = live_token("session:5", &[stream]);
    let second = token("session:5", &[stream], orbitcast::protocol::v1::now_unix() + 700);
    assert_ne!(first, second);
    let (mut socket_a, id_a) = connect_recovering(&server, &[stream], &first).await;
    let (mut socket_b, id_b) = connect_recovering(&server, &[stream], &second).await;

    resume(&mut socket_a, &id_a, &BTreeMap::from([(stream, Cursor::new(1, 0))])).await;
    let _ = recovery_frames_until(&mut socket_a, &id_a, |m| is_event(m, "replay_done")).await;
    resume(&mut socket_b, &id_b, &BTreeMap::from([(stream, Cursor::new(1, 0))])).await;
    let _ = recovery_frames_until(&mut socket_b, &id_b, |m| is_event(m, "replay_done")).await;

    // The loser learns it within a reconcile interval; the winner keeps
    // delivering.
    let lost = recovery_frames_until(&mut socket_a, &id_a, |m| is_event(m, "gap")).await;
    assert_eq!(gaps(&lost), vec![(stream.to_string(), "fenced".to_string())]);

    server.publish(stream, r#"{"n":2}"#).await;
    let live = recovery_frames_until(&mut socket_b, &id_b, |m| {
        m["type"] == "orbitcast_delivery" && m["sequence"] == "2"
    })
    .await;
    assert!(gaps(&live).is_empty(), "the winner sees no gap: {live:?}");
    assert_eq!(server.broker.stats().fenced, 2);
}

#[tokio::test]
async fn a_resume_without_cursors_continues_from_the_last_acknowledgement() {
    let server = start().await;
    let stream = "chat:memory";
    for n in 1..=3 {
        server.publish(stream, &format!(r#"{{"n":{n}}}"#)).await;
    }

    // First life: resume from the start, ack up to 2, go away.
    let (mut socket, identifier) =
        connect_recovering(&server, &[stream], &live_token("session:6", &[stream])).await;
    resume(&mut socket, &identifier, &BTreeMap::from([(stream, Cursor::new(1, 0))])).await;
    let _ = recovery_frames_until(&mut socket, &identifier, |m| is_event(m, "replay_done")).await;
    let ack = json!({
        "action": "orbitcast_recovery", "version": 1, "op": "ack",
        "cursors": { stream: Cursor::new(1, 2).encode() }
    });
    send_command(
        &mut socket,
        json!({ "command": "message", "identifier": identifier, "data": ack.to_string() }),
    )
    .await;
    // An ack answers nothing; give it time to land, then drop the socket.
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(socket);

    // Second life, no cursors persisted: the relay remembers.
    let (mut socket, identifier) =
        connect_recovering(&server, &[stream], &live_token("session:6", &[stream])).await;
    resume(&mut socket, &identifier, &BTreeMap::new()).await;
    let frames =
        recovery_frames_until(&mut socket, &identifier, |m| is_event(m, "replay_done")).await;
    let sequences: Vec<u64> = deliveries(&frames, stream).iter().map(|(s, _)| *s).collect();
    assert_eq!(sequences, vec![3], "replay continues after the acknowledged position");
}
