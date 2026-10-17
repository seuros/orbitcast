//! End-to-end tests for distributed presence in Rails-dispatch mode.
//!
//! A stub Rails endpoint that grants presence from `subscribed`, the real rama
//! listener, real WebSocket frames, the real in-process pub/sub, and the
//! in-process presence store (`MemoryPresence`): the only thing faked is the
//! Ruby on the other end of the HTTP/JSON contract. What these assert is the
//! client-visible contract - which frames a stock ActionCable client sees for
//! two tabs, a forged identity, a refused operation, and a closed socket.
//!
//! Requires the `memory` feature, like `tests/rpc.rs`: the default
//! (PostgreSQL) build has no in-process store to run against.
#![cfg(feature = "memory")]
#![expect(
    clippy::significant_drop_tightening,
    reason = "TestServer is a fixture of Arcs/JoinHandles reaching pool permits, not a lock guard"
)]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rama::http::server::HttpServer;
use rama::http::ws::Message;
use rama::http::ws::handshake::client::ClientWebSocket;
use rama::http::{Body, BodyExtractExt as _, Request, Response, StatusCode};
use rama::rt::Executor;
use rama::tcp::server::TcpListener;
use rama::{Service, http::service::web::response::IntoResponse};
use serde_json::{Value, json};

#[path = "support/ws.rs"]
mod ws;

use orbitcast::MemoryPubSub;
use orbitcast::config::{Config, HttpConfig, RpcSettings};
use orbitcast::presence::PresencePublisher;
use orbitcast::presence::memory::MemoryPresence;
use orbitcast::protocol::v1::rpc::{DescribeInfo, Operation, RpcReply, RpcRequest};
use orbitcast::rpc_client::REQUIRED_CAPABILITIES;
use orbitcast::server::{Backend, Shutdown};
use orbitcast::streams::SecretBytes;
use orbitcast::transport::http::{HttpTransport, bind};
use ws::{next_frame, send_command};

const TOKEN: &[u8] = b"rails-service-token";
const NAMESPACE: &str = "orbitcast_test";

/// Two tabs of one subject.
const ROOM_AS_USER_7: &str = r#"{"channel":"RoomChannel","room":"42"}"#;
/// The same room as another subject, granted `join` and `snapshot` only.
const ROOM_AS_USER_8: &str = r#"{"channel":"RoomChannel","room":"42","as":"8"}"#;
/// A subscription that streams but was granted nothing.
const UNGRANTED: &str = r#"{"channel":"RoomChannel","room":"43"}"#;

// --- the stub Rails endpoint ------------------------------------------------

/// What the stub's `subscribed` does for each identifier: the effects a real
/// channel would record with `stream_from` and `grant_presence`.
fn subscribed_effects(identifier: &str) -> Vec<Value> {
    let add_stream =
        |stream: &str| json!({ "kind": "add_stream", "identifier": identifier, "stream": stream });
    let grant = |subject: &str, operations: &[&str]| {
        json!({
            "kind": "presence_grant",
            "identifier": identifier,
            "stream": "chat:42",
            "subject_id": subject,
            "metadata": { "name": format!("User {}", &subject[5..]) },
            "ttl_ms": 45_000,
            "operations": operations,
        })
    };
    match identifier {
        ROOM_AS_USER_7 => {
            vec![add_stream("chat:42"), grant("user:7", &["join", "leave", "snapshot"])]
        }
        ROOM_AS_USER_8 => vec![add_stream("chat:42"), grant("user:8", &["join", "snapshot"])],
        UNGRANTED => vec![add_stream("chat:43")],
        _ => Vec::new(),
    }
}

#[derive(Default)]
struct Journal {
    actions: Mutex<Vec<String>>,
}

#[derive(Clone)]
struct RailsStub {
    journal: Arc<Journal>,
}

impl Service<Request> for RailsStub {
    type Output = Response;
    type Error = Infallible;

    async fn serve(&self, request: Request) -> Result<Self::Output, Self::Error> {
        let authorized = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == format!("Bearer {}", String::from_utf8_lossy(TOKEN)));
        if !authorized {
            return Ok((StatusCode::UNAUTHORIZED, "bad credential").into_response());
        }

        let body = request.into_body().try_into_string().await.unwrap_or_default();
        let decoded: RpcRequest = match serde_json::from_str(&body) {
            Ok(decoded) => decoded,
            Err(_) => return Ok((StatusCode::BAD_REQUEST, "not an rpc request").into_response()),
        };

        let mut reply = serde_json::to_value(RpcReply::accepted(&decoded)).expect("reply");
        match decoded.operation {
            Operation::Describe => {
                reply["describe"] = serde_json::to_value(DescribeInfo {
                    supported_versions: vec![1],
                    software_version: "stub".to_string(),
                    capabilities: REQUIRED_CAPABILITIES.iter().map(|c| (*c).to_string()).collect(),
                    ready: true,
                })
                .expect("describe");
            }
            Operation::Identify => {
                reply["identity"] = json!({ "identity_key": "user:7" });
            }
            Operation::OpenChannel => {
                let identifier =
                    decoded.subscription.as_ref().map(|s| s.identifier.clone()).unwrap_or_default();
                reply["effects"] = Value::Array(subscribed_effects(&identifier));
            }
            Operation::RunAction => {
                // Presence never gets here; record anything that does.
                let data = decoded.action.as_ref().map(|a| a.json_data.clone()).unwrap_or_default();
                self.journal.actions.lock().expect("journal").push(data);
            }
            _ => {}
        }

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(reply.to_string()))
            .expect("stub response"))
    }
}

async fn start_rails() -> (SocketAddr, Arc<Journal>) {
    let journal = Arc::new(Journal::default());
    let stub = RailsStub { journal: Arc::clone(&journal) };
    let listener = TcpListener::build(Executor::default())
        .bind_address("127.0.0.1:0")
        .await
        .expect("stub binds");
    let addr = listener.local_addr().expect("stub address");
    tokio::spawn(async move {
        listener.serve(HttpServer::auto(Executor::default()).service(stub)).await;
    });
    (addr, journal)
}

// --- the server under test --------------------------------------------------

struct TestServer {
    addr: SocketAddr,
    _transport: Arc<HttpTransport>,
    _listener_task: tokio::task::JoinHandle<()>,
    _serve_task: tokio::task::JoinHandle<()>,
}

fn dispatch_config(rails: SocketAddr) -> Config {
    Config {
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
        broker: orbitcast::broker::BrokerConfig::default(),
        recovery_secret: None,
        streams: orbitcast::streams::StreamsConfig::default(),
        rpc: RpcSettings {
            endpoint_url: Some(format!("http://{rails}/_orbitcast/v1/rails")),
            token: Some(SecretBytes::new(TOKEN.to_vec())),
            deadline: Duration::from_secs(2),
            node_id: "node-test".to_string(),
            ..RpcSettings::default()
        },
        ingress: orbitcast::config::IngressSettings::default(),
        explicit: orbitcast::config::ExplicitKeys::default(),
    }
}

async fn start(rails: SocketAddr) -> TestServer {
    let config = dispatch_config(rails);
    config.validate().expect("test config is valid");

    // The store announces through the same in-process pub/sub the hub
    // listens on, exactly as `Backend::init` wires a memory build.
    let pubsub = Arc::new(MemoryPubSub::new());
    let publisher = Arc::clone(&pubsub) as Arc<dyn PresencePublisher>;
    let presence = MemoryPresence::start(NAMESPACE, publisher, Duration::from_secs(5));
    let backend = Arc::new(
        Backend::with_pubsub(pubsub as Arc<dyn orbitcast::pubsub::PubSub>)
            .with_namespace(NAMESPACE)
            .with_presence(presence),
    );
    let listener_task = backend.spawn_listener().expect("pub/sub listener starts");

    let transport = HttpTransport::new(Arc::clone(&backend), &config, Shutdown::new())
        .expect("transport builds");
    let listener = bind(config.http.listen_addr.as_deref().unwrap()).await.expect("bind");
    let addr = listener.local_addr();
    let serve_task = tokio::spawn(Arc::clone(&transport).serve(listener));

    TestServer {
        addr,
        _transport: transport,
        _listener_task: listener_task,
        _serve_task: serve_task,
    }
}

/// Connect, wait for the welcome, subscribe, and wait for the confirmation.
async fn subscribed_tab(server: &TestServer, identifier: &str) -> ClientWebSocket {
    let mut socket = ws::connect(format!("ws://{}/cable", server.addr)).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": identifier })).await;
    let confirmed = next_frame(&mut socket).await;
    assert_eq!(confirmed["type"], "confirm_subscription", "{confirmed}");
    socket
}

/// The ordinary `perform` a stock client sends for a presence operation.
async fn perform(socket: &mut ClientWebSocket, identifier: &str, data: Value) {
    send_command(
        socket,
        json!({ "command": "message", "identifier": identifier, "data": data.to_string() }),
    )
    .await;
}

/// The next presence message on `socket`, as the `message` body.
async fn next_presence(socket: &mut ClientWebSocket) -> Value {
    let frame = next_frame(socket).await;
    assert_eq!(frame["message"]["type"], "presence", "{frame}");
    frame["message"].clone()
}

/// Assert that nothing but heartbeats arrives within `window`.
async fn assert_silent(socket: &mut ClientWebSocket, window: Duration) {
    let deadline = tokio::time::Instant::now() + window;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        match tokio::time::timeout(remaining, socket.recv_message()).await {
            Err(_) => return,
            Ok(Ok(Message::Text(text))) => {
                let frame: Value = serde_json::from_str(&text).expect("frame is JSON");
                assert_eq!(
                    frame["type"], "ping",
                    "unexpected frame while silence was expected: {frame}"
                );
            }
            Ok(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Ok(other) => panic!("unexpected websocket message: {other:?}"),
        }
    }
}

// --- the gate ----------------------------------------------------------------

#[tokio::test]
async fn two_tabs_make_one_join_identity_is_grant_only_and_the_last_socket_is_the_leave() {
    let (rails, journal) = start_rails().await;
    let server = start(rails).await;

    let mut tab_a = subscribed_tab(&server, ROOM_AS_USER_7).await;
    let mut tab_b = subscribed_tab(&server, ROOM_AS_USER_7).await;
    let mut other = subscribed_tab(&server, ROOM_AS_USER_8).await;

    // First tab joins: one subject-level join, delivered to every granted
    // subscription of the room (including the joining tab's own).
    perform(&mut tab_a, ROOM_AS_USER_7, json!({ "action": "orbitcast_presence", "op": "join" }))
        .await;
    for socket in [&mut tab_a, &mut tab_b, &mut other] {
        let joined = next_presence(socket).await;
        assert_eq!(joined["event"], "join", "{joined}");
        assert_eq!(joined["stream"], "chat:42");
        assert_eq!(joined["subject_id"], "user:7");
        assert_eq!(joined["metadata"]["name"], "User 7", "metadata comes from the grant");
        assert_eq!(joined["total"], 1);
        assert_eq!(joined["version"], 1);
    }

    // Second tab of the same subject, with a forged identity on top: no
    // second join, and nothing the client sent is in the roster.
    perform(
        &mut tab_b,
        ROOM_AS_USER_7,
        json!({
            "action": "orbitcast_presence", "op": "join",
            "subject_id": "user:999", "metadata": { "role": "admin" }, "stream": "chat:other"
        }),
    )
    .await;
    assert_silent(&mut other, Duration::from_millis(300)).await;

    perform(
        &mut other,
        ROOM_AS_USER_8,
        json!({ "action": "orbitcast_presence", "op": "snapshot" }),
    )
    .await;
    let page = next_presence(&mut other).await;
    assert_eq!(page["event"], "snapshot", "{page}");
    assert_eq!(page["total"], 1, "two tabs are one subject");
    assert_eq!(
        page["members"],
        json!([{ "subject_id": "user:7", "metadata": { "name": "User 7" } }])
    );
    assert_eq!(page["page"], json!({ "limit": 50, "offset": 0, "has_more": false }));

    // A refused operation is answered on the asking subscription, nothing
    // else happens, and the connection stays open.
    perform(&mut other, ROOM_AS_USER_8, json!({ "action": "orbitcast_presence", "op": "leave" }))
        .await;
    let refused = next_presence(&mut other).await;
    assert_eq!(refused["event"], "error", "{refused}");
    assert_eq!(refused["reason"], "operation_not_allowed");
    assert_eq!(refused["op"], "leave");
    assert_eq!(refused["stream"], "chat:42");

    let mut ungranted = subscribed_tab(&server, UNGRANTED).await;
    perform(&mut ungranted, UNGRANTED, json!({ "action": "orbitcast_presence", "op": "join" }))
        .await;
    let refused = next_presence(&mut ungranted).await;
    assert_eq!(refused["reason"], "not_granted", "{refused}");
    assert_eq!(refused["op"], "join");
    assert!(refused.get("stream").is_none(), "no roster was resolved: {refused}");

    // One tab leaving is silent; the last tab's socket closing is the leave.
    perform(&mut tab_b, ROOM_AS_USER_7, json!({ "action": "orbitcast_presence", "op": "leave" }))
        .await;
    assert_silent(&mut other, Duration::from_millis(300)).await;

    drop(tab_a);
    for socket in [&mut tab_b, &mut other] {
        let left = next_presence(socket).await;
        assert_eq!(left["event"], "leave", "{left}");
        assert_eq!(left["subject_id"], "user:7");
        assert_eq!(left["total"], 0);
        assert!(left.get("metadata").is_none());
    }

    // Presence was answered by the relay: Rails never saw an action.
    assert!(
        journal.actions.lock().expect("journal").is_empty(),
        "no presence action reached Rails"
    );
}
