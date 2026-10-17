//! End-to-end tests for Rails-dispatch mode.
//!
//! A stub Rails endpoint, the real rama listener, real WebSocket frames, and
//! the real in-process pub/sub backend: the only thing these tests fake is the
//! Ruby on the other end of the HTTP/JSON contract, and they assert the exact
//! shape of what it receives.
//!
//! Requires the `memory` feature, like `tests/websocket.rs`: the default
//! (PostgreSQL) build has no backend that can be published into.
#![cfg(feature = "memory")]
// Every binding clippy::significant_drop_tightening flags in this file is a
// fixture of Arcs and JoinHandles, never a lock guard: the lint walks field
// types transitively and reaches `RpcClient`'s rama connection pool (or a rama
// HTTP client's own pool), whose slots are tokio semaphore permits marked
// `#[clippy::has_significant_drop]`.
#![expect(
    clippy::significant_drop_tightening,
    reason = "TestServer is a fixture of Arcs/JoinHandles reaching pool permits, not a lock guard"
)]

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use orbitcast::protocol::Envelope as _;
use orbitcast::protocol::v1::rpc::{DescribeInfo, Operation, Outcome, RpcReply, RpcRequest};
use orbitcast::pubsub::PubSub as _;
use orbitcast::rpc_client::{REQUIRED_CAPABILITIES, RpcClient, RpcConfig, RpcError};
use orbitcast::server::{Backend, Shutdown};
use orbitcast::streams::SecretBytes;
use orbitcast::transport::http::{HttpTransport, bind};
use ws::{next_frame, send_command};

/// The service credential the stub expects.
const TOKEN: &[u8] = b"rails-service-token";

const IDENTIFIER: &str = r#"{"channel":"ChatChannel","room":"42"}"#;

// --- the stub Rails endpoint ------------------------------------------------

/// How the stub answers one operation.
#[derive(Clone)]
enum Answer {
    /// Accept with these effects (rendered from the request).
    Accept(Vec<Value>),
    /// Refuse.
    Deny,
    /// Wait, then accept with these effects.
    Slow(Duration, Vec<Value>),
    /// Wait on the *first* request only, then accept immediately: one slow
    /// Rails operation, not a slow endpoint.
    SlowFirst(Duration, Vec<Value>),
    /// Answer with a `fault`: Rails could not run the operation.
    Fault,
    /// Answer `describe` with exactly these capabilities (the default answer
    /// advertises every required one).
    Describe(Vec<&'static str>),
    /// Answer with this pinned reply, with the request's correlation fields
    /// substituted in.
    Pinned(Value),
}

/// One request the stub saw.
#[derive(Clone, Debug)]
struct Seen {
    path: String,
    authorization: String,
    body: Value,
}

#[derive(Default)]
struct Journal {
    seen: Mutex<Vec<Seen>>,
    unauthorized: AtomicUsize,
    /// Requests answered per operation, for `Answer::SlowFirst`.
    answered: Mutex<HashMap<String, usize>>,
}

impl Journal {
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("journal").clone()
    }

    fn requests_for(&self, operation: &str) -> Vec<Seen> {
        self.seen()
            .into_iter()
            .filter(|seen| seen.path.ends_with(&format!("/{operation}")))
            .collect()
    }
}

#[derive(Clone)]
struct RailsStub {
    answers: HashMap<String, Answer>,
    journal: Arc<Journal>,
}

impl Service<Request> for RailsStub {
    type Output = Response;
    type Error = Infallible;

    async fn serve(&self, request: Request) -> Result<Self::Output, Self::Error> {
        let path = request.uri().path_or_root().into_owned();
        let authorization = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();

        let body = request.into_body().try_into_string().await.unwrap_or_default();
        let decoded: RpcRequest = match serde_json::from_str(&body) {
            Ok(decoded) => decoded,
            Err(_) => return Ok((StatusCode::BAD_REQUEST, "not an rpc request").into_response()),
        };

        self.journal.seen.lock().expect("journal").push(Seen {
            path: path.clone(),
            authorization: authorization.clone(),
            body: serde_json::from_str(&body).expect("request is JSON"),
        });

        // Service authentication is the endpoint's own job, not the caller's.
        if authorization != format!("Bearer {}", String::from_utf8_lossy(TOKEN)) {
            self.journal.unauthorized.fetch_add(1, Ordering::SeqCst);
            return Ok((StatusCode::UNAUTHORIZED, "bad credential").into_response());
        }

        let operation = decoded.operation.as_str().to_string();
        let default = if decoded.operation == Operation::Describe {
            Answer::Describe(REQUIRED_CAPABILITIES.to_vec())
        } else {
            Answer::Accept(Vec::new())
        };
        let answer = self.answers.get(&operation).cloned().unwrap_or(default);

        // One statement: the journal lock is a temporary, released at its `;`.
        let answered = *self
            .journal
            .answered
            .lock()
            .expect("journal")
            .entry(operation.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);

        let (effects, delay) = match answer {
            Answer::Accept(effects) => (effects, Duration::ZERO),
            Answer::Slow(delay, effects) => (effects, delay),
            Answer::SlowFirst(delay, effects) => {
                (effects, if answered == 1 { delay } else { Duration::ZERO })
            }
            Answer::Deny => {
                let reply = RpcReply::denied(&decoded, "stub refuses");
                return Ok(json_ok(&serde_json::to_string(&reply).expect("reply")));
            }
            Answer::Fault => {
                let reply = RpcReply {
                    outcome: Outcome::Fault,
                    reason: Some("ActiveRecord::ConnectionTimeoutError".to_string()),
                    ..RpcReply::denied(&decoded, "")
                };
                return Ok(json_ok(&serde_json::to_string(&reply).expect("reply")));
            }
            Answer::Describe(capabilities) => {
                let reply = RpcReply {
                    describe: Some(DescribeInfo {
                        supported_versions: vec![1],
                        software_version: "stub".to_string(),
                        capabilities: capabilities.iter().map(|c| (*c).to_string()).collect(),
                        ready: true,
                    }),
                    ..RpcReply::accepted(&decoded)
                };
                return Ok(json_ok(&serde_json::to_string(&reply).expect("reply")));
            }
            Answer::Pinned(mut pinned) => {
                pinned["request_id"] = Value::from(decoded.request_id.clone());
                pinned["sequence"] = Value::from(decoded.sequence);
                return Ok(json_ok(&pinned.to_string()));
            }
        };

        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }

        let mut reply = serde_json::to_value(RpcReply::accepted(&decoded)).expect("reply");
        if decoded.operation == Operation::Identify {
            reply["identity"] = json!({ "identity_key": "user:7" });
        }
        reply["effects"] = Value::Array(effects);

        Ok(json_ok(&reply.to_string()))
    }
}

fn json_ok(body: &str) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("stub response")
}

/// Boot the stub endpoint on an ephemeral port.
async fn start_rails(answers: &[(&str, Answer)]) -> (SocketAddr, Arc<Journal>) {
    let journal = Arc::new(Journal::default());
    let stub = RailsStub {
        answers: answers.iter().map(|(op, answer)| ((*op).to_string(), answer.clone())).collect(),
        journal: Arc::clone(&journal),
    };

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

/// An `add_stream` effect for the test channel.
fn add_stream(stream: &str) -> Value {
    json!({ "kind": "add_stream", "identifier": IDENTIFIER, "stream": stream })
}

// --- the server under test --------------------------------------------------

struct TestServer {
    addr: SocketAddr,
    pubsub: Arc<MemoryPubSub>,
    transport: Arc<HttpTransport>,
    _listener_task: tokio::task::JoinHandle<()>,
    _serve_task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    fn ws_url(&self) -> String {
        format!("ws://{}/cable", self.addr)
    }
}

fn dispatch_config(rails: SocketAddr) -> Config {
    Config {
        docking: None,
        http: HttpConfig { listen_addr: Some("127.0.0.1:0".to_string()), ..Default::default() },
        shutdown_timeout: Duration::from_secs(5),
        database_url: None,
        listener_database_url: None,
        namespace: "orbitcast_test".to_string(),
        pubsub_ack_timeout: Duration::from_secs(5),
        pubsub_queue_capacity: 64,
        overflow: orbitcast::pubsub::OverflowConfig::default(),
        ping_interval: 3,
        presence_ttl: Duration::from_millis(orbitcast::presence::DEFAULT_PRESENCE_TTL_MS),
        presence_refresh_interval: Duration::from_millis(
            orbitcast::presence::DEFAULT_PRESENCE_REFRESH_INTERVAL_MS,
        ),
        // Retained history and recovery stay off: an ordinary client's
        // behaviour must not depend on them existing.
        broker: orbitcast::broker::BrokerConfig::default(),
        recovery_secret: None,
        // No signed-stream profile at all: authorization is Rails' job here.
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

async fn start(config: Config) -> TestServer {
    config.validate().expect("test config is valid");

    let pubsub = Arc::new(MemoryPubSub::new());
    let backend =
        Arc::new(Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn orbitcast::pubsub::PubSub>));
    let listener_task = backend.spawn_listener().expect("pub/sub listener starts");

    let transport = HttpTransport::new(Arc::clone(&backend), &config, Shutdown::new())
        .expect("transport builds");

    let listener = bind(config.http.listen_addr.as_deref().unwrap()).await.expect("bind");
    let addr = listener.local_addr();
    let serve_task = tokio::spawn(Arc::clone(&transport).serve(listener));

    TestServer { addr, pubsub, transport, _listener_task: listener_task, _serve_task: serve_task }
}

async fn connect(server: &TestServer) -> ClientWebSocket {
    ws::connect(server.ws_url()).await
}

/// `GET /readyz`: the status and the decoded JSON body.
async fn readiness(server: &TestServer) -> (StatusCode, Value) {
    let client = rama::http::client::EasyHttpWebClient::default();
    let request = Request::get(format!("http://{}/readyz", server.addr))
        .body(Body::empty())
        .expect("request");
    let response = tokio::time::timeout(ws::TIMEOUT, client.serve(request))
        .await
        .expect("response within deadline")
        .expect("http response");
    let status = response.status();
    let body: Value =
        serde_json::from_str(&response.into_body().try_into_string().await.expect("body"))
            .expect("readiness is JSON");
    (status, body)
}

/// Poll `/readyz` until `predicate` holds on `(status, body)`.
async fn wait_for_readiness(
    server: &TestServer,
    predicate: impl Fn(StatusCode, &Value) -> bool + Send + Sync,
) -> (StatusCode, Value) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let (status, body) = readiness(server).await;
            if predicate(status, &body) {
                return (status, body);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("readiness reached the expected state")
}

// --- tests -------------------------------------------------------------------

#[tokio::test]
async fn identify_then_welcome_then_subscribe_and_deliver() {
    let (rails, journal) = start_rails(&[
        ("identify", Answer::Accept(Vec::new())),
        ("open_channel", Answer::Accept(vec![add_stream("chat:42")])),
    ])
    .await;
    let server = start(dispatch_config(rails)).await;

    let mut socket = connect(&server).await;

    // 1. A welcome arrives only after Rails accepted the connection.
    let welcome = next_frame(&mut socket).await;
    assert_eq!(welcome["type"], "welcome");

    let identify = journal.requests_for("identify");
    assert_eq!(identify.len(), 1, "exactly one identify per connection");

    // The request shape is the contract the Ruby endpoint must implement.
    let request = &identify[0];
    assert_eq!(request.path, "/_orbitcast/v1/rails/identify");
    assert_eq!(request.authorization, "Bearer rails-service-token");
    assert_eq!(request.body["version"], 1);
    assert_eq!(request.body["operation"], "identify");
    assert_eq!(request.body["namespace"], "orbitcast_test");
    assert_eq!(request.body["node_id"], "node-test");
    assert_eq!(request.body["sequence"], 1);
    assert_eq!(request.body["expected_revision"], 0);
    assert_eq!(request.body["deadline_ms"], 2000);
    assert!(
        uuid::Uuid::parse_str(request.body["session_id"].as_str().unwrap()).is_ok(),
        "session_id is a server-generated UUID"
    );
    assert!(uuid::Uuid::parse_str(request.body["request_id"].as_str().unwrap()).is_ok());
    assert_eq!(request.body["connection"]["url"], "/cable");
    assert!(
        request.body["connection"]["client_address"].as_str().unwrap().starts_with("127.0.0.1")
    );
    assert!(
        request.body["connection"]["headers"].as_object().unwrap().is_empty(),
        "only allowlisted headers are forwarded, and this client sent none"
    );
    // The browser cannot submit identity or subscriptions.
    assert!(request.body["connection"]["identity"].as_object().unwrap().is_empty());
    assert!(request.body.get("subscription").is_none());

    // 2. subscribe -> open_channel -> confirm, echoing the original identifier.
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": IDENTIFIER })).await;
    let confirm = next_frame(&mut socket).await;
    assert_eq!(confirm["type"], "confirm_subscription");
    assert_eq!(confirm["identifier"], IDENTIFIER);

    let open = journal.requests_for("open_channel");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].path, "/_orbitcast/v1/rails/open_channel");
    assert_eq!(open[0].body["sequence"], 2, "the sequence is per-session and monotonic");
    assert_eq!(open[0].body["expected_revision"], 1, "identify advanced the revision");
    assert_eq!(open[0].body["subscription"]["identifier"], IDENTIFIER);
    assert_eq!(open[0].body["subscription"]["params"]["channel"], "ChatChannel");
    assert_eq!(open[0].body["subscription"]["params"]["room"], "42");
    // Rails saw the identity it returned from identify.
    assert_eq!(open[0].body["connection"]["identity"]["identity_key"], "user:7");

    // 3. The add_stream effect installed a real membership.
    server.pubsub.publish("chat:42", r#"{"body":"hello","n":1}"#).await.expect("published");
    let message = next_frame(&mut socket).await;
    assert_eq!(message["identifier"], IDENTIFIER);
    assert_eq!(message["message"], json!({ "body": "hello", "n": 1 }));

    // 4. unsubscribe tears the membership down immediately and tells Rails.
    send_command(&mut socket, json!({ "command": "unsubscribe", "identifier": IDENTIFIER })).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!server.pubsub.is_subscribed("chat:42"), "backend interest released");

    let close = journal.requests_for("close_channel");
    assert_eq!(close.len(), 1);
    assert_eq!(close[0].body["subscription"]["identifier"], IDENTIFIER);
    assert_eq!(
        close[0].body["subscription"]["streams"][0], "chat:42",
        "Rails is told which streams the subscription held"
    );
}

#[tokio::test]
async fn actions_are_dispatched_with_the_client_encoded_data() {
    let transmit = json!({
        "kind": "transmit",
        "identifier": IDENTIFIER,
        "json_payload": r#"{"ok":true,"echo":"héllo"}"#,
    });
    let (rails, journal) = start_rails(&[
        ("identify", Answer::Accept(Vec::new())),
        ("open_channel", Answer::Accept(vec![add_stream("chat:42")])),
        ("run_action", Answer::Accept(vec![transmit])),
    ])
    .await;
    let server = start(dispatch_config(rails)).await;

    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    send_command(&mut socket, json!({ "command": "subscribe", "identifier": IDENTIFIER })).await;
    assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");

    let data = r#"{"action":"speak","text":"héllo"}"#;
    send_command(
        &mut socket,
        json!({ "command": "message", "identifier": IDENTIFIER, "data": data }),
    )
    .await;

    let echoed = next_frame(&mut socket).await;
    assert_eq!(echoed["identifier"], IDENTIFIER);
    assert_eq!(echoed["message"]["ok"], true);
    assert_eq!(echoed["message"]["echo"], "héllo");

    let actions = journal.requests_for("run_action");
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0].body["action"]["json_data"], data,
        "the client's encoded data reaches Rails byte for byte"
    );
    assert_eq!(actions[0].body["action"]["identifier"], IDENTIFIER);
    assert_eq!(actions[0].body["subscription"]["streams"][0], "chat:42");

    // An action for a subscription that was never confirmed never reaches Rails.
    send_command(
        &mut socket,
        json!({ "command": "message", "identifier": r#"{"channel":"Other"}"#, "data": "{}" }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(journal.requests_for("run_action").len(), 1);
}

#[tokio::test]
async fn a_slow_rails_operation_does_not_block_another_session() {
    let (rails, _journal) = start_rails(&[
        ("identify", Answer::Accept(Vec::new())),
        (
            "open_channel",
            Answer::SlowFirst(Duration::from_millis(700), vec![add_stream("chat:42")]),
        ),
    ])
    .await;
    let server = start(dispatch_config(rails)).await;

    let mut slow = connect(&server).await;
    let mut fast = connect(&server).await;
    assert_eq!(next_frame(&mut slow).await["type"], "welcome");
    assert_eq!(next_frame(&mut fast).await["type"], "welcome");

    // The slow session starts a 700 ms open_channel...
    send_command(&mut slow, json!({ "command": "subscribe", "identifier": IDENTIFIER })).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ...and the unrelated session is still served promptly. Fanout too: the
    // publication below travels the hub, which the slow call never touches.
    let started = std::time::Instant::now();
    send_command(&mut fast, json!({ "command": "subscribe", "identifier": IDENTIFIER })).await;
    let confirm = tokio::time::timeout(Duration::from_millis(400), next_frame(&mut fast))
        .await
        .expect("an unrelated session must not wait behind a slow Rails call");
    assert_eq!(confirm["type"], "confirm_subscription");
    assert!(started.elapsed() < Duration::from_millis(400));

    server.pubsub.publish("chat:42", r#"{"body":"fanout"}"#).await.expect("published");
    let message = next_frame(&mut fast).await;
    assert_eq!(message["message"]["body"], "fanout");

    // The slow session still gets its own confirmation, in its own time.
    let confirm = next_frame(&mut slow).await;
    assert_eq!(confirm["type"], "confirm_subscription");
}

#[tokio::test]
async fn a_denied_identify_disconnects_without_reconnect() {
    let (rails, journal) = start_rails(&[("identify", Answer::Deny)]).await;
    let server = start(dispatch_config(rails)).await;

    let mut socket = connect(&server).await;

    let frame = next_frame(&mut socket).await;
    assert_eq!(frame["type"], "disconnect", "a refused connection is never welcomed");
    assert_eq!(frame["reason"], "unauthorized");
    assert_eq!(
        frame["reconnect"], false,
        "wrong credentials must not invite an immediate retry loop"
    );

    assert_eq!(journal.requests_for("identify").len(), 1);
    // A refused connection never reaches release_connection: Rails ran no
    // connection callbacks to unwind.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(journal.requests_for("release_connection").is_empty());
    assert_eq!(server.transport.connections().session_count(), 0);
}

#[tokio::test]
async fn a_denied_subscription_is_rejected_and_installs_nothing() {
    let (rails, _journal) =
        start_rails(&[("identify", Answer::Accept(Vec::new())), ("open_channel", Answer::Deny)])
            .await;
    let server = start(dispatch_config(rails)).await;

    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    send_command(&mut socket, json!({ "command": "subscribe", "identifier": IDENTIFIER })).await;
    let frame = next_frame(&mut socket).await;
    assert_eq!(frame["type"], "reject_subscription");
    assert_eq!(frame["identifier"], IDENTIFIER);

    server.pubsub.publish("chat:42", r#"{"body":"nope"}"#).await.expect("published");
    let idle = tokio::time::timeout(Duration::from_millis(250), socket.recv_message()).await;
    assert!(
        idle.is_err() || matches!(&idle, Ok(Ok(Message::Text(text))) if text.contains("\"ping\"")),
        "a rejected subscription receives nothing"
    );
}

#[tokio::test]
async fn a_rails_outage_closes_connections_with_reconnect_true() {
    // Bind and drop: nothing is listening on this address.
    let listener =
        TcpListener::build(Executor::default()).bind_address("127.0.0.1:0").await.expect("binds");
    let rails = listener.local_addr().expect("address");
    drop(listener);

    let server = start(dispatch_config(rails)).await;
    let mut socket = connect(&server).await;

    let frame = next_frame(&mut socket).await;
    assert_eq!(frame["type"], "disconnect", "an outage must never be silently accepted");
    assert_eq!(frame["reason"], "server_restart");
    assert_eq!(frame["reconnect"], true);

    // Fail closed: no welcome, no subscriptions, nothing locally authorized.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.transport.connections().session_count(), 0);
}

#[tokio::test]
async fn teardown_submits_release_connection_without_blocking() {
    let (rails, journal) = start_rails(&[
        ("identify", Answer::Accept(Vec::new())),
        ("open_channel", Answer::Accept(vec![add_stream("chat:42")])),
        // A slow release must not hold the socket or the hub open.
        ("release_connection", Answer::Slow(Duration::from_millis(300), Vec::new())),
    ])
    .await;
    let server = start(dispatch_config(rails)).await;

    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": IDENTIFIER })).await;
    assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");

    socket.close(None).await.expect("close sent");

    // Teardown is immediate: memberships and interest go with the socket.
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.transport.connections().session_count() > 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("teardown does not wait for Rails");
    assert!(!server.pubsub.is_subscribed("chat:42"));

    // The callback still runs, behind the bounded cleanup queue.
    tokio::time::timeout(Duration::from_secs(3), async {
        while journal.requests_for("release_connection").is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("release_connection is submitted");

    let release = journal.requests_for("release_connection");
    assert_eq!(release.len(), 1);
    assert_eq!(release[0].path, "/_orbitcast/v1/rails/release_connection");
    assert_eq!(release[0].body["release"]["subscriptions"][0]["identifier"], IDENTIFIER);
    assert_eq!(release[0].body["release"]["subscriptions"][0]["streams"][0], "chat:42");
    assert_eq!(release[0].body["connection"]["identity"]["identity_key"], "user:7");
}

#[tokio::test]
async fn duplicate_subscribe_reconfirms_without_a_second_open_channel() {
    let (rails, journal) = start_rails(&[
        ("identify", Answer::Accept(Vec::new())),
        ("open_channel", Answer::Accept(vec![add_stream("chat:42")])),
    ])
    .await;
    let server = start(dispatch_config(rails)).await;

    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    for _ in 0..2 {
        send_command(&mut socket, json!({ "command": "subscribe", "identifier": IDENTIFIER }))
            .await;
        assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");
    }

    assert_eq!(
        journal.requests_for("open_channel").len(),
        1,
        "a repeat subscribe must not run the Rails `subscribed` callback twice"
    );

    // And exactly one membership exists, so one publication is one frame.
    server.pubsub.publish("chat:42", r#"{"body":"once"}"#).await.expect("published");
    let message = next_frame(&mut socket).await;
    assert_eq!(message["message"]["body"], "once");
    let idle = tokio::time::timeout(Duration::from_millis(250), socket.recv_message()).await;
    assert!(
        idle.is_err() || matches!(&idle, Ok(Ok(Message::Text(text))) if text.contains("\"ping\"")),
        "a duplicate subscribe installs no second membership"
    );
}

#[tokio::test]
async fn the_endpoint_authenticates_the_service_credential() {
    let (rails, journal) = start_rails(&[("identify", Answer::Accept(Vec::new()))]).await;

    let mut config = dispatch_config(rails);
    config.rpc.token = Some(SecretBytes::new(b"the-wrong-token".to_vec()));
    let server = start(config).await;

    let mut socket = connect(&server).await;
    let frame = next_frame(&mut socket).await;

    // The endpoint answered 401; a rejected credential is a backend failure,
    // not an acceptance.
    assert_eq!(frame["type"], "disconnect");
    assert_eq!(frame["reconnect"], true);
    // The startup describe probe and the identify were both refused; the
    // identify is the one that answered this socket.
    assert!(journal.unauthorized.load(Ordering::SeqCst) >= 1);
    assert_eq!(journal.requests_for("identify").len(), 1);
}

#[tokio::test]
async fn a_close_effect_disconnects_with_the_requested_policy() {
    let close = json!({ "kind": "close", "reason": "unauthorized", "reconnect": false });
    let (rails, _journal) = start_rails(&[
        ("identify", Answer::Accept(Vec::new())),
        ("open_channel", Answer::Accept(vec![add_stream("chat:42"), close])),
    ])
    .await;
    let server = start(dispatch_config(rails)).await;

    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    send_command(&mut socket, json!({ "command": "subscribe", "identifier": IDENTIFIER })).await;

    // Order on the wire: the subscription result cannot appear after the
    // disconnect that was queued behind it.
    let mut frames = Vec::new();
    for _ in 0..3 {
        let frame = next_frame(&mut socket).await;
        let kind = frame["type"].as_str().unwrap_or("message").to_string();
        frames.push(kind.clone());
        if kind == "disconnect" {
            assert_eq!(frame["reason"], "unauthorized");
            assert_eq!(frame["reconnect"], false);
            break;
        }
    }
    assert!(frames.contains(&"disconnect".to_string()), "{frames:?}");
}

#[tokio::test]
async fn a_faulted_identify_closes_with_reconnect_true_never_unauthorized() {
    let (rails, journal) = start_rails(&[("identify", Answer::Fault)]).await;
    let server = start(dispatch_config(rails)).await;

    let mut socket = connect(&server).await;

    let frame = next_frame(&mut socket).await;
    assert_eq!(frame["type"], "disconnect", "a faulted connection is never welcomed");
    assert_eq!(frame["reason"], "server_restart", "a Rails fault is not the client's fault");
    assert_eq!(frame["reconnect"], true, "the client is invited back once Rails recovers");

    assert_eq!(journal.requests_for("identify").len(), 1);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.transport.connections().session_count(), 0);
}

#[tokio::test]
async fn a_faulted_action_closes_for_a_retry_while_a_denied_one_is_dropped() {
    let (rails, _journal) = start_rails(&[
        ("identify", Answer::Accept(Vec::new())),
        ("open_channel", Answer::Accept(vec![add_stream("chat:42")])),
        ("run_action", Answer::Deny),
    ])
    .await;
    let server = start(dispatch_config(rails)).await;
    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": IDENTIFIER })).await;
    assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");

    // Denied: a decision about this action, the session goes on.
    send_command(
        &mut socket,
        json!({ "command": "message", "identifier": IDENTIFIER, "data": "{\"action\":\"speak\"}" }),
    )
    .await;
    server.pubsub.publish("chat:42", r#"{"still":"here"}"#).await.expect("published");
    assert_eq!(next_frame(&mut socket).await["message"]["still"], "here");

    // Faulted: Rails failed, the session is closed for a retry.
    let (rails, _journal) = start_rails(&[
        ("identify", Answer::Accept(Vec::new())),
        ("open_channel", Answer::Accept(vec![add_stream("chat:42")])),
        ("run_action", Answer::Fault),
    ])
    .await;
    let server = start(dispatch_config(rails)).await;
    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": IDENTIFIER })).await;
    assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");
    send_command(
        &mut socket,
        json!({ "command": "message", "identifier": IDENTIFIER, "data": "{\"action\":\"speak\"}" }),
    )
    .await;
    let frame = next_frame(&mut socket).await;
    assert_eq!(frame["type"], "disconnect");
    assert_eq!(frame["reason"], "server_restart");
    assert_eq!(frame["reconnect"], true);
}

#[tokio::test]
async fn describe_is_probed_at_startup_and_gates_readiness() {
    let (rails, journal) = start_rails(&[]).await;
    let server = start(dispatch_config(rails)).await;

    let (status, body) = wait_for_readiness(&server, |status, _| status == StatusCode::OK).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "ready": true, "pubsub": "ok", "rails": "ok" }));

    let probes = journal.requests_for("describe");
    assert_eq!(probes.len(), 1, "one describe at startup, before any connection");
    assert_eq!(probes[0].path, "/_orbitcast/v1/rails/describe");
    assert_eq!(
        probes[0].body["session_id"],
        uuid::Uuid::nil().to_string(),
        "describe has no session"
    );
    assert_eq!(probes[0].body["sequence"], 0);
    assert_eq!(probes[0].body["namespace"], "orbitcast_test");

    let scrape = server.transport.connections().backend().render_metrics();
    assert!(scrape.contains("orbitcast_rails_ready 1"), "{scrape}");
}

#[tokio::test]
async fn an_endpoint_missing_a_capability_is_unready_and_admits_nothing() {
    let (rails, _journal) =
        start_rails(&[("describe", Answer::Describe(vec!["identify", "open_channel"]))]).await;
    let server = start(dispatch_config(rails)).await;

    let (status, body) =
        wait_for_readiness(&server, |_, body| body["rails"] == "missing_capability").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, json!({ "ready": false, "pubsub": "ok", "rails": "missing_capability" }));

    // A connection is refused with a reconnectable disconnect, never
    // authenticated against an endpoint this build must not dispatch to.
    let mut socket = connect(&server).await;
    let frame = next_frame(&mut socket).await;
    assert_eq!(frame["type"], "disconnect");
    assert_eq!(frame["reconnect"], true);

    let scrape = server.transport.connections().backend().render_metrics();
    assert!(scrape.contains("orbitcast_rails_ready 0"), "{scrape}");
}

#[tokio::test]
async fn an_unsupported_envelope_version_is_reported_by_name() {
    // A stub that speaks only a future version.
    let (rails, _journal) = start_rails(&[(
        "describe",
        Answer::Pinned(json!({
            "version": 1, "operation": "describe", "outcome": "accepted",
            "request_id": "", "sequence": 0, "new_revision": 0,
            "describe": {
                "supported_versions": [2], "software_version": "9.0",
                "capabilities": REQUIRED_CAPABILITIES, "ready": true
            }
        })),
    )])
    .await;
    let server = start(dispatch_config(rails)).await;

    let (status, body) =
        wait_for_readiness(&server, |_, body| body["rails"] == "unsupported_version").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["ready"], false);
}

/// The integer-boundary fixtures, consumed end to end: requests through the
/// real caller (which validates before sending), the reply through the real
/// session reducer.
#[tokio::test]
async fn integer_boundary_fixtures_round_trip_through_the_caller_and_the_reducer() {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/rpc/integer_boundaries.json");
    let doc: Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).unwrap();
    let cases = doc["cases"].as_array().expect("cases");

    let (rails, journal) = start_rails(&[]).await;
    let client = RpcClient::new(RpcConfig::new(
        format!("http://{rails}/_orbitcast/v1/rails"),
        SecretBytes::new(TOKEN.to_vec()),
    ))
    .expect("client builds");

    let mut sent = 0;
    let mut refused = 0;
    let mut pinned_reply = None;
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let wire = case["input"]["wire"].as_str().unwrap();
        let valid = case["expect"]["valid"].as_bool().unwrap();
        if case["envelope"].as_str().unwrap_or("reply") != "request" {
            assert!(valid, "{name}: only valid replies are pinned here");
            pinned_reply = Some(serde_json::from_str::<Value>(wire).unwrap());
            continue;
        }
        let Ok(request) = serde_json::from_str::<RpcRequest>(wire) else {
            assert!(!valid, "{name}: an undecodable request is never valid");
            refused += 1;
            continue;
        };
        let outcome = client.call(request).await;
        if valid {
            outcome.unwrap_or_else(|error| panic!("{name}: the caller must send it: {error}"));
            sent += 1;
        } else {
            assert!(matches!(outcome, Err(RpcError::InvalidRequest(_))), "{name}: {outcome:?}");
            refused += 1;
        }
    }
    assert_eq!(sent, 2, "both valid requests reached the endpoint");
    assert_eq!(journal.seen().len(), sent, "invalid requests never leave the process");
    assert_eq!(refused, 4);

    // The pinned reply - counters at u64::MAX - drives a real session: the
    // subscription is confirmed, and the next request carries the revision
    // the reply set, untouched.
    let pinned = pinned_reply.expect("a pinned reply");
    let identifier = pinned["effects"][0]["identifier"].as_str().unwrap().to_string();
    let reply: RpcReply = serde_json::from_value(pinned.clone()).unwrap();
    reply.validate().expect("the pinned reply is valid");
    let (rails, journal) = start_rails(&[("open_channel", Answer::Pinned(pinned))]).await;
    let server = start(dispatch_config(rails)).await;
    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": identifier })).await;
    assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");
    send_command(
        &mut socket,
        json!({ "command": "message", "identifier": identifier, "data": "{\"action\":\"speak\"}" }),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while journal.requests_for("run_action").is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the action reached the endpoint");
    let action = &journal.requests_for("run_action")[0];
    assert_eq!(
        action.body["expected_revision"].as_u64(),
        Some(u64::MAX),
        "the 64-bit revision survived the reducer and the encoder"
    );
}

#[tokio::test]
async fn describe_and_reply_types_match_the_http_binding() {
    // The operation name in the envelope is also the last path segment, which
    // is what lets the Rails router refuse a replayed body for another route.
    let (rails, journal) = start_rails(&[("identify", Answer::Accept(Vec::new()))]).await;
    let server = start(dispatch_config(rails)).await;

    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    for seen in journal.seen() {
        let operation = seen.body["operation"].as_str().expect("operation");
        assert!(
            seen.path.ends_with(&format!("/{operation}")),
            "{} must be posted to .../{operation}",
            seen.path
        );
    }

    // And the reply the stub built is exactly what the canonical types encode.
    let request = RpcRequest::new(
        Operation::Describe,
        "orbitcast_test",
        "node-test",
        uuid::Uuid::nil().to_string(),
        0,
        0,
        1_000,
    );
    let reply = RpcReply::accepted(&request);
    assert_eq!(reply.outcome, Outcome::Accepted);
    assert_eq!(serde_json::to_value(&reply).unwrap()["operation"], "describe");
}
