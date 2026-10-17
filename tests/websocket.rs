//! End-to-end tests for the direct HTTP/WebSocket listener in signed mode.
//!
//! These run the real server — rama listener, real WebSocket handshake, real
//! `ActionCable` frames — against the in-process memory pub/sub backend, so
//! publishing goes through the same backend path the PostgreSQL build uses.
//!
//! Requires the `memory` feature: the default (PostgreSQL) build has no
//! backend that can be published into without a database.
#![cfg(feature = "memory")]
// Every binding clippy::significant_drop_tightening flags in this file is a
// fixture of Arcs and JoinHandles, never a lock guard: the lint walks field
// types transitively and reaches `RpcClient`'s rama connection pool (or a rama
// HTTP client's own pool), whose slots are tokio semaphore permits marked
// `#[clippy::has_significant_drop]`.
#![expect(
    clippy::significant_drop_tightening,
    reason = "TestServer and the rama client are fixtures reaching pool permits, not lock guards"
)]

use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;

use rama::futures::SinkExt as _;
use rama::http::client::EasyHttpWebClient;
use rama::http::headers::SecWebSocketProtocol;
use rama::http::ws::Message;
use rama::http::ws::handshake::client::ClientWebSocket;
use rama::http::{Body, Request, StatusCode};
use rama::utils::str::non_empty_str;
use rama::{Service as _, http::Response};
use serde_json::json;

#[path = "support/ws.rs"]
mod ws;

use orbitcast::MemoryPubSub;
use orbitcast::config::{Config, HttpConfig};
use orbitcast::pubsub::PubSub as _;
use orbitcast::server::{Backend, Shutdown};
use orbitcast::transport::http::{ACTIONCABLE_SUBPROTOCOL, HttpTransport, bind};
use ws::{TIMEOUT, actioncable_offer, next_frame, send_command};

/// A running server plus the handles a test needs to poke at it.
struct TestServer {
    addr: std::net::SocketAddr,
    pubsub: Arc<MemoryPubSub>,
    transport: Arc<HttpTransport>,
    shutdown: Shutdown,
    _listener_task: tokio::task::JoinHandle<()>,
    _serve_task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn ws_url(&self) -> String {
        format!("ws://{}/cable", self.addr)
    }
}

fn base_config() -> Config {
    let mut config = Config {
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
        streams: orbitcast::streams::StreamsConfig::default(),
        rpc: orbitcast::config::RpcSettings::default(),
        ingress: orbitcast::config::IngressSettings::default(),
        explicit: orbitcast::config::ExplicitKeys::default(),
    };
    // Public (unsigned) streams keep the test focused on the transport; the
    // signed profiles have their own fixture-driven coverage.
    config.streams.public = true;
    config.streams.public_insecure = true;
    config
}

/// Boot a server on an ephemeral port with the memory backend.
async fn start(config: Config) -> TestServer {
    config.validate().expect("test config is valid");

    let pubsub = Arc::new(MemoryPubSub::new());
    let backend =
        Arc::new(Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn orbitcast::pubsub::PubSub>));
    let listener_task = backend.spawn_listener().expect("pub/sub listener starts");

    let shutdown = Shutdown::new();
    let transport = HttpTransport::new(Arc::clone(&backend), &config, shutdown.clone())
        .expect("transport builds");

    let listener = bind(config.http.listen_addr.as_deref().unwrap()).await.expect("bind");
    let addr = listener.local_addr();
    let serve_task = tokio::spawn(Arc::clone(&transport).serve(listener));

    TestServer {
        addr,
        pubsub,
        transport,
        shutdown,
        _listener_task: listener_task,
        _serve_task: serve_task,
    }
}

/// Connect with the standard `ActionCable` offer and no `Origin` header.
async fn connect(server: &TestServer) -> ClientWebSocket {
    ws::connect(server.ws_url()).await
}

/// Attempt one handshake, controlling the subprotocol offer and `Origin`.
async fn try_connect(
    server: &TestServer,
    protocols: Option<SecWebSocketProtocol>,
    origin: Option<&str>,
) -> Result<ClientWebSocket, rama::error::BoxError> {
    ws::try_connect(server.ws_url(), protocols, origin).await
}

/// Write raw text that is not required to be a valid command at all.
async fn send_raw(socket: &mut ClientWebSocket, text: impl Into<String>) {
    socket.send(Message::text(text.into())).await.expect("text written to socket");
}

/// A public-stream `ActionCable` identifier, exactly as the client would send it.
fn identifier(stream: &str) -> String {
    json!({ "channel": "Orbitcast::StreamsChannel", "stream_name": stream }).to_string()
}

async fn http_get(server: &TestServer, path: &str) -> Response {
    let client = EasyHttpWebClient::default();
    let request = Request::get(server.url(path)).body(Body::empty()).expect("request");
    tokio::time::timeout(TIMEOUT, client.serve(request))
        .await
        .expect("response within deadline")
        .expect("http response")
}

/// Read one response body as text.
async fn body_text(response: Response) -> String {
    use rama::http::BodyExtractExt as _;
    response.into_body().try_into_string().await.expect("body is text")
}

#[tokio::test]
async fn welcome_subscribe_publish_unsubscribe_round_trip() {
    let server = start(base_config()).await;
    let mut socket = connect(&server).await;

    // 1. welcome, unprompted, exactly once.
    let welcome = next_frame(&mut socket).await;
    assert_eq!(welcome["type"], "welcome");
    assert!(welcome["sid"].is_string());

    // 2. subscribe -> confirm, echoing the client's original identifier.
    let id = identifier("chat:42");
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": id })).await;

    let confirm = next_frame(&mut socket).await;
    assert_eq!(confirm["type"], "confirm_subscription");
    assert_eq!(confirm["identifier"], id);

    // 3. a publication issued right after confirmation is delivered, addressed
    //    with the subscriber's own identifier and with the payload verbatim.
    server.pubsub.publish("chat:42", r#"{"body":"hello","n":1}"#).await.expect("published");

    let message = next_frame(&mut socket).await;
    assert_eq!(message["identifier"], id);
    assert_eq!(message["message"], json!({ "body": "hello", "n": 1 }));

    // 4. unsubscribe stops delivery; the backend loses interest with the last
    //    member.
    send_command(&mut socket, json!({ "command": "unsubscribe", "identifier": id })).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!server.pubsub.is_subscribed("chat:42"), "backend interest released");

    server.pubsub.publish("chat:42", r#"{"body":"ignored"}"#).await.expect("published");
    let idle = tokio::time::timeout(Duration::from_millis(250), socket.recv_message()).await;
    assert!(
        idle.is_err() || matches!(&idle, Ok(Ok(Message::Text(text))) if text.contains("\"ping\"")),
        "unsubscribed socket must receive no publication"
    );

    // 5. client-initiated disconnect tears the session down.
    socket.close(None).await.expect("close sent");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.transport.connections().session_count(), 0);
}

#[tokio::test]
async fn two_identifiers_on_one_socket_receive_correctly_addressed_frames() {
    let server = start(base_config()).await;
    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    // Two different identifiers subscribing to the same stream: each must get
    // its own correctly addressed frame.
    let first = identifier("chat:42");
    let second = json!({
        "channel": "Orbitcast::StreamsChannel",
        "stream_name": "chat:42",
        "tab": 2,
    })
    .to_string();

    for id in [&first, &second] {
        send_command(&mut socket, json!({ "command": "subscribe", "identifier": id })).await;
        assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");
    }

    server.pubsub.publish("chat:42", r#"{"body":"fanout"}"#).await.expect("published");

    let mut seen = vec![
        next_frame(&mut socket).await["identifier"].as_str().unwrap().to_string(),
        next_frame(&mut socket).await["identifier"].as_str().unwrap().to_string(),
    ];
    seen.sort();
    let mut expected = vec![first, second];
    expected.sort();
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn subprotocol_negotiation_selects_actioncable_v1_json() {
    let server = start(base_config()).await;
    let socket = connect(&server).await;

    assert_eq!(
        socket.accepted_protocol(),
        Some(ACTIONCABLE_SUBPROTOCOL),
        "the offer contains actioncable-v1-json, so the response must select it"
    );
    assert_eq!(
        socket
            .response()
            .headers
            .get("sec-websocket-protocol")
            .and_then(|value| value.to_str().ok()),
        Some(ACTIONCABLE_SUBPROTOCOL)
    );
}

#[tokio::test]
async fn client_offering_no_or_other_subprotocol_still_connects() {
    let server = start(base_config()).await;

    // No offer at all: connection established, no subprotocol selected.
    let mut socket = try_connect(&server, None, None).await.expect("no-offer client connects");
    assert_eq!(socket.accepted_protocol(), None);
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    // An offer we do not speak: still connected, and we never echo it back.
    let mut socket =
        try_connect(&server, Some(SecWebSocketProtocol::new(non_empty_str!("graphql-ws"))), None)
            .await
            .expect("unsupported-offer client connects");
    assert_eq!(socket.accepted_protocol(), None);
    assert!(socket.response().headers.get("sec-websocket-protocol").is_none());
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");
}

#[tokio::test]
async fn origin_must_be_allowed_when_present() {
    let mut config = base_config();
    config.http.allowed_origins = vec!["https://app.example".to_string()];
    let server = start(config).await;

    // Allowed origin: normal connection.
    let mut socket = try_connect(&server, Some(actioncable_offer()), Some("https://app.example"))
        .await
        .expect("allowed origin connects");
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    // Foreign origin: refused before the upgrade.
    let refused =
        try_connect(&server, Some(actioncable_offer()), Some("https://evil.example")).await;
    assert!(refused.is_err(), "cross-origin upgrade must be refused");

    // The refusal is a plain 403, not a broken handshake.
    let client = EasyHttpWebClient::default();
    let request = Request::get(server.url("/cable"))
        .header("origin", "https://evil.example")
        .body(Body::empty())
        .expect("request");
    let response = client.serve(request).await.expect("http response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn empty_origin_allow_list_rejects_every_browser_origin() {
    // Default config: no origins configured at all.
    let server = start(base_config()).await;

    // A client that sends no Origin (any non-browser client) is fine.
    connect(&server).await;

    let response = {
        let client = EasyHttpWebClient::default();
        let request = Request::get(server.url("/cable"))
            .header("origin", "https://app.example")
            .body(Body::empty())
            .expect("request");
        client.serve(request).await.expect("http response")
    };
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn health_and_readiness_endpoints() {
    let server = start(base_config()).await;

    assert_eq!(http_get(&server, "/healthz").await.status(), StatusCode::OK);
    // The memory backend is always ready; signed mode has no Rails to report.
    let ready = http_get(&server, "/readyz").await;
    assert_eq!(ready.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&body_text(ready).await).unwrap();
    assert_eq!(body, json!({ "ready": true, "pubsub": "ok" }));
    assert_eq!(http_get(&server, "/nope").await.status(), StatusCode::NOT_FOUND);

    // Draining flips readiness, while liveness stays up until the process exits.
    server.shutdown.trigger();
    let draining = http_get(&server, "/readyz").await;
    assert_eq!(draining.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = serde_json::from_str(&body_text(draining).await).unwrap();
    assert_eq!(body["draining"], true);
    assert_eq!(http_get(&server, "/healthz").await.status(), StatusCode::OK);
}

/// MOCKED: a pub/sub listener that is (re)connecting forever, as the
/// PostgreSQL backend reports itself between a connection loss and the next
/// successful `LISTEN` session.
struct ReconnectingPubSub;

#[async_trait::async_trait]
impl orbitcast::pubsub::PubSub for ReconnectingPubSub {
    async fn publish(&self, _: &str, _: &str) -> Result<(), orbitcast::pubsub::PubSubError> {
        Err(orbitcast::pubsub::PubSubError::NotReady)
    }

    async fn publish_publication(
        &self,
        _: &orbitcast::protocol::v1::publication::Publication,
    ) -> Result<(), orbitcast::pubsub::PubSubError> {
        Err(orbitcast::pubsub::PubSubError::NotReady)
    }

    async fn subscribe(&self, _: &str) -> Result<(), orbitcast::pubsub::PubSubError> {
        Err(orbitcast::pubsub::PubSubError::NotReady)
    }

    async fn unsubscribe(&self, _: &str) -> Result<(), orbitcast::pubsub::PubSubError> {
        Ok(())
    }

    fn is_ready(&self) -> bool {
        false
    }

    fn stats(&self) -> orbitcast::pubsub::StatsSnapshot {
        orbitcast::pubsub::StatsSnapshot::default()
    }

    async fn listen(
        &self,
        _: orbitcast::pubsub::EventHandler,
    ) -> Result<(), orbitcast::pubsub::PubSubError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn readyz_reports_a_reconnecting_listener_by_name() {
    let config = base_config();
    config.validate().unwrap();

    // The listener is reconnecting: the node must never advertise readiness,
    // and the body says which component is the reason.
    let backend = Arc::new(Backend::with_pubsub(
        Arc::new(ReconnectingPubSub) as Arc<dyn orbitcast::pubsub::PubSub>
    ));
    let shutdown = Shutdown::new();
    let transport = HttpTransport::new(backend, &config, shutdown).expect("transport builds");
    let listener = bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr();
    let serve = tokio::spawn(Arc::clone(&transport).serve(listener));

    let client = EasyHttpWebClient::default();
    let request =
        Request::get(format!("http://{addr}/readyz")).body(Body::empty()).expect("request");
    let response = client.serve(request).await.expect("http response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(body, json!({ "ready": false, "pubsub": "reconnecting" }));

    serve.abort();
}

#[tokio::test]
async fn connection_limit_refuses_further_upgrades() {
    let mut config = base_config();
    config.http.max_connections = 1;
    let server = start(config).await;

    let mut first = connect(&server).await;
    assert_eq!(next_frame(&mut first).await["type"], "welcome");

    let response = {
        let client = EasyHttpWebClient::default();
        let request = Request::get(server.url("/cable")).body(Body::empty()).expect("request");
        client.serve(request).await.expect("http response")
    };
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// --- malformed input ---------------------------------------------------------
//
// Bounded behaviour under hostile input: every frame
// below must be refused or ignored *without* panicking the server, and without
// taking down a connection that is otherwise behaving. A panic in a session
// task would abort only that task, so these assert the observable contract
// instead: the socket keeps working, and the server keeps serving.

#[tokio::test]
async fn garbage_frames_are_refused_and_the_socket_keeps_working() {
    let mut config = base_config();
    // Enough budget for the parseable garbage below plus the real subscribe;
    // unparseable frames are never charged, because they did no work.
    config.http.rate_commands_per_conn_per_sec = 100;
    let server = start(config).await;
    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    // Large but inside the default 64 KiB per-message limit, so this exercises
    // the identifier path rather than the frame limit (which has its own test).
    let long_identifier = "x".repeat(8 * 1024);

    // Deep nesting: serde_json stops at its own recursion limit and reports an
    // error. The point is that it is an error, not a stack overflow.
    let deep = format!("{}{}", "[".repeat(512), "]".repeat(512));

    for garbage in [
        // Not JSON at all.
        "{not json".to_string(),
        String::new(),
        "null".to_string(),
        "[]".to_string(),
        // JSON, but not a command envelope.
        r#"{"hello":"world"}"#.to_string(),
        // A command type this server does not have.
        r#"{"command":"self_destruct","identifier":"{}"}"#.to_string(),
        // Right command, wrong field types.
        r#"{"command":"subscribe","identifier":42}"#.to_string(),
        format!(r#"{{"command":"subscribe","identifier":{deep}}}"#),
        // Well-formed, unresolvable identifier.
        json!({ "command": "subscribe", "identifier": long_identifier }).to_string(),
        json!({ "command": "unsubscribe", "identifier": "never-subscribed" }).to_string(),
        json!({ "command": "message", "identifier": "nonexistent", "data": "{}" }).to_string(),
    ] {
        send_raw(&mut socket, garbage).await;
    }

    // The connection survived every one of them: a normal subscribe still
    // round-trips on the same socket.
    let id = identifier("chat:42");
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": &id })).await;

    // A parseable-but-unauthorized subscribe is answered with an explicit
    // rejection; unparseable frames are answered with nothing at all. Both are
    // bounded, so the only frames before the confirmation are rejections.
    let confirm = loop {
        let frame = next_frame(&mut socket).await;
        if frame["type"] == "confirm_subscription" {
            break frame;
        }
        assert_eq!(
            frame["type"], "reject_subscription",
            "garbage may only produce rejections or silence: {frame}"
        );
    };
    assert_eq!(confirm["identifier"], id);

    // And the server as a whole is still healthy.
    assert_eq!(http_get(&server, "/healthz").await.status(), StatusCode::OK);
    assert_eq!(server.transport.connections().session_count(), 1);
}

#[tokio::test]
async fn an_oversized_message_closes_that_socket_cleanly_and_only_that_one() {
    let mut config = base_config();
    config.http.max_message_size = 4 * 1024;
    let server = start(config).await;

    let mut victim = connect(&server).await;
    assert_eq!(next_frame(&mut victim).await["type"], "welcome");

    let mut bystander = connect(&server).await;
    assert_eq!(next_frame(&mut bystander).await["type"], "welcome");

    // One frame above the configured per-message limit.
    send_raw(
        &mut victim,
        json!({ "command": "subscribe", "identifier": "y".repeat(8 * 1024) }).to_string(),
    )
    .await;

    // The oversize socket is closed by the server rather than buffered.
    let closed = tokio::time::timeout(TIMEOUT, async {
        loop {
            match victim.recv_message().await {
                Ok(Message::Text(_) | Message::Ping(_) | Message::Pong(_)) => {}
                other => return other,
            }
        }
    })
    .await
    .expect("the oversize socket is closed within the deadline");
    assert_matches!(
        closed,
        Ok(Message::Close(_)) | Err(_),
        "an oversize message closes the connection"
    );

    // The other socket is untouched and still delivers publications.
    let id = identifier("chat:42");
    send_command(&mut bystander, json!({ "command": "subscribe", "identifier": &id })).await;
    assert_eq!(next_frame(&mut bystander).await["type"], "confirm_subscription");

    server.pubsub.publish("chat:42", r#"{"body":"still here"}"#).await.expect("published");
    assert_eq!(next_frame(&mut bystander).await["identifier"], id);
}

// --- rate limits and handshake deadline --------------------------------------

#[tokio::test]
async fn the_per_session_command_limit_closes_with_a_reconnectable_disconnect() {
    let mut config = base_config();
    // Subscribes spend the subscribe-class budget; the action budget stays
    // at its default and is never touched by this test.
    config.http.rate_subscribes_per_conn_per_sec = 2;
    let server = start(config).await;

    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    // Burst equals rate: two commands pass, the third trips the limit.
    for index in 0..2 {
        send_command(
            &mut socket,
            json!({ "command": "subscribe", "identifier": identifier(&format!("chat:{index}")) }),
        )
        .await;
        assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");
    }

    send_command(
        &mut socket,
        json!({ "command": "subscribe", "identifier": identifier("chat:over") }),
    )
    .await;

    let disconnect = next_frame(&mut socket).await;
    assert_eq!(disconnect["type"], "disconnect");
    assert_eq!(disconnect["reconnect"], true, "the client is invited back");

    // Closed, torn down, and counted.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.transport.connections().session_count(), 0);

    let scrape = server.transport.connections().backend().render_metrics();
    assert!(
        scrape.contains("orbitcast_commands_rate_limited_total{class=\"subscribe\"} 1"),
        "the trip is counted against the subscribe class:\n{scrape}"
    );
}

#[tokio::test]
async fn the_per_address_connection_limit_answers_429() {
    let mut config = base_config();
    config.http.rate_connect_per_ip_per_min = 1;
    let server = start(config).await;

    // The whole minute's burst, spent.
    let mut first = connect(&server).await;
    assert_eq!(next_frame(&mut first).await["type"], "welcome");

    let response = http_get(&server, "/cable").await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response.headers().get("retry-after").and_then(|value| value.to_str().ok()),
        Some("1")
    );

    // A real handshake from the same address is refused too, rather than
    // hanging.
    assert!(try_connect(&server, Some(actioncable_offer()), None).await.is_err());

    let scrape = server.transport.connections().backend().render_metrics();
    assert!(
        scrape.contains("orbitcast_connections_rejected_total{reason=\"rate_limited\"} 2"),
        "both refusals are counted:\n{scrape}"
    );
}

#[tokio::test]
async fn a_zero_rate_disables_the_limiter_entirely() {
    let mut config = base_config();
    config.http.rate_connect_per_ip_per_min = 0;
    config.http.rate_commands_per_conn_per_sec = 0;
    config.http.rate_subscribes_per_conn_per_sec = 0;
    let server = start(config).await;

    // Many connections and many commands from one address, none refused.
    for _ in 0..5 {
        let mut socket = connect(&server).await;
        assert_eq!(next_frame(&mut socket).await["type"], "welcome");

        for index in 0..10 {
            send_command(
                &mut socket,
                json!({
                    "command": "subscribe",
                    "identifier": identifier(&format!("chat:{index}")),
                }),
            )
            .await;
            assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");
        }
    }

    let scrape = server.transport.connections().backend().render_metrics();
    assert!(scrape.contains("orbitcast_connections_rejected_total{reason=\"rate_limited\"} 0"));
    assert!(scrape.contains("orbitcast_commands_rate_limited_total{class=\"subscribe\"} 0"));
    assert!(scrape.contains("orbitcast_commands_rate_limited_total{class=\"action\"} 0"));
}

#[tokio::test]
async fn a_forwarded_client_address_counts_only_behind_a_trusted_proxy() {
    // One connection per address per minute. The test client is the TCP
    // peer (loopback); whether X-Forwarded-For moves it into another bucket
    // depends entirely on whether loopback is a trusted proxy.
    let mut config = base_config();
    config.http.rate_connect_per_ip_per_min = 1;
    let untrusted = start(config.clone()).await;

    let mut first = try_connect_forwarded(&untrusted, "203.0.113.1").await.expect("first");
    assert_eq!(next_frame(&mut first).await["type"], "welcome");
    assert!(
        try_connect_forwarded(&untrusted, "203.0.113.2").await.is_err(),
        "an untrusted peer's header is ignored: same bucket, refused"
    );

    config.http.trusted_proxies = vec![orbitcast::config::Cidr::parse("127.0.0.0/8").unwrap()];
    let trusted = start(config).await;
    let mut first = try_connect_forwarded(&trusted, "203.0.113.1").await.expect("first");
    assert_eq!(next_frame(&mut first).await["type"], "welcome");
    let mut second = try_connect_forwarded(&trusted, "203.0.113.2").await.expect("second");
    assert_eq!(next_frame(&mut second).await["type"], "welcome");
    assert!(
        try_connect_forwarded(&trusted, "203.0.113.1").await.is_err(),
        "the forwarded client's own bucket is spent"
    );
}

/// Connect with an `X-Forwarded-For` header naming `client`.
async fn try_connect_forwarded(
    server: &TestServer,
    client: &str,
) -> Result<ClientWebSocket, rama::error::BoxError> {
    use rama::http::ws::handshake::client::HttpClientWebSocketExt as _;

    let http = EasyHttpWebClient::default();
    let builder = http
        .websocket(server.ws_url())
        .with_protocols(actioncable_offer())
        .with_header("x-forwarded-for", client);
    tokio::time::timeout(TIMEOUT, builder.handshake(rama::extensions::Extensions::default()))
        .await
        .map_err(|_| -> rama::error::BoxError { "handshake timed out".into() })?
        .map_err(rama::error::BoxError::from)
}

#[tokio::test]
async fn a_socket_that_stops_reading_is_closed_for_lag_and_counted() {
    // A tiny outbound queue, and a client that never reads: the server must
    // tell it to reconnect and drop it, without stalling anyone else.
    let mut config = base_config();
    config.http.outbound_queue_messages = 4;
    let server = start(config).await;

    let mut slow = connect(&server).await;
    assert_eq!(next_frame(&mut slow).await["type"], "welcome");
    let id = identifier("chat:42");
    send_command(&mut slow, json!({ "command": "subscribe", "identifier": id })).await;
    assert_eq!(next_frame(&mut slow).await["type"], "confirm_subscription");

    let mut bystander = connect(&server).await;
    assert_eq!(next_frame(&mut bystander).await["type"], "welcome");

    // Flood the stream without reading on the slow socket. The kernel buffers
    // absorb some, so send enough to fill them and then the queue.
    let payload = format!(r#"{{"blob":"{}"}}"#, "x".repeat(32 * 1024));
    for _ in 0..200 {
        server.pubsub.publish("chat:42", &payload).await.expect("published");
    }

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let scrape = server.transport.connections().backend().render_metrics();
            if scrape.contains("orbitcast_socket_lag_disconnects_total{reason=\"queue\"} 1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the lagging socket is counted");

    // The bystander is unaffected, and the lagging session is torn down.
    send_command(
        &mut bystander,
        json!({ "command": "subscribe", "identifier": identifier("chat:43") }),
    )
    .await;
    assert_eq!(next_frame(&mut bystander).await["type"], "confirm_subscription");
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.transport.connections().session_count() != 1 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the lagging session is torn down");

    // Whatever the slow client reads back ends with the reconnectable
    // disconnect, then the close.
    let mut saw_disconnect = false;
    loop {
        let message = tokio::time::timeout(TIMEOUT, slow.recv_message())
            .await
            .expect("the lagging socket was never closed");
        match message {
            Ok(Message::Text(text)) => {
                let frame: serde_json::Value = serde_json::from_str(&text).unwrap();
                if frame["type"] == "disconnect" {
                    assert_eq!(frame["reconnect"], true);
                    saw_disconnect = true;
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => {}
        }
    }
    assert!(saw_disconnect, "the disconnect frame rides the close signal, not the full queue");
}

/// One stream, 1 000 subscribers, a burst of publications: every subscriber
/// sees every frame, in order, or is told to
/// reconnect. A silent gap - a frame missing without a `disconnect
/// {reconnect: true}` behind it - is the defect this guards against: the hub
/// used to push every frame through one shared 1 024-slot queue that a
/// 1 000-way burst overflowed, dropping frames with nothing but a counter.
///
/// The 1 000 subscribers are 250 sockets × 4 identifiers each, so the fan-out
/// is 1 000 addressed frames per publication while the test stays inside a
/// 1 024 file-descriptor limit (client and server sockets share one process).
#[tokio::test]
async fn a_thousand_way_fanout_burst_loses_nothing_silently() {
    const SOCKETS: usize = 250;
    const IDENTIFIERS_PER_SOCKET: usize = 4;
    const PUBLICATIONS: usize = 32;
    /// One deadline per reader, which the server's periodic ping cannot
    /// extend: a socket with a silent gap would otherwise sit there forever.
    const READ_DEADLINE: Duration = Duration::from_secs(20);

    // Admission limits off, as in a bench run: every client is one address.
    let mut config = base_config();
    config.http.rate_connect_per_ip_per_min = 0;
    config.http.rate_subscribes_per_conn_per_sec = 0;
    let server = start(config).await;

    // Subscribe everyone; the confirmations are read before the burst so the
    // memberships are active (not pending) when it starts.
    let mut sockets = Vec::with_capacity(SOCKETS);
    for socket_index in 0..SOCKETS {
        let mut socket = connect(&server).await;
        assert_eq!(next_frame(&mut socket).await["type"], "welcome");
        for identifier_index in 0..IDENTIFIERS_PER_SOCKET {
            let id = json!({
                "channel": "Orbitcast::StreamsChannel",
                "stream_name": "chat:fanout",
                "socket": socket_index,
                "tab": identifier_index,
            })
            .to_string();
            send_command(&mut socket, json!({ "command": "subscribe", "identifier": id })).await;
            assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");
        }
        sockets.push(socket);
    }

    // Every socket reads concurrently with the burst, as browsers would; what
    // each one saw is judged afterwards.
    let readers: Vec<_> = sockets
        .into_iter()
        .map(|mut socket| {
            tokio::spawn(async move {
                let mut seen: std::collections::HashMap<String, Vec<u64>> =
                    std::collections::HashMap::new();
                let mut disconnected = false;
                let mut closed = false;
                let wanted = IDENTIFIERS_PER_SOCKET * PUBLICATIONS;
                let read = async {
                    while seen.values().map(Vec::len).sum::<usize>() < wanted && !closed {
                        match socket.recv_message().await {
                            Ok(Message::Text(text)) => {
                                let frame: serde_json::Value = serde_json::from_str(&text).unwrap();
                                if frame["type"] == "ping" {
                                    continue;
                                }
                                if frame["type"] == "disconnect" {
                                    assert_eq!(frame["reconnect"], true, "{frame}");
                                    disconnected = true;
                                    continue;
                                }
                                let identifier = frame["identifier"].as_str().unwrap().to_string();
                                seen.entry(identifier)
                                    .or_default()
                                    .push(frame["message"]["n"].as_u64().unwrap());
                            }
                            Ok(Message::Close(_)) | Err(_) => closed = true,
                            Ok(_) => {}
                        }
                    }
                };
                let timed_out = tokio::time::timeout(READ_DEADLINE, read).await.is_err();
                (seen, disconnected, closed, timed_out)
            })
        })
        .collect();

    // The burst: back to back, no reading in between.
    let payload_filler = "x".repeat(1024);
    for n in 0..PUBLICATIONS {
        let payload = json!({ "n": n, "filler": payload_filler }).to_string();
        server.pubsub.publish("chat:fanout", &payload).await.expect("published");
    }

    let mut complete = 0;
    let mut told_to_reconnect = 0;
    for reader in readers {
        let (seen, disconnected, closed, timed_out) = reader.await.expect("reader task");
        let mut frames = 0;
        for (identifier, sequence) in &seen {
            let expected: Vec<u64> = (0..sequence.len() as u64).collect();
            assert_eq!(sequence, &expected, "{identifier}: frames arrive in order, no gap");
            frames += sequence.len();
        }
        if frames == IDENTIFIERS_PER_SOCKET * PUBLICATIONS {
            complete += 1;
        } else {
            assert!(
                disconnected,
                "a socket short of frames ({frames}; closed: {closed}, timed out: {timed_out}) \
                 must have been told to reconnect"
            );
            told_to_reconnect += 1;
        }
    }
    assert_eq!(complete + told_to_reconnect, SOCKETS);

    // The frames that reached clients were counted as delivered, and nothing
    // was lost anywhere but a socket's own queue: no shared queue exists on
    // this transport. With the default per-socket bound the burst fits, so
    // nobody lagged at all.
    let scrape = server.transport.connections().backend().render_metrics();
    assert!(scrape.contains("orbitcast_hub_queue_overflow_total 0"), "{scrape}");
    assert!(
        scrape.contains(&format!(
            "orbitcast_socket_lag_disconnects_total{{reason=\"queue\"}} {told_to_reconnect}"
        )),
        "every short socket is a counted lag close:\n{scrape}"
    );
    assert_eq!(told_to_reconnect, 0, "the burst fits the per-socket queue; nobody should lag");
}

// --- metrics exposure --------------------------------------------------------

#[tokio::test]
async fn metrics_are_absent_from_the_public_listener_by_default() {
    let server = start(base_config()).await;
    assert_eq!(
        http_get(&server, "/metrics").await.status(),
        StatusCode::NOT_FOUND,
        "an administrative route must not appear on a browser-facing port by accident"
    );
}

#[tokio::test]
async fn metrics_public_serves_a_prometheus_scrape_with_live_gauges() {
    let mut config = base_config();
    config.http.metrics_public = true;
    let server = start(config).await;

    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    let id = identifier("chat:42");
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": &id })).await;
    assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");

    server.pubsub.publish("chat:42", r#"{"body":"counted"}"#).await.expect("published");
    assert_eq!(next_frame(&mut socket).await["identifier"], id);

    let response = http_get(&server, "/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").and_then(|value| value.to_str().ok()),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );

    let scrape = body_text(response).await;
    assert!(scrape.contains("orbitcast_connections_current 1"), "{scrape}");
    assert!(scrape.contains("orbitcast_connections_total 1"), "{scrape}");
    assert!(scrape.contains("orbitcast_subscriptions_current 1"), "{scrape}");
    assert!(scrape.contains("orbitcast_streams_with_interest 1"), "{scrape}");
    assert!(scrape.contains("orbitcast_pubsub_received_total 1"), "{scrape}");
    assert!(scrape.contains("orbitcast_pubsub_delivered_total 1"), "{scrape}");

    // No identifier, stream name, or address is ever a label.
    assert!(!scrape.contains("chat:42"), "no per-stream labels:\n{scrape}");
    assert!(!scrape.contains("127.0.0.1"), "no per-address labels:\n{scrape}");
}

#[tokio::test]
async fn shutdown_advertises_a_reconnectable_disconnect_and_drains() {
    let server = start(base_config()).await;
    let mut socket = connect(&server).await;
    assert_eq!(next_frame(&mut socket).await["type"], "welcome");

    let id = identifier("chat:42");
    send_command(&mut socket, json!({ "command": "subscribe", "identifier": id })).await;
    assert_eq!(next_frame(&mut socket).await["type"], "confirm_subscription");

    // Drain runs concurrently with the client still reading, which is the real
    // shutdown shape: advertise, close, then tear down.
    let transport = Arc::clone(&server.transport);
    let drain = tokio::spawn(async move { transport.drain(Duration::from_secs(2)).await });

    let disconnect = next_frame(&mut socket).await;
    assert_eq!(disconnect["type"], "disconnect");
    assert_eq!(disconnect["reason"], "server_restart");
    assert_eq!(disconnect["reconnect"], true);

    // The socket is then closed by the server, not left hanging.
    let closed =
        tokio::time::timeout(TIMEOUT, socket.recv_message()).await.expect("close within deadline");
    assert_matches!(
        closed,
        Ok(Message::Close(_)) | Err(_),
        "server closes the socket after the disconnect frame"
    );

    tokio::time::timeout(TIMEOUT, drain)
        .await
        .expect("drain completes within the deadline")
        .expect("drain task");

    // Every session went through teardown, and the backend interest with it.
    assert_eq!(server.transport.connections().session_count(), 0);
    assert!(!server.pubsub.is_subscribed("chat:42"));
}
