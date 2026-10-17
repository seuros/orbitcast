//! End-to-end tests for the private HTTP ingress and the Rails-initiated
//! control path.
//!
//! The memory-backend module runs the real listeners - rama ingress, rama
//! `/cable` WebSocket - so a publication `POSTed` by the "Rails" side reaches a
//! subscribed browser through the same backend path every build uses.
//!
//! The PostgreSQL module (`#[ignore]`, needs `DATABASE_URL`) exercises the
//! control LISTEN round trip exactly as the Ruby publisher drives it: a raw
//! `pg_notify` on the derived control channel.

// Every binding clippy::significant_drop_tightening flags in this file is a
// fixture of Arcs and JoinHandles, never a lock guard: the lint walks field
// types transitively and reaches `RpcClient`'s rama connection pool (or a rama
// HTTP client's own pool), whose slots are tokio semaphore permits marked
// `#[clippy::has_significant_drop]`.
#![expect(
    clippy::significant_drop_tightening,
    reason = "TestServer/Backend/Connections are fixtures reaching pool permits, not lock guards"
)]

#[cfg(feature = "memory")]
#[path = "support/ws.rs"]
mod ws;

/// Tests that need a publishable in-process backend.
#[cfg(feature = "memory")]
mod memory_ingress {
    use std::assert_matches;
    use std::sync::Arc;
    use std::time::Duration;

    use rama::Service as _;

    use rama::http::client::EasyHttpWebClient;

    use rama::http::ws::Message;
    use rama::http::ws::handshake::client::ClientWebSocket;
    use rama::http::{Body, Request, Response, StatusCode};

    use serde_json::{Value, json};

    use orbitcast::MemoryPubSub;
    use orbitcast::config::{Config, HttpConfig, IngressSettings};
    use orbitcast::server::{Backend, Shutdown};
    use orbitcast::streams::SecretBytes;
    use orbitcast::transport::http::{HttpTransport, bind};
    use orbitcast::transport::ingress::{CONTROL_PATH, IngressTransport, PUBLICATIONS_PATH};

    use super::ws::{TIMEOUT, next_frame, send_command};

    const NAMESPACE: &str = "orbitcast_test";
    const TOKEN: &str = "ingress-token";

    struct TestServer {
        cable_addr: std::net::SocketAddr,
        ingress_addr: std::net::SocketAddr,
        transport: Arc<HttpTransport>,
        _listener_task: tokio::task::JoinHandle<()>,
        _serve_task: tokio::task::JoinHandle<()>,
        _ingress_task: tokio::task::JoinHandle<()>,
    }

    fn base_config() -> Config {
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
            broker: orbitcast::broker::BrokerConfig::default(),
            recovery_secret: None,
            streams: orbitcast::streams::StreamsConfig::default(),
            rpc: orbitcast::config::RpcSettings::default(),
            ingress: IngressSettings {
                listen_addr: Some("127.0.0.1:0".to_string()),
                token: Some(SecretBytes::new(TOKEN.as_bytes().to_vec())),
                allow_nonlocal: false,
            },
            explicit: orbitcast::config::ExplicitKeys::default(),
        };
        config.streams.public = true;
        config.streams.public_insecure = true;
        config
    }

    async fn start(config: Config) -> TestServer {
        config.validate().expect("test config is valid");

        let pubsub = Arc::new(MemoryPubSub::new());
        let backend = Arc::new(
            Backend::with_pubsub(Arc::clone(&pubsub) as Arc<dyn orbitcast::pubsub::PubSub>)
                .with_namespace(NAMESPACE),
        );
        let listener_task = backend.spawn_listener().expect("pub/sub listener starts");

        let shutdown = Shutdown::new();
        let transport = HttpTransport::new(Arc::clone(&backend), &config, shutdown.clone())
            .expect("transport builds");
        let cable = bind(config.http.listen_addr.as_deref().unwrap()).await.expect("bind cable");
        let cable_addr = cable.local_addr();
        let serve_task = tokio::spawn(Arc::clone(&transport).serve(cable));

        let ingress = IngressTransport::new(Arc::clone(&backend), &config, shutdown.clone())
            .expect("ingress builds");
        let listener =
            bind(config.ingress.listen_addr.as_deref().unwrap()).await.expect("bind ingress");
        let ingress_addr = listener.local_addr();
        let ingress_task = tokio::spawn(ingress.serve(listener));

        TestServer {
            cable_addr,
            ingress_addr,
            transport,
            _listener_task: listener_task,
            _serve_task: serve_task,
            _ingress_task: ingress_task,
        }
    }

    async fn connect(server: &TestServer) -> ClientWebSocket {
        super::ws::connect(format!("ws://{}/cable", server.cable_addr)).await
    }

    async fn subscribe(socket: &mut ClientWebSocket, stream: &str) -> String {
        let id =
            json!({ "channel": "Orbitcast::StreamsChannel", "stream_name": stream }).to_string();
        send_command(socket, json!({ "command": "subscribe", "identifier": id })).await;
        let confirm = next_frame(socket).await;
        assert_eq!(confirm["type"], "confirm_subscription");
        id
    }

    async fn post(server: &TestServer, path: &str, token: Option<&str>, body: String) -> Response {
        let client = EasyHttpWebClient::default();
        let mut builder = Request::post(format!("http://{}{path}", server.ingress_addr))
            .header("content-type", "application/json");
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        let request = builder.body(Body::from(body)).expect("request builds");
        tokio::time::timeout(TIMEOUT, client.serve(request))
            .await
            .expect("response within deadline")
            .expect("http response")
    }

    fn publication_body(namespace: &str, stream: &str, payload: &str) -> String {
        json!({
            "version": 1,
            "namespace": namespace,
            "id": uuid::Uuid::new_v4().to_string(),
            "stream": stream,
            "json_payload": payload,
        })
        .to_string()
    }

    #[tokio::test]
    async fn a_posted_publication_reaches_a_subscribed_websocket_client() {
        let server = start(base_config()).await;
        let mut socket = connect(&server).await;
        assert_eq!(next_frame(&mut socket).await["type"], "welcome");
        let id = subscribe(&mut socket, "chat:42").await;

        let body = publication_body(NAMESPACE, "chat:42", r#"{"body":"over http","n":1}"#);
        let posted: Value = serde_json::from_str(&body).unwrap();
        let response = post(&server, PUBLICATIONS_PATH, Some(TOKEN), body).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // The 202 body is the minimal ack envelope, echoing the caller's id.
        let ack: Value = serde_json::from_str(
            &rama::http::BodyExtractExt::try_into_string(response.into_body()).await.unwrap(),
        )
        .unwrap();
        assert_eq!(ack["version"], 1);
        assert_eq!(ack["publication_id"], posted["id"]);

        let message = next_frame(&mut socket).await;
        assert_eq!(message["identifier"], id);
        assert_eq!(message["message"], json!({ "body": "over http", "n": 1 }));
    }

    #[tokio::test]
    async fn a_wrong_or_missing_token_is_401_and_nothing_is_published() {
        let server = start(base_config()).await;
        let mut socket = connect(&server).await;
        assert_eq!(next_frame(&mut socket).await["type"], "welcome");
        subscribe(&mut socket, "chat:42").await;

        let body = publication_body(NAMESPACE, "chat:42", r#"{"body":"stolen"}"#);
        let response = post(&server, PUBLICATIONS_PATH, Some("wrong-token"), body.clone()).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = post(&server, PUBLICATIONS_PATH, None, body).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let idle = tokio::time::timeout(Duration::from_millis(250), socket.recv_message()).await;
        assert!(
            idle.is_err()
                || matches!(&idle, Ok(Ok(Message::Text(text))) if text.contains("\"ping\"")),
            "an unauthenticated publication must not be delivered"
        );
    }

    #[tokio::test]
    async fn a_foreign_namespace_is_403() {
        let server = start(base_config()).await;
        let body = publication_body("other_app", "chat:42", r#"{"body":"foreign"}"#);
        let response = post(&server, PUBLICATIONS_PATH, Some(TOKEN), body).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn an_oversized_publication_is_413() {
        let mut config = base_config();
        // The smallest configurable ceiling: the NOTIFY payload limit.
        config.overflow.max_publication_size = 8000;
        let server = start(config).await;

        let payload = format!(r#"{{"html":"{}"}}"#, "x".repeat(8000));
        let body = publication_body(NAMESPACE, "chat:42", &payload);
        let response = post(&server, PUBLICATIONS_PATH, Some(TOKEN), body).await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn get_on_ingress_routes_is_405() {
        let server = start(base_config()).await;
        let client = EasyHttpWebClient::default();
        for path in [PUBLICATIONS_PATH, CONTROL_PATH] {
            let request = Request::get(format!("http://{}{path}", server.ingress_addr))
                .body(Body::empty())
                .unwrap();
            let response = client.serve(request).await.expect("http response");
            assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "{path}");
        }
    }

    #[tokio::test]
    async fn a_control_post_disconnects_the_identity_indexed_session() {
        let server = start(base_config()).await;
        let mut socket = connect(&server).await;
        assert_eq!(next_frame(&mut socket).await["type"], "welcome");

        // The identity index the Rails-dispatch `identify` path populates;
        // this transport's first connection has conn_id 1.
        server.transport.connections().hub().set_identifier(1, "user:7".to_string()).expect("live");

        let control = json!({
            "version": 1,
            "kind": "disconnect",
            "namespace": NAMESPACE,
            "control_id": uuid::Uuid::new_v4().to_string(),
            "identity_key": "user:7",
            "reason": "remote",
            "reconnect": false,
        })
        .to_string();
        let response = post(&server, CONTROL_PATH, Some(TOKEN), control).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // The standard ActionCable disconnect frame first...
        let disconnect = next_frame(&mut socket).await;
        assert_eq!(disconnect["type"], "disconnect");
        assert_eq!(disconnect["reason"], "remote");
        assert_eq!(disconnect["reconnect"], false);

        // ...then the socket close, then the ordinary teardown path.
        let closed = tokio::time::timeout(TIMEOUT, socket.recv_message())
            .await
            .expect("close within deadline");
        assert_matches!(
            closed,
            Ok(Message::Close(_)) | Err(_),
            "server closes the socket after the disconnect frame"
        );

        tokio::time::timeout(TIMEOUT, async {
            while server.transport.connections().session_count() > 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the session goes through teardown");
    }

    #[tokio::test]
    async fn a_control_post_for_an_unknown_identity_is_an_accepted_no_op() {
        let server = start(base_config()).await;
        let mut socket = connect(&server).await;
        assert_eq!(next_frame(&mut socket).await["type"], "welcome");
        server.transport.connections().hub().set_identifier(1, "user:7".to_string()).expect("live");

        let control = json!({
            "version": 1,
            "kind": "disconnect",
            "namespace": NAMESPACE,
            "control_id": uuid::Uuid::new_v4().to_string(),
            "identity_key": "user:404",
            "reason": "remote",
            "reconnect": false,
        })
        .to_string();
        let response = post(&server, CONTROL_PATH, Some(TOKEN), control).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let idle = tokio::time::timeout(Duration::from_millis(250), socket.recv_message()).await;
        assert!(
            idle.is_err()
                || matches!(&idle, Ok(Ok(Message::Text(text))) if text.contains("\"ping\"")),
            "an unmatched identity key must disconnect nobody"
        );
        assert_eq!(server.transport.connections().session_count(), 1);
    }
}

/// Live-PostgreSQL control round trip, exactly as the Ruby publisher drives
/// it. Run with:
///
/// ```text
/// DATABASE_URL=postgres://... cargo test --test ingress -- --ignored
/// ```
#[cfg(all(feature = "postgres", not(feature = "memory")))]
mod postgres_control {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc;

    use orbitcast::config::{Config, HttpConfig, IngressSettings};
    use orbitcast::protocol::v1::control::ControlMessage;
    use orbitcast::pubsub::{PostgresConfig, PostgresPubSub, control_channel_for};
    use orbitcast::server::{Backend, Connections};
    use orbitcast::transport::docking::Outgoing;

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// Two connections subscribe to one stream while its LISTEN is in flight:
    /// the second must wait for the acknowledgement instead of confirming onto
    /// a stream nobody is listening to yet. Both subscribes are driven at
    /// once, as their connection tasks would be.
    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn a_second_subscriber_waits_for_the_pending_listen() {
        let ps = Arc::new(
            PostgresPubSub::connect(PostgresConfig::new(integration_dsn(), namespace("pending")))
                .await
                .expect("connect to DATABASE_URL"),
        );
        let backend = Arc::new(Backend::with_pubsub(ps as Arc<dyn orbitcast::pubsub::PubSub>));
        let (tx, mut rx) = mpsc::channel(64);
        let driver = Connections::new(backend, &test_config(), tx).expect("driver");
        for id in [1, 2] {
            driver.accept(id, "/cable".into(), "127.0.0.1".into(), HashMap::new()).await;
            rx.try_recv().expect("welcome");
        }
        let identifier = serde_json::json!({
            "channel": "Orbitcast::StreamsChannel",
            "stream_name": "pending:listen",
        })
        .to_string();
        let frame =
            serde_json::json!({"command": "subscribe", "identifier": identifier}).to_string();

        // Both subscribes are queued on their sessions' mailboxes without
        // waiting: the transport is never blocked by a LISTEN round trip.
        driver.handle_frame(1, frame.as_bytes()).await;
        driver.handle_frame(2, frame.as_bytes()).await;
        assert!(rx.try_recv().is_err(), "nothing is confirmed before LISTEN is acknowledged");

        // The first member's LISTEN is acknowledged by PostgreSQL, the second
        // waited at the stream's gate for it: both confirm, first then second,
        // and exactly one LISTEN happened for the stream.
        let mut confirmed = Vec::new();
        tokio::time::timeout(TIMEOUT, async {
            while confirmed.len() < 2 {
                match rx.recv().await.expect("queue open") {
                    Outgoing::Cargo(cargo) => {
                        let frame: serde_json::Value =
                            serde_json::from_slice(&cargo.data).expect("frame is JSON");
                        assert_eq!(frame["type"], "confirm_subscription", "{frame}");
                        confirmed.push(cargo.conn_id);
                    }
                    other @ Outgoing::Disembark(_) => {
                        panic!("unexpected outgoing message: {other:?}")
                    }
                }
            }
        })
        .await
        .expect("both subscribes finish once LISTEN is acknowledged");
        assert_eq!(confirmed, [1, 2], "each subscriber is confirmed once, in order");
        driver.drain().await;
    }

    fn integration_dsn() -> String {
        std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set for #[ignore] postgres integration tests")
    }

    fn namespace(label: &str) -> String {
        format!("itest_ctrl_{}_{}", label, std::process::id())
    }

    fn test_config() -> Config {
        let mut config = Config {
            docking: None,
            http: HttpConfig { listen_addr: Some("127.0.0.1:0".to_string()), ..Default::default() },
            shutdown_timeout: Duration::from_secs(5),
            database_url: None,
            listener_database_url: None,
            namespace: String::new(),
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
            rpc: orbitcast::config::RpcSettings::default(),
            ingress: IngressSettings::default(),
            explicit: orbitcast::config::ExplicitKeys::default(),
        };
        config.streams.public = true;
        config.streams.public_insecure = true;
        config
    }

    /// A raw client for issuing the Ruby-style `pg_notify`.
    async fn raw_client() -> tokio_postgres::Client {
        let (client, connection) =
            tokio_postgres::connect(&integration_dsn(), tokio_postgres::NoTls)
                .await
                .expect("connect to DATABASE_URL (NoTls; use a non-TLS DSN for these tests)");
        tokio::spawn(async move {
            // The connection ends with the client at test end; a failure
            // before that already fails the query that hit it.
            if let Err(e) = connection.await {
                eprintln!("pg_notify connection ended: {e}");
            }
        });
        client
    }

    /// Publish one control message the way `Orbitcast::Publisher::Control`
    /// does: `SELECT pg_notify(channel_for(ns, CONTROL_STREAM), message.to_json)`.
    async fn notify_control(client: &tokio_postgres::Client, ns: &str, wire: &str) {
        client
            .execute("SELECT pg_notify($1, $2)", &[&control_channel_for(ns), &wire])
            .await
            .expect("pg_notify");
    }

    /// Wait for the next disconnect frame + close pair on the outbound queue.
    async fn expect_disconnect(rx: &mut mpsc::Receiver<Outgoing>) -> (serde_json::Value, u32) {
        let mut frame = None;
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let message = tokio::time::timeout(remaining, rx.recv())
                .await
                .expect("outbound event within deadline")
                .expect("queue open");
            match message {
                Outgoing::Cargo(cargo) => {
                    let parsed: serde_json::Value =
                        serde_json::from_slice(&cargo.data).expect("frame is JSON");
                    if parsed["type"] == "disconnect" {
                        frame = Some(parsed);
                    }
                }
                Outgoing::Disembark(disembark) => {
                    return (
                        frame.expect("disconnect frame precedes the close"),
                        disembark.conn_id,
                    );
                }
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn a_ruby_style_control_notify_disconnects_the_indexed_session() {
        let ns = namespace("roundtrip");
        let pubsub = Arc::new(
            PostgresPubSub::connect(PostgresConfig::new(integration_dsn(), ns.clone()))
                .await
                .expect("connect to DATABASE_URL"),
        );
        let backend = Arc::new(
            Backend::with_pubsub(pubsub as Arc<dyn orbitcast::pubsub::PubSub>)
                .with_namespace(ns.clone()),
        );
        let listener = backend.spawn_listener().expect("listener starts");

        let (tx, mut rx) = mpsc::channel(64);
        let connections =
            Connections::new(Arc::clone(&backend), &test_config(), tx).expect("driver");
        connections
            .accept(1, "/cable".into(), "127.0.0.1".into(), std::collections::HashMap::new())
            .await;
        connections
            .accept(2, "/cable".into(), "127.0.0.1".into(), std::collections::HashMap::new())
            .await;
        connections.hub().set_identifier(1, "user:7".to_string()).expect("live");
        connections.hub().set_identifier(2, "user:8".to_string()).expect("live");
        // Drain the welcomes.
        while rx.try_recv().is_ok() {}

        let client = raw_client().await;
        let control = ControlMessage::disconnect(&ns, "user:7", "remote", false);
        let wire = serde_json::to_string(&control).unwrap();
        notify_control(&client, &ns, &wire).await;

        let (frame, conn_id) = expect_disconnect(&mut rx).await;
        assert_eq!(conn_id, 1, "only the matching identity is disconnected");
        assert_eq!(frame["reason"], "remote");
        assert_eq!(frame["reconnect"], false);

        // Duplicate control_id: consumed, never applied twice.
        notify_control(&client, &ns, &wire).await;
        // An unknown identity key: a logged no-op.
        let stranger = ControlMessage::disconnect(&ns, "user:404", "remote", true);
        notify_control(&client, &ns, &serde_json::to_string(&stranger).unwrap()).await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut leftovers = Vec::new();
        while let Ok(message) = rx.try_recv() {
            leftovers.push(message);
        }
        assert!(
            leftovers.is_empty(),
            "duplicates and unknown identities must produce nothing: {leftovers:?}"
        );

        listener.abort();
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn a_foreign_namespace_control_notify_is_invisible() {
        // Channel derivation is namespace-scoped: a control message published
        // for another namespace lands on a channel this node never LISTENs
        // on, so nothing arrives at all.
        let ns = namespace("foreign");
        let pubsub = Arc::new(
            PostgresPubSub::connect(PostgresConfig::new(integration_dsn(), ns.clone()))
                .await
                .expect("connect to DATABASE_URL"),
        );
        let backend = Arc::new(
            Backend::with_pubsub(pubsub as Arc<dyn orbitcast::pubsub::PubSub>)
                .with_namespace(ns.clone()),
        );
        let listener = backend.spawn_listener().expect("listener starts");

        let (tx, mut rx) = mpsc::channel(64);
        let connections =
            Connections::new(Arc::clone(&backend), &test_config(), tx).expect("driver");
        connections
            .accept(1, "/cable".into(), "127.0.0.1".into(), std::collections::HashMap::new())
            .await;
        connections.hub().set_identifier(1, "user:7".to_string()).expect("live");
        while rx.try_recv().is_ok() {}

        let client = raw_client().await;
        let other_ns = format!("{ns}_other");
        let control = ControlMessage::disconnect(&other_ns, "user:7", "remote", false);
        notify_control(&client, &other_ns, &serde_json::to_string(&control).unwrap()).await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(rx.try_recv().is_err(), "a foreign-namespace control must not arrive");
        assert_eq!(connections.session_count(), 1);

        listener.abort();
    }
}
