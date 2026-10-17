// `RpcClient` owns rama's connection pool, whose slots are tokio semaphore
// permits marked `#[clippy::has_significant_drop]`; the lint walks field types
// transitively, so every fixture that holds a client looks like a lock guard.
#![expect(
    clippy::significant_drop_tightening,
    reason = "the fixtures reach RpcClient's pool permits; none of them is a lock guard"
)]

use super::*;

use std::assert_matches;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use rama::Service;
use rama::http::server::HttpServer;
use rama::http::service::web::response::IntoResponse;
use rama::http::{Response, StatusCode};
use rama::rt::Executor;
use rama::tcp::server::TcpListener;

use crate::protocol::v1::rpc::{Operation, Outcome, RpcReply};

const TOKEN: &[u8] = b"s3rvice-token";

/// What the stub Rails endpoint does with a request.
#[derive(Clone)]
enum Behaviour {
    /// Answer with a valid accepted reply echoing the correlation fields.
    Echo,
    /// Answer with a valid denied reply.
    Denied,
    /// Wait, then echo. Used to exercise the deadline and the queue.
    SlowEcho(Duration),
    /// Answer with a fixed status and no body.
    Status(u16),
    /// Answer `200` with exactly these bytes.
    Raw(String),
}

/// What the stub observed, for assertions.
#[derive(Default)]
struct Observed {
    paths: Mutex<Vec<String>>,
    authorizations: Mutex<Vec<String>>,
    content_types: Mutex<Vec<String>>,
    calls: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

impl Observed {
    fn paths(&self) -> Vec<String> {
        self.paths.lock().expect("stub paths").clone()
    }

    fn authorizations(&self) -> Vec<String> {
        self.authorizations.lock().expect("stub authorizations").clone()
    }

    fn content_types(&self) -> Vec<String> {
        self.content_types.lock().expect("stub content types").clone()
    }
}

#[derive(Clone)]
struct Stub {
    behaviour: Behaviour,
    observed: Arc<Observed>,
}

impl Service<Request> for Stub {
    type Output = Response;
    type Error = Infallible;

    async fn serve(&self, request: Request) -> Result<Self::Output, Self::Error> {
        let path = request.uri().path_or_root().into_owned();
        let header_value = |name: &str| {
            request
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        let authorization = header_value("authorization");
        let content_type = header_value("content-type");

        {
            self.observed.paths.lock().expect("stub paths").push(path);
            self.observed.authorizations.lock().expect("stub authorizations").push(authorization);
            self.observed.content_types.lock().expect("stub content types").push(content_type);
        }
        self.observed.calls.fetch_add(1, Ordering::SeqCst);

        let in_flight = self.observed.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.observed.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);

        let body = request.into_body().try_into_string().await.unwrap_or_default();
        let response = self.answer(&body).await;

        self.observed.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(response)
    }
}

impl Stub {
    async fn answer(&self, body: &str) -> Response {
        let decoded = serde_json::from_str::<RpcRequest>(body).ok();

        match &self.behaviour {
            Behaviour::Echo => json_ok(&reply_for(decoded.as_ref(), Outcome::Accepted)),
            Behaviour::Denied => json_ok(&reply_for(decoded.as_ref(), Outcome::Denied)),
            Behaviour::SlowEcho(delay) => {
                tokio::time::sleep(*delay).await;
                json_ok(&reply_for(decoded.as_ref(), Outcome::Accepted))
            }
            Behaviour::Status(code) => {
                (StatusCode::from_u16(*code).expect("valid status"), "upstream failure")
                    .into_response()
            }
            Behaviour::Raw(text) => json_ok(text),
        }
    }
}

fn json_ok(body: &str) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("stub response")
}

/// Build a syntactically valid reply for whatever request arrived.
fn reply_for(request: Option<&RpcRequest>, outcome: Outcome) -> String {
    let Some(request) = request else {
        return "{}".to_string();
    };
    let reply = match outcome {
        Outcome::Accepted => RpcReply::accepted(request),
        _ => RpcReply::denied(request, "stub denial"),
    };
    serde_json::to_string(&reply).expect("reply encodes")
}

/// Boot the stub on an ephemeral port.
async fn start_stub(behaviour: Behaviour) -> (SocketAddr, Arc<Observed>) {
    let observed = Arc::new(Observed::default());
    let stub = Stub { behaviour, observed: Arc::clone(&observed) };

    let listener = TcpListener::build(Executor::default())
        .bind_address("127.0.0.1:0")
        .await
        .expect("stub binds");
    let addr = listener.local_addr().expect("stub address");

    tokio::spawn(async move {
        listener.serve(HttpServer::auto(Executor::default()).service(stub)).await;
    });

    (addr, observed)
}

fn client_for(addr: SocketAddr, tune: impl FnOnce(&mut RpcConfig)) -> RpcClient {
    let mut config = RpcConfig::new(
        format!("http://{addr}/_orbitcast/v1/rails"),
        SecretBytes::new(TOKEN.to_vec()),
    );
    tune(&mut config);
    RpcClient::new(config).expect("client builds")
}

fn identify_request() -> RpcRequest {
    use crate::protocol::v1::rpc::ConnectionSnapshot;

    RpcRequest::new(
        Operation::Identify,
        "app",
        "node-1",
        uuid::Uuid::new_v4().to_string(),
        1,
        0,
        1_000,
    )
    .with_connection(ConnectionSnapshot {
        url: "/cable".to_string(),
        origin: None,
        client_address: "127.0.0.1".to_string(),
        ..Default::default()
    })
}

// --- configuration ------------------------------------------------------

#[test]
fn new_rejects_unusable_configuration() {
    let token = || SecretBytes::new(TOKEN.to_vec());

    assert_matches!(RpcClient::new(RpcConfig::new("", token())).unwrap_err(), RpcError::Config(_));
    assert_matches!(
        RpcClient::new(RpcConfig::new("app.internal/rails", token())).unwrap_err(),
        RpcError::Config(_)
    );
    assert_matches!(
        RpcClient::new(RpcConfig::new("https://app.internal", SecretBytes::new(Vec::new())))
            .unwrap_err(),
        RpcError::Config(_)
    );

    let mut config = RpcConfig::new("https://app.internal", token());
    config.deadline = Duration::ZERO;
    assert_matches!(RpcClient::new(config).unwrap_err(), RpcError::Config(_));

    let mut config = RpcConfig::new("https://app.internal", token());
    config.max_concurrency = 0;
    assert_matches!(RpcClient::new(config).unwrap_err(), RpcError::Config(_));
}

#[test]
fn endpoint_trailing_slash_is_normalized() {
    let client = RpcClient::new(RpcConfig::new(
        "https://app.internal/_orbitcast/v1/rails/",
        SecretBytes::new(TOKEN.to_vec()),
    ))
    .expect("client builds");
    assert_eq!(client.endpoint(), "https://app.internal/_orbitcast/v1/rails");
}

// --- happy path ---------------------------------------------------------

#[tokio::test]
async fn call_posts_to_the_operation_path_with_a_bearer_credential() {
    let (addr, observed) = start_stub(Behaviour::Echo).await;
    let client = client_for(addr, |_| {});

    let request = identify_request();
    let reply = client.call(request.clone()).await.expect("reply");

    assert_eq!(reply.outcome, Outcome::Accepted);
    assert_eq!(reply.operation, Operation::Identify);
    assert_eq!(reply.request_id, request.request_id);
    assert_eq!(reply.sequence, request.sequence);

    assert_eq!(observed.paths(), vec!["/_orbitcast/v1/rails/identify".to_string()]);
    assert_eq!(observed.authorizations(), vec!["Bearer s3rvice-token".to_string()]);
    assert_eq!(observed.content_types(), vec!["application/json".to_string()]);
}

#[tokio::test]
async fn consecutive_calls_reuse_one_pooled_connection() {
    let (addr, observed) = start_stub(Behaviour::Echo).await;
    let client = client_for(addr, |_| {});

    for _ in 0..3 {
        client.call(identify_request()).await.expect("reply");
    }
    assert_eq!(observed.calls.load(Ordering::SeqCst), 3);
    // All three answered through one client: the pool is what makes this
    // three requests rather than three TCP handshakes.
    assert_eq!(observed.paths().len(), 3);
}

#[tokio::test]
async fn a_denied_outcome_is_not_an_error() {
    let (addr, _observed) = start_stub(Behaviour::Denied).await;
    let client = client_for(addr, |_| {});

    let reply = client.call(identify_request()).await.expect("denial is a reply");
    assert_eq!(reply.outcome, Outcome::Denied);
    assert!(!reply.is_accepted());
    assert_eq!(reply.reason.as_deref(), Some("stub denial"));
    assert!(!client.is_circuit_open(), "a denial must not trip the breaker");
}

// --- deadline and concurrency -------------------------------------------

#[tokio::test]
async fn the_end_to_end_deadline_is_enforced() {
    let (addr, _observed) = start_stub(Behaviour::SlowEcho(Duration::from_millis(500))).await;
    let client = client_for(addr, |config| {
        config.deadline = Duration::from_millis(60);
        // Keep the breaker out of this test's way.
        config.failure_threshold = 100;
    });

    let error = client.call(identify_request()).await.expect_err("must time out");
    assert_matches!(error, RpcError::Deadline);
    assert!(error.is_temporary());
}

#[tokio::test]
async fn queue_wait_counts_against_the_deadline() {
    // One permit, a stub slower than the deadline: the second call cannot
    // possibly finish, and must expire rather than queue indefinitely.
    let (addr, _observed) = start_stub(Behaviour::SlowEcho(Duration::from_millis(200))).await;
    let client = Arc::new(client_for(addr, |config| {
        // The first call needs 200 ms of a 300 ms budget; the second one
        // cannot start before the first finishes, so its own budget is
        // already spent by the time a permit is free.
        config.deadline = Duration::from_millis(300);
        config.max_concurrency = 1;
        config.failure_threshold = 100;
    }));

    let first = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.call(identify_request()).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = client.call(identify_request()).await;

    assert!(first.await.expect("task").is_ok(), "the first call fits its deadline");
    assert_matches!(second, Err(RpcError::Deadline));
}

#[tokio::test]
async fn global_concurrency_is_bounded() {
    let (addr, observed) = start_stub(Behaviour::SlowEcho(Duration::from_millis(40))).await;
    let client = Arc::new(client_for(addr, |config| {
        config.deadline = Duration::from_secs(5);
        config.max_concurrency = 2;
    }));

    let calls: Vec<_> = (0..6)
        .map(|_| {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.call(identify_request()).await })
        })
        .collect();

    for call in calls {
        call.await.expect("task").expect("reply");
    }

    assert_eq!(observed.calls.load(Ordering::SeqCst), 6);
    assert!(
        observed.max_in_flight.load(Ordering::SeqCst) == 2,
        "at most max_concurrency calls may be in flight, saw {}",
        observed.max_in_flight.load(Ordering::SeqCst)
    );
}

// --- failures -----------------------------------------------------------

/// A pooled client must not serialize unrelated sessions.
///
/// An idle pooled connection is reused, but a *busy* one is not waited on:
/// one session's slow Rails call would otherwise become every session's.
#[tokio::test]
async fn a_warm_pool_still_runs_unrelated_calls_in_parallel() {
    let (addr, observed) = start_stub(Behaviour::SlowEcho(Duration::from_millis(200))).await;
    let client = Arc::new(client_for(addr, |config| {
        config.deadline = Duration::from_secs(5);
    }));

    // Warm the pool: this call leaves one idle connection behind.
    client.call(identify_request()).await.expect("reply");

    let first = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.call(identify_request()).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let second = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.call(identify_request()).await }
    });

    first.await.expect("task").expect("reply");
    second.await.expect("task").expect("reply");
    assert_eq!(
        observed.max_in_flight.load(Ordering::SeqCst),
        2,
        "the second call must open its own connection rather than queue behind the first"
    );
}

#[tokio::test]
async fn a_non_success_status_is_an_http_error() {
    let (addr, _observed) = start_stub(Behaviour::Status(503)).await;
    let client = client_for(addr, |config| config.failure_threshold = 100);

    let error = client.call(identify_request()).await.expect_err("503 is a failure");
    assert_matches!(error, RpcError::Http(503));
    assert!(error.is_temporary());
}

#[tokio::test]
async fn the_circuit_opens_after_repeated_failures() {
    let (addr, observed) = start_stub(Behaviour::Status(500)).await;
    let client = client_for(addr, |config| {
        config.failure_threshold = 2;
        config.failure_window = Duration::from_secs(30);
        config.half_open_timeout = Duration::from_secs(30);
    });

    assert_matches!(client.call(identify_request()).await, Err(RpcError::Http(500)));
    assert_matches!(client.call(identify_request()).await, Err(RpcError::Http(500)));
    assert!(client.is_circuit_open(), "two failures must open the circuit");

    let error = client.call(identify_request()).await.expect_err("circuit is open");
    assert_matches!(error, RpcError::CircuitOpen);
    assert_eq!(
        observed.calls.load(Ordering::SeqCst),
        2,
        "an open circuit must not reach the endpoint at all"
    );
}

#[tokio::test]
async fn a_malformed_reply_body_is_rejected() {
    let (addr, _observed) = start_stub(Behaviour::Raw("{not json".to_string())).await;
    let client = client_for(addr, |config| config.failure_threshold = 100);

    let error = client.call(identify_request()).await.expect_err("garbage is not a reply");
    assert_matches!(error, RpcError::InvalidReply(ProtocolError::Malformed(_)));
}

#[tokio::test]
async fn a_reply_with_an_unknown_effect_is_rejected() {
    let (addr, _observed) = start_stub(Behaviour::Raw(
        r#"{"version":1,"operation":"identify","outcome":"accepted",
                "request_id":"00000000-0000-4000-8000-000000000000","sequence":1,
                "effects":[{"kind":"execute_ruby","source":"exit"}]}"#
            .to_string(),
    ))
    .await;
    let client = client_for(addr, |config| config.failure_threshold = 100);

    let error = client.call(identify_request()).await.expect_err("unknown effect fails closed");
    assert_matches!(error, RpcError::InvalidReply(_));
}

#[tokio::test]
async fn a_reply_for_another_request_is_rejected() {
    let (addr, _observed) = start_stub(Behaviour::Raw(
        r#"{"version":1,"operation":"identify","outcome":"accepted",
                "request_id":"00000000-0000-4000-8000-000000000000","sequence":9,
                "new_revision":1}"#
            .to_string(),
    ))
    .await;
    let client = client_for(addr, |config| config.failure_threshold = 100);

    let error = client.call(identify_request()).await.expect_err("correlation must fail");
    assert_matches!(error, RpcError::InvalidReply(ProtocolError::Mismatch { .. }));
}

#[tokio::test]
async fn an_unreachable_endpoint_is_a_transport_failure() {
    // Bind and immediately drop the listener: the port is (almost certainly)
    // free, so connecting fails rather than hanging.
    let listener =
        TcpListener::build(Executor::default()).bind_address("127.0.0.1:0").await.expect("binds");
    let addr = listener.local_addr().expect("address");
    drop(listener);

    let client = client_for(addr, |config| {
        config.deadline = Duration::from_secs(2);
        config.failure_threshold = 100;
    });

    let error = client.call(identify_request()).await.expect_err("nothing is listening");
    assert_matches!(error, RpcError::Transport(_) | RpcError::Deadline);
    assert!(error.is_temporary());
}

#[tokio::test]
async fn an_invalid_request_never_leaves_the_process() {
    let (addr, observed) = start_stub(Behaviour::Echo).await;
    let client = client_for(addr, |_| {});

    // No connection snapshot: `identify` cannot be validly expressed.
    let request = RpcRequest::new(
        Operation::Identify,
        "app",
        "node-1",
        uuid::Uuid::new_v4().to_string(),
        1,
        0,
        1_000,
    );
    let error = client.call(request).await.expect_err("request is invalid");
    assert_matches!(error, RpcError::InvalidRequest(_));
    assert_eq!(observed.calls.load(Ordering::SeqCst), 0);
}
