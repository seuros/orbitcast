// Every binding clippy::significant_drop_tightening flags in this module is a
// fixture of Arcs and JoinHandles, never a lock guard: the lint walks field
// types transitively and reaches `RpcClient`'s rama connection pool (or a rama
// HTTP client's own pool), whose slots are tokio semaphore permits marked
// `#[clippy::has_significant_drop]`.
#![expect(
    clippy::significant_drop_tightening,
    reason = "IngressRouter (over Backend) is a fixture reaching pool permits, not a lock guard"
)]

use super::*;
use crate::protocol::v1::control::ControlMessage;
use crate::protocol::v1::rpc::CLOSE_REASON_REMOTE;
use crate::pubsub::test_support::ScriptedPubSub;
use crate::transport::test_support::{NAMESPACE, backend_unready, config};

const TOKEN: &[u8] = b"ingress-token";

fn test_config() -> Config {
    let mut config = config();
    config.ingress = crate::config::IngressSettings {
        listen_addr: Some("127.0.0.1:0".to_string()),
        token: Some(SecretBytes::new(TOKEN.to_vec())),
        allow_nonlocal: false,
    };
    config
}

fn transport(backend: Arc<Backend>) -> Arc<IngressTransport> {
    IngressTransport::new(backend, &test_config(), Shutdown::new()).expect("ingress builds")
}

fn router(backend: Arc<Backend>) -> IngressRouter {
    IngressRouter { transport: transport(backend) }
}

fn post(path: &str, token: Option<&str>, body: &str) -> Request {
    let mut builder = Request::post(format!("http://127.0.0.1{path}"))
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    builder.body(Body::from(body.to_string())).expect("request builds")
}

fn publication_body(namespace: &str, payload: &str) -> String {
    serde_json::to_string(&Publication::new(namespace, "chat:42", payload)).unwrap()
}

fn control_body() -> String {
    serde_json::to_string(&ControlMessage::disconnect(
        NAMESPACE,
        "user:7",
        CLOSE_REASON_REMOTE,
        false,
    ))
    .unwrap()
}

async fn status_of(router: &IngressRouter, req: Request) -> StatusCode {
    router.serve(req).await.expect("router is infallible").status()
}

#[test]
fn token_comparison_accepts_only_the_exact_credential() {
    let expected = SecretBytes::new(TOKEN.to_vec());
    assert!(token_matches(&expected, TOKEN));
    assert!(!token_matches(&expected, b"ingress-tokem"), "same length, different bytes");
    assert!(!token_matches(&expected, b"ingress-token-longer"), "length mismatch");
    assert!(!token_matches(&expected, b""), "empty presentation");
}

fn get(path: &str, token: Option<&str>) -> Request {
    let mut builder = Request::get(format!("http://127.0.0.1{path}"));
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    builder.body(Body::empty()).expect("request builds")
}

#[tokio::test]
async fn unknown_routes_are_404_and_known_routes_refuse_get() {
    let router = router(backend_unready());

    let get =
        Request::get(format!("http://127.0.0.1{PUBLICATIONS_PATH}")).body(Body::empty()).unwrap();
    let response = router.serve(get).await.unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers().get(header::ALLOW).and_then(|v| v.to_str().ok()), Some("POST"));

    let get = Request::get(format!("http://127.0.0.1{CONTROL_PATH}")).body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, get).await, StatusCode::METHOD_NOT_ALLOWED);

    let elsewhere = post("/v1/other", Some("ingress-token"), "{}");
    assert_eq!(status_of(&router, elsewhere).await, StatusCode::NOT_FOUND);
}

/// Every refusal - the 404 included - is the versioned `IngressError`
/// envelope with a stable code, never a bare string.
#[tokio::test]
async fn every_refusal_is_a_versioned_error_envelope() {
    let router = router(backend_unready());
    let cases: Vec<(Request, StatusCode, &str)> = vec![
        (post("/v1/nope", Some("ingress-token"), "{}"), StatusCode::NOT_FOUND, "not_found"),
        (
            get(PUBLICATIONS_PATH, Some("ingress-token")),
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
        ),
        (post(PUBLICATIONS_PATH, None, "{}"), StatusCode::UNAUTHORIZED, "unauthorized"),
        (
            post(PUBLICATIONS_PATH, Some("ingress-token"), "{oops"),
            StatusCode::BAD_REQUEST,
            "malformed_envelope",
        ),
        (
            post(PUBLICATIONS_PATH, Some("ingress-token"), r#"{"version":1,"namespace":""}"#),
            StatusCode::BAD_REQUEST,
            "invalid_envelope",
        ),
        (
            post(PUBLICATIONS_PATH, Some("ingress-token"), &publication_body("other_app", "{}")),
            StatusCode::FORBIDDEN,
            "foreign_namespace",
        ),
        (
            post(PUBLICATIONS_PATH, Some("ingress-token"), &publication_body(NAMESPACE, "{}")),
            StatusCode::SERVICE_UNAVAILABLE,
            "backend_unavailable",
        ),
        (
            post(CONTROL_PATH, Some("ingress-token"), "{oops"),
            StatusCode::BAD_REQUEST,
            "invalid_control",
        ),
    ];
    for (req, expected_status, expected_code) in cases {
        let response = router.serve(req).await.unwrap();
        assert_eq!(response.status(), expected_status, "{expected_code}");
        let body = response.into_body().try_into_string().await.expect("body");
        let error: IngressError =
            serde_json::from_str(&body).unwrap_or_else(|e| panic!("{expected_code}: {body}: {e}"));
        error.validate().expect("the error envelope is itself valid");
        assert_eq!(error.version, 1, "{expected_code}");
        assert_eq!(error.error, expected_code, "{body}");
        assert!(!error.message.is_empty(), "{body}");
    }
}

#[tokio::test]
async fn an_accepted_control_answers_with_the_control_ack_envelope() {
    let backend = Arc::new(Backend::with_pubsub(ScriptedPubSub::new()).with_namespace(NAMESPACE));
    let router = router(backend);
    let control = ControlMessage::disconnect(NAMESPACE, "user:7", CLOSE_REASON_REMOTE, false);
    let body = serde_json::to_string(&control).unwrap();

    for attempt in ["first", "duplicate"] {
        let response =
            router.serve(post(CONTROL_PATH, Some("ingress-token"), &body)).await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED, "{attempt}: idempotent");
        let ack: ControlAck =
            serde_json::from_str(&response.into_body().try_into_string().await.expect("body"))
                .expect("ack envelope");
        ack.validate().expect("ack is valid");
        assert_eq!(ack.version, 1);
        assert_eq!(ack.control_id, control.control_id, "{attempt}: the caller's id is echoed");
    }
}

#[tokio::test]
async fn a_missing_or_wrong_token_is_401_before_the_body_is_touched() {
    let router = router(backend_unready());
    let body = publication_body(NAMESPACE, "{}");

    for req in [
        post(PUBLICATIONS_PATH, None, &body),
        post(PUBLICATIONS_PATH, Some("wrong"), &body),
        post(CONTROL_PATH, Some("wrong"), &control_body()),
    ] {
        assert_eq!(status_of(&router, req).await, StatusCode::UNAUTHORIZED);
    }
}

#[tokio::test]
async fn malformed_and_invalid_publications_are_400() {
    let router = router(backend_unready());

    for body in ["{not json", r#"{"version":99}"#, r#"{"version":1,"namespace":""}"#] {
        let req = post(PUBLICATIONS_PATH, Some("ingress-token"), body);
        assert_eq!(status_of(&router, req).await, StatusCode::BAD_REQUEST, "body {body:?}");
    }
}

#[tokio::test]
async fn a_foreign_namespace_is_403() {
    let router = router(backend_unready());
    let req = post(PUBLICATIONS_PATH, Some("ingress-token"), &publication_body("other_app", "{}"));
    assert_eq!(status_of(&router, req).await, StatusCode::FORBIDDEN);

    let foreign = serde_json::to_string(&ControlMessage::disconnect(
        "other_app",
        "user:7",
        CLOSE_REASON_REMOTE,
        false,
    ))
    .unwrap();
    let req = post(CONTROL_PATH, Some("ingress-token"), &foreign);
    assert_eq!(status_of(&router, req).await, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn an_oversized_body_is_413_from_the_declared_length() {
    let router = router(backend_unready());
    let limit = router.transport.publication_body_limit();

    let req = Request::post(format!("http://127.0.0.1{PUBLICATIONS_PATH}"))
        .header(header::AUTHORIZATION, "Bearer ingress-token")
        .header(header::CONTENT_LENGTH, (limit + 1).to_string())
        .body(Body::empty())
        .unwrap();
    assert_eq!(status_of(&router, req).await, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn an_oversized_payload_is_413_after_decoding() {
    // The envelope fits the transport cap; the decoded payload exceeds
    // max_publication_size, which is the authoritative bound.
    let backend = backend_unready();
    let mut config = test_config();
    config.overflow.max_publication_size = MAX_NOTIFY_PAYLOAD_LEN;
    let router = IngressRouter {
        transport: IngressTransport::new(backend, &config, Shutdown::new()).unwrap(),
    };

    let payload = format!(r#"{{"html":"{}"}}"#, "x".repeat(MAX_NOTIFY_PAYLOAD_LEN));
    let req =
        post(PUBLICATIONS_PATH, Some("ingress-token"), &publication_body(NAMESPACE, &payload));
    assert_eq!(status_of(&router, req).await, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn a_valid_publication_without_a_backend_is_503() {
    let router = router(backend_unready());
    let req = post(PUBLICATIONS_PATH, Some("ingress-token"), &publication_body(NAMESPACE, "{}"));
    assert_eq!(status_of(&router, req).await, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn exhausted_permits_answer_429() {
    let router = router(backend_unready());
    let _held = Arc::clone(&router.transport.permits)
        .try_acquire_many_owned(u32::try_from(MAX_IN_FLIGHT).expect("the permit cap fits a u32"))
        .expect("all permits held");

    let req = post(PUBLICATIONS_PATH, Some("ingress-token"), &publication_body(NAMESPACE, "{}"));
    assert_eq!(status_of(&router, req).await, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn a_control_message_is_202_even_when_no_session_matches() {
    // Control acts locally: no pub/sub backend is required, and an
    // identity key with no local session is a normal multi-node no-op.
    let router = router(backend_unready());
    let response =
        router.serve(post(CONTROL_PATH, Some("ingress-token"), &control_body())).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = response.into_body().try_into_string().await.expect("body");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["version"], 1);
    assert!(json["control_id"].is_string());
}

#[tokio::test]
async fn the_metrics_route_is_authenticated_and_get_only() {
    let router = router(backend_unready());

    // No credential: refused before anything is rendered.
    assert_eq!(status_of(&router, get(METRICS_PATH, None)).await, StatusCode::UNAUTHORIZED);
    assert_eq!(
        status_of(&router, get(METRICS_PATH, Some("wrong"))).await,
        StatusCode::UNAUTHORIZED
    );

    // Wrong method on a known route is `405`, advertising the right one.
    let response =
        router.serve(post(METRICS_PATH, Some("ingress-token"), "")).await.expect("infallible");
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers().get(header::ALLOW).and_then(|v| v.to_str().ok()), Some("GET"));
}

#[tokio::test]
async fn an_authenticated_scrape_is_prometheus_text_without_identity_labels() {
    let router = router(backend_unready());

    let response =
        router.serve(get(METRICS_PATH, Some("ingress-token"))).await.expect("infallible");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()),
        Some(crate::metrics::CONTENT_TYPE)
    );

    let body = response.into_body().try_into_string().await.expect("body");
    assert!(body.contains("# TYPE orbitcast_connections_current gauge"), "{body}");
    assert!(body.contains("orbitcast_ingress_requests_total{status=\"200\"} 0"), "{body}");
    assert!(!body.contains(NAMESPACE), "the namespace is never a label:\n{body}");

    // A scrape counts itself - on the *next* scrape, because the counter is
    // incremented after the body is rendered.
    let next = router.serve(get(METRICS_PATH, Some("ingress-token"))).await.expect("infallible");
    let body = next.into_body().try_into_string().await.expect("body");
    assert!(body.contains("orbitcast_ingress_requests_total{status=\"200\"} 1"), "{body}");
    assert!(
        body.contains("orbitcast_ingress_requests_total{status=\"202\"} 0"),
        "a scrape is never counted as an accepted publication:\n{body}"
    );
}

#[tokio::test]
async fn every_reply_status_is_counted_exactly_once() {
    let backend = backend_unready();
    let router = router(Arc::clone(&backend));

    // One request per status this listener can produce, except `429`
    // (permit exhaustion has its own test) and `202` (needs a backend).
    let requests = [
        (post("/v1/nope", Some("ingress-token"), "{}"), StatusCode::NOT_FOUND),
        (get(PUBLICATIONS_PATH, Some("ingress-token")), StatusCode::METHOD_NOT_ALLOWED),
        (post(PUBLICATIONS_PATH, None, "{}"), StatusCode::UNAUTHORIZED),
        (post(PUBLICATIONS_PATH, Some("ingress-token"), "{oops"), StatusCode::BAD_REQUEST),
        (
            post(PUBLICATIONS_PATH, Some("ingress-token"), &publication_body("other_app", "{}")),
            StatusCode::FORBIDDEN,
        ),
        (
            post(PUBLICATIONS_PATH, Some("ingress-token"), &publication_body(NAMESPACE, "{}")),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ];
    for (req, expected) in requests {
        assert_eq!(status_of(&router, req).await, expected);
    }

    let scrape = backend.render_metrics();
    for status in ["404", "405", "401", "400", "403", "503"] {
        assert!(
            scrape.contains(&format!("orbitcast_ingress_requests_total{{status=\"{status}\"}} 1")),
            "status {status} must be counted exactly once:\n{scrape}"
        );
    }
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn an_accepted_publication_answers_202_with_its_own_id() {
    let pubsub = Arc::new(crate::pubsub::MemoryPubSub::new());
    let backend = Arc::new(Backend::with_pubsub(pubsub).with_namespace(NAMESPACE));
    let router = router(backend);

    let publication = Publication::new(NAMESPACE, "chat:42", r#"{"body":"hi"}"#);
    let body = serde_json::to_string(&publication).unwrap();
    let response =
        router.serve(post(PUBLICATIONS_PATH, Some("ingress-token"), &body)).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let ack: IngressAck =
        serde_json::from_str(&response.into_body().try_into_string().await.expect("body"))
            .expect("ack envelope");
    ack.validate().expect("ack is valid");
    assert_eq!(ack.version, 1);
    assert_eq!(ack.publication_id, publication.id, "the caller's UUID is echoed back");
}
