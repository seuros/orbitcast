// Every binding clippy::significant_drop_tightening flags in this module is a
// fixture of Arcs and JoinHandles, never a lock guard: the lint walks field
// types transitively and reaches `RpcClient`'s rama connection pool (or a rama
// HTTP client's own pool), whose slots are tokio semaphore permits marked
// `#[clippy::has_significant_drop]`.
#![expect(
    clippy::significant_drop_tightening,
    reason = "HttpTransport is a fixture of Arcs/JoinHandles reaching pool permits, \
              not a lock guard"
)]

use super::*;
use crate::transport::test_support::{backend_unready, config};
use rama::http::headers::HeaderMapExt;

fn request_with_protocols(value: &str) -> Request {
    Request::builder()
        .uri("http://localhost/cable")
        .header("sec-websocket-protocol", value)
        .body(Body::empty())
        .unwrap()
}

#[test]
fn actioncable_offer_is_preserved() {
    // What @rails/actioncable actually sends.
    let req = normalize_subprotocol_offer(request_with_protocols(
        "actioncable-v1-json, actioncable-unsupported",
    ));
    let protocols: SecWebSocketProtocol =
        req.headers().typed_get().expect("offer kept for negotiation");
    assert!(protocols.contains(ACTIONCABLE_SUBPROTOCOL).is_some());
}

#[test]
fn unsupported_offer_is_dropped_rather_than_echoed_or_rejected() {
    let req = normalize_subprotocol_offer(request_with_protocols("graphql-ws"));
    assert!(
        req.headers().get(rama::http::header::SEC_WEBSOCKET_PROTOCOL).is_none(),
        "an unsupported offer must leave no protocol to select"
    );
}

#[test]
fn absent_offer_stays_absent() {
    let req = Request::builder().uri("http://localhost/cable").body(Body::empty()).unwrap();
    let req = normalize_subprotocol_offer(req);
    assert!(req.headers().get(rama::http::header::SEC_WEBSOCKET_PROTOCOL).is_none());
}

fn registry(messages: usize, bytes: usize) -> SocketRegistry {
    SocketRegistry::new(QueueLimits { messages, bytes })
}

/// The reconnectable close the hub issues for a lagging connection.
fn lag_close_of(conn_id: u32) -> (Bytes, Disembark) {
    (
        Bytes::from_static(
            b"{\"type\":\"disconnect\",\"reason\":\"server_restart\",\"reconnect\":true}",
        ),
        Disembark { conn_id, code: 1000, reason: "server_restart".to_string() },
    )
}

#[tokio::test]
async fn socket_registry_delivers_directly_and_reports_a_missing_socket() {
    let registry = registry(4, 1024);
    let mut rx = registry.insert(7);
    assert_eq!(registry.len(), 1);

    assert_eq!(registry.try_frame(7, Bytes::from_static(b"{}")), LinkOutcome::Sent);
    assert_eq!(rx.frames.try_next().as_deref(), Some(&b"{}"[..]));
    assert_eq!(rx.frames.queued_bytes.load(Ordering::Relaxed), 0, "dequeued bytes are released");

    // Unknown connection: reported, never a panic.
    assert_eq!(registry.try_frame(99, Bytes::from_static(b"{}")), LinkOutcome::Closed);

    registry.remove(7);
    assert!(registry.is_empty());
    assert_eq!(registry.try_frame(7, Bytes::from_static(b"{}")), LinkOutcome::Closed);
}

/// Either bound overflowing is that socket's lag: the frame is refused with
/// `Lagged` (the hub counts and closes), and the lag close the hub then
/// issues carries the reconnectable disconnect that the full queue could not.
#[tokio::test]
async fn an_overflowing_socket_queue_is_lagged_and_the_lag_close_carries_the_disconnect() {
    for (messages, bytes, label) in [(2, 1 << 20, "messages"), (64, 10, "bytes")] {
        let registry = registry(messages, bytes);
        let rx = registry.insert(1);

        let outcomes: Vec<LinkOutcome> =
            (0..4).map(|_| registry.try_frame(1, Bytes::from_static(b"{\"n\":1}"))).collect();
        assert_eq!(outcomes[0], LinkOutcome::Sent, "{label}");
        assert_eq!(outcomes[3], LinkOutcome::Lagged, "{label}");
        assert!(rx.close.borrow().is_none(), "{label}: the registry itself closes nothing");

        let (disconnect, close) = lag_close_of(1);
        registry.lag_close(disconnect, close);
        let signal = rx.close.borrow().clone().unwrap_or_else(|| panic!("{label}: lag closes"));
        assert!(!signal.flush, "{label}: a lag close does not flush the backlog");
        let last: serde_json::Value =
            serde_json::from_slice(&signal.final_frame.expect("disconnect frame")).unwrap();
        assert_eq!(last["type"], "disconnect", "{label}");
        assert_eq!(last["reconnect"], true, "{label}");

        // An ordered close arriving later does not displace the lag close.
        registry.close(1, 1001, "going away".to_string());
        assert!(!rx.close.borrow().as_ref().expect("still set").flush, "{label}");
    }
}

#[tokio::test]
async fn an_ordered_close_flushes_what_was_queued_first() {
    let registry = registry(8, 1 << 20);
    let mut rx = registry.insert(1);
    assert_eq!(
        registry.frame(1, Bytes::from_static(b"{\"type\":\"disconnect\"}")).await,
        LinkOutcome::Sent
    );
    registry.disconnect(Disembark { conn_id: 1, code: 1001, reason: "going away".into() }).await;

    let signal = rx.close.borrow_and_update().clone().expect("close signalled");
    assert!(signal.flush);
    assert_eq!((signal.code, signal.reason.as_str()), (1001, "going away"));
    assert!(signal.final_frame.is_none());
    assert_eq!(rx.frames.try_next().as_deref(), Some(&b"{\"type\":\"disconnect\"}"[..]));
}

#[test]
fn forwarded_chains_are_parsed_client_first() {
    let req = Request::builder()
        .uri("http://localhost/cable")
        .header("forwarded", "for=198.51.100.17;proto=https, for=\"[2001:db8::1]:4711\"")
        .header("x-forwarded-for", "203.0.113.9")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        forwarded_chain(&req),
        vec!["198.51.100.17".parse::<IpAddr>().unwrap(), "2001:db8::1".parse().unwrap()],
        "Forwarded wins over X-Forwarded-For"
    );

    let req = Request::builder()
        .uri("http://localhost/cable")
        .header("x-forwarded-for", "203.0.113.9:1234, unknown, 10.0.0.2")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        forwarded_chain(&req),
        vec!["203.0.113.9".parse::<IpAddr>().unwrap(), "10.0.0.2".parse().unwrap()],
        "ports are stripped and obfuscated nodes skipped"
    );

    let req = Request::builder().uri("http://localhost/cable").body(Body::empty()).unwrap();
    assert_eq!(forwarded_chain(&req), Vec::<IpAddr>::new());
}

#[tokio::test]
async fn readyz_names_the_unready_component() {
    let transport = HttpTransport::new(backend_unready(), &config(), Shutdown::new())
        .expect("transport builds");
    let router = Router { transport: Arc::clone(&transport) };
    let req = Request::get("http://127.0.0.1/readyz").body(Body::empty()).unwrap();
    let response = router.serve(req).await.expect("router is infallible");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = serde_json::from_str(
        &rama::http::BodyExtractExt::try_into_string(response.into_body()).await.unwrap(),
    )
    .unwrap();
    assert_eq!(body, serde_json::json!({"ready": false, "pubsub": "reconnecting"}));
}

async fn status_of(router: &Router, path: &str) -> StatusCode {
    let req = Request::get(format!("http://127.0.0.1{path}")).body(Body::empty()).unwrap();
    router.serve(req).await.expect("router is infallible").status()
}

#[tokio::test]
async fn bind_names_the_address_it_could_not_bind() {
    let Err(error) = bind("not-an-address").await else {
        panic!("a malformed address must not bind");
    };
    assert!(error.to_string().contains("not-an-address"), "{error}");
}

/// A drain refuses upgrades at once, keeps liveness answering, and stops the
/// listener only when it is done.
#[tokio::test]
async fn a_drain_refuses_upgrades_and_then_stops_the_listener() {
    let listener = bind("127.0.0.1:0").await.expect("an ephemeral port binds");
    assert_ne!(listener.local_addr().port(), 0, "the assigned port is reported");

    let transport = HttpTransport::new(backend_unready(), &config(), Shutdown::new())
        .expect("transport builds");
    let serving = tokio::spawn(Arc::clone(&transport).serve(listener));
    let router = Router { transport: Arc::clone(&transport) };

    transport.drain(Duration::from_secs(1)).await;
    tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("the listener stops once the drain is done")
        .expect("serve task");

    assert_eq!(status_of(&router, "/healthz").await, StatusCode::OK);
    assert_eq!(status_of(&router, "/cable").await, StatusCode::SERVICE_UNAVAILABLE);
    let scrape = transport.connections().backend().render_metrics();
    assert!(
        scrape.contains("orbitcast_connections_rejected_total{reason=\"draining\"} 1"),
        "{scrape}"
    );
}
