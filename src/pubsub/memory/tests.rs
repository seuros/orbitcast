use std::assert_matches;
use std::time::Duration;

use super::*;
use crate::protocol::ProtocolError;

/// Start the listener and return a queue of everything it delivered.
///
/// A failed `listen()` panics the task, which drops the sender: a test waiting
/// on the receiver then fails at once instead of timing out.
fn spawn_listener(
    pubsub: Arc<MemoryPubSub>,
) -> (mpsc::UnboundedReceiver<(String, Vec<u8>)>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        pubsub
            .listen(Box::new(move |event| {
                if let PubSubEvent::Publication { stream, payload } = event {
                    // Fails only once the test dropped its receiver: done listening.
                    let _ = tx.send((stream, payload.to_vec()));
                }
            }))
            .await
            .expect("memory listener failed");
    });
    (rx, handle)
}

async fn next(rx: &mut mpsc::UnboundedReceiver<(String, Vec<u8>)>) -> (String, String) {
    let (stream, payload) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("publication delivered")
        .expect("delivery channel open");
    (stream, String::from_utf8(payload).expect("utf-8 payload"))
}

#[tokio::test]
async fn subscribe_then_immediate_publish_is_delivered() {
    let pubsub = Arc::new(MemoryPubSub::new());
    let (mut rx, handle) = spawn_listener(pubsub.clone());

    // No sleep: subscribe is acknowledged, so the receiver is already live.
    pubsub.subscribe("chat:42").await.unwrap();
    pubsub.publish("chat:42", r#"{"body":"hi"}"#).await.unwrap();

    assert_eq!(next(&mut rx).await, ("chat:42".to_string(), r#"{"body":"hi"}"#.to_string()));
    handle.abort();
}

#[tokio::test]
async fn delivery_preserves_publication_order() {
    let pubsub = Arc::new(MemoryPubSub::new());
    let (mut rx, handle) = spawn_listener(pubsub.clone());

    pubsub.subscribe("chat:42").await.unwrap();
    for i in 0..10 {
        pubsub.publish("chat:42", &format!(r#"{{"i":{i}}}"#)).await.unwrap();
    }

    for i in 0..10 {
        assert_eq!(next(&mut rx).await.1, format!(r#"{{"i":{i}}}"#));
    }
    handle.abort();
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let pubsub = Arc::new(MemoryPubSub::new());
    let (mut rx, handle) = spawn_listener(pubsub.clone());

    pubsub.subscribe("chat:42").await.unwrap();
    pubsub.publish("chat:42", r#"{"n":1}"#).await.unwrap();
    assert_eq!(next(&mut rx).await.1, r#"{"n":1}"#);

    pubsub.unsubscribe("chat:42").await.unwrap();
    assert!(!pubsub.is_subscribed("chat:42"));
    pubsub.publish("chat:42", r#"{"n":2}"#).await.unwrap();

    let idle = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
    assert!(idle.is_err(), "unsubscribed stream must not be delivered");
    handle.abort();
}

#[tokio::test]
async fn publish_without_interest_is_discarded() {
    let pubsub = Arc::new(MemoryPubSub::new());
    let (mut rx, handle) = spawn_listener(pubsub.clone());

    pubsub.publish("chat:42", r#"{"n":1}"#).await.unwrap();

    let idle = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
    assert!(idle.is_err(), "publication without local interest must not be delivered");
    handle.abort();
}

#[tokio::test]
async fn other_streams_are_not_delivered() {
    let pubsub = Arc::new(MemoryPubSub::new());
    let (mut rx, handle) = spawn_listener(pubsub.clone());

    pubsub.subscribe("chat:42").await.unwrap();
    pubsub.publish("chat:99", r#"{"n":1}"#).await.unwrap();
    pubsub.publish("chat:42", r#"{"n":2}"#).await.unwrap();

    assert_eq!(next(&mut rx).await, ("chat:42".to_string(), r#"{"n":2}"#.to_string()));
    handle.abort();
}

#[tokio::test]
async fn publish_rejects_invalid_stream_and_payload() {
    let pubsub = MemoryPubSub::new();
    pubsub.subscribe("chat:42").await.unwrap();

    assert_matches!(
        pubsub.publish("", "{}").await.unwrap_err(),
        PubSubError::Protocol(ProtocolError::EmptyField { field: "stream" })
    );
    assert_matches!(
        pubsub.publish("chat:42", "{not json").await.unwrap_err(),
        PubSubError::Protocol(ProtocolError::InvalidJson { field: "json_payload", .. })
    );
    assert_matches!(
        pubsub.subscribe("").await.unwrap_err(),
        PubSubError::Protocol(ProtocolError::EmptyField { field: "stream" })
    );
}

#[tokio::test]
async fn full_queue_drops_and_counts_instead_of_blocking() {
    // No listener: the queue fills and then drops.
    let pubsub = MemoryPubSub::with_capacity(2);
    pubsub.subscribe("chat:42").await.unwrap();

    for _ in 0..5 {
        // Never blocks, never errors: an overload is a counted gap.
        pubsub.publish("chat:42", r#"{"n":1}"#).await.unwrap();
    }

    assert_eq!(pubsub.stats().delivery_dropped, 3);
}

#[tokio::test]
async fn listen_can_only_be_started_once() {
    let pubsub = Arc::new(MemoryPubSub::new());
    let (_rx, handle) = spawn_listener(pubsub.clone());
    tokio::task::yield_now().await;

    let err = pubsub.listen(Box::new(|_| {})).await.unwrap_err();
    assert_matches!(err, PubSubError::ListenerAlreadyStarted);
    handle.abort();
}

#[tokio::test]
async fn backend_is_always_ready() {
    assert!(MemoryPubSub::default().is_ready());
}
