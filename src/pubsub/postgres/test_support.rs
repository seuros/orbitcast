//! Live-PostgreSQL fixtures for the `#[ignore]` integration tests of every
//! PostgreSQL-backed module (pub/sub, presence, broker).
//!
//! Run with:
//!
//! ```text
//! DATABASE_URL=postgres://... cargo test -p orbitcast -- --ignored
//! ```

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::{PostgresPubSub, connect_with_sslmode};
use crate::pubsub::{PubSub as _, PubSubEvent};

/// DSN for the ignored integration tests.
///
/// Contract: `DATABASE_URL` points at a PostgreSQL instance that accepts a
/// dedicated session (direct connection or session pooling - **not**
/// `PgBouncer` transaction pooling, which cannot hold `LISTEN`).
pub fn integration_dsn() -> String {
    std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set for #[ignore] postgres integration tests")
}

/// Per-test namespace so parallel integration tests on one shared database
/// derive disjoint NOTIFY channels and never observe each other's traffic.
pub fn integration_namespace(label: &str) -> String {
    format!("itest_{}_{}", label, std::process::id())
}

/// A plain client for test setup. Tests may issue DDL; production code
/// never does, which is why this is a test fixture.
pub async fn integration_client() -> tokio_postgres::Client {
    let (client, connection) =
        connect_with_sslmode(&integration_dsn()).await.expect("connect to DATABASE_URL");
    tokio::spawn(async move {
        // Ends when the test drops the client; a setup failure surfaces on
        // the client's next statement instead.
        let _ = connection.run().await;
    });
    client
}

/// Collect every listener event (publications and gaps) on a background task.
///
/// Like [`spawn_listener`], but nothing is filtered: a test asserting on a
/// delivery gap needs the gap itself, not only the silence around it.
pub fn spawn_event_listener(
    backend: Arc<PostgresPubSub>,
) -> (mpsc::UnboundedReceiver<PubSubEvent>, JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        backend
            .listen(Box::new(move |event| {
                // Fails only once the test dropped its receiver: done listening.
                let _ = tx.send(event);
            }))
            .await
            .expect("integration listener failed");
    });
    (rx, handle)
}

/// Collect deliveries from a backend on a background task.
///
/// A failed `listen()` panics the task: that drops the sender, so a test
/// waiting on the receiver fails at once with the error in its output
/// instead of timing out on a delivery that can never arrive.
pub fn spawn_listener(
    backend: Arc<PostgresPubSub>,
) -> (mpsc::UnboundedReceiver<(String, Vec<u8>)>, JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        backend
            .listen(Box::new(move |event| {
                if let PubSubEvent::Publication { stream, payload } = event {
                    // Fails only once the test dropped its receiver: done listening.
                    let _ = tx.send((stream, payload.to_vec()));
                }
            }))
            .await
            .expect("integration listener failed");
    });
    (rx, handle)
}
