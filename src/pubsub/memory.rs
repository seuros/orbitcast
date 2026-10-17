//! In-process pub/sub backend.
//!
//! Single node, single process: development, tests, and deployments that accept
//! no cross-node delivery. There is no wire format here - publications never
//! leave the process - but the same v1 semantic validation is applied to the
//! stream name and the JSON payload text, so a payload that memory mode accepts
//! is one PostgreSQL mode would also accept.
//!
//! # Event-driven delivery
//!
//! Publishing sends into one bounded queue that [`PubSub::listen`] drains; there
//! is no polling, no `try_recv` scan, and no sleep. The queue is created with
//! the backend, so an acknowledged [`PubSub::subscribe`] always already has a
//! live receiver: a publish issued immediately after `subscribe` returns is
//! delivered.
//!
//! A single queue also gives one total delivery order across streams, which
//! matches the per-connection ordering a PostgreSQL listener session provides.
//!
//! # Drop policy
//!
//! The queue is bounded. When it is full the publication is dropped, counted in
//! [`PubSubStats`], and logged at `warn` - the same policy as the PostgreSQL
//! backend, for the same reason: a publisher must never be able to stall the
//! delivery task.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashSet;
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, warn};

use crate::protocol::v1::publication::Publication;
use crate::protocol::v1::{validate_json_text, validate_stream};
use crate::pubsub::{
    DEFAULT_QUEUE_CAPACITY, EventHandler, PubSub, PubSubError, PubSubEvent, PubSubStats,
    StatsSnapshot,
};

/// One publication on its way to the hub.
struct Delivery {
    stream: String,
    payload: Bytes,
}

/// In-process pub/sub.
///
/// # Limitations
/// - single process: no cross-node delivery, no persistence
/// - a publication for a stream with no local interest is discarded, exactly as
///   PostgreSQL discards a `NOTIFY` nobody is listening for
pub struct MemoryPubSub {
    subscriptions: Arc<DashSet<String>>,
    deliveries: mpsc::Sender<Delivery>,
    receiver: Mutex<Option<mpsc::Receiver<Delivery>>>,
    stats: Arc<PubSubStats>,
}

impl MemoryPubSub {
    /// Create an in-process backend with the default queue bound.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_QUEUE_CAPACITY)
    }

    /// Create an in-process backend with an explicit queue bound.
    ///
    /// # Panics
    /// Panics if `capacity` is zero; a zero-capacity queue could never deliver.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "memory pub/sub queue capacity must be greater than 0");
        let (deliveries, receiver) = mpsc::channel(capacity);
        Self {
            subscriptions: Arc::new(DashSet::new()),
            deliveries,
            receiver: Mutex::new(Some(receiver)),
            stats: Arc::new(PubSubStats::default()),
        }
    }

    /// Whether this node currently holds interest in `stream`.
    pub fn is_subscribed(&self, stream: &str) -> bool {
        self.subscriptions.contains(stream)
    }

    /// Hand one validated delivery to the queue, counting a full queue.
    fn enqueue(&self, stream: &str, json_payload: &str) {
        if !self.subscriptions.contains(stream) {
            debug!(stream = %stream, "No local interest; publication discarded");
            return;
        }

        let delivery = Delivery {
            stream: stream.to_string(),
            payload: Bytes::copy_from_slice(json_payload.as_bytes()),
        };

        match self.deliveries.try_send(delivery) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(dropped)) => {
                self.stats.record_delivery_drop();
                warn!(stream = %dropped.stream, "Delivery queue full; dropping publication");
            }
            Err(mpsc::error::TrySendError::Closed(dropped)) => {
                self.stats.record_delivery_drop();
                debug!(stream = %dropped.stream, "No delivery consumer; dropping publication");
            }
        }
    }
}

impl Default for MemoryPubSub {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PubSub for MemoryPubSub {
    async fn publish(&self, stream: &str, json_payload: &str) -> Result<(), PubSubError> {
        validate_stream(stream)?;
        validate_json_text("json_payload", json_payload)?;
        self.enqueue(stream, json_payload);
        Ok(())
    }

    async fn publish_publication(&self, publication: &Publication) -> Result<(), PubSubError> {
        // In-process delivery has no envelope: the namespace and id go
        // nowhere, but the same semantic checks apply so memory mode accepts
        // exactly what PostgreSQL mode would.
        validate_stream(&publication.stream)?;
        validate_json_text("json_payload", &publication.json_payload)?;
        self.enqueue(&publication.stream, &publication.json_payload);
        Ok(())
    }

    async fn subscribe(&self, stream: &str) -> Result<(), PubSubError> {
        validate_stream(stream)?;
        // The receiver exists from construction, so interest is live the moment
        // this returns - no window where a confirmed subscription has no reader.
        self.subscriptions.insert(stream.to_string());
        Ok(())
    }

    async fn unsubscribe(&self, stream: &str) -> Result<(), PubSubError> {
        self.subscriptions.remove(stream);
        Ok(())
    }

    fn is_ready(&self) -> bool {
        true
    }

    fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    async fn listen(&self, on_event: EventHandler) -> Result<(), PubSubError> {
        let mut deliveries =
            self.receiver.lock().await.take().ok_or(PubSubError::ListenerAlreadyStarted)?;

        while let Some(delivery) = deliveries.recv().await {
            on_event(PubSubEvent::Publication {
                stream: delivery.stream,
                payload: delivery.payload,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
