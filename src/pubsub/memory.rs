//! In-memory pub/sub backend using tokio::sync::broadcast
//!
//! Single-node only. No cross-process coordination.
//! Ideal for development and single-instance deployments.

use crate::pubsub::PubSub;
use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

/// Default buffer size for broadcast channels
const DEFAULT_BUFFER_SIZE: usize = 1024;

/// In-memory pub/sub using tokio::sync::broadcast
///
/// Each stream gets its own broadcast channel. Messages are delivered
/// to all subscribers immediately with no persistence.
///
/// # Limitations
/// - Single-process only
/// - No message persistence
/// - Lagging receivers lose messages
pub struct MemoryPubSub {
    channels: Arc<DashMap<String, broadcast::Sender<Vec<u8>>>>,
    subscriptions: Arc<RwLock<HashSet<String>>>,
    buffer_size: usize,
}

impl MemoryPubSub {
    /// Create a new in-memory pub/sub backend
    ///
    /// # Example
    /// ```ignore
    /// use crate::pubsub::MemoryPubSub;
    ///
    /// let pubsub = MemoryPubSub::new();
    /// ```
    pub fn new() -> Self {
        Self::with_buffer_size(DEFAULT_BUFFER_SIZE)
    }

    /// Create with custom buffer size
    ///
    /// Larger buffers reduce message loss for slow receivers
    /// but consume more memory.
    pub fn with_buffer_size(buffer_size: usize) -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            subscriptions: Arc::new(RwLock::new(HashSet::new())),
            buffer_size,
        }
    }

    /// Get or create a broadcast channel for a stream
    fn get_or_create_channel(&self, stream: &str) -> broadcast::Sender<Vec<u8>> {
        self.channels
            .entry(stream.to_string())
            .or_insert_with(|| broadcast::channel(self.buffer_size).0)
            .clone()
    }
}

impl Default for MemoryPubSub {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PubSub for MemoryPubSub {
    async fn publish(&self, stream: &str, payload: &[u8]) -> anyhow::Result<()> {
        let tx = self.get_or_create_channel(stream);

        // send() returns Err only if there are no receivers
        // This is fine - fire and forget semantics
        let _ = tx.send(payload.to_vec());

        Ok(())
    }

    async fn subscribe(&self, stream: &str) -> anyhow::Result<()> {
        // Ensure channel exists
        self.get_or_create_channel(stream);

        let mut subs = self.subscriptions.write().await;
        subs.insert(stream.to_string());

        Ok(())
    }

    async fn unsubscribe(&self, stream: &str) -> anyhow::Result<()> {
        let mut subs = self.subscriptions.write().await;
        subs.remove(stream);

        // Cleanup channel if no more subscribers
        if let Some(entry) = self.channels.get(stream)
            && entry.receiver_count() == 0
        {
            drop(entry);
            self.channels.remove(stream);
        }

        Ok(())
    }

    async fn listen<F>(&self, callback: F) -> anyhow::Result<()>
    where
        F: Fn(String, Vec<u8>) + Send + Sync + 'static,
    {
        let callback = Arc::new(callback);
        let channels = self.channels.clone();
        let subscriptions = self.subscriptions.clone();

        // Track active receivers per channel
        let mut receivers: std::collections::HashMap<String, broadcast::Receiver<Vec<u8>>> =
            std::collections::HashMap::new();

        loop {
            // Sync receivers with current subscriptions
            {
                let subs = subscriptions.read().await;

                // Add new subscriptions
                for stream in subs.iter() {
                    if !receivers.contains_key(stream)
                        && let Some(tx) = channels.get(stream)
                    {
                        receivers.insert(stream.clone(), tx.subscribe());
                    }
                }

                // Remove stale subscriptions
                receivers.retain(|stream, _| subs.contains(stream));
            }

            if receivers.is_empty() {
                // No subscriptions, wait a bit before checking again
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }

            // Poll all receivers
            // Using a simple round-robin approach with timeout
            let mut received_any = false;

            for (stream, rx) in receivers.iter_mut() {
                match rx.try_recv() {
                    Ok(payload) => {
                        callback(stream.clone(), payload);
                        received_any = true;
                    }
                    Err(broadcast::error::TryRecvError::Lagged(n)) => {
                        tracing::warn!("Receiver lagged, lost {} messages on stream {}", n, stream);
                        // Continue receiving from current position
                    }
                    Err(broadcast::error::TryRecvError::Empty) => {
                        // No messages available
                    }
                    Err(broadcast::error::TryRecvError::Closed) => {
                        // Channel closed, will be cleaned up on next sync
                    }
                }
            }

            if !received_any {
                // Yield to prevent busy-spinning
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn test_publish_subscribe() {
        let pubsub = MemoryPubSub::new();
        let received = Arc::new(AtomicUsize::new(0));
        let received_clone = received.clone();

        pubsub.subscribe("test").await.unwrap();

        // Spawn listener
        let pubsub_clone = MemoryPubSub {
            channels: pubsub.channels.clone(),
            subscriptions: pubsub.subscriptions.clone(),
            buffer_size: pubsub.buffer_size,
        };

        let handle = tokio::spawn(async move {
            let _ = pubsub_clone
                .listen(move |_stream, _payload| {
                    received_clone.fetch_add(1, Ordering::SeqCst);
                })
                .await;
        });

        // Give listener time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Publish messages
        pubsub.publish("test", b"hello").await.unwrap();
        pubsub.publish("test", b"world").await.unwrap();

        // Give time to receive
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(received.load(Ordering::SeqCst), 2);

        handle.abort();
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let pubsub = MemoryPubSub::new();

        pubsub.subscribe("test").await.unwrap();
        assert!(pubsub.subscriptions.read().await.contains("test"));

        pubsub.unsubscribe("test").await.unwrap();
        assert!(!pubsub.subscriptions.read().await.contains("test"));
    }

    #[test]
    fn test_default() {
        let pubsub = MemoryPubSub::default();
        assert_eq!(pubsub.buffer_size, DEFAULT_BUFFER_SIZE);
    }
}
