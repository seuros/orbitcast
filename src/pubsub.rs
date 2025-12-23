//! Pub/Sub backend abstraction
//!
//! Provides a trait for pub/sub backends that can be used to broadcast
//! messages across multiple OrbitCast nodes.
//!
//! # Features
//!
//! At least one backend must be enabled at compile time:
//!
//! - `postgres` - PostgreSQL LISTEN/NOTIFY for multi-node deployments
//! - `memory` - In-memory broadcast for single-node/development (preferred if both enabled)

#[cfg(feature = "postgres")]
mod postgres;

#[cfg(feature = "postgres")]
pub use postgres::PostgresPubSub;

#[cfg(feature = "memory")]
mod memory;

#[cfg(feature = "memory")]
pub use memory::MemoryPubSub;

use async_trait::async_trait;

/// Pub/Sub backend trait
///
/// Implementations handle cross-node message broadcasting.
/// Each node subscribes to streams and receives broadcasts from other nodes.
#[async_trait]
pub trait PubSub: Send + Sync {
    /// Publish a message to a stream
    ///
    /// All nodes listening on this stream will receive the message.
    async fn publish(&self, stream: &str, payload: &[u8]) -> anyhow::Result<()>;

    /// Subscribe to a stream
    ///
    /// The node will start receiving messages published to this stream.
    async fn subscribe(&self, stream: &str) -> anyhow::Result<()>;

    /// Unsubscribe from a stream
    async fn unsubscribe(&self, stream: &str) -> anyhow::Result<()>;

    /// Start the listener loop
    ///
    /// This should be spawned as a background task. It receives messages
    /// from other nodes and calls the provided callback.
    async fn listen<F>(&self, callback: F) -> anyhow::Result<()>
    where
        F: Fn(String, Vec<u8>) + Send + Sync + 'static;
}
