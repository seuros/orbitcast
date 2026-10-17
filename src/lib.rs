//! OrbitCast: an ActionCable-compatible real-time WebSocket relay.
//!
//! This library holds the whole server; `src/main.rs` is only a CLI/bootstrap
//! shell over it, and the integration tests drive the same code the binary
//! runs.
//!
//! # Layout
//!
//! - [`protocol`] owns the canonical v1 wire envelopes (serde types only).
//! - [`actioncable`] is the browser-facing client/server frame contract.
//! - [`hub`] owns sessions and the subscription membership indexes;
//!   [`encoder`] turns one publication into per-subscriber addressed frames.
//! - [`streams`] verifies signed-stream authorization; [`ratelimit`] bounds
//!   connection and command admission, and [`metrics`] exports what all of it
//!   counted.
//! - [`pubsub`] carries publications between nodes; [`broker`] retains them as
//!   ordered history and [`recovery`] replays that history to opt-in clients.
//! - [`server`] is the transport-agnostic connection driver plus backend
//!   readiness and shutdown; [`doctor`] describes a build and its
//!   configuration without serving.
//! - [`transport`] moves bytes: `transport::docking` (Mothership Unix socket)
//!   and `transport::http` (direct rama HTTP/WebSocket listener). Both drive
//!   the *same* [`server::Connections`] logic.

// Compile-time feature validation: at least one pub/sub backend required.
#[cfg(not(any(feature = "postgres", feature = "memory")))]
compile_error!(
    "OrbitCast requires a pub/sub backend. Enable: --features postgres OR --features memory"
);

pub mod actioncable;
pub mod broker;
pub mod config;
pub mod doctor;
pub mod encoder;
pub mod hub;
pub mod metrics;
#[cfg(all(feature = "postgres", not(feature = "memory")))]
mod pg;
pub mod presence;
pub mod protocol;
pub mod pubsub;
pub mod ratelimit;
pub mod recovery;
pub mod rpc_client;
pub mod server;
pub mod session;
pub mod streams;
mod sync;
pub mod transport;

#[cfg(all(not(feature = "memory"), feature = "postgres"))]
pub use crate::pubsub::{PostgresConfig, PostgresPubSub};

pub use crate::pubsub::MemoryPubSub;
