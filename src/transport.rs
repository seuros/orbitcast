//! Transport layer.
//!
//! Everything that moves bytes in and out of OrbitCast lives here:
//!
//! - [`docking`]: the Unix-socket link to Mothership and its frame codec.
//! - [`http`]: the direct rama HTTP/WebSocket listener, which lets the binary
//!   run without a supervisor.
//! - [`ingress`]: the private HTTP publication/control listener; never
//!   served on the public `/cable` listener.
//!
//! Both are only byte movers. Session lifecycle, ActionCable command handling,
//! and teardown live in [`crate::server::Connections`], so the two transports
//! cannot drift apart behaviourally, and both may run in one process.
//!
//! The docking codec is *not* an OrbitCast-owned envelope: it keeps the
//! Mothership framing and version rules unchanged. OrbitCast's own wire
//! contract lives in `crate::protocol`.
//!
//! # The outbound link
//!
//! The hub addresses frames to connection ids and hands them to an
//! [`OutboundLink`], the one thing a transport implements for the outbound
//! direction. The two implementations bound delivery differently:
//!
//! - the HTTP transport delivers each frame straight into the target socket's
//!   own bounded queue, so the only bound a frame can meet is that socket's
//!   (one slow reader never costs another socket a frame);
//! - the docking transport queues every connection's frames on the one link
//!   that carries them all, because one Unix socket is what Mothership gave it.
//!
//! Either way a bound that is met is reported as [`LinkOutcome::Lagged`] and
//! the hub ends *that* connection with a reconnectable `disconnect`
//! ([`OutboundLink::lag_close`]), counted under
//! `orbitcast_socket_lag_disconnects_total{reason="queue"}`. A frame is never
//! lost without the client being told to resubscribe.

use async_trait::async_trait;
use bytes::Bytes;

pub mod docking;
pub mod http;
pub mod ingress;

#[cfg(test)]
mod test_support;

pub use docking::Disembark;

/// What became of a frame handed to an [`OutboundLink`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkOutcome {
    /// Queued for the connection, in order behind what preceded it.
    Sent,
    /// The bounded queue the connection's frames depend on was full. The
    /// frame is lost for that connection, which must therefore be closed with
    /// a reconnectable `disconnect` (the hub does that, once per connection).
    Lagged,
    /// The connection, or the whole link, is gone: nothing was queued and
    /// nothing is owed.
    Closed,
}

/// The transport side of the hub: where addressed frames and closes go.
///
/// Implementations never call back into the hub, so the hub may use a link
/// while it holds its own index guards.
#[async_trait]
pub trait OutboundLink: Send + Sync {
    /// Enqueue one frame for `conn_id` without waiting for room.
    fn try_frame(&self, conn_id: u32, frame: Bytes) -> LinkOutcome;

    /// Enqueue one frame for `conn_id`, waiting for room where the link has
    /// a shared queue to wait on. A direct link does not wait: the only room
    /// is the socket's own, and running out of it is that socket's lag.
    async fn frame(&self, conn_id: u32, frame: Bytes) -> LinkOutcome;

    /// Close a connection in order: frames queued for it before this call are
    /// written before the close.
    async fn disconnect(&self, close: Disembark);

    /// Close a lagging connection. `disconnect` is the reconnectable
    /// ActionCable `disconnect` frame the client is owed, written right before
    /// `close`; a full queue, which is the reason this is being called, must
    /// not be able to lose either. Must not wait, and must not re-enter the
    /// hub.
    fn lag_close(&self, disconnect: Bytes, close: Disembark);
}
