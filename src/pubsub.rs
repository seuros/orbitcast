//! Pub/Sub backend abstraction.
//!
//! A backend carries publications *between* nodes. Everything a backend hands
//! back to the hub is the exact Rails-encoded JSON text of one publication;
//! routing keys (hashed PostgreSQL channel names) never reach the hub, and are
//! never used as ActionCable identifiers.
//!
//! # Wire contract
//!
//! The transport record is the v1 [`PublicationNotice`] from
//! [`crate::protocol::v1::publication`]:
//!
//! ```json
//! {"version":1,"kind":"inline","publication":{
//!    "version":1,"namespace":"example_production","id":"<uuid>",
//!    "stream":"chat:42","json_payload":"{\"body\":\"hi\"}"}}
//! ```
//!
//! Decoding is strict: malformed JSON, wrong field types, unknown `kind`
//! values, unsupported versions, foreign namespaces, notices that arrived on a
//! channel that does not match their own `(namespace, stream)`, and oversized
//! payloads are all rejected and counted - never best-effort repaired.
//!
//! # Overflow relay
//!
//! A publication whose encoded inline notice would reach
//! [`MAX_NOTIFY_PAYLOAD_LEN`] cannot travel inside a notification. With
//! [`OverflowConfig::enabled`], the publisher instead inserts the payload into
//! `orbitcast_payloads` and emits a `kind: "overflow"` reference **in the same
//! transaction**, and every listening node fetches the row back from the same
//! primary. With overflow disabled the publish fails with
//! [`PubSubError::PayloadTooLarge`] and an inbound overflow notice is a counted
//! gap ([`PubSubError::OverflowUnsupported`]) - never a silent drop.
//!
//! Regardless of the mode, a publication larger than
//! [`OverflowConfig::max_publication_size`] is rejected with
//! [`PubSubError::PublicationTooLarge`]: the relay table is a transport for
//! large Turbo HTML, not an unbounded object store.
//!
//! # Channel derivation
//!
//! [`channel_for`] maps `(namespace, stream)` to `orbitcast_` plus the first 48
//! lowercase hex characters of SHA-256 over the length-prefixed UTF-8 tuple:
//!
//! ```text
//! u32-BE(namespace.len()) || namespace || u32-BE(stream.len()) || stream
//! ```
//!
//! Lengths are UTF-8 *byte* lengths. The result is 58 ASCII bytes, below
//! PostgreSQL's 63-byte identifier limit. Golden vectors are shared with the
//! Ruby publisher and asserted in this module's tests. Hashing is routing, not
//! authorization: the decoder re-checks namespace and stream on receipt.
//!
//! # Readiness
//!
//! [`PubSub::subscribe`] and [`PubSub::unsubscribe`] are *acknowledged*: they
//! return `Ok` only after the backend has confirmed the corresponding
//! `LISTEN`/`UNLISTEN` (server command completion, not enqueue). Callers may
//! therefore install membership, await this call, and only then confirm the
//! subscription to the browser - a rejected acknowledgement rolls the
//! membership back instead of confirming a silent subscription.
//!
//! # Bounded delivery and the drop policy
//!
//! Every queue between the database driver and the hub is bounded. When a queue
//! is full the publication is **dropped**, counted in [`PubSubStats`], and
//! logged at `warn`. Dropping (rather than applying backpressure) is deliberate:
//! blocking the intake would stall the PostgreSQL connection's poll loop, which
//! also carries query responses, and would deadlock the very `LISTEN` command
//! that a subscriber is waiting on. A drop is a delivery gap, visible in
//! `PubSubStats::delivery_dropped` / `PubSubStats::intake_dropped`, and the
//! intake high-water mark (`intake_depth_max`) shows how close the listener
//! runs to that edge. An overflow-relay gap additionally surfaces as
//! [`PubSubEvent::Gap`], which the server turns into a reconnectable
//! disconnect of the stream's local members.
//!
//! # Gaps
//!
//! Notification-only mode cannot recover messages published while a listener
//! connection is down. A backend that loses its connection reports
//! [`PubSub::is_ready`] as `false` (new subscribes and publishes fail fast),
//! re-`LISTEN`s every currently interested channel on reconnect, and logs a gap
//! warning. Nothing replays the gap.
//!
//! # Features
//!
//! At least one backend must be enabled at compile time. When multiple backends
//! are enabled, `memory` wins so `--all-features` builds stay deterministic.
//!
//! - `postgres` - PostgreSQL LISTEN/NOTIFY for multi-node deployments
//! - `memory` - in-process delivery for single-node/development

// `pub(crate)` rather than private: the broker and presence integration tests
// share this module's live-PostgreSQL fixtures (`postgres::test_support`).
#[cfg(all(feature = "postgres", not(feature = "memory")))]
pub(crate) mod postgres;

#[cfg(all(feature = "postgres", not(feature = "memory")))]
pub use postgres::{DEFAULT_KEEPALIVE_INTERVAL, PostgresConfig, PostgresPubSub};

// Always compiled: it is the backend *selected* only by the `memory` feature,
// but every build's unit and integration tests need an in-process backend to
// drive the server with, and the PostgreSQL build has nothing else.
pub mod memory;

pub use memory::MemoryPubSub;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use crate::protocol::Envelope;
use crate::protocol::ProtocolError;
use crate::protocol::v1::publication::{
    MAX_NOTIFY_PAYLOAD_LEN, NoticeKind, OverflowRef, Publication, PublicationNotice,
};

// The notice codec below is the PostgreSQL wire contract. It stays compiled -
// and tested - in memory-only builds too, so the cross-language golden vectors
// are verified in every feature set rather than only where the backend exists.

/// Prefix of every OrbitCast PostgreSQL channel name.
pub const CHANNEL_PREFIX: &str = "orbitcast_";

/// Reserved stream name for Rails-initiated control messages.
///
/// Shared with `gem/lib/orbitcast/publisher/control.rb`
/// (`Orbitcast::Publisher::Control::CONTROL_STREAM`): the control channel is
/// derived through [`channel_for`] with this stream name, so it is
/// namespace-scoped like every other channel and can only collide with an
/// application stream that deliberately uses this reserved name.
pub const CONTROL_STREAM: &str = "__orbitcast_control__";

/// Derive the PostgreSQL control channel for `namespace`.
///
/// Same function as [`channel_for`], with the reserved control stream name.
/// Golden vectors are pinned in this module's tests; changing the derivation
/// is a wire break against the Ruby publisher.
#[must_use]
pub fn control_channel_for(namespace: &str) -> String {
    channel_for(namespace, CONTROL_STREAM)
}

/// Hex characters of the SHA-256 digest kept in a channel name.
pub const CHANNEL_HASH_HEX_LEN: usize = 48;

/// Default acknowledgement deadline for `LISTEN`/`UNLISTEN`.
pub const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Default bound for the notification intake and delivery queues.
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Default lifetime of an `orbitcast_payloads` row, from the database clock.
///
/// The relay is a short-lived hand-off between publish and delivery, not
/// history: five minutes covers a slow listener, not an offline node.
pub const DEFAULT_OVERFLOW_TTL: Duration = Duration::from_mins(5);

/// Default ceiling on one publication's payload (1 MiB).
pub const DEFAULT_MAX_PUBLICATION_SIZE: usize = 1024 * 1024;

/// Hard ceiling on the configurable publication limit (64 MiB).
pub const MAX_PUBLICATION_SIZE_LIMIT: usize = 64 * 1024 * 1024;

/// Name of the overflow relay table. Fixed: it is a cross-language contract
/// shared with `gem/db/orbitcast_migrate/001_create_orbitcast_payloads.rb`.
///
/// Compiled with the PostgreSQL backend only: no other backend has the table.
#[cfg(all(feature = "postgres", not(feature = "memory")))]
pub const OVERFLOW_TABLE: &str = "orbitcast_payloads";

/// How an oversized publication is handled.
///
/// `enabled` is opt-in because the relay needs a migration: no production code
/// path ever creates the table implicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverflowConfig {
    /// Relay oversized publications through `orbitcast_payloads`.
    pub enabled: bool,
    /// Lifetime of a relay row, measured from the database clock at insertion.
    pub ttl: Duration,
    /// Reject any publication whose payload exceeds this many bytes.
    pub max_publication_size: usize,
}

impl Default for OverflowConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ttl: DEFAULT_OVERFLOW_TTL,
            max_publication_size: DEFAULT_MAX_PUBLICATION_SIZE,
        }
    }
}

impl OverflowConfig {
    /// Reject unusable overflow settings at the configuration boundary.
    ///
    /// The publication limit must leave room for at least one inline notice;
    /// a limit below the notification limit would reject payloads that need no
    /// relay at all.
    ///
    /// # Errors
    ///
    /// [`PubSubError::Config`] when `ttl` is zero, or when
    /// `max_publication_size` is below [`MAX_NOTIFY_PAYLOAD_LEN`] or above
    /// [`MAX_PUBLICATION_SIZE_LIMIT`].
    pub fn validate(&self) -> Result<(), PubSubError> {
        if self.ttl.is_zero() {
            return Err(PubSubError::Config("overflow_ttl_ms must be greater than 0".to_string()));
        }
        if self.max_publication_size < MAX_NOTIFY_PAYLOAD_LEN
            || self.max_publication_size > MAX_PUBLICATION_SIZE_LIMIT
        {
            return Err(PubSubError::Config(format!(
                "max_publication_size must be between {MAX_NOTIFY_PAYLOAD_LEN} and \
                 {MAX_PUBLICATION_SIZE_LIMIT} bytes"
            )));
        }
        Ok(())
    }
}

/// Pub/Sub failures.
///
/// Decode/validation failures keep the offending value out of the message so a
/// log line never leaks an application payload.
#[derive(Debug, thiserror::Error)]
pub enum PubSubError {
    /// The backend has no usable connection right now.
    #[error("pub/sub backend is not ready")]
    NotReady,
    /// Static configuration is unusable; never a transient condition.
    #[error("pub/sub configuration error: {0}")]
    Config(String),
    /// A subscription command was not acknowledged within the deadline.
    #[error("backend did not acknowledge {command} for stream {stream:?} within {timeout:?}")]
    AckTimeout { command: &'static str, stream: String, timeout: Duration },
    /// The listener supervisor is gone (shutting down or panicked).
    #[error("pub/sub listener task is unavailable")]
    ListenerGone,
    /// `listen()` was called more than once on the same backend.
    #[error("pub/sub listener has already been started")]
    ListenerAlreadyStarted,
    /// Two distinct streams derived the same channel name.
    #[error("channel {channel} maps to both {existing:?} and {incoming:?}")]
    ChannelCollision { channel: String, existing: String, incoming: String },
    /// The encoded notice does not fit in a PostgreSQL notification and the
    /// overflow relay is disabled.
    ///
    /// Never truncated and never fragmented: enable the overflow relay (and
    /// run its migration) or publish a smaller payload.
    #[error(
        "encoded notice is {len} bytes, limit is {max}; enable the overflow relay \
         (overflow_enabled) after running the orbitcast_payloads migration"
    )]
    PayloadTooLarge { len: usize, max: usize },
    /// The publication exceeds the configured publication ceiling.
    ///
    /// Checked before any SQL and regardless of the relay mode: the overflow
    /// table relays large Turbo HTML, it is not an object store.
    #[error("publication payload is {len} bytes, configured maximum is {max}")]
    PublicationTooLarge { len: usize, max: usize },
    /// Envelope decode or semantic validation failure.
    #[error("invalid publication notice: {0}")]
    Protocol(#[from] ProtocolError),
    /// The notice belongs to a different OrbitCast deployment.
    #[error("notice namespace {actual:?} does not match node namespace {expected:?}")]
    NamespaceMismatch { expected: String, actual: String },
    /// The notice arrived on a channel that its own `(namespace, stream)` does
    /// not derive. Routing and content disagree, so it is rejected.
    #[error("notice for stream {stream:?} arrived on {channel}, expected {expected}")]
    ChannelMismatch { stream: String, channel: String, expected: String },
    /// A valid overflow notice arrived while the relay is disabled on this
    /// node: understood, addressed to us, and not fetchable. A delivery gap.
    #[error(
        "overflow notice for publication {id} (stream {stream:?}) cannot be fetched: \
         the overflow relay is disabled on this node"
    )]
    OverflowUnsupported { id: String, stream: String },
    /// The relay row could not be read back: missing, expired, mismatched, or
    /// the fetch failed. The publication is lost - a counted delivery gap.
    #[error("overflow publication {id} (stream {stream:?}) could not be fetched: {reason}")]
    OverflowGap { id: String, stream: String, reason: String },
    /// Transport-level failure (connection, query, TLS).
    #[error("pub/sub backend error: {0}")]
    Backend(#[from] anyhow::Error),
}

/// Overload and rejection counters.
///
/// Cheap atomic counters rather than a metrics facade; `snapshot` is what a
/// `/metrics` exporter will read once one exists.
#[derive(Debug, Default)]
pub struct PubSubStats {
    /// Notifications dropped because the driver intake queue was full.
    intake_dropped: AtomicU64,
    /// Publications dropped because the hub delivery queue was full or closed.
    delivery_dropped: AtomicU64,
    /// Notices rejected as malformed, foreign, or misrouted.
    rejected: AtomicU64,
    /// Valid overflow notices received (fetch attempted or refused).
    overflow: AtomicU64,
    /// Overflow notices that produced no publication: missing, expired, or
    /// mismatched row, failed/timed-out fetch, or a disabled relay.
    overflow_gaps: AtomicU64,
    /// Listener connection losses.
    reconnects: AtomicU64,
    /// Deepest the driver intake queue has been (lag high-water mark).
    intake_depth_max: AtomicU64,
}

/// Point-in-time copy of [`PubSubStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatsSnapshot {
    pub intake_dropped: u64,
    pub delivery_dropped: u64,
    pub rejected: u64,
    pub overflow: u64,
    pub overflow_gaps: u64,
    pub reconnects: u64,
    pub intake_depth_max: u64,
}

impl PubSubStats {
    pub fn record_intake_drop(&self) {
        self.intake_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_delivery_drop(&self) {
        self.delivery_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_overflow(&self) {
        self.overflow.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_overflow_gap(&self) {
        self.overflow_gaps.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    /// The intake queue currently holds `depth` notifications; keeps the
    /// high-water mark.
    pub fn record_intake_depth(&self, depth: u64) {
        self.intake_depth_max.fetch_max(depth, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            intake_dropped: self.intake_dropped.load(Ordering::Relaxed),
            delivery_dropped: self.delivery_dropped.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            overflow: self.overflow.load(Ordering::Relaxed),
            overflow_gaps: self.overflow_gaps.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            intake_depth_max: self.intake_depth_max.load(Ordering::Relaxed),
        }
    }
}

/// Derive the PostgreSQL channel name for `(namespace, stream)`.
///
/// See the module documentation for the exact pre-image. The mapping is stable
/// across languages and versions: changing it is a wire break.
#[must_use]
pub fn channel_for(namespace: &str, stream: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(length_prefix(namespace));
    hasher.update(namespace.as_bytes());
    hasher.update(length_prefix(stream));
    hasher.update(stream.as_bytes());

    let digest = hex::encode(hasher.finalize());
    let mut channel = String::with_capacity(CHANNEL_PREFIX.len() + CHANNEL_HASH_HEX_LEN);
    channel.push_str(CHANNEL_PREFIX);
    channel.push_str(&digest[..CHANNEL_HASH_HEX_LEN]);
    channel
}

/// The `u32-BE(part.len())` prefix of one part of a hashed tuple.
///
/// The low 32 bits of the UTF-8 byte length, big-endian: byte for byte what
/// the Ruby publisher's `[part.bytesize].pack("N")` emits, including for a
/// part too long to fit (validated names never come close). Shared by
/// [`channel_for`] and [`crate::presence::MembershipKey::lock_key`].
pub(crate) const fn length_prefix(part: &str) -> [u8; 4] {
    let [.., b3, b2, b1, b0] = part.len().to_be_bytes();
    [b3, b2, b1, b0]
}

/// One publication prepared for `NOTIFY`.
///
/// [`Self::relay_row`] is `Some` exactly when the notice is an overflow
/// reference: that publication must be inserted into `orbitcast_payloads` and
/// notified inside a single transaction, so no node can observe a reference to
/// a row it cannot read.
#[derive(Debug, Clone)]
pub struct PreparedNotice {
    /// Derived PostgreSQL channel name.
    pub channel: String,
    /// Encoded notice, already checked against the notification limit.
    pub encoded: String,
    /// The publication to store when this notice is an overflow reference.
    pub relay_row: Option<Publication>,
}

/// Prepare one publication for `NOTIFY`, choosing inline or overflow.
///
/// Validation order, all before any SQL runs:
///
/// 1. envelope semantics (namespace, stream, UUID, payload is JSON),
/// 2. [`OverflowConfig::max_publication_size`] against the payload bytes,
/// 3. the *encoded* inline notice - envelope quoting and escaping included -
///    against [`MAX_NOTIFY_PAYLOAD_LEN`],
/// 4. with the relay enabled, the overflow reference against the same limit.
///
/// # Errors
///
/// - [`PubSubError::Protocol`] when the envelope is invalid (empty or
///   malformed namespace or stream, payload that is not JSON) or cannot be
///   encoded,
/// - [`PubSubError::PublicationTooLarge`] when the payload exceeds
///   `overflow.max_publication_size`,
/// - [`PubSubError::PayloadTooLarge`] when the inline notice does not fit a
///   notification and the relay is disabled, or when even the overflow
///   reference does not fit.
pub fn prepare_notice(
    namespace: &str,
    stream: &str,
    json_payload: &str,
    overflow: OverflowConfig,
) -> Result<PreparedNotice, PubSubError> {
    prepare_publication_notice(Publication::new(namespace, stream, json_payload), overflow)
}

/// [`prepare_notice`] for a publication whose id the caller already chose.
///
/// The id travels unchanged into the notice, the overflow row, and the
/// relay reference: a publisher that retries with the same id produces the
/// same records, which is what lets the retained broker deduplicate it.
///
/// # Errors
///
/// Exactly those of [`prepare_notice`].
pub fn prepare_publication_notice(
    publication: Publication,
    overflow: OverflowConfig,
) -> Result<PreparedNotice, PubSubError> {
    publication.validate()?;

    if publication.json_payload.len() > overflow.max_publication_size {
        return Err(PubSubError::PublicationTooLarge {
            len: publication.json_payload.len(),
            max: overflow.max_publication_size,
        });
    }

    let channel = channel_for(&publication.namespace, &publication.stream);
    let inline = serde_json::to_string(&PublicationNotice::inline(publication.clone()))
        .map_err(ProtocolError::Malformed)?;

    if PublicationNotice::fits_notify_payload(&inline) {
        return Ok(PreparedNotice { channel, encoded: inline, relay_row: None });
    }

    if !overflow.enabled {
        return Err(PubSubError::PayloadTooLarge {
            len: inline.len(),
            max: MAX_NOTIFY_PAYLOAD_LEN,
        });
    }

    let reference = PublicationNotice::overflow(OverflowRef::for_publication(&publication));
    let encoded = serde_json::to_string(&reference).map_err(ProtocolError::Malformed)?;

    // A reference is a UUID plus namespace and stream, so this only triggers
    // for an absurdly long stream name - still checked, never assumed.
    if !PublicationNotice::fits_notify_payload(&encoded) {
        return Err(PubSubError::PayloadTooLarge {
            len: encoded.len(),
            max: MAX_NOTIFY_PAYLOAD_LEN,
        });
    }

    Ok(PreparedNotice { channel, encoded, relay_row: Some(publication) })
}

/// Encode one inline publication for `NOTIFY`, with the relay disabled.
///
/// Returns the channel to notify and the encoded notice.
///
/// # Errors
///
/// Everything [`prepare_notice`] rejects with the default [`OverflowConfig`]:
/// an invalid envelope ([`PubSubError::Protocol`]), a payload above
/// [`DEFAULT_MAX_PUBLICATION_SIZE`] ([`PubSubError::PublicationTooLarge`]),
/// or an encoded notice that does not fit a notification
/// ([`PubSubError::PayloadTooLarge`]).
pub fn encode_notice(
    namespace: &str,
    stream: &str,
    json_payload: &str,
) -> Result<(String, String), PubSubError> {
    let prepared = prepare_notice(namespace, stream, json_payload, OverflowConfig::default())?;
    debug_assert!(prepared.relay_row.is_none(), "a disabled relay cannot produce a reference");
    Ok((prepared.channel, prepared.encoded))
}

/// What one accepted notification carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedNotice {
    /// The publication travelled inside the notification.
    Inline(Publication),
    /// The publication is in `orbitcast_payloads`; fetch it by reference.
    ///
    /// The reference is routing, not authorization: the fetch re-checks the
    /// namespace and stream of the stored row against it.
    Overflow(OverflowRef),
}

/// Strictly decode a notification payload into the notice it carries.
///
/// Validation order: size, decode, envelope semantics, namespace ownership,
/// then channel agreement. Every failure is an error; nothing is repaired or
/// partially accepted.
///
/// # Errors
///
/// - [`PubSubError::PayloadTooLarge`] when `payload` reaches
///   [`MAX_NOTIFY_PAYLOAD_LEN`],
/// - [`PubSubError::Protocol`] when it is not a valid v1 notice (malformed
///   JSON, unknown `kind`, unsupported version, invalid fields),
/// - [`PubSubError::NamespaceMismatch`] when it belongs to another namespace,
/// - [`PubSubError::ChannelMismatch`] when `channel` is not the one its own
///   `(namespace, stream)` derives.
pub fn decode_notice(
    expected_namespace: &str,
    channel: &str,
    payload: &str,
) -> Result<DecodedNotice, PubSubError> {
    if payload.len() >= MAX_NOTIFY_PAYLOAD_LEN {
        return Err(PubSubError::PayloadTooLarge {
            len: payload.len(),
            max: MAX_NOTIFY_PAYLOAD_LEN,
        });
    }

    let notice: PublicationNotice =
        serde_json::from_str(payload).map_err(ProtocolError::Malformed)?;
    notice.validate()?;

    if notice.namespace() != expected_namespace {
        return Err(PubSubError::NamespaceMismatch {
            expected: expected_namespace.to_string(),
            actual: notice.namespace().to_string(),
        });
    }

    let expected_channel = channel_for(notice.namespace(), notice.stream());
    if expected_channel != channel {
        return Err(PubSubError::ChannelMismatch {
            stream: notice.stream().to_string(),
            channel: channel.to_string(),
            expected: expected_channel,
        });
    }

    match notice.kind {
        NoticeKind::Inline { publication } => Ok(DecodedNotice::Inline(publication)),
        NoticeKind::Overflow { id, namespace, stream } => {
            Ok(DecodedNotice::Overflow(OverflowRef { id, namespace, stream }))
        }
    }
}

/// What a backend hands to the listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubEvent {
    /// One accepted publication: the logical stream and the exact JSON text
    /// of its payload.
    Publication { stream: String, payload: Bytes },
    /// A publication for `stream` was lost between the publisher and this
    /// node (an overflow row that could not be fetched, a fetch that missed
    /// its deadline). Notification-only delivery cannot recover it: the
    /// stream's local members must be told to resubscribe.
    Gap { stream: String, reason: String },
}

/// The listener callback: runs on the delivery task and must not block.
pub type EventHandler = Box<dyn Fn(PubSubEvent) + Send + Sync>;

/// Pub/Sub backend.
///
/// Implementations move publications between nodes. The hub is the only
/// consumer, and it receives logical stream names plus validated JSON payload
/// bytes.
///
/// Object-safe on purpose: the server holds `Arc<dyn PubSub>`, which is what
/// lets a build select its backend at startup and lets tests drive the server
/// with a scripted one.
#[async_trait]
pub trait PubSub: Send + Sync {
    /// Publish `json_payload` (exact Rails-encoded JSON text) to `stream`
    /// under a freshly minted publication id.
    ///
    /// Returns once the backend accepted the publication, not once it was
    /// delivered. The publishing node receives its own publication back through
    /// its own listener; backends never fan out locally as well.
    async fn publish(&self, stream: &str, json_payload: &str) -> Result<(), PubSubError>;

    /// Publish one fully formed publication, id included.
    ///
    /// The id is preserved end to end (notice, overflow row, acknowledgement),
    /// so a retried publication carries the same id and can be deduplicated
    /// downstream.
    async fn publish_publication(&self, publication: &Publication) -> Result<(), PubSubError>;

    /// Subscribe to `stream`, returning only once the backend acknowledged it.
    ///
    /// After `Ok`, every publication the backend accepts for `stream` is
    /// delivered to the [`PubSub::listen`] callback.
    async fn subscribe(&self, stream: &str) -> Result<(), PubSubError>;

    /// Drop interest in `stream`, returning only once the backend acknowledged.
    async fn unsubscribe(&self, stream: &str) -> Result<(), PubSubError>;

    /// Whether the backend can currently accept subscriptions and publications.
    fn is_ready(&self) -> bool;

    /// Overload and rejection counters.
    fn stats(&self) -> StatsSnapshot;

    /// The stream of raw control notification payloads, for backends that
    /// carry Rails-initiated control messages. Taken once; `None` for
    /// backends without a control channel, and after the first call.
    async fn take_control_stream(&self) -> Option<mpsc::Receiver<String>> {
        None
    }

    /// Consume delivered events until the backend shuts down.
    ///
    /// Spawn this once; a second call fails with
    /// [`PubSubError::ListenerAlreadyStarted`]. The callback runs on the
    /// delivery task and must not block.
    async fn listen(&self, on_event: EventHandler) -> Result<(), PubSubError>;
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests;
