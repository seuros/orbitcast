//! Retained publication store.
//!
//! Ephemeral pub/sub and durable history are **different concepts** and this
//! module is the second one. [`crate::pubsub`] carries a publication between
//! nodes and forgets it; a broker-enabled stream additionally *retains* every
//! publication with an order, an identity, and an expiry, so a client that
//! comes back with a cursor can be replayed.
//!
//! ```text
//! publish  ->  broker.retain()  ->  [ lock stream position | insert row |
//!                                     bump position | trim | pg_notify ]  (one tx)
//! resume   ->  broker.fetch_after(cursor)  ->  ordered rows  ->  delivery envelopes
//! ```
//!
//! # Ordering: an explicit per-stream lock, never a global sequence
//!
//! Sequences are assigned inside the transaction that inserts the message,
//! while holding `SELECT ... FOR UPDATE` on that stream's
//! `orbitcast_stream_positions` row. Two concurrent publishers to one stream
//! therefore serialize: the second one waits for the first to **commit** before
//! it reads `next_seq`, so sequence order is commit order and a reader can
//! treat "seq > `last_delivered`" as "everything I have not seen".
//!
//! An unconstrained global `BIGSERIAL` cannot do this: two
//! transactions can take sequence values in one order and commit in the other,
//! and a reader that had already passed the higher value would skip the lower
//! one forever. A per-stream lock also costs nothing on unrelated streams.
//!
//! # Generations
//!
//! A generation is bumped whenever a stream's retained history is *wiped*
//! (retention dropped everything, or an operator truncated it). It makes a
//! stale cursor recognizable instead of silently matching sequence numbers from
//! a new era, and it is what turns "your history is gone" into an explicit
//! [`crate::protocol::v1::recovery::GapReason::UnknownGeneration`].
//!
//! # Deduplication
//!
//! `(namespace, stream, publication_id)` is unique within retention. A repeat
//! insert of the same publication id is a no-op that returns the **existing**
//! position: first write wins. The two shapes of a repeat are told apart and
//! counted separately ([`DuplicateKind`]):
//!
//! - **identical** - the same JSON text again. A publisher retried after a
//!   lost acknowledgement; expected, counted, logged at debug.
//! - **conflicting** - a different payload under the same id. Not stored and
//!   not delivered, because the id *is* the client's deduplication key;
//!   counted and warned, because two publications were given one identity,
//!   which is a publisher bug.
//!
//! After the retention window the id is forgotten and the same id may be stored
//! again; deduplication is explicitly scoped to retention.
//!
//! # Retention, and where each bound is enforced
//!
//! | bound | default | enforced |
//! | --- | --- | --- |
//! | time (`retention`) | 300 s | sweeper, batched `DELETE` on `(namespace, expires_at)` |
//! | messages per stream | 1000 | publish transaction, one indexed range `DELETE` |
//! | bytes per stream | 4 MiB | sweeper, per-stream size query then bounded `DELETE` |
//! | messages per namespace | 1 000 000 | sweeper, oldest rows first, bounded `DELETE` |
//! | bytes per namespace | 1 GiB | sweeper, oldest rows first, bounded `DELETE` |
//!
//! The count bound is enforced inline because it is exactly one indexed range
//! delete on the primary key and each publish adds exactly one row, so the
//! work per publish is O(1). The byte bound is *not* inline: it needs a
//! `sum(length(json_payload))` over the stream, which would turn every publish
//! into an aggregate scan. Every sweeper bound runs in bounded batches, so a
//! backlog costs many short statements rather than one long lock.
//!
//! The namespace quotas are a ceiling on the whole deployment's history, so a
//! burst across many streams cannot grow the table without bound. Trimming is
//! oldest-first across the namespace, which leaves holes in the trimmed
//! streams: a resuming client sees an honest `expired` gap there, never a
//! silently skipped sequence. The same aggregate that enforces them is
//! exported as `orbitcast_broker_messages_current` /
//! `orbitcast_broker_bytes_current`, refreshed every reconcile interval.
//!
//! # Large publications
//!
//! A retained row holds the full payload text, whatever its size. The
//! notification that announces it does not: when the inline notice would not
//! fit a `NOTIFY` payload, the ordering transaction also writes the overflow
//! relay row and announces the usual overflow *reference* (the same notice
//! every ephemeral listener already understands), so live delivery reads the
//! relay row and replay reads the broker row. Without the relay enabled an
//! oversized publication on a retained stream is refused, never retained
//! un-announced or announced un-retained.
//!
//! # NOTIFY is a wakeup
//!
//! The retained row is the durable record; the notification only says "look
//! again". Readers query ordered committed rows after their own watermark,
//! notice a sequence jump as a gap, and reconcile on a timer even when no
//! notification ever arrives (a dropped queue, a lost listener connection, or a
//! node that was simply not running).
//!
//! # Sessions
//!
//! A recovery session row is keyed by `(namespace, session_id)`: the identity
//! reference the application put in the recovery token. Fencing is therefore
//! **per session**, not per token - two different tokens minted for one
//! session fence each other, so the newest resume of a session always wins,
//! wherever it lands. The row carries the fencing generation, the last
//! acknowledged cursors ([`crate::protocol::v1::recovery::SessionSnapshot`]), and an expiry that every
//! resume, acknowledgement, and live tail extends: a row lives
//! [`BrokerConfig::session_ttl`] past the session's last activity, after which
//! the next resume starts a fresh lease and finds no snapshot.
//!
//! # Features
//!
//! The broker is PostgreSQL-only: retained history needs a database. This
//! module (configuration, bounds, the [`Broker`] trait) compiles in every
//! feature set so the configuration boundary and its
//! tests exist everywhere; only the implementation is gated, exactly like the
//! PostgreSQL pub/sub backend. A `memory` build has no broker and therefore no
//! recovery: [`BrokerConfig::validate`] rejects an enabled broker there.

#[cfg(all(feature = "postgres", not(feature = "memory")))]
pub mod postgres;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use crate::protocol::ProtocolError;
use crate::protocol::v1::publication::Publication;
use crate::protocol::v1::recovery::Cursor;
use crate::pubsub::PubSubError;

/// Retained history lifetime, from the database clock.
pub const DEFAULT_RETENTION: Duration = Duration::from_mins(5);

/// Retained messages kept per stream.
pub const DEFAULT_MAX_MESSAGES_PER_STREAM: u64 = 1000;

/// Retained bytes kept per stream (4 MiB of payload text).
pub const DEFAULT_MAX_BYTES_PER_STREAM: u64 = 4 * 1024 * 1024;

/// How often a reader re-queries even without a notification.
pub const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

/// Live deliveries buffered per session while a replay runs.
pub const DEFAULT_REPLAY_BUFFER: usize = 256;

/// Lifetime of a recovery session row (snapshot + fencing generation), from
/// the session's last activity.
pub const DEFAULT_SESSION_TTL: Duration = Duration::from_hours(1);

/// Retained messages kept per namespace, across every stream.
pub const DEFAULT_MAX_MESSAGES_PER_NAMESPACE: u64 = 1_000_000;

/// Retained bytes kept per namespace, across every stream (1 GiB).
pub const DEFAULT_MAX_BYTES_PER_NAMESPACE: u64 = 1024 * 1024 * 1024;

/// How often a live session extends its row's expiry, at most.
///
/// A tail checks whether a touch is due on every reconcile pass; this bounds
/// the writes to one per session per minute however busy the stream is.
pub const SESSION_TOUCH_INTERVAL: Duration = Duration::from_mins(1);

/// Rows read per replay batch.
pub const REPLAY_BATCH: usize = 500;

/// Upper bound on the configurable retention window (24 h).
pub const MAX_RETENTION: Duration = Duration::from_hours(24);

/// Upper bound on the configurable per-stream message count.
pub const MAX_MESSAGES_PER_STREAM_LIMIT: u64 = 1_000_000;

/// Upper bound on the configurable per-stream byte budget (1 GiB).
pub const MAX_BYTES_PER_STREAM_LIMIT: u64 = 1024 * 1024 * 1024;

/// Upper bound on the configurable replay buffer.
pub const MAX_REPLAY_BUFFER: usize = 65_536;

/// Upper bound on the configurable per-namespace message count.
pub const MAX_MESSAGES_PER_NAMESPACE_LIMIT: u64 = 100_000_000;

/// Upper bound on the configurable per-namespace byte budget (64 GiB).
pub const MAX_BYTES_PER_NAMESPACE_LIMIT: u64 = 64 * 1024 * 1024 * 1024;

// The three broker tables. Fixed: a cross-language contract shared with
// `gem/db/orbitcast_migrate/003_create_orbitcast_broker.rb` and
// `004_rekey_orbitcast_sessions_by_session_id.rb`, and compiled with the
// PostgreSQL implementation only - no other build has them.

/// Retained message table.
#[cfg(all(feature = "postgres", not(feature = "memory")))]
pub const MESSAGES_TABLE: &str = "orbitcast_messages";

/// Per-stream position table (generation + next sequence).
#[cfg(all(feature = "postgres", not(feature = "memory")))]
pub const POSITIONS_TABLE: &str = "orbitcast_stream_positions";

/// Recovery session table (snapshot + fencing generation), keyed by
/// `(namespace, session_id)` since migration 004.
#[cfg(all(feature = "postgres", not(feature = "memory")))]
pub const SESSIONS_TABLE: &str = "orbitcast_sessions";

/// Static configuration of the retained broker.
///
/// Off by default: retention needs the migration in
/// `gem/db/orbitcast_migrate/003_create_orbitcast_broker.rb`, and no code path
/// ever creates those tables implicitly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerConfig {
    /// Retain publications and allow recovery at all.
    pub enabled: bool,
    /// Stream prefixes routed through the broker. Empty means *every* stream.
    ///
    /// A prefix allowlist (rather than exact names) is what lets an
    /// application retain `chat:` while leaving high-volume presence or
    /// telemetry streams ephemeral.
    pub streams: Vec<String>,
    /// Retained history lifetime, from the database clock.
    pub retention: Duration,
    /// Retained messages kept per stream (enforced in the publish transaction).
    pub max_messages_per_stream: u64,
    /// Retained payload bytes kept per stream (enforced by the sweeper).
    pub max_bytes_per_stream: u64,
    /// How often a reader re-queries without a notification.
    pub reconcile_interval: Duration,
    /// Live deliveries buffered per session during replay; overflow is a gap.
    pub replay_buffer: usize,
    /// Lifetime of a recovery session row, from its last activity.
    pub session_ttl: Duration,
    /// Retained messages kept per namespace, across every stream (sweeper).
    pub max_messages_per_namespace: u64,
    /// Retained payload bytes kept per namespace, across every stream
    /// (sweeper).
    pub max_bytes_per_namespace: u64,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            streams: Vec::new(),
            retention: DEFAULT_RETENTION,
            max_messages_per_stream: DEFAULT_MAX_MESSAGES_PER_STREAM,
            max_bytes_per_stream: DEFAULT_MAX_BYTES_PER_STREAM,
            reconcile_interval: DEFAULT_RECONCILE_INTERVAL,
            replay_buffer: DEFAULT_REPLAY_BUFFER,
            session_ttl: DEFAULT_SESSION_TTL,
            max_messages_per_namespace: DEFAULT_MAX_MESSAGES_PER_NAMESPACE,
            max_bytes_per_namespace: DEFAULT_MAX_BYTES_PER_NAMESPACE,
        }
    }
}

impl BrokerConfig {
    /// Whether `stream` is retained.
    ///
    /// An empty allowlist retains everything; otherwise a stream is retained
    /// when it starts with one of the configured prefixes. Matching is exact
    /// prefix matching on bytes - no globbing, no regular expressions - so the
    /// allowlist cannot accidentally widen.
    #[must_use]
    pub fn handles(&self, stream: &str) -> bool {
        if !self.enabled {
            return false;
        }
        self.streams.is_empty() || self.streams.iter().any(|prefix| stream.starts_with(prefix))
    }

    /// Reject unusable broker settings at the configuration boundary.
    ///
    /// A disabled broker is never rejected: its bounds are inert.
    ///
    /// # Errors
    ///
    /// [`BrokerError::Config`] when the broker is enabled and either this is a
    /// `memory` build (no durable history), or a bound is out of range: zero
    /// or above-ceiling `retention`, `max_messages_per_stream`,
    /// `max_bytes_per_stream`, or `replay_buffer`; a zero
    /// `reconcile_interval` or `session_ttl`; a namespace quota that is zero,
    /// above its ceiling, or below the matching per-stream bound; or an empty
    /// prefix in `streams`.
    pub fn validate(&self) -> Result<(), BrokerError> {
        if !self.enabled {
            return Ok(());
        }

        if cfg!(feature = "memory") {
            return Err(BrokerError::Config(
                "the retained broker requires the postgres backend; a memory build has no \
                 durable history"
                    .to_string(),
            ));
        }

        if self.retention.is_zero() || self.retention > MAX_RETENTION {
            return Err(BrokerError::Config(format!(
                "broker_retention_secs must be between 1 and {} seconds",
                MAX_RETENTION.as_secs()
            )));
        }

        if self.max_messages_per_stream == 0
            || self.max_messages_per_stream > MAX_MESSAGES_PER_STREAM_LIMIT
        {
            return Err(BrokerError::Config(format!(
                "broker_max_messages_per_stream must be between 1 and \
                 {MAX_MESSAGES_PER_STREAM_LIMIT}"
            )));
        }

        if self.max_bytes_per_stream == 0 || self.max_bytes_per_stream > MAX_BYTES_PER_STREAM_LIMIT
        {
            return Err(BrokerError::Config(format!(
                "broker_max_bytes_per_stream must be between 1 and {MAX_BYTES_PER_STREAM_LIMIT}"
            )));
        }

        if self.reconcile_interval.is_zero() {
            return Err(BrokerError::Config(
                "broker_reconcile_interval_ms must be greater than 0: a reader that never \
                 re-queries cannot repair a missed notification"
                    .to_string(),
            ));
        }

        if self.replay_buffer == 0 || self.replay_buffer > MAX_REPLAY_BUFFER {
            return Err(BrokerError::Config(format!(
                "broker_replay_buffer must be between 1 and {MAX_REPLAY_BUFFER}"
            )));
        }

        if self.session_ttl.is_zero() {
            return Err(BrokerError::Config(
                "broker_session_ttl_secs must be greater than 0".to_string(),
            ));
        }

        if self.max_messages_per_namespace == 0
            || self.max_messages_per_namespace > MAX_MESSAGES_PER_NAMESPACE_LIMIT
        {
            return Err(BrokerError::Config(format!(
                "broker_max_messages_per_namespace must be between 1 and \
                 {MAX_MESSAGES_PER_NAMESPACE_LIMIT}"
            )));
        }
        if self.max_messages_per_namespace < self.max_messages_per_stream {
            return Err(BrokerError::Config(
                "broker_max_messages_per_namespace must not be below \
                 broker_max_messages_per_stream: a namespace holds at least one full stream"
                    .to_string(),
            ));
        }

        if self.max_bytes_per_namespace == 0
            || self.max_bytes_per_namespace > MAX_BYTES_PER_NAMESPACE_LIMIT
        {
            return Err(BrokerError::Config(format!(
                "broker_max_bytes_per_namespace must be between 1 and \
                 {MAX_BYTES_PER_NAMESPACE_LIMIT}"
            )));
        }
        if self.max_bytes_per_namespace < self.max_bytes_per_stream {
            return Err(BrokerError::Config(
                "broker_max_bytes_per_namespace must not be below broker_max_bytes_per_stream: \
                 a namespace holds at least one full stream"
                    .to_string(),
            ));
        }

        if self.streams.iter().any(String::is_empty) {
            return Err(BrokerError::Config(
                "broker_streams must not contain an empty prefix; leave the list empty to \
                 retain every stream"
                    .to_string(),
            ));
        }

        Ok(())
    }
}

/// Broker failures.
///
/// Payloads never appear in a message: a log line carries identities and
/// positions, never application data.
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    /// Static configuration is unusable; never a transient condition.
    #[error("broker configuration error: {0}")]
    Config(String),
    /// The retained store has no usable connection right now.
    #[error("broker store is not ready")]
    NotReady,
    /// A stored row failed the same validation an inbound envelope gets.
    #[error("retained message for stream {stream:?} is invalid: {source}")]
    InvalidRow { stream: String, source: ProtocolError },
    /// The requested position is no longer retained.
    #[error(
        "stream {stream:?} cannot be replayed from generation {generation} sequence {sequence}: \
         {reason}"
    )]
    Gap { stream: String, generation: u64, sequence: u64, reason: &'static str },
    /// The publication cannot be announced: it fails envelope validation, or
    /// it does not fit a notification and the overflow relay is off.
    ///
    /// Carries the pub/sub refusal unchanged so the ingress answers exactly
    /// as it would for an ephemeral stream.
    #[error("retained publication refused: {0}")]
    Publication(#[source] PubSubError),
    /// Transport-level failure (connection, query, TLS).
    #[error("broker store error: {0}")]
    Backend(#[from] anyhow::Error),
}

/// Which kind of repeat a duplicate publication id was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicateKind {
    /// Same id, byte-identical payload text: a publisher retry.
    Identical,
    /// Same id, different payload: two publications share one identity.
    Conflicting,
}

impl DuplicateKind {
    /// The `kind` label value on `orbitcast_broker_duplicates_total`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Identical => "identical",
            Self::Conflicting => "conflicting",
        }
    }
}

/// Broker counters and growth gauges.
///
/// Shared between the store (retained, duplicates, swept, fenced, growth) and
/// the recovery controller (replayed, gaps, reconciled): one registry per
/// broker, exported as `orbitcast_broker_*`.
#[derive(Debug, Default)]
pub struct BrokerStats {
    retained: AtomicU64,
    duplicates_identical: AtomicU64,
    duplicates_conflicting: AtomicU64,
    replayed: AtomicU64,
    gaps: AtomicU64,
    reconciled: AtomicU64,
    swept: AtomicU64,
    fenced: AtomicU64,
    messages_current: AtomicU64,
    bytes_current: AtomicU64,
}

/// Point-in-time copy of [`BrokerStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BrokerStatsSnapshot {
    /// Publications committed to retained history.
    pub retained: u64,
    /// Repeat publication ids carrying the same payload (publisher retries).
    pub duplicates_identical: u64,
    /// Repeat publication ids carrying a different payload (publisher bugs).
    pub duplicates_conflicting: u64,
    /// Retained messages replayed to a resuming session.
    pub replayed: u64,
    /// Streams a session could not be replayed on.
    pub gaps: u64,
    /// Reader passes that found rows a notification never announced.
    pub reconciled: u64,
    /// Rows removed by the sweeper.
    pub swept: u64,
    /// Recovery leases taken (each one fences an older holder out).
    pub fenced: u64,
    /// Unexpired retained messages in this node's namespace, as of the last
    /// growth refresh.
    pub messages_current: u64,
    /// Unexpired retained payload bytes in this node's namespace, as of the
    /// last growth refresh.
    pub bytes_current: u64,
}

impl BrokerStats {
    pub fn record_retained(&self) {
        self.retained.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_duplicate(&self, kind: DuplicateKind) {
        let counter = match kind {
            DuplicateKind::Identical => &self.duplicates_identical,
            DuplicateKind::Conflicting => &self.duplicates_conflicting,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_replayed(&self, count: u64) {
        self.replayed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_gap(&self) {
        self.gaps.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_reconciled(&self) {
        self.reconciled.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_swept(&self, rows: u64) {
        self.swept.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn record_fenced(&self) {
        self.fenced.fetch_add(1, Ordering::Relaxed);
    }

    /// Replace the growth gauges with a fresh aggregate.
    pub fn set_growth(&self, messages: u64, bytes: u64) {
        self.messages_current.store(messages, Ordering::Relaxed);
        self.bytes_current.store(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> BrokerStatsSnapshot {
        BrokerStatsSnapshot {
            retained: self.retained.load(Ordering::Relaxed),
            duplicates_identical: self.duplicates_identical.load(Ordering::Relaxed),
            duplicates_conflicting: self.duplicates_conflicting.load(Ordering::Relaxed),
            replayed: self.replayed.load(Ordering::Relaxed),
            gaps: self.gaps.load(Ordering::Relaxed),
            reconciled: self.reconciled.load(Ordering::Relaxed),
            swept: self.swept.load(Ordering::Relaxed),
            fenced: self.fenced.load(Ordering::Relaxed),
            messages_current: self.messages_current.load(Ordering::Relaxed),
            bytes_current: self.bytes_current.load(Ordering::Relaxed),
        }
    }
}

/// One retained publication read back from the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedMessage {
    /// Logical stream.
    pub stream: String,
    /// Publication UUID: the deduplication key a client sees.
    pub publication_id: String,
    /// Position in the stream's history.
    pub position: Cursor,
    /// Exact Rails-encoded JSON text of the payload.
    pub json_payload: String,
}

/// What one [`Broker::retain`] call committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retained {
    /// The position the publication occupies.
    pub position: Cursor,
    /// `Some` when this publication id was already retained (first write
    /// wins): nothing was stored or announced this time.
    pub duplicate: Option<DuplicateKind>,
    /// Whether the announcement went through the overflow relay because the
    /// inline notice would not fit a notification.
    pub relayed: bool,
}

/// What one stream's retained history currently spans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRange {
    /// Current generation and last committed sequence.
    pub watermark: Cursor,
    /// Oldest still-retained sequence, or `None` when nothing is retained.
    pub oldest: Option<u64>,
}

impl StreamRange {
    /// Whether history starting strictly after `cursor` is fully retained.
    ///
    /// A cursor from a different generation is never replayable, and a cursor
    /// older than the oldest retained row means the hole cannot be filled.
    #[must_use]
    pub const fn covers(&self, cursor: Cursor) -> bool {
        if cursor.generation != self.watermark.generation {
            return false;
        }
        if cursor.sequence >= self.watermark.sequence {
            // Nothing to replay: the client is already at (or past) the live
            // edge, which is trivially covered.
            return true;
        }
        match self.oldest {
            // The next message the client needs is `cursor.sequence + 1`.
            Some(oldest) => oldest <= cursor.sequence + 1,
            None => false,
        }
    }
}

/// Retained publication store.
///
/// Object-safe on purpose: the runtime holds `Arc<dyn Broker>`, so a build
/// without the PostgreSQL implementation simply has none rather than needing a
/// second compile-time alias.
#[async_trait]
pub trait Broker: Send + Sync {
    /// Retain one publication and announce it, atomically.
    ///
    /// The sequence is assigned under the stream's position lock and the
    /// notification becomes visible on commit, so no node can be woken for a
    /// row it cannot read. A repeat `publication.id` returns the existing
    /// position with [`Retained::duplicate`] set and stores nothing. A
    /// publication too large to announce inline is announced through the
    /// overflow relay when it is enabled, and refused otherwise.
    ///
    /// # Errors
    ///
    /// [`BrokerError::Publication`] when the publication cannot be announced
    /// (invalid envelope, or oversized with the relay off), and the store's
    /// own errors for everything else.
    async fn retain(&self, publication: &Publication) -> Result<Retained, BrokerError>;

    /// The span of history currently retained for `stream`.
    async fn range(&self, stream: &str) -> Result<StreamRange, BrokerError>;

    /// Ordered messages strictly after `after`, at most `limit` of them.
    async fn fetch_after(
        &self,
        stream: &str,
        after: Cursor,
        limit: usize,
    ) -> Result<Vec<RetainedMessage>, BrokerError>;

    /// Atomically claim the recovery lease for `session_id`.
    ///
    /// Returns the new fencing generation and extends the row's expiry. A
    /// concurrent claim - on this node or any other, with this token or
    /// another token for the same session - returns a strictly greater value,
    /// and the holder of the lower value has lost: its replay must be
    /// cancelled.
    async fn fence(&self, session_id: &str) -> Result<u64, BrokerError>;

    /// The current fencing generation, or `0` when no live session row
    /// exists.
    async fn fencing_generation(&self, session_id: &str) -> Result<u64, BrokerError>;

    /// Extend the session row's expiry to a full `session_ttl` from now.
    ///
    /// Called by a live tail at most every [`SESSION_TOUCH_INTERVAL`], so a
    /// quiet but connected session is never swept out from under itself. A
    /// row that no longer exists is left alone: the next resume recreates it.
    async fn touch_session(&self, session_id: &str) -> Result<(), BrokerError>;

    /// Best-effort snapshot write. Never a precondition for delivery.
    async fn save_snapshot(&self, session_id: &str, snapshot: &str) -> Result<(), BrokerError>;

    /// The stored snapshot text, if a live session row has one.
    async fn load_snapshot(&self, session_id: &str) -> Result<Option<String>, BrokerError>;

    /// The counter registry the store and the recovery controller share.
    fn counters(&self) -> &BrokerStats;

    /// Counters for `/metrics`.
    fn stats(&self) -> BrokerStatsSnapshot {
        self.counters().snapshot()
    }
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests;
