//! Presence: the authorization front, the backend contract, and the stores
//! behind it.
//!
//! # Dispatch mode only
//!
//! Presence exists only where Rails grants it. Every lease is the result of a
//! `presence_grant` effect Rails returned for one `(connection, subscription,
//! stream)`; a signed-only node has no grants, so it has no presence at all.
//! There is no client-chosen identity anywhere: the legacy `join` / `leave` /
//! `presence` top-level commands are parsed for compatibility and dropped as
//! unsupported in every mode.
//!
//! | store | scope | enabled by |
//! | --- | --- | --- |
//! | [`postgres`] | the whole deployment | a PostgreSQL build with Rails dispatch and a `database_url` |
//! | [`memory`] | one process | a `memory` build with Rails dispatch (development, tests) |
//!
//! Both honour the same contract ([`PresenceBackend`]): per-session leases,
//! subject-level transitions, expiry, paged snapshots. The memory store makes
//! no cross-node claim, exactly like the memory pub/sub it rides on.
//!
//! # The authorization front
//!
//! [`PresenceGrants`] is the per-session registry the `presence_grant` /
//! `presence_revoke` effects feed. It is the *only* source of presence
//! identity: [`PresenceGrants::authorize`] resolves a client operation to the
//! granted subject and metadata, or refuses. A client that never received a
//! grant can do nothing, and a client that did cannot widen it - it cannot
//! choose another subject, attach its own metadata, extend the TTL, or run an
//! operation outside the grant's allowlist.
//!
//! A grant is bound to a subscription's stream: the relay refuses a grant for
//! a stream the subscription does not hold, and drops the grant - releasing
//! its lease - the moment the subscription stops streaming from it
//! (`stop_stream_from`, `stop_all_streams`, unsubscribe, socket close).
//!
//! # Change notifications
//!
//! Presence transitions travel over the ordinary pub/sub backend on a
//! **derived presence stream key**: [`presence_stream_key`] prefixes the
//! application stream with [`PRESENCE_STREAM_PREFIX`], and the existing
//! `channel_for(namespace, key)` derivation then produces a namespaced
//! channel for it. Two consequences, both deliberate:
//!
//! 1. Interest in a presence key is acquired and released by the ordinary hub
//!    membership machinery (a grant installs the key as a membership of the
//!    granted subscription), so unsubscribe, teardown, and drain release it
//!    with no parallel bookkeeping, and only granted subscriptions receive
//!    roster events - each addressed with its own identifier.
//! 2. Stream names beginning with `presence:` are **reserved**. An application
//!    stream literally named `presence:chat:42` would share a channel with the
//!    roster of `chat:42`; the publication envelope's own namespace/stream
//!    validation still applies, but do not name streams that way.
//!
//! # Liveness
//!
//! Leases are refreshed only for connections whose socket produced *inbound*
//! traffic inside the TTL ([`PresenceSessions`]). On the direct HTTP listener
//! that is a WebSocket control pong answering the server's ping, so an idle
//! browser tab stays present without the page doing anything. Under the
//! Mothership docking transport the WebSocket is terminated by the supervisor
//! and no pong reaches this process - the docking protocol carries no
//! client-activity frame of its own - so a docked connection is refreshable
//! only while the client is sending frames: an idle tab's lease expires and
//! the subject leaves until the next `join`.
//!
//! Each lease is refreshed with the TTL *its grant* carried, for its whole
//! life: a batch refresh never resets a lease to the node's configured bound.
//!
//! # Consistency
//!
//! Presence is eventually consistent and is not an authorization database.
//! Expiry bounds staleness after a node dies (nothing else will remove its
//! leases), a lost notification is repaired by the next snapshot rather than
//! replayed, and a pub/sub reconnect resynchronizes by dropping the snapshot
//! cache and re-announcing a roster page to every granted subscription.

pub mod memory;

#[cfg(all(feature = "postgres", not(feature = "memory")))]
pub mod postgres;

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tracing::debug;

use crate::protocol::ProtocolError;
use crate::protocol::v1::presence::{
    PresenceMember, PresenceOp, PresencePage, validate_metadata, validate_subject_id,
};
use crate::protocol::v1::validate_stream;
use crate::pubsub::{PubSub, length_prefix};

/// Prefix of every derived presence stream key. Reserved: see the module
/// documentation.
pub const PRESENCE_STREAM_PREFIX: &str = "presence:";

/// Name of the presence table. Fixed: it is a cross-language contract shared
/// with `gem/db/orbitcast_migrate/002_create_orbitcast_presence.rb`.
///
/// Compiled with the PostgreSQL store only: no other build has the table.
#[cfg(all(feature = "postgres", not(feature = "memory")))]
pub const PRESENCE_TABLE: &str = "orbitcast_presence";

/// Default lease TTL (45 seconds).
pub const DEFAULT_PRESENCE_TTL_MS: u64 = 45_000;

/// Default batch refresh period (10 seconds).
pub const DEFAULT_PRESENCE_REFRESH_INTERVAL_MS: u64 = 10_000;

/// Ceiling on a configured or granted lease TTL (10 minutes).
///
/// A lease is a liveness statement, not a session: a longer TTL would mean a
/// dead node's roster entries linger for longer than anyone would wait.
pub const MAX_PRESENCE_TTL_MS: u64 = 600_000;

/// The membership one lease belongs to: a subject on a stream, in a namespace.
///
/// This triple is the *per-membership key* first-join/last-leave transitions
/// are serialized under.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MembershipKey {
    /// Deployment namespace; never crosses deployments.
    pub namespace: String,
    /// The application stream the roster belongs to.
    pub stream: String,
    /// The granted subject.
    pub subject_id: String,
}

impl MembershipKey {
    /// Build a key.
    pub fn new(
        namespace: impl Into<String>,
        stream: impl Into<String>,
        subject_id: impl Into<String>,
    ) -> Self {
        Self { namespace: namespace.into(), stream: stream.into(), subject_id: subject_id.into() }
    }

    /// The PostgreSQL advisory-lock key for this membership.
    ///
    /// SHA-256 over the length-prefixed UTF-8 triple
    ///
    /// ```text
    /// u32-BE(namespace.len()) || namespace
    ///   || u32-BE(stream.len()) || stream
    ///   || u32-BE(subject_id.len()) || subject_id
    /// ```
    ///
    /// with the first 8 bytes read big-endian as the `bigint` handed to
    /// `pg_advisory_xact_lock`. Length prefixing is what stops
    /// `("ab", "c", …)` and `("a", "bc", …)` from sharing a lock, exactly as in
    /// the channel derivation. Two different memberships *may* still collide in
    /// 64 bits: a collision costs serialization, never correctness, because the
    /// lock only orders the decision that the transactional counts then make.
    #[must_use]
    pub fn lock_key(&self) -> i64 {
        let mut hasher = Sha256::new();
        for part in [&self.namespace, &self.stream, &self.subject_id] {
            hasher.update(length_prefix(part));
            hasher.update(part.as_bytes());
        }
        let digest = hasher.finalize();
        let mut head = [0u8; 8];
        head.copy_from_slice(&digest[..8]);
        i64::from_be_bytes(head)
    }

    /// Semantic validation of a key built from a grant.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::EmptyField`] or [`ProtocolError::FieldTooLong`] for
    /// the first invalid part, checked in order: `namespace`, `stream`, then
    /// `subject_id`.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        crate::protocol::v1::validate_namespace(&self.namespace)?;
        validate_stream(&self.stream)?;
        validate_subject_id(&self.subject_id)
    }
}

/// The derived pub/sub stream key carrying `stream`'s roster changes.
#[must_use]
pub fn presence_stream_key(stream: &str) -> String {
    format!("{PRESENCE_STREAM_PREFIX}{stream}")
}

/// The application stream a presence key belongs to, if it is one.
#[must_use]
pub fn presence_stream_target(key: &str) -> Option<&str> {
    key.strip_prefix(PRESENCE_STREAM_PREFIX)
}

/// One session's claim on a membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    /// Which membership is claimed.
    pub key: MembershipKey,
    /// The server-generated session id holding it. Unique per connection, so
    /// one tab's refresh can never preserve another tab's lease.
    pub session_id: String,
    /// The node holding it, for operational attribution and batch refresh.
    pub node_id: String,
    /// The granted public metadata, as exact JSON text.
    pub metadata_json: String,
    /// Lifetime from the *database* clock, never a node's.
    pub ttl: Duration,
}

impl Lease {
    /// The refresh record for this lease: what a batch refresh needs and
    /// nothing more.
    #[must_use]
    pub fn held(&self) -> HeldLease {
        HeldLease { key: self.key.clone(), session_id: self.session_id.clone(), ttl: self.ttl }
    }
}

/// A lease this node holds, as the batch refresher sees it.
///
/// Carries the lease's *own* TTL, so a refresh extends it by what the grant
/// said rather than by the node's configured bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldLease {
    /// Which membership is held.
    pub key: MembershipKey,
    /// The session holding it.
    pub session_id: String,
    /// The lease's granted lifetime.
    pub ttl: Duration,
}

/// What one join or leave did to the subject's presence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// The subject had no live lease and now has one: announce a join.
    FirstJoin,
    /// The subject already had a lease; this one was added or refreshed.
    Refreshed,
    /// A lease went away but the subject still has others: announce nothing.
    Reduced,
    /// The subject's last live lease went away: announce a leave.
    LastLeave,
    /// Nothing changed (a leave for a lease that was already gone).
    Unchanged,
}

impl Transition {
    /// Whether this transition is announced to the stream.
    #[must_use]
    pub const fn is_announced(self) -> bool {
        matches!(self, Self::FirstJoin | Self::LastLeave)
    }
}

/// A transition plus the roster size it left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransitionOutcome {
    /// What happened.
    pub transition: Transition,
    /// Distinct live subjects on the stream after the transaction committed.
    pub total: u64,
}

/// One page of a roster.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PresenceSnapshot {
    /// Members of this page, in stable `subject_id` order.
    pub members: Vec<PresenceMember>,
    /// Distinct live subjects on the stream.
    pub total: u64,
    /// Whether more members exist past this page.
    pub has_more: bool,
}

/// Presence failures.
///
/// Authorization refusals are values, not transport errors: a client that is
/// not granted is refused the same way whatever the backend is doing. Every
/// variant has a stable wire code ([`PresenceError::code`]) that the refusal
/// message sent back to the client carries.
#[derive(Debug, thiserror::Error)]
pub enum PresenceError {
    /// The connection holds no presence grant for this subscription.
    #[error("no active presence grant for this subscription")]
    NotGranted,
    /// The grant exists but does not allow this operation.
    #[error("presence operation {op} is not in this grant's allowlist")]
    OperationNotAllowed { op: PresenceOp },
    /// The grant itself was unusable (bad subject, oversized metadata, no TTL).
    #[error("invalid presence grant: {0}")]
    InvalidGrant(#[from] ProtocolError),
    /// The grant names a stream the subscription does not stream from.
    #[error("presence grant for {stream:?} names a stream this subscription does not hold")]
    StreamNotSubscribed { stream: String },
    /// The client's command did not decode, or asked for an out-of-range page.
    #[error("invalid presence command: {0}")]
    InvalidCommand(ProtocolError),
    /// No presence store is configured on this node.
    #[error("distributed presence is not configured on this node")]
    Unavailable,
    /// Transport-level failure (connection, query).
    #[error("presence backend error: {0}")]
    Backend(#[from] anyhow::Error),
}

impl PresenceError {
    /// The stable wire code of this failure: the `reason` of a presence
    /// `error` message (`src/protocol/v1/presence.rs`).
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::NotGranted => "not_granted",
            Self::OperationNotAllowed { .. } => "operation_not_allowed",
            Self::InvalidGrant(_) => "invalid_grant",
            Self::StreamNotSubscribed { .. } => "stream_not_subscribed",
            Self::InvalidCommand(_) => "invalid_command",
            Self::Unavailable => "unavailable",
            Self::Backend(_) => "backend",
        }
    }
}

/// One authorized presence membership, as Rails granted it.
///
/// Everything a client may act on lives here; nothing a client sends does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceGrant {
    /// The stream the roster belongs to.
    pub stream: String,
    /// The subject this connection is allowed to be. Never client-chosen.
    pub subject_id: String,
    /// Public metadata, as granted.
    pub metadata: Map<String, Value>,
    /// The same metadata as exact JSON text, which is what is stored.
    pub metadata_json: String,
    /// Lease lifetime: never zero, and clamped to at most
    /// [`MAX_PRESENCE_TTL_MS`] milliseconds.
    pub ttl: Duration,
    /// Operations this grant allows. Unknown names are dropped on the way in.
    pub operations: Vec<PresenceOp>,
}

impl PresenceGrant {
    /// Build a grant from a validated `presence_grant` effect.
    ///
    /// Re-validates rather than trusting the envelope check: this is the value
    /// identity is taken from, so it is checked where it is used.
    ///
    /// # Errors
    ///
    /// [`PresenceError::InvalidGrant`] when `stream`, `subject_id`, or
    /// `metadata` fails v1 validation, when `ttl_ms` is zero, or when the
    /// metadata cannot be encoded as JSON. A TTL above
    /// [`MAX_PRESENCE_TTL_MS`] is clamped, not rejected.
    pub fn from_effect(
        stream: &str,
        subject_id: &str,
        metadata: &Map<String, Value>,
        ttl_ms: u64,
        operations: &[String],
    ) -> Result<Self, PresenceError> {
        validate_stream(stream)?;
        validate_subject_id(subject_id)?;
        validate_metadata(metadata)?;

        if ttl_ms == 0 {
            return Err(ProtocolError::OutOfRange {
                field: "ttl_ms",
                value: 0,
                max: MAX_PRESENCE_TTL_MS,
            }
            .into());
        }

        let metadata_json = serde_json::to_string(metadata).map_err(ProtocolError::Malformed)?;

        // Unknown operation names are ignored rather than fatal: a newer Rails
        // may grant an operation this build has never heard of, and a rolling
        // deploy must not fail closed on the *whole* grant for it. What this
        // build cannot name, it cannot run.
        let operations =
            operations.iter().filter_map(|name| PresenceOp::from_wire(name)).collect::<Vec<_>>();

        Ok(Self {
            stream: stream.to_string(),
            subject_id: subject_id.to_string(),
            metadata: metadata.clone(),
            metadata_json,
            ttl: Duration::from_millis(ttl_ms.min(MAX_PRESENCE_TTL_MS)),
            operations,
        })
    }

    /// Whether this grant allows `op`.
    #[must_use]
    pub fn allows(&self, op: PresenceOp) -> bool {
        self.operations.contains(&op)
    }

    /// The membership key this grant leases in `namespace`.
    #[must_use]
    pub fn key(&self, namespace: &str) -> MembershipKey {
        MembershipKey::new(namespace, &self.stream, &self.subject_id)
    }

    /// The lease this grant produces for `session_id` on `node_id`.
    #[must_use]
    pub fn lease(&self, namespace: &str, session_id: &str, node_id: &str) -> Lease {
        Lease {
            key: self.key(namespace),
            session_id: session_id.to_string(),
            node_id: node_id.to_string(),
            metadata_json: self.metadata_json.clone(),
            ttl: self.ttl,
        }
    }
}

/// A grant that a replacement displaced, with whether its lease was live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacedGrant {
    /// The grant that was in force until now.
    pub grant: PresenceGrant,
    /// Whether the session had joined under it (so a lease exists to release
    /// or carry over).
    pub joined: bool,
}

/// One registered grant and whether the session has joined under it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GrantEntry {
    grant: PresenceGrant,
    joined: bool,
}

/// Every presence grant one connection currently holds.
///
/// Owned by that connection's session task, so it needs no locking: grants are
/// installed by the reducer and read by the same task's command handling.
#[derive(Debug, Default)]
pub struct PresenceGrants {
    /// `(identifier, stream) -> grant`. Ordered so behaviour is deterministic
    /// when one subscription holds grants for several streams.
    grants: BTreeMap<(String, String), GrantEntry>,
}

impl PresenceGrants {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether this connection holds any grant at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.grants.is_empty()
    }

    /// Number of grants held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.grants.len()
    }

    /// Install (or replace) the grant for `(identifier, grant.stream)`.
    ///
    /// Replacing is how Rails changes a subject's metadata, TTL, operations,
    /// or the subject itself. The displaced grant comes back with whether the
    /// session had joined under it, so the caller can release the old lease
    /// (a different subject must leave before the new one joins) or carry the
    /// join over (the same subject with new metadata). The new entry starts
    /// *not joined*: the caller re-joins it when it carries a live lease over.
    pub fn insert(&mut self, identifier: &str, grant: PresenceGrant) -> Option<ReplacedGrant> {
        debug!(
            identifier,
            stream = %grant.stream,
            subject_id = %grant.subject_id,
            operations = grant.operations.len(),
            "presence grant registered"
        );
        self.grants
            .insert(
                (identifier.to_string(), grant.stream.clone()),
                GrantEntry { grant, joined: false },
            )
            .map(|entry| ReplacedGrant { grant: entry.grant, joined: entry.joined })
    }

    /// Record that the session joined (or left) under the grant for
    /// `(identifier, stream)`.
    ///
    /// Returns `false` when there is no such grant.
    pub fn set_joined(&mut self, identifier: &str, stream: &str, joined: bool) -> bool {
        match self.grants.get_mut(&(identifier.to_string(), stream.to_string())) {
            Some(entry) => {
                entry.joined = joined;
                true
            }
            None => false,
        }
    }

    /// Whether the session has joined under the grant for `(identifier, stream)`.
    #[must_use]
    pub fn is_joined(&self, identifier: &str, stream: &str) -> bool {
        self.grants
            .get(&(identifier.to_string(), stream.to_string()))
            .is_some_and(|entry| entry.joined)
    }

    /// Remove the grant a `presence_revoke` effect names.
    ///
    /// The subject must match: a revoke for a subject this connection is not
    /// holding is a no-op rather than a way to drop somebody else's grant.
    pub fn revoke(
        &mut self,
        identifier: &str,
        stream: &str,
        subject_id: &str,
    ) -> Option<ReplacedGrant> {
        let key = (identifier.to_string(), stream.to_string());
        match self.grants.get(&key) {
            Some(entry) if entry.grant.subject_id == subject_id => self
                .grants
                .remove(&key)
                .map(|entry| ReplacedGrant { grant: entry.grant, joined: entry.joined }),
            _ => None,
        }
    }

    /// Remove and return every grant of one subscription whose stream
    /// `keep` does not accept (a subscription that stopped streaming from it).
    pub fn remove_where(
        &mut self,
        identifier: &str,
        mut drop: impl FnMut(&PresenceGrant) -> bool,
    ) -> Vec<ReplacedGrant> {
        self.grants
            .extract_if(.., |(owner, _), entry| owner == identifier && drop(&entry.grant))
            .map(|(_, entry)| ReplacedGrant { grant: entry.grant, joined: entry.joined })
            .collect()
    }

    /// Remove and return every grant of one subscription (unsubscribe).
    pub fn remove_identifier(&mut self, identifier: &str) -> Vec<ReplacedGrant> {
        self.remove_where(identifier, |_| true)
    }

    /// Remove and return every grant (socket close).
    pub fn drain(&mut self) -> Vec<ReplacedGrant> {
        std::mem::take(&mut self.grants)
            .into_values()
            .map(|entry| ReplacedGrant { grant: entry.grant, joined: entry.joined })
            .collect()
    }

    /// Every grant of one subscription, without removing them.
    #[must_use]
    pub fn for_identifier(&self, identifier: &str) -> Vec<PresenceGrant> {
        self.grants
            .iter()
            .filter(|((owner, _), _)| owner == identifier)
            .map(|(_, entry)| entry.grant.clone())
            .collect()
    }

    /// Every `(identifier, grant)` pair held, in registry order.
    #[must_use]
    pub fn all(&self) -> Vec<(String, PresenceGrant)> {
        self.grants.iter().map(|((owner, _), entry)| (owner.clone(), entry.grant.clone())).collect()
    }

    /// Resolve a client operation to the grants that authorize it.
    ///
    /// Two distinct refusals, both fail-closed:
    ///
    /// - [`PresenceError::NotGranted`] - this subscription has no grant, so
    ///   the client has no presence identity at all.
    /// - [`PresenceError::OperationNotAllowed`] - it has one, but the
    ///   operation is outside the allowlist (a read-only grant asked to join,
    ///   or a join-only grant asked for a snapshot).
    ///
    /// # Errors
    ///
    /// Exactly those two refusals; on success the result is never empty.
    pub fn authorize(
        &self,
        identifier: &str,
        op: PresenceOp,
    ) -> Result<Vec<PresenceGrant>, PresenceError> {
        let held = self.for_identifier(identifier);
        if held.is_empty() {
            return Err(PresenceError::NotGranted);
        }

        let allowed: Vec<PresenceGrant> =
            held.into_iter().filter(|grant| grant.allows(op)).collect();
        if allowed.is_empty() {
            return Err(PresenceError::OperationNotAllowed { op });
        }

        Ok(allowed)
    }
}

/// What one connection holds on this node: its session id, the leases it
/// has joined, and when its socket was last verified alive.
#[derive(Debug)]
struct ConnectionLeases {
    session_id: String,
    /// `membership -> the lease's own TTL`.
    leases: HashMap<MembershipKey, Duration>,
    seen: std::time::Instant,
}

/// Which connections hold leases on this node, which leases, and when each
/// socket was last *verified* to be alive.
///
/// This is the liveness source leases depend on: a lease is refreshed
/// only for a connection whose WebSocket produced inbound traffic - a control
/// pong answering the server's ping, or a real client frame - inside the
/// window. An ActionCable JSON `ping` is not a liveness proof, because the
/// server sends it; nothing here is updated by an outbound frame.
///
/// A connection that goes silent (crashed browser, black-holed network,
/// suspended laptop) simply stops appearing in
/// [`PresenceSessions::live_leases`], so its leases expire on schedule while
/// other tabs of the same subject keep theirs. A lease the client explicitly
/// left is released here at once, so it is neither refreshed nor counted.
#[derive(Debug, Default)]
pub struct PresenceSessions {
    /// `conn_id -> what it holds`.
    connections: dashmap::DashMap<u32, ConnectionLeases>,
}

impl PresenceSessions {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record verified inbound activity on a connection.
    ///
    /// A connection holding no lease is not tracked: liveness only matters
    /// for a lease, and tracking every socket would grow with connections
    /// rather than with presence.
    pub fn record_liveness(&self, conn_id: u32) {
        if let Some(mut entry) = self.connections.get_mut(&conn_id) {
            entry.seen = std::time::Instant::now();
        }
    }

    /// Note that `conn_id` now holds `lease` under `session_id`.
    ///
    /// Holding also counts as liveness: the client just sent the frame that
    /// asked to join.
    pub fn hold(&self, conn_id: u32, lease: &Lease) {
        let mut entry = self.connections.entry(conn_id).or_insert_with(|| ConnectionLeases {
            session_id: lease.session_id.clone(),
            leases: HashMap::new(),
            seen: std::time::Instant::now(),
        });
        entry.session_id.clone_from(&lease.session_id);
        entry.leases.insert(lease.key.clone(), lease.ttl);
        entry.seen = std::time::Instant::now();
    }

    /// Note that `conn_id` released its lease on `key` (an explicit leave, a
    /// revoke, a dropped grant). Returns whether it was held.
    #[must_use]
    pub fn release(&self, conn_id: u32, key: &MembershipKey) -> bool {
        let Some(mut entry) = self.connections.get_mut(&conn_id) else {
            return false;
        };
        let held = entry.leases.remove(key).is_some();
        if entry.leases.is_empty() {
            drop(entry);
            self.connections.remove(&conn_id);
        }
        held
    }

    /// Forget a connection entirely (teardown).
    pub fn forget(&self, conn_id: u32) {
        self.connections.remove(&conn_id);
    }

    /// The leases held by connections whose sockets were verified alive
    /// within `window`, each with its own TTL.
    #[must_use]
    pub fn live_leases(&self, window: Duration) -> Vec<HeldLease> {
        self.connections
            .iter()
            .filter(|entry| entry.seen.elapsed() <= window)
            .flat_map(|entry| {
                entry
                    .leases
                    .iter()
                    .map(|(key, ttl)| HeldLease {
                        key: key.clone(),
                        session_id: entry.session_id.clone(),
                        ttl: *ttl,
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Leases currently held on this node, across every connection.
    #[must_use]
    pub fn lease_count(&self) -> usize {
        self.connections.iter().map(|entry| entry.leases.len()).sum()
    }

    /// Connections currently holding at least one lease on this node.
    #[must_use]
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Whether any connection holds a lease on this node.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}

/// Backend-agnostic presence counters for `/metrics`.
///
/// One shape for every store, so the exporter compiles in every feature
/// combination; a store that keeps no counters reports zeros through the
/// default [`PresenceBackend::metrics`] implementation. The field names are
/// the exported series (`orbitcast_presence_<field>_total`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PresenceMetrics {
    /// Join operations applied.
    pub joins: u64,
    /// Leave operations applied.
    pub leaves: u64,
    /// Subject-level transitions published.
    pub announced: u64,
    /// Leases extended by a batch refresh.
    pub refreshed: u64,
    /// Leases removed by the expiry sweeper.
    pub swept: u64,
    /// Roster snapshot pages served.
    pub snapshots: u64,
    /// Transitions that could not be published (the roster still changed).
    pub publish_failures: u64,
}

/// Publishes presence transitions to the rest of the deployment.
///
/// A trait rather than the concrete backend so a store can be tested without
/// a pub/sub, and so no store depends on which backend is compiled in. Every
/// pub/sub backend is a publisher.
#[async_trait]
pub trait PresencePublisher: Send + Sync {
    /// Publish one presence message on a derived presence stream key.
    async fn publish_presence(
        &self,
        stream_key: &str,
        json_payload: &str,
    ) -> Result<(), PresenceError>;
}

#[async_trait]
impl<T: PubSub + ?Sized> PresencePublisher for T {
    async fn publish_presence(
        &self,
        stream_key: &str,
        json_payload: &str,
    ) -> Result<(), PresenceError> {
        self.publish(stream_key, json_payload)
            .await
            .map_err(|e| PresenceError::Backend(anyhow::anyhow!("{e}")))
    }
}

/// The presence store contract.
///
/// Implemented by [`postgres::PostgresPresence`] (the deployment-wide store)
/// and [`memory::MemoryPresence`] (one process). Every implementation owes the
/// same guarantees: per-session leases aggregated to subjects, first-join /
/// last-leave transitions announced exactly once and serialized per
/// membership, expiry that survives the holder's death, and paged snapshots.
#[async_trait]
pub trait PresenceBackend: Send + Sync {
    /// Operational counters for `/metrics`.
    ///
    /// Defaulted so a test double stays a two-line implementation; real
    /// stores override it.
    fn metrics(&self) -> PresenceMetrics {
        PresenceMetrics::default()
    }

    /// Claim or refresh one lease, reporting the subject-level transition.
    async fn join(&self, lease: &Lease) -> Result<TransitionOutcome, PresenceError>;

    /// Drop one session's lease, reporting the subject-level transition.
    async fn leave(
        &self,
        key: &MembershipKey,
        session_id: &str,
    ) -> Result<TransitionOutcome, PresenceError>;

    /// Read one page of a stream's roster.
    async fn snapshot(
        &self,
        stream: &str,
        page: PresencePage,
    ) -> Result<PresenceSnapshot, PresenceError>;

    /// Extend exactly these leases, each by its own TTL, returning how many
    /// were refreshed.
    ///
    /// Only leases this node holds are named, so a dead session's lease is
    /// never extended on the strength of another tab, and a lease the grant
    /// gave a short TTL keeps it.
    async fn refresh(&self, leases: &[HeldLease]) -> Result<u64, PresenceError>;

    /// Drop whatever this node caches about `stream`'s roster.
    ///
    /// Called when a change notification for the stream arrives, so the next
    /// snapshot is read from the store rather than from a stale page.
    /// Synchronous: it runs on the pub/sub delivery path, which must not await.
    fn invalidate(&self, stream: &str);

    /// Drop every cached roster page (pub/sub reconnect: notifications were
    /// lost, nothing cached can be trusted).
    fn invalidate_all(&self);
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests;
