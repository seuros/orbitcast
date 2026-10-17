//! PostgreSQL-backed distributed presence.
//!
//! **Dispatch mode only.** Every lease here exists because Rails returned a
//! `presence_grant` effect for a specific `(connection, subscription, stream)`;
//! a signed-only node has no grants and therefore never writes a row. See
//! [`crate::presence`] for the contract this store implements.
//!
//! # Table contract
//!
//! Shared with `gem/db/orbitcast_migrate/002_create_orbitcast_presence.rb`;
//! the column names and types are wire contract, not an implementation detail:
//!
//! ```text
//! orbitcast_presence(namespace text, stream text, subject_id text,
//!                    session_id text, node_id text, metadata text,
//!                    expires_at timestamptz,
//!                    primary key (namespace, stream, subject_id, session_id))
//! ```
//!
//! Production code never issues DDL: a missing table is reported with the
//! migration to run.
//!
//! # Per-session leases, subject-level transitions
//!
//! The primary key is per *session*, so a subject with three tabs holds three
//! rows and one tab's refresh cannot preserve another tab's lease. What clients
//! see is aggregated to the subject: the first live lease announces a join, the
//! last one to go announces a leave, and everything between is silent.
//!
//! That decision is serialized per membership. Each join/leave transaction
//! first takes `pg_advisory_xact_lock` on
//! [`MembershipKey::lock_key`](crate::presence::MembershipKey::lock_key) - the
//! hash of `(namespace, stream, subject_id)` - so two nodes racing on the same
//! subject cannot both observe "zero live leases" and both announce a join. The
//! lock is transaction-scoped: it is released by the commit, and a crashed
//! backend releases it with its connection.
//!
//! # Expiry is the only thing that survives a node's death
//!
//! A lease lives for the TTL *its grant* carried (capped by the node's
//! configured bound), measured from the **database** clock, and is refreshed
//! in batches - each lease by its own TTL - only for sessions with verified
//! WebSocket liveness. A node that is killed refreshes nothing, so its leases
//! expire and the sweeper removes them: expiry bounds staleness, and nothing
//! else does.
//!
//! The sweeper reclaims one membership at a time under the same advisory lock
//! a join or leave takes, so an expiry and a concurrent join of the same
//! subject are decided in one order: the sweep either sees the fresh lease and
//! announces nothing, or removes the stale rows before the join counts them.
//! Announcements are still published after commit, so two announcements for
//! one subject can in principle cross on the wire; the roster in PostgreSQL is
//! never wrong, and a snapshot reconciles any listener that saw them crossed.
//!
//! # Notifications, caches, and eventual consistency
//!
//! Transitions are published on the derived presence stream key
//! ([`presence_stream_key`]) through the ordinary pub/sub backend, and the
//! local snapshot cache for that stream is dropped when one arrives. A lost
//! notification is *not* replayed: the next snapshot reconciles against
//! PostgreSQL, which is also what a reconnect does. Presence is eventually
//! consistent by construction and is not an authorization database.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::{Map, Value};
use tracing::{debug, warn};

use crate::pg::{AbortOnDrop, PgSlot, describe_pg_error};
use crate::presence::{
    HeldLease, Lease, MembershipKey, PRESENCE_TABLE, PresenceBackend, PresenceError,
    PresencePublisher, PresenceSnapshot, Transition, TransitionOutcome, presence_stream_key,
};
use crate::protocol::v1::presence::{MAX_PAGE_SIZE, PresenceMember, PresenceMessage, PresencePage};

/// Take the per-membership lock for the rest of the transaction.
const LOCK_SQL: &str = "SELECT pg_advisory_xact_lock($1::bigint)";

/// Live leases one subject holds on one stream.
const COUNT_LEASES_SQL: &str = "SELECT count(*) FROM orbitcast_presence \
     WHERE namespace = $1 AND stream = $2 AND subject_id = $3 AND expires_at > now()";

/// Distinct live subjects on one stream.
const COUNT_SUBJECTS_SQL: &str = "SELECT count(DISTINCT subject_id) FROM orbitcast_presence \
     WHERE namespace = $1 AND stream = $2 AND expires_at > now()";

/// Claim or refresh one session's lease.
///
/// `expires_at` uses the **database** clock, so a node with a skewed clock
/// cannot hold a lease open (or drop it early).
const UPSERT_SQL: &str = "INSERT INTO orbitcast_presence \
     (namespace, stream, subject_id, session_id, node_id, metadata, expires_at) \
     VALUES ($1, $2, $3, $4, $5, $6, now() + ($7::bigint * interval '1 millisecond')) \
     ON CONFLICT (namespace, stream, subject_id, session_id) DO UPDATE SET \
     node_id = EXCLUDED.node_id, metadata = EXCLUDED.metadata, \
     expires_at = EXCLUDED.expires_at";

/// Drop one session's lease.
const DELETE_LEASE_SQL: &str = "DELETE FROM orbitcast_presence \
     WHERE namespace = $1 AND stream = $2 AND subject_id = $3 AND session_id = $4";

/// One roster page, one row per subject, newest lease's metadata.
///
/// Ordered by `subject_id` so paging is stable across requests rather than
/// dependent on physical row order.
const SNAPSHOT_SQL: &str = "SELECT subject_id, \
     (array_agg(metadata ORDER BY expires_at DESC))[1] AS metadata \
     FROM orbitcast_presence \
     WHERE namespace = $1 AND stream = $2 AND expires_at > now() \
     GROUP BY subject_id ORDER BY subject_id LIMIT $3::bigint OFFSET $4::bigint";

/// Batch lease refresh: exactly the leases this node holds, each extended by
/// its own TTL.
///
/// Matching on `(session_id, stream, subject_id)` rather than on the session
/// alone is what keeps a replaced subject's old row from being kept alive,
/// and carrying `ttl_ms` per row is what keeps a grant's TTL in force for the
/// lease's whole life.
const REFRESH_SQL: &str = "UPDATE orbitcast_presence AS p \
     SET expires_at = now() + (l.ttl_ms * interval '1 millisecond') \
     FROM unnest($3::text[], $4::text[], $5::text[], $6::bigint[]) \
          AS l(session_id, stream, subject_id, ttl_ms) \
     WHERE p.namespace = $1 AND p.node_id = $2 \
       AND p.session_id = l.session_id AND p.stream = l.stream \
       AND p.subject_id = l.subject_id";

/// Memberships holding at least one expired lease, scoped to this
/// deployment's namespace and bounded per pass.
///
/// Only a candidate list: the removal itself happens per membership, under
/// that membership's advisory lock ([`EXPIRE_SQL`]), so it is ordered with
/// any concurrent join or leave of the same subject.
const SWEEP_CANDIDATES_SQL: &str = "SELECT DISTINCT stream, subject_id \
     FROM orbitcast_presence WHERE namespace = $1 AND expires_at < now() \
     ORDER BY stream, subject_id LIMIT $2::bigint";

/// Remove one membership's expired leases.
const EXPIRE_SQL: &str = "DELETE FROM orbitcast_presence \
     WHERE namespace = $1 AND stream = $2 AND subject_id = $3 AND expires_at < now()";

/// Memberships examined per sweep statement.
const SWEEP_BATCH: i64 = 500;

/// Sweep statements per tick; caps one node's delete load per pass.
const SWEEP_BATCHES_PER_TICK: usize = 10;

/// Floor for the sweep period, regardless of a short TTL.
const SWEEP_MIN_INTERVAL: Duration = Duration::from_secs(5);

/// How long one snapshot page may be served from the local cache.
///
/// Short on purpose: the cache absorbs a burst of snapshot requests for the
/// same roster, it does not hold a roster open. A change notification drops it
/// earlier.
const CACHE_TTL: Duration = Duration::from_secs(2);

/// Bound on cached snapshot pages, across every stream.
///
/// Reached only by pathological paging; the cache is cleared rather than
/// evicted one entry at a time, because a presence cache miss costs one query.
const CACHE_CAPACITY: usize = 1024;

/// What to tell an operator whose presence table does not exist.
fn missing_table_hint() -> String {
    format!(
        "the {PRESENCE_TABLE} table does not exist; install the presence migration \
         (bin/rails orbitcast:install:migrations && bin/rails db:migrate, from \
         gem/db/orbitcast_migrate/002_create_orbitcast_presence.rb)"
    )
}

/// Static configuration of the presence store.
#[derive(Debug, Clone)]
pub struct PresenceConfig {
    /// Connection string for the presence session.
    pub database_url: String,
    /// Deployment namespace. Every row, query, and sweep is scoped to it.
    pub namespace: String,
    /// Stable identifier of this node, recorded on every lease it holds.
    pub node_id: String,
    /// Lease lifetime, from the database clock.
    pub ttl: Duration,
    /// Batch refresh period. Must be shorter than the TTL.
    pub refresh_interval: Duration,
}

impl PresenceConfig {
    /// Reject unusable configuration at the boundary.
    ///
    /// # Errors
    ///
    /// [`PresenceError::InvalidGrant`] when `namespace` is not a valid
    /// namespace, and [`PresenceError::Backend`] when `database_url` is blank,
    /// `node_id` is empty, `ttl` is zero, or `refresh_interval` is zero or not
    /// shorter than `ttl`.
    pub fn validate(&self) -> Result<(), PresenceError> {
        if self.database_url.trim().is_empty() {
            return Err(PresenceError::Backend(anyhow::anyhow!(
                "database_url must not be empty for presence"
            )));
        }
        crate::protocol::v1::validate_namespace(&self.namespace)?;
        if self.node_id.is_empty() {
            return Err(PresenceError::Backend(anyhow::anyhow!("node_id must not be empty")));
        }
        if self.ttl.is_zero() {
            return Err(PresenceError::Backend(anyhow::anyhow!(
                "presence_ttl_ms must be greater than 0"
            )));
        }
        if self.refresh_interval.is_zero() || self.refresh_interval >= self.ttl {
            return Err(PresenceError::Backend(anyhow::anyhow!(
                "presence_refresh_interval_ms must be greater than 0 and shorter than \
                 presence_ttl_ms"
            )));
        }
        Ok(())
    }
}

/// A TTL as the millisecond bigint the SQL binds.
fn ttl_millis(ttl: Duration) -> i64 {
    i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX)
}

/// Operational counters.
#[derive(Debug, Default)]
pub struct PresenceStats {
    joins: AtomicU64,
    leaves: AtomicU64,
    announced: AtomicU64,
    refreshed: AtomicU64,
    swept: AtomicU64,
    snapshots: AtomicU64,
    cache_hits: AtomicU64,
    publish_failures: AtomicU64,
}

/// Point-in-time copy of [`PresenceStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PresenceStatsSnapshot {
    /// Join operations applied.
    pub joins: u64,
    /// Leave operations applied.
    pub leaves: u64,
    /// Subject-level transitions published.
    pub announced: u64,
    /// Leases extended by a batch refresh.
    pub refreshed: u64,
    /// Leases removed by the expiry sweeper (`orbitcast_presence_sweeps_total`).
    pub swept: u64,
    /// Snapshot pages served.
    pub snapshots: u64,
    /// Snapshot pages served from the local cache.
    pub cache_hits: u64,
    /// Transitions that could not be published (the roster still changed).
    pub publish_failures: u64,
}

impl PresenceStats {
    fn snapshot(&self) -> PresenceStatsSnapshot {
        PresenceStatsSnapshot {
            joins: self.joins.load(Ordering::Relaxed),
            leaves: self.leaves.load(Ordering::Relaxed),
            announced: self.announced.load(Ordering::Relaxed),
            refreshed: self.refreshed.load(Ordering::Relaxed),
            swept: self.swept.load(Ordering::Relaxed),
            snapshots: self.snapshots.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            publish_failures: self.publish_failures.load(Ordering::Relaxed),
        }
    }
}

/// One cached roster page.
struct CachedPage {
    snapshot: PresenceSnapshot,
    stored_at: Instant,
}

/// PostgreSQL-backed presence store.
pub struct PostgresPresence {
    config: PresenceConfig,
    session: PgPresenceSession,
    publisher: Arc<dyn PresencePublisher>,
    /// `(stream, limit, offset) -> page`, dropped on change notification.
    cache: DashMap<(String, u32, u64), CachedPage>,
    stats: Arc<PresenceStats>,
    _sweeper: AbortOnDrop,
}

impl PostgresPresence {
    /// Connect the presence session and start the expiry sweeper.
    ///
    /// # Errors
    ///
    /// Whatever [`PresenceConfig::validate`] rejects (checked before any
    /// connection is opened), and [`PresenceError::Backend`] when the store
    /// or sweeper session cannot connect.
    pub async fn connect(
        config: PresenceConfig,
        publisher: Arc<dyn PresencePublisher>,
    ) -> Result<Arc<Self>, PresenceError> {
        config.validate()?;

        let session = PgPresenceSession::connect(config.database_url.clone()).await?;
        let stats = Arc::new(PresenceStats::default());

        let sweeper_session = PgPresenceSession::connect(config.database_url.clone()).await?;
        let sweeper = tokio::spawn(sweep_expired(
            sweeper_session,
            config.clone(),
            Arc::clone(&publisher),
            Arc::clone(&stats),
        ));

        Ok(Arc::new(Self {
            config,
            session,
            publisher,
            cache: DashMap::new(),
            stats,
            _sweeper: AbortOnDrop(sweeper),
        }))
    }

    /// Counters for `/metrics` and tests.
    pub fn stats(&self) -> PresenceStatsSnapshot {
        self.stats.snapshot()
    }

    /// This node's id, as recorded on every lease it holds.
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// The namespace every row is scoped to.
    pub fn namespace(&self) -> &str {
        &self.config.namespace
    }

    /// The configured lease TTL.
    pub const fn ttl(&self) -> Duration {
        self.config.ttl
    }

    /// Publish one transition on the stream's derived presence key.
    ///
    /// A publish failure is counted and warned, never retried: the roster in
    /// PostgreSQL already changed, and the next snapshot is what repairs a
    /// listener that missed the notice.
    async fn announce(&self, message: &PresenceMessage) {
        announce_with(&*self.publisher, &self.stats, message).await;
    }

    /// Serve a cached page, if one is fresh.
    fn cached(&self, stream: &str, page: PresencePage) -> Option<PresenceSnapshot> {
        let key = (stream.to_string(), page.limit, page.offset);
        let entry = self.cache.get(&key)?;
        if entry.stored_at.elapsed() > CACHE_TTL {
            return None;
        }
        self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
        Some(entry.snapshot.clone())
    }

    /// Store one page, clearing the cache wholesale if it grew past its bound.
    fn store_cached(&self, stream: &str, page: PresencePage, snapshot: &PresenceSnapshot) {
        if self.cache.len() >= CACHE_CAPACITY {
            self.cache.clear();
        }
        self.cache.insert(
            (stream.to_string(), page.limit, page.offset),
            CachedPage { snapshot: snapshot.clone(), stored_at: Instant::now() },
        );
    }
}

/// Publish one transition through `publisher`, counting the outcome.
async fn announce_with(
    publisher: &dyn PresencePublisher,
    stats: &PresenceStats,
    message: &PresenceMessage,
) {
    let json = match message.to_json() {
        Ok(json) => json,
        Err(e) => {
            stats.publish_failures.fetch_add(1, Ordering::Relaxed);
            warn!(error = %e, "Could not encode a presence transition");
            return;
        }
    };

    let key = presence_stream_key(&message.stream);
    match publisher.publish_presence(&key, &json).await {
        Ok(()) => {
            stats.announced.fetch_add(1, Ordering::Relaxed);
            debug!(
                stream = %message.stream,
                event = %message.event,
                total = message.total,
                "presence transition announced"
            );
        }
        Err(e) => {
            stats.publish_failures.fetch_add(1, Ordering::Relaxed);
            warn!(
                stream = %message.stream,
                event = %message.event,
                error = %e,
                "Presence transition could not be announced; snapshots will reconcile"
            );
        }
    }
}

#[async_trait]
impl PresenceBackend for PostgresPresence {
    /// The store's own counters, mapped onto the backend-agnostic shape the
    /// exporter reads. `cache_hits` is deliberately not exported: it is a
    /// tuning detail of this store, not an operational signal.
    fn metrics(&self) -> crate::presence::PresenceMetrics {
        let stats = self.stats.snapshot();
        crate::presence::PresenceMetrics {
            joins: stats.joins,
            leaves: stats.leaves,
            announced: stats.announced,
            refreshed: stats.refreshed,
            swept: stats.swept,
            snapshots: stats.snapshots,
            publish_failures: stats.publish_failures,
        }
    }

    async fn join(&self, lease: &Lease) -> Result<TransitionOutcome, PresenceError> {
        lease.key.validate()?;
        self.stats.joins.fetch_add(1, Ordering::Relaxed);

        let outcome = match self.session.run(SessionOp::Join(lease)).await? {
            OpOutcome::Transition(outcome) => outcome,
            other => return Err(unexpected(&other)),
        };

        self.invalidate(&lease.key.stream);

        if outcome.transition == Transition::FirstJoin {
            let metadata = decode_metadata(&lease.metadata_json);
            let message = PresenceMessage::join(
                &lease.key.stream,
                &lease.key.subject_id,
                metadata,
                outcome.total,
            );
            self.announce(&message).await;
        }

        Ok(outcome)
    }

    async fn leave(
        &self,
        key: &MembershipKey,
        session_id: &str,
    ) -> Result<TransitionOutcome, PresenceError> {
        key.validate()?;
        self.stats.leaves.fetch_add(1, Ordering::Relaxed);

        let outcome = match self.session.run(SessionOp::Leave { key, session_id }).await? {
            OpOutcome::Transition(outcome) => outcome,
            other => return Err(unexpected(&other)),
        };

        self.invalidate(&key.stream);

        if outcome.transition == Transition::LastLeave {
            let message = PresenceMessage::leave(&key.stream, &key.subject_id, outcome.total);
            self.announce(&message).await;
        }

        Ok(outcome)
    }

    async fn snapshot(
        &self,
        stream: &str,
        page: PresencePage,
    ) -> Result<PresenceSnapshot, PresenceError> {
        let page = PresencePage { limit: page.limit.clamp(1, MAX_PAGE_SIZE), offset: page.offset };
        self.stats.snapshots.fetch_add(1, Ordering::Relaxed);

        if let Some(cached) = self.cached(stream, page) {
            return Ok(cached);
        }

        let snapshot = match self
            .session
            .run(SessionOp::Snapshot { namespace: &self.config.namespace, stream, page })
            .await?
        {
            OpOutcome::Snapshot(snapshot) => snapshot,
            other => return Err(unexpected(&other)),
        };

        self.store_cached(stream, page, &snapshot);
        Ok(snapshot)
    }

    async fn refresh(&self, leases: &[HeldLease]) -> Result<u64, PresenceError> {
        if leases.is_empty() {
            return Ok(0);
        }

        let rows = match self
            .session
            .run(SessionOp::Refresh {
                leases,
                namespace: &self.config.namespace,
                node_id: &self.config.node_id,
            })
            .await?
        {
            OpOutcome::Rows(rows) => rows,
            other => return Err(unexpected(&other)),
        };

        self.stats.refreshed.fetch_add(rows, Ordering::Relaxed);
        Ok(rows)
    }

    fn invalidate(&self, stream: &str) {
        self.cache.retain(|(cached, _, _), _| cached != stream);
    }

    fn invalidate_all(&self) {
        self.cache.clear();
    }
}

impl std::fmt::Debug for PostgresPresence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresPresence")
            .field("namespace", &self.config.namespace)
            .field("node_id", &self.config.node_id)
            .field("ttl", &self.config.ttl)
            .field("cached_pages", &self.cache.len())
            .finish_non_exhaustive()
    }
}

/// Turn an impossible outcome into an error rather than a panic.
fn unexpected(outcome: &OpOutcome) -> PresenceError {
    PresenceError::Backend(anyhow::anyhow!(
        "presence operation produced {} instead of its own outcome",
        outcome.label()
    ))
}

/// Decode stored metadata text, tolerating a row this build cannot read.
///
/// A row is data, not a trusted envelope: unreadable metadata yields an empty
/// map rather than failing a whole roster.
fn decode_metadata(json: &str) -> Map<String, Value> {
    serde_json::from_str::<Map<String, Value>>(json).unwrap_or_default()
}

/// One unit of work for the presence session.
///
/// A concrete enum rather than a closure, for the same reason the pub/sub
/// publisher uses one: a higher-ranked async closure would not carry a `Send`
/// bound across the boxed futures of an async trait.
enum SessionOp<'a> {
    Join(&'a Lease),
    Leave {
        key: &'a MembershipKey,
        session_id: &'a str,
    },
    Snapshot {
        namespace: &'a str,
        stream: &'a str,
        page: PresencePage,
    },
    Refresh {
        leases: &'a [HeldLease],
        namespace: &'a str,
        node_id: &'a str,
    },
    /// Memberships with at least one expired lease.
    SweepCandidates {
        namespace: &'a str,
        limit: i64,
    },
    /// Remove one membership's expired leases under its lock.
    Expire {
        key: &'a MembershipKey,
    },
}

impl SessionOp<'_> {
    /// Short operation name for error messages and logs; never a payload.
    const fn label(&self) -> &'static str {
        match self {
            SessionOp::Join(_) => "presence join",
            SessionOp::Leave { .. } => "presence leave",
            SessionOp::Snapshot { .. } => "presence snapshot",
            SessionOp::Refresh { .. } => "presence refresh",
            SessionOp::SweepCandidates { .. } => "presence sweep",
            SessionOp::Expire { .. } => "presence expire",
        }
    }
}

/// What one [`SessionOp`] produced.
#[derive(Debug)]
enum OpOutcome {
    Transition(TransitionOutcome),
    Snapshot(PresenceSnapshot),
    Rows(u64),
    /// Memberships holding an expired lease: `(stream, subject_id)`.
    Candidates(Vec<(String, String)>),
    /// One membership's expiry: the subject-level transition and how many
    /// lease rows were removed.
    Expired {
        outcome: TransitionOutcome,
        removed: u64,
    },
}

impl OpOutcome {
    const fn label(&self) -> &'static str {
        match self {
            Self::Transition(_) => "a transition",
            Self::Snapshot(_) => "a snapshot",
            Self::Rows(_) => "a row count",
            Self::Candidates(_) => "a candidate list",
            Self::Expired { .. } => "an expiry",
        }
    }
}

/// A persistent, single-connection PostgreSQL session for presence.
///
/// One connection, serialized by a mutex, reconnected on failure - the same
/// shape the pub/sub publisher uses. Presence is not on the fanout hot path:
/// one session per role keeps transaction ordering obvious and connection
/// count low.
struct PgPresenceSession {
    slot: PgSlot,
}

impl PgPresenceSession {
    async fn connect(database_url: String) -> Result<Self, PresenceError> {
        Ok(Self { slot: PgSlot::connect(database_url, "presence").await? })
    }

    /// Run one operation against a live client, reconnecting once if the
    /// session died between statements.
    ///
    /// The mutex is held for the whole call, so a transaction cannot interleave
    /// with another caller's statement on the same session. The retry re-runs
    /// the operation from the start: each one is a single statement or a
    /// self-contained transaction, so re-running is safe. A reconnect also
    /// means any advisory lock the dead backend held is gone - correctness
    /// still rests on the fresh transaction's counts, not on the lock.
    async fn run(&self, op: SessionOp<'_>) -> Result<OpOutcome, PresenceError> {
        let what = op.label();
        let mut open = self.slot.acquire().await?;

        let first_error = match execute_op(&mut open.client, &op).await {
            Ok(value) => return Ok(value),
            Err(e) => e,
        };

        // A server-reported failure (missing table, constraint violation) must
        // not be retried as if it were a dead socket: report it as-is.
        if first_error.code().is_some() {
            return Err(PresenceError::Backend(anyhow::anyhow!(
                "{what} failed: {}",
                describe_pg_error(&first_error, missing_table_hint)
            )));
        }

        debug!(
            error = %first_error,
            operation = what,
            "presence session failed; reconnecting once"
        );
        self.slot.reopen(&mut open).await?;
        execute_op(&mut open.client, &op).await.map_err(|retry_error| {
            PresenceError::Backend(anyhow::anyhow!(
                "{what} failed after reconnect ({}); first attempt: {first_error}",
                describe_pg_error(&retry_error, missing_table_hint)
            ))
        })
    }
}

async fn execute_op(
    client: &mut tokio_postgres::Client,
    op: &SessionOp<'_>,
) -> Result<OpOutcome, tokio_postgres::Error> {
    match *op {
        SessionOp::Join(lease) => join_tx(client, lease).await,

        SessionOp::Leave { key, session_id } => leave_tx(client, key, session_id).await,

        SessionOp::Snapshot { namespace, stream, page } => {
            let limit = i64::from(page.limit);
            let offset = i64::try_from(page.offset).unwrap_or(i64::MAX);

            // Both statements in one transaction: a page and a total taken from
            // different snapshots would disagree with each other for no reason.
            let transaction = client.transaction().await?;
            let rows =
                transaction.query(SNAPSHOT_SQL, &[&namespace, &stream, &limit, &offset]).await?;
            let total: i64 =
                transaction.query_one(COUNT_SUBJECTS_SQL, &[&namespace, &stream]).await?.get(0);
            transaction.commit().await?;

            let members: Vec<PresenceMember> = rows
                .iter()
                .map(|row| PresenceMember {
                    subject_id: row.get::<_, String>(0),
                    metadata: decode_metadata(&row.get::<_, Option<String>>(1).unwrap_or_default()),
                })
                .collect();

            let total = total.max(0).cast_unsigned();
            let has_more = page.offset + u64::try_from(members.len()).unwrap_or(u64::MAX) < total;
            Ok(OpOutcome::Snapshot(PresenceSnapshot { members, total, has_more }))
        }

        SessionOp::Refresh { leases, namespace, node_id } => {
            // Leases of another namespace are not this store's to extend;
            // the registry is per node, so there are none in practice.
            let ours = leases.iter().filter(|held| held.key.namespace == namespace);
            let sessions: Vec<&str> = ours.clone().map(|held| held.session_id.as_str()).collect();
            let streams: Vec<&str> = ours.clone().map(|held| held.key.stream.as_str()).collect();
            let subjects: Vec<&str> =
                ours.clone().map(|held| held.key.subject_id.as_str()).collect();
            let ttls: Vec<i64> = ours.map(|held| ttl_millis(held.ttl)).collect();
            if sessions.is_empty() {
                return Ok(OpOutcome::Rows(0));
            }
            let rows = client
                .execute(
                    REFRESH_SQL,
                    &[&namespace, &node_id, &sessions, &streams, &subjects, &ttls],
                )
                .await?;
            Ok(OpOutcome::Rows(rows))
        }

        SessionOp::SweepCandidates { namespace, limit } => {
            let rows = client.query(SWEEP_CANDIDATES_SQL, &[&namespace, &limit]).await?;
            Ok(OpOutcome::Candidates(
                rows.iter().map(|row| (row.get::<_, String>(0), row.get::<_, String>(1))).collect(),
            ))
        }

        SessionOp::Expire { key } => expire_tx(client, key).await,
    }
}

/// Live leases of `key`'s subject, read inside `transaction`.
async fn count_leases(
    transaction: &tokio_postgres::Transaction<'_>,
    key: &MembershipKey,
) -> Result<i64, tokio_postgres::Error> {
    Ok(transaction
        .query_one(COUNT_LEASES_SQL, &[&key.namespace, &key.stream, &key.subject_id])
        .await?
        .get(0))
}

/// Distinct live subjects on `key`'s stream, read inside `transaction`.
async fn count_subjects(
    transaction: &tokio_postgres::Transaction<'_>,
    key: &MembershipKey,
) -> Result<u64, tokio_postgres::Error> {
    let total: i64 =
        transaction.query_one(COUNT_SUBJECTS_SQL, &[&key.namespace, &key.stream]).await?.get(0);
    Ok(total.max(0).cast_unsigned())
}

/// The subject-level transition of a removal: `deleted` rows went, `remaining`
/// live leases are left.
const fn departure(deleted: u64, remaining: i64) -> Transition {
    match (deleted, remaining) {
        (0, _) => Transition::Unchanged,
        (_, 0) => Transition::LastLeave,
        _ => Transition::Reduced,
    }
}

/// Claim or refresh one lease under the membership lock, as one transaction.
async fn join_tx(
    client: &mut tokio_postgres::Client,
    lease: &Lease,
) -> Result<OpOutcome, tokio_postgres::Error> {
    let key = &lease.key;
    let ttl_ms = ttl_millis(lease.ttl);

    // Dropping an uncommitted transaction rolls it back, so a failure
    // between the count and the upsert leaves no lease and announces
    // nothing.
    let transaction = client.transaction().await?;
    transaction.execute(LOCK_SQL, &[&key.lock_key()]).await?;

    let before = count_leases(&transaction, key).await?;

    transaction
        .execute(
            UPSERT_SQL,
            &[
                &key.namespace,
                &key.stream,
                &key.subject_id,
                &lease.session_id,
                &lease.node_id,
                &lease.metadata_json,
                &ttl_ms,
            ],
        )
        .await?;

    let total = count_subjects(&transaction, key).await?;
    transaction.commit().await?;

    let transition = if before == 0 { Transition::FirstJoin } else { Transition::Refreshed };
    Ok(OpOutcome::Transition(TransitionOutcome { transition, total }))
}

/// Drop one session's lease under the membership lock, as one transaction.
async fn leave_tx(
    client: &mut tokio_postgres::Client,
    key: &MembershipKey,
    session_id: &str,
) -> Result<OpOutcome, tokio_postgres::Error> {
    let transaction = client.transaction().await?;
    transaction.execute(LOCK_SQL, &[&key.lock_key()]).await?;

    let deleted = transaction
        .execute(DELETE_LEASE_SQL, &[&key.namespace, &key.stream, &key.subject_id, &session_id])
        .await?;

    let remaining = count_leases(&transaction, key).await?;
    let total = count_subjects(&transaction, key).await?;
    transaction.commit().await?;

    let transition = departure(deleted, remaining);
    Ok(OpOutcome::Transition(TransitionOutcome { transition, total }))
}

/// Remove one membership's expired leases under the membership lock, as one
/// transaction, reporting whether the subject is now gone.
///
/// The same lock [`join_tx`] and [`leave_tx`] take: a join racing this
/// expiry is ordered with it, so the sweep either counts the fresh lease and
/// announces nothing, or removes the stale rows before the join observes
/// them. The remaining-lease count is what keeps a subject with another live
/// tab from being announced as gone.
async fn expire_tx(
    client: &mut tokio_postgres::Client,
    key: &MembershipKey,
) -> Result<OpOutcome, tokio_postgres::Error> {
    let transaction = client.transaction().await?;
    transaction.execute(LOCK_SQL, &[&key.lock_key()]).await?;

    let deleted =
        transaction.execute(EXPIRE_SQL, &[&key.namespace, &key.stream, &key.subject_id]).await?;

    let remaining = count_leases(&transaction, key).await?;
    let total = count_subjects(&transaction, key).await?;
    transaction.commit().await?;

    let transition = departure(deleted, remaining);
    Ok(OpOutcome::Expired { outcome: TransitionOutcome { transition, total }, removed: deleted })
}

/// Bounded periodic removal of expired leases, announcing the subjects that
/// are now gone.
///
/// Every node runs this. Each membership is reclaimed in its own transaction
/// under its own lock, so the work is idempotent, bounded, and ordered with
/// the joins and leaves of the same subject.
async fn sweep_expired(
    session: PgPresenceSession,
    config: PresenceConfig,
    publisher: Arc<dyn PresencePublisher>,
    stats: Arc<PresenceStats>,
) {
    let period = (config.ttl / 2).max(SWEEP_MIN_INTERVAL);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // consume the immediate first tick

    loop {
        ticker.tick().await;
        sweep_once(&session, &config, &*publisher, &stats).await;
    }
}

/// One bounded sweep pass. Separated so the live tests can force one.
async fn sweep_once(
    session: &PgPresenceSession,
    config: &PresenceConfig,
    publisher: &dyn PresencePublisher,
    stats: &PresenceStats,
) {
    for _ in 0..SWEEP_BATCHES_PER_TICK {
        let candidates = match session
            .run(SessionOp::SweepCandidates { namespace: &config.namespace, limit: SWEEP_BATCH })
            .await
        {
            Ok(OpOutcome::Candidates(candidates)) => candidates,
            Ok(_) => return,
            Err(e) => {
                // Expired leases are invisible to every query anyway
                // (`expires_at > now()`), so a failed sweep delays reclamation
                // rather than resurrecting a subject.
                warn!(error = %e, "Presence expiry sweep failed; retrying next tick");
                return;
            }
        };

        if candidates.is_empty() {
            return;
        }

        for (stream, subject_id) in candidates {
            let key = MembershipKey::new(&config.namespace, &stream, &subject_id);
            let (outcome, removed) = match session.run(SessionOp::Expire { key: &key }).await {
                Ok(OpOutcome::Expired { outcome, removed }) => (outcome, removed),
                Ok(_) => return,
                Err(e) => {
                    warn!(
                        error = %e,
                        stream,
                        subject_id,
                        "Presence expiry failed; retrying next tick"
                    );
                    return;
                }
            };

            // A candidate refreshed between the scan and its lock removes no
            // row; it is not counted and not announced.
            stats.swept.fetch_add(removed, Ordering::Relaxed);
            if outcome.transition == Transition::LastLeave {
                let message = PresenceMessage::leave(&stream, &subject_id, outcome.total);
                announce_with(publisher, stats, &message).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
