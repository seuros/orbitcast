//! PostgreSQL-backed retained broker.
//!
//! # The ordering transaction
//!
//! ```text
//! BEGIN
//!   INSERT INTO orbitcast_stream_positions ... ON CONFLICT DO NOTHING   -- first publish
//!   SELECT generation, next_seq ... FOR UPDATE                          -- the per-stream lock
//!   SELECT generation, seq, json_payload ... WHERE publication_id = $id -- dedup, inside the lock
//!   INSERT INTO orbitcast_messages (... generation, seq ...)
//!   UPDATE orbitcast_stream_positions SET next_seq = next_seq + 1
//!   DELETE ... (count bound, one indexed range)
//!   [INSERT INTO orbitcast_payloads ...]                                -- oversized: relay row
//!   SELECT pg_notify($channel, $notice)
//! COMMIT
//! ```
//!
//! The row lock is held until commit, so the *second* publisher to a stream
//! reads `next_seq` only after the first one is durable: sequence order equals
//! commit order, and a reader that has seen sequence `n` can never be handed an
//! `n - 1` afterwards. A rolled-back transaction releases both the row and its
//! sequence, so numbering stays contiguous - there is no burnt sequence value
//! for a reader to wait on forever.
//!
//! Message, position bump, relay row (when the notice is a reference), and
//! notification commit **together**: a node woken by the notification always
//! finds the row, and a rollback sends nothing.
//!
//! # Sessions come from a small pool
//!
//! Retention is on the publish path, so a single serialized connection would
//! turn the per-stream lock into a per-process lock. [`PostgresBroker`] keeps a
//! small pool of independent sessions (default [`DEFAULT_POOL_SIZE`]), which is
//! also what makes the concurrency tests below meaningful: they really do race
//! inside PostgreSQL.
//!
//! # Sweeper
//!
//! One task per node, on its own session, runs two cadences:
//!
//! - every `max(retention / 2, 10 s)`: delete expired messages and sessions of
//!   this namespace in bounded batches, enforce the per-stream byte budget the
//!   publish path deliberately does not, and bump a stream's generation once
//!   retention has wiped its history;
//! - every `reconcile_interval`: one aggregate over the namespace's unexpired
//!   rows, exported as the growth gauges and compared against the namespace
//!   quotas; an excess is trimmed oldest-first in bounded batches.
//!
//! Every statement is small; a backlog costs more statements, never a longer
//! lock.
//!
//! # Generation bumps take the stream lock
//!
//! A bump is `SELECT ... FOR UPDATE` on the position row, a fresh `NOT EXISTS`
//! over the stream's messages *under that lock*, then the `UPDATE`, in one
//! transaction. The publish transaction holds the same lock until it commits,
//! so the existence check cannot run between "no rows" and "one row": either
//! the bump sees the committed message and skips, or the publish waits and
//! numbers its message in the new generation.
//!
//! # DDL is never issued here
//!
//! The three tables ship as `gem/db/orbitcast_migrate/003_create_orbitcast_broker.rb`
//! and `004_rekey_orbitcast_sessions_by_session_id.rb`, applied deliberately. A
//! missing table or a pre-004 session table is reported with the migration to
//! run, never created or altered on the fly.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio_postgres::error::SqlState;
use tracing::{debug, info, warn};

use crate::broker::{
    Broker, BrokerConfig, BrokerError, BrokerStats, DuplicateKind, MESSAGES_TABLE, POSITIONS_TABLE,
    REPLAY_BATCH, Retained, RetainedMessage, SESSIONS_TABLE, StreamRange,
};
use crate::pg::{AbortOnDrop, PgSlot, describe_pg_error};
use crate::protocol::v1::publication::Publication;
use crate::protocol::v1::recovery::{Cursor, SessionSnapshot};
use crate::protocol::v1::validate_namespace;
use crate::pubsub::postgres::OVERFLOW_INSERT_SQL;
use crate::pubsub::{OverflowConfig, PreparedNotice, prepare_publication_notice};

/// Sessions opened per broker.
pub const DEFAULT_POOL_SIZE: usize = 4;

/// Upper bound on the configurable pool size.
pub const MAX_POOL_SIZE: usize = 64;

/// Rows deleted per expiry or quota statement.
const SWEEP_BATCH: i64 = 500;

/// Expiry/quota statements per sweep pass, per table.
const SWEEP_BATCHES_PER_TICK: usize = 20;

/// Oldest rows removed per pass from one over-budget stream.
const BYTE_TRIM_BATCH: i64 = 100;

/// Streams inspected for the byte budget per pass.
const BYTE_TRIM_STREAMS: i64 = 50;

/// Floor for the sweep period, regardless of a short retention window.
const SWEEP_MIN_INTERVAL: Duration = Duration::from_secs(10);

/// Floor for the growth refresh, regardless of a short reconcile interval:
/// the aggregate scans the namespace's rows, and a reader's reconcile cadence
/// can reasonably be far shorter than that is worth running.
const GROWTH_MIN_INTERVAL: Duration = Duration::from_secs(1);

/// Extra rows the inline count trim is allowed to remove in one publish.
///
/// Steady state removes at most one row (each publish adds one), so the window
/// only matters after `max_messages_per_stream` was lowered: the backlog then
/// drains over the next publishes instead of in one long statement.
const COUNT_TRIM_WINDOW: i64 = 64;

const ENSURE_POSITION_SQL: &str = "INSERT INTO orbitcast_stream_positions \
     (namespace, stream, generation, next_seq, updated_at) \
     VALUES ($1, $2, 1, 1, now()) ON CONFLICT (namespace, stream) DO NOTHING";

/// The explicit per-stream lock. Held until commit.
const LOCK_POSITION_SQL: &str = "SELECT generation, next_seq FROM orbitcast_stream_positions \
     WHERE namespace = $1 AND stream = $2 FOR UPDATE";

/// Dedup lookup, executed *inside* the lock so two concurrent publishes of one
/// id cannot both decide they are the first. The payload comes back so an
/// identical retry and a conflicting reuse of the id can be told apart.
const FIND_PUBLICATION_SQL: &str = "SELECT generation, seq, json_payload FROM orbitcast_messages \
     WHERE namespace = $1 AND stream = $2 AND publication_id = $3::text::uuid";

const INSERT_MESSAGE_SQL: &str = "INSERT INTO orbitcast_messages \
     (namespace, stream, generation, seq, publication_id, json_payload, created_at, expires_at) \
     VALUES ($1, $2, $3::bigint, $4::bigint, $5::text::uuid, $6, now(), \
             now() + ($7::bigint * interval '1 millisecond'))";

const BUMP_POSITION_SQL: &str = "UPDATE orbitcast_stream_positions \
     SET next_seq = $3::bigint, updated_at = now() WHERE namespace = $1 AND stream = $2";

/// Count bound, inline: one indexed range on the primary key.
const TRIM_COUNT_SQL: &str = "DELETE FROM orbitcast_messages \
     WHERE namespace = $1 AND stream = $2 AND generation = $3::bigint \
       AND seq <= $4::bigint AND seq > $5::bigint";

const NOTIFY_SQL: &str = "SELECT pg_notify($1, $2)";

/// Position plus the oldest sequence still readable, in one round trip.
const RANGE_SQL: &str = "SELECT p.generation, p.next_seq, \
     (SELECT min(m.seq) FROM orbitcast_messages m \
       WHERE m.namespace = p.namespace AND m.stream = p.stream \
         AND m.generation = p.generation AND m.expires_at > now()) AS oldest \
     FROM orbitcast_stream_positions p WHERE p.namespace = $1 AND p.stream = $2";

const FETCH_AFTER_SQL: &str = "SELECT seq, publication_id::text, json_payload \
     FROM orbitcast_messages \
     WHERE namespace = $1 AND stream = $2 AND generation = $3::bigint AND seq > $4::bigint \
       AND expires_at > now() \
     ORDER BY seq LIMIT $5::bigint";

/// Atomic fencing claim: the `RETURNING` value is the lease generation, and a
/// concurrent claim (any node, any token for this session) always returns a
/// strictly greater one. Also the row's first write, with an empty (but
/// valid) snapshot; a conflict keeps the stored one.
const FENCE_SQL: &str = "INSERT INTO orbitcast_sessions \
     (namespace, session_id, snapshot, fencing_generation, expires_at) \
     VALUES ($1, $2, $4, 1, now() + ($3::bigint * interval '1 millisecond')) \
     ON CONFLICT (namespace, session_id) DO UPDATE \
     SET fencing_generation = orbitcast_sessions.fencing_generation + 1, \
         expires_at = now() + ($3::bigint * interval '1 millisecond') \
     RETURNING fencing_generation";

const READ_FENCE_SQL: &str = "SELECT fencing_generation FROM orbitcast_sessions \
     WHERE namespace = $1 AND session_id = $2 AND expires_at > now()";

/// Expiry extension for a *live* session. An expired row - swept or not yet
/// swept - is left alone: a lease that lapsed must not come back without a
/// fence, so the next resume recreates it through `FENCE_SQL` instead.
const TOUCH_SESSION_SQL: &str = "UPDATE orbitcast_sessions \
     SET expires_at = now() + ($3::bigint * interval '1 millisecond') \
     WHERE namespace = $1 AND session_id = $2 AND expires_at > now()";

const SAVE_SNAPSHOT_SQL: &str = "INSERT INTO orbitcast_sessions \
     (namespace, session_id, snapshot, fencing_generation, expires_at) \
     VALUES ($1, $2, $3, 0, now() + ($4::bigint * interval '1 millisecond')) \
     ON CONFLICT (namespace, session_id) DO UPDATE \
     SET snapshot = EXCLUDED.snapshot, expires_at = EXCLUDED.expires_at";

const LOAD_SNAPSHOT_SQL: &str = "SELECT snapshot FROM orbitcast_sessions \
     WHERE namespace = $1 AND session_id = $2 AND expires_at > now()";

/// Expiry sweeps, scoped to this node's namespace on the
/// `(namespace, expires_at)` index.
const SWEEP_MESSAGES_SQL: &str = "DELETE FROM orbitcast_messages WHERE ctid IN \
     (SELECT ctid FROM orbitcast_messages WHERE namespace = $1 AND expires_at < now() \
       ORDER BY expires_at LIMIT $2::bigint)";

const SWEEP_SESSIONS_SQL: &str = "DELETE FROM orbitcast_sessions WHERE ctid IN \
     (SELECT ctid FROM orbitcast_sessions WHERE namespace = $1 AND expires_at < now() \
       ORDER BY expires_at LIMIT $2::bigint)";

/// Streams whose retained payload text exceeds the byte budget.
const OVER_BUDGET_SQL: &str = "SELECT stream, generation FROM orbitcast_messages \
     WHERE namespace = $1 GROUP BY stream, generation \
     HAVING sum(length(json_payload))::bigint > $2::bigint \
     ORDER BY sum(length(json_payload)) DESC LIMIT $3::bigint";

const TRIM_BYTES_SQL: &str = "DELETE FROM orbitcast_messages WHERE ctid IN \
     (SELECT ctid FROM orbitcast_messages \
       WHERE namespace = $1 AND stream = $2 AND generation = $3::bigint \
       ORDER BY seq LIMIT $4::bigint)";

/// The namespace's live footprint: the growth gauges and the quota input.
const GROWTH_SQL: &str = "SELECT count(*)::bigint, \
     coalesce(sum(length(json_payload)), 0)::bigint \
     FROM orbitcast_messages WHERE namespace = $1 AND expires_at > now()";

/// Namespace quota trim: the rows closest to expiry go first, which under a
/// fixed retention window is the oldest history.
const TRIM_NAMESPACE_SQL: &str = "DELETE FROM orbitcast_messages WHERE ctid IN \
     (SELECT ctid FROM orbitcast_messages WHERE namespace = $1 \
       ORDER BY expires_at LIMIT $2::bigint)";

/// Streams whose history is gone but whose numbering has moved on: the
/// candidates for a generation bump. Only a hint - the bump transaction
/// re-checks under the stream lock.
const WIPED_STREAMS_SQL: &str = "SELECT p.stream, p.generation FROM orbitcast_stream_positions p \
     WHERE p.namespace = $1 AND p.next_seq > 1 \
       AND NOT EXISTS (SELECT 1 FROM orbitcast_messages m \
                        WHERE m.namespace = p.namespace AND m.stream = p.stream \
                          AND m.generation = p.generation) \
     LIMIT $2::bigint";

/// The existence check a bump makes under the lock: a fresh statement, so a
/// message the lock waited for is visible.
const STREAM_HAS_MESSAGES_SQL: &str = "SELECT EXISTS (SELECT 1 FROM orbitcast_messages \
     WHERE namespace = $1 AND stream = $2 AND generation = $3::bigint)";

/// Generation bump, under the same row lock a publish takes.
const BUMP_GENERATION_SQL: &str = "UPDATE orbitcast_stream_positions \
     SET generation = generation + 1, next_seq = 1, updated_at = now() \
     WHERE namespace = $1 AND stream = $2 AND generation = $3::bigint";

/// A `bigint` column read as the non-negative count it is.
fn unsigned(row: &tokio_postgres::Row, index: usize) -> u64 {
    row.get::<_, i64>(index).max(0).cast_unsigned()
}

/// What to tell an operator whose broker tables do not exist.
fn missing_table_hint() -> String {
    format!(
        "the broker tables ({MESSAGES_TABLE}, {POSITIONS_TABLE}, {SESSIONS_TABLE}) do not \
         exist; install the broker migrations (bin/rails orbitcast:install:migrations && \
         bin/rails db:migrate, from gem/db/orbitcast_migrate/003_create_orbitcast_broker.rb \
         and 004_rekey_orbitcast_sessions_by_session_id.rb) or disable the broker"
    )
}

/// What to tell an operator whose session table predates migration 004.
fn stale_schema_hint() -> String {
    format!(
        "{SESSIONS_TABLE} has the migration-003 shape (keyed by token hash); apply \
         gem/db/orbitcast_migrate/004_rekey_orbitcast_sessions_by_session_id.rb, which keys \
         recovery sessions by session_id so that fencing is per session"
    )
}

/// Render one server-reported broker failure with the migration it points at.
fn describe_broker_error(error: &tokio_postgres::Error) -> String {
    match error.code() {
        Some(code) if *code == SqlState::UNDEFINED_COLUMN => {
            format!("{}: {error}", stale_schema_hint())
        }
        _ => describe_pg_error(error, missing_table_hint),
    }
}

/// Static configuration of the PostgreSQL broker.
#[derive(Debug, Clone)]
pub struct PostgresBrokerConfig {
    /// Connection string; the same cable database the pub/sub backend uses.
    pub database_url: String,
    /// Deployment namespace. Part of every key; two deployments sharing a
    /// database never share history.
    pub namespace: String,
    /// Retention and replay bounds.
    pub broker: BrokerConfig,
    /// Independent sessions opened for retention and replay.
    pub pool_size: usize,
    /// The overflow relay settings of the pub/sub backend: whether an
    /// oversized retained publication may be announced by reference, and how
    /// long its relay row lives.
    pub overflow: OverflowConfig,
}

impl PostgresBrokerConfig {
    /// Build a config with the default pool size and the relay off.
    pub fn new(
        database_url: impl Into<String>,
        namespace: impl Into<String>,
        broker: BrokerConfig,
    ) -> Self {
        Self {
            database_url: database_url.into(),
            namespace: namespace.into(),
            broker,
            pool_size: DEFAULT_POOL_SIZE,
            overflow: OverflowConfig::default(),
        }
    }

    /// Reject unusable configuration at the boundary.
    ///
    /// # Errors
    ///
    /// [`BrokerError::Config`] when `database_url` is blank, `namespace` is
    /// not a valid namespace, `pool_size` is zero or above [`MAX_POOL_SIZE`],
    /// the overflow settings fail [`OverflowConfig::validate`], or the
    /// retention bounds fail [`BrokerConfig::validate`].
    pub fn validate(&self) -> Result<(), BrokerError> {
        if self.database_url.trim().is_empty() {
            return Err(BrokerError::Config("database_url must not be empty".to_string()));
        }
        validate_namespace(&self.namespace).map_err(|e| {
            BrokerError::Config(format!("namespace is required for the retained broker: {e}"))
        })?;
        if self.pool_size == 0 || self.pool_size > MAX_POOL_SIZE {
            return Err(BrokerError::Config(format!(
                "broker_pool_size must be between 1 and {MAX_POOL_SIZE}"
            )));
        }
        self.overflow
            .validate()
            .map_err(|e| BrokerError::Config(format!("overflow relay settings: {e}")))?;
        self.broker.validate()
    }
}

/// Run one statement (or self-contained transaction) on a pooled session.
///
/// A server-reported failure (missing table, constraint violation) is returned
/// as-is; a transport failure reconnects once and re-runs, which is safe
/// because every operation here is a single statement or one transaction that
/// commits or rolls back as a whole.
///
/// A macro rather than a generic helper: the operation borrows both the client
/// *and* the caller's parameters, which no single higher-ranked `Fn` bound can
/// express without boxing everything into `'static`.
macro_rules! on_pool {
    ($broker:expr, $what:literal, |$client:ident| $body:expr) => {{
        let broker = $broker;
        let index = broker.next_link.fetch_add(1, Ordering::Relaxed) % broker.links.len();
        let link = &broker.links[index];
        let mut guard = link.acquire().await?;

        let first = {
            let $client = &mut guard.client;
            match $body.await {
                Ok(value) => Ok(value),
                Err(error) => Err(error),
            }
        };

        // The session stays locked through the retry: a reconnect replaces
        // the client this caller holds, never one another caller is using.
        let outcome = match first {
            Ok(value) => Ok(value),
            Err(error) if error.code().is_some() => Err(BrokerError::Backend(anyhow::anyhow!(
                "{} failed: {}",
                $what,
                describe_broker_error(&error)
            ))),
            Err(error) => {
                debug!(
                    error = %error,
                    operation = $what,
                    "Broker session failed; reconnecting once"
                );
                link.reopen(&mut guard).await?;
                let $client = &mut guard.client;
                $body.await.map_err(|retry| {
                    BrokerError::Backend(anyhow::anyhow!(
                        "{} failed after reconnect ({}); first attempt: {error}",
                        $what,
                        describe_broker_error(&retry)
                    ))
                })
            }
        };
        drop(guard);
        outcome
    }};
}

/// PostgreSQL-backed retained broker.
pub struct PostgresBroker {
    config: PostgresBrokerConfig,
    links: Vec<PgSlot>,
    next_link: AtomicUsize,
    stats: Arc<BrokerStats>,
    _sweeper: AbortOnDrop,
}

impl PostgresBroker {
    /// Open the session pool and start the sweeper.
    ///
    /// Fails when the configuration is unusable or no session can be
    /// established: a broker that cannot write is never returned half-working.
    ///
    /// # Errors
    ///
    /// [`BrokerError::Config`] when `config` fails
    /// [`PostgresBrokerConfig::validate`] (checked before any connection is
    /// opened), and [`BrokerError::Backend`] when any pooled or sweeper
    /// session cannot connect.
    pub async fn connect(config: PostgresBrokerConfig) -> Result<Arc<Self>, BrokerError> {
        config.validate()?;

        let mut links = Vec::with_capacity(config.pool_size);
        for _ in 0..config.pool_size {
            links.push(PgSlot::connect(config.database_url.clone(), "broker").await?);
        }

        let stats = Arc::new(BrokerStats::default());
        let sweeper = PgSlot::connect(config.database_url.clone(), "broker sweeper").await?;
        let sweeper_task = tokio::spawn(sweep_forever(
            sweeper,
            config.namespace.clone(),
            config.broker.clone(),
            stats.clone(),
        ));

        info!(
            namespace = %config.namespace,
            retention_secs = config.broker.retention.as_secs(),
            max_messages = config.broker.max_messages_per_stream,
            max_messages_per_namespace = config.broker.max_messages_per_namespace,
            relay = config.overflow.enabled,
            pool = config.pool_size,
            "Retained broker connected"
        );

        Ok(Arc::new(Self {
            config,
            links,
            next_link: AtomicUsize::new(0),
            stats,
            _sweeper: AbortOnDrop(sweeper_task),
        }))
    }

    /// The configured retention and replay bounds.
    pub const fn config(&self) -> &BrokerConfig {
        &self.config.broker
    }

    fn retention_ms(&self) -> i64 {
        i64::try_from(self.config.broker.retention.as_millis()).unwrap_or(i64::MAX)
    }

    fn session_ttl_ms(&self) -> i64 {
        i64::try_from(self.config.broker.session_ttl.as_millis()).unwrap_or(i64::MAX)
    }
}

/// Everything one retention transaction needs, borrowed from the caller.
struct RetainRequest<'a> {
    namespace: &'a str,
    publication: &'a Publication,
    /// The channel to notify, the encoded notice, and - for an oversized
    /// publication - the relay row to write in the same transaction.
    prepared: &'a PreparedNotice,
    retention_ms: i64,
    relay_ttl_ms: i64,
    max_messages: i64,
}

/// The whole ordering transaction, as one unit.
async fn retain_tx(
    client: &mut tokio_postgres::Client,
    request: &RetainRequest<'_>,
) -> Result<Retained, tokio_postgres::Error> {
    let RetainRequest {
        namespace,
        publication,
        prepared,
        retention_ms,
        relay_ttl_ms,
        max_messages,
    } = *request;
    let stream = publication.stream.as_str();
    let publication_id = publication.id.as_str();
    let relayed = prepared.relay_row.is_some();

    // Dropping an uncommitted transaction rolls it back, which also releases
    // the sequence it took: numbering stays contiguous.
    let transaction = client.transaction().await?;

    transaction.execute(ENSURE_POSITION_SQL, &[&namespace, &stream]).await?;

    let position = transaction.query_one(LOCK_POSITION_SQL, &[&namespace, &stream]).await?;
    let generation: i64 = position.get(0);
    let next_seq: i64 = position.get(1);

    // Deduplication happens under the lock, so a racing retry of the same
    // publication cannot be assigned a second sequence.
    if let Some(existing) =
        transaction.query_opt(FIND_PUBLICATION_SQL, &[&namespace, &stream, &publication_id]).await?
    {
        let existing_generation: i64 = existing.get(0);
        let existing_seq: i64 = existing.get(1);
        let existing_payload: String = existing.get(2);
        transaction.commit().await?;
        let kind = if existing_payload == publication.json_payload {
            DuplicateKind::Identical
        } else {
            DuplicateKind::Conflicting
        };
        return Ok(Retained {
            position: Cursor::new(
                existing_generation.cast_unsigned(),
                existing_seq.cast_unsigned(),
            ),
            duplicate: Some(kind),
            relayed,
        });
    }

    transaction
        .execute(
            INSERT_MESSAGE_SQL,
            &[
                &namespace,
                &stream,
                &generation,
                &next_seq,
                &publication_id,
                &publication.json_payload,
                &retention_ms,
            ],
        )
        .await?;

    let bumped = next_seq + 1;
    transaction.execute(BUMP_POSITION_SQL, &[&namespace, &stream, &bumped]).await?;

    // Count bound: drop everything older than the window, bounded so a lowered
    // limit drains over several publishes instead of one long statement.
    let ceiling = next_seq - max_messages;
    if ceiling > 0 {
        let floor = (ceiling - COUNT_TRIM_WINDOW).max(0);
        transaction
            .execute(TRIM_COUNT_SQL, &[&namespace, &stream, &generation, &ceiling, &floor])
            .await?;
    }

    // Oversized: the live listeners read the relay row the notice references,
    // committed together with the retained row so neither can exist alone.
    if let Some(row) = &prepared.relay_row {
        transaction
            .execute(
                OVERFLOW_INSERT_SQL,
                &[&row.id, &row.namespace, &row.stream, &row.json_payload, &relay_ttl_ms],
            )
            .await?;
    }

    // Visible on commit, never before.
    transaction.execute(NOTIFY_SQL, &[&prepared.channel, &prepared.encoded]).await?;
    transaction.commit().await?;

    Ok(Retained {
        position: Cursor::new(generation.cast_unsigned(), next_seq.cast_unsigned()),
        duplicate: None,
        relayed,
    })
}

#[async_trait]
impl Broker for PostgresBroker {
    async fn retain(&self, publication: &Publication) -> Result<Retained, BrokerError> {
        // Same decision the ephemeral path makes - inline when it fits, a
        // relay reference when the relay is on, refused otherwise - so the
        // ingress answers identically whether or not a stream is retained.
        let prepared = prepare_publication_notice(publication.clone(), self.config.overflow)
            .map_err(BrokerError::Publication)?;

        let request = RetainRequest {
            namespace: self.config.namespace.as_str(),
            publication,
            prepared: &prepared,
            retention_ms: self.retention_ms(),
            relay_ttl_ms: i64::try_from(self.config.overflow.ttl.as_millis()).unwrap_or(i64::MAX),
            max_messages: i64::try_from(self.config.broker.max_messages_per_stream)
                .unwrap_or(i64::MAX),
        };

        let retained = on_pool!(self, "retain", |client| retain_tx(client, &request))?;

        match retained.duplicate {
            None => self.stats.record_retained(),
            Some(kind @ DuplicateKind::Identical) => {
                self.stats.record_duplicate(kind);
                debug!(
                    stream = %publication.stream,
                    publication_id = %publication.id,
                    sequence = retained.position.sequence,
                    "Publication retried with the same id and payload; already retained"
                );
            }
            Some(kind @ DuplicateKind::Conflicting) => {
                self.stats.record_duplicate(kind);
                warn!(
                    stream = %publication.stream,
                    publication_id = %publication.id,
                    sequence = retained.position.sequence,
                    "Duplicate publication id with a different payload within retention: first \
                     write wins, this one is not stored (two payloads under one id is a \
                     publisher bug)"
                );
            }
        }

        Ok(retained)
    }

    async fn range(&self, stream: &str) -> Result<StreamRange, BrokerError> {
        let namespace = self.config.namespace.as_str();

        let row = on_pool!(self, "broker range", |client| client
            .query_opt(RANGE_SQL, &[&namespace, &stream]))?;

        let Some(row) = row else {
            // No position row: the stream has never been published to.
            return Ok(StreamRange { watermark: Cursor::new(1, 0), oldest: None });
        };

        let generation: i64 = row.get(0);
        let next_seq: i64 = row.get(1);
        let oldest: Option<i64> = row.get(2);

        Ok(StreamRange {
            watermark: Cursor::new(
                generation.cast_unsigned(),
                (next_seq - 1).max(0).cast_unsigned(),
            ),
            oldest: oldest.map(i64::cast_unsigned),
        })
    }

    async fn fetch_after(
        &self,
        stream: &str,
        after: Cursor,
        limit: usize,
    ) -> Result<Vec<RetainedMessage>, BrokerError> {
        let namespace = self.config.namespace.as_str();
        let generation = i64::try_from(after.generation).unwrap_or(i64::MAX);
        let sequence = i64::try_from(after.sequence).unwrap_or(i64::MAX);
        // At most `REPLAY_BATCH`, so the conversion cannot actually fail.
        let limit = i64::try_from(limit.min(REPLAY_BATCH)).unwrap_or(i64::MAX);

        let rows = on_pool!(self, "broker fetch", |client| client
            .query(FETCH_AFTER_SQL, &[&namespace, &stream, &generation, &sequence, &limit]))?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let seq: i64 = row.get(0);
                RetainedMessage {
                    stream: stream.to_string(),
                    publication_id: row.get::<_, String>(1),
                    position: Cursor::new(after.generation, seq.cast_unsigned()),
                    json_payload: row.get::<_, String>(2),
                }
            })
            .collect())
    }

    async fn fence(&self, session_id: &str) -> Result<u64, BrokerError> {
        let namespace = self.config.namespace.as_str();
        let ttl_ms = self.session_ttl_ms();
        // Every stored snapshot validates, the first one included.
        let empty = SessionSnapshot::new(session_id, BTreeMap::new())
            .to_json()
            .map_err(|e| BrokerError::InvalidRow { stream: String::new(), source: e })?;

        let row = on_pool!(self, "broker fence", |client| client
            .query_one(FENCE_SQL, &[&namespace, &session_id, &ttl_ms, &empty]))?;

        self.stats.record_fenced();
        Ok(unsigned(&row, 0))
    }

    async fn fencing_generation(&self, session_id: &str) -> Result<u64, BrokerError> {
        let namespace = self.config.namespace.as_str();

        let row = on_pool!(self, "broker fence read", |client| client
            .query_opt(READ_FENCE_SQL, &[&namespace, &session_id]))?;

        Ok(row.map_or(0, |row| unsigned(&row, 0)))
    }

    async fn touch_session(&self, session_id: &str) -> Result<(), BrokerError> {
        let namespace = self.config.namespace.as_str();
        let ttl_ms = self.session_ttl_ms();

        on_pool!(self, "broker session touch", |client| client
            .execute(TOUCH_SESSION_SQL, &[&namespace, &session_id, &ttl_ms]))?;

        Ok(())
    }

    async fn save_snapshot(&self, session_id: &str, snapshot: &str) -> Result<(), BrokerError> {
        let namespace = self.config.namespace.as_str();
        let ttl_ms = self.session_ttl_ms();

        on_pool!(self, "broker snapshot", |client| client
            .execute(SAVE_SNAPSHOT_SQL, &[&namespace, &session_id, &snapshot, &ttl_ms]))?;

        Ok(())
    }

    async fn load_snapshot(&self, session_id: &str) -> Result<Option<String>, BrokerError> {
        let namespace = self.config.namespace.as_str();

        let row = on_pool!(self, "broker snapshot read", |client| client
            .query_opt(LOAD_SNAPSHOT_SQL, &[&namespace, &session_id]))?;

        Ok(row.map(|row| row.get::<_, String>(0)))
    }

    fn counters(&self) -> &BrokerStats {
        &self.stats
    }
}

/// Periodic bounded expiry, byte-budget enforcement, generation bumps, and -
/// on the reconcile cadence - the growth refresh with its quota trim.
async fn sweep_forever(
    link: PgSlot,
    namespace: String,
    config: BrokerConfig,
    stats: Arc<BrokerStats>,
) {
    let sweep_period = (config.retention / 2).max(SWEEP_MIN_INTERVAL);
    let mut sweep = tokio::time::interval(sweep_period);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    sweep.tick().await; // consume the immediate first tick

    let growth_period = config.reconcile_interval.max(GROWTH_MIN_INTERVAL);
    let mut growth = tokio::time::interval(growth_period);
    growth.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        // Expired rows are already unreadable (`expires_at > now()` in every
        // read), and a quota is a ceiling on growth, not on correctness: a
        // failed pass delays reclamation rather than corrupting delivery.
        tokio::select! {
            _ = sweep.tick() => {
                if let Err(e) = sweep_once(&link, &namespace, &config, &stats).await {
                    warn!(error = %e, "Broker sweep failed; retrying next tick");
                }
            }
            _ = growth.tick() => {
                if let Err(e) = refresh_growth(&link, &namespace, &config, &stats).await {
                    warn!(error = %e, "Broker growth refresh failed; retrying next tick");
                }
            }
        }
    }
}

/// One sweep pass. Exposed to the integration tests so expiry is deterministic
/// there instead of timing-dependent.
async fn sweep_once(
    link: &PgSlot,
    namespace: &str,
    config: &BrokerConfig,
    stats: &BrokerStats,
) -> Result<u64, BrokerError> {
    // One lock for the whole pass: the sweeper's session runs its statements
    // back to back, and nothing else ever uses it.
    let mut guard = link.acquire().await?;
    let client = &mut guard.client;
    let mut removed: u64 = 0;

    for (sql, label) in [(SWEEP_MESSAGES_SQL, "messages"), (SWEEP_SESSIONS_SQL, "sessions")] {
        for _ in 0..SWEEP_BATCHES_PER_TICK {
            let deleted = client
                .execute(sql, &[&namespace, &SWEEP_BATCH])
                .await
                .map_err(|e| BrokerError::Backend(anyhow::anyhow!("{label} sweep: {e}")))?;
            removed += deleted;
            if deleted == 0 {
                break;
            }
        }
    }

    // Byte budget: one aggregate per pass, then bounded deletes of the oldest
    // rows. Deliberately not on the publish path (see the module docs).
    let budget = i64::try_from(config.max_bytes_per_stream).unwrap_or(i64::MAX);
    let over_budget = client
        .query(OVER_BUDGET_SQL, &[&namespace, &budget, &BYTE_TRIM_STREAMS])
        .await
        .map_err(|e| BrokerError::Backend(anyhow::anyhow!("byte budget scan: {e}")))?;

    for row in over_budget {
        let stream: String = row.get(0);
        let generation: i64 = row.get(1);
        let deleted = client
            .execute(TRIM_BYTES_SQL, &[&namespace, &stream, &generation, &BYTE_TRIM_BATCH])
            .await
            .map_err(|e| BrokerError::Backend(anyhow::anyhow!("byte budget trim: {e}")))?;
        removed += deleted;
        debug!(stream = %stream, rows = deleted, "Trimmed an over-budget stream");
    }

    // A stream whose history retention wiped starts a new generation, so an old
    // cursor is an explicit gap rather than a sequence that silently matches.
    let wiped = client
        .query(WIPED_STREAMS_SQL, &[&namespace, &BYTE_TRIM_STREAMS])
        .await
        .map_err(|e| BrokerError::Backend(anyhow::anyhow!("generation scan: {e}")))?;

    for row in wiped {
        let stream: String = row.get(0);
        let generation: i64 = row.get(1);
        if bump_generation_tx(client, namespace, &stream, generation)
            .await
            .map_err(|e| BrokerError::Backend(anyhow::anyhow!("generation bump: {e}")))?
        {
            info!(
                stream = %stream,
                generation = generation + 1,
                "Retention wiped a stream's history; cursors from the previous generation are \
                 now explicit gaps"
            );
        }
    }
    drop(guard);

    if removed > 0 {
        stats.record_swept(removed);
        debug!(rows = removed, "Broker sweep removed expired or over-budget rows");
    }

    Ok(removed)
}

/// Bump one stream's generation, checked and applied under its position lock.
///
/// Returns whether the bump happened. It does not when the stream was
/// published to (or already bumped) between the candidate scan and the lock:
/// the existence check is a fresh statement after `FOR UPDATE` returned, so a
/// message committed by the publish the lock waited for is visible to it.
async fn bump_generation_tx(
    client: &mut tokio_postgres::Client,
    namespace: &str,
    stream: &str,
    expected_generation: i64,
) -> Result<bool, tokio_postgres::Error> {
    let transaction = client.transaction().await?;

    let Some(position) = transaction.query_opt(LOCK_POSITION_SQL, &[&namespace, &stream]).await?
    else {
        return Ok(false);
    };
    let generation: i64 = position.get(0);
    let next_seq: i64 = position.get(1);
    if generation != expected_generation || next_seq <= 1 {
        return Ok(false);
    }

    let has_messages: bool = transaction
        .query_one(STREAM_HAS_MESSAGES_SQL, &[&namespace, &stream, &generation])
        .await?
        .get(0);
    if has_messages {
        return Ok(false);
    }

    let bumped =
        transaction.execute(BUMP_GENERATION_SQL, &[&namespace, &stream, &generation]).await?;
    transaction.commit().await?;
    Ok(bumped > 0)
}

/// Refresh the growth gauges and enforce the namespace quotas.
///
/// Returns the rows the quota trim removed. Exposed to the integration tests
/// so the quota path is deterministic there.
async fn refresh_growth(
    link: &PgSlot,
    namespace: &str,
    config: &BrokerConfig,
    stats: &BrokerStats,
) -> Result<u64, BrokerError> {
    let mut guard = link.acquire().await?;
    let client = &mut guard.client;

    let row = client
        .query_one(GROWTH_SQL, &[&namespace])
        .await
        .map_err(|e| BrokerError::Backend(anyhow::anyhow!("growth aggregate: {e}")))?;
    let messages = unsigned(&row, 0);
    let bytes = unsigned(&row, 1);
    stats.set_growth(messages, bytes);

    // Rows to drop: the message excess, or the byte excess converted at the
    // namespace's current average row size. Either way bounded per pass; a
    // large excess drains over the next passes.
    let excess_messages = messages.saturating_sub(config.max_messages_per_namespace);
    let excess_bytes = bytes.saturating_sub(config.max_bytes_per_namespace);
    let average = bytes.checked_div(messages).unwrap_or(1).max(1);
    let to_remove = excess_messages.max(excess_bytes.div_ceil(average));
    if to_remove == 0 {
        drop(guard);
        return Ok(0);
    }

    let per_pass = u64::try_from(SWEEP_BATCH).unwrap_or(u64::MAX)
        * u64::try_from(SWEEP_BATCHES_PER_TICK).unwrap_or(u64::MAX);
    let mut remaining = to_remove.min(per_pass);
    let mut removed: u64 = 0;
    while remaining > 0 {
        let batch = i64::try_from(remaining).unwrap_or(i64::MAX).min(SWEEP_BATCH);
        let deleted = client
            .execute(TRIM_NAMESPACE_SQL, &[&namespace, &batch])
            .await
            .map_err(|e| BrokerError::Backend(anyhow::anyhow!("namespace quota trim: {e}")))?;
        removed += deleted;
        if deleted == 0 {
            break;
        }
        remaining = remaining.saturating_sub(deleted);
    }
    drop(guard);

    if removed > 0 {
        stats.record_swept(removed);
        stats.set_growth(messages.saturating_sub(removed), bytes);
        warn!(
            namespace,
            messages,
            bytes,
            removed,
            max_messages = config.max_messages_per_namespace,
            max_bytes = config.max_bytes_per_namespace,
            "Namespace quota exceeded; trimmed the oldest retained rows (resuming clients see \
             an expired gap there)"
        );
    }

    Ok(removed)
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests;
