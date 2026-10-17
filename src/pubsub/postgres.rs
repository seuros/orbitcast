//! PostgreSQL pub/sub backend using LISTEN/NOTIFY.
//!
//! # Task layout
//!
//! ```text
//!   subscribe()/unsubscribe()  --command+oneshot ack-->  supervisor
//!   publish()  -->  persistent publisher connection  -->  pg_notify()
//!
//!   connection driver task --bounded intake--> supervisor --bounded delivery--> listen()
//! ```
//!
//! * The **connection driver task** does nothing but poll
//!   [`tokio_postgres::Connection`] and forward notifications. It is never
//!   blocked by a query, so awaiting `LISTEN` in the supervisor cannot deadlock
//!   the driver.
//! * The **supervisor** owns the interest set (`channel -> stream`), executes
//!   subscription commands, decodes notices, and reconnects. It is spawned at
//!   construction, so subscriptions are acknowledged before anyone calls
//!   [`PubSub::listen`].
//! * The **publisher** is one long-lived connection reused across publishes,
//!   reconnecting on failure. No connection-per-broadcast.
//!
//! # Acknowledged subscriptions
//!
//! `subscribe`/`unsubscribe` send a command with a `oneshot` reply channel and
//! await it under [`PostgresConfig::ack_timeout`]. The supervisor replies only
//! after `client.batch_execute("LISTEN ...")` resolves, which the simple query
//! protocol completes on the server's `ReadyForQuery` - i.e. the command was
//! executed, not merely queued. A failed `LISTEN` both fails the caller and
//! recycles the connection.
//!
//! # Overflow relay
//!
//! With [`OverflowConfig::enabled`], a publication too large for a
//! notification is inserted into `orbitcast_payloads` and referenced by an
//! overflow notice **in one transaction** on the publisher session: `NOTIFY`
//! becomes visible on commit, so no node can see a reference to a row it
//! cannot read.
//!
//! Receiving nodes fetch the row from the same primary over a dedicated
//! **overflow session**, using a single autocommit `SELECT` - a fresh `READ
//! COMMITTED` snapshot, never a reused one - and re-validate the row's
//! namespace and stream against the reference.
//!
//! Ordering: the supervisor never awaits a fetch. It spawns the fetch and
//! immediately pushes a *slot* into the same bounded delivery queue the inline
//! publications use, so the single consumer in [`PubSub::listen`] observes
//! notices in arrival order - per-stream order is preserved across mixed
//! inline and overflow traffic. Buffering is bounded by the delivery queue;
//! head-of-line blocking is bounded by [`PostgresConfig::ack_timeout`].
//!
//! A missing, expired, or mismatched row, a failed or timed-out fetch, and an
//! overflow notice arriving while the relay is disabled are all **delivery
//! gaps**: counted in [`PubSubStats::snapshot`] as `overflow_gaps`, warned, and
//! handed to the listener as a [`PubSubEvent::Gap`] in the stream's delivery
//! order, so the server can disconnect that stream's local members with a
//! reconnectable `disconnect` - notification-only delivery cannot recover the
//! publication, and a client that resubscribes at least knows it missed one.
//!
//! Expiry cleanup belongs to Rust: a bounded periodic `DELETE` task runs while
//! the relay is enabled. The table itself is never created here - a missing
//! table is reported with the migration to run.
//!
//! # Drop policy and gaps
//!
//! Both queues are bounded; a full queue drops the publication, counts it in
//! [`PubSubStats`], and warns. See the [`crate::pubsub`] module documentation.
//! On connection loss the backend goes unready (subscribes and publishes fail
//! fast), re-`LISTEN`s every interested channel on reconnect, and logs a gap
//! warning: notification-only mode cannot recover publications emitted while
//! the listener was down.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio_postgres::AsyncMessage;
use tracing::{debug, info, warn};

use crate::pg::{AbortOnDrop, PgSlot, connect_with_sslmode, describe_pg_error};
use crate::protocol::Envelope;
use crate::protocol::v1::publication::{OverflowRef, Publication};
use crate::protocol::v1::validate_namespace;
use crate::pubsub::{
    DEFAULT_ACK_TIMEOUT, DEFAULT_QUEUE_CAPACITY, DecodedNotice, EventHandler, OVERFLOW_TABLE,
    OverflowConfig, PubSub, PubSubError, PubSubEvent, PubSubStats, StatsSnapshot, channel_for,
    control_channel_for, decode_notice, prepare_publication_notice,
};

/// Parameterized notify; the channel name is never interpolated into SQL.
const NOTIFY_SQL: &str = "SELECT pg_notify($1, $2)";

/// Store one oversized publication. `expires_at` uses the **database** clock,
/// so relay lifetime does not depend on a publishing node's clock.
///
/// Every parameter is bound as text (`$n::text::uuid` keeps the *parameter*
/// type `text` while the column stays `uuid`), so no extra type mapping
/// feature is needed to speak the table contract.
pub const OVERFLOW_INSERT_SQL: &str = "INSERT INTO orbitcast_payloads \
     (id, namespace, stream, json_payload, expires_at) \
     VALUES ($1::text::uuid, $2, $3, $4, now() + ($5::bigint * interval '1 millisecond'))";

/// Read one relay row back.
///
/// Runs as a single autocommit statement: PostgreSQL takes a fresh `READ
/// COMMITTED` snapshot per statement, so a row committed after this session's
/// last statement is still visible. The namespace/stream predicates re-validate
/// the reference - it is routing, not authorization - and `expires_at > now()`
/// uses the database clock, like the insert.
const OVERFLOW_FETCH_SQL: &str = "SELECT json_payload FROM orbitcast_payloads \
     WHERE id = $1::text::uuid AND namespace = $2 AND stream = $3 AND expires_at > now()";

/// Bounded expiry cleanup. The subselect keeps each statement short, so the
/// delete never holds locks across a large scan.
const OVERFLOW_CLEANUP_SQL: &str = "DELETE FROM orbitcast_payloads WHERE id IN \
     (SELECT id FROM orbitcast_payloads WHERE expires_at < now() \
      ORDER BY expires_at LIMIT $1::bigint)";

/// Rows deleted per cleanup statement.
const OVERFLOW_CLEANUP_BATCH: i64 = 500;

/// Cleanup statements per tick; caps one node's delete load per pass.
const OVERFLOW_CLEANUP_BATCHES_PER_TICK: usize = 20;

/// Floor for the cleanup period, regardless of a short TTL.
const OVERFLOW_CLEANUP_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// What to tell an operator whose relay table does not exist.
///
/// Production code never issues DDL: the migration ships with the gem and
/// is applied deliberately, to the cable database.
fn missing_table_hint() -> String {
    format!(
        "the {OVERFLOW_TABLE} table does not exist; install the overflow relay migration \
         (bin/rails orbitcast:install:migrations && bin/rails db:migrate, from \
         gem/db/orbitcast_migrate/001_create_orbitcast_payloads.rb) or disable overflow"
    )
}

/// Default keepalive period on the listener connection.
pub const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Reconnect backoff bounds for the listener supervisor.
const RECONNECT_MIN_BACKOFF: Duration = Duration::from_millis(250);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Bound on unacknowledged subscription commands in flight.
const COMMAND_QUEUE_CAPACITY: usize = 256;

/// Bound on undelivered control notifications.
///
/// Control messages are rare (a sign-out, a forced reconnect); 64 in flight is
/// already a burst no deployment produces. A full queue drops the message with
/// a warning - the affected session then simply stays connected, which fails
/// safe for a disconnect instruction.
const CONTROL_QUEUE_CAPACITY: usize = 64;

/// Static configuration of the PostgreSQL backend.
#[derive(Clone)]
pub struct PostgresConfig {
    /// Connection string for the publisher (and overflow) sessions.
    pub database_url: String,
    /// Connection string for the `LISTEN` session, when it must differ from
    /// `database_url` (a direct connection beside a transaction pooler).
    /// `None` uses `database_url`.
    pub listener_database_url: Option<String>,
    /// Deployment namespace; part of every channel name and every notice.
    pub namespace: String,
    /// Deadline for `LISTEN`/`UNLISTEN` acknowledgement, and for one overflow
    /// fetch.
    pub ack_timeout: Duration,
    /// Bound for the intake and delivery queues.
    pub queue_capacity: usize,
    /// Period of the `SELECT 1` liveness probe on the listener session.
    pub keepalive_interval: Duration,
    /// Overflow relay settings for oversized publications.
    pub overflow: OverflowConfig,
}

impl PostgresConfig {
    /// Build a config with default timeouts for `database_url`/`namespace`.
    ///
    /// The overflow relay is off: it requires a migration, so enabling it is
    /// always an explicit decision.
    pub fn new(database_url: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            listener_database_url: None,
            namespace: namespace.into(),
            ack_timeout: DEFAULT_ACK_TIMEOUT,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            keepalive_interval: DEFAULT_KEEPALIVE_INTERVAL,
            overflow: OverflowConfig::default(),
        }
    }

    /// Reject unusable configuration at the boundary.
    ///
    /// An empty namespace is invalid: it would collide with every other
    /// deployment sharing the database and defeat namespace validation on
    /// receipt.
    ///
    /// # Errors
    ///
    /// [`PubSubError::Config`] when `database_url` is blank, `namespace` is
    /// not a valid namespace, `queue_capacity`, `ack_timeout`, or
    /// `keepalive_interval` is zero, or the [`OverflowConfig`] is unusable
    /// (see [`OverflowConfig::validate`]).
    pub fn validate(&self) -> Result<(), PubSubError> {
        if self.database_url.trim().is_empty() {
            return Err(PubSubError::Config("database_url must not be empty".to_string()));
        }
        if self.listener_database_url.as_ref().is_some_and(|url| url.trim().is_empty()) {
            return Err(PubSubError::Config(
                "listener_database_url must not be empty when set".to_string(),
            ));
        }
        validate_namespace(&self.namespace).map_err(|e| {
            PubSubError::Config(format!(
                "namespace is required for the postgres pub/sub backend: {e}"
            ))
        })?;
        if self.queue_capacity == 0 {
            return Err(PubSubError::Config("queue_capacity must be greater than 0".to_string()));
        }
        if self.ack_timeout.is_zero() {
            return Err(PubSubError::Config("ack_timeout must be greater than 0".to_string()));
        }
        // `tokio::time::interval` panics on a zero period.
        if self.keepalive_interval.is_zero() {
            return Err(PubSubError::Config(
                "keepalive_interval must be greater than 0".to_string(),
            ));
        }
        self.overflow.validate()?;
        Ok(())
    }
}

impl PostgresConfig {
    /// The DSN the `LISTEN` session connects with.
    #[must_use]
    pub fn listener_url(&self) -> &str {
        self.listener_database_url.as_deref().unwrap_or(&self.database_url)
    }
}

impl std::fmt::Debug for PostgresConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresConfig")
            .field("database_url", &crate::config::redact_url(&self.database_url))
            .field(
                "listener_database_url",
                &self.listener_database_url.as_deref().map(crate::config::redact_url),
            )
            .field("namespace", &self.namespace)
            .field("ack_timeout", &self.ack_timeout)
            .field("queue_capacity", &self.queue_capacity)
            .field("keepalive_interval", &self.keepalive_interval)
            .field("overflow", &self.overflow)
            .finish()
    }
}

/// A subscription command awaiting acknowledgement from the listener session.
enum ListenerCommand {
    Subscribe { stream: String, channel: String, ack: oneshot::Sender<Result<(), PubSubError>> },
    Unsubscribe { stream: String, channel: String, ack: oneshot::Sender<Result<(), PubSubError>> },
}

/// One slot in the ordered delivery queue.
///
/// Slots are enqueued in notification arrival order and consumed by the single
/// [`PubSub::listen`] task, which is what keeps per-stream order intact when
/// inline publications and pending overflow fetches interleave.
enum DeliverySlot {
    /// The publication travelled inline; nothing to wait for.
    Ready(Publication),
    /// An overflow fetch for `stream` is in flight. `Err(reason)` means the
    /// fetch produced a gap, already counted and warned by the fetch task.
    Pending { stream: String, fetch: oneshot::Receiver<Result<Publication, String>> },
    /// A gap known at routing time (the relay is disabled on this node),
    /// kept in arrival order so the listener reports it in sequence.
    Gap { stream: String, reason: String },
}

/// PostgreSQL-backed pub/sub using LISTEN/NOTIFY.
pub struct PostgresPubSub {
    config: PostgresConfig,
    commands: mpsc::Sender<ListenerCommand>,
    deliveries: Mutex<Option<mpsc::Receiver<DeliverySlot>>>,
    /// Raw control notification payloads. Taken once by
    /// [`Self::take_control_stream`]; the listener session is permanently
    /// subscribed to the namespaced control channel, and control traffic never
    /// enters the publication delivery queue or the hub fan-out.
    controls: Mutex<Option<mpsc::Receiver<String>>>,
    ready: watch::Receiver<bool>,
    publisher: PgSession,
    stats: Arc<PubSubStats>,
    _supervisor: AbortOnDrop,
    _cleanup: Option<AbortOnDrop>,
}

impl PostgresPubSub {
    /// Connect the publisher, start the listener supervisor, and wait until the
    /// listener session is established.
    ///
    /// Returns an error if either session cannot be established within
    /// [`PostgresConfig::ack_timeout`]: a node that cannot listen would accept
    /// subscriptions that receive nothing.
    ///
    /// # Errors
    ///
    /// - [`PubSubError::Config`] when `config` fails
    ///   [`PostgresConfig::validate`], or the listener session is not
    ///   established within `ack_timeout`,
    /// - [`PubSubError::Backend`] when the publisher (or, with the relay
    ///   enabled, the overflow) session cannot connect,
    /// - [`PubSubError::ListenerGone`] when the listener supervisor exits
    ///   before announcing readiness.
    pub async fn connect(config: PostgresConfig) -> Result<Self, PubSubError> {
        config.validate()?;

        let stats = Arc::new(PubSubStats::default());
        let publisher = PgSession::connect(config.database_url.clone()).await?;

        // The relay needs its own session: an overflow fetch must never queue
        // behind a publish, and a publish must never queue behind a fetch.
        // `None` when the relay is disabled, so an inline-only node opens no
        // extra connection. Only the supervisor (fetches) and the cleanup task
        // (expiry) hold it.
        let overflow = if config.overflow.enabled {
            Some(Arc::new(PgSession::connect(config.database_url.clone()).await?))
        } else {
            None
        };

        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let (delivery_tx, delivery_rx) = mpsc::channel(config.queue_capacity);
        let (control_tx, control_rx) = mpsc::channel(CONTROL_QUEUE_CAPACITY);
        let (ready_tx, ready_rx) = watch::channel(false);

        let supervisor = tokio::spawn(supervise(
            config.clone(),
            command_rx,
            delivery_tx,
            control_tx,
            ready_tx,
            stats.clone(),
            overflow.clone(),
        ));

        // Fail fast rather than accept subscriptions against a dead listener.
        // The `watch::Ref` that `wait_for` yields is released at once: the
        // supervisor's `send_replace` needs the same lock.
        let mut ready_wait = ready_rx.clone();
        let established = tokio::time::timeout(config.ack_timeout, ready_wait.wait_for(|r| *r))
            .await
            .map(|ready| ready.map(|_| ()))
            .map_err(|_| {
                PubSubError::Config(format!(
                    "listener session was not established within {:?}",
                    config.ack_timeout
                ))
            });

        if established.is_err() {
            supervisor.abort();
        }
        established?.map_err(|_| PubSubError::ListenerGone)?;

        info!(
            namespace = %config.namespace,
            overflow = config.overflow.enabled,
            "PostgreSQL listener session established"
        );

        let cleanup = overflow.map(|session| {
            AbortOnDrop(tokio::spawn(cleanup_expired(session, config.overflow.ttl)))
        });

        Ok(Self {
            config,
            commands: command_tx,
            deliveries: Mutex::new(Some(delivery_rx)),
            controls: Mutex::new(Some(control_rx)),
            ready: ready_rx,
            publisher,
            stats,
            _supervisor: AbortOnDrop(supervisor),
            _cleanup: cleanup,
        })
    }

    /// Send a subscription command and await the backend acknowledgement.
    async fn command(
        &self,
        command: &'static str,
        stream: &str,
        build: impl FnOnce(oneshot::Sender<Result<(), PubSubError>>) -> ListenerCommand,
    ) -> Result<(), PubSubError> {
        if !self.is_ready() {
            return Err(PubSubError::NotReady);
        }

        let (ack_tx, ack_rx) = oneshot::channel();
        self.commands.send(build(ack_tx)).await.map_err(|_| PubSubError::ListenerGone)?;

        match tokio::time::timeout(self.config.ack_timeout, ack_rx).await {
            Ok(Ok(result)) => result,
            // The supervisor dropped the ack: the session died mid-command.
            Ok(Err(_)) => Err(PubSubError::NotReady),
            Err(_) => Err(PubSubError::AckTimeout {
                command,
                stream: stream.to_string(),
                timeout: self.config.ack_timeout,
            }),
        }
    }
}

/// Quote a derived channel name for `LISTEN`/`UNLISTEN`.
///
/// Channel names are produced by [`channel_for`], so they are always
/// `orbitcast_` plus lowercase hex; quoting is defensive, not sanitizing.
fn quote_channel(channel: &str) -> String {
    debug_assert!(channel.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
    format!("\"{}\"", channel.replace('"', "\"\""))
}

/// A persistent, single-connection PostgreSQL session.
///
/// One connection, serialized by a mutex, reconnected on failure. Two roles use
/// it: the **publisher** (`pg_notify`, and the overflow insert+notify
/// transaction) and, when the relay is enabled, the **overflow** session
/// (fetches and expiry cleanup). Publishing is not on the fanout hot path
/// (Rails publishes directly), so one session per role is enough and keeps
/// notification ordering per node.
struct PgSession {
    slot: PgSlot,
}

impl PgSession {
    async fn connect(database_url: String) -> Result<Self, PubSubError> {
        Ok(Self { slot: PgSlot::connect(database_url, "pub/sub").await? })
    }

    /// Run one operation against a live client, reconnecting once if the
    /// session died between statements.
    ///
    /// The mutex is held for the whole call, so a multi-statement transaction
    /// cannot interleave with another caller's statement on the same session.
    /// The retry re-runs the operation from the start: each one is a single
    /// statement or a self-contained transaction, so re-running is safe.
    ///
    /// (The operation is a concrete enum rather than a closure: a
    /// higher-ranked async closure would not carry a `Send` bound across the
    /// `PubSub` trait's boxed futures.)
    async fn run(&self, op: SessionOp<'_>) -> Result<SessionOutcome, PubSubError> {
        let what = op.label();
        let mut open = self.slot.acquire().await?;

        let first_error = match execute_op(&mut open.client, &op).await {
            Ok(value) => return Ok(value),
            Err(e) => e,
        };

        // A server-reported failure (missing table, constraint violation) must
        // not be retried as if it were a dead socket: report it as-is.
        if first_error.code().is_some() {
            return Err(PubSubError::Backend(anyhow::anyhow!(
                "{what} failed: {}",
                describe_pg_error(&first_error, missing_table_hint)
            )));
        }

        debug!(
            error = %first_error,
            operation = what,
            "PostgreSQL session failed; reconnecting once"
        );
        self.slot.reopen(&mut open).await?;
        execute_op(&mut open.client, &op).await.map_err(|retry_error| {
            PubSubError::Backend(anyhow::anyhow!(
                "{what} failed after reconnect ({}); first attempt: {first_error}",
                describe_pg_error(&retry_error, missing_table_hint)
            ))
        })
    }

    /// Execute `pg_notify` on the persistent session.
    async fn notify(&self, channel: &str, payload: &str) -> Result<(), PubSubError> {
        self.run(SessionOp::Notify { channel, payload }).await?;
        Ok(())
    }

    /// Store an oversized publication and notify its reference atomically.
    ///
    /// `NOTIFY` becomes visible on commit, so a listener never sees a
    /// reference before the row it points at; a rollback sends nothing and
    /// leaves no row.
    async fn publish_overflow(
        &self,
        channel: &str,
        payload: &str,
        row: &Publication,
        ttl: Duration,
    ) -> Result<(), PubSubError> {
        let ttl_ms = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        self.run(SessionOp::PublishOverflow { channel, payload, row, ttl_ms }).await?;
        Ok(())
    }

    /// Read one relay row back, re-validating the reference against the row.
    ///
    /// `Ok(None)` is a gap: the row is missing, expired, or does not belong to
    /// the referenced `(namespace, stream)`.
    async fn fetch_overflow(&self, reference: &OverflowRef) -> Result<Option<String>, PubSubError> {
        match self.run(SessionOp::FetchOverflow(reference)).await? {
            SessionOutcome::Fetched(payload) => Ok(payload),
            other => Err(PubSubError::Backend(anyhow::anyhow!(
                "overflow fetch produced {other:?} instead of a payload"
            ))),
        }
    }

    /// Delete up to `limit` expired rows, returning how many were removed.
    async fn delete_expired(&self, limit: i64) -> Result<u64, PubSubError> {
        match self.run(SessionOp::DeleteExpired { limit }).await? {
            SessionOutcome::Deleted(rows) => Ok(rows),
            other => Err(PubSubError::Backend(anyhow::anyhow!(
                "overflow cleanup produced {other:?} instead of a row count"
            ))),
        }
    }
}

/// One unit of work for a [`PgSession`].
enum SessionOp<'a> {
    Notify { channel: &'a str, payload: &'a str },
    PublishOverflow { channel: &'a str, payload: &'a str, row: &'a Publication, ttl_ms: i64 },
    FetchOverflow(&'a OverflowRef),
    DeleteExpired { limit: i64 },
}

impl SessionOp<'_> {
    /// Short operation name for error messages and logs; never a payload.
    const fn label(&self) -> &'static str {
        match self {
            SessionOp::Notify { .. } => "pg_notify",
            SessionOp::PublishOverflow { .. } => "overflow publish",
            SessionOp::FetchOverflow(_) => "overflow fetch",
            SessionOp::DeleteExpired { .. } => "overflow cleanup",
        }
    }
}

/// What one [`SessionOp`] produced.
#[derive(Debug)]
enum SessionOutcome {
    /// The statement completed and returns nothing interesting.
    Done,
    /// A relay row's `json_payload`, or `None` when no live row matched.
    Fetched(Option<String>),
    /// Rows removed by a cleanup batch.
    Deleted(u64),
}

async fn execute_op(
    client: &mut tokio_postgres::Client,
    op: &SessionOp<'_>,
) -> Result<SessionOutcome, tokio_postgres::Error> {
    match *op {
        SessionOp::Notify { channel, payload } => {
            client.execute(NOTIFY_SQL, &[&channel, &payload]).await?;
            Ok(SessionOutcome::Done)
        }
        SessionOp::PublishOverflow { channel, payload, row, ttl_ms } => {
            // Dropping an uncommitted Transaction rolls it back, so a failure
            // between the insert and the notify leaves no row and sends nothing.
            let transaction = client.transaction().await?;
            transaction
                .execute(
                    OVERFLOW_INSERT_SQL,
                    &[&row.id, &row.namespace, &row.stream, &row.json_payload, &ttl_ms],
                )
                .await?;
            transaction.execute(NOTIFY_SQL, &[&channel, &payload]).await?;
            transaction.commit().await?;
            Ok(SessionOutcome::Done)
        }
        SessionOp::FetchOverflow(reference) => {
            let row = client
                .query_opt(
                    OVERFLOW_FETCH_SQL,
                    &[&reference.id, &reference.namespace, &reference.stream],
                )
                .await?;
            Ok(SessionOutcome::Fetched(row.map(|row| row.get::<_, String>(0))))
        }
        SessionOp::DeleteExpired { limit } => {
            Ok(SessionOutcome::Deleted(client.execute(OVERFLOW_CLEANUP_SQL, &[&limit]).await?))
        }
    }
}

/// Bounded periodic deletion of expired relay rows.
///
/// Rust owns cleanup. Each tick deletes at most
/// `OVERFLOW_CLEANUP_BATCH * OVERFLOW_CLEANUP_BATCHES_PER_TICK` rows, releasing
/// the session between statements so a fetch is never stuck behind a long
/// delete. Every node runs this; the deletes are idempotent and bounded, and a
/// row this node deletes is one no node can still deliver.
async fn cleanup_expired(session: Arc<PgSession>, ttl: Duration) {
    let period = (ttl / 2).max(OVERFLOW_CLEANUP_MIN_INTERVAL);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // consume the immediate first tick

    loop {
        ticker.tick().await;

        let mut removed: u64 = 0;
        for _ in 0..OVERFLOW_CLEANUP_BATCHES_PER_TICK {
            match session.delete_expired(OVERFLOW_CLEANUP_BATCH).await {
                Ok(0) => break,
                Ok(count) => removed += count,
                Err(e) => {
                    // Expired rows are unreadable anyway (`expires_at > now()`
                    // in the fetch), so a failed sweep delays reclamation
                    // rather than corrupting delivery.
                    warn!(error = %e, "Overflow expiry cleanup failed; retrying next tick");
                    break;
                }
            }
        }

        if removed > 0 {
            debug!(rows = removed, table = OVERFLOW_TABLE, "Deleted expired overflow rows");
        }
    }
}

/// Reconnect supervision loop for the listener session.
async fn supervise(
    config: PostgresConfig,
    mut commands: mpsc::Receiver<ListenerCommand>,
    delivery_tx: mpsc::Sender<DeliverySlot>,
    control_tx: mpsc::Sender<String>,
    ready_tx: watch::Sender<bool>,
    stats: Arc<PubSubStats>,
    overflow: Option<Arc<PgSession>>,
) {
    // channel -> logical stream. Survives reconnects so every interest is
    // re-established, and gives O(1) collision detection without scanning.
    let mut interests: HashMap<String, String> = HashMap::new();
    let mut backoff = RECONNECT_MIN_BACKOFF;

    // The control channel is a permanent interest of this node, held outside
    // the refcounted `interests` map: no subscription ever acquires it and no
    // release may ever drop it.
    let control_channel = control_channel_for(&config.namespace);

    let shared = ListenerShared {
        config: &config,
        delivery_tx: &delivery_tx,
        control_tx: &control_tx,
        control_channel: &control_channel,
        ready_tx: &ready_tx,
        stats: &stats,
        overflow: overflow.as_ref(),
    };

    loop {
        match run_session(&shared, &mut commands, &mut interests).await {
            Ok(()) => {
                ready_tx.send_replace(false);
                debug!("PostgreSQL listener supervisor shutting down");
                return;
            }
            Err(e) => {
                ready_tx.send_replace(false);
                stats.record_reconnect();
                warn!(
                    error = %e,
                    channels = interests.len(),
                    backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                    "PostgreSQL listener connection lost; publications emitted during the gap \
                     cannot be recovered (notification-only mode)"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
            }
        }
    }
}

/// What every listener session borrows from its supervisor.
///
/// The command queue and the interest set are passed separately: they are the
/// only state a session mutates, and the interest set outlives every session.
struct ListenerShared<'a> {
    config: &'a PostgresConfig,
    delivery_tx: &'a mpsc::Sender<DeliverySlot>,
    control_tx: &'a mpsc::Sender<String>,
    /// The namespaced control channel, a permanent interest of this node.
    control_channel: &'a str,
    ready_tx: &'a watch::Sender<bool>,
    stats: &'a Arc<PubSubStats>,
    overflow: Option<&'a Arc<PgSession>>,
}

/// One listener connection's lifetime.
///
/// `Ok(())` means the backend is shutting down (command channel closed);
/// `Err` means the session must be re-established.
async fn run_session(
    shared: &ListenerShared<'_>,
    commands: &mut mpsc::Receiver<ListenerCommand>,
    interests: &mut HashMap<String, String>,
) -> anyhow::Result<()> {
    let ListenerShared {
        config,
        delivery_tx,
        control_tx,
        control_channel,
        ready_tx,
        stats,
        overflow,
    } = *shared;
    let (client, mut connection) = connect_with_sslmode(config.listener_url()).await?;

    // The driver task only polls the connection and forwards notifications, so
    // awaiting a LISTEN below can never stall it.
    let (intake_tx, mut intake_rx) = mpsc::channel(config.queue_capacity);
    let driver_stats = stats.clone();
    let queue_capacity = config.queue_capacity;
    let driver = tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(AsyncMessage::Notification(notification))) => {
                    match intake_tx.try_send(notification) {
                        Ok(()) => {
                            // Lag high-water mark: how far behind the consumer
                            // runs is what predicts the next intake drop.
                            let depth = queue_capacity.saturating_sub(intake_tx.capacity());
                            driver_stats.record_intake_depth(depth as u64);
                        }
                        Err(mpsc::error::TrySendError::Full(dropped)) => {
                            driver_stats.record_intake_drop();
                            warn!(
                                channel = %dropped.channel(),
                                "Notification intake queue full; dropping publication"
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    warn!(error = %e, "Listener connection error");
                    break;
                }
                None => break,
            }
        }
        // Dropping intake_tx signals connection loss to the session loop.
    });
    let _driver_guard = AbortOnDrop(driver);

    // Control interest first, then every stream interest, all before
    // announcing readiness: a ready node is always reachable for a
    // Rails-initiated disconnect.
    client
        .batch_execute(&format!("LISTEN {}", quote_channel(control_channel)))
        .await
        .map_err(|e| anyhow::anyhow!("LISTEN {control_channel} (control) failed: {e}"))?;

    // Re-establish every interest before announcing readiness.
    for (channel, stream) in interests.iter() {
        client
            .batch_execute(&format!("LISTEN {}", quote_channel(channel)))
            .await
            .map_err(|e| anyhow::anyhow!("re-LISTEN {channel} ({stream}) failed: {e}"))?;
    }
    if !interests.is_empty() {
        info!(channels = interests.len(), "Re-established PostgreSQL subscriptions");
    }

    ready_tx.send_replace(true);

    let mut keepalive = tokio::time::interval(config.keepalive_interval);
    keepalive.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    return Ok(()); // backend dropped: shut down
                };
                apply_command(&client, interests, command).await?;
            }
            notification = intake_rx.recv() => {
                let Some(notification) = notification else {
                    anyhow::bail!("listener connection closed");
                };
                // Control messages are consumed by this node, not fanned out:
                // intercepted here, before the publication decoder, because a
                // control envelope is not a `PublicationNotice` and must never
                // enter the hub delivery path.
                if notification.channel() == control_channel {
                    match control_tx.try_send(notification.payload().to_string()) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => warn!(
                            channel = %control_channel,
                            "Control queue full; dropping control notification"
                        ),
                        Err(mpsc::error::TrySendError::Closed(_)) => debug!(
                            channel = %control_channel,
                            "No control consumer; dropping control notification"
                        ),
                    }
                    continue;
                }
                route_notification(config, delivery_tx, stats, overflow, &notification);
            }
            _ = keepalive.tick() => {
                client
                    .execute("SELECT 1", &[])
                    .await
                    .map_err(|e| anyhow::anyhow!("listener keepalive failed: {e}"))?;
            }
        }
    }
}

/// Execute one subscription command and acknowledge it.
///
/// The acknowledgement is sent only after PostgreSQL completed the command.
/// A failed command is reported to the caller *and* recycles the session.
async fn apply_command(
    client: &tokio_postgres::Client,
    interests: &mut HashMap<String, String>,
    command: ListenerCommand,
) -> anyhow::Result<()> {
    match command {
        ListenerCommand::Subscribe { stream, channel, ack } => {
            match interests.get(&channel) {
                // Already listening: idempotent, still acknowledged.
                Some(existing) if existing == &stream => {
                    ack_ok(ack);
                    return Ok(());
                }
                Some(existing) => {
                    // A caller that already timed out has nothing left to be
                    // told; the session itself is unaffected.
                    let _ = ack.send(Err(PubSubError::ChannelCollision {
                        channel,
                        existing: existing.clone(),
                        incoming: stream,
                    }));
                    return Ok(());
                }
                None => {}
            }

            match client.batch_execute(&format!("LISTEN {}", quote_channel(&channel))).await {
                Ok(()) => {
                    debug!(channel = %channel, stream = %stream, "LISTEN acknowledged");
                    interests.insert(channel, stream);
                    ack_ok(ack);
                    Ok(())
                }
                Err(e) => ack_failure(ack, "LISTEN", &channel, &e),
            }
        }
        ListenerCommand::Unsubscribe { stream, channel, ack } => {
            if interests.remove(&channel).is_none() {
                ack_ok(ack);
                return Ok(());
            }

            match client.batch_execute(&format!("UNLISTEN {}", quote_channel(&channel))).await {
                Ok(()) => {
                    debug!(channel = %channel, stream = %stream, "UNLISTEN acknowledged");
                    ack_ok(ack);
                    Ok(())
                }
                Err(e) => ack_failure(ack, "UNLISTEN", &channel, &e),
            }
        }
    }
}

/// Acknowledge a completed (or already-satisfied) subscription command.
///
/// A dropped receiver means the caller timed out or went away; the session
/// stays healthy either way.
fn ack_ok(ack: oneshot::Sender<Result<(), PubSubError>>) {
    // Dropped receiver: caller already gave up, nothing to recover.
    let _ = ack.send(Ok(()));
}

/// Report a failed `LISTEN`/`UNLISTEN` to the waiting caller *and* to the
/// session loop, which recycles the connection: an interest set that PostgreSQL
/// disagrees with must not survive.
fn ack_failure(
    ack: oneshot::Sender<Result<(), PubSubError>>,
    verb: &str,
    channel: &str,
    error: &tokio_postgres::Error,
) -> anyhow::Result<()> {
    let message = format!("{verb} {channel} failed: {error}");
    // The caller may already have timed out; the session is recycled either way.
    let _ = ack.send(Err(PubSubError::Backend(anyhow::anyhow!(message.clone()))));
    Err(anyhow::anyhow!(message))
}

/// Decode one notification and place it in the bounded delivery queue.
///
/// Nothing here awaits, so the session loop keeps draining intake and keeps
/// processing subscription commands: an overflow reference enqueues a *pending*
/// slot and spawns the fetch. Because slots are enqueued in arrival order and
/// drained by one consumer, order is preserved per stream (and globally) across
/// mixed inline and overflow traffic.
fn route_notification(
    config: &PostgresConfig,
    delivery_tx: &mpsc::Sender<DeliverySlot>,
    stats: &Arc<PubSubStats>,
    overflow: Option<&Arc<PgSession>>,
    notification: &tokio_postgres::Notification,
) {
    let channel = notification.channel();

    let decoded = match decode_notice(&config.namespace, channel, notification.payload()) {
        Ok(decoded) => decoded,
        Err(e) => {
            stats.record_rejected();
            warn!(channel = %channel, error = %e, "Rejecting notification");
            return;
        }
    };

    let reference = match decoded {
        DecodedNotice::Inline(publication) => {
            enqueue(delivery_tx, stats, DeliverySlot::Ready(publication));
            return;
        }
        DecodedNotice::Overflow(reference) => reference,
    };

    stats.record_overflow();

    let Some(session) = overflow else {
        // A publisher relayed a payload this node cannot read back. Counted and
        // warned as a gap, and reported in order: the publication is lost, not
        // quietly skipped, and the stream's members are told so.
        stats.record_overflow_gap();
        let error =
            PubSubError::OverflowUnsupported { id: reference.id, stream: reference.stream.clone() };
        warn!(
            channel = %channel,
            %error,
            "Overflow publication lost: set overflow_enabled on this node (and install the \
             orbitcast_payloads migration) so relayed payloads can be fetched"
        );
        enqueue(
            delivery_tx,
            stats,
            DeliverySlot::Gap { stream: reference.stream, reason: error.to_string() },
        );
        return;
    };

    let (fetched_tx, fetched_rx) = oneshot::channel();
    // Reserve the ordered slot *before* the fetch starts, so a fast inline
    // publication cannot overtake a slower overflow one on the same stream.
    // Spawning only after the slot is accepted also bounds in-flight fetches by
    // the delivery queue's capacity - a burst cannot spawn without limit.
    let slot = DeliverySlot::Pending { stream: reference.stream.clone(), fetch: fetched_rx };
    if !enqueue(delivery_tx, stats, slot) {
        return;
    }

    let session = session.clone();
    let stats = stats.clone();
    let deadline = config.ack_timeout;
    tokio::spawn(async move {
        // The pending slot is gone only when its queue was dropped, i.e. the
        // backend shut down: there is no consumer left to hand this to.
        let _ = fetched_tx
            .send(fetch_overflow_publication(&session, &stats, reference, deadline).await);
    });
}

/// Fetch one relay row, or record the gap it represents.
///
/// Every failure mode - missing, expired, foreign, unreadable, or too slow -
/// returns `Err(reason)` after counting `overflow_gaps` and warning. The
/// consumer then reports exactly this gap, in order, and carries on with the
/// next slot.
async fn fetch_overflow_publication(
    session: &PgSession,
    stats: &PubSubStats,
    reference: OverflowRef,
    deadline: Duration,
) -> Result<Publication, String> {
    let gap = |reason: String| -> Result<Publication, String> {
        stats.record_overflow_gap();
        let error = PubSubError::OverflowGap {
            id: reference.id.clone(),
            stream: reference.stream.clone(),
            reason,
        };
        warn!(%error, "Delivery gap: the overflow payload could not be relayed");
        Err(error.to_string())
    };

    let fetched = match tokio::time::timeout(deadline, session.fetch_overflow(&reference)).await {
        Ok(Ok(Some(json_payload))) => json_payload,
        Ok(Ok(None)) => {
            return gap(format!(
                "no live {OVERFLOW_TABLE} row for this (id, namespace, stream): expired, \
                 already cleaned up, or never committed"
            ));
        }
        Ok(Err(e)) => return gap(e.to_string()),
        Err(_) => return gap(format!("fetch exceeded {deadline:?}")),
    };

    let publication = Publication {
        version: crate::protocol::v1::VERSION,
        namespace: reference.namespace.clone(),
        id: reference.id.clone(),
        stream: reference.stream.clone(),
        json_payload: fetched,
    };

    // The stored row is data, not a trusted envelope: validate it exactly like
    // an inline publication before it reaches a browser.
    if let Err(e) = publication.validate() {
        return gap(format!("stored payload failed validation: {e}"));
    }

    Ok(publication)
}

/// Place one slot in the bounded delivery queue, counting a full or closed
/// queue as a drop. Returns whether the slot was accepted.
fn enqueue(
    delivery_tx: &mpsc::Sender<DeliverySlot>,
    stats: &Arc<PubSubStats>,
    slot: DeliverySlot,
) -> bool {
    match delivery_tx.try_send(slot) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(dropped)) => {
            stats.record_delivery_drop();
            warn!(slot = describe_slot(&dropped), "Delivery queue full; dropping publication");
            false
        }
        Err(mpsc::error::TrySendError::Closed(dropped)) => {
            stats.record_delivery_drop();
            debug!(slot = describe_slot(&dropped), "No delivery consumer; dropping publication");
            false
        }
    }
}

/// Log-safe description of a dropped slot: identifiers only, never a payload.
fn describe_slot(slot: &DeliverySlot) -> String {
    match slot {
        DeliverySlot::Ready(publication) => {
            format!("inline {} ({})", publication.id, publication.stream)
        }
        DeliverySlot::Pending { stream, .. } => format!("pending overflow fetch ({stream})"),
        DeliverySlot::Gap { stream, .. } => format!("delivery gap ({stream})"),
    }
}

#[async_trait]
impl PubSub for PostgresPubSub {
    async fn publish(&self, stream: &str, json_payload: &str) -> Result<(), PubSubError> {
        self.publish_publication(&Publication::new(&self.config.namespace, stream, json_payload))
            .await
    }

    async fn publish_publication(&self, publication: &Publication) -> Result<(), PubSubError> {
        // Local delivery on this node happens through the listener only, so
        // publishing while the listener is down would lose local delivery
        // silently. Fail fast instead.
        if !self.is_ready() {
            return Err(PubSubError::NotReady);
        }

        let prepared = prepare_publication_notice(publication.clone(), self.config.overflow)?;

        match prepared.relay_row {
            // Inline: one statement, visible immediately.
            None => self.publisher.notify(&prepared.channel, &prepared.encoded).await,
            // Overflow: row and reference commit together, or neither does.
            Some(row) => {
                self.publisher
                    .publish_overflow(
                        &prepared.channel,
                        &prepared.encoded,
                        &row,
                        self.config.overflow.ttl,
                    )
                    .await
            }
        }
    }

    async fn subscribe(&self, stream: &str) -> Result<(), PubSubError> {
        let channel = channel_for(&self.config.namespace, stream);
        let stream_owned = stream.to_string();
        self.command("LISTEN", stream, move |ack| ListenerCommand::Subscribe {
            stream: stream_owned,
            channel,
            ack,
        })
        .await
    }

    async fn unsubscribe(&self, stream: &str) -> Result<(), PubSubError> {
        let channel = channel_for(&self.config.namespace, stream);
        let stream_owned = stream.to_string();
        self.command("UNLISTEN", stream, move |ack| ListenerCommand::Unsubscribe {
            stream: stream_owned,
            channel,
            ack,
        })
        .await
    }

    fn is_ready(&self) -> bool {
        *self.ready.borrow()
    }

    fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    /// The listener session `LISTEN`s on the namespaced control channel from
    /// the moment it is established (and again after every reconnect), so the
    /// consumer misses nothing it could have had. There is exactly one control
    /// consumer per backend.
    async fn take_control_stream(&self) -> Option<mpsc::Receiver<String>> {
        self.controls.lock().await.take()
    }

    async fn listen(&self, on_event: EventHandler) -> Result<(), PubSubError> {
        let mut deliveries =
            self.deliveries.lock().await.take().ok_or(PubSubError::ListenerAlreadyStarted)?;

        // One consumer, strict FIFO: awaiting a pending overflow fetch here is
        // exactly what holds later publications back until the earlier one is
        // resolved, preserving per-stream order. The wait is bounded by the
        // fetch deadline, and a gap is reported in that same order.
        while let Some(slot) = deliveries.recv().await {
            let event = match slot {
                DeliverySlot::Ready(publication) => PubSubEvent::Publication {
                    stream: publication.stream,
                    payload: Bytes::from(publication.json_payload),
                },
                DeliverySlot::Gap { stream, reason } => PubSubEvent::Gap { stream, reason },
                DeliverySlot::Pending { stream, fetch } => match fetch.await {
                    Ok(Ok(publication)) => PubSubEvent::Publication {
                        stream: publication.stream,
                        payload: Bytes::from(publication.json_payload),
                    },
                    // Gap: already counted and warned by the fetch task.
                    Ok(Err(reason)) => PubSubEvent::Gap { stream, reason },
                    Err(_) => {
                        // The fetch task vanished without answering.
                        self.stats.record_overflow_gap();
                        warn!(
                            stream,
                            "Delivery gap: an overflow fetch task ended without a result"
                        );
                        PubSubEvent::Gap {
                            stream,
                            reason: "overflow fetch task ended without a result".to_string(),
                        }
                    }
                },
            };

            on_event(event);
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod test_support;

#[cfg(test)]
mod tests;
