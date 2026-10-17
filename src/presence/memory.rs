//! In-process presence store.
//!
//! The memory counterpart of [`crate::presence::postgres`]: the same contract
//! ([`PresenceBackend`]) with the same grant-only identity, per-session leases,
//! subject-level transitions, expiry, and paged snapshots - held in this
//! process and nowhere else. It exists for the same reason
//! [`crate::pubsub::MemoryPubSub`] does: a `memory` build is single-node by
//! definition, and development and integration tests need the whole dispatch
//! flow (grant, join, roster event, snapshot, leave) without a database.
//!
//! Transitions are serialized by one mutex over the whole table, which is
//! stricter than the PostgreSQL store's per-membership lock and costs nothing
//! at this scale. Announcements go out after the lock is released, exactly as
//! the PostgreSQL store announces after its commit.
//!
//! Expiry is measured on this process's monotonic clock. A sweeper task
//! removes expired leases and announces the subjects that are now gone; a
//! lease that expired but was not swept yet is already invisible to every
//! read, so a late sweep delays reclamation rather than resurrecting anyone.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{Map, Value};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::presence::{
    HeldLease, Lease, MembershipKey, PresenceBackend, PresenceError, PresenceMetrics,
    PresencePublisher, PresenceSnapshot, Transition, TransitionOutcome, presence_stream_key,
};
use crate::protocol::v1::presence::{MAX_PAGE_SIZE, PresenceMember, PresenceMessage, PresencePage};
use crate::sync::lock;

/// One session's lease.
#[derive(Debug, Clone)]
struct Row {
    expires_at: Instant,
    metadata_json: String,
}

/// `stream -> subject_id -> session_id -> lease`, one namespace per store.
type Table = BTreeMap<String, BTreeMap<String, HashMap<String, Row>>>;

/// Aborts the sweeper when the store is dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug, Default)]
struct Counters {
    joins: AtomicU64,
    leaves: AtomicU64,
    announced: AtomicU64,
    refreshed: AtomicU64,
    swept: AtomicU64,
    snapshots: AtomicU64,
    publish_failures: AtomicU64,
}

/// In-process presence store.
pub struct MemoryPresence {
    namespace: String,
    publisher: Arc<dyn PresencePublisher>,
    table: Arc<Mutex<Table>>,
    counters: Arc<Counters>,
    _sweeper: AbortOnDrop,
}

impl MemoryPresence {
    /// Start a store for `namespace`, announcing through `publisher` and
    /// reclaiming expired leases every `sweep_interval`.
    ///
    /// Must be called inside a tokio runtime: the sweeper is spawned here and
    /// aborted when the store is dropped.
    #[must_use]
    pub fn start(
        namespace: impl Into<String>,
        publisher: Arc<dyn PresencePublisher>,
        sweep_interval: Duration,
    ) -> Arc<Self> {
        let table = Arc::new(Mutex::new(Table::new()));
        let counters = Arc::new(Counters::default());
        let sweeper = tokio::spawn(sweep_loop(
            Arc::clone(&table),
            Arc::clone(&publisher),
            Arc::clone(&counters),
            sweep_interval.max(Duration::from_millis(1)),
        ));
        Arc::new(Self {
            namespace: namespace.into(),
            publisher,
            table,
            counters,
            _sweeper: AbortOnDrop(sweeper),
        })
    }

    /// The namespace every lease is scoped to.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Run one sweep pass now (tests, and anything that cannot wait a tick).
    pub async fn sweep_once(&self) {
        sweep_once(&self.table, &*self.publisher, &self.counters).await;
    }

    /// Refuse a lease from another namespace: the store is one deployment.
    fn check_namespace(&self, key: &MembershipKey) -> Result<(), PresenceError> {
        key.validate()?;
        if key.namespace != self.namespace {
            return Err(PresenceError::Backend(anyhow::anyhow!(
                "presence lease for namespace {:?} on a store scoped to {:?}",
                key.namespace,
                self.namespace
            )));
        }
        Ok(())
    }
}

impl std::fmt::Debug for MemoryPresence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryPresence")
            .field("namespace", &self.namespace)
            .field("streams", &lock(&self.table).len())
            .finish_non_exhaustive()
    }
}

/// Live leases of `subject` on `stream`.
fn live_leases(table: &Table, stream: &str, subject: &str, now: Instant) -> usize {
    table
        .get(stream)
        .and_then(|subjects| subjects.get(subject))
        .map_or(0, |sessions| sessions.values().filter(|row| row.expires_at > now).count())
}

/// Distinct subjects with a live lease on `stream`.
fn live_subjects(table: &Table, stream: &str, now: Instant) -> u64 {
    table.get(stream).map_or(0, |subjects| {
        subjects
            .values()
            .filter(|sessions| sessions.values().any(|row| row.expires_at > now))
            .count() as u64
    })
}

/// Publish one transition, counting the outcome.
async fn announce(
    publisher: &dyn PresencePublisher,
    counters: &Counters,
    message: &PresenceMessage,
) {
    let json = match message.to_json() {
        Ok(json) => json,
        Err(e) => {
            counters.publish_failures.fetch_add(1, Ordering::Relaxed);
            warn!(error = %e, "Could not encode a presence transition");
            return;
        }
    };
    match publisher.publish_presence(&presence_stream_key(&message.stream), &json).await {
        Ok(()) => {
            counters.announced.fetch_add(1, Ordering::Relaxed);
            debug!(
                stream = %message.stream,
                event = %message.event,
                "presence transition announced"
            );
        }
        Err(e) => {
            counters.publish_failures.fetch_add(1, Ordering::Relaxed);
            warn!(stream = %message.stream, event = %message.event, error = %e,
                "Presence transition could not be announced; snapshots will reconcile");
        }
    }
}

#[async_trait]
impl PresenceBackend for MemoryPresence {
    fn metrics(&self) -> PresenceMetrics {
        PresenceMetrics {
            joins: self.counters.joins.load(Ordering::Relaxed),
            leaves: self.counters.leaves.load(Ordering::Relaxed),
            announced: self.counters.announced.load(Ordering::Relaxed),
            refreshed: self.counters.refreshed.load(Ordering::Relaxed),
            swept: self.counters.swept.load(Ordering::Relaxed),
            snapshots: self.counters.snapshots.load(Ordering::Relaxed),
            publish_failures: self.counters.publish_failures.load(Ordering::Relaxed),
        }
    }

    async fn join(&self, lease: &Lease) -> Result<TransitionOutcome, PresenceError> {
        self.check_namespace(&lease.key)?;
        self.counters.joins.fetch_add(1, Ordering::Relaxed);
        let key = &lease.key;

        let outcome = {
            let now = Instant::now();
            let mut table = lock(&self.table);
            let before = live_leases(&table, &key.stream, &key.subject_id, now);
            table
                .entry(key.stream.clone())
                .or_default()
                .entry(key.subject_id.clone())
                .or_default()
                .insert(
                    lease.session_id.clone(),
                    Row { expires_at: now + lease.ttl, metadata_json: lease.metadata_json.clone() },
                );
            let total = live_subjects(&table, &key.stream, now);
            drop(table);
            let transition =
                if before == 0 { Transition::FirstJoin } else { Transition::Refreshed };
            TransitionOutcome { transition, total }
        };

        if outcome.transition == Transition::FirstJoin {
            let metadata = serde_json::from_str::<Map<String, Value>>(&lease.metadata_json)
                .unwrap_or_default();
            let message =
                PresenceMessage::join(&key.stream, &key.subject_id, metadata, outcome.total);
            announce(&*self.publisher, &self.counters, &message).await;
        }
        Ok(outcome)
    }

    async fn leave(
        &self,
        key: &MembershipKey,
        session_id: &str,
    ) -> Result<TransitionOutcome, PresenceError> {
        self.check_namespace(key)?;
        self.counters.leaves.fetch_add(1, Ordering::Relaxed);

        let outcome = {
            let now = Instant::now();
            let mut table = lock(&self.table);
            let deleted = table
                .get_mut(&key.stream)
                .and_then(|subjects| subjects.get_mut(&key.subject_id))
                .and_then(|sessions| sessions.remove(session_id))
                .is_some();
            let remaining = live_leases(&table, &key.stream, &key.subject_id, now);
            prune(&mut table, &key.stream, &key.subject_id);
            let total = live_subjects(&table, &key.stream, now);
            drop(table);
            let transition = match (deleted, remaining) {
                (false, _) => Transition::Unchanged,
                (true, 0) => Transition::LastLeave,
                (true, _) => Transition::Reduced,
            };
            TransitionOutcome { transition, total }
        };

        if outcome.transition == Transition::LastLeave {
            let message = PresenceMessage::leave(&key.stream, &key.subject_id, outcome.total);
            announce(&*self.publisher, &self.counters, &message).await;
        }
        Ok(outcome)
    }

    async fn snapshot(
        &self,
        stream: &str,
        page: PresencePage,
    ) -> Result<PresenceSnapshot, PresenceError> {
        let limit = page.limit.clamp(1, MAX_PAGE_SIZE) as usize;
        let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
        self.counters.snapshots.fetch_add(1, Ordering::Relaxed);

        let now = Instant::now();
        let table = lock(&self.table);
        let Some(subjects) = table.get(stream) else {
            return Ok(PresenceSnapshot::default());
        };

        // Newest lease's metadata represents the subject, like the SQL store.
        let live: Vec<PresenceMember> = subjects
            .iter()
            .filter_map(|(subject_id, sessions)| {
                sessions
                    .values()
                    .filter(|row| row.expires_at > now)
                    .max_by_key(|row| row.expires_at)
                    .map(|row| PresenceMember {
                        subject_id: subject_id.clone(),
                        metadata: serde_json::from_str(&row.metadata_json).unwrap_or_default(),
                    })
            })
            .collect();
        drop(table);

        let total = live.len() as u64;
        let members: Vec<PresenceMember> = live.into_iter().skip(offset).take(limit).collect();
        let has_more = page.offset + (members.len() as u64) < total;
        Ok(PresenceSnapshot { members, total, has_more })
    }

    async fn refresh(&self, leases: &[HeldLease]) -> Result<u64, PresenceError> {
        let now = Instant::now();
        let mut table = lock(&self.table);
        let mut refreshed = 0;
        for held in leases {
            if held.key.namespace != self.namespace {
                continue;
            }
            if let Some(row) = table
                .get_mut(&held.key.stream)
                .and_then(|subjects| subjects.get_mut(&held.key.subject_id))
                .and_then(|sessions| sessions.get_mut(&held.session_id))
            {
                row.expires_at = now + held.ttl;
                refreshed += 1;
            }
        }
        self.counters.refreshed.fetch_add(refreshed, Ordering::Relaxed);
        Ok(refreshed)
    }

    fn invalidate(&self, _stream: &str) {
        // Reads come straight from the table: there is no cache to drop.
    }

    fn invalidate_all(&self) {}
}

/// Drop an empty subject and an empty stream after a removal.
fn prune(table: &mut Table, stream: &str, subject: &str) {
    if let Some(subjects) = table.get_mut(stream) {
        if subjects.get(subject).is_some_and(HashMap::is_empty) {
            subjects.remove(subject);
        }
        if subjects.is_empty() {
            table.remove(stream);
        }
    }
}

async fn sweep_loop(
    table: Arc<Mutex<Table>>,
    publisher: Arc<dyn PresencePublisher>,
    counters: Arc<Counters>,
    period: Duration,
) {
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // consume the immediate first tick
    loop {
        ticker.tick().await;
        sweep_once(&table, &*publisher, &counters).await;
    }
}

/// Remove expired leases and announce the subjects that are now gone.
async fn sweep_once(table: &Mutex<Table>, publisher: &dyn PresencePublisher, counters: &Counters) {
    let departed: Vec<PresenceMessage> = {
        let now = Instant::now();
        let mut table = lock(table);
        let mut departed = Vec::new();
        let mut swept = 0u64;

        for (stream, subjects) in table.iter_mut() {
            let mut gone = Vec::new();
            for (subject_id, sessions) in subjects.iter_mut() {
                let before = sessions.len();
                sessions.retain(|_, row| row.expires_at > now);
                swept += (before - sessions.len()) as u64;
                if before > sessions.len() && sessions.is_empty() {
                    gone.push(subject_id.clone());
                }
            }
            for subject_id in gone {
                subjects.remove(&subject_id);
                departed.push((stream.clone(), subject_id));
            }
        }
        table.retain(|_, subjects| !subjects.is_empty());
        counters.swept.fetch_add(swept, Ordering::Relaxed);

        let messages: Vec<PresenceMessage> = departed
            .into_iter()
            .map(|(stream, subject_id)| {
                let total = live_subjects(&table, &stream, now);
                PresenceMessage::leave(stream, subject_id, total)
            })
            .collect();
        drop(table);
        messages
    };

    for message in &departed {
        announce(publisher, counters, message).await;
    }
}

#[cfg(test)]
mod tests;
