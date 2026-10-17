//! Connection Hub
//!
//! Owns every WebSocket session and the subscription membership indexes that
//! address outgoing frames.
//!
//! # Membership model
//!
//! A *membership* is the triple `(conn_id, original identifier, stream)`:
//!
//! - forward index: `stream -> conn_id -> identifier` (who must be told about a
//!   publication, and under which identifier)
//! - reverse index: `conn_id -> identifier -> streams` (what a connection or a
//!   single subscription owns, for deterministic teardown)
//!
//! Backend (pub/sub) interest is derived from the forward index being
//! non-empty: [`Hub::join`] acquires it for the first member arriving and the
//! removal methods report the last member leaving. Interest is never inferred
//! from `tokio::sync::broadcast` receiver counts or from aborting tasks, both of
//! which race with delivery.
//!
//! Joins are serialized per stream, so a member arriving while the first
//! member's acquisition is still in flight waits for its outcome instead of
//! being confirmed onto a stream nobody listens to yet.
//!
//! # Readiness barrier
//!
//! A membership is installed **pending**: publications that arrive while the
//! backend acknowledgement and the confirmation frame are still on their way
//! are buffered per membership, in order, with a bound of
//! [`PENDING_BUFFER_FRAMES`] frames / [`PENDING_BUFFER_BYTES`] bytes. Once the
//! caller has enqueued `confirm_subscription` it calls [`Hub::activate`], which
//! flushes the buffer behind the confirmation on the same ordered queue - no
//! message frame ever precedes its confirmation. A buffer that overflows is
//! lag: the connection is closed with a reconnectable `disconnect` and the
//! event is counted.
//!
//! # Sessions are explicit
//!
//! Every mutation that could (re)create an entry for a connection -
//! [`Hub::join`], [`Hub::ensure_subscription`], [`Hub::set_identifier`] -
//! refuses a connection that is not in the reverse index with
//! [`HubError::UnknownSession`]. A session that was torn down therefore cannot
//! be resurrected by a command that was still in flight, and a caller holding
//! backend interest for it knows to release it.
//!
//! # Delivery and lag
//!
//! Fan-out routes through the indexes straight into the transport's
//! [`OutboundLink`]; there is no task per `(connection, stream)` pair, and a
//! frame is encoded once per distinct identifier and shared as [`Bytes`] by
//! every recipient. The link reports a bounded queue that could not take a
//! frame as [`LinkOutcome::Lagged`], and the hub then ends **that**
//! connection, once, with a reconnectable `disconnect`
//! ([`OutboundLink::lag_close`]) and counts it under
//! `orbitcast_socket_lag_disconnects_total{reason="queue"}`. A connection
//! being closed this way receives nothing more; the close travels where a full
//! queue cannot lose it, so a lost frame is never silent for the client.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use tokio::sync::{Mutex, OwnedMutexGuard, mpsc};
use tracing::{debug, warn};

use crate::actioncable::{self, ServerMessage};
use crate::encoder;
use crate::metrics::{LagReason, Metrics};
use crate::session::Session;
use crate::transport::docking::{Disembark, DockingLink, Outgoing};
use crate::transport::{LinkOutcome, OutboundLink};

/// Frames one pending membership may buffer before it counts as lag.
pub const PENDING_BUFFER_FRAMES: usize = 64;

/// Bytes one pending membership may buffer before it counts as lag.
pub const PENDING_BUFFER_BYTES: usize = 256 * 1024;

/// RFC 6455 close code 1000: a normal, intended close.
const CLOSE_NORMAL: u16 = 1000;

/// The close reason reported when a bounded buffer overflowed.
pub const CLOSE_REASON_LAG: &str = "server_restart";

/// Why a hub mutation was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HubError {
    /// The connection has no session (it was never added, or it was torn
    /// down). Nothing was installed; a caller holding backend interest it
    /// acquired for this join must release it.
    #[error("connection {0} has no session")]
    UnknownSession(u32),
    /// The backend refused interest in the stream; the membership was rolled
    /// back and no interest is held.
    #[error("backend refused interest in stream {0:?}")]
    Refused(String),
}

/// One confirmed subscription of a connection.
#[derive(Debug)]
struct Subscription {
    /// The client's original identifier, pre-encoded as a JSON string literal.
    identifier_json: Arc<str>,
    /// Streams this subscription is a member of.
    streams: HashSet<String>,
}

impl Subscription {
    /// A record for `identifier` that holds no stream yet.
    fn new(identifier: &str) -> Self {
        Self { identifier_json: encoder::encode_identifier(identifier), streams: HashSet::new() }
    }
}

/// Deliveries held for a membership whose confirmation is not enqueued yet.
#[derive(Debug, Default)]
struct PendingBuffer {
    frames: VecDeque<Bytes>,
    bytes: usize,
    /// The bound was exceeded: the connection is being closed, and nothing
    /// more is kept.
    overflowed: bool,
}

impl PendingBuffer {
    /// Keep `frame` for the flush, or report that the bound is exceeded.
    fn push(&mut self, frame: Bytes) -> bool {
        if self.overflowed {
            return false;
        }
        if self.frames.len() >= PENDING_BUFFER_FRAMES
            || self.bytes.saturating_add(frame.len()) > PENDING_BUFFER_BYTES
        {
            self.overflowed = true;
            self.frames.clear();
            self.bytes = 0;
            return false;
        }
        self.bytes += frame.len();
        self.frames.push_back(frame);
        true
    }
}

/// One member of a stream in the forward index.
#[derive(Debug)]
struct Member {
    /// The pre-encoded identifier the frame is addressed with.
    identifier_json: Arc<str>,
    /// `Some` while the subscription awaits its confirmation.
    pending: Option<PendingBuffer>,
    /// This member is the first of its stream and its backend acquisition is
    /// still in flight: the hub holds no interest for the stream yet, so
    /// detaching it releases nothing.
    acquiring: bool,
}

/// Members of a stream: `conn_id -> (identifier -> member)`.
type StreamMembers = HashMap<u32, HashMap<String, Member>>;

/// Hub manages all sessions, memberships and addressed delivery
pub struct Hub {
    /// All sessions by connection ID
    sessions: DashMap<u32, Session>,

    /// Forward membership index: stream -> members
    stream_members: DashMap<String, StreamMembers>,

    /// Reverse membership index: `conn_id` -> identifier -> subscription
    subscriptions: DashMap<u32, HashMap<String, Subscription>>,

    /// Identifier to connection IDs (for multi-device)
    identifiers: DashMap<String, Vec<u32>>,

    /// The transport's outbound link the hub was built with (the docking
    /// queue, for a hub built through `Connections::new`).
    link: Arc<dyn OutboundLink>,

    /// A link installed afterwards with [`Hub::attach_link`], which takes
    /// precedence over `link`: the HTTP transport delivers directly and
    /// installs its socket registry here before it accepts a session.
    direct: OnceLock<Arc<dyn OutboundLink>>,

    /// Connections a reconnectable close has been issued for (lag, or a
    /// delivery gap) and whose teardown has not run yet. They receive nothing
    /// more, and they are counted and closed once. Cleared by
    /// [`Hub::remove_session`].
    closing: DashSet<u32>,

    /// Per-stream join gates (see [`Hub::join`]). An entry lives only while a
    /// join holds or awaits it.
    stream_gates: DashMap<String, Arc<Mutex<()>>>,

    /// Where lag disconnects are counted.
    metrics: Arc<Metrics>,
}

impl Hub {
    /// Create a hub that writes into a docking queue, with a private counter
    /// registry.
    #[must_use]
    pub fn new(outgoing_tx: mpsc::Sender<Outgoing>) -> Self {
        Self::with_metrics(outgoing_tx, Arc::new(Metrics::new()))
    }

    /// Create a hub that writes into a docking queue and counts into
    /// `metrics`.
    #[must_use]
    pub fn with_metrics(outgoing_tx: mpsc::Sender<Outgoing>, metrics: Arc<Metrics>) -> Self {
        let link = Arc::new(DockingLink::new(outgoing_tx, Arc::clone(&metrics)));
        Self::with_link(link, metrics)
    }

    /// Create a hub that delivers through `link` and counts into `metrics`.
    #[must_use]
    pub fn with_link(link: Arc<dyn OutboundLink>, metrics: Arc<Metrics>) -> Self {
        Self {
            sessions: DashMap::new(),
            stream_members: DashMap::new(),
            subscriptions: DashMap::new(),
            identifiers: DashMap::new(),
            link,
            direct: OnceLock::new(),
            closing: DashSet::new(),
            stream_gates: DashMap::new(),
            metrics,
        }
    }

    /// Replace the link the hub was built with, before any session exists.
    ///
    /// For a transport that is built through `Connections::new` (which hands
    /// the hub a docking queue sender) but delivers directly: the HTTP
    /// transport installs its socket registry here, so no frame of its ever
    /// meets a queue other than its own socket's.
    ///
    /// # Panics
    ///
    /// When a link was already attached, or a session was already registered:
    /// both are programming errors in the transport's construction order, and
    /// a frame that went to the wrong link is exactly the silent loss this
    /// prevents.
    pub fn attach_link(&self, link: Arc<dyn OutboundLink>) {
        assert!(self.sessions.is_empty(), "the outbound link must be attached before any session");
        assert!(self.direct.set(link).is_ok(), "the outbound link is attached once");
    }

    /// The link frames and closes go to.
    fn link(&self) -> &dyn OutboundLink {
        self.direct.get().unwrap_or(&self.link).as_ref()
    }

    /// Register a new session.
    ///
    /// Connection ids are reused (the docking transport numbers them per
    /// link), so a new session must start from a clean state: any entry still
    /// indexed under this id is stale, is removed first, and is counted.
    /// Returns the streams those stale entries were the last member of, whose
    /// backend interest the caller must release.
    pub fn add_session(&self, session: Session) -> Vec<String> {
        let conn_id = session.conn_id;

        let stale_memberships = self
            .subscriptions
            .get(&conn_id)
            .map_or(0, |subs| subs.values().map(|s| s.streams.len()).sum::<usize>());
        let stale_session = usize::from(self.sessions.contains_key(&conn_id));
        let released = if stale_memberships > 0 || stale_session > 0 {
            let stale = (stale_memberships + stale_session) as u64;
            warn!(conn_id, stale, "connection id reused with stale hub entries; clearing them");
            self.metrics.record_session_reuse_stale_entries(stale);
            self.remove_session(conn_id)
        } else {
            Vec::new()
        };

        debug!(conn_id, path = %session.path, "session registered");
        self.subscriptions.insert(conn_id, HashMap::new());
        self.sessions.insert(conn_id, session);
        released
    }

    /// Remove a session and every membership it owns.
    ///
    /// This is the single teardown path (normal close, error close, disembark,
    /// and drain). Returns the streams that lost their last member and whose
    /// backend interest the caller must release. Pending buffers are dropped
    /// with the memberships: a registration in flight is cancelled here.
    pub fn remove_session(&self, conn_id: u32) -> Vec<String> {
        let owned = self.subscriptions.remove(&conn_id).map(|(_, subs)| subs).unwrap_or_default();

        let mut released = Vec::new();
        for (identifier, subscription) in owned {
            for stream in subscription.streams {
                if self.detach_member(conn_id, &identifier, &stream) {
                    released.push(stream);
                }
            }
        }

        self.closing.remove(&conn_id);
        if let Some((_, session)) = self.sessions.remove(&conn_id) {
            if let Some(ref identifier) = session.identifier {
                self.identifiers.alter(identifier, |_, mut ids| {
                    ids.retain(|&id| id != conn_id);
                    ids
                });
                self.identifiers.remove_if(identifier, |_, ids| ids.is_empty());
            }

            debug!(conn_id, released = released.len(), "session removed");
        }

        released
    }

    /// Get session by connection ID
    #[must_use]
    pub fn get_session(&self, conn_id: u32) -> Option<dashmap::mapref::one::Ref<'_, u32, Session>> {
        self.sessions.get(&conn_id)
    }

    /// Get mutable session by connection ID
    #[must_use]
    pub fn get_session_mut(
        &self,
        conn_id: u32,
    ) -> Option<dashmap::mapref::one::RefMut<'_, u32, Session>> {
        self.sessions.get_mut(&conn_id)
    }

    /// All live connection IDs (snapshot, for drain)
    #[must_use]
    pub fn session_ids(&self) -> Vec<u32> {
        self.sessions.iter().map(|entry| *entry.key()).collect()
    }

    /// Whether `conn_id` already has a confirmed subscription for `identifier`
    #[must_use]
    pub fn has_subscription(&self, conn_id: u32, identifier: &str) -> bool {
        self.subscriptions.get(&conn_id).is_some_and(|subs| subs.contains_key(identifier))
    }

    /// Install `(conn_id, identifier)` in `stream` as a pending membership,
    /// running `acquire` (backend interest) first when the stream has no
    /// members yet.
    ///
    /// Joins of one stream are serialized: a member arriving while the first
    /// member's `acquire` is in flight waits for its outcome, so it is never
    /// confirmed onto a stream that is not being listened to yet. If that
    /// acquisition was refused, the waiter finds the stream empty again and
    /// becomes the first member itself.
    ///
    /// `acquire` is only polled for the first member. Dropping the join
    /// mid-acquire rolls back too. Re-joining an installed membership returns
    /// `Ok` without acquiring. On `Ok` the membership is **pending**: call
    /// [`Hub::activate`] once the confirmation is enqueued.
    ///
    /// # Errors
    ///
    /// - [`HubError::UnknownSession`] when the connection has no session -
    ///   before the join, or torn down while the acquisition was in flight.
    ///   In the latter case the acquisition succeeded and **the caller must
    ///   release the interest it acquired**: the hub holds no membership for
    ///   it any more.
    /// - [`HubError::Refused`] when `acquire` returned `false`: the membership
    ///   is rolled back - with its subscription record, when this join created
    ///   the record - and no interest is released, because none was taken.
    pub async fn join(
        &self,
        conn_id: u32,
        identifier: &str,
        stream: &str,
        acquire: impl Future<Output = bool>,
    ) -> Result<(), HubError> {
        let _gate = StreamGate::lock(&self.stream_gates, stream).await;

        let Some(installed) = self.install(conn_id, identifier, stream)? else {
            return Ok(());
        };
        if !installed.first {
            return Ok(());
        }

        let mut rollback = Rollback {
            hub: self,
            conn_id,
            identifier,
            stream,
            created_record: installed.created_record,
            armed: true,
        };
        let acquired = acquire.await;
        rollback.armed = !acquired;
        if !acquired {
            return Err(HubError::Refused(stream.to_string()));
        }

        // The interest is held now. If the session went away meanwhile, its
        // teardown found an acquiring member and released nothing: the caller
        // owns that release.
        if !self.finish_acquiring(conn_id, identifier, stream) {
            return Err(HubError::UnknownSession(conn_id));
        }
        Ok(())
    }

    /// Install the membership `(conn_id, identifier)` in `stream`, without
    /// touching backend interest, and activate it at once. [`Hub::join`] is
    /// the entry point; this is for tests that exercise the indexes alone.
    ///
    /// Returns `true` when the stream had no members before. Re-installing an
    /// existing membership is a no-op and returns `false`.
    #[cfg(test)]
    fn add_membership(&self, conn_id: u32, identifier: &str, stream: &str) -> bool {
        let installed = self
            .install(conn_id, identifier, stream)
            .expect("test session exists")
            .is_some_and(|installed| installed.first);
        self.finish_acquiring(conn_id, identifier, stream);
        self.activate(conn_id, identifier);
        installed
    }

    /// Install a membership; `Ok(None)` when it was already installed.
    fn install(
        &self,
        conn_id: u32,
        identifier: &str,
        stream: &str,
    ) -> Result<Option<Installed>, HubError> {
        // The reverse-index shard guard lives for this one statement only. A
        // connection without a reverse-index entry has no session: nothing is
        // created for it.
        let recorded = match self.subscriptions.get_mut(&conn_id) {
            Some(mut subs) => record_stream(&mut subs, identifier, stream),
            None => return Err(HubError::UnknownSession(conn_id)),
        };
        let Some((identifier_json, created_record)) = recorded else {
            debug!(conn_id, stream, "membership already installed");
            return Ok(None);
        };

        let first = {
            let mut members = self.stream_members.entry(stream.to_string()).or_default();
            let first = members.is_empty();
            members.entry(conn_id).or_default().insert(
                identifier.to_string(),
                Member {
                    identifier_json,
                    pending: Some(PendingBuffer::default()),
                    acquiring: first,
                },
            );
            first
        };

        debug!(conn_id, stream, first, "membership installed (pending)");
        Ok(Some(Installed { first, created_record }))
    }

    /// Mark a first member's acquisition as complete. Returns whether the
    /// membership still exists.
    fn finish_acquiring(&self, conn_id: u32, identifier: &str, stream: &str) -> bool {
        let Some(mut members) = self.stream_members.get_mut(stream) else {
            return false;
        };
        let Some(member) = members.get_mut(&conn_id).and_then(|conn| conn.get_mut(identifier))
        else {
            return false;
        };
        member.acquiring = false;
        true
    }

    /// Release the buffered deliveries of every pending membership of
    /// `(conn_id, identifier)`, in order, behind the confirmation the caller
    /// has already enqueued. Returns the number of frames flushed.
    ///
    /// Idempotent: an active membership has nothing to flush. A membership
    /// whose buffer overflowed is already being closed and flushes nothing.
    pub fn activate(&self, conn_id: u32, identifier: &str) -> usize {
        let streams: Vec<String> = match self.subscriptions.get(&conn_id) {
            Some(subs) => match subs.get(identifier) {
                Some(subscription) => subscription.streams.iter().cloned().collect(),
                None => return 0,
            },
            None => return 0,
        };

        let mut flushed = 0;
        for stream in streams {
            let Some(mut members) = self.stream_members.get_mut(&stream) else {
                continue;
            };
            let Some(member) = members.get_mut(&conn_id).and_then(|conn| conn.get_mut(identifier))
            else {
                continue;
            };
            let Some(buffer) = member.pending.take() else {
                continue;
            };
            if buffer.overflowed {
                continue;
            }
            // Under the stream's shard guard, so no delivery for this stream
            // can interleave between the flush and the state change above.
            for frame in buffer.frames {
                if self.try_send_frame(conn_id, frame) {
                    flushed += 1;
                }
            }
        }

        if flushed > 0 {
            debug!(conn_id, flushed, "pending deliveries released behind the confirmation");
        }
        flushed
    }

    /// Remove every membership held by `(conn_id, identifier)`.
    ///
    /// Other identifiers on the same connection keep their memberships, even
    /// when they share a stream. Returns the streams that lost their last
    /// member and whose backend interest the caller must release.
    pub fn remove_subscription(&self, conn_id: u32, identifier: &str) -> Vec<String> {
        let streams = {
            let Some(mut subs) = self.subscriptions.get_mut(&conn_id) else {
                return Vec::new();
            };
            let Some(subscription) = subs.remove(identifier) else {
                return Vec::new();
            };
            subscription.streams
        };

        let released: Vec<String> = streams
            .into_iter()
            .filter(|stream| self.detach_member(conn_id, identifier, stream))
            .collect();

        debug!(conn_id, released = released.len(), "subscription removed");
        released
    }

    /// Remove a single `(conn_id, identifier)` membership from one stream.
    ///
    /// The subscription itself survives with its remaining streams: this is
    /// `stop_stream_from`, not `unsubscribe`. Returns `true` when the stream
    /// lost its last member and the caller must release backend interest.
    pub fn remove_membership(&self, conn_id: u32, identifier: &str, stream: &str) -> bool {
        let held = {
            let Some(mut subs) = self.subscriptions.get_mut(&conn_id) else {
                return false;
            };
            let Some(subscription) = subs.get_mut(identifier) else {
                return false;
            };
            subscription.streams.remove(stream)
        };

        if !held {
            return false;
        }

        let released = self.detach_member(conn_id, identifier, stream);
        debug!(conn_id, stream, released, "membership removed");
        released
    }

    /// Remove every stream of one subscription, keeping the subscription.
    ///
    /// This is `stop_all_streams`: the client stays subscribed and keeps
    /// receiving `transmit`s, it just has no stream memberships left. Returns
    /// the streams that lost their last member.
    pub fn clear_streams(&self, conn_id: u32, identifier: &str) -> Vec<String> {
        let streams = {
            let Some(mut subs) = self.subscriptions.get_mut(&conn_id) else {
                return Vec::new();
            };
            let Some(subscription) = subs.get_mut(identifier) else {
                return Vec::new();
            };
            std::mem::take(&mut subscription.streams)
        };

        let released: Vec<String> = streams
            .into_iter()
            .filter(|stream| self.detach_member(conn_id, identifier, stream))
            .collect();

        debug!(conn_id, released = released.len(), "subscription streams cleared");
        released
    }

    /// Install an empty subscription so a channel with no streams still owns
    /// its identifier.
    ///
    /// Rails may accept a subscription that never calls `stream_from`; it must
    /// still be addressable by `transmit` and torn down like any other.
    ///
    /// # Errors
    ///
    /// [`HubError::UnknownSession`] when the connection has no session.
    pub fn ensure_subscription(&self, conn_id: u32, identifier: &str) -> Result<(), HubError> {
        let Some(mut subs) = self.subscriptions.get_mut(&conn_id) else {
            return Err(HubError::UnknownSession(conn_id));
        };
        subs.entry(identifier.to_string()).or_insert_with(|| Subscription::new(identifier));
        Ok(())
    }

    /// Drop one member from the forward index.
    ///
    /// Returns `true` when the stream is now empty (last member left), the
    /// stream entry has been dropped, **and** the hub held interest for it: a
    /// first member whose acquisition is still in flight leaves nothing to
    /// release (its `join` reports that to its own caller).
    fn detach_member(&self, conn_id: u32, identifier: &str, stream: &str) -> bool {
        let mut acquiring = false;
        if let Some(mut members) = self.stream_members.get_mut(stream)
            && let Some(conn_members) = members.get_mut(&conn_id)
        {
            if let Some(member) = conn_members.remove(identifier) {
                acquiring = member.acquiring;
            }
            if conn_members.is_empty() {
                members.remove(&conn_id);
            }
        }

        // Separate lookup: removing while a `get_mut` guard is alive deadlocks.
        let emptied =
            self.stream_members.remove_if(stream, |_, members| members.is_empty()).is_some();
        emptied && !acquiring
    }

    /// Number of memberships on a stream (across all connections).
    ///
    /// Membership count, not receiver count: this is the value backend interest
    /// is derived from. Test-only: no production path reads it.
    #[cfg(test)]
    #[must_use]
    pub fn stream_member_count(&self, stream: &str) -> usize {
        self.stream_members
            .get(stream)
            .map_or(0, |members| members.values().map(HashMap::len).sum())
    }

    /// Connections currently holding a membership of `stream`.
    #[must_use]
    pub fn stream_connections(&self, stream: &str) -> Vec<u32> {
        self.stream_members
            .get(stream)
            .map(|members| members.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Build one addressed frame per membership of `stream`.
    ///
    /// The payload is encoded once by the caller and spliced in verbatim here.
    /// Members whose identifiers are byte-identical share a single frame
    /// allocation.
    #[cfg(test)]
    fn addressed_frames(
        &self,
        stream: &str,
        payload: &[u8],
        exclude: Option<u32>,
    ) -> Vec<(u32, Bytes)> {
        let Some(members) = self.stream_members.get(stream) else {
            return Vec::new();
        };

        let mut frames = Vec::new();
        let mut encoded = FrameCache::default();
        for (conn_id, identifiers) in members.iter() {
            if exclude == Some(*conn_id) {
                continue;
            }
            for member in identifiers.values() {
                frames.push((*conn_id, encoded.frame(&member.identifier_json, payload)));
            }
        }
        frames
    }

    /// Deliver a publication to every membership of `stream`.
    ///
    /// `payload` must be a valid JSON value: it becomes the `message` field of
    /// each addressed frame. Returns the number of frames handed to the
    /// outbound queue; frames held for a pending membership are not counted
    /// until [`Hub::activate`] flushes them.
    ///
    /// Synchronous so pub/sub backends can call it from their delivery
    /// callback: frames are handed to the link without waiting, and a bounded
    /// queue that cannot take one is that connection's lag - it is closed with
    /// a reconnectable `disconnect` and counted, and the backend listener is
    /// never stalled. A pending buffer that overflows is lag the same way.
    pub fn deliver(&self, stream: &str, payload: &[u8]) -> usize {
        let mut sent = 0;
        let mut lagged: Vec<u32> = Vec::new();

        if let Some(mut members) = self.stream_members.get_mut(stream) {
            let mut encoded = FrameCache::default();
            for (conn_id, identifiers) in members.iter_mut() {
                for member in identifiers.values_mut() {
                    let frame = encoded.frame(&member.identifier_json, payload);
                    match member.pending {
                        Some(ref mut buffer) => {
                            if !buffer.push(frame) && !lagged.contains(conn_id) {
                                lagged.push(*conn_id);
                            }
                        }
                        None => {
                            if self.try_send_frame(*conn_id, frame) {
                                sent += 1;
                            }
                        }
                    }
                }
            }
        }

        for conn_id in lagged {
            self.close_for_lag(conn_id, LagReason::PendingBuffer);
        }

        debug!(stream, sent, "publication delivered");
        sent
    }

    /// Close `conn_id` for lag: counted once under `reason`, closed once.
    fn close_for_lag(&self, conn_id: u32, reason: LagReason) {
        if self.reconnect_close(conn_id) {
            self.metrics.record_socket_lag_disconnect(reason);
            warn!(conn_id, reason = reason.label(), "connection lagged; closing with reconnect");
        }
    }

    /// Close `conn_id` with a reconnectable `disconnect`, without blocking the
    /// caller and at most once per session. Returns whether this call issued
    /// the close.
    ///
    /// From here until its teardown the connection is delivered nothing more:
    /// what it has already missed cannot be recovered, and the close must not
    /// queue behind frames it will never read.
    fn reconnect_close(&self, conn_id: u32) -> bool {
        if !self.closing.insert(conn_id) {
            return false;
        }
        self.link().lag_close(
            Bytes::from(reconnect_disconnect_frame()),
            Disembark { conn_id, code: CLOSE_NORMAL, reason: CLOSE_REASON_LAG.to_string() },
        );
        true
    }

    /// Apply a link's verdict on one frame; `true` when it was sent.
    fn settle(&self, conn_id: u32, outcome: LinkOutcome) -> bool {
        match outcome {
            LinkOutcome::Sent => true,
            LinkOutcome::Lagged => {
                self.close_for_lag(conn_id, LagReason::Queue);
                false
            }
            LinkOutcome::Closed => {
                debug!(conn_id, "connection gone; frame dropped");
                false
            }
        }
    }

    /// Close every connection holding a membership of `stream` with a
    /// reconnectable `disconnect`: a delivery gap on that stream cannot be
    /// recovered, so its members must resubscribe. Returns how many were told.
    #[must_use]
    pub fn disconnect_members(&self, stream: &str) -> usize {
        let targets = self.stream_connections(stream);
        for &conn_id in &targets {
            self.reconnect_close(conn_id);
        }
        targets.len()
    }

    /// Deliver to every membership of `stream` except those on
    /// `exclude_conn_id` (whisper semantics), awaiting link capacity where
    /// the link has a shared queue to wait on.
    pub async fn deliver_excluding(
        &self,
        stream: &str,
        payload: &[u8],
        exclude_conn_id: u32,
    ) -> usize {
        // Frames are materialized before the first await: no DashMap guard may
        // be held across one.
        let frames: Vec<(u32, Bytes)> = {
            let Some(members) = self.stream_members.get(stream) else {
                return 0;
            };
            let mut encoded = FrameCache::default();
            let mut frames = Vec::new();
            for (conn_id, identifiers) in members.iter() {
                if *conn_id == exclude_conn_id {
                    continue;
                }
                for member in identifiers.values() {
                    if member.pending.is_none() {
                        frames.push((*conn_id, encoded.frame(&member.identifier_json, payload)));
                    }
                }
            }
            frames
        };

        let mut sent = 0;
        for (conn_id, frame) in frames {
            if self.closing.contains(&conn_id) {
                continue;
            }
            let outcome = self.link().frame(conn_id, frame).await;
            if self.settle(conn_id, outcome) {
                sent += 1;
            }
        }

        debug!(stream, sent, excluded = exclude_conn_id, "whisper delivered");
        sent
    }

    /// Send an identical frame to every session (heartbeat).
    ///
    /// Pings carry no identifier, so there is nothing to address and no
    /// per-session forwarding task: one shared frame goes to every session's
    /// link.
    pub fn broadcast_ping(&self, payload: &[u8]) -> usize {
        let frame = Bytes::copy_from_slice(payload);
        let mut sent = 0;

        for entry in &self.sessions {
            if self.try_send_frame(*entry.key(), frame.clone()) {
                sent += 1;
            }
        }

        debug!(sent, "ping broadcast sent");
        sent
    }

    /// Send message to a specific connection
    pub async fn send(&self, conn_id: u32, payload: &[u8]) {
        self.send_bytes(conn_id, Bytes::copy_from_slice(payload)).await;
    }

    /// Send an already-shared frame to a specific connection, awaiting link
    /// capacity where the link has a shared queue to wait on. A connection
    /// already being closed for lag receives nothing.
    pub async fn send_bytes(&self, conn_id: u32, frame: Bytes) {
        if self.closing.contains(&conn_id) {
            return;
        }
        let outcome = self.link().frame(conn_id, frame).await;
        self.settle(conn_id, outcome);
    }

    /// Hand one frame to the link without waiting; `true` when it was sent.
    ///
    /// The only bound a frame can meet here is the one its own connection
    /// depends on, and meeting it is that connection's lag: it is closed with
    /// a reconnectable `disconnect`, once, and counted. A connection already
    /// being closed is skipped.
    fn try_send_frame(&self, conn_id: u32, frame: Bytes) -> bool {
        if self.closing.contains(&conn_id) {
            return false;
        }
        let outcome = self.link().try_frame(conn_id, frame);
        self.settle(conn_id, outcome)
    }

    /// Disconnect a specific connection in order: frames already queued for
    /// it are written first (server-initiated close).
    pub async fn disconnect(&self, conn_id: u32, code: u16, reason: &str) {
        self.link().disconnect(Disembark { conn_id, code, reason: reason.to_string() }).await;
    }

    /// Set identifier for a session (after authentication)
    ///
    /// # Errors
    ///
    /// [`HubError::UnknownSession`] when the connection has no session; the
    /// identity index is left untouched.
    pub fn set_identifier(&self, conn_id: u32, identifier: String) -> Result<(), HubError> {
        {
            let Some(mut session) = self.sessions.get_mut(&conn_id) else {
                return Err(HubError::UnknownSession(conn_id));
            };
            session.set_identifier(identifier.clone());
        }

        self.identifiers.entry(identifier).or_default().push(conn_id);
        Ok(())
    }

    /// Get all connection IDs for an identifier
    #[must_use]
    pub fn get_connections_by_identifier(&self, identifier: &str) -> Vec<u32> {
        self.identifiers.get(identifier).map(|entry| entry.clone()).unwrap_or_default()
    }

    /// Get session count
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Number of streams with at least one membership. Test-only.
    #[cfg(test)]
    #[must_use]
    pub fn stream_count(&self) -> usize {
        self.stream_members.len()
    }

    /// Stale entries counted on reused ids so far (tests).
    #[cfg(test)]
    fn metrics_snapshot_session_reuse(&self) -> u64 {
        let scrape = crate::metrics::render(crate::metrics::Exposition {
            metrics: &self.metrics,
            gauges: crate::metrics::Gauges::default(),
            pubsub: None,
            presence: None,
            broker: None,
        });
        scrape
            .lines()
            .find_map(|line| line.strip_prefix("orbitcast_session_reuse_stale_entries_total "))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    /// Confirmed subscriptions across every session.
    ///
    /// One session holds one entry per identifier, so this is the number of
    /// `(connection, identifier)` pairs - not the number of streams, which a
    /// subscription may hold several of. Exported as a gauge, so it walks the
    /// reverse index rather than maintaining a counter that could drift.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.iter().map(|entry| entry.value().len()).sum()
    }
}

impl std::fmt::Debug for Hub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hub")
            .field("sessions", &self.sessions.len())
            .field("streams", &self.stream_members.len())
            .field("identifiers", &self.identifiers.len())
            .field("closing", &self.closing.len())
            .finish_non_exhaustive()
    }
}

/// The standard reconnectable `disconnect` frame a lagging or gapped
/// connection is sent before its close.
fn reconnect_disconnect_frame() -> Vec<u8> {
    actioncable::encode(&ServerMessage::Disconnect {
        reason: CLOSE_REASON_LAG.to_string(),
        reconnect: true,
    })
}

/// Frames encoded during one fan-out, keyed by pre-encoded identifier, so
/// every recipient sharing an identifier shares one allocation.
#[derive(Default)]
struct FrameCache(HashMap<Arc<str>, Bytes>);

impl FrameCache {
    fn frame(&mut self, identifier_json: &Arc<str>, payload: &[u8]) -> Bytes {
        if let Some(frame) = self.0.get(identifier_json) {
            return frame.clone();
        }
        let frame = encoder::addressed_frame(identifier_json, payload);
        self.0.insert(Arc::clone(identifier_json), frame.clone());
        frame
    }
}

/// Add `stream` to `identifier`'s record in one connection's reverse index,
/// creating the record when it does not exist yet.
///
/// `None` when the record already held `stream`; otherwise the pre-encoded
/// identifier and whether the record was created by this call.
fn record_stream(
    subs: &mut HashMap<String, Subscription>,
    identifier: &str,
    stream: &str,
) -> Option<(Arc<str>, bool)> {
    let mut created_record = false;
    let subscription = subs.entry(identifier.to_string()).or_insert_with(|| {
        created_record = true;
        Subscription::new(identifier)
    });

    subscription
        .streams
        .insert(stream.to_string())
        .then(|| (Arc::clone(&subscription.identifier_json), created_record))
}

/// What [`Hub::install`] did, so a refused join can undo exactly that.
struct Installed {
    /// The stream had no members before: backend interest must be acquired.
    first: bool,
    /// The subscription record did not exist before this install.
    created_record: bool,
}

/// One stream's join gate, held for the duration of a [`Hub::join`].
struct StreamGate<'a> {
    gates: &'a DashMap<String, Arc<Mutex<()>>>,
    stream: &'a str,
    guard: Option<OwnedMutexGuard<()>>,
}

impl<'a> StreamGate<'a> {
    async fn lock(gates: &'a DashMap<String, Arc<Mutex<()>>>, stream: &'a str) -> Self {
        // Clone out of the map first: no shard lock may be held across the await.
        let gate = Arc::clone(&gates.entry(stream.to_string()).or_default());
        Self { gates, stream, guard: Some(gate.lock_owned().await) }
    }
}

impl Drop for StreamGate<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        // Only the map's own reference left: nobody holds or awaits this gate.
        self.gates.remove_if(self.stream, |_, gate| Arc::strong_count(gate) == 1);
    }
}

/// Undoes a first member's install unless its acquisition succeeded.
///
/// Runs under the stream's gate, so the stream it empties is the one this
/// join found empty: there is no other member, and no interest to release.
struct Rollback<'a> {
    hub: &'a Hub,
    conn_id: u32,
    identifier: &'a str,
    stream: &'a str,
    created_record: bool,
    armed: bool,
}

impl Drop for Rollback<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        warn!(
            conn_id = self.conn_id,
            stream = self.stream,
            "Backend interest not acquired; rolling the membership back"
        );
        // The returned "stream emptied" reports are deliberately not released:
        // this join never acquired the stream's interest.
        if self.created_record {
            self.hub.remove_subscription(self.conn_id, self.identifier);
        } else {
            self.hub.remove_membership(self.conn_id, self.identifier, self.stream);
        }
    }
}

#[cfg(test)]
mod tests;
