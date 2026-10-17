//! Opt-in recovery: authorized replay of retained history.
//!
//! Everything here hangs off **one** reserved subscription,
//! `Orbitcast::RecoveryChannel`. An unchanged ActionCable or Turbo client never
//! subscribes to it, never receives a [`DeliveryEnvelope`], and keeps exactly
//! the plain live-delivery behavior it has today. Recovery is not a mode the
//! server is in; it is a subscription a client opts into.
//!
//! ```text
//! subscribe  {"channel":"Orbitcast::RecoveryChannel","token":"<recovery token>"}
//!            -> verify (dedicated key, purpose "recover") -> confirm_subscription
//! perform    {"action":"orbitcast_recovery","op":"resume","cursors":{...}}
//!            -> fence -> replay_start -> retained rows -> replay_done -> live
//! perform    {"action":"orbitcast_recovery","op":"ack","cursors":{...}}
//!            -> best-effort session snapshot
//! ```
//!
//! # Its own key, its own purpose
//!
//! A recovery token is verified by [`RecoveryVerifier`], which is keyed from
//! `ORBITCAST_RECOVERY_SECRET_BASE64` and **nothing else**. It is deliberately
//! a separate verifier from [`crate::streams::ClaimsVerifier`]: there is no
//! code path in which the streams secret can verify a recovery token or the
//! recovery secret can verify a subscribe token, even by accident. The claims
//! additionally carry `purpose: "recover"` and the exact stream set they grant,
//! so a recovery token is a capability Rails minted, not a mode that widens an
//! existing subscription.
//!
//! Because the token is its own authorization, recovery works in signed-only
//! deployments as well as Rails-dispatch ones. Recovery has no dispatch
//! requirement - unlike presence, whose identity can only come from a Rails
//! grant.
//!
//! # A token is necessary, never sufficient
//!
//! Every `resume` re-decides authorization from scratch ([`ResumeRefusal`]),
//! so a recovery token cannot resurrect revoked access:
//!
//! 1. the connection must be **identified** - in Rails-dispatch mode that is
//!    the `welcome` Rails granted, in signed mode every accepted socket is;
//! 2. the token must still be valid *now* (expiry is re-checked, not just at
//!    subscribe time);
//! 3. each stream is replayed only while this connection holds an **active
//!    subscription membership** of it - the same membership ordinary delivery
//!    uses. A stream whose subscribe was rejected on reconnect, or that was
//!    unsubscribed since, is reported as [`GapReason::Unauthorized`] and
//!    neither replayed nor tailed; a tail that loses its membership later stops
//!    the same way.
//!
//! The practical consequence for a client: subscribe to the application
//! streams first, then create the recovery subscription (the shipped helper
//! resumes on `connected`, and ActionCable confirms subscriptions in creation
//! order).
//!
//! # Fencing is per session
//!
//! `resume` starts with an atomic `UPDATE ... RETURNING` on the session row
//! keyed by the token's `session_id` claim. The returned lease generation
//! strictly increases, so of two concurrent resumes of one *session* - on this
//! node or on another one, with this token or a refreshed one - exactly one
//! holds the current lease. The loser's replay is cancelled and its streams
//! report [`GapReason::Fenced`]: two nodes can never restore one session at
//! once, and two tokens for one session cannot run side by side.
//!
//! A session row lives `session_ttl` past the session's last activity: a
//! resume and an ack write it, and a live tail touches it at most every
//! [`SESSION_TOUCH_INTERVAL`] (or a quarter of the TTL, whichever is shorter),
//! so a quiet but connected session never fences itself out when the row
//! would otherwise expire.
//!
//! # Replay, then live, with one ordered path
//!
//! ```text
//! resume ->  fence
//!        ->  capture watermark W per stream
//!        ->  start the live tail at W (it *buffers*, bounded)
//!        ->  replay (cursor, W] in order
//!        ->  replay_done
//!        ->  flip to live: the tail drains its buffer, deduplicated
//! ```
//!
//! Both phases deliver through the same per-session path and the same
//! deduplication window, so the handoff cannot skip a sequence and a message
//! delivered twice is dropped by `publication_id`. A tail that fills its
//! bounded buffer stops buffering and reports [`GapReason::BufferOverflow`]:
//! live traffic outran the replay, and an application resync is the honest
//! answer rather than an unbounded queue.
//!
//! Sequences are contiguous by construction, so both phases treat a jump - or
//! a batch that comes back empty before the watermark - as rows that were
//! expired or trimmed underneath them: an [`GapReason::Expired`] gap, never a
//! silent skip.
//!
//! # Server-side cursor memory
//!
//! A `resume` that carries **no cursors at all** asks the store for the
//! session's last acknowledged snapshot
//! ([`crate::protocol::v1::recovery::SessionSnapshot`]) and resumes from it: a
//! page reload that lost its in-memory cursors still continues where its last
//! `ack` left off, as long as the session row is alive. A resume *with*
//! cursors trusts the client's, and a session with no snapshot starts at the
//! live edge.
//!
//! # Deliveries are reads, not fanout
//!
//! A recovery subscription installs **no hub membership** for the application
//! streams. Every delivery is read from retained history, in sequence order, by
//! this session's own tail. The ordinary fanout path is untouched, which is
//! what keeps stock clients byte-identical; the node still holds pub/sub
//! interest in the stream, because a notification is the wakeup that tells a
//! tail to look again.
//!
//! # At-least-once
//!
//! An `ack` records receipt. It is not proof that the browser applied anything,
//! it is never a precondition for delivery, and it never moves the fencing
//! generation. Within retention, delivery is at-least-once with a stable
//! `publication_id`; outside retention there is a gap and the application
//! resynchronizes.
//!
//! # Counters
//!
//! Replayed deliveries, gaps, and reconciliation passes are recorded on the
//! broker's own registry ([`crate::broker::BrokerStats`]), so one scrape shows
//! the store and the readers side by side (`orbitcast_broker_*`).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use hmac::{Hmac, KeyInit, Mac};
use serde::Deserialize;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::actioncable::{self, ClientCommand, ServerMessage};
use crate::broker::{Broker, BrokerConfig, REPLAY_BATCH, RetainedMessage, SESSION_TOUCH_INTERVAL};
use crate::hub::Hub;
use crate::protocol::ProtocolError;
use crate::protocol::v1::now_unix;
use crate::protocol::v1::recovery::{
    Cursor, DeliveryEnvelope, GapReason, RECOVERY_CHANNEL, RecoveryClaims, RecoveryCommand,
    RecoveryMessage, RecoveryOp, SessionSnapshot, is_recovery_action,
};
use crate::streams::{MAX_TOKEN_LEN, SecretBytes, VerifyError};
use crate::sync::lock;

type HmacSha256 = Hmac<Sha256>;

/// Publication ids remembered per session for deduplication.
///
/// Four batches' worth: enough to cover a replay-to-live handoff and a
/// reconnect storm, small enough that the window is a fixed cost per session.
const DEDUP_WINDOW: usize = 4 * REPLAY_BATCH;

/// Wake channel depth per stream. A reader only needs to know that *something*
/// arrived, so a lagged receiver is not an error: it re-reads either way.
const WAKE_CHANNEL_DEPTH: usize = 16;

/// Backend interest a recovery session needs, without depending on the server
/// module (which depends on this one).
#[async_trait]
pub trait StreamInterest: Send + Sync {
    /// Hold pub/sub interest in `stream`, so notifications wake this node.
    async fn acquire(&self, stream: &str) -> bool;

    /// Drop interest in `streams`.
    async fn release(&self, streams: Vec<String>);
}

/// Wakes retained-stream readers when a publication lands on this node.
///
/// The notification is a wakeup, never the message: a reader always re-queries
/// committed rows after its own position, so a missed, dropped, or duplicated
/// wake costs latency at worst.
#[derive(Debug, Default)]
pub struct StreamWaker {
    channels: DashMap<String, broadcast::Sender<()>>,
}

impl StreamWaker {
    /// Create an empty waker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Announce that `stream` may have new retained rows.
    pub fn wake(&self, stream: &str) {
        let Some(sender) = self.channels.get(stream).map(|entry| entry.clone()) else {
            return;
        };
        if sender.send(()).is_err() {
            // Nobody is tailing this stream any more; stop tracking it so the
            // map cannot grow with every stream this node ever saw.
            self.channels.remove(stream);
        }
    }

    /// Subscribe to wakeups for `stream`.
    #[must_use]
    pub fn subscribe(&self, stream: &str) -> broadcast::Receiver<()> {
        self.channels
            .entry(stream.to_string())
            .or_insert_with(|| broadcast::channel(WAKE_CHANNEL_DEPTH).0)
            .subscribe()
    }
}

/// Verifier for recovery tokens: `base64(JSON(claims))--hex(HMAC-SHA256)`.
///
/// Same wire shape as the subscribe profile and deliberately a separate
/// implementation: this type is only ever constructed with the recovery key, so
/// no refactor can quietly let one profile's key verify the other's token
/// (the no-fallback-between-profiles rule, applied to the recovery key).
pub struct RecoveryVerifier {
    key: SecretBytes,
}

impl RecoveryVerifier {
    /// Create a verifier keyed with raw HMAC key bytes.
    #[must_use]
    pub const fn new(key: SecretBytes) -> Self {
        Self { key }
    }

    /// Verify, decode, and validate a recovery token at clock reading `now`.
    ///
    /// Order, all fail-closed:
    /// 1. constant-time HMAC over the encoded bytes,
    /// 2. strict base64 decode + tolerant serde parse,
    /// 3. version/namespace/session/streams/purpose/expiry validation,
    /// 4. namespace must equal the configured namespace.
    ///
    /// # Errors
    ///
    /// The [`VerifyError`] of the first failed step: an oversized token
    /// ([`VerifyError::TokenTooLong`]), a missing `--` separator or empty half
    /// ([`VerifyError::InvalidFormat`]), a signature mismatch
    /// ([`VerifyError::InvalidSignature`]), undecodable base64 or JSON
    /// ([`VerifyError::Base64`], [`VerifyError::Json`]), invalid or expired
    /// claims ([`VerifyError::Claims`]), or a token for another namespace
    /// ([`VerifyError::NamespaceMismatch`]).
    pub fn verify(
        &self,
        token: &str,
        namespace: &str,
        now: i64,
    ) -> Result<RecoveryClaims, VerifyError> {
        use base64::{Engine, engine::general_purpose::STANDARD};

        if token.len() > MAX_TOKEN_LEN {
            return Err(VerifyError::TokenTooLong { len: token.len(), max: MAX_TOKEN_LEN });
        }

        let (encoded, signature) = token.split_once("--").ok_or(VerifyError::InvalidFormat)?;
        if encoded.is_empty() || signature.is_empty() {
            return Err(VerifyError::InvalidFormat);
        }

        // Authenticate before interpreting anything inside the token.
        let mut mac =
            HmacSha256::new_from_slice(self.key.as_bytes()).map_err(|_| VerifyError::InvalidKey)?;
        mac.update(encoded.as_bytes());
        let expected = hex::encode(mac.finalize().into_bytes());
        let matches: bool = expected.as_bytes().ct_eq(signature.as_bytes()).into();
        if !matches {
            return Err(VerifyError::InvalidSignature);
        }

        let claims: RecoveryClaims = serde_json::from_slice(&STANDARD.decode(encoded)?)?;
        claims.validate_at(now)?;

        if claims.namespace != namespace {
            return Err(VerifyError::NamespaceMismatch {
                expected: namespace.to_string(),
                actual: claims.namespace,
            });
        }

        Ok(claims)
    }

    /// Produce a token the way the gem does (`gem/lib/orbitcast/recovery.rb`):
    /// strict base64 of the fixed-key-order JSON, `--`, lowercase-hex HMAC.
    ///
    /// The gem is the authoritative producer; this exists so fixtures and tests
    /// can generate equivalent tokens and prove the two agree byte for byte.
    ///
    /// # Panics
    ///
    /// Never for any input: encoding [`RecoveryClaims`] (strings, integers,
    /// and a list of strings) cannot fail, and HMAC-SHA256 accepts a key of
    /// any length. Both are asserted as invariants rather than returned.
    #[must_use]
    pub fn sign(&self, claims: &RecoveryClaims) -> String {
        use base64::{Engine, engine::general_purpose::STANDARD};

        let json = serde_json::to_string(claims).expect("claims serialize");
        let encoded = STANDARD.encode(json.as_bytes());
        let mut mac = HmacSha256::new_from_slice(self.key.as_bytes()).expect("hmac key");
        mac.update(encoded.as_bytes());
        format!("{encoded}--{}", hex::encode(mac.finalize().into_bytes()))
    }
}

/// Why a `resume` (or part of one) was refused.
///
/// Typed so the refusal is logged with its cause and tested by variant; on the
/// wire every one of them is a [`GapReason::Unauthorized`] gap, because the
/// client's only correct response is the same: resynchronize through the
/// ordinary subscription path.
#[derive(Debug, thiserror::Error)]
pub enum ResumeRefusal {
    /// The connection has not been identified (no `welcome` yet): in
    /// Rails-dispatch mode nothing may be replayed before Rails accepted the
    /// connection.
    #[error("the connection is not identified; recovery needs a welcomed connection")]
    Unidentified,
    /// The token that confirmed the subscription is no longer valid.
    #[error("recovery token is no longer valid: {0}")]
    TokenInvalid(#[source] ProtocolError),
    /// The connection holds no active subscription membership of this stream.
    #[error("stream {stream:?} is not an active subscription of this connection")]
    NotSubscribed { stream: String },
}

/// What one resume may replay, once re-authorized.
struct Authorized {
    /// Granted streams the connection currently subscribes to.
    granted: Vec<String>,
    /// Granted streams it does not, each with its refusal.
    refused: Vec<(String, ResumeRefusal)>,
}

/// One stream's replay plan: where to start and where the tail took over.
struct ReplayPlan {
    stream: String,
    start: Cursor,
    watermark: Cursor,
}

/// The recovery subscription's identifier.
#[derive(Debug, Deserialize)]
struct RecoveryIdentifier {
    #[serde(default)]
    channel: String,
    /// The signed recovery token. Absent defaults to empty, which verifies
    /// against nothing.
    #[serde(default)]
    token: String,
}

/// Whether `identifier` addresses the reserved recovery channel.
fn is_recovery_identifier(identifier: &str) -> bool {
    serde_json::from_str::<RecoveryIdentifier>(identifier)
        .is_ok_and(|parsed| parsed.channel == RECOVERY_CHANNEL)
}

/// Bounded "already delivered" window, keyed by publication id.
#[derive(Debug)]
struct DedupWindow {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl DedupWindow {
    fn new() -> Self {
        Self { seen: HashSet::new(), order: VecDeque::new() }
    }

    /// Record `id`; `false` means it was already delivered.
    fn accept(&mut self, id: &str) -> bool {
        if !self.seen.insert(id.to_string()) {
            return false;
        }
        self.order.push_back(id.to_string());
        if self.order.len() > DEDUP_WINDOW
            && let Some(evicted) = self.order.pop_front()
        {
            self.seen.remove(&evicted);
        }
        true
    }
}

/// Aborts a tail task when its session drops it.
struct PumpHandle(JoinHandle<()>);

impl Drop for PumpHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Everything a tail task needs, shared by every session on this node.
struct Shared {
    namespace: String,
    broker: Arc<dyn Broker>,
    hub: Arc<Hub>,
    waker: Arc<StreamWaker>,
    config: BrokerConfig,
    /// How often a live tail extends the session row's expiry.
    touch_interval: Duration,
    /// `session id -> newest lease claimed on this node`.
    ///
    /// The in-process half of fencing: it cancels a losing replay immediately
    /// instead of waiting for the next store read.
    leases: DashMap<String, u64>,
}

impl Shared {
    /// Whether `conn_id` currently holds a subscription membership of
    /// `stream`: the authorization a replay or a tail needs, re-read from the
    /// hub rather than remembered from the token.
    fn is_member(&self, conn_id: u32, stream: &str) -> bool {
        self.hub.stream_connections(stream).contains(&conn_id)
    }
}

/// One live recovery subscription.
struct RecoverySession {
    conn_id: u32,
    /// The exact identifier every frame of this session is addressed with.
    identifier: String,
    claims: RecoveryClaims,
    /// The lease this session holds; a higher one elsewhere fences it out.
    lease: AtomicU64,
    /// True while replay is in flight: tails buffer instead of delivering.
    replaying: AtomicBool,
    /// Wakes tails the instant replay finishes.
    flip: Notify,
    /// Last position delivered per stream.
    cursors: Mutex<BTreeMap<String, String>>,
    delivered: Mutex<DedupWindow>,
    pumps: Mutex<HashMap<String, PumpHandle>>,
    /// When the session row's expiry was last extended, shared by every tail
    /// of this session so the writes stay bounded per session, not per stream.
    last_touch: Mutex<Instant>,
}

impl RecoverySession {
    fn new(conn_id: u32, identifier: String, claims: RecoveryClaims) -> Self {
        Self {
            conn_id,
            identifier,
            claims,
            lease: AtomicU64::new(0),
            replaying: AtomicBool::new(false),
            flip: Notify::new(),
            cursors: Mutex::new(BTreeMap::new()),
            delivered: Mutex::new(DedupWindow::new()),
            pumps: Mutex::new(HashMap::new()),
            last_touch: Mutex::new(Instant::now()),
        }
    }

    /// The fencing key: the session identity the token named.
    fn session_id(&self) -> &str {
        &self.claims.session_id
    }

    /// Whether a newer lease has been claimed for this session.
    fn fenced(&self, shared: &Shared) -> bool {
        shared
            .leases
            .get(self.session_id())
            .is_some_and(|newest| *newest != self.lease.load(Ordering::Acquire))
    }

    /// Claim the next touch slot if one is due: `true` at most once per
    /// `interval`, whichever tail asks first.
    fn touch_due(&self, interval: Duration) -> bool {
        let mut last = lock(&self.last_touch);
        if last.elapsed() < interval {
            return false;
        }
        *last = Instant::now();
        true
    }

    /// Extend the session row's expiry from this tail, when due.
    async fn keep_alive(&self, shared: &Shared) {
        if !self.touch_due(shared.touch_interval) {
            return;
        }
        if let Err(e) = shared.broker.touch_session(self.session_id()).await {
            // The next due touch retries; only a TTL's worth of consecutive
            // failures lets the row expire, and that shows up as a fenced gap.
            debug!(conn_id = self.conn_id, error = %e, "Could not extend the session row");
        }
    }

    fn cursor_snapshot(&self) -> BTreeMap<String, String> {
        lock(&self.cursors).clone()
    }

    /// Deliver one retained message, unless it was already delivered.
    async fn deliver(&self, shared: &Shared, message: &RetainedMessage) -> bool {
        {
            let mut delivered = lock(&self.delivered);
            if !delivered.accept(&message.publication_id) {
                debug!(
                    conn_id = self.conn_id,
                    stream = %message.stream,
                    publication_id = %message.publication_id,
                    "Skipping a publication this session already received"
                );
                return false;
            }
        }

        let envelope = DeliveryEnvelope::new(
            &message.stream,
            &message.publication_id,
            message.position,
            &message.json_payload,
        );

        lock(&self.cursors).insert(message.stream.clone(), message.position.encode());

        self.send(shared, &envelope).await;
        true
    }

    /// Send one already-built envelope as this subscription's `message` body.
    async fn send<T: serde::Serialize + Sync>(&self, shared: &Shared, body: &T) {
        let Ok(message) = serde_json::to_value(body) else {
            warn!(conn_id = self.conn_id, "Could not encode a recovery message; dropping it");
            return;
        };

        let frame = actioncable::encode(&ServerMessage::Message {
            identifier: self.identifier.clone(),
            message,
        });
        shared.hub.send(self.conn_id, &frame).await;
    }

    /// Report that a stream cannot be replayed. The client resyncs; nothing is
    /// invented to fill the hole.
    async fn gap(&self, shared: &Shared, stream: &str, reason: GapReason) {
        warn!(
            conn_id = self.conn_id,
            stream = %stream,
            reason = %reason,
            "Recovery gap: the application must resynchronize this stream"
        );
        shared.broker.counters().record_gap();
        self.send(shared, &RecoveryMessage::gap(stream, reason)).await;
    }

    /// Refuse one stream of a resume, with the typed cause in the log.
    async fn refuse(&self, shared: &Shared, stream: &str, refusal: &ResumeRefusal) {
        debug!(conn_id = self.conn_id, stream = %stream, refusal = %refusal, "Resume refused");
        self.gap(shared, stream, GapReason::Unauthorized).await;
    }

    /// Stop every tail. Used by a repeat resume and by teardown.
    fn stop_pumps(&self) {
        lock(&self.pumps).clear();
    }
}

/// Recovery: token verification, fencing, replay, and live tails.
pub struct RecoveryController {
    shared: Arc<Shared>,
    verifier: RecoveryVerifier,
    interest: Arc<dyn StreamInterest>,
    sessions: DashMap<u32, Arc<RecoverySession>>,
}

impl RecoveryController {
    /// Build a controller for `namespace`.
    pub fn new(
        namespace: String,
        secret: SecretBytes,
        broker: Arc<dyn Broker>,
        config: BrokerConfig,
        hub: Arc<Hub>,
        waker: Arc<StreamWaker>,
        interest: Arc<dyn StreamInterest>,
    ) -> Arc<Self> {
        info!(
            namespace = %namespace,
            retention_secs = config.retention.as_secs(),
            replay_buffer = config.replay_buffer,
            "Opt-in recovery enabled on {RECOVERY_CHANNEL}"
        );

        // A short session TTL needs more frequent touches than the default
        // cadence, or a live session would expire between two of them.
        let touch_interval = (config.session_ttl / 4).min(SESSION_TOUCH_INTERVAL);

        Arc::new(Self {
            shared: Arc::new(Shared {
                namespace,
                broker,
                hub,
                waker,
                config,
                touch_interval,
                leases: DashMap::new(),
            }),
            verifier: RecoveryVerifier::new(secret),
            interest,
            sessions: DashMap::new(),
        })
    }

    /// Whether this command belongs to the recovery channel.
    ///
    /// Checked before any mode-specific routing: a recovery subscription is
    /// authorized by its own token, so it never reaches Rails dispatch and
    /// never reaches the signed-stream controller.
    #[must_use]
    pub fn handles(&self, command: &ClientCommand) -> bool {
        let identifier = match command {
            ClientCommand::Subscribe { identifier }
            | ClientCommand::Unsubscribe { identifier }
            | ClientCommand::Message { identifier, .. }
            | ClientCommand::Join { identifier, .. }
            | ClientCommand::Leave { identifier }
            | ClientCommand::Presence { identifier }
            | ClientCommand::Whisper { identifier, .. } => identifier,
        };
        is_recovery_identifier(identifier)
    }

    /// Apply one recovery-channel command.
    ///
    /// `identified` is whether the connection has been welcomed: always true
    /// for a signed-mode socket, and only once Rails accepted the connection
    /// in dispatch mode. An unidentified connection cannot subscribe to the
    /// channel and cannot resume on it.
    pub async fn handle(&self, conn_id: u32, command: ClientCommand, identified: bool) {
        let welcomed = identified;
        match command {
            ClientCommand::Subscribe { identifier } => {
                if !welcomed {
                    debug!(conn_id, "Rejecting recovery subscribe before identification");
                    return self.reject(conn_id, identifier).await;
                }
                self.subscribe(conn_id, identifier).await;
            }
            ClientCommand::Unsubscribe { identifier } => {
                debug!(conn_id, identifier, "Recovery subscription dropped by the client");
                let streams = self.forget(conn_id);
                self.interest.release(streams).await;
            }
            ClientCommand::Message { identifier, data } => {
                self.perform(conn_id, &identifier, &data, welcomed).await;
            }
            // Presence and whisper have no meaning on the recovery channel.
            other => debug!(conn_id, command = ?other, "Ignoring a non-recovery command"),
        }
    }

    /// Number of live recovery subscriptions on this node.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Drop a connection's recovery session, returning the streams whose
    /// pub/sub interest it held.
    #[must_use]
    pub fn forget(&self, conn_id: u32) -> Vec<String> {
        let Some((_, session)) = self.sessions.remove(&conn_id) else {
            return Vec::new();
        };

        session.stop_pumps();
        // Only drop the in-process lease if we are still the holder: a newer
        // resume elsewhere must keep its claim.
        if let Some(entry) = self.shared.leases.get(session.session_id())
            && *entry == session.lease.load(Ordering::Acquire)
        {
            drop(entry);
            self.shared.leases.remove(session.session_id());
        }

        session.claims.streams.clone()
    }

    /// Verify the token and install the subscription.
    async fn subscribe(&self, conn_id: u32, identifier: String) {
        let parsed: RecoveryIdentifier = match serde_json::from_str(&identifier) {
            Ok(parsed) => parsed,
            Err(e) => {
                debug!(conn_id, error = %e, "Malformed recovery identifier");
                return self.reject(conn_id, identifier).await;
            }
        };

        let claims = match self.verifier.verify(&parsed.token, &self.shared.namespace, now_unix()) {
            Ok(claims) => claims,
            Err(e) => {
                // Never log the token, only why it failed.
                debug!(conn_id, error = %e, "Recovery token rejected");
                return self.reject(conn_id, identifier).await;
            }
        };

        // Hold pub/sub interest so this node is woken for the granted streams.
        let mut acquired = Vec::new();
        for stream in &claims.streams {
            if self.interest.acquire(stream).await {
                acquired.push(stream.clone());
                continue;
            }

            warn!(conn_id, stream = %stream, "Backend refused interest; rejecting recovery");
            self.interest.release(acquired).await;
            return self.reject(conn_id, identifier).await;
        }

        let session = Arc::new(RecoverySession::new(conn_id, identifier.clone(), claims));
        let session_id = session.claims.session_id.clone();
        let streams = session.claims.streams.len();
        self.sessions.insert(conn_id, session);

        let confirm = ServerMessage::ConfirmSubscription { identifier };
        self.shared.hub.send(conn_id, &actioncable::encode(&confirm)).await;
        debug!(conn_id, session_id = %session_id, streams, "Recovery subscription confirmed");
    }

    async fn reject(&self, conn_id: u32, identifier: String) {
        let reject = ServerMessage::RejectSubscription { identifier };
        self.shared.hub.send(conn_id, &actioncable::encode(&reject)).await;
    }

    /// Apply one `perform` on a confirmed recovery subscription.
    async fn perform(&self, conn_id: u32, identifier: &str, data: &str, welcomed: bool) {
        let Some(session) = self.sessions.get(&conn_id).map(|entry| Arc::clone(&entry)) else {
            debug!(conn_id, "Recovery command for an unconfirmed subscription; ignored");
            return;
        };

        if session.identifier != identifier {
            debug!(conn_id, "Recovery command for a foreign identifier; ignored");
            return;
        }

        if !is_recovery_action(data) {
            debug!(conn_id, "Ignoring a non-recovery action on the recovery channel");
            return;
        }

        let command = match RecoveryCommand::decode(data) {
            Ok(command) => command,
            Err(e) => {
                debug!(conn_id, error = %e, "Malformed recovery command");
                return;
            }
        };

        let cursors = match command.decoded_cursors() {
            Ok(cursors) => cursors,
            Err(e) => {
                debug!(conn_id, error = %e, "Undecodable recovery cursor");
                return;
            }
        };

        match command.op {
            RecoveryOp::Resume => self.resume(&session, cursors, welcomed).await,
            RecoveryOp::Ack => self.ack(&session, command.cursors).await,
        }
    }

    /// Re-decide what this resume may replay: the typed refusal for the whole
    /// session, or the subset of granted streams the connection currently
    /// subscribes to (the rest are refused one by one).
    fn authorize(
        &self,
        session: &RecoverySession,
        welcomed: bool,
    ) -> Result<Authorized, ResumeRefusal> {
        if !welcomed {
            return Err(ResumeRefusal::Unidentified);
        }

        // A capability that has expired since the subscription was confirmed
        // replays nothing: recovery cannot resurrect revoked access.
        session.claims.validate_at(now_unix()).map_err(ResumeRefusal::TokenInvalid)?;

        let (granted, refused): (Vec<_>, Vec<_>) = session
            .claims
            .streams
            .iter()
            .cloned()
            .partition(|stream| self.shared.is_member(session.conn_id, stream));
        let refused = refused
            .into_iter()
            .map(|stream| {
                let refusal = ResumeRefusal::NotSubscribed { stream: stream.clone() };
                (stream, refusal)
            })
            .collect();
        Ok(Authorized { granted, refused })
    }

    /// The cursors a resume starts from: the client's when it sent any, else
    /// the session's stored snapshot (server-side memory), else nothing.
    async fn starting_cursors(
        &self,
        session: &RecoverySession,
        client: BTreeMap<String, Cursor>,
    ) -> BTreeMap<String, Cursor> {
        if !client.is_empty() {
            return client;
        }

        let stored = match self.shared.broker.load_snapshot(session.session_id()).await {
            Ok(Some(stored)) => stored,
            Ok(None) => return client,
            Err(e) => {
                // Memory is a convenience, not a guarantee: resume live.
                warn!(conn_id = session.conn_id, error = %e, "Could not read the session snapshot");
                return client;
            }
        };

        let snapshot = match SessionSnapshot::decode(&stored) {
            Ok(snapshot) => snapshot,
            Err(e) => {
                warn!(
                    conn_id = session.conn_id,
                    error = %e,
                    "Ignoring an unusable session snapshot"
                );
                return client;
            }
        };
        if snapshot.session_id != session.session_id() {
            warn!(conn_id = session.conn_id, "Ignoring a snapshot stored for another session");
            return client;
        }

        match snapshot.decoded_cursors() {
            Ok(cursors) => {
                debug!(
                    conn_id = session.conn_id,
                    streams = cursors.len(),
                    "Resuming from the session's stored snapshot"
                );
                cursors
            }
            Err(e) => {
                warn!(
                    conn_id = session.conn_id,
                    error = %e,
                    "Ignoring an unusable session snapshot"
                );
                client
            }
        }
    }

    /// Authorize, fence, replay, and hand over to live delivery.
    async fn resume(
        &self,
        session: &Arc<RecoverySession>,
        cursors: BTreeMap<String, Cursor>,
        welcomed: bool,
    ) {
        let shared = &self.shared;

        let Authorized { granted: streams, refused } = match self.authorize(session, welcomed) {
            Ok(decision) => decision,
            Err(refusal) => {
                for stream in &session.claims.streams {
                    session.refuse(shared, stream, &refusal).await;
                }
                return;
            }
        };
        for (stream, refusal) in &refused {
            session.refuse(shared, stream, refusal).await;
        }
        if streams.is_empty() {
            return;
        }

        // Fencing: the atomic bump decides the winner before anything is read.
        let lease = match shared.broker.fence(session.session_id()).await {
            Ok(lease) => lease,
            Err(e) => {
                warn!(conn_id = session.conn_id, error = %e, "Could not claim the recovery lease");
                for stream in &streams {
                    session.gap(shared, stream, GapReason::Unavailable).await;
                }
                return;
            }
        };
        session.lease.store(lease, Ordering::Release);
        shared.leases.insert(session.session_id().to_string(), lease);
        *lock(&session.last_touch) = Instant::now();

        let cursors = self.starting_cursors(session, cursors).await;

        // A repeat resume on this connection replaces its own tails.
        session.stop_pumps();
        session.replaying.store(true, Ordering::Release);

        session.send(shared, &RecoveryMessage::replay_start(streams.clone())).await;

        // 1. Capture each watermark, then start the live tail *at* it, so
        //    everything published from here on is buffered rather than lost.
        let mut plan: Vec<ReplayPlan> = Vec::new();
        for stream in &streams {
            let range = match shared.broker.range(stream).await {
                Ok(range) => range,
                Err(e) => {
                    warn!(stream = %stream, error = %e, "Could not read the retained range");
                    session.gap(shared, stream, GapReason::Unavailable).await;
                    continue;
                }
            };

            let start = match cursors.get(stream).copied() {
                Some(cursor) if range.covers(cursor) => cursor,
                Some(cursor) => {
                    let reason = if cursor.generation == range.watermark.generation {
                        GapReason::Expired
                    } else {
                        GapReason::UnknownGeneration
                    };
                    session.gap(shared, stream, reason).await;
                    // Continue live from the edge: a gap is not a disconnect.
                    range.watermark
                }
                // No cursor: this stream asks for no history at all.
                None => range.watermark,
            };

            self.spawn_pump(session, stream, range.watermark);
            plan.push(ReplayPlan { stream: stream.clone(), start, watermark: range.watermark });
        }

        // 2. Replay strictly after each cursor, up to the captured watermark.
        let Some(replayed) = self.replay(session, plan).await else {
            // Fenced mid-replay: the loser stops here, its tails stop on
            // their own, and nothing is handed off.
            return;
        };

        // 3. Hand off: the tails drain their buffers, deduplicated, and then
        //    keep delivering live.
        session.replaying.store(false, Ordering::Release);
        session.flip.notify_waiters();
        session.send(shared, &RecoveryMessage::replay_done(session.cursor_snapshot())).await;

        debug!(
            conn_id = session.conn_id,
            lease, replayed, "Replay finished; delivery continues live"
        );
    }

    /// Replay every planned stream in order, returning how many retained
    /// messages were delivered - or `None` when a newer resume fenced this
    /// one out partway through.
    ///
    /// A stream whose rows jump, or run out before the watermark, was trimmed
    /// or expired underneath the replay: that stream gets an
    /// [`GapReason::Expired`] gap and the rest of the plan continues, because
    /// live delivery resumes from the watermark either way.
    async fn replay(&self, session: &Arc<RecoverySession>, plan: Vec<ReplayPlan>) -> Option<u64> {
        let shared = &self.shared;
        let mut replayed: u64 = 0;

        for ReplayPlan { stream, start, watermark } in plan {
            let mut cursor = start;

            'stream: while cursor.sequence < watermark.sequence {
                if session.fenced(shared) {
                    debug!(conn_id = session.conn_id, "Replay cancelled: a newer resume won");
                    session.gap(shared, &stream, GapReason::Fenced).await;
                    shared.broker.counters().record_replayed(replayed);
                    return None;
                }

                let rows = match shared.broker.fetch_after(&stream, cursor, REPLAY_BATCH).await {
                    Ok(rows) => rows,
                    Err(e) => {
                        warn!(stream = %stream, error = %e, "Replay read failed");
                        session.gap(shared, &stream, GapReason::Unavailable).await;
                        break;
                    }
                };

                // Nothing left before the watermark means the rows between
                // here and there were expired or trimmed underneath us.
                if rows.is_empty() {
                    session.gap(shared, &stream, GapReason::Expired).await;
                    break;
                }

                for message in rows {
                    if message.position.sequence > watermark.sequence {
                        // Past the watermark: the tail owns it (and will
                        // deduplicate if we somehow raced it).
                        break 'stream;
                    }
                    // Sequences are contiguous by construction: a jump is a
                    // hole retention opened mid-replay.
                    if message.position.sequence > cursor.sequence + 1 {
                        session.gap(shared, &stream, GapReason::Expired).await;
                        break 'stream;
                    }
                    cursor = message.position;
                    if session.deliver(shared, &message).await {
                        replayed += 1;
                    }
                }
            }
        }

        shared.broker.counters().record_replayed(replayed);
        Some(replayed)
    }

    /// Record receipt. Best effort by definition: recovery is at-least-once
    /// with receipt cursors.
    async fn ack(&self, session: &Arc<RecoverySession>, cursors: BTreeMap<String, String>) {
        let snapshot = SessionSnapshot::new(session.session_id(), cursors);
        let encoded = match snapshot.to_json() {
            Ok(encoded) => encoded,
            Err(e) => {
                warn!(error = %e, "Could not encode a recovery session snapshot");
                return;
            }
        };

        if let Err(e) = self.shared.broker.save_snapshot(session.session_id(), &encoded).await {
            // An unrecorded acknowledgement costs a longer replay next time;
            // it never costs a delivery.
            warn!(
                conn_id = session.conn_id,
                error = %e,
                "Could not record a recovery acknowledgement; replay will simply start earlier"
            );
        }
    }

    /// Start (or restart) one stream's tail at `watermark`.
    fn spawn_pump(&self, session: &Arc<RecoverySession>, stream: &str, watermark: Cursor) {
        let handle = tokio::spawn(run_pump(
            Arc::clone(&self.shared),
            Arc::clone(session),
            stream.to_string(),
            watermark,
        ));

        lock(&session.pumps).insert(stream.to_string(), PumpHandle(handle));
    }
}

impl std::fmt::Debug for RecoveryController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryController")
            .field("namespace", &self.shared.namespace)
            .field("sessions", &self.sessions.len())
            .finish_non_exhaustive()
    }
}

/// One stream's ordered tail for one session.
///
/// Reads committed rows after its own position - never the notification's
/// content - so a missed wake, a duplicate wake, and a node that was not
/// running all resolve the same way: the next read finds the rows.
async fn run_pump(
    shared: Arc<Shared>,
    session: Arc<RecoverySession>,
    stream: String,
    start: Cursor,
) {
    let mut cursor = start;
    let mut buffer: VecDeque<RetainedMessage> = VecDeque::new();
    let mut buffering = true;
    let mut wake = shared.waker.subscribe(&stream);
    let mut last_check = Instant::now();
    // Whether this pass was started by the reconcile timer rather than a
    // wake: rows found on such a pass are rows no notification announced.
    let mut reconciling = false;

    loop {
        if session.fenced(&shared) {
            session.gap(&shared, &stream, GapReason::Fenced).await;
            return;
        }

        if buffering && !session.replaying.load(Ordering::Acquire) {
            buffering = false;
            while let Some(message) = buffer.pop_front() {
                session.deliver(&shared, &message).await;
            }
        }

        let rows = match shared.broker.fetch_after(&stream, cursor, REPLAY_BATCH).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(stream = %stream, error = %e, "Retained read failed; retrying");
                tokio::time::sleep(shared.config.reconcile_interval).await;
                continue;
            }
        };
        if reconciling && !rows.is_empty() {
            shared.broker.counters().record_reconciled();
        }

        let full_batch = rows.len() == REPLAY_BATCH;
        for message in rows {
            // Sequences are contiguous by construction, so a jump means the
            // rows in between were expired or trimmed: an honest gap.
            if message.position.sequence > cursor.sequence + 1 {
                session.gap(&shared, &stream, GapReason::Expired).await;
            }
            cursor = message.position;

            if buffering {
                if buffer.len() >= shared.config.replay_buffer {
                    // Live traffic outran the replay. Stop buffering, say so,
                    // and keep delivering: the client resynchronizes.
                    session.gap(&shared, &stream, GapReason::BufferOverflow).await;
                    buffer.clear();
                    buffering = false;
                    session.deliver(&shared, &message).await;
                } else {
                    buffer.push_back(message);
                }
            } else {
                session.deliver(&shared, &message).await;
            }
        }

        if full_batch {
            // More rows are waiting; drain before sleeping.
            reconciling = false;
            continue;
        }

        session.keep_alive(&shared).await;

        // Cross-node fencing and the membership re-check, at most once per
        // reconcile window: a resume on another node bumped the lease without
        // touching this process, and an unsubscribe (or a Rails revocation)
        // ends the authorization this tail was started under.
        if last_check.elapsed() >= shared.config.reconcile_interval {
            last_check = Instant::now();
            if !shared.is_member(session.conn_id, &stream) {
                let refusal = ResumeRefusal::NotSubscribed { stream: stream.clone() };
                session.refuse(&shared, &stream, &refusal).await;
                return;
            }
            match shared.broker.fencing_generation(session.session_id()).await {
                Ok(current) if current != session.lease.load(Ordering::Acquire) => {
                    session.gap(&shared, &stream, GapReason::Fenced).await;
                    return;
                }
                Ok(_) => {}
                Err(e) => debug!(error = %e, "Could not re-read the recovery lease"),
            }
        }

        // A wake is latency, the tick is correctness: reconciliation happens
        // even when no notification ever arrives.
        reconciling = tokio::select! {
            _ = wake.recv() => false,
            () = session.flip.notified() => false,
            () = tokio::time::sleep(shared.config.reconcile_interval) => true,
        };
    }
}

/// Build a recovery controller when this node is configured for one.
///
/// `None` - and therefore no recovery at all - whenever the broker is off, the
/// build has no retained store, or no dedicated recovery key is configured.
/// Recovery is never enabled implicitly.
pub fn build(
    namespace: String,
    secret: Option<SecretBytes>,
    broker: Option<Arc<dyn Broker>>,
    config: &BrokerConfig,
    hub: Arc<Hub>,
    waker: Option<Arc<StreamWaker>>,
    interest: Arc<dyn StreamInterest>,
) -> Option<Arc<RecoveryController>> {
    let (secret, broker, waker) = (secret?, broker?, waker?);
    if !config.enabled {
        return None;
    }

    Some(RecoveryController::new(namespace, secret, broker, config.clone(), hub, waker, interest))
}

#[cfg(test)]
mod tests;
