//! Server lifecycle: shared connection driver, backend readiness, shutdown.
//!
//! Nothing in this module knows how bytes reach a browser. A transport supplies
//! an outbound sink ([`crate::transport::docking::Outgoing`] frames) and then
//! calls the same driver for every connection event:
//!
//! ```text
//! transport                     server::Connections
//! ---------                     -------------------
//! connection opened      ->     accept()          (session task + welcome)
//! text frame received    ->     handle_frame()    (parse + enqueue on the session mailbox)
//! connection closed      ->     teardown()        (the one teardown path)
//! link/listener ended    ->     drain()           (teardown every session)
//! ```
//!
//! The docking transport and the direct HTTP listener therefore behave
//! identically at the ActionCable level; only framing and lifetime differ.
//!
//! # Sessions
//!
//! Every session - signed-only or Rails-dispatch, docked or direct - owns a
//! bounded, ordered command mailbox and a task of its own. A transport never
//! awaits a command's outcome: `handle_frame` parses, charges the rate limit,
//! and enqueues. One slow backend acknowledgement or Rails call therefore
//! delays only the session that issued it. Teardown closes the session's
//! gate, drops its mailbox (queued commands are never run), lets a command
//! already in flight finish against deadlines it has anyway, and refuses its
//! late result at the hub, which no longer knows the session.
//!
//! # Backend interest
//!
//! [`Backend`] owns the single pub/sub connection and the *process-wide*
//! interest count per stream. Each transport has its own [`Hub`] (its own
//! connection-id space and outbound sink), so a stream can hold members in more
//! than one hub; interest is released only when the last hub loses its last
//! member. Delivery fans a publication out to every registered hub. Interest
//! is refcounted per stream, under a per-stream lock: unrelated streams never
//! wait for each other's `LISTEN`.
//!
//! # Readiness
//!
//! A backend that cannot be established at startup is a startup failure, not
//! a node that runs without broadcasting. `/readyz` reports the pub/sub
//! listener (unready while it reconnects) and, in dispatch mode, the Rails
//! endpoint's `describe` handshake.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde_json::{Map, Value};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::actioncable::{self, ClientCommand, ServerMessage};
use crate::broker::{Broker, BrokerConfig};
use crate::config::{Config, RpcSettings};
use crate::encoder;
use crate::hub::{Hub, HubError};
use crate::metrics::{self, CommandClass, Metrics, UnsupportedCommand};
use crate::presence::{
    PresenceBackend, PresenceError, PresenceGrant, PresenceGrants, PresencePublisher,
    PresenceSessions, ReplacedGrant, presence_stream_key, presence_stream_target,
};
use crate::protocol::Envelope;
use crate::protocol::ProtocolError;
use crate::protocol::v1::control::ControlMessage;
use crate::protocol::v1::now_unix;
use crate::protocol::v1::presence::{
    PresenceCommand, PresenceMessage, PresenceOp, is_presence_action,
};
use crate::protocol::v1::publication::Publication;
use crate::protocol::v1::rpc::{
    ActionPayload, CLOSE_REASON_SERVER_RESTART, CLOSE_REASON_UNAUTHORIZED, ConnectionSnapshot,
    Effect, Operation, Outcome, ReleasePayload, RpcReply, RpcRequest, StateReplacement,
    SubscriptionSnapshot,
};
use crate::pubsub::{PubSub, PubSubError, PubSubEvent};
use crate::recovery::{RecoveryController, StreamWaker};
use crate::rpc_client::{DESCRIBE_INTERVAL, DescribeRefusal, Described, RpcClient, RpcError};
use crate::session::Session;
use crate::streams::StreamsController;
use crate::sync::lock;
use crate::transport::docking::Outgoing;

#[cfg(all(not(feature = "memory"), feature = "postgres"))]
use crate::pubsub::{DEFAULT_KEEPALIVE_INTERVAL, PostgresConfig, PostgresPubSub};

#[cfg(feature = "memory")]
use crate::pubsub::MemoryPubSub;

/// How often the presence resync task re-reads the pub/sub reconnect counter.
const PRESENCE_RESYNC_WATCH_INTERVAL: Duration = Duration::from_secs(1);

/// How often the Rails prober re-reads the circuit breaker, so a circuit
/// that just half-opened is probed at once rather than on the next tick.
const CIRCUIT_WATCH_INTERVAL: Duration = Duration::from_secs(1);

/// The standard ActionCable disconnect reason used when a node drains.
pub const DISCONNECT_REASON_SERVER_RESTART: &str = "server_restart";

/// Process-wide shutdown signal.
///
/// One sender, many receivers: transports stop accepting, connection tasks stop
/// reading and close their sockets, and background loops exit. The same shape
/// serves as a per-session gate (see [`SessionGate`]).
#[derive(Debug, Clone)]
pub struct Shutdown {
    tx: watch::Sender<bool>,
}

impl Shutdown {
    /// Create a signal that has not fired yet.
    #[must_use]
    pub fn new() -> Self {
        Self { tx: watch::channel(false).0 }
    }

    /// Fire the signal. Idempotent.
    ///
    /// Uses `send_replace` rather than `send`: the value must change even when
    /// nobody is subscribed yet, so a later subscriber (and every
    /// `is_triggered` check) still observes the drain.
    pub fn trigger(&self) {
        self.tx.send_replace(true);
    }

    /// Whether the signal has already fired.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        *self.tx.borrow()
    }

    /// A receiver that resolves once the signal fires.
    #[must_use]
    pub fn subscribe(&self) -> ShutdownRx {
        ShutdownRx(self.tx.subscribe())
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

/// Receiving half of [`Shutdown`].
#[derive(Debug, Clone)]
pub struct ShutdownRx(watch::Receiver<bool>);

impl ShutdownRx {
    /// Resolve once shutdown has been signalled (immediately if it already
    /// was). Cancel-safe: usable directly in a `select!` branch.
    pub async fn wait(&mut self) {
        // A dropped sender means the process is going away anyway; treat it as
        // a shutdown rather than hanging forever.
        let _ = self.0.wait_for(|fired| *fired).await;
    }

    /// Whether shutdown has already been signalled.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        *self.0.borrow()
    }
}

/// Aborts background tasks when dropped, including on early error returns.
#[derive(Debug, Default)]
pub struct TaskGuard(Vec<JoinHandle<()>>);

impl TaskGuard {
    /// Create an empty guard.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Take ownership of one more task handle.
    pub fn push(&mut self, handle: JoinHandle<()>) {
        self.0.push(handle);
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        for handle in &self.0 {
            handle.abort();
        }
    }
}

/// Weakly held registrations: hubs that receive deliveries, or connection
/// drivers that report gauges.
///
/// `Weak` so a transport that goes away (a docking link dropping) cannot be
/// kept alive by the backend; dead entries are pruned on every pass.
///
/// Poisoning is recovered ([`lock`]): a stale weak handle fails to upgrade and
/// is pruned on the next pass, so no panic can leave a shape that matters.
struct WeakRegistry<T>(Mutex<Vec<Weak<T>>>);

impl<T> WeakRegistry<T> {
    const fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }

    /// The registry lock, with dead entries already pruned.
    fn pruned(&self) -> MutexGuard<'_, Vec<Weak<T>>> {
        let mut entries = lock(&self.0);
        entries.retain(|weak| weak.strong_count() > 0);
        entries
    }

    /// Add one entry.
    fn register(&self, entry: &Arc<T>) {
        self.pruned().push(Arc::downgrade(entry));
    }

    /// Strong handles to every live entry.
    ///
    /// The lock covers the prune and the upgrade only: callers act on the
    /// returned handles after it is released, so no delivery or gauge read
    /// ever runs under it.
    fn live(&self) -> Vec<Arc<T>> {
        self.pruned().iter().filter_map(Weak::upgrade).collect()
    }
}

/// How long one control UUID is remembered for deduplication.
///
/// Long enough to cover a control message arriving over both the HTTP control
/// ingress and the PostgreSQL notification (seconds apart at worst), short
/// enough that the set never grows into history.
const CONTROL_DEDUPE_TTL: Duration = Duration::from_mins(5);

/// How many control UUIDs are remembered at most.
///
/// 1024 UUID strings plus timestamps is on the order of 100 KiB - a fixed,
/// documented ceiling. Control traffic is rare (sign-outs, forced
/// reconnects); overflowing this bound evicts the *oldest* entry, so the only
/// way a duplicate slips through is a burst of more than 1024 distinct
/// control messages inside one TTL window, and the replayed effect is an
/// idempotent disconnect.
const CONTROL_DEDUPE_CAPACITY: usize = 1024;

/// Bounded, TTL-expiring first-sighting set for control UUIDs.
///
/// Insertion order equals time order, so expiry and capacity eviction both
/// pop from the front of one queue; every operation is O(1) amortized.
struct ControlDeduper {
    capacity: usize,
    ttl: Duration,
    seen: HashMap<String, Instant>,
    order: VecDeque<String>,
}

impl ControlDeduper {
    fn new(capacity: usize, ttl: Duration) -> Self {
        Self { capacity, ttl, seen: HashMap::new(), order: VecDeque::new() }
    }

    /// Whether `control_id` is being seen for the first time within the TTL.
    ///
    /// A duplicate does *not* refresh the original sighting: the window is
    /// measured from the first arrival, which is the one that acted.
    fn first_sighting(&mut self, control_id: &str, now: Instant) -> bool {
        // Expire from the front; entries are in arrival order.
        while let Some(front) = self.order.front() {
            let expired = self
                .seen
                .get(front)
                .is_none_or(|at| now.saturating_duration_since(*at) >= self.ttl);
            if !expired {
                break;
            }
            self.seen.remove(front.as_str());
            self.order.pop_front();
        }

        if self.seen.contains_key(control_id) {
            return false;
        }

        // Capacity eviction: drop the oldest sighting, never the newest.
        while self.order.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }

        self.seen.insert(control_id.to_string(), now);
        self.order.push_back(control_id.to_string());
        true
    }
}

/// Why an inbound control message was refused.
///
/// The offending payload is never part of the message, matching the pub/sub
/// decode errors: a log line must not leak identity material.
#[derive(Debug, thiserror::Error)]
pub enum ControlRejection {
    /// Not decodable as a v1 control message (unknown `kind` included).
    #[error("malformed control message: {0}")]
    Malformed(String),
    /// Decoded, but semantically invalid (bad UUID, unlisted reason, ...).
    #[error("invalid control message: {0}")]
    Invalid(#[from] ProtocolError),
    /// Valid, but addressed to another deployment's namespace.
    #[error("control message namespace {actual:?} does not match this node")]
    ForeignNamespace { actual: String },
}

/// What applying an accepted control message did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlOutcome {
    /// Applied to `sessions` local sessions (0 when the identity key is not
    /// connected to this node - a normal no-op in a multi-node deployment).
    Applied { sessions: usize },
    /// This `control_id` was already consumed; nothing was done.
    Duplicate,
}

/// What the Rails dispatch endpoint's `describe` handshake last said.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RailsStatus {
    /// Signed-only mode: there is no endpoint to probe.
    NotConfigured,
    /// Dispatch mode, first probe not finished yet.
    Probing,
    /// The endpoint speaks v1 with every required capability and is ready.
    Admitted,
    /// The probe could not reach the endpoint (transport, deadline, open
    /// circuit, or a reply this build cannot use).
    Unreachable,
    /// The endpoint answered, and the answer forbids dispatch.
    Refused(DescribeRefusal),
}

impl RailsStatus {
    /// Whether dispatch traffic may be admitted.
    #[must_use]
    pub const fn is_admitted(self) -> bool {
        matches!(self, Self::Admitted)
    }

    /// The stable name `/readyz` reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotConfigured => "not_configured",
            Self::Probing => "probing",
            Self::Admitted => "ok",
            Self::Unreachable => "unreachable",
            Self::Refused(refusal) => refusal.as_str(),
        }
    }
}

/// What `/readyz` reports, component by component.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Readiness {
    /// The verdict: every required component is usable and the node is not
    /// draining.
    pub ready: bool,
    /// `ok`, or `reconnecting` while the pub/sub listener is re-establishing
    /// its session (notification-only delivery cannot recover that gap).
    pub pubsub: &'static str,
    /// Dispatch mode only: the Rails endpoint's `describe` verdict.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rails: Option<&'static str>,
    /// Set while the node drains.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub draining: bool,
}

/// The pub/sub backend, its delivery fan-out, and the interest refcounts.
pub struct Backend {
    /// The compiled-in backend, established at startup or not at all.
    pubsub: Arc<dyn PubSub>,
    /// Distributed presence store, when this node runs Rails dispatch against
    /// PostgreSQL. `None` in signed-only mode, in memory builds, and whenever
    /// the store could not be established: presence then simply does not
    /// happen, which is never a reason to refuse ordinary traffic.
    presence: Option<Arc<dyn PresenceBackend>>,
    /// Hubs that receive delivered publications. `Weak` so a transport that
    /// goes away (a docking link dropping) cannot keep its hub alive.
    hubs: WeakRegistry<Hub>,
    /// `stream -> interest entry`, one lock per stream.
    ///
    /// The entry's lock is held across the backend acknowledgement, so a
    /// 1 -> 0 release can never interleave with a concurrent 0 -> 1 acquire
    /// of the *same* stream; unrelated streams never wait for each other.
    /// Idle entries (count 0, nobody waiting) are removed.
    interest: DashMap<String, Arc<tokio::sync::Mutex<usize>>>,
    listener_started: AtomicBool,
    /// Deployment namespace: part of every publication envelope this node
    /// originates, and of every retained key.
    namespace: String,
    /// Retained publication store. `None` unless the broker is enabled
    /// *and* this build has a PostgreSQL store: without it there is
    /// no retained history and therefore no recovery.
    broker: Option<Arc<dyn Broker>>,
    /// Which streams are retained, and with what bounds.
    broker_config: BrokerConfig,
    /// Wakes retained-stream readers when a publication lands on this node.
    /// Present exactly when `broker` is.
    waker: Option<Arc<StreamWaker>>,
    /// First-sighting set for control UUIDs, shared by the PostgreSQL control
    /// listener and the HTTP control ingress (that is the point: one message
    /// arriving on both paths acts once).
    control_seen: Mutex<ControlDeduper>,
    /// Process-wide counter registry.
    ///
    /// It lives here because the backend is the one object every transport
    /// already holds, which is what lets the private ingress listener render a
    /// scrape that includes the `/cable` listener's numbers without the two
    /// transports knowing about each other.
    metrics: Arc<Metrics>,
    /// Connection drivers registered for metrics, one per transport. `Weak`
    /// for the same reason `hubs` is: a transport that goes away must not be
    /// kept alive by the exporter.
    drivers: WeakRegistry<Connections>,
    /// The Rails endpoint's last `describe` verdict.
    rails: Mutex<RailsStatus>,
}

impl Backend {
    /// Connect the configured pub/sub backend.
    ///
    /// # Errors
    ///
    /// When the PostgreSQL backend cannot be established: a missing or
    /// unusable configuration, a failed initial connection, or a `LISTEN`
    /// session that is not acknowledged within the deadline. There is no
    /// "pub/sub disabled" state to fall back to; the caller exits non-zero.
    /// The memory backend never fails.
    pub async fn init(config: &Config) -> anyhow::Result<Arc<Self>> {
        let pubsub = init_pubsub(config).await?;
        let presence = init_presence(config, &pubsub).await;
        let broker = init_broker(config).await;
        let waker = broker.as_ref().map(|_| Arc::new(StreamWaker::new()));
        let rails =
            if config.rpc.is_enabled() { RailsStatus::Probing } else { RailsStatus::NotConfigured };
        Ok(Arc::new(Self {
            presence,
            namespace: config.namespace.clone(),
            broker,
            broker_config: config.broker.clone(),
            waker,
            rails: Mutex::new(rails),
            ..Self::with_pubsub(pubsub)
        }))
    }

    /// Build a backend around an already-constructed pub/sub handle.
    ///
    /// Used by tests, which need to publish through the same handle the server
    /// listens on.
    #[must_use]
    pub fn with_pubsub(pubsub: Arc<dyn PubSub>) -> Self {
        Self {
            pubsub,
            presence: None,
            hubs: WeakRegistry::new(),
            interest: DashMap::new(),
            listener_started: AtomicBool::new(false),
            namespace: String::new(),
            broker: None,
            broker_config: BrokerConfig::default(),
            waker: None,
            control_seen: Mutex::new(ControlDeduper::new(
                CONTROL_DEDUPE_CAPACITY,
                CONTROL_DEDUPE_TTL,
            )),
            metrics: Arc::new(Metrics::new()),
            drivers: WeakRegistry::new(),
            rails: Mutex::new(RailsStatus::NotConfigured),
        }
    }

    /// The process-wide counter registry.
    pub const fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Register a connection driver as a metrics source.
    ///
    /// Called by [`Connections::new`]; a driver registers itself exactly once,
    /// and dead entries are pruned on every pass.
    pub fn register_driver(&self, driver: &Arc<Connections>) {
        self.drivers.register(driver);
    }

    /// Gather the live numbers one `/metrics` scrape reports.
    pub fn gauges(&self) -> metrics::Gauges {
        let rails = self.rails_status();
        let mut gauges = metrics::Gauges {
            streams_with_interest: self.interest.len() as u64,
            rails_ready: (rails != RailsStatus::NotConfigured).then(|| rails.is_admitted()),
            ..metrics::Gauges::default()
        };

        for driver in self.drivers.live() {
            gauges.connections += driver.session_count() as u64;
            gauges.subscriptions += driver.hub().subscription_count() as u64;
            gauges.presence_leases += driver.presence_lease_count() as u64;
        }

        gauges
    }

    /// Render one Prometheus scrape for this process.
    ///
    /// The exporter reads every subsystem through the backend, so both the
    /// public listener (when `metrics_public` is set) and the private ingress
    /// produce byte-identical output.
    pub fn render_metrics(&self) -> String {
        metrics::render(metrics::Exposition {
            metrics: &self.metrics,
            gauges: self.gauges(),
            pubsub: Some(self.pubsub.stats()),
            presence: self.presence.as_ref().map(|presence| presence.metrics()),
            broker: self.broker.as_ref().map(|broker| metrics::BrokerExposition {
                namespace: &self.namespace,
                stats: broker.stats(),
            }),
        })
    }

    /// Set the deployment namespace on an already-built backend (tests).
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// The deployment namespace this node validates envelopes against.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Attach a retained broker to an already-built backend.
    ///
    /// For tests and embedders that construct the store themselves;
    /// [`Backend::init`] wires the configured one.
    #[must_use]
    pub fn with_broker(
        mut self,
        namespace: impl Into<String>,
        broker: Arc<dyn Broker>,
        config: BrokerConfig,
    ) -> Self {
        self.namespace = namespace.into();
        self.broker = Some(broker);
        self.broker_config = config;
        self.waker = Some(Arc::new(StreamWaker::new()));
        self
    }

    /// The retained publication store, if this node has one.
    pub fn broker(&self) -> Option<&Arc<dyn Broker>> {
        self.broker.as_ref()
    }

    /// The retained-stream waker, if this node has one.
    pub const fn waker(&self) -> Option<&Arc<StreamWaker>> {
        self.waker.as_ref()
    }

    /// The pub/sub handle.
    pub const fn pubsub(&self) -> &Arc<dyn PubSub> {
        &self.pubsub
    }

    /// The distributed presence store, if this node has one.
    pub fn presence(&self) -> Option<&Arc<dyn PresenceBackend>> {
        self.presence.as_ref()
    }

    /// Attach a presence store to an already-built backend.
    ///
    /// For tests and embedders that construct the store themselves;
    /// [`Backend::init`] wires the configured one.
    #[must_use]
    pub fn with_presence(mut self, presence: Arc<dyn PresenceBackend>) -> Self {
        self.presence = Some(presence);
        self
    }

    /// Whether publications can currently flow and, in dispatch mode, Rails
    /// is admitted.
    ///
    /// `false` while the listener reconnects: a node that cannot receive
    /// must never advertise readiness.
    pub fn is_ready(&self) -> bool {
        self.readiness(false).ready
    }

    /// The component-by-component readiness report behind `/readyz`.
    pub fn readiness(&self, draining: bool) -> Readiness {
        let pubsub_ready = self.pubsub.is_ready();
        let rails = self.rails_status();
        let rails_ready = matches!(rails, RailsStatus::NotConfigured | RailsStatus::Admitted);
        Readiness {
            ready: pubsub_ready && rails_ready && !draining,
            pubsub: if pubsub_ready { "ok" } else { "reconnecting" },
            rails: (rails != RailsStatus::NotConfigured).then(|| rails.as_str()),
            draining,
        }
    }

    /// The Rails endpoint's last `describe` verdict.
    pub fn rails_status(&self) -> RailsStatus {
        *lock(&self.rails)
    }

    /// Record a `describe` verdict, logging the transition.
    pub fn set_rails_status(&self, status: RailsStatus) {
        let previous = std::mem::replace(&mut *lock(&self.rails), status);
        if previous == status {
            return;
        }
        match status {
            RailsStatus::Admitted => info!(previous = previous.as_str(), "Rails endpoint admitted"),
            RailsStatus::Refused(refusal) => error!(
                refusal = refusal.as_str(),
                "Rails endpoint refused by the describe handshake; dispatch connections are \
                 not admitted and /readyz reports 503 until it passes"
            ),
            RailsStatus::Unreachable => {
                warn!("Rails endpoint unreachable; /readyz reports 503 until a probe succeeds");
            }
            RailsStatus::Probing | RailsStatus::NotConfigured => {}
        }
    }

    /// Register a hub as a delivery target.
    pub fn register_hub(&self, hub: &Arc<Hub>) {
        self.hubs.register(hub);
    }

    /// Deliver one publication to every live hub.
    ///
    /// A publication on a derived presence stream key is a roster change: the
    /// local snapshot cache for the underlying application stream is dropped
    /// before the frames go out, so a client that reacts to the event by asking
    /// for a snapshot cannot be answered from a page that predates it. The
    /// frames themselves are addressed exactly like any other publication -
    /// only subscriptions that hold a grant are members of a presence key.
    fn deliver(&self, stream: &str, payload: &[u8]) {
        if let Some(application_stream) = presence_stream_target(stream)
            && let Some(ref presence) = self.presence
        {
            presence.invalidate(application_stream);
        }

        // A notification is a wakeup for retained readers, never their message
        // source: they re-read committed rows after their own position, so a
        // wake that never arrives only costs reconciliation latency.
        if let Some(ref waker) = self.waker {
            waker.wake(stream);
        }

        let targets = self.hubs.live();

        self.metrics.record_received();
        for hub in targets {
            self.metrics.record_delivered(hub.deliver(stream, payload) as u64);
        }
    }

    /// A publication for `stream` was lost between the publisher and this
    /// node. Nothing can replay it, so every local member of the stream is
    /// closed with a reconnectable `disconnect`: a client that resubscribes
    /// at least knows it missed something, where silence would not tell it.
    fn gap(&self, stream: &str, reason: &str) {
        let mut sessions = 0;
        for hub in self.hubs.live() {
            sessions += hub.disconnect_members(stream);
        }
        self.metrics.record_pubsub_gap_disconnects(sessions as u64);
        warn!(stream, sessions, reason, "Delivery gap: disconnecting the stream's local members");
    }

    /// Start the single pub/sub listener task.
    ///
    /// Returns `None` when the listener is already running.
    pub fn spawn_listener(self: &Arc<Self>) -> Option<JoinHandle<()>> {
        if self.listener_started.swap(true, Ordering::SeqCst) {
            return None;
        }

        let pubsub = Arc::clone(&self.pubsub);
        let backend = Arc::clone(self);
        Some(tokio::spawn(async move {
            // Control consumer: a backend with a control channel holds a
            // permanent LISTEN on it, and this task strict-decodes and applies
            // each message. It is a child of the listener task: aborting the
            // listener drops the guard and aborts the consumer with it.
            let _control_guard = {
                let mut guard = TaskGuard::new();
                if let Some(mut controls) = pubsub.take_control_stream().await {
                    let control_backend = Arc::clone(&backend);
                    guard.push(tokio::spawn(async move {
                        info!("Starting control-channel consumer");
                        while let Some(wire) = controls.recv().await {
                            control_backend.handle_control_wire(&wire).await;
                        }
                        debug!("Control-channel consumer stopped");
                    }));
                }
                guard
            };

            info!("Starting pub/sub listener");
            let result = pubsub
                .listen(Box::new(move |event| match event {
                    // The backend already decoded and validated the v1 notice,
                    // so `payload` is the publication's exact JSON text. Each
                    // hub emits one frame per membership, addressed with that
                    // subscriber's own identifier; the stream name is a
                    // routing key and is never used as an identifier.
                    //
                    // This is also the only local delivery path: a publication
                    // emitted by this node comes back through this listener
                    // rather than being fanned out locally as well.
                    PubSubEvent::Publication { stream, payload } => {
                        backend.deliver(&stream, &payload);
                    }
                    PubSubEvent::Gap { stream, reason } => backend.gap(&stream, &reason),
                }))
                .await;

            if let Err(e) = result {
                error!(error = %e, "Pub/sub listener error");
            }
        }))
    }

    /// Publish one payload under a fresh publication id.
    ///
    /// # Errors
    ///
    /// Exactly those of [`Backend::publish_publication`], which also explains
    /// the routing.
    pub async fn publish(&self, stream: &str, json_payload: &str) -> Result<(), PubSubError> {
        self.publish_publication(Publication::new(&self.namespace, stream, json_payload)).await
    }

    /// Publish one publication, id included, through the broker when the
    /// stream is retained.
    ///
    /// The id is the publisher's: it reaches the notice, the overflow row, the
    /// broker row, and the ingress acknowledgement unchanged, so a retried
    /// publication with the same id is deduplicated by the broker rather than
    /// stored twice.
    ///
    /// Routing is a property of the *stream*: a broker-enabled stream commits
    /// its message, its position bump, and its notification in one transaction
    /// (`crate::broker::postgres`), so a retained publication and its live
    /// delivery can never disagree. Every other stream takes the ephemeral
    /// path unchanged.
    ///
    /// An oversized publication on a retained stream is retained in full and
    /// announced through the overflow relay when the relay is enabled - the
    /// relay row and the retained row commit in the broker's one transaction
    /// - and refused otherwise, exactly as it would be on an ephemeral stream.
    ///
    /// **Note:** with the default `publish_via: :postgres`, Rails emits
    /// `pg_notify` itself and never passes through this method, so those
    /// publications are *not* retained. Retention requires publications to
    /// reach the server (the private HTTP publication ingress).
    ///
    /// # Errors
    ///
    /// On a retained stream:
    /// - [`PubSubError::Protocol`] when the publication envelope does not
    ///   validate (namespace, stream name, or a payload that is not exactly
    ///   one JSON value);
    /// - [`PubSubError::PublicationTooLarge`] above `max_publication_size`,
    ///   and [`PubSubError::PayloadTooLarge`] when the encoded notice does
    ///   not fit one notification and the overflow relay is off;
    /// - [`PubSubError::Backend`] when the broker's retaining transaction
    ///   fails.
    ///
    /// On every other stream, whatever the compiled-in backend's
    /// [`PubSub::publish_publication`] refuses (size limits, readiness,
    /// transport).
    pub async fn publish_publication(&self, publication: Publication) -> Result<(), PubSubError> {
        let result = self.publish_routed(publication).await;
        if result.is_ok() {
            self.metrics.record_published();
        }
        result
    }

    /// [`Backend::publish_publication`] without the counter, so retained and
    /// ephemeral publications are counted in exactly one place.
    async fn publish_routed(&self, publication: Publication) -> Result<(), PubSubError> {
        let Some(broker) =
            self.broker.as_ref().filter(|_| self.broker_config.handles(&publication.stream))
        else {
            return self.pubsub.publish_publication(&publication).await;
        };

        broker.retain(&publication).await.map(|_| ()).map_err(|e| match e {
            // The publication itself was refused: same answer as the
            // ephemeral path gives, so the ingress status does not depend on
            // whether a stream happens to be retained.
            crate::broker::BrokerError::Publication(refused) => refused,
            other => PubSubError::Backend(anyhow::anyhow!("retained publish failed: {other}")),
        })
    }

    /// Acquire backend interest for a stream that just gained its first member
    /// in one hub.
    ///
    /// Returns `false` when the backend refused, so the caller can roll the
    /// membership back instead of confirming a subscription that receives
    /// nothing. A `LISTEN` whose acknowledgement timed out is followed by an
    /// `UNLISTEN` before the refusal is reported: the command may still be
    /// executed by the backend, and PostgreSQL must agree with this map.
    pub async fn acquire(&self, stream: &str) -> bool {
        let entry = Arc::clone(&self.interest.entry(stream.to_string()).or_default());
        let mut count = entry.lock().await;

        if *count == 0
            && let Err(e) = self.pubsub.subscribe(stream).await
        {
            warn!(stream = %stream, error = %e, "Failed to subscribe pubsub");
            if matches!(e, PubSubError::AckTimeout { .. })
                && let Err(e) = self.pubsub.unsubscribe(stream).await
            {
                warn!(stream = %stream, error = %e, "UNLISTEN after a timed-out LISTEN failed");
            }
            drop(count);
            drop(entry);
            self.prune_interest(stream);
            return false;
        }

        *count += 1;
        true
    }

    /// Release backend interest for streams that lost their last member.
    ///
    /// A hub reports a stream here only when its forward index became empty, so
    /// this never races with a concurrent subscriber inside that hub; the
    /// refcount covers the other hubs.
    pub async fn release(&self, streams: Vec<String>) {
        for stream in streams {
            let Some(entry) = self.interest.get(&stream).map(|entry| Arc::clone(&entry)) else {
                warn!(stream = %stream, "Released interest for an unheld stream");
                continue;
            };

            {
                // Held across the unsubscribe acknowledgement on purpose: a
                // 1 -> 0 release must not interleave with a 0 -> 1 acquire of
                // the same stream.
                let mut count = entry.lock().await;
                if *count == 0 {
                    warn!(stream = %stream, "Released interest for an unheld stream");
                } else {
                    *count -= 1;
                    if *count == 0
                        && let Err(e) = self.pubsub.unsubscribe(&stream).await
                    {
                        warn!(stream = %stream, error = %e, "Failed to unsubscribe pubsub");
                    }
                }
            }
            drop(entry);
            self.prune_interest(&stream);
        }
    }

    /// Drop an interest entry nobody holds or waits for.
    fn prune_interest(&self, stream: &str) {
        self.interest.remove_if(stream, |_, entry| {
            Arc::strong_count(entry) == 1 && entry.try_lock().is_ok_and(|count| *count == 0)
        });
    }

    /// Streams this node currently holds backend interest on (tests).
    #[cfg(test)]
    pub(crate) fn interest_count(&self, stream: &str) -> usize {
        self.interest.get(stream).and_then(|entry| entry.try_lock().ok().map(|c| *c)).unwrap_or(0)
    }

    // --- Rails-initiated control -----

    /// Strictly decode and validate one control message for this node.
    ///
    /// Shared by the PostgreSQL control listener and the HTTP control ingress,
    /// so both paths refuse exactly the same messages: malformed JSON and
    /// unknown kinds are hard errors, the envelope must validate (allowlisted
    /// reason included), and the namespace must be this deployment's own.
    ///
    /// # Errors
    ///
    /// - [`ControlRejection::Malformed`] when `wire` is not a v1 control
    ///   message: invalid JSON, a missing field, or an unknown `kind`.
    /// - [`ControlRejection::Invalid`] when it decodes but does not validate:
    ///   wrong version, a bad namespace or `control_id`, an oversized identity
    ///   key, or a close reason outside the allowlist.
    /// - [`ControlRejection::ForeignNamespace`] when it is valid but addressed
    ///   to another deployment.
    pub fn decode_control(&self, wire: &str) -> Result<ControlMessage, ControlRejection> {
        let control: ControlMessage =
            serde_json::from_str(wire).map_err(|e| ControlRejection::Malformed(e.to_string()))?;
        control.validate()?;

        if control.namespace != self.namespace {
            return Err(ControlRejection::ForeignNamespace { actual: control.namespace });
        }

        Ok(control)
    }

    /// Apply one accepted control message to this node's sessions.
    ///
    /// Deduplicates by `control_id` first (bounded set, see
    /// `CONTROL_DEDUPE_CAPACITY` / `CONTROL_DEDUPE_TTL`), then looks the
    /// identity key up in every registered hub's identity index - never a scan
    /// over sessions. Each match is sent the standard ActionCable `disconnect`
    /// frame with the message's reason and reconnect policy, followed by a
    /// socket close on the same ordered outbound queue; the transport then
    /// drives the one teardown path exactly as it does for any other close.
    ///
    /// An identity key with no local session is a no-op: on a multi-node
    /// deployment every node consumes the same control message and each one
    /// disconnects only its own sessions.
    pub async fn apply_control(&self, control: &ControlMessage) -> ControlOutcome {
        // A poisoned set is still a usable set ([`lock`]): `first_sighting`
        // has no reachable panic path between its two collection updates, and
        // the worst a desync could cost is one id that is never forgotten.
        let first_sighting =
            lock(&self.control_seen).first_sighting(&control.control_id, Instant::now());
        if !first_sighting {
            debug!(control_id = %control.control_id, "Ignoring duplicate control message");
            return ControlOutcome::Duplicate;
        }

        let targets = self.hubs.live();

        let disconnect = actioncable::encode(&ServerMessage::Disconnect {
            reason: control.reason.clone(),
            reconnect: control.reconnect,
        });

        let mut sessions = 0;
        for hub in targets {
            for conn_id in hub.get_connections_by_identifier(&control.identity_key) {
                // The disconnect frame and the close share one ordered
                // per-socket mailbox, so the client always sees the standard
                // ActionCable disconnect before its socket goes away.
                hub.send(conn_id, &disconnect).await;
                hub.disconnect(conn_id, CLOSE_NORMAL, &control.reason).await;
                sessions += 1;
            }
        }

        if sessions == 0 {
            // The key itself stays out of the logs: identity material.
            debug!(
                control_id = %control.control_id,
                "Control message matched no local session"
            );
        } else {
            info!(
                control_id = %control.control_id,
                sessions,
                reason = %control.reason,
                reconnect = control.reconnect,
                "Applied remote disconnect"
            );
        }

        ControlOutcome::Applied { sessions }
    }

    /// Consume one raw control notification payload (the PostgreSQL path).
    ///
    /// Refusals are logged and dropped: a notification has no one to answer.
    pub async fn handle_control_wire(&self, wire: &str) {
        match self.decode_control(wire) {
            Ok(control) => {
                self.apply_control(&control).await;
            }
            Err(rejection) => {
                warn!(error = %rejection, "Rejecting control notification");
            }
        }
    }
}

impl std::fmt::Debug for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("ready", &self.is_ready())
            .field("rails", &self.rails_status())
            .field("streams_with_interest", &self.interest.len())
            .finish_non_exhaustive()
    }
}

/// Establish the compiled-in pub/sub backend from configuration, or fail.
#[cfg_attr(
    any(feature = "memory", not(feature = "postgres")),
    expect(
        clippy::unused_async,
        reason = "only the PostgreSQL backend awaits a connection; other builds keep the \
                  signature so `Backend::init` stays feature-agnostic"
    )
)]
async fn init_pubsub(config: &Config) -> anyhow::Result<Arc<dyn PubSub>> {
    #[cfg(feature = "memory")]
    {
        let _ = config;
        let pubsub = MemoryPubSub::new();
        info!(ready = pubsub.is_ready(), "Memory pub/sub initialized (single-node only)");
        Ok(Arc::new(pubsub))
    }

    #[cfg(all(not(feature = "memory"), feature = "postgres"))]
    {
        use anyhow::Context as _;

        // `Config::validate` already requires both; this is the backstop for
        // a configuration built by hand.
        let db_url = config.database_url.clone().filter(|url| !url.trim().is_empty()).context(
            "the PostgreSQL backend requires database_url (ORBITCAST_DATABASE_URL); the relay \
             never runs with broadcasting disabled",
        )?;
        anyhow::ensure!(
            !config.namespace.is_empty(),
            "the PostgreSQL backend requires a namespace (ORBITCAST_NAMESPACE)"
        );

        let pg_config = PostgresConfig {
            database_url: db_url,
            listener_database_url: config.listener_database_url.clone(),
            namespace: config.namespace.clone(),
            ack_timeout: config.pubsub_ack_timeout,
            queue_capacity: config.pubsub_queue_capacity,
            keepalive_interval: DEFAULT_KEEPALIVE_INTERVAL,
            overflow: config.overflow,
        };

        let pubsub = PostgresPubSub::connect(pg_config).await.with_context(|| {
            format!(
                "failed to establish the PostgreSQL pub/sub backend (publisher at {}, \
                 listener at {}); refusing to start without broadcasting",
                crate::config::redact_url(config.database_url.as_deref().unwrap_or_default()),
                crate::config::redact_url(
                    config
                        .listener_database_url
                        .as_deref()
                        .or(config.database_url.as_deref())
                        .unwrap_or_default()
                ),
            )
        })?;
        info!(
            namespace = %config.namespace,
            ready = pubsub.is_ready(),
            "PostgreSQL pub/sub connected"
        );
        Ok(Arc::new(pubsub))
    }
}

/// Initialize the retained broker, if this build and this configuration can
/// have one.
///
/// Three conditions, none of them a new switch: the broker is enabled, this
/// build has a PostgreSQL store (retained history cannot be node-local), and a
/// database URL exists. A store that cannot be established yields `None`: the
/// node serves ordinary traffic and simply retains nothing, rather than
/// accepting recovery subscriptions it could never replay.
#[cfg_attr(
    any(feature = "memory", not(feature = "postgres")),
    expect(
        clippy::unused_async,
        reason = "only the PostgreSQL store awaits a connection; other builds keep the \
                  signature so `Backend::init` stays feature-agnostic"
    )
)]
async fn init_broker(config: &Config) -> Option<Arc<dyn Broker>> {
    if !config.broker.enabled {
        return None;
    }

    #[cfg(all(not(feature = "memory"), feature = "postgres"))]
    {
        use crate::broker::postgres::{PostgresBroker, PostgresBrokerConfig};

        let database_url = config.database_url.clone()?;
        let mut broker_config = PostgresBrokerConfig::new(
            database_url,
            config.namespace.clone(),
            config.broker.clone(),
        );
        // Oversized retained publications ride the same relay the ephemeral
        // path uses, so the broker needs the relay's switch and TTL.
        broker_config.overflow = config.overflow;

        match PostgresBroker::connect(broker_config).await {
            Ok(broker) => Some(broker as Arc<dyn Broker>),
            Err(e) => {
                error!(
                    error = %e,
                    "Failed to start the retained broker; publications will not be retained and \
                     recovery will be unavailable (install \
                     gem/db/orbitcast_migrate/003_create_orbitcast_broker.rb and \
                     004_rekey_orbitcast_sessions_by_session_id.rb)"
                );
                None
            }
        }
    }

    #[cfg(any(feature = "memory", not(feature = "postgres")))]
    {
        warn!("Retained history requires the postgres backend; the broker stays off in this build");
        None
    }
}

/// Pub/sub interest on behalf of a recovery session.
///
/// Recovery holds interest exactly like an ordinary subscription does, which is
/// what keeps the refcount honest: the node stops listening only when the last
/// member - recovery session or browser subscription - is gone.
#[async_trait::async_trait]
impl crate::recovery::StreamInterest for Backend {
    async fn acquire(&self, stream: &str) -> bool {
        Self::acquire(self, stream).await
    }

    async fn release(&self, streams: Vec<String>) {
        Self::release(self, streams).await;
    }
}

/// Presence transitions ride the ordinary pub/sub backend.
///
/// A newtype rather than the blanket `impl<T: PubSub>`: an `Arc<dyn PubSub>`
/// cannot be coerced to an `Arc<dyn PresencePublisher>`, so the trait object
/// is wrapped once here.
struct PubSubPublisher(Arc<dyn PubSub>);

#[async_trait::async_trait]
impl PresencePublisher for PubSubPublisher {
    async fn publish_presence(
        &self,
        stream_key: &str,
        json_payload: &str,
    ) -> Result<(), PresenceError> {
        self.0
            .publish(stream_key, json_payload)
            .await
            .map_err(|e| PresenceError::Backend(anyhow::anyhow!("{e}")))
    }
}

/// Initialize the presence store, if this build and this configuration can
/// have one.
///
/// Presence identity comes from a Rails `presence_grant` effect, so a
/// signed-only node has nothing to lease and gets no store.
/// In dispatch mode the store follows the pub/sub backend: a PostgreSQL build
/// needs a `database_url` and gets the deployment-wide
/// [`crate::presence::postgres::PostgresPresence`]; a memory build gets the
/// single-process [`crate::presence::memory::MemoryPresence`], exactly as it
/// gets the single-process pub/sub.
///
/// A store that cannot be established yields `None`: the node serves ordinary
/// traffic, and presence operations are refused with
/// [`PresenceError::Unavailable`] instead of half-working.
#[cfg_attr(
    any(feature = "memory", not(feature = "postgres")),
    expect(
        clippy::unused_async,
        reason = "only the PostgreSQL store awaits a connection; other builds keep the \
                  signature so `Backend::init` stays feature-agnostic"
    )
)]
async fn init_presence(
    config: &Config,
    pubsub: &Arc<dyn PubSub>,
) -> Option<Arc<dyn PresenceBackend>> {
    if !config.rpc.is_enabled() {
        debug!("Presence stays off: signed-only mode grants no presence");
        return None;
    }

    #[cfg(all(not(feature = "memory"), feature = "postgres"))]
    {
        use crate::presence::postgres::{PostgresPresence, PresenceConfig};

        let database_url = config.database_url.clone()?;
        let publisher = Arc::new(PubSubPublisher(Arc::clone(pubsub))) as Arc<dyn PresencePublisher>;

        let presence_config = PresenceConfig {
            database_url,
            namespace: config.namespace.clone(),
            node_id: config.rpc.node_id.clone(),
            ttl: config.presence_ttl,
            refresh_interval: config.presence_refresh_interval,
        };

        match PostgresPresence::connect(presence_config, publisher).await {
            Ok(store) => {
                info!(
                    namespace = %config.namespace,
                    ttl_ms = saturating_millis(config.presence_ttl),
                    refresh_ms = saturating_millis(config.presence_refresh_interval),
                    "Distributed presence enabled (grants required per subscription)"
                );
                Some(store as Arc<dyn PresenceBackend>)
            }
            Err(e) => {
                error!(
                    error = %e,
                    "Failed to start distributed presence; presence operations will be \
                     refused (install gem/db/orbitcast_migrate/002_create_orbitcast_presence.rb)"
                );
                None
            }
        }
    }

    #[cfg(any(feature = "memory", not(feature = "postgres")))]
    {
        let publisher = Arc::new(PubSubPublisher(Arc::clone(pubsub))) as Arc<dyn PresencePublisher>;
        // The same sweep cadence the PostgreSQL store uses: half the TTL,
        // floored so a short TTL does not turn into a busy loop.
        let sweep = (config.presence_ttl / 2).max(Duration::from_secs(5));
        let store =
            crate::presence::memory::MemoryPresence::start(&config.namespace, publisher, sweep);
        info!(
            namespace = %config.namespace,
            ttl_ms = saturating_millis(config.presence_ttl),
            refresh_ms = saturating_millis(config.presence_refresh_interval),
            "In-process presence enabled (single node; grants required per subscription)"
        );
        Some(store as Arc<dyn PresenceBackend>)
    }
}

/// RFC 6455 close code 1000: a normal, intended close.
const CLOSE_NORMAL: u16 = 1000;

/// `duration` in whole milliseconds, saturating at `u64::MAX`.
///
/// Every duration converted here is bounded far below that by
/// `Config::validate`; saturating keeps an unvalidated value from wrapping.
fn saturating_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// One client command waiting its turn in a session's mailbox.
///
/// The mailbox is what makes a session *connection-locally ordered*: commands
/// are applied one at a time, in arrival order, and a slow Rails call or
/// backend acknowledgement delays only the session that issued it.
#[derive(Debug)]
enum SessionCommand {
    /// Authenticate the connection (dispatch mode). Enqueued once, by `accept`.
    Identify,
    /// A parsed client command.
    Client(ClientCommand),
    /// The pub/sub listener reconnected: roster notifications may have been
    /// lost, so re-announce a roster page on every granted subscription.
    PresenceResync,
}

impl SessionCommand {
    /// Short name for logs.
    const fn name(&self) -> &'static str {
        match self {
            Self::Identify => "identify",
            Self::Client(command) => command_name(command),
            Self::PresenceResync => "presence_resync",
        }
    }
}

/// Short name of a client command, for logs.
const fn command_name(command: &ClientCommand) -> &'static str {
    match command {
        ClientCommand::Subscribe { .. } => "subscribe",
        ClientCommand::Unsubscribe { .. } => "unsubscribe",
        ClientCommand::Message { .. } => "message",
        ClientCommand::Join { .. } => "join",
        ClientCommand::Leave { .. } => "leave",
        ClientCommand::Presence { .. } => "presence",
        ClientCommand::Whisper { .. } => "whisper",
    }
}

/// Which rate-limit budget a client command spends.
const fn command_class(command: &ClientCommand) -> CommandClass {
    match command {
        ClientCommand::Subscribe { .. } | ClientCommand::Unsubscribe { .. } => {
            CommandClass::Subscribe
        }
        ClientCommand::Message { .. }
        | ClientCommand::Join { .. }
        | ClientCommand::Leave { .. }
        | ClientCommand::Presence { .. }
        | ClientCommand::Whisper { .. } => CommandClass::Action,
    }
}

/// The per-session liveness gate a session task and its driver share.
///
/// Closed by `teardown` before anything else is removed: a command still in
/// flight finishes against its own deadlines, but its result is refused
/// (`is_closed`), queued commands are never run, and the task exits as soon as
/// it is idle. `generation` distinguishes two sessions that reused one
/// connection id.
#[derive(Debug)]
pub struct SessionGate {
    generation: u64,
    cancel: Shutdown,
}

impl SessionGate {
    fn new(generation: u64) -> Self {
        Self { generation, cancel: Shutdown::new() }
    }

    /// Monotonic generation of this session on its driver.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Whether the session has been torn down.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.cancel.is_triggered()
    }

    fn close(&self) {
        self.cancel.trigger();
    }

    fn subscribe(&self) -> ShutdownRx {
        self.cancel.subscribe()
    }
}

/// What the driver keeps per live session.
struct SessionHandle {
    /// Dropping the sender closes the mailbox.
    mailbox: mpsc::Sender<SessionCommand>,
    gate: Arc<SessionGate>,
}

/// Wait for the next command, or for the gate to close.
///
/// Idle-waits only: a command already handed out is never interrupted, so a
/// call in flight finishes against its own deadlines and its result is refused
/// at the hub instead. `None` once the gate closed or the mailbox was dropped;
/// a command queued behind a closed gate is dropped, never run.
async fn next_command(
    mailbox: &mut mpsc::Receiver<SessionCommand>,
    closed: &mut ShutdownRx,
    gate: &SessionGate,
    conn_id: u32,
) -> Option<SessionCommand> {
    let command = tokio::select! {
        biased;
        () = closed.wait() => return None,
        command = mailbox.recv() => command?,
    };
    if gate.is_closed() {
        debug!(conn_id, command = command.name(), "Dropping command for a closed session");
        return None;
    }
    Some(command)
}

/// Shared machinery every Rails-dispatch session task uses.
///
/// Deliberately *not* a handle to [`Connections`]: a session task holds the
/// hub, the backend, and the caller directly, so the mailbox registry cannot
/// form a reference cycle with the tasks it owns.
pub struct DispatchContext {
    hub: Arc<Hub>,
    backend: Arc<Backend>,
    rpc: Arc<RpcClient>,
    /// Local authorization for explicitly configured read-capability channels
    /// (signed Turbo/CableReady/Orbitcast names). Everything else goes to
    /// Rails: connection authentication always runs there, and local signed
    /// verification applies only to explicitly enabled profiles.
    streams: Option<Arc<StreamsController>>,
    /// Opt-in recovery on `Orbitcast::RecoveryChannel`.
    recovery: Option<Arc<RecoveryController>>,
    namespace: String,
    node_id: String,
    deadline_ms: u64,
    settings: RpcSettings,
    /// Distributed presence store; `None` when this node has none, which makes
    /// every presence operation an explicit refusal rather than a silent no-op.
    presence: Option<Arc<dyn PresenceBackend>>,
    /// Shared lease/liveness registry the batch refresher reads.
    presence_sessions: Arc<PresenceSessions>,
    /// Lease TTL, as granted leases are written with it.
    presence_ttl: Duration,
    /// Bounded, best-effort queue of `release_connection` requests.
    cleanup_tx: mpsc::Sender<RpcRequest>,
}

impl DispatchContext {
    /// Build the dispatch machinery: the caller, its cleanup worker, and the
    /// shared session context.
    #[expect(
        clippy::too_many_arguments,
        reason = "one argument per shared handle the context holds"
    )]
    fn build(
        rpc_config: crate::rpc_client::RpcConfig,
        config: &Config,
        backend: &Arc<Backend>,
        hub: &Arc<Hub>,
        streams: Option<Arc<StreamsController>>,
        recovery: Option<Arc<RecoveryController>>,
        presence_sessions: &Arc<PresenceSessions>,
        cleanup: &mut TaskGuard,
    ) -> Result<Arc<Self>, RpcError> {
        let rpc = Arc::new(RpcClient::new(rpc_config)?.with_metrics(Arc::clone(backend.metrics())));
        let (cleanup_tx, cleanup_rx) = mpsc::channel(config.rpc.cleanup_capacity.max(1));
        cleanup.push(tokio::spawn(run_cleanup_queue(Arc::clone(&rpc), cleanup_rx)));
        let deadline_ms = saturating_millis(config.rpc.deadline);

        info!(
            endpoint = rpc.endpoint(),
            deadline_ms,
            node_id = %config.rpc.node_id,
            "Rails dispatch enabled; every connection is authenticated by Rails"
        );
        if backend.rails_status() == RailsStatus::NotConfigured {
            backend.set_rails_status(RailsStatus::Probing);
        }

        Ok(Arc::new(Self {
            hub: Arc::clone(hub),
            backend: Arc::clone(backend),
            rpc,
            streams,
            recovery,
            namespace: config.namespace.clone(),
            node_id: config.rpc.node_id.clone(),
            deadline_ms: deadline_ms.max(1),
            settings: config.rpc.clone(),
            presence: backend.presence().cloned(),
            presence_sessions: Arc::clone(presence_sessions),
            presence_ttl: config.presence_ttl,
            cleanup_tx,
        }))
    }

    /// The deadline advertised to Rails: at least 1 ms, and inside the
    /// envelope's range because `Config::validate` bounds it.
    const fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }
}

impl std::fmt::Debug for DispatchContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchContext")
            .field("namespace", &self.namespace)
            .field("node_id", &self.node_id)
            .field("deadline_ms", &self.deadline_ms)
            .field("signed_streams", &self.streams.is_some())
            .field("presence", &self.presence.is_some())
            .finish_non_exhaustive()
    }
}

/// What this node holds for one accepted subscription.
#[derive(Debug, Default)]
struct SubscriptionRecord {
    params: Map<String, Value>,
    streams: BTreeSet<String>,
    state: Map<String, Value>,
    /// Locally authorized signed stream: Rails never saw this subscription, so
    /// it gets no `close_channel` or release entry.
    local: bool,
}

impl SubscriptionRecord {
    fn snapshot(&self, identifier: &str) -> SubscriptionSnapshot {
        SubscriptionSnapshot {
            identifier: identifier.to_string(),
            params: self.params.clone(),
            streams: self.streams.iter().cloned().collect(),
            state: self.state.clone(),
        }
    }
}

/// Why a reply could not be applied.
#[derive(Debug, PartialEq, Eq)]
enum ApplyError {
    /// The reply belongs to an older command or an older revision, or the
    /// session was torn down while the call was in flight.
    Stale,
    /// An effect named an identifier this session does not own.
    Unowned,
    /// The pub/sub backend refused interest; every effect was rolled back.
    Backend,
}

/// A refused presence operation, with what the refusal message may name.
#[derive(Debug)]
struct PresenceRefusal {
    error: PresenceError,
    /// The operation, when the command decoded far enough to carry one.
    op: Option<PresenceOp>,
    /// The roster, when the refusal happened on exactly one grant.
    stream: Option<String>,
}

impl PresenceRefusal {
    const fn new(error: PresenceError, op: Option<PresenceOp>) -> Self {
        Self { error, op, stream: None }
    }

    fn on(mut self, stream: &str) -> Self {
        self.stream = Some(stream.to_string());
        self
    }
}

/// One Rails-dispatch session: its state, its reducer, and its command loop.
///
/// Owned exclusively by its own task, so the state needs no locking and
/// commands cannot interleave.
struct SessionDispatch {
    context: Arc<DispatchContext>,
    conn_id: u32,
    /// Liveness gate shared with the driver; also carries the generation.
    gate: Arc<SessionGate>,
    /// Server-generated session UUID, sent with every request.
    session_id: String,
    sequence: u64,
    revision: u64,
    identified: bool,
    /// Set once this session is on its way out; queued commands then drain
    /// without touching Rails.
    closed: bool,
    /// Reason/policy reported to `release_connection`.
    close_reason: (&'static str, bool),
    connection: ConnectionSnapshot,
    subscriptions: BTreeMap<String, SubscriptionRecord>,
    /// Presence memberships Rails granted this connection. The only source of
    /// presence identity: a client contributes an operation, never a subject.
    grants: PresenceGrants,
}

impl SessionDispatch {
    fn new(
        context: Arc<DispatchContext>,
        conn_id: u32,
        gate: Arc<SessionGate>,
        path: String,
        remote_addr: String,
        headers: &HashMap<String, String>,
    ) -> Self {
        let origin = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("origin"))
            .map(|(_, value)| value.clone());

        Self {
            connection: ConnectionSnapshot {
                url: path,
                origin,
                client_address: remote_addr,
                headers: context.settings.allowlisted_headers(headers),
                identity: Map::new(),
                state: Map::new(),
            },
            context,
            conn_id,
            gate,
            session_id: uuid::Uuid::new_v4().to_string(),
            sequence: 0,
            revision: 0,
            identified: false,
            closed: false,
            close_reason: (CLOSE_REASON_REMOTE_DEFAULT, false),
            subscriptions: BTreeMap::new(),
            grants: PresenceGrants::new(),
        }
    }

    /// Process commands until the gate closes or the mailbox is dropped, then
    /// release.
    async fn run(mut self, mut mailbox: mpsc::Receiver<SessionCommand>) {
        let gate = Arc::clone(&self.gate);
        let mut closed = gate.subscribe();
        let conn_id = self.conn_id;
        while let Some(command) = next_command(&mut mailbox, &mut closed, &gate, conn_id).await {
            if self.closed {
                debug!(conn_id, command = command.name(), "Dropping command for a closing session");
                continue;
            }
            match command {
                SessionCommand::Identify => self.identify().await,
                SessionCommand::Client(command) => self.handle(command).await,
                SessionCommand::PresenceResync => self.resync_presence().await,
            }
            if gate.is_closed() {
                break;
            }
        }

        // Teardown has already stopped routing (or the mailbox was dropped):
        // nothing here can block it. The release goes onto a bounded queue and
        // this task ends.
        //
        // Presence leases go first, and they are *awaited*: a socket close must
        // drop this session's leases now, so the last tab of a subject reports
        // a leave immediately instead of waiting out the TTL. The leases are
        // per session, so another tab's rows are untouched.
        self.release_presence().await;
        self.submit_release();
    }

    /// Route one client command.
    async fn handle(&mut self, command: ClientCommand) {
        // The recovery channel is authorized by its own token, so it is routed
        // before anything else: it never reaches Rails dispatch and never
        // reaches the signed-stream controller.
        if let Some(ref recovery) = self.context.recovery
            && recovery.handles(&command)
        {
            // The channel still needs an identified connection: Rails has
            // to have welcomed it before anything is replayed.
            recovery.handle(self.conn_id, command, self.identified).await;
            return;
        }

        match command {
            ClientCommand::Subscribe { identifier } => self.subscribe(identifier).await,
            ClientCommand::Unsubscribe { identifier } => self.unsubscribe(identifier).await,
            ClientCommand::Message { identifier, data } => self.run_action(identifier, data).await,
            // Neither is a client command in dispatch mode: presence identity
            // comes only from a Rails `presence_grant` effect on a subscription,
            // and whisper is a signed-mode capability carried by the
            // `whisper: true` claim. Counted, never acted on.
            ClientCommand::Join { identifier, .. }
            | ClientCommand::Leave { identifier }
            | ClientCommand::Presence { identifier } => {
                self.context
                    .backend
                    .metrics()
                    .record_unsupported_command(UnsupportedCommand::Presence);
                debug!(
                    conn_id = self.conn_id,
                    identifier, "Dropping presence command in rails-dispatch mode"
                );
            }
            ClientCommand::Whisper { identifier, .. } => {
                self.context
                    .backend
                    .metrics()
                    .record_unsupported_command(UnsupportedCommand::Whisper);
                debug!(
                    conn_id = self.conn_id,
                    identifier, "Dropping whisper command in rails-dispatch mode"
                );
            }
        }
    }

    // --- operations ---------------------------------------------------------

    /// Authenticate the connection; send `welcome` only once Rails accepts.
    ///
    /// A `denied` outcome is final (`unauthorized`, no reconnect): the
    /// credentials are wrong. A `fault` is Rails failing, not the client: the
    /// session is closed with `reconnect: true`, never `unauthorized`.
    async fn identify(&mut self) {
        let request =
            self.next_request(Operation::Identify).with_connection(self.connection.clone());

        let Some(reply) = self.call(request).await else {
            return;
        };

        match reply.outcome {
            Outcome::Accepted => {}
            Outcome::Denied => {
                debug!(
                    conn_id = self.conn_id,
                    reason = reply.reason.as_deref().unwrap_or_default(),
                    "Connection identification denied"
                );
                self.close(CLOSE_REASON_UNAUTHORIZED, false).await;
                return;
            }
            Outcome::Fault => {
                warn!(
                    conn_id = self.conn_id,
                    reason = reply.reason.as_deref().unwrap_or_default(),
                    "Connection identification faulted on the Rails side; closing for a retry"
                );
                self.fail_temporarily().await;
                return;
            }
        }

        if self.apply(&reply, None).await.is_err() {
            self.fail_temporarily().await;
            return;
        }

        self.identified = true;
        self.index_identity();

        let welcome = ServerMessage::Welcome { sid: self.session_id.clone() };
        self.context.hub.send(self.conn_id, &actioncable::encode(&welcome)).await;
        debug!(conn_id = self.conn_id, "Rails-dispatch connection identified");
    }

    /// Authorize and install one subscription.
    async fn subscribe(&mut self, identifier: String) {
        if !self.identified {
            debug!(conn_id = self.conn_id, "Rejecting subscribe before identification");
            self.reject(&identifier).await;
            return;
        }

        // Duplicate subscribe: re-confirm, install nothing, and do not run the
        // Rails `subscribed` callback a second time.
        if self.subscriptions.contains_key(&identifier) {
            debug!(conn_id = self.conn_id, "Duplicate subscribe; re-confirming");
            self.confirm(&identifier).await;
            return;
        }

        if self.subscribe_signed(&identifier).await {
            return;
        }

        let snapshot = SubscriptionSnapshot::new(identifier.clone());
        let request = self
            .next_request(Operation::OpenChannel)
            .with_connection(self.connection.clone())
            .with_subscription(snapshot.clone());

        let Some(reply) = self.call(request).await else {
            return;
        };

        match reply.outcome {
            Outcome::Accepted => {}
            Outcome::Denied => {
                debug!(conn_id = self.conn_id, "Subscription denied");
                self.reject(&identifier).await;
                return;
            }
            Outcome::Fault => {
                warn!(
                    conn_id = self.conn_id,
                    "Subscription faulted on the Rails side; closing for a retry"
                );
                self.reject(&identifier).await;
                self.fail_temporarily().await;
                return;
            }
        }

        // The record is installed before the effects are applied so an
        // `add_stream` naming this identifier is owned rather than rejected.
        self.subscriptions.insert(
            identifier.clone(),
            SubscriptionRecord { params: snapshot.params, ..SubscriptionRecord::default() },
        );
        if let Err(error) = self.context.hub.ensure_subscription(self.conn_id, &identifier) {
            debug!(conn_id = self.conn_id, %error, "Subscription arrived for a torn-down session");
            self.subscriptions.remove(&identifier);
            return;
        }

        if let Err(error) = self.apply(&reply, Some(&identifier)).await {
            // Nothing was installed: the readiness barrier refused, or the
            // reply was not usable. The client is told no rather than being
            // confirmed onto a subscription that receives nothing.
            warn!(conn_id = self.conn_id, ?error, "Subscription reply could not be applied");
            self.subscriptions.remove(&identifier);
            let released = self.context.hub.remove_subscription(self.conn_id, &identifier);
            self.context.backend.release(released).await;
            self.reject(&identifier).await;
            return;
        }

        // Readiness barrier: the confirmation goes out first, then the
        // deliveries buffered while the streams were pending, in order.
        self.confirm(&identifier).await;
        self.context.hub.activate(self.conn_id, &identifier);
    }

    /// Authorize a signed stream locally, if this identifier is one.
    ///
    /// Returns `true` when the command was handled here. Local verification is
    /// limited to the explicitly configured read-capability profiles; every
    /// other channel is authorized by Rails.
    async fn subscribe_signed(&mut self, identifier: &str) -> bool {
        let Some(ref controller) = self.context.streams else {
            return false;
        };
        if !controller.handles(identifier) {
            return false;
        }

        let stream = match controller.resolve(identifier) {
            Ok(result) => result.stream,
            Err(e) => {
                debug!(conn_id = self.conn_id, error = %e, "Signed stream verification failed");
                self.reject(identifier).await;
                return true;
            }
        };

        let hub = &self.context.hub;
        match hub
            .join(self.conn_id, identifier, &stream, self.context.backend.acquire(&stream))
            .await
        {
            Ok(()) => {}
            Err(HubError::UnknownSession(_)) => {
                // Torn down mid-acquire: the interest we took is ours to give
                // back, and nobody is left to confirm.
                self.context.backend.release(vec![stream]).await;
                return true;
            }
            Err(HubError::Refused(_)) => {
                self.reject(identifier).await;
                return true;
            }
        }

        let mut record = SubscriptionRecord { local: true, ..SubscriptionRecord::default() };
        record.streams.insert(stream);
        self.subscriptions.insert(identifier.to_string(), record);

        self.confirm(identifier).await;
        self.context.hub.activate(self.conn_id, identifier);
        true
    }

    /// Tear a subscription down locally, then tell Rails.
    ///
    /// Local removal happens first and unconditionally: Rails cannot veto a
    /// client leaving, and a failed or slow `unsubscribed` callback must not
    /// keep delivering to a subscription the client has abandoned.
    async fn unsubscribe(&mut self, identifier: String) {
        let Some(record) = self.subscriptions.remove(&identifier) else {
            debug!(conn_id = self.conn_id, "Unsubscribe for an unknown subscription");
            return;
        };

        let snapshot = record.snapshot(&identifier);
        let released = self.context.hub.remove_subscription(self.conn_id, &identifier);
        self.context.backend.release(released).await;

        // Unsubscribe drops this subscription's grants and leases, like a
        // close does for the whole connection. The roster memberships went
        // with the subscription above; the release path finds nothing left to
        // remove from the hub and only releases the leases.
        let dropped = self.grants.remove_identifier(&identifier);
        self.release_grants(&identifier, dropped).await;

        if record.local {
            // Rails never ran `subscribed` for it, so there is nothing to
            // unwind on that side.
            return;
        }

        let request = self
            .next_request(Operation::CloseChannel)
            .with_connection(self.connection.clone())
            .with_subscription(snapshot);

        if let Some(reply) = self.call(request).await {
            match reply.outcome {
                Outcome::Accepted => {
                    // The subscription is already gone; only connection-level
                    // effects of this reply can still apply. A refusal is
                    // ignored on purpose: `apply` already logged it, and there
                    // is nothing left to undo.
                    let _ = self.apply(&reply, None).await;
                }
                Outcome::Denied => {
                    debug!(
                        conn_id = self.conn_id,
                        "close_channel denied; the subscription is gone regardless"
                    );
                }
                Outcome::Fault => {
                    // Rails could not run `unsubscribed`; the client has left
                    // regardless, and this is recorded, not retried.
                    warn!(conn_id = self.conn_id, "close_channel faulted on the Rails side");
                }
            }
        }
    }

    /// Dispatch one `perform` through Rails.
    async fn run_action(&mut self, identifier: String, data: String) {
        let Some(record) = self.subscriptions.get(&identifier) else {
            // Never invoke an action before the subscription was accepted.
            debug!(conn_id = self.conn_id, "Dropping action for an unconfirmed subscription");
            return;
        };

        if record.local {
            debug!(
                conn_id = self.conn_id,
                "Dropping action for a locally authorized signed stream (read capability only)"
            );
            return;
        }

        let snapshot = record.snapshot(&identifier);

        // The reserved presence namespace is answered here and never
        // dispatched: Rails made its decision when it issued the grant, and an
        // application channel may not define an `orbitcast_presence` action.
        if is_presence_action(&data) {
            self.run_presence(&identifier, &data).await;
            return;
        }

        let request = self
            .next_request(Operation::RunAction)
            .with_connection(self.connection.clone())
            .with_subscription(snapshot)
            .with_action(ActionPayload { identifier: identifier.clone(), json_data: data });

        let Some(reply) = self.call(request).await else {
            return;
        };

        match reply.outcome {
            Outcome::Accepted => {}
            Outcome::Denied => {
                debug!(conn_id = self.conn_id, "Action denied");
                return;
            }
            Outcome::Fault => {
                warn!(
                    conn_id = self.conn_id,
                    "Action faulted on the Rails side; closing for a retry"
                );
                self.fail_temporarily().await;
                return;
            }
        }

        if let Err(error) = self.apply(&reply, Some(&identifier)).await {
            warn!(conn_id = self.conn_id, ?error, "Action reply could not be applied");
            return;
        }
        // Streams an action added join an already-confirmed subscription:
        // nothing precedes their deliveries, so they are released at once.
        self.context.hub.activate(self.conn_id, &identifier);
    }

    // --- the single reducer -------------------------------------------------

    /// Apply one accepted reply: effects in order, then state and revision.
    ///
    /// `subject` is the identifier the request was about, if any; it is the
    /// only identifier a `subscription_state` replacement may target.
    ///
    /// Ownership of every effect identifier is checked *before* anything is
    /// installed, and an effect that cannot be applied rolls the whole reply
    /// back: a half-applied reply would leave memberships Rails does not know
    /// about. A reply arriving after the session's gate closed is stale too:
    /// the hub no longer knows the session, and a reused connection id must
    /// never receive an older generation's result.
    async fn apply(&mut self, reply: &RpcReply, subject: Option<&str>) -> Result<(), ApplyError> {
        let other_command = reply.sequence != self.sequence;
        let revision_regressed = reply.new_revision < self.revision;
        let torn_down = self.gate.is_closed();
        if other_command || revision_regressed || torn_down {
            warn!(
                conn_id = self.conn_id,
                generation = self.gate.generation(),
                expected_sequence = self.sequence,
                reply_sequence = reply.sequence,
                torn_down,
                "Discarding a stale reply"
            );
            return Err(ApplyError::Stale);
        }

        for effect in &reply.effects {
            if let Some(identifier) = effect.identifier()
                && !self.subscriptions.contains_key(identifier)
            {
                warn!(
                    conn_id = self.conn_id,
                    kind = effect.kind(),
                    "Rejecting an effect for a subscription this connection does not own"
                );
                return Err(ApplyError::Unowned);
            }
        }

        let mut installed: Vec<(String, String)> = Vec::new();
        let mut close: Option<(String, bool)> = None;

        for effect in &reply.effects {
            match effect {
                Effect::AddStream { identifier, stream } => {
                    if !self.add_stream(identifier, stream).await {
                        self.rollback(installed).await;
                        return Err(ApplyError::Backend);
                    }
                    installed.push((identifier.clone(), stream.clone()));
                }
                Effect::RemoveStream { identifier, stream } => {
                    self.remove_stream(identifier, stream).await;
                }
                Effect::RemoveAllStreams { identifier } => {
                    if let Some(record) = self.subscriptions.get_mut(identifier) {
                        record.streams.clear();
                    }
                    // The roster keys are hub memberships too, so they are
                    // cleared here with everything else; the grants and their
                    // leases are released by the same path every other
                    // removal uses.
                    let released = self.context.hub.clear_streams(self.conn_id, identifier);
                    self.context.backend.release(released).await;
                    self.drop_grants_without_streams(identifier).await;
                }
                Effect::Transmit { identifier, json_payload } => {
                    let frame = encoder::addressed_frame(
                        &encoder::encode_identifier(identifier),
                        json_payload.as_bytes(),
                    );
                    self.context.hub.send_bytes(self.conn_id, frame).await;
                }
                Effect::Close { reason, reconnect } => {
                    // Recorded, not applied mid-list: the remaining effects are
                    // queued ahead of the disconnect frame on the same ordered
                    // outbound queue, so the client sees them first.
                    close = Some((reason.clone(), *reconnect));
                }
                Effect::PresenceGrant {
                    identifier,
                    stream,
                    subject_id,
                    metadata,
                    ttl_ms,
                    operations,
                } => {
                    self.grant_presence(
                        identifier, stream, subject_id, metadata, *ttl_ms, operations,
                    )
                    .await;
                }
                Effect::PresenceRevoke { identifier, stream, subject_id } => {
                    self.revoke_presence(identifier, stream, subject_id).await;
                }
            }
        }

        if let StateReplacement::Replace(ref state) = reply.state {
            self.connection.state.clone_from(state);
        }
        if let Some(identifier) = subject
            && let StateReplacement::Replace(ref state) = reply.subscription_state
            && let Some(record) = self.subscriptions.get_mut(identifier)
        {
            record.state.clone_from(state);
        }
        if !reply.identity.is_empty() {
            self.connection.identity.clone_from(&reply.identity);
        }
        self.revision = reply.new_revision;

        if let Some((reason, reconnect)) = close {
            self.close_owned(reason, reconnect).await;
        }

        Ok(())
    }

    /// Install one stream, acquiring backend interest before it counts.
    ///
    /// Returns `false` when the backend refused: the membership is removed
    /// again, because a confirmed-but-silent subscription is worse than a
    /// rejected one.
    async fn add_stream(&mut self, identifier: &str, stream: &str) -> bool {
        let Some(record) = self.subscriptions.get_mut(identifier) else {
            return false;
        };
        if !record.streams.insert(stream.to_string()) {
            return true;
        }

        let hub = &self.context.hub;
        match hub.join(self.conn_id, identifier, stream, self.context.backend.acquire(stream)).await
        {
            Ok(()) => true,
            Err(error) => {
                if matches!(error, HubError::UnknownSession(_)) {
                    // Torn down mid-acquire: the interest is ours to return.
                    self.context.backend.release(vec![stream.to_string()]).await;
                }
                if let Some(record) = self.subscriptions.get_mut(identifier) {
                    record.streams.remove(stream);
                }
                false
            }
        }
    }

    /// Drop one stream from an owned subscription, and with it any presence
    /// grant that was bound to it.
    async fn remove_stream(&mut self, identifier: &str, stream: &str) {
        if let Some(record) = self.subscriptions.get_mut(identifier) {
            record.streams.remove(stream);
        }
        if self.context.hub.remove_membership(self.conn_id, identifier, stream) {
            self.context.backend.release(vec![stream.to_string()]).await;
        }
        self.drop_grants_without_streams(identifier).await;
    }

    /// Undo the streams installed by a reply that then failed.
    async fn rollback(&mut self, installed: Vec<(String, String)>) {
        for (identifier, stream) in installed {
            self.remove_stream(&identifier, &stream).await;
        }
    }

    // --- presence -----------------------------------------------------------

    /// Register one granted presence membership for `(connection, identifier,
    /// stream)`.
    ///
    /// A grant this node cannot honour - unusable values, no presence store,
    /// a stream the subscription does not hold, or a backend that refuses the
    /// roster subscription - is refused locally and loudly, and the rest of
    /// the reply still applies: a roster that cannot be delivered must not
    /// cost the client its subscription.
    ///
    /// The roster itself arrives as an ordinary publication: the derived
    /// presence key is installed as a hub membership of this subscription, so
    /// interest is refcounted, addressed, and released by the machinery that
    /// already exists, and only granted subscriptions ever see the events. It
    /// deliberately does *not* join the subscription's `streams` set, which is
    /// Rails' own view of what it installed.
    ///
    /// A grant that replaces one the subscription already holds for the same
    /// stream carries the live lease over: a different subject leaves first
    /// (its leave is announced) and the new subject joins; the same subject
    /// re-joins, which updates its metadata and TTL.
    async fn grant_presence(
        &mut self,
        identifier: &str,
        stream: &str,
        subject_id: &str,
        metadata: &Map<String, Value>,
        ttl_ms: u64,
        operations: &[String],
    ) {
        match self.install_grant(identifier, stream, subject_id, metadata, ttl_ms, operations).await
        {
            Ok(()) => {}
            Err(error) => {
                warn!(
                    conn_id = self.conn_id,
                    stream,
                    reason = error.code(),
                    %error,
                    "Refusing a presence grant"
                );
            }
        }
    }

    /// [`Self::grant_presence`], with the refusal as a value.
    async fn install_grant(
        &mut self,
        identifier: &str,
        stream: &str,
        subject_id: &str,
        metadata: &Map<String, Value>,
        ttl_ms: u64,
        operations: &[String],
    ) -> Result<(), PresenceError> {
        // Rails may shorten a lease, never lengthen it past this node's
        // configured bound: the TTL is what caps staleness after a node dies.
        let ttl_ms = ttl_ms.min(saturating_millis(self.context.presence_ttl));
        let grant = PresenceGrant::from_effect(stream, subject_id, metadata, ttl_ms, operations)?;

        if self.context.presence.is_none() {
            return Err(PresenceError::Unavailable);
        }

        // A roster is granted on a stream the subscription streams from: the
        // grant is bound to that membership and goes away with it.
        let holds_stream = self
            .subscriptions
            .get(identifier)
            .is_some_and(|record| record.streams.contains(stream));
        if !holds_stream {
            return Err(PresenceError::StreamNotSubscribed { stream: stream.to_string() });
        }

        let key = presence_stream_key(stream);
        let hub = &self.context.hub;
        match hub.join(self.conn_id, identifier, &key, self.context.backend.acquire(&key)).await {
            Ok(()) => {}
            Err(HubError::UnknownSession(conn_id)) => {
                self.context.backend.release(vec![key]).await;
                return Err(PresenceError::Backend(anyhow::anyhow!(
                    "session {conn_id} was torn down while its roster subscription was acquired"
                )));
            }
            Err(HubError::Refused(key)) => {
                return Err(PresenceError::Backend(anyhow::anyhow!(
                    "the pub/sub backend refused interest in {key:?}"
                )));
            }
        }

        let Some(replaced) = self.grants.insert(identifier, grant.clone()) else {
            return Ok(());
        };
        if !replaced.joined {
            return Ok(());
        }

        // The session was present under the displaced grant: carry the lease
        // over rather than leaving it to expire under a subject Rails no
        // longer vouches for.
        if replaced.grant.subject_id != grant.subject_id {
            self.leave_lease(&replaced.grant).await;
        }
        self.join_lease(identifier, &grant).await?;
        Ok(())
    }

    /// Claim this session's lease under `grant`, recording it for refresh.
    async fn join_lease(
        &mut self,
        identifier: &str,
        grant: &PresenceGrant,
    ) -> Result<(), PresenceError> {
        let presence = self.context.presence.clone().ok_or(PresenceError::Unavailable)?;
        let lease = grant.lease(&self.context.namespace, &self.session_id, &self.context.node_id);
        presence.join(&lease).await?;
        // Held after the write: the frame that asked to join is itself verified
        // inbound activity, so the lease is refreshable from the moment it
        // exists.
        self.context.presence_sessions.hold(self.conn_id, &lease);
        self.grants.set_joined(identifier, &grant.stream, true);
        Ok(())
    }

    /// Best-effort release of this session's lease under `grant`.
    ///
    /// A failure is recorded, not retried: the lease expires on its own, which
    /// is exactly the bound a dead node relies on. Either way the lease is no
    /// longer refreshed or counted here.
    async fn leave_lease(&self, grant: &PresenceGrant) {
        let key = grant.key(&self.context.namespace);
        let was_held = self.context.presence_sessions.release(self.conn_id, &key);
        debug!(
            conn_id = self.conn_id,
            stream = %grant.stream,
            was_held,
            "Releasing a presence lease"
        );

        let Some(ref presence) = self.context.presence else {
            return;
        };
        if let Err(error) = presence.leave(&key, &self.session_id).await {
            warn!(
                conn_id = self.conn_id,
                stream = %grant.stream,
                %error,
                "Presence lease could not be released; it will expire"
            );
        }
    }

    /// Drop a granted membership: the lease goes, and so does the roster
    /// subscription it installed.
    async fn revoke_presence(&mut self, identifier: &str, stream: &str, subject_id: &str) {
        let Some(revoked) = self.grants.revoke(identifier, stream, subject_id) else {
            debug!(
                conn_id = self.conn_id,
                stream, "Ignoring a presence revoke for a grant this connection does not hold"
            );
            return;
        };

        self.release_grants(identifier, vec![revoked]).await;
    }

    /// The one release path for grants that are no longer in force: revoke,
    /// unsubscribe, socket close, and a subscription that stopped streaming
    /// from the granted stream (`stop_stream_from`, `stop_all_streams`).
    ///
    /// The lease is released only when the session had joined; the roster
    /// membership is removed when it is still installed (an unsubscribe or
    /// close already took it with the subscription).
    async fn release_grants(&self, identifier: &str, dropped: Vec<ReplacedGrant>) {
        for ReplacedGrant { grant, joined } in dropped {
            if joined {
                self.leave_lease(&grant).await;
            }
            let key = presence_stream_key(&grant.stream);
            if self.context.hub.remove_membership(self.conn_id, identifier, &key) {
                self.context.backend.release(vec![key]).await;
            }
        }
    }

    /// Release every grant of `identifier` whose stream the subscription no
    /// longer holds.
    async fn drop_grants_without_streams(&mut self, identifier: &str) {
        let streams = self.subscriptions.get(identifier).map(|record| record.streams.clone());
        let dropped = self.grants.remove_where(identifier, |grant| {
            !streams.as_ref().is_some_and(|streams| streams.contains(&grant.stream))
        });
        if dropped.is_empty() {
            return;
        }
        debug!(
            conn_id = self.conn_id,
            grants = dropped.len(),
            "Releasing presence grants whose streams the subscription stopped"
        );
        self.release_grants(identifier, dropped).await;
    }

    /// Drop every grant and lease this session holds (socket close, drain).
    async fn release_presence(&mut self) {
        let identifiers: BTreeSet<String> =
            self.grants.all().into_iter().map(|(identifier, _)| identifier).collect();
        for identifier in identifiers {
            let dropped = self.grants.remove_identifier(&identifier);
            self.release_grants(&identifier, dropped).await;
        }
    }

    /// Apply one `orbitcast_presence` operation, answering a refusal.
    ///
    /// A refusal is a decision about this client, not an error the connection
    /// is punished for: the client is told why on the same subscription, and
    /// the session continues.
    async fn run_presence(&mut self, identifier: &str, data: &str) {
        match self.apply_presence(identifier, data).await {
            Ok(applied) => {
                debug!(conn_id = self.conn_id, applied, "Presence operation applied");
            }
            Err(refusal) => {
                debug!(
                    conn_id = self.conn_id,
                    reason = refusal.error.code(),
                    error = %refusal.error,
                    "Presence operation refused"
                );
                let message = PresenceMessage::error(
                    refusal.error.code(),
                    refusal.op,
                    refusal.stream.as_deref(),
                );
                self.send_presence_message(identifier, &message).await;
            }
        }
    }

    /// The presence reducer: authorize, then act, entirely inside the grant.
    ///
    /// Returns how many granted memberships the operation was applied to (one,
    /// unless a single subscription holds grants for several streams).
    async fn apply_presence(
        &mut self,
        identifier: &str,
        data: &str,
    ) -> Result<usize, PresenceRefusal> {
        let command = PresenceCommand::decode(data)
            .map_err(|error| PresenceRefusal::new(PresenceError::InvalidCommand(error), None))?;
        let op = Some(command.op);
        let presence = self
            .context
            .presence
            .clone()
            .ok_or_else(|| PresenceRefusal::new(PresenceError::Unavailable, op))?;

        // Identity and metadata come from here and nowhere else. An ungranted
        // subscription cannot join at all, and a grant that does not list the
        // operation cannot run it.
        let grants = self.grants.authorize(identifier, command.op).map_err(|error| {
            // A refusal on one grant names its roster; a subscription holding
            // several grants gets the reason alone.
            let refusal = PresenceRefusal::new(error, op);
            match self.grants.for_identifier(identifier).as_slice() {
                [only] => refusal.on(&only.stream),
                _ => refusal,
            }
        })?;

        for grant in &grants {
            let refuse = |error: PresenceError| PresenceRefusal::new(error, op).on(&grant.stream);
            match command.op {
                PresenceOp::Join => {
                    self.join_lease(identifier, grant).await.map_err(refuse)?;
                }
                PresenceOp::Leave => {
                    let key = grant.key(&self.context.namespace);
                    // Not refreshed or counted from here on, whether or not the
                    // store still has the row; the store answers that below.
                    let was_held = self.context.presence_sessions.release(self.conn_id, &key);
                    self.grants.set_joined(identifier, &grant.stream, false);
                    debug!(conn_id = self.conn_id, was_held, "Client leaving its presence lease");
                    presence.leave(&key, &self.session_id).await.map_err(refuse)?;
                }
                PresenceOp::Snapshot => {
                    let page = command.page();
                    let snapshot = presence.snapshot(&grant.stream, page).await.map_err(refuse)?;
                    self.send_snapshot(identifier, &grant.stream, &snapshot, page).await;
                }
            }
        }

        Ok(grants.len())
    }

    /// Re-announce a roster page on every granted subscription that may read
    /// one (pub/sub reconnect: notifications were lost, so the client
    /// reconciles against a fresh snapshot rather than against silence).
    ///
    /// Only grants whose allowlist includes `snapshot` receive a page: the
    /// allowlist is the authorization boundary for reads, server-initiated or
    /// not. Other grants keep receiving transitions from here on.
    async fn resync_presence(&self) {
        let Some(presence) = self.context.presence.clone() else {
            return;
        };
        for (identifier, grant) in self.grants.all() {
            if !grant.allows(PresenceOp::Snapshot) {
                continue;
            }
            let page = crate::protocol::v1::presence::PresencePage::default();
            match presence.snapshot(&grant.stream, page).await {
                Ok(snapshot) => {
                    self.send_snapshot(&identifier, &grant.stream, &snapshot, page).await;
                }
                Err(error) => warn!(
                    conn_id = self.conn_id,
                    stream = %grant.stream,
                    %error,
                    "Presence roster could not be re-announced after a reconnect"
                ),
            }
        }
    }

    /// Send one roster page to the requesting subscription only.
    async fn send_snapshot(
        &self,
        identifier: &str,
        stream: &str,
        snapshot: &crate::presence::PresenceSnapshot,
        page: crate::protocol::v1::presence::PresencePage,
    ) {
        let message = PresenceMessage::snapshot(
            stream,
            snapshot.members.clone(),
            snapshot.total,
            crate::protocol::v1::presence::PresencePageInfo {
                limit: page.limit,
                offset: page.offset,
                has_more: snapshot.has_more,
            },
        );
        self.send_presence_message(identifier, &message).await;
    }

    /// Send one presence message, addressed with `identifier`, to this
    /// connection only.
    async fn send_presence_message(&self, identifier: &str, message: &PresenceMessage) {
        let json = match message.to_json() {
            Ok(json) => json,
            Err(error) => {
                warn!(conn_id = self.conn_id, %error, "Could not encode a presence message");
                return;
            }
        };

        let frame =
            encoder::addressed_frame(&encoder::encode_identifier(identifier), json.as_bytes());
        self.context.hub.send_bytes(self.conn_id, frame).await;
    }

    // --- calling ------------------------------------------------------------

    /// Build the next request for this session, advancing the sequence.
    fn next_request(&mut self, operation: Operation) -> RpcRequest {
        self.sequence += 1;
        RpcRequest::new(
            operation,
            self.context.namespace.clone(),
            self.context.node_id.clone(),
            self.session_id.clone(),
            self.sequence,
            self.revision,
            self.context.deadline_ms(),
        )
    }

    /// Run one request, closing the session when there is no usable reply.
    ///
    /// Returns `None` on failure. There is **no** fallback to local acceptance:
    /// a Rails outage closes sessions. The close always invites a reconnect,
    /// because the client did nothing wrong - a contract
    /// violation on the Rails side is an operator problem, and its own reconnect
    /// backoff bounds the retries.
    async fn call(&mut self, request: RpcRequest) -> Option<RpcReply> {
        let operation = request.operation;
        match self.context.rpc.call(request).await {
            Ok(reply) => Some(reply),
            Err(error) => {
                if error.is_temporary() {
                    warn!(
                        conn_id = self.conn_id,
                        %operation,
                        %error,
                        "Rails dispatch failed; closing the session"
                    );
                } else {
                    // Not availability: the endpoint answered with something
                    // this build refuses to apply. Loud, because it is a bug or
                    // a broken deploy rather than a blip.
                    error!(
                        conn_id = self.conn_id,
                        %operation,
                        %error,
                        "Rails dispatch returned an unusable reply; closing the session"
                    );
                }
                self.fail_temporarily().await;
                None
            }
        }
    }

    // --- framing ------------------------------------------------------------

    async fn confirm(&self, identifier: &str) {
        let confirm = ServerMessage::ConfirmSubscription { identifier: identifier.to_string() };
        self.context.hub.send(self.conn_id, &actioncable::encode(&confirm)).await;
    }

    async fn reject(&self, identifier: &str) {
        let reject = ServerMessage::RejectSubscription { identifier: identifier.to_string() };
        self.context.hub.send(self.conn_id, &actioncable::encode(&reject)).await;
    }

    /// Close after a temporary backend failure: come back, do not give up.
    async fn fail_temporarily(&mut self) {
        self.close(CLOSE_REASON_SERVER_RESTART, true).await;
    }

    /// Close with an allowlisted reason owned by this build.
    async fn close(&mut self, reason: &'static str, reconnect: bool) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.close_reason = (reason, reconnect);

        let disconnect = ServerMessage::Disconnect { reason: reason.to_string(), reconnect };
        self.context.hub.send(self.conn_id, &actioncable::encode(&disconnect)).await;
        // Travels through the same outbound queue as the frame above, so the
        // client always sees the ActionCable disconnect before the socket goes.
        self.context.hub.disconnect(self.conn_id, CLOSE_NORMAL, reason).await;
    }

    /// Close for a reason Rails asked for; it is already allowlisted by the
    /// envelope validator, which is why a borrowed `&'static str` is recovered
    /// here instead of trusting the string.
    async fn close_owned(&mut self, reason: String, reconnect: bool) {
        let reason = crate::protocol::v1::rpc::ALLOWED_CLOSE_REASONS
            .iter()
            .find(|allowed| **allowed == reason)
            .copied()
            .unwrap_or(CLOSE_REASON_SERVER_RESTART);
        self.close(reason, reconnect).await;
    }

    // --- identity and release ------------------------------------------------

    /// Index this session by its canonical identity key, so a Rails-initiated
    /// remote disconnect can find it without scanning every session.
    fn index_identity(&self) {
        if let Some(key) = identity_key(&self.connection.identity)
            && let Err(error) = self.context.hub.set_identifier(self.conn_id, key)
        {
            debug!(conn_id = self.conn_id, %error, "Identity not indexed: session torn down");
        }
    }

    /// Hand the final snapshot to the bounded cleanup queue.
    ///
    /// Best effort by definition: process death, a full queue, or a Rails
    /// outage all mean the disconnect callback does not run. Nothing
    /// business-critical may depend on it.
    fn submit_release(&mut self) {
        if !self.identified {
            return;
        }

        let (reason, reconnect) = self.close_reason;
        let subscriptions = self
            .subscriptions
            .iter()
            .filter(|(_, record)| !record.local)
            .map(|(identifier, record)| record.snapshot(identifier))
            .collect();

        let request = self
            .next_request(Operation::ReleaseConnection)
            .with_connection(self.connection.clone())
            .with_release(ReleasePayload { reason: reason.to_string(), reconnect, subscriptions });

        match self.context.cleanup_tx.try_send(request) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(conn_id = self.conn_id, "Cleanup queue full; disconnect callback skipped");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!(conn_id = self.conn_id, "Cleanup queue closed; disconnect callback skipped");
            }
        }
    }
}

/// The close reason recorded for a session that simply went away.
const CLOSE_REASON_REMOTE_DEFAULT: &str = crate::protocol::v1::rpc::CLOSE_REASON_REMOTE;

/// The canonical key a session is indexed by for remote disconnect.
///
/// Rails should send an explicit `identity_key` attribute; when it does not,
/// the whole identity map is canonicalized instead (`serde_json` maps are
/// key-sorted, so this is stable). Returns `None` for an anonymous connection,
/// which is therefore not remotely addressable - by design.
fn identity_key(identity: &Map<String, Value>) -> Option<String> {
    if identity.is_empty() {
        return None;
    }
    if let Some(Value::String(key)) = identity.get("identity_key")
        && !key.is_empty()
    {
        return Some(key.clone());
    }
    // Serializing a map of JSON values cannot fail; `ok()` only fits the
    // result to the `Option` this returns.
    serde_json::to_string(identity).ok()
}

/// Drain the bounded cleanup queue, running `release_connection` per job.
///
/// One worker, so disconnect callbacks cannot outcompete live traffic for the
/// RPC concurrency budget.
async fn run_cleanup_queue(rpc: Arc<RpcClient>, mut jobs: mpsc::Receiver<RpcRequest>) {
    while let Some(request) = jobs.recv().await {
        let session_id = request.session_id.clone();
        match rpc.call(request).await {
            Ok(reply) if reply.is_accepted() => {
                debug!(session_id, "Release callbacks completed");
            }
            Ok(reply) => {
                debug!(session_id, outcome = %reply.outcome, "Release callbacks refused");
            }
            Err(error) => {
                // Recorded, never retried: disconnect callbacks can have side
                // effects, so an ambiguous failure is reported, not replayed.
                warn!(session_id, %error, "Release callbacks failed");
            }
        }
    }
}

/// Probe the Rails endpoint with `describe` at startup, every
/// [`DESCRIBE_INTERVAL`], and immediately after the circuit breaker leaves its
/// open state, recording the verdict on the backend.
async fn probe_rails(context: Arc<DispatchContext>) {
    let rpc = &context.rpc;
    loop {
        let status = match rpc.describe(&context.namespace, &context.node_id).await {
            Ok(Described::Admitted(info)) => {
                debug!(
                    software_version = %info.software_version,
                    capabilities = info.capabilities.len(),
                    "Rails describe handshake passed"
                );
                RailsStatus::Admitted
            }
            Ok(Described::Refused(refusal)) => RailsStatus::Refused(refusal),
            Err(error) => {
                debug!(%error, "Rails describe probe failed");
                RailsStatus::Unreachable
            }
        };
        context.backend.set_rails_status(status);

        // Wait for the next tick, or for the circuit to leave `open`: a
        // half-open circuit admits a probe, and this one should be it.
        let next_tick = tokio::time::sleep(DESCRIBE_INTERVAL);
        tokio::pin!(next_tick);
        let mut was_open = rpc.is_circuit_open();
        loop {
            tokio::select! {
                () = &mut next_tick => break,
                () = tokio::time::sleep(CIRCUIT_WATCH_INTERVAL) => {
                    let open = rpc.is_circuit_open();
                    if was_open && !open {
                        debug!(state = rpc.circuit_state(), "Rails circuit left open; re-probing");
                        break;
                    }
                    was_open = open;
                }
            }
        }
    }
}

/// Shared machinery every signed-only session task uses.
struct SignedContext {
    hub: Arc<Hub>,
    backend: Arc<Backend>,
    streams: Option<Arc<StreamsController>>,
    recovery: Option<Arc<RecoveryController>>,
}

/// One signed-only session: local authorization, no Rails.
struct SignedSession {
    context: Arc<SignedContext>,
    conn_id: u32,
    gate: Arc<SessionGate>,
    /// `perform` is not executable here; the first one is warned about, the
    /// rest only counted.
    warned_unsupported_perform: bool,
}

impl SignedSession {
    async fn run(mut self, mut mailbox: mpsc::Receiver<SessionCommand>) {
        let gate = Arc::clone(&self.gate);
        let mut closed = gate.subscribe();
        let conn_id = self.conn_id;
        while let Some(command) = next_command(&mut mailbox, &mut closed, &gate, conn_id).await {
            match command {
                // Nothing to identify and no grants to resync in signed mode.
                SessionCommand::Identify | SessionCommand::PresenceResync => {}
                SessionCommand::Client(command) => self.handle(command).await,
            }
            if gate.is_closed() {
                break;
            }
        }
    }

    async fn handle(&mut self, command: ClientCommand) {
        let conn_id = self.conn_id;
        let hub = &self.context.hub;

        // The recovery channel is authorized by its own token, so it is routed
        // before anything else. A node without recovery configured has no
        // such channel, and the command falls through to the ordinary
        // "unknown channel" rejection.
        if let Some(ref recovery) = self.context.recovery
            && recovery.handles(&command)
        {
            // Signed mode welcomes every accepted socket before its first
            // command is routed, so the connection is identified by here.
            recovery.handle(conn_id, command, true).await;
            return;
        }

        match command {
            ClientCommand::Subscribe { identifier } => {
                // Signed stream subscriptions are authorized locally.
                if let Some(ref controller) = self.context.streams
                    && controller.handles(&identifier)
                {
                    self.subscribe(controller, identifier).await;
                    return;
                }

                debug!(conn_id, identifier, "Rejecting non-signed subscription in standalone mode");
                let reject = ServerMessage::RejectSubscription { identifier };
                hub.send(conn_id, &actioncable::encode(&reject)).await;
            }

            ClientCommand::Unsubscribe { identifier } => {
                self.unsubscribe(&identifier).await;
            }

            ClientCommand::Message { identifier, data } => {
                // Signed mode runs no Ruby: a `perform` has nothing to reach.
                // Counted every time, warned once per session so the log says
                // what the application is missing without repeating itself.
                self.context
                    .backend
                    .metrics()
                    .record_unsupported_command(UnsupportedCommand::Message);
                if self.warned_unsupported_perform {
                    debug!(
                        conn_id,
                        identifier,
                        bytes = data.len(),
                        "Dropping perform message in standalone mode"
                    );
                } else {
                    self.warned_unsupported_perform = true;
                    warn!(
                        conn_id,
                        identifier,
                        bytes = data.len(),
                        "Dropping perform message: signed-only mode executes no channel actions \
                         (further performs on this session are counted, not logged)"
                    );
                }
            }

            // The legacy top-level presence commands carried a client-chosen
            // identity; presence is granted by Rails in dispatch mode and
            // does not exist here. Counted, never acted on.
            ClientCommand::Join { identifier, .. }
            | ClientCommand::Leave { identifier }
            | ClientCommand::Presence { identifier } => {
                self.context
                    .backend
                    .metrics()
                    .record_unsupported_command(UnsupportedCommand::Presence);
                debug!(
                    conn_id,
                    identifier,
                    "Dropping legacy presence command: presence is a Rails grant in dispatch mode"
                );
            }

            ClientCommand::Whisper { identifier, data } => {
                let whisper_stream = if let Some(session) = hub.get_session(conn_id) {
                    session.get_whisper_stream(&identifier).map(str::to_string)
                } else {
                    warn!(conn_id, "Received whisper for unknown session");
                    return;
                };

                let Some(wstream) = whisper_stream else {
                    warn!(conn_id, identifier, "No whisper stream for subscription");
                    return;
                };

                // Validate the whisper body as one JSON value; it is forwarded
                // verbatim as each recipient's `message`.
                let message = match serde_json::from_str::<&serde_json::value::RawValue>(&data) {
                    Ok(value) => value,
                    Err(e) => {
                        warn!(conn_id, error = %e, "Invalid whisper data");
                        return;
                    }
                };

                // Fan out to the other members, each addressed with their own
                // identifier — not the sender's.
                hub.deliver_excluding(&wstream, message.get().as_bytes(), conn_id).await;
            }
        }
    }

    /// Authorize and install a signed-stream subscription.
    ///
    /// Order is `authorize -> install pending membership -> acquire backend
    /// interest -> confirm -> release buffered deliveries`, so a publication
    /// landing between installation and confirmation is delivered *after* the
    /// confirmation instead of dropped, and a refused backend never yields a
    /// confirmed but silent subscription. [`Hub::join`] runs the middle steps
    /// under the stream's gate, so a second subscriber waits for a pending
    /// acquisition rather than confirming ahead of it.
    ///
    /// **Duplicate subscribe semantics:** a repeat `subscribe` for an identifier
    /// this connection already owns installs no second membership and is
    /// answered with a repeat `confirm_subscription`. Re-confirmation (rather
    /// than silence) is what unwedges a client that retried after losing the
    /// first confirmation, and the ActionCable JS client dispatches one
    /// confirmation to every local subscription object sharing that identifier.
    async fn subscribe(&self, controller: &StreamsController, identifier: String) {
        let conn_id = self.conn_id;
        let hub = &self.context.hub;

        if hub.has_subscription(conn_id, &identifier) {
            debug!(conn_id, "Duplicate subscribe; re-confirming existing subscription");
            let confirm = ServerMessage::ConfirmSubscription { identifier };
            hub.send(conn_id, &actioncable::encode(&confirm)).await;
            return;
        }

        let result = match controller.resolve(&identifier) {
            Ok(result) => result,
            Err(e) => {
                debug!(conn_id, error = %e, "Signed stream verification failed");
                let reject = ServerMessage::RejectSubscription { identifier };
                hub.send(conn_id, &actioncable::encode(&reject)).await;
                return;
            }
        };

        debug!(conn_id, stream = %result.stream, "Signed stream subscription");

        let acquire = self.context.backend.acquire(&result.stream);
        match hub.join(conn_id, &identifier, &result.stream, acquire).await {
            Ok(()) => {}
            Err(HubError::UnknownSession(_)) => {
                // Torn down while the acquisition was in flight: the interest
                // is ours to return, and nobody is left to confirm.
                self.context.backend.release(vec![result.stream]).await;
                return;
            }
            Err(HubError::Refused(_)) => {
                let reject = ServerMessage::RejectSubscription { identifier };
                hub.send(conn_id, &actioncable::encode(&reject)).await;
                return;
            }
        }

        if result.whisper
            && let Some(mut session) = hub.get_session_mut(conn_id)
        {
            session.set_whisper_stream(&identifier, result.stream.clone());
        }

        let confirm = ServerMessage::ConfirmSubscription { identifier: identifier.clone() };
        hub.send(conn_id, &actioncable::encode(&confirm)).await;
        hub.activate(conn_id, &identifier);
    }

    /// Tear down a subscription locally.
    ///
    /// Subscriptions are confirmed locally via signed streams, so they are torn
    /// down locally too. Only the memberships of this exact
    /// `(conn_id, identifier)` pair are removed: other identifiers on the same
    /// connection keep receiving, even when they share the stream.
    async fn unsubscribe(&self, identifier: &str) {
        let conn_id = self.conn_id;
        let hub = &self.context.hub;

        if let Some(mut session) = hub.get_session_mut(conn_id) {
            session.remove_whisper_stream(identifier);
        } else {
            warn!(conn_id, "Received unsubscribe for unknown session");
            return;
        }

        let released = hub.remove_subscription(conn_id, identifier);
        self.context.backend.release(released).await;
    }
}

/// The transport-agnostic connection driver for one transport.
///
/// Owns that transport's [`Hub`] (sessions, memberships, outbound sink) and the
/// per-connection command handling; shares the process-wide [`Backend`].
pub struct Connections {
    hub: Arc<Hub>,
    backend: Arc<Backend>,
    /// Lease/liveness registry for the presence store; empty and unused
    /// until a connection actually takes a lease.
    presence_sessions: Arc<PresenceSessions>,
    /// Lease TTL, which is also the liveness window a refresh requires.
    presence_ttl: Duration,
    /// Batch refresh period.
    presence_refresh_interval: Duration,
    /// Opt-in recovery on `Orbitcast::RecoveryChannel`; `None` unless this node
    /// has a retained broker *and* a dedicated recovery key.
    recovery: Option<Arc<RecoveryController>>,
    ping_interval: Duration,
    /// Rails-dispatch machinery; `None` in signed-only mode.
    dispatch: Option<Arc<DispatchContext>>,
    /// Signed-only machinery; used when `dispatch` is `None`.
    signed: Arc<SignedContext>,
    /// `conn_id -> session`. Removing the entry closes the gate and drops the
    /// mailbox, which is what ends a session task.
    sessions: DashMap<u32, SessionHandle>,
    /// Bound on one session's mailbox.
    mailbox_capacity: usize,
    /// Per-session action-class budget; `None` when disabled
    /// (`rate_commands_per_conn_per_sec = 0`). Keyed by connection id, which
    /// this driver owns the lifetime of: the bucket is created on `accept` and
    /// dropped on `teardown`.
    action_limit: Option<crate::ratelimit::KeyedRateLimit<u32>>,
    /// Per-session subscribe-class budget (`rate_subscribes_per_conn_per_sec`).
    subscribe_limit: Option<crate::ratelimit::KeyedRateLimit<u32>>,
    /// Monotonic session generation, bumped per accepted connection.
    generation: AtomicU64,
    /// Keeps the cleanup worker alive for this driver's lifetime.
    _cleanup: TaskGuard,
}

impl Connections {
    /// Build a driver whose outbound frames go to `outgoing_tx`.
    ///
    /// The hub is registered with the backend before this returns, so a
    /// publication cannot be delivered to a hub that is not yet reachable.
    ///
    /// # Errors
    ///
    /// [`RpcError::Config`] when `config.rpc` enables Rails dispatch (an
    /// endpoint and a token are both set) but the caller cannot be built from
    /// it: an empty or non-http(s) endpoint, a zero deadline or concurrency
    /// limit, or a bearer token that is empty or not a valid header value.
    /// That is a boundary error, never a silent downgrade to signed-only
    /// acceptance; a signed-only configuration cannot fail here.
    pub fn new(
        backend: Arc<Backend>,
        config: &Config,
        outgoing_tx: mpsc::Sender<Outgoing>,
    ) -> Result<Arc<Self>, RpcError> {
        let hub = Arc::new(Hub::with_metrics(outgoing_tx, Arc::clone(backend.metrics())));
        backend.register_hub(&hub);

        let streams = config.streams.is_enabled().then(|| {
            info!(
                public = config.streams.public,
                turbo = config.streams.turbo,
                cable_ready = config.streams.cable_ready,
                "Signed streams enabled"
            );
            Arc::new(
                StreamsController::new(config.streams.clone(), config.namespace.clone())
                    .with_metrics(Arc::clone(backend.metrics())),
            )
        });

        let presence_sessions = Arc::new(PresenceSessions::new());

        // Recovery is authorized by its own token, so it is built the same way
        // in signed-only and Rails-dispatch mode - and not at all unless the
        // broker and the dedicated key are both configured.
        let recovery = crate::recovery::build(
            config.namespace.clone(),
            config.recovery_secret.clone(),
            backend.broker().cloned(),
            &config.broker,
            Arc::clone(&hub),
            backend.waker().cloned(),
            Arc::clone(&backend) as Arc<dyn crate::recovery::StreamInterest>,
        );

        let mut cleanup = TaskGuard::new();
        let dispatch = match config.rpc.to_rpc_config() {
            Some(rpc_config) => Some(DispatchContext::build(
                rpc_config,
                config,
                &backend,
                &hub,
                streams.clone(),
                recovery.clone(),
                &presence_sessions,
                &mut cleanup,
            )?),
            None => None,
        };

        let signed = Arc::new(SignedContext {
            hub: Arc::clone(&hub),
            backend: Arc::clone(&backend),
            streams,
            recovery: recovery.clone(),
        });

        // One table per budget per driver, sized like the connection ceiling
        // it can ever hold keys for: a session that exists is admitted through
        // `accept`, and `max_connections` already bounds how many that is.
        let keys = config.http.max_connections.max(1);
        let action_limit = crate::ratelimit::KeyedRateLimit::per_second(
            config.http.rate_commands_per_conn_per_sec,
            keys,
        );
        if action_limit.is_none() {
            warn!(
                "Per-connection action command rate limiting is disabled \
                 (rate_commands_per_conn_per_sec = 0)"
            );
        }
        let subscribe_limit = crate::ratelimit::KeyedRateLimit::per_second(
            config.http.rate_subscribes_per_conn_per_sec,
            keys,
        );
        if subscribe_limit.is_none() {
            warn!(
                "Per-connection subscribe rate limiting is disabled \
                 (rate_subscribes_per_conn_per_sec = 0)"
            );
        }

        let metrics_backend = Arc::clone(&backend);
        let connections = Arc::new(Self {
            hub,
            backend,
            presence_sessions,
            presence_ttl: config.presence_ttl,
            presence_refresh_interval: config.presence_refresh_interval,
            recovery,
            ping_interval: Duration::from_secs(config.ping_interval.max(1)),
            dispatch,
            signed,
            sessions: DashMap::new(),
            mailbox_capacity: config.rpc.mailbox_capacity.max(1),
            action_limit,
            subscribe_limit,
            generation: AtomicU64::new(0),
            _cleanup: cleanup,
        });

        // Registered weakly, so the exporter can read this driver's gauges
        // without keeping a dead transport alive.
        metrics_backend.register_driver(&connections);

        Ok(connections)
    }

    /// Whether this driver authorizes through Rails.
    pub const fn is_rails_dispatch(&self) -> bool {
        self.dispatch.is_some()
    }

    /// This transport's hub.
    pub const fn hub(&self) -> &Arc<Hub> {
        &self.hub
    }

    /// The shared pub/sub backend.
    pub const fn backend(&self) -> &Arc<Backend> {
        &self.backend
    }

    /// Spawn the per-transport background loops (ActionCable ping cadence,
    /// presence lease refresh and reconnect resync, and the Rails `describe`
    /// prober in dispatch mode). The returned guard aborts them when dropped.
    pub fn spawn_background_tasks(self: &Arc<Self>) -> TaskGuard {
        let mut tasks = TaskGuard::new();
        tasks.push(self.spawn_ping_task());
        if let Some(refresh) = self.spawn_presence_refresh_task() {
            tasks.push(refresh);
        }
        if let Some(resync) = self.spawn_presence_resync_task() {
            tasks.push(resync);
        }
        if let Some(ref context) = self.dispatch {
            tasks.push(tokio::spawn(probe_rails(Arc::clone(context))));
        }
        tasks
    }

    /// Extend the leases of every connection with verified socket liveness,
    /// each by the TTL its grant carried.
    ///
    /// Returns `None` when this node has no presence store (signed-only mode),
    /// so no task is spawned at all.
    ///
    /// One statement per pass, for every live lease at once: a roster of
    /// 10 000 tabs costs one `UPDATE`, not 10 000. A connection whose socket
    /// has been silent for a whole TTL is left out and its leases expire -
    /// which is the point. Nothing here refreshes a lease on the strength of
    /// the ActionCable JSON heartbeat, because that is a frame *this* node
    /// sends.
    fn spawn_presence_refresh_task(self: &Arc<Self>) -> Option<JoinHandle<()>> {
        let presence = self.backend.presence().cloned()?;
        let sessions = Arc::clone(&self.presence_sessions);
        let window = self.presence_ttl;
        let period = self.presence_refresh_interval;

        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // consume the immediate first tick

            loop {
                interval.tick().await;

                let leases = sessions.live_leases(window);
                if leases.is_empty() {
                    continue;
                }

                match presence.refresh(&leases).await {
                    Ok(rows) => {
                        debug!(held = leases.len(), refreshed = rows, "presence leases refreshed");
                    }
                    Err(error) => warn!(
                        held = leases.len(),
                        %error,
                        "Presence refresh failed; leases will expire unless the next pass \
                         succeeds"
                    ),
                }
            }
        }))
    }

    /// Reconcile presence after a pub/sub reconnect.
    ///
    /// Roster notifications emitted while the listener was down are lost
    /// (notification-only delivery), so once the backend is ready again the
    /// snapshot cache is dropped and every live session is asked to
    /// re-announce a roster page on its granted subscriptions. Returns `None`
    /// when this node has no presence store.
    fn spawn_presence_resync_task(self: &Arc<Self>) -> Option<JoinHandle<()>> {
        let presence = self.backend.presence().cloned()?;
        let pubsub = Arc::clone(self.backend.pubsub());
        let driver = Arc::downgrade(self);

        Some(tokio::spawn(async move {
            let mut seen = pubsub.stats().reconnects;
            let mut pending = false;
            loop {
                tokio::time::sleep(PRESENCE_RESYNC_WATCH_INTERVAL).await;

                let reconnects = pubsub.stats().reconnects;
                if reconnects != seen {
                    seen = reconnects;
                    pending = true;
                }
                // Wait for the listener to be back: a resync against a backend
                // that is still reconnecting would only fail.
                if !pending || !pubsub.is_ready() {
                    continue;
                }
                pending = false;

                let Some(driver) = driver.upgrade() else {
                    return;
                };
                presence.invalidate_all();
                let notified = driver.request_presence_resync();
                info!(
                    sessions = notified,
                    reconnects, "Presence rosters re-announced after a pub/sub reconnect"
                );
            }
        }))
    }

    /// Ask every live session to re-announce its rosters; returns how many
    /// were asked. Best-effort: a session whose mailbox is full is skipped
    /// (its client is already behind) and reconciles on its next snapshot.
    fn request_presence_resync(&self) -> usize {
        let mut notified = 0;
        for entry in &self.sessions {
            if entry.mailbox.try_send(SessionCommand::PresenceResync).is_ok() {
                notified += 1;
            }
        }
        notified
    }

    /// Record verified inbound activity on a connection.
    ///
    /// Called by the transport for every inbound WebSocket message, including
    /// the control pong that answers the server's ping. This is the only
    /// liveness signal presence leases are refreshed from.
    pub fn record_liveness(&self, conn_id: u32) {
        self.presence_sessions.record_liveness(conn_id);
    }

    /// Presence leases currently held on this node (a client that left is
    /// not counted; a client that joined twice on two subscriptions counts
    /// twice).
    pub fn presence_lease_count(&self) -> usize {
        self.presence_sessions.lease_count()
    }

    /// Emit the ActionCable JSON `ping` heartbeat on the configured cadence.
    ///
    /// This is the protocol-level heartbeat the browser client's connection
    /// monitor watches; it is independent of WebSocket control ping/pong, which
    /// the transport handles.
    fn spawn_ping_task(self: &Arc<Self>) -> JoinHandle<()> {
        let hub = Arc::clone(&self.hub);
        let period = self.ping_interval;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(period);
            loop {
                interval.tick().await;
                let timestamp = now_unix();
                let payload = actioncable::encode(&ServerMessage::Ping { timestamp });

                // One identical frame per session; no per-session forwarding task.
                hub.broadcast_ping(&payload);
                debug!(timestamp, sessions = hub.session_count(), "Ping sent");
            }
        })
    }

    /// Register a new connection.
    ///
    /// Every session gets a hub entry, a bounded mailbox, and a task of its
    /// own. A reused connection id (the docking transport numbers connections
    /// per link) starts from a clean state: stale hub entries are cleared and
    /// counted, and an older session under the same id is closed first.
    ///
    /// Signed-stream mode accepts immediately and sends the standard `welcome`:
    /// there is no identification round-trip, and acceptance is not user
    /// identification.
    ///
    /// Rails-dispatch mode instead leaves the connection **pending**: the
    /// session task runs `identify` first, and exactly one `welcome` is sent
    /// once Rails accepts. A Rails endpoint the `describe` handshake refused
    /// admits nothing: the connection is closed with `reconnect: true`.
    pub async fn accept(
        &self,
        conn_id: u32,
        path: String,
        remote_addr: String,
        headers: HashMap<String, String>,
    ) {
        // Counted once per accepted connection, for every transport: the
        // docking link and the direct listener both arrive here.
        self.backend.metrics().record_connection();

        // A reused id with a session still registered under it is the
        // transport's bug, but it must not leak a task or a membership.
        if let Some((_, stale)) = self.sessions.remove(&conn_id) {
            warn!(
                conn_id,
                generation = stale.gate.generation(),
                "connection id reused; closing the older session"
            );
            stale.gate.close();
        }

        let released = self.hub.add_session(Session::new(conn_id, path.clone()));
        if !released.is_empty() {
            self.backend.release(released).await;
        }

        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let gate = Arc::new(SessionGate::new(generation));
        let (tx, rx) = mpsc::channel(self.mailbox_capacity);

        if let Some(ref context) = self.dispatch {
            // Queued before the task is spawned, on an empty mailbox: the
            // first thing this session ever does is identify.
            let identified = tx.try_send(SessionCommand::Identify).is_ok();
            self.sessions.insert(conn_id, SessionHandle { mailbox: tx, gate: Arc::clone(&gate) });
            tokio::spawn(
                SessionDispatch::new(
                    Arc::clone(context),
                    conn_id,
                    gate,
                    path,
                    remote_addr,
                    &headers,
                )
                .run(rx),
            );

            if let RailsStatus::Refused(refusal) = self.backend.rails_status() {
                warn!(
                    conn_id,
                    refusal = refusal.as_str(),
                    "Refusing a dispatch connection: Rails endpoint not admitted"
                );
                self.overload_close(conn_id).await;
                return;
            }
            if !identified {
                warn!(conn_id, "Could not queue identification; closing");
                self.overload_close(conn_id).await;
            }

            debug!(conn_id, generation, "Rails-dispatch connect pending identification");
            return;
        }

        self.sessions.insert(conn_id, SessionHandle { mailbox: tx, gate: Arc::clone(&gate) });
        tokio::spawn(
            SignedSession {
                context: Arc::clone(&self.signed),
                conn_id,
                gate,
                warned_unsupported_perform: false,
            }
            .run(rx),
        );

        let welcome = ServerMessage::Welcome { sid: uuid::Uuid::new_v4().to_string() };
        self.hub.send(conn_id, &actioncable::encode(&welcome)).await;
        debug!(conn_id, generation, "Standalone connect accepted");
    }

    /// Queue one command on a session's mailbox.
    ///
    /// A full mailbox is overload, not backpressure: the session is closed with
    /// `reconnect: true` rather than allowed to accumulate unbounded work or to
    /// block the socket's read loop.
    async fn enqueue(&self, conn_id: u32, command: SessionCommand) {
        let Some(mailbox) = self.sessions.get(&conn_id).map(|entry| entry.mailbox.clone()) else {
            debug!(conn_id, command = command.name(), "No mailbox for command; dropped");
            return;
        };

        match mailbox.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                warn!(conn_id, command = command.name(), "Session mailbox full; closing");
                self.overload_close(conn_id).await;
            }
            Err(mpsc::error::TrySendError::Closed(command)) => {
                debug!(conn_id, command = command.name(), "Session mailbox closed; dropped");
            }
        }
    }

    /// Close an overloaded session, inviting it back.
    async fn overload_close(&self, conn_id: u32) {
        let disconnect = ServerMessage::Disconnect {
            reason: CLOSE_REASON_SERVER_RESTART.to_string(),
            reconnect: true,
        };
        self.hub.send(conn_id, &actioncable::encode(&disconnect)).await;
        self.hub.disconnect(conn_id, CLOSE_NORMAL, CLOSE_REASON_SERVER_RESTART).await;
    }

    /// Handle one inbound ActionCable text frame.
    ///
    /// Whitespace-only frames are ignored (browsers send them as keepalives);
    /// anything unparseable is logged and dropped, never guessed at. Never
    /// waits on the session: the command is queued on its mailbox.
    pub async fn handle_frame(&self, conn_id: u32, data: &[u8]) {
        if data.iter().all(u8::is_ascii_whitespace) {
            debug!(conn_id, "Ignoring empty ActionCable text frame");
            return;
        }

        match actioncable::parse_command(data) {
            Ok(command) => self.handle_command(conn_id, command).await,
            Err(e) => warn!(conn_id, error = %e, "Invalid ActionCable command"),
        }
    }

    /// Charge and queue one parsed client command.
    ///
    /// The per-session rate limits are charged here, before any routing: a
    /// subscribe-class command (`subscribe`/`unsubscribe`) spends from the
    /// subscribe budget and everything else from the action budget, so a
    /// `perform` loop cannot starve a page's subscribes and a subscribe storm
    /// cannot buy extra action budget. A frame that never parsed was already
    /// refused and is not charged - it did no work worth bounding.
    pub async fn handle_command(&self, conn_id: u32, command: ClientCommand) {
        let class = command_class(&command);
        let limit = match class {
            CommandClass::Subscribe => self.subscribe_limit.as_ref(),
            CommandClass::Action => self.action_limit.as_ref(),
        };
        if limit.is_some_and(|limit| !limit.allow(conn_id)) {
            warn!(
                conn_id,
                class = class.label(),
                "Command rate limit exceeded; closing the session"
            );
            self.backend.metrics().record_command_rate_limited(class);
            self.overload_close(conn_id).await;
            return;
        }

        self.enqueue(conn_id, SessionCommand::Client(command)).await;
    }

    /// The single connection teardown path: session task, presence,
    /// memberships, backend interest, and session state, in that order.
    ///
    /// Used for client disconnect, server-initiated close, and draining —
    /// normal or not. Never waits for Rails or for the session task: the gate
    /// closes first, so queued commands are never run and a late result is
    /// refused at the hub.
    pub async fn teardown(&self, conn_id: u32) {
        // Stop routing first: close the gate, then drop the mailbox. The
        // dispatch task then submits `release_connection` onto the bounded
        // cleanup queue.
        if let Some((_, session)) = self.sessions.remove(&conn_id) {
            session.gate.close();
            drop(session);
        }

        // The command budgets die with the connection: a reconnect is a new
        // session, and connection ids are reused across a process' lifetime.
        if let Some(ref limit) = self.action_limit {
            limit.forget(&conn_id);
        }
        if let Some(ref limit) = self.subscribe_limit {
            limit.forget(&conn_id);
        }

        // Drop any recovery session: its tails stop and its pub/sub interest
        // is released through the same refcount ordinary subscriptions use.
        if let Some(ref recovery) = self.recovery {
            let streams = recovery.forget(conn_id);
            self.backend.release(streams).await;
        }

        // Stop refreshing this connection's leases. The leases themselves are
        // released by the session task, which is ending now; whatever it
        // cannot release expires.
        self.presence_sessions.forget(conn_id);

        let released = self.hub.remove_session(conn_id);
        self.backend.release(released).await;
    }

    /// Tear every remaining connection down through [`Self::teardown`].
    pub async fn drain(&self) {
        for conn_id in self.hub.session_ids() {
            self.teardown(conn_id).await;
        }
    }

    /// Send the standard ActionCable `disconnect` frame to every session.
    ///
    /// `reconnect: true` tells the client's connection monitor that this node
    /// is going away and it should come back, which is the correct signal for a
    /// rolling restart.
    pub fn disconnect_all(&self, reason: &str, reconnect: bool) {
        let payload = actioncable::encode(&ServerMessage::Disconnect {
            reason: reason.to_string(),
            reconnect,
        });

        // A disconnect frame carries no identifier, so — like the heartbeat —
        // one identical frame goes to every session.
        let sent = self.hub.broadcast_ping(&payload);
        debug!(sessions = sent, reason, reconnect, "Disconnect advertised");
    }

    /// Ask the transport to close every session's socket.
    ///
    /// The close travels through the *same* outbound queue as ordinary frames,
    /// so a client always sees the ActionCable `disconnect` emitted by
    /// [`Self::disconnect_all`] before its socket goes away. Enqueuing the
    /// close on a side channel would race that order.
    pub async fn close_all(&self, code: u16, reason: &str) {
        for conn_id in self.hub.session_ids() {
            self.hub.disconnect(conn_id, code, reason).await;
        }
    }

    /// Number of live sessions on this transport.
    pub fn session_count(&self) -> usize {
        self.hub.session_count()
    }

    /// Number of session tasks currently registered (tests).
    #[cfg(test)]
    pub(crate) fn task_count(&self) -> usize {
        self.sessions.len()
    }

    /// The liveness gate of a live session (tests).
    #[cfg(test)]
    pub(crate) fn gate(&self, conn_id: u32) -> Option<Arc<SessionGate>> {
        self.sessions.get(&conn_id).map(|entry| Arc::clone(&entry.gate))
    }
}

impl std::fmt::Debug for Connections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connections")
            .field("hub", &self.hub)
            .field("backend", &self.backend)
            .field("signed_streams", &self.signed.streams.is_some())
            .field("rails_dispatch", &self.dispatch.is_some())
            .field("recovery", &self.recovery.is_some())
            .field("presence_sessions", &self.presence_sessions.len())
            .field("sessions", &self.sessions.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
