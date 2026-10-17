//! Prometheus text exposition for the operational counters.
//!
//! # What is exposed
//!
//! Everything here is either a fixed-cardinality counter kept in [`Metrics`],
//! or a snapshot of a counter struct that already existed
//! ([`crate::pubsub::PubSubStats`], [`crate::broker::BrokerStats`], the
//! presence store's own counters). Nothing is aggregated per user, per
//! connection, or per stream: a `/metrics` scrape has a **bounded, constant**
//! number of series regardless of traffic: no per-user or per-stream metric
//! labels, ever. The only labels used are drawn
//! from closed enumerations declared in this module.
//!
//! | family | kind | source |
//! |--------|------|--------|
//! | `orbitcast_build_info` | gauge | crate version |
//! | `orbitcast_connections_current` | gauge | live sessions across this process' transports |
//! | `orbitcast_connections_total` | counter | accepted WebSocket sessions |
//! | `orbitcast_connections_rejected_total{reason}` | counter | refused upgrades ([`ConnectionRejection`]) |
//! | `orbitcast_subscriptions_current` | gauge | confirmed subscriptions |
//! | `orbitcast_streams_with_interest` | gauge | streams this node holds backend interest on |
//! | `orbitcast_commands_rate_limited_total{class}` | counter | per-session command limit trips ([`CommandClass`]) |
//! | `orbitcast_unsupported_commands_total{command}` | counter | client commands this mode does not execute ([`UnsupportedCommand`]) |
//! | `orbitcast_socket_lag_disconnects_total{reason}` | counter | sessions closed because a bounded outbound buffer overflowed ([`LagReason`]) |
//! | `orbitcast_hub_queue_overflow_total` | counter | docking transport only: frames lost because the shared docking link queue was full; each is followed by a lag close of that connection (`reason="queue"`). The HTTP transport has no shared queue and never increments it |
//! | `orbitcast_session_reuse_stale_entries_total` | counter | reused connection ids that still had stale hub entries |
//! | `orbitcast_rails_ready` | gauge | Rails dispatch endpoint admitted by `describe` (dispatch mode only) |
//! | `orbitcast_pubsub_published_total` | counter | publications this node originated |
//! | `orbitcast_pubsub_received_total` | counter | publications delivered *to* this node |
//! | `orbitcast_pubsub_delivered_total` | counter | addressed frames handed to sockets |
//! | `orbitcast_pubsub_dropped_total{queue}` | counter | intake/delivery queue overflow |
//! | `orbitcast_pubsub_intake_queue_depth_max` | gauge | deepest the notification intake queue has been |
//! | `orbitcast_pubsub_gap_disconnects_total` | counter | sessions closed because a delivery gap hit a stream they held |
//! | `orbitcast_pubsub_rejected_total` | counter | malformed/foreign/misrouted notices |
//! | `orbitcast_pubsub_overflow_total` | counter | overflow-relay notices seen |
//! | `orbitcast_pubsub_overflow_gaps_total` | counter | overflow notices that produced nothing |
//! | `orbitcast_pubsub_reconnects_total` | counter | listener connection losses |
//! | `orbitcast_rpc_calls_total{outcome}` | counter | Rails dispatch calls ([`RpcOutcome`]) |
//! | `orbitcast_rpc_latency_seconds` | histogram | Rails dispatch round trips, fixed buckets |
//! | `orbitcast_presence_*` | gauge/counter | lease registry and presence store counters |
//! | `orbitcast_broker_*_total` | counter | retained history counters; `duplicates_total` is labelled `kind="identical"\|"conflicting"` |
//! | `orbitcast_broker_messages_current`, `orbitcast_broker_bytes_current` | gauge | unexpired retained rows / payload bytes of this node's namespace (`namespace` label), refreshed every reconcile interval |
//! | `orbitcast_ingress_requests_total{status}` | counter | private ingress replies ([`IngressStatus`]) |
//! | `orbitcast_signature_verifications_total{profile,key}` | counter | signed-stream HMAC checks by profile and which ring key matched ([`SignatureProfile`], [`SignatureKey`]) |
//!
//! # Where it is served, and why not on `/cable`
//!
//! The route is mounted on the **private ingress listener** (`GET /metrics`,
//! same `Authorization: Bearer` credential as the publication routes). A node
//! without `ingress_listen_addr` therefore exposes no metrics at all, which is
//! the fail-closed default: an administrative route must not appear on a
//! browser-facing port by accident.
//!
//! `metrics_public = true` (`ORBITCAST_METRICS_PUBLIC`) moves the route onto
//! the public listener **unauthenticated**, because there is no second
//! credential to check there and inventing one would be a new secret in a
//! browser-facing path. The tradeoff is explicit: anyone who can reach
//! `/cable` can then read this node's connection, queue, and dependency
//! counters. Use it only when the listener is already bound to a private
//! interface or a scrape-only network; the recommended deployment is the
//! authenticated ingress listener.
//!
//! # Cost
//!
//! Recording is a relaxed atomic add. Rendering walks a handful of atomics and
//! one `HashMap` length behind the backend's interest lock, so a scrape never
//! blocks delivery for more than that lock's normal hold time.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::broker::BrokerStatsSnapshot;
use crate::presence::PresenceMetrics;
use crate::pubsub::StatsSnapshot;

/// Upper bounds of the RPC latency histogram, in seconds.
///
/// Fixed and hand-picked rather than configurable: a histogram whose buckets
/// move between deployments cannot be compared across them. The range covers a
/// local Rails call (a few hundred microseconds) through the default 5 s
/// dispatch deadline, so a saturated or timing-out endpoint is visible as
/// mass piling into the last buckets.
pub const RPC_LATENCY_BUCKETS_SECONDS: [f64; 12] =
    [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0];

/// Why a WebSocket upgrade was refused before any session existed.
///
/// A closed set: these are the only values the `reason` label ever takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionRejection {
    /// The node is draining and no longer admits connections.
    Draining,
    /// The `Origin` header is not on the allow-list.
    Origin,
    /// `max_connections` is already reached.
    Limit,
    /// The per-IP connection rate limit tripped.
    RateLimited,
    /// The upgrade did not complete within `handshake_timeout_ms`.
    HandshakeTimeout,
}

impl ConnectionRejection {
    /// Every variant, in exposition order.
    pub const ALL: [Self; 5] =
        [Self::Draining, Self::Origin, Self::Limit, Self::RateLimited, Self::HandshakeTimeout];

    const fn index(self) -> usize {
        match self {
            Self::Draining => 0,
            Self::Origin => 1,
            Self::Limit => 2,
            Self::RateLimited => 3,
            Self::HandshakeTimeout => 4,
        }
    }

    /// The `reason` label value.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Draining => "draining",
            Self::Origin => "origin",
            Self::Limit => "limit",
            Self::RateLimited => "rate_limited",
            Self::HandshakeTimeout => "handshake_timeout",
        }
    }
}

/// How one Rails dispatch call ended.
///
/// `accepted` and `denied` are both *successful* calls: denial is a decision,
/// not a failure. The remaining variants are the failure taxonomy of
/// [`crate::rpc_client::RpcError`], collapsed to a closed label set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcOutcome {
    /// Rails ran the operation and authorized it.
    Accepted,
    /// Rails ran the operation and refused it.
    Denied,
    /// Rails answered, but could not run the operation.
    Fault,
    /// The end-to-end deadline expired.
    Deadline,
    /// The circuit breaker was open, so no call was made.
    CircuitOpen,
    /// A non-2xx HTTP status.
    Http,
    /// Connection, DNS, TLS, or body-read failure.
    Transport,
    /// A reply (or a request) this build refuses to use.
    Invalid,
}

impl RpcOutcome {
    /// Every variant, in exposition order.
    pub const ALL: [Self; 8] = [
        Self::Accepted,
        Self::Denied,
        Self::Fault,
        Self::Deadline,
        Self::CircuitOpen,
        Self::Http,
        Self::Transport,
        Self::Invalid,
    ];

    const fn index(self) -> usize {
        match self {
            Self::Accepted => 0,
            Self::Denied => 1,
            Self::Fault => 2,
            Self::Deadline => 3,
            Self::CircuitOpen => 4,
            Self::Http => 5,
            Self::Transport => 6,
            Self::Invalid => 7,
        }
    }

    /// The `outcome` label value.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Denied => "denied",
            Self::Fault => "fault",
            Self::Deadline => "deadline",
            Self::CircuitOpen => "circuit_open",
            Self::Http => "http_error",
            Self::Transport => "transport",
            Self::Invalid => "invalid",
        }
    }
}

/// The reply status of one private-ingress request.
///
/// The ingress answers with exactly this set of statuses (see
/// [`crate::transport::ingress`]), so the label stays closed even though the
/// underlying value is an HTTP status code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngressStatus {
    /// `200`: a served `GET /metrics` scrape. Deliberately distinct from
    /// `202`, so a scrape is never mistaken for an accepted publication.
    Ok,
    /// `202`: accepted (and, for retained streams, committed).
    Accepted,
    /// `400`: unreadable, malformed, or invalid envelope.
    BadRequest,
    /// `401`: missing or wrong credential.
    Unauthorized,
    /// `403`: another deployment's namespace.
    Forbidden,
    /// `404`: unknown route.
    NotFound,
    /// `405`: known route, wrong method.
    MethodNotAllowed,
    /// `413`: payload above the configured ceiling.
    TooLarge,
    /// `429`: too many requests already in flight.
    TooManyRequests,
    /// `503`: the pub/sub backend cannot accept publications.
    Unavailable,
}

impl IngressStatus {
    /// Every variant, in exposition order.
    pub const ALL: [Self; 10] = [
        Self::Ok,
        Self::Accepted,
        Self::BadRequest,
        Self::Unauthorized,
        Self::Forbidden,
        Self::NotFound,
        Self::MethodNotAllowed,
        Self::TooLarge,
        Self::TooManyRequests,
        Self::Unavailable,
    ];

    const fn index(self) -> usize {
        match self {
            Self::Ok => 0,
            Self::Accepted => 1,
            Self::BadRequest => 2,
            Self::Unauthorized => 3,
            Self::Forbidden => 4,
            Self::NotFound => 5,
            Self::MethodNotAllowed => 6,
            Self::TooLarge => 7,
            Self::TooManyRequests => 8,
            Self::Unavailable => 9,
        }
    }

    /// The `status` label value: the HTTP status code, as text.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ok => "200",
            Self::Accepted => "202",
            Self::BadRequest => "400",
            Self::Unauthorized => "401",
            Self::Forbidden => "403",
            Self::NotFound => "404",
            Self::MethodNotAllowed => "405",
            Self::TooLarge => "413",
            Self::TooManyRequests => "429",
            Self::Unavailable => "503",
        }
    }

    /// Classify a status code the ingress produced.
    ///
    /// `None` for anything outside the documented set: an unexpected status is
    /// a bug to log, never a new series.
    #[must_use]
    pub const fn from_code(code: u16) -> Option<Self> {
        Some(match code {
            200 => Self::Ok,
            202 => Self::Accepted,
            400 => Self::BadRequest,
            401 => Self::Unauthorized,
            403 => Self::Forbidden,
            404 => Self::NotFound,
            405 => Self::MethodNotAllowed,
            413 => Self::TooLarge,
            429 => Self::TooManyRequests,
            503 => Self::Unavailable,
            _ => return None,
        })
    }
}

/// Which per-session command budget a client command is charged against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandClass {
    /// `subscribe` / `unsubscribe` (`rate_subscribes_per_conn_per_sec`).
    Subscribe,
    /// `message` and every other command (`rate_commands_per_conn_per_sec`).
    Action,
}

impl CommandClass {
    /// Every variant, in exposition order.
    pub const ALL: [Self; 2] = [Self::Subscribe, Self::Action];

    const fn index(self) -> usize {
        match self {
            Self::Subscribe => 0,
            Self::Action => 1,
        }
    }

    /// The `class` label value.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Subscribe => "subscribe",
            Self::Action => "action",
        }
    }
}

/// A client command the current authorization mode does not execute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnsupportedCommand {
    /// `message` (`perform`) in signed-only mode: nothing runs Ruby here.
    Message,
    /// Legacy top-level `join` / `leave` / `presence` commands. They carried
    /// a client-chosen identity and are dropped in every mode: presence is a
    /// Rails grant answered through the `orbitcast_presence` action.
    Presence,
    /// Whisper in Rails-dispatch mode.
    Whisper,
}

impl UnsupportedCommand {
    /// Every variant, in exposition order.
    pub const ALL: [Self; 3] = [Self::Message, Self::Presence, Self::Whisper];

    const fn index(self) -> usize {
        match self {
            Self::Message => 0,
            Self::Presence => 1,
            Self::Whisper => 2,
        }
    }

    /// The `command` label value.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Presence => "presence",
            Self::Whisper => "whisper",
        }
    }
}

/// Which signed-stream profile a signature was checked under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignatureProfile {
    /// `Orbitcast::StreamsChannel` claims tokens.
    Orbitcast,
    /// `Turbo::StreamsChannel` signed names.
    Turbo,
    /// `CableReady::Stream` signed names.
    CableReady,
}

impl SignatureProfile {
    /// Every variant, in exposition order.
    pub const ALL: [Self; 3] = [Self::Orbitcast, Self::Turbo, Self::CableReady];

    const fn index(self) -> usize {
        match self {
            Self::Orbitcast => 0,
            Self::Turbo => 1,
            Self::CableReady => 2,
        }
    }

    /// The `profile` label value.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Orbitcast => "orbitcast",
            Self::Turbo => "turbo",
            Self::CableReady => "cable_ready",
        }
    }
}

/// Which ring key a signature matched, if any.
///
/// A steady `previous` count after a rotation says pages minted with the
/// old key are still open; a rising `failed` count says a key is missing
/// from the ring, or someone is probing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignatureKey {
    /// The current key produced the signature.
    Current,
    /// A verify-only previous key produced it.
    Previous,
    /// No ring key produced it.
    Failed,
}

impl SignatureKey {
    /// Every variant, in exposition order.
    pub const ALL: [Self; 3] = [Self::Current, Self::Previous, Self::Failed];

    const fn index(self) -> usize {
        match self {
            Self::Current => 0,
            Self::Previous => 1,
            Self::Failed => 2,
        }
    }

    /// The `key` label value.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Previous => "previous",
            Self::Failed => "failed",
        }
    }
}

/// Which bounded per-socket buffer overflowed before a lag disconnect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LagReason {
    /// The socket's outbound queue (message or byte limit).
    Queue,
    /// The buffer holding deliveries for a subscription awaiting its
    /// confirmation.
    PendingBuffer,
}

impl LagReason {
    /// Every variant, in exposition order.
    pub const ALL: [Self; 2] = [Self::Queue, Self::PendingBuffer];

    const fn index(self) -> usize {
        match self {
            Self::Queue => 0,
            Self::PendingBuffer => 1,
        }
    }

    /// The `reason` label value.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::PendingBuffer => "pending_buffer",
        }
    }
}

/// Process-wide counters that have no other home.
///
/// Everything the pub/sub, broker, and presence layers already count keeps
/// living in their own stats structs; this holds the counters that only exist
/// because something has to be measured at the transport and dispatch seams.
///
/// One instance per process, owned by [`crate::server::Backend`] and shared by
/// every transport driven from it.
#[derive(Debug, Default)]
pub struct Metrics {
    connections_total: AtomicU64,
    connections_rejected: [AtomicU64; ConnectionRejection::ALL.len()],
    commands_rate_limited: [AtomicU64; CommandClass::ALL.len()],
    unsupported_commands: [AtomicU64; UnsupportedCommand::ALL.len()],
    socket_lag_disconnects: [AtomicU64; LagReason::ALL.len()],
    hub_queue_overflow: AtomicU64,
    session_reuse_stale_entries: AtomicU64,
    pubsub_gap_disconnects: AtomicU64,
    published: AtomicU64,
    received: AtomicU64,
    delivered: AtomicU64,
    rpc_calls: [AtomicU64; RpcOutcome::ALL.len()],
    /// Cumulative counts per bucket, plus one trailing `+Inf` slot.
    rpc_latency: [AtomicU64; RPC_LATENCY_BUCKETS_SECONDS.len() + 1],
    /// Observed latency total, in microseconds (integer, so the sum is exact).
    rpc_latency_sum_micros: AtomicU64,
    ingress_requests: [AtomicU64; IngressStatus::ALL.len()],
    /// `[profile][key]`.
    signature_verifications: [[AtomicU64; SignatureKey::ALL.len()]; SignatureProfile::ALL.len()],
}

impl Metrics {
    /// A fresh registry with every counter at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// One WebSocket session was accepted.
    pub fn record_connection(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    /// One upgrade was refused.
    pub fn record_connection_rejected(&self, reason: ConnectionRejection) {
        self.connections_rejected[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// One session exceeded the command rate limit of `class`.
    pub fn record_command_rate_limited(&self, class: CommandClass) {
        self.commands_rate_limited[class.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// One client command was dropped because this mode does not execute it.
    pub fn record_unsupported_command(&self, command: UnsupportedCommand) {
        self.unsupported_commands[command.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// One session was closed because a bounded outbound buffer overflowed.
    pub fn record_socket_lag_disconnect(&self, reason: LagReason) {
        self.socket_lag_disconnects[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// One frame was lost because the shared docking link queue was full
    /// (docking transport only; the connection is then closed for lag).
    pub fn record_hub_queue_overflow(&self) {
        self.hub_queue_overflow.fetch_add(1, Ordering::Relaxed);
    }

    /// A reused connection id still had `entries` stale hub entries.
    pub fn record_session_reuse_stale_entries(&self, entries: u64) {
        self.session_reuse_stale_entries.fetch_add(entries, Ordering::Relaxed);
    }

    /// `sessions` sessions were closed because a delivery gap hit a stream
    /// they held.
    pub fn record_pubsub_gap_disconnects(&self, sessions: u64) {
        self.pubsub_gap_disconnects.fetch_add(sessions, Ordering::Relaxed);
    }

    /// Socket lag disconnects recorded so far, for `reason` (tests, probes).
    #[must_use]
    pub fn socket_lag_disconnects(&self, reason: LagReason) -> u64 {
        self.socket_lag_disconnects[reason.index()].load(Ordering::Relaxed)
    }

    /// Frames lost on the shared docking link queue so far (tests, probes).
    #[must_use]
    pub fn hub_queue_overflows(&self) -> u64 {
        self.hub_queue_overflow.load(Ordering::Relaxed)
    }

    /// One publication was accepted by the backend from this node.
    pub fn record_published(&self) {
        self.published.fetch_add(1, Ordering::Relaxed);
    }

    /// One publication arrived from the backend for local fanout.
    pub fn record_received(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }

    /// `frames` addressed frames were handed to sockets.
    pub fn record_delivered(&self, frames: u64) {
        self.delivered.fetch_add(frames, Ordering::Relaxed);
    }

    /// One Rails dispatch call finished in `elapsed`.
    pub fn record_rpc(&self, outcome: RpcOutcome, elapsed: std::time::Duration) {
        self.rpc_calls[outcome.index()].fetch_add(1, Ordering::Relaxed);

        let seconds = elapsed.as_secs_f64();
        // Cumulative buckets: a Prometheus histogram bucket counts everything
        // at or below its bound, so every bucket from the first match up is
        // incremented, `+Inf` included.
        let first = RPC_LATENCY_BUCKETS_SECONDS.iter().position(|bound| seconds <= *bound);
        let start = first.unwrap_or(RPC_LATENCY_BUCKETS_SECONDS.len());
        for bucket in &self.rpc_latency[start..] {
            bucket.fetch_add(1, Ordering::Relaxed);
        }

        self.rpc_latency_sum_micros
            .fetch_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// One private-ingress request was answered with `status`.
    pub fn record_ingress(&self, status: IngressStatus) {
        self.ingress_requests[status.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// One signed-stream HMAC check under `profile` ended with `key`.
    pub fn record_signature_verification(&self, profile: SignatureProfile, key: SignatureKey) {
        self.signature_verifications[profile.index()][key.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Signature checks recorded so far for `profile` / `key` (tests, probes).
    #[must_use]
    pub fn signature_verifications(&self, profile: SignatureProfile, key: SignatureKey) -> u64 {
        self.signature_verifications[profile.index()][key.index()].load(Ordering::Relaxed)
    }
}

/// The live, non-cumulative numbers a scrape reports.
///
/// Collected by the caller (they live in the transports' connection drivers and
/// in the backend's interest map) and handed to [`render`], so this module
/// needs no access to either.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Gauges {
    /// Sessions currently held across this process' transports.
    pub connections: u64,
    /// Confirmed subscriptions currently held.
    pub subscriptions: u64,
    /// Streams this node currently holds backend interest on.
    pub streams_with_interest: u64,
    /// Connections currently holding a distributed presence lease here.
    pub presence_leases: u64,
    /// Whether the Rails dispatch endpoint is admitted; `None` outside
    /// dispatch mode (the series is then not emitted).
    pub rails_ready: Option<bool>,
}

/// Everything one scrape needs, gathered by the caller.
#[derive(Debug, Clone, Copy)]
pub struct Exposition<'a> {
    /// The process-wide counter registry.
    pub metrics: &'a Metrics,
    /// Live values.
    pub gauges: Gauges,
    /// Pub/sub overload counters; `None` when no backend is configured.
    pub pubsub: Option<StatsSnapshot>,
    /// Presence store counters; `None` when this node has no presence store.
    pub presence: Option<PresenceMetrics>,
    /// Retained-history counters; `None` when this node has no broker.
    pub broker: Option<BrokerExposition<'a>>,
}

/// The broker's counters plus the namespace its growth gauges describe.
///
/// A node retains exactly one namespace, so the gauges carry it as a label
/// (the one per-deployment label this exporter allows) rather than a series
/// per stream.
#[derive(Debug, Clone, Copy)]
pub struct BrokerExposition<'a> {
    /// The deployment namespace the broker is keyed by.
    pub namespace: &'a str,
    /// The counters and growth gauges.
    pub stats: BrokerStatsSnapshot,
}

/// Render one scrape in the Prometheus text exposition format (version 0.0.4).
///
/// Absent subsystems emit **nothing** rather than zeros: a node without a
/// broker has no retained history to report, and a zero series would read as
/// "configured and idle". The counters that always exist are always emitted,
/// zero included, so a dashboard never waits for the first event to see a
/// series.
#[must_use]
pub fn render(exposition: Exposition<'_>) -> String {
    let Exposition { metrics, gauges, pubsub, presence, broker } = exposition;
    let mut scrape = Scrape(String::with_capacity(4096));

    scrape.help("orbitcast_build_info", "gauge", "Build metadata; always 1.");
    scrape
        .line(format_args!("orbitcast_build_info{{version=\"{}\"}} 1", env!("CARGO_PKG_VERSION")));

    scrape.connections(metrics, gauges);
    scrape.pubsub(metrics, pubsub);
    scrape.rpc(metrics);
    scrape.presence(gauges.presence_leases, presence);
    if let Some(broker) = broker {
        scrape.broker(broker);
    }
    scrape.ingress(metrics);
    scrape.signatures(metrics);

    scrape.0
}

/// The `Content-Type` a Prometheus scrape expects.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// One exposition being written, section by section.
struct Scrape(String);

impl Scrape {
    /// Append one line.
    fn line(&mut self, line: std::fmt::Arguments<'_>) {
        // `fmt::Write` for `String` only grows the buffer; it has no error path.
        self.0.write_fmt(line).expect("writing to a String cannot fail");
        self.0.push('\n');
    }

    fn help(&mut self, name: &str, kind: &str, description: &str) {
        self.line(format_args!("# HELP {name} {description}"));
        self.line(format_args!("# TYPE {name} {kind}"));
    }

    fn counter(&mut self, name: &str, description: &str, value: u64) {
        self.help(name, "counter", description);
        self.line(format_args!("{name} {value}"));
    }

    fn gauge(&mut self, name: &str, description: &str, value: u64) {
        self.help(name, "gauge", description);
        self.line(format_args!("{name} {value}"));
    }

    fn connections(&mut self, metrics: &Metrics, gauges: Gauges) {
        self.gauge(
            "orbitcast_connections_current",
            "Sessions currently connected.",
            gauges.connections,
        );
        self.counter(
            "orbitcast_connections_total",
            "WebSocket sessions accepted since start.",
            metrics.connections_total.load(Ordering::Relaxed),
        );

        self.help(
            "orbitcast_connections_rejected_total",
            "counter",
            "Upgrades refused before a session existed, by reason.",
        );
        for reason in ConnectionRejection::ALL {
            let value = metrics.connections_rejected[reason.index()].load(Ordering::Relaxed);
            self.line(format_args!(
                "orbitcast_connections_rejected_total{{reason=\"{}\"}} {value}",
                reason.label()
            ));
        }

        self.gauge(
            "orbitcast_subscriptions_current",
            "Confirmed subscriptions currently held.",
            gauges.subscriptions,
        );
        self.gauge(
            "orbitcast_streams_with_interest",
            "Streams this node currently holds pub/sub interest on.",
            gauges.streams_with_interest,
        );
        self.help(
            "orbitcast_commands_rate_limited_total",
            "counter",
            "Sessions closed for exceeding a per-connection command rate limit, by class.",
        );
        for class in CommandClass::ALL {
            let value = metrics.commands_rate_limited[class.index()].load(Ordering::Relaxed);
            self.line(format_args!(
                "orbitcast_commands_rate_limited_total{{class=\"{}\"}} {value}",
                class.label()
            ));
        }

        self.help(
            "orbitcast_unsupported_commands_total",
            "counter",
            "Client commands dropped because the authorization mode does not execute them.",
        );
        for command in UnsupportedCommand::ALL {
            let value = metrics.unsupported_commands[command.index()].load(Ordering::Relaxed);
            self.line(format_args!(
                "orbitcast_unsupported_commands_total{{command=\"{}\"}} {value}",
                command.label()
            ));
        }

        self.help(
            "orbitcast_socket_lag_disconnects_total",
            "counter",
            "Sessions closed with a reconnectable disconnect because an outbound buffer \
             overflowed.",
        );
        for reason in LagReason::ALL {
            let value = metrics.socket_lag_disconnects[reason.index()].load(Ordering::Relaxed);
            self.line(format_args!(
                "orbitcast_socket_lag_disconnects_total{{reason=\"{}\"}} {value}",
                reason.label()
            ));
        }

        self.counter(
            "orbitcast_hub_queue_overflow_total",
            "Frames dropped because a transport's shared outbound queue was full.",
            metrics.hub_queue_overflow.load(Ordering::Relaxed),
        );
        self.counter(
            "orbitcast_session_reuse_stale_entries_total",
            "Stale hub entries found when a connection id was reused.",
            metrics.session_reuse_stale_entries.load(Ordering::Relaxed),
        );
        if let Some(ready) = gauges.rails_ready {
            self.gauge(
                "orbitcast_rails_ready",
                "Whether the Rails dispatch endpoint passed the describe handshake (1) or not (0).",
                u64::from(ready),
            );
        }
    }

    fn pubsub(&mut self, metrics: &Metrics, stats: Option<StatsSnapshot>) {
        self.counter(
            "orbitcast_pubsub_published_total",
            "Publications this node handed to the backend.",
            metrics.published.load(Ordering::Relaxed),
        );
        self.counter(
            "orbitcast_pubsub_received_total",
            "Publications received from the backend for local fanout.",
            metrics.received.load(Ordering::Relaxed),
        );
        self.counter(
            "orbitcast_pubsub_delivered_total",
            "Addressed frames handed to sockets.",
            metrics.delivered.load(Ordering::Relaxed),
        );

        let Some(stats) = stats else {
            return;
        };

        self.help(
            "orbitcast_pubsub_dropped_total",
            "counter",
            "Publications dropped because a bounded queue was full.",
        );
        self.line(format_args!(
            "orbitcast_pubsub_dropped_total{{queue=\"intake\"}} {}",
            stats.intake_dropped
        ));
        self.line(format_args!(
            "orbitcast_pubsub_dropped_total{{queue=\"delivery\"}} {}",
            stats.delivery_dropped
        ));

        self.counter(
            "orbitcast_pubsub_rejected_total",
            "Notices rejected as malformed, foreign, or misrouted.",
            stats.rejected,
        );
        self.counter(
            "orbitcast_pubsub_overflow_total",
            "Overflow-relay notices received.",
            stats.overflow,
        );
        self.counter(
            "orbitcast_pubsub_overflow_gaps_total",
            "Overflow notices that produced no publication (a delivery gap).",
            stats.overflow_gaps,
        );
        self.counter(
            "orbitcast_pubsub_reconnects_total",
            "Pub/sub listener connection losses.",
            stats.reconnects,
        );
        self.gauge(
            "orbitcast_pubsub_intake_queue_depth_max",
            "Deepest the notification intake queue has been since start.",
            stats.intake_depth_max,
        );
        self.counter(
            "orbitcast_pubsub_gap_disconnects_total",
            "Sessions closed with a reconnectable disconnect because a delivery gap hit a stream \
             they held.",
            metrics.pubsub_gap_disconnects.load(Ordering::Relaxed),
        );
    }

    fn rpc(&mut self, metrics: &Metrics) {
        self.help("orbitcast_rpc_calls_total", "counter", "Rails dispatch calls, by outcome.");
        for outcome in RpcOutcome::ALL {
            let value = metrics.rpc_calls[outcome.index()].load(Ordering::Relaxed);
            self.line(format_args!(
                "orbitcast_rpc_calls_total{{outcome=\"{}\"}} {value}",
                outcome.label()
            ));
        }

        self.help(
            "orbitcast_rpc_latency_seconds",
            "histogram",
            "Rails dispatch round-trip latency, queue wait included.",
        );
        for (index, bound) in RPC_LATENCY_BUCKETS_SECONDS.iter().enumerate() {
            let value = metrics.rpc_latency[index].load(Ordering::Relaxed);
            self.line(format_args!(
                "orbitcast_rpc_latency_seconds_bucket{{le=\"{bound}\"}} {value}"
            ));
        }
        let total = metrics.rpc_latency[RPC_LATENCY_BUCKETS_SECONDS.len()].load(Ordering::Relaxed);
        self.line(format_args!("orbitcast_rpc_latency_seconds_bucket{{le=\"+Inf\"}} {total}"));
        // Seconds with six decimals, split from the integer microsecond total
        // so the sum stays exact at any magnitude (an `f64` would not).
        let sum_micros = metrics.rpc_latency_sum_micros.load(Ordering::Relaxed);
        self.line(format_args!(
            "orbitcast_rpc_latency_seconds_sum {}.{:06}",
            sum_micros / 1_000_000,
            sum_micros % 1_000_000
        ));
        self.line(format_args!("orbitcast_rpc_latency_seconds_count {total}"));
    }

    fn presence(&mut self, leases: u64, presence: Option<PresenceMetrics>) {
        self.gauge(
            "orbitcast_presence_leases_current",
            "Presence leases currently held by connections on this node.",
            leases,
        );

        let Some(presence) = presence else {
            return;
        };

        self.counter(
            "orbitcast_presence_joins_total",
            "Presence join operations applied.",
            presence.joins,
        );
        self.counter(
            "orbitcast_presence_leaves_total",
            "Presence leave operations applied.",
            presence.leaves,
        );
        self.counter(
            "orbitcast_presence_announced_total",
            "Subject-level presence transitions published.",
            presence.announced,
        );
        self.counter(
            "orbitcast_presence_refreshed_total",
            "Leases extended by a batch refresh.",
            presence.refreshed,
        );
        self.counter(
            "orbitcast_presence_sweeps_total",
            "Leases removed by the expiry sweeper.",
            presence.swept,
        );
        self.counter(
            "orbitcast_presence_snapshots_total",
            "Roster snapshot pages served.",
            presence.snapshots,
        );
        self.counter(
            "orbitcast_presence_publish_failures_total",
            "Presence transitions that could not be announced.",
            presence.publish_failures,
        );
    }

    fn broker(&mut self, exposition: BrokerExposition<'_>) {
        let BrokerExposition { namespace, stats: broker } = exposition;
        self.counter(
            "orbitcast_broker_retained_total",
            "Publications committed to retained history.",
            broker.retained,
        );
        self.help(
            "orbitcast_broker_duplicates_total",
            "counter",
            "Repeat publication ids that were not stored again, by kind: identical (a retry) or \
             conflicting (a different payload under one id, a publisher bug).",
        );
        self.line(format_args!(
            "orbitcast_broker_duplicates_total{{kind=\"identical\"}} {}",
            broker.duplicates_identical
        ));
        self.line(format_args!(
            "orbitcast_broker_duplicates_total{{kind=\"conflicting\"}} {}",
            broker.duplicates_conflicting
        ));
        self.counter(
            "orbitcast_broker_replayed_total",
            "Retained messages replayed to resuming sessions.",
            broker.replayed,
        );
        self.counter(
            "orbitcast_broker_gaps_total",
            "Streams a session could not be replayed on.",
            broker.gaps,
        );
        self.counter(
            "orbitcast_broker_reconciled_total",
            "Reader passes that found rows no notification announced.",
            broker.reconciled,
        );
        self.counter(
            "orbitcast_broker_swept_total",
            "Retained rows removed by the sweeper.",
            broker.swept,
        );
        self.counter(
            "orbitcast_broker_fenced_total",
            "Recovery leases taken, each fencing an older holder out.",
            broker.fenced,
        );
        self.help(
            "orbitcast_broker_messages_current",
            "gauge",
            "Unexpired retained messages in this node's namespace, refreshed every reconcile \
             interval.",
        );
        self.line(format_args!(
            "orbitcast_broker_messages_current{{namespace=\"{namespace}\"}} {}",
            broker.messages_current
        ));
        self.help(
            "orbitcast_broker_bytes_current",
            "gauge",
            "Unexpired retained payload bytes in this node's namespace, refreshed every \
             reconcile interval.",
        );
        self.line(format_args!(
            "orbitcast_broker_bytes_current{{namespace=\"{namespace}\"}} {}",
            broker.bytes_current
        ));
    }

    fn ingress(&mut self, metrics: &Metrics) {
        self.help(
            "orbitcast_ingress_requests_total",
            "counter",
            "Private ingress requests, by reply status.",
        );
        for status in IngressStatus::ALL {
            let value = metrics.ingress_requests[status.index()].load(Ordering::Relaxed);
            self.line(format_args!(
                "orbitcast_ingress_requests_total{{status=\"{}\"}} {value}",
                status.label()
            ));
        }
    }

    fn signatures(&mut self, metrics: &Metrics) {
        self.help(
            "orbitcast_signature_verifications_total",
            "counter",
            "Signed-stream HMAC checks, by profile and which ring key matched (failed = none).",
        );
        for profile in SignatureProfile::ALL {
            for key in SignatureKey::ALL {
                let value = metrics.signature_verifications(profile, key);
                self.line(format_args!(
                    "orbitcast_signature_verifications_total{{profile=\"{}\",key=\"{}\"}} {value}",
                    profile.label(),
                    key.label()
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests;
