//! OrbitCast configuration
//!
//! Configuration comes from, in precedence order (first wins):
//! 1. Environment variables (`MS_*` for the docking transport, `ORBITCAST_*`
//!    for everything else)
//! 2. An optional TOML file named by `ORBITCAST_CONFIG`. Unknown keys in the
//!    file are a hard error.
//! 3. Handshake with Mothership (`Moored.config`), when docked. A handshake
//!    value only fills a key that neither the environment nor the file set
//!    ([`Config::apply_moored_config`]); an unknown key, a refused key (binds,
//!    credentials), or a value that does not parse is a startup error.
//! 4. Defaults.
//!
//! # Transports
//!
//! Two transports can be selected independently, and both may run at once:
//!
//! - **docking**: enabled when the `MS_*` supervisor environment is present.
//!   Unchanged behaviour: OrbitCast binds its Unix socket and waits for
//!   Mothership to dock.
//! - **HTTP/WebSocket**: enabled when [`HttpConfig::listen_addr`] is set
//!   (`ORBITCAST_LISTEN_ADDR`), so the binary runs without a supervisor.
//!
//! [`Config::validate`] is the boundary check: it rejects a configuration that
//! selects no transport at all, and every out-of-range limit. Configuration is
//! deliberately stricter than the tolerant wire envelopes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD};

use crate::broker::BrokerConfig;
use crate::presence::{
    DEFAULT_PRESENCE_REFRESH_INTERVAL_MS, DEFAULT_PRESENCE_TTL_MS, MAX_PRESENCE_TTL_MS,
};
use crate::protocol::v1::rpc::{HOP_BY_HOP_HEADERS, MAX_DEADLINE_MS};
use crate::pubsub::{DEFAULT_ACK_TIMEOUT, DEFAULT_QUEUE_CAPACITY, OverflowConfig};
use crate::rpc_client::{
    DEFAULT_DEADLINE, DEFAULT_FAILURE_THRESHOLD, DEFAULT_HALF_OPEN_TIMEOUT,
    DEFAULT_MAX_CONCURRENCY, RpcConfig,
};
use crate::streams::{PreviousKeys, SecretBytes, StreamsConfig};

/// Default per-message limit for inbound WebSocket frames (64 KiB).
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024;

/// Hard ceiling on the configurable per-message limit (16 MiB).
pub const MAX_MESSAGE_SIZE_LIMIT: usize = 16 * 1024 * 1024;

/// Default ceiling on simultaneously accepted WebSocket connections.
pub const DEFAULT_MAX_CONNECTIONS: usize = 10_000;

/// Default drain deadline applied to SIGTERM/ctrl-c shutdown.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);

/// Default per-IP connection admission rate, in new connections per minute.
///
/// A browser tab opens one socket and reconnects with backoff, so 60/min is
/// far above any honest client and still bounds a reconnect storm from one
/// address. Set `0` to disable the limit entirely.
pub const DEFAULT_RATE_CONNECT_PER_IP_PER_MIN: u32 = 60;

/// Default per-session client command rate, in commands per second.
///
/// Subscribe/unsubscribe/perform traffic from a real client is bursty but
/// small; 20/s absorbs a page that subscribes a dozen channels at load and
/// still refuses a loop. Set `0` to disable the limit entirely.
pub const DEFAULT_RATE_COMMANDS_PER_CONN_PER_SEC: u32 = 20;

/// Default per-session subscribe/unsubscribe rate, in commands per second.
///
/// Subscribe-class commands are charged separately from actions so a busy
/// `perform` loop cannot starve a page's (un)subscribes, and vice versa.
pub const DEFAULT_RATE_SUBSCRIBES_PER_CONN_PER_SEC: u32 = 20;

/// Default bound on one socket's outbound queue, in frames.
pub const DEFAULT_OUTBOUND_QUEUE_MESSAGES: usize = 256;

/// Default bound on one socket's outbound queue, in bytes (1 MiB).
pub const DEFAULT_OUTBOUND_QUEUE_BYTES: usize = 1024 * 1024;

/// Default ceiling on the `/cable` upgrade request's header section (16 KiB).
pub const DEFAULT_MAX_HEADER_BYTES: usize = 16 * 1024;

/// Floor on the configurable header ceiling: rama's HTTP/1 parser refuses a
/// smaller read buffer.
pub const MIN_MAX_HEADER_BYTES: usize = 8192;

/// Hard ceiling on the configurable header ceiling (1 MiB).
pub const MAX_MAX_HEADER_BYTES: usize = 1024 * 1024;

/// Default deadline for completing the WebSocket handshake.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Hard ceiling on the configurable handshake deadline (10 minutes).
pub const MAX_HANDSHAKE_TIMEOUT: Duration = Duration::from_mins(10);

/// Hard ceiling on either configurable rate limit.
///
/// Not a capacity claim: a bound above this is indistinguishable from "off",
/// and writing `0` says that on purpose.
pub const MAX_RATE_LIMIT: u32 = 1_000_000;

/// Mothership docking parameters.
///
/// Present only when the supervisor environment (`MS_PID` + `MS_SHIP`) was
/// exported; absent for a standalone run that only serves HTTP.
#[derive(Debug, Clone)]
pub struct DockingConfig {
    /// Mothership PID
    pub mothership_pid: u32,
    /// Ship name
    pub ship_name: String,
    /// Socket directory
    pub socket_dir: PathBuf,
    /// Routes this ship handles (received from Mothership)
    pub routes: Vec<String>,
}

impl DockingConfig {
    /// Get the Unix socket path
    #[must_use]
    pub fn socket_path(&self) -> PathBuf {
        self.socket_dir.join(format!("{}.sock", self.ship_name))
    }
}

/// Direct HTTP/WebSocket listener settings.
///
/// The listener is disabled while [`Self::listen_addr`] is `None`; there is no
/// implicit default address, so a supervised ship never accidentally opens a
/// public port.
#[derive(Debug, Clone)]
pub struct HttpConfig {
    /// Address to bind, e.g. `127.0.0.1:8080`. `None` disables the listener.
    ///
    /// Must be a numeric `IP:port` pair: names are not resolved, so a typo can
    /// never widen the bind to an unintended interface.
    pub listen_addr: Option<String>,
    /// Origins allowed to open a WebSocket, already normalized.
    ///
    /// A request without an `Origin` header is always allowed (non-browser
    /// clients do not send one). A request *with* an `Origin` header is allowed
    /// only when the normalized origin is listed, so an empty list rejects
    /// every browser origin. The single entry `*` allows any origin and is a
    /// development-only escape hatch.
    pub allowed_origins: Vec<String>,
    /// Per-message limit for inbound WebSocket frames, in bytes.
    pub max_message_size: usize,
    /// Maximum simultaneous WebSocket connections; further upgrades get 503.
    pub max_connections: usize,
    /// New connections one remote address may open per minute, as a token
    /// bucket whose burst equals this rate. Exceeding it is answered `429`
    /// before the upgrade. `0` disables the limit.
    ///
    /// The address is the peer address of the TCP connection. Behind a reverse
    /// proxy that is the proxy, so every client shares one bucket: raise the
    /// limit, or leave it off, and rate-limit at the proxy where the real
    /// client address is known. Forwarded headers are deliberately *not*
    /// trusted here - a client-settable header is not an identity.
    pub rate_connect_per_ip_per_min: u32,
    /// Action-class client commands (`message`/`perform`, presence, whisper)
    /// one session may send per second, as a token bucket whose burst equals
    /// this rate. Exceeding it closes the session with a reconnectable
    /// `disconnect`. `0` disables the limit.
    pub rate_commands_per_conn_per_sec: u32,
    /// Subscribe-class client commands (`subscribe`/`unsubscribe`) one session
    /// may send per second, charged separately from actions. `0` disables.
    pub rate_subscribes_per_conn_per_sec: u32,
    /// Deadline for completing the WebSocket handshake, from the moment the
    /// `/cable` request reaches the router until the socket is established.
    /// A handshake that misses it is abandoned and counted. The docking
    /// transport reuses it for its own handshake and for incomplete frames.
    pub handshake_timeout: Duration,
    /// Bound on one socket's outbound queue, in frames. Overflow is lag: the
    /// socket is closed with a reconnectable `disconnect` and counted.
    pub outbound_queue_messages: usize,
    /// Bound on one socket's outbound queue, in bytes. Same overflow policy.
    pub outbound_queue_bytes: usize,
    /// Ceiling on the header section of the `/cable` upgrade request (and
    /// every other request on the public listener), in bytes.
    pub max_header_bytes: usize,
    /// Peers allowed to supply the client address through `Forwarded` /
    /// `X-Forwarded-For`. A request from any other peer uses the TCP peer
    /// address, whatever the headers say.
    pub trusted_proxies: Vec<Cidr>,
    /// Serve `GET /metrics` on this (browser-facing) listener, without
    /// authentication.
    ///
    /// Default `false`: metrics live on the private ingress listener, behind
    /// its bearer credential. Enabling this exposes connection, queue, and
    /// dependency counters to anyone who can reach `/cable`; it exists for
    /// deployments whose only listener is already on a private interface. See
    /// [`crate::metrics`] for the full tradeoff.
    pub metrics_public: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            listen_addr: None,
            allowed_origins: Vec::new(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            rate_connect_per_ip_per_min: DEFAULT_RATE_CONNECT_PER_IP_PER_MIN,
            rate_commands_per_conn_per_sec: DEFAULT_RATE_COMMANDS_PER_CONN_PER_SEC,
            rate_subscribes_per_conn_per_sec: DEFAULT_RATE_SUBSCRIBES_PER_CONN_PER_SEC,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            outbound_queue_messages: DEFAULT_OUTBOUND_QUEUE_MESSAGES,
            outbound_queue_bytes: DEFAULT_OUTBOUND_QUEUE_BYTES,
            max_header_bytes: DEFAULT_MAX_HEADER_BYTES,
            trusted_proxies: Vec::new(),
            metrics_public: false,
        }
    }
}

impl HttpConfig {
    /// Whether the direct listener is configured.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.listen_addr.is_some()
    }

    /// Whether a request carrying `origin` may open a WebSocket.
    ///
    /// `None` (no `Origin` header) is allowed; anything else must match the
    /// configured allow-list exactly after normalization.
    #[must_use]
    pub fn allows_origin(&self, origin: Option<&str>) -> bool {
        let Some(origin) = origin else {
            return true;
        };

        let origin = normalize_origin(origin);
        self.allowed_origins.iter().any(|allowed| allowed == "*" || *allowed == origin)
    }

    /// Whether `peer` is a proxy whose forwarding headers are trusted.
    #[must_use]
    pub fn is_trusted_proxy(&self, peer: IpAddr) -> bool {
        self.trusted_proxies.iter().any(|cidr| cidr.contains(peer))
    }

    /// The client address a request from `peer` is attributed to.
    ///
    /// Only when `peer` is a trusted proxy may `forwarded` (the parsed hop
    /// chain, client first) supply it: the chain is walked from the proxy
    /// side, every trusted hop is skipped, and the first address that is not
    /// a trusted proxy is the client. Otherwise - an untrusted peer, no
    /// header, or a chain made only of trusted proxies - the peer itself is.
    #[must_use]
    pub fn client_ip(&self, peer: IpAddr, forwarded: &[IpAddr]) -> IpAddr {
        if !self.is_trusted_proxy(peer) {
            return peer;
        }
        forwarded.iter().rev().copied().find(|hop| !self.is_trusted_proxy(*hop)).unwrap_or(peer)
    }

    /// Validate the listener settings at the configuration boundary.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidValue`] naming the key when `listen_addr` is not
    /// a numeric `IP:port`, an `allowed_origins` entry is neither `*` nor
    /// `scheme://host[:port]`, `max_message_size` is zero or above
    /// [`MAX_MESSAGE_SIZE_LIMIT`], `max_connections` is zero, a rate
    /// limit exceeds [`MAX_RATE_LIMIT`], the handshake deadline is zero or
    /// above [`MAX_HANDSHAKE_TIMEOUT`], an outbound queue bound is zero, or
    /// `max_header_bytes` is below [`MIN_MAX_HEADER_BYTES`] or above
    /// [`MAX_MAX_HEADER_BYTES`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(ref addr) = self.listen_addr {
            addr.parse::<SocketAddr>().map_err(|_| {
                ConfigError::InvalidValue("listen_addr", "expected a numeric IP:port pair")
            })?;
        }

        for origin in &self.allowed_origins {
            if origin == "*" {
                continue;
            }
            if origin.is_empty() || !origin.contains("://") || origin.contains(char::is_whitespace)
            {
                return Err(ConfigError::InvalidValue(
                    "allowed_origins",
                    "expected `*` or entries of the form scheme://host[:port]",
                ));
            }
        }

        if self.max_message_size == 0 || self.max_message_size > MAX_MESSAGE_SIZE_LIMIT {
            return Err(ConfigError::InvalidValue(
                "max_message_size",
                "expected 1..=16777216 bytes",
            ));
        }

        if self.max_connections == 0 {
            return Err(ConfigError::InvalidValue("max_connections", "expected at least 1"));
        }

        // `0` is the documented "off" spelling for both limiters; anything
        // above the ceiling is off with extra steps, and saying so explicitly
        // keeps a fat-fingered value from reading as a deliberate bound.
        if self.rate_connect_per_ip_per_min > MAX_RATE_LIMIT {
            return Err(ConfigError::InvalidValue(
                "rate_connect_per_ip_per_min",
                "expected 0 (disabled) or 1..=1000000 connections per minute",
            ));
        }

        if self.rate_commands_per_conn_per_sec > MAX_RATE_LIMIT {
            return Err(ConfigError::InvalidValue(
                "rate_commands_per_conn_per_sec",
                "expected 0 (disabled) or 1..=1000000 commands per second",
            ));
        }

        if self.rate_subscribes_per_conn_per_sec > MAX_RATE_LIMIT {
            return Err(ConfigError::InvalidValue(
                "rate_subscribes_per_conn_per_sec",
                "expected 0 (disabled) or 1..=1000000 commands per second",
            ));
        }

        if self.outbound_queue_messages == 0 {
            return Err(ConfigError::InvalidValue(
                "outbound_queue_messages",
                "expected at least 1",
            ));
        }
        if self.outbound_queue_bytes == 0 {
            return Err(ConfigError::InvalidValue("outbound_queue_bytes", "expected at least 1"));
        }

        if !(MIN_MAX_HEADER_BYTES..=MAX_MAX_HEADER_BYTES).contains(&self.max_header_bytes) {
            return Err(ConfigError::InvalidValue(
                "max_header_bytes",
                "expected 8192..=1048576 bytes",
            ));
        }

        // A zero handshake deadline would refuse every connection, so it is an
        // error rather than an "off" switch: there is no safe way to wait
        // forever for a half-open upgrade.
        if self.handshake_timeout.is_zero() || self.handshake_timeout > MAX_HANDSHAKE_TIMEOUT {
            return Err(ConfigError::InvalidValue(
                "handshake_timeout_ms",
                "expected 1..=600000 milliseconds",
            ));
        }

        Ok(())
    }
}

/// An IPv4 or IPv6 network in CIDR notation (`10.0.0.0/8`, `fd00::/8`).
///
/// A bare address is the `/32` (or `/128`) network of itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cidr {
    network: IpAddr,
    prefix: u8,
}

impl Cidr {
    /// Parse `addr[/prefix]`.
    ///
    /// # Errors
    ///
    /// A malformed address, a non-numeric prefix, or a prefix longer than the
    /// address family allows.
    pub fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        let (addr, prefix) = match text.split_once('/') {
            Some((addr, prefix)) => (addr, Some(prefix)),
            None => (text, None),
        };
        let network: IpAddr =
            addr.parse().map_err(|_| format!("{text:?} is not an IP address or CIDR network"))?;
        let bits = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        let prefix = match prefix {
            Some(prefix) => prefix
                .parse::<u8>()
                .ok()
                .filter(|prefix| *prefix <= bits)
                .ok_or_else(|| format!("{text:?} has an invalid prefix length (0..={bits})"))?,
            None => bits,
        };
        Ok(Self { network, prefix })
    }

    /// Whether `ip` is inside this network. Families never match each other.
    #[must_use]
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => {
                let mask = u32::MAX.checked_shl(u32::from(32 - self.prefix)).unwrap_or(0);
                u32::from(network) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(network), IpAddr::V6(ip)) => {
                let mask = u128::MAX.checked_shl(u32::from(128 - self.prefix)).unwrap_or(0);
                u128::from(network) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }
}

impl std::fmt::Display for Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix)
    }
}

/// Parse a comma-separated (or TOML-array) list of CIDR networks.
///
/// Empty entries are dropped; a malformed entry is an error, never a silently
/// narrower (or wider) trust list.
fn parse_cidrs(entries: impl IntoIterator<Item = String>) -> Result<Vec<Cidr>, ConfigError> {
    entries
        .into_iter()
        .map(|entry| entry.trim().trim_matches(['[', ']', '"', '\''].as_slice()).to_string())
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            Cidr::parse(&entry).map_err(|_| {
                ConfigError::InvalidValue(
                    "trusted_proxies",
                    "expected comma-separated IP addresses or CIDR networks",
                )
            })
        })
        .collect()
}

/// Split a comma-separated proxy list into its entries.
fn split_list(value: &str) -> Vec<String> {
    value.split(',').map(str::to_string).collect()
}

/// Redact the password of a connection URL for logs and `Debug` output.
///
/// `scheme://user:secret@host/db` becomes `scheme://user:[REDACTED]@host/db`;
/// a URL without credentials is returned unchanged. Only the userinfo part is
/// touched, so the host and database stay readable for diagnostics.
#[must_use]
pub fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let Some((userinfo, host)) = rest.rsplit_once('@') else {
        return url.to_string();
    };
    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:[REDACTED]@{host}"),
        None => url.to_string(),
    }
}

/// Default per-session command mailbox capacity, in commands.
pub const DEFAULT_RPC_MAILBOX_CAPACITY: usize = 32;

/// Default bound on the `release_connection` cleanup queue, in jobs.
pub const DEFAULT_RPC_CLEANUP_CAPACITY: usize = 1024;

/// Headers forwarded to Rails when no allowlist is configured.
///
/// Minimal on purpose: the credentials Rails authentication
/// actually reads, and nothing else. Everything Rails needs about the request
/// besides these is reconstructed from validated metadata.
pub const DEFAULT_RPC_FORWARD_HEADERS: &[&str] = &["cookie", "authorization"];

/// How the server decides who may connect and subscribe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchMode {
    /// Signed mode: local signed-stream authorization only; no Rails calls.
    SignedOnly,
    /// Dispatch mode: every connection and subscription is authorized by Rails.
    ///
    /// There is no fallback from this mode to [`DispatchMode::SignedOnly`] on
    /// an outage: a Rails outage closes connections, it does not widen access.
    RailsDispatch,
}

/// Rails-dispatch RPC settings.
///
/// `endpoint_url` is the switch: set it and the node runs in
/// [`DispatchMode::RailsDispatch`], leave it unset and nothing here is used.
///
/// The service credential is **environment-only**
/// (`ORBITCAST_RPC_TOKEN_BASE64`, strict base64 of the raw token bytes). It is
/// deliberately not readable from the TOML file or the Moored handshake: new
/// secrets do not get a plain-text configuration path.
#[derive(Debug, Clone)]
pub struct RpcSettings {
    /// Base URL of the private Rails endpoint. `None` disables dispatch mode.
    pub endpoint_url: Option<String>,
    /// Bearer credential, from `ORBITCAST_RPC_TOKEN_BASE64` only.
    pub token: Option<SecretBytes>,
    /// End-to-end deadline for one call.
    pub deadline: Duration,
    /// Ceiling on simultaneously in-flight calls, tied to Rails capacity.
    pub max_concurrency: usize,
    /// Per-session command mailbox capacity. A full mailbox is an overload:
    /// the session is closed with `reconnect: true` rather than queued forever.
    pub mailbox_capacity: usize,
    /// Bound on the best-effort `release_connection` cleanup queue.
    pub cleanup_capacity: usize,
    /// Failures that open the RPC circuit.
    pub failure_threshold: usize,
    /// Cooldown before the circuit admits a probe.
    pub half_open_timeout: Duration,
    /// Lowercase header names forwarded to Rails.
    pub forward_headers: Vec<String>,
    /// Stable identifier of this node, reported in every request.
    pub node_id: String,
}

impl Default for RpcSettings {
    fn default() -> Self {
        Self {
            endpoint_url: None,
            token: None,
            deadline: DEFAULT_DEADLINE,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            mailbox_capacity: DEFAULT_RPC_MAILBOX_CAPACITY,
            cleanup_capacity: DEFAULT_RPC_CLEANUP_CAPACITY,
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            half_open_timeout: DEFAULT_HALF_OPEN_TIMEOUT,
            forward_headers: DEFAULT_RPC_FORWARD_HEADERS
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            node_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

impl RpcSettings {
    /// Whether Rails dispatch is configured.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.endpoint_url.is_some()
    }

    /// The caller configuration for these settings, if dispatch is enabled.
    ///
    /// Returns `None` in signed-only mode; [`Config::validate`] has already
    /// guaranteed the credential is present when the endpoint is.
    #[must_use]
    pub fn to_rpc_config(&self) -> Option<RpcConfig> {
        let endpoint_url = self.endpoint_url.clone()?;
        let token = self.token.clone()?;

        let mut config = RpcConfig::new(endpoint_url, token);
        config.deadline = self.deadline;
        config.max_concurrency = self.max_concurrency;
        config.failure_threshold = self.failure_threshold;
        config.half_open_timeout = self.half_open_timeout;
        Some(config)
    }

    /// Filter request headers down to the configured allowlist.
    ///
    /// Names are compared lowercased; hop-by-hop headers are dropped even if
    /// someone adds one to the allowlist, because they describe *this* hop.
    #[must_use]
    pub fn allowlisted_headers(
        &self,
        headers: &HashMap<String, String>,
    ) -> BTreeMap<String, String> {
        let mut forwarded = BTreeMap::new();

        for (name, value) in headers {
            let name = name.to_ascii_lowercase();
            if HOP_BY_HOP_HEADERS.contains(&name.as_str()) {
                continue;
            }
            if !self.forward_headers.contains(&name) {
                continue;
            }
            forwarded.insert(name, value.clone());
        }

        forwarded
    }

    /// Validate the RPC settings at the configuration boundary.
    fn validate(&self) -> Result<(), ConfigError> {
        let Some(ref endpoint) = self.endpoint_url else {
            return Ok(());
        };

        let authority = if let Some(rest) = endpoint.strip_prefix("https://") {
            rest
        } else if let Some(rest) = endpoint.strip_prefix("http://") {
            // TLS is required between hosts. Plain HTTP is only tolerated to a
            // loopback address, which is the sidecar and
            // test-suite case.
            if !is_loopback_authority(rest) {
                return Err(ConfigError::InvalidValue(
                    "rpc_endpoint_url",
                    "plain http is only allowed for a loopback endpoint; use https",
                ));
            }
            rest
        } else {
            return Err(ConfigError::InvalidValue(
                "rpc_endpoint_url",
                "expected an http:// or https:// URL",
            ));
        };

        if authority.is_empty() || authority.starts_with('/') {
            return Err(ConfigError::InvalidValue("rpc_endpoint_url", "expected a host"));
        }

        // Never fall back to signed-only: an endpoint without a credential is a
        // boot-time error, not a silently unauthenticated call.
        if self.token.as_ref().is_none_or(|token| token.as_bytes().is_empty()) {
            return Err(ConfigError::InvalidValue(
                "rpc_endpoint_url",
                "Rails dispatch requires ORBITCAST_RPC_TOKEN_BASE64 (strict base64 \
                 of the service token); it is never read from the config file",
            ));
        }

        let deadline_ms = self.deadline.as_millis();
        if deadline_ms == 0 || deadline_ms > u128::from(MAX_DEADLINE_MS) {
            return Err(ConfigError::InvalidValue(
                "rpc_deadline_ms",
                "expected 1..=60000 milliseconds",
            ));
        }

        if self.max_concurrency == 0 {
            return Err(ConfigError::InvalidValue("rpc_max_concurrency", "expected at least 1"));
        }
        if self.mailbox_capacity == 0 {
            return Err(ConfigError::InvalidValue("rpc_mailbox_capacity", "expected at least 1"));
        }
        if self.cleanup_capacity == 0 {
            return Err(ConfigError::InvalidValue("rpc_cleanup_capacity", "expected at least 1"));
        }
        if self.failure_threshold == 0 {
            return Err(ConfigError::InvalidValue("rpc_failure_threshold", "expected at least 1"));
        }
        if self.half_open_timeout.is_zero() {
            return Err(ConfigError::InvalidValue(
                "rpc_half_open_timeout_ms",
                "expected a non-zero duration",
            ));
        }

        if self.node_id.is_empty() {
            return Err(ConfigError::InvalidValue("node_id", "must not be empty"));
        }

        for name in &self.forward_headers {
            if name.is_empty() || name.chars().any(|c| c.is_ascii_uppercase() || c.is_whitespace())
            {
                return Err(ConfigError::InvalidValue(
                    "rpc_forward_headers",
                    "expected lowercase header names without whitespace",
                ));
            }
            if HOP_BY_HOP_HEADERS.contains(&name.as_str()) {
                return Err(ConfigError::InvalidValue(
                    "rpc_forward_headers",
                    "hop-by-hop headers are never forwarded",
                ));
            }
        }

        Ok(())
    }
}

/// Private HTTP ingress settings: the alternative publication path to
/// PostgreSQL.
///
/// The ingress is a *separate* rama listener serving `POST /v1/publications`
/// and `POST /v1/control`; it is never mounted on the public `/cable`
/// listener. Disabled while [`Self::listen_addr`] is `None`: a node with only
/// the public listener has no ingress at all, so publication ingestion can
/// never be silently exposed publicly.
///
/// The bearer credential is **environment-only**
/// (`ORBITCAST_INGRESS_TOKEN_BASE64`, strict base64 of the raw token bytes),
/// like every other service credential: it is not readable from the TOML file
/// or the Moored handshake.
#[derive(Debug, Clone, Default)]
pub struct IngressSettings {
    /// Address to bind the private ingress listener, e.g. `127.0.0.1:28081`.
    /// `None` disables the ingress. Loopback is the intended default shape;
    /// any non-loopback bind must be explicitly allowed (fail closed).
    pub listen_addr: Option<String>,
    /// Bearer credential, from `ORBITCAST_INGRESS_TOKEN_BASE64` only.
    pub token: Option<SecretBytes>,
    /// Explicit opt-in for binding the ingress to a non-loopback address
    /// (`ORBITCAST_INGRESS_ALLOW_NONLOCAL` / `ingress_allow_nonlocal`). The
    /// listener speaks plain HTTP, so a non-local bind needs a protected
    /// network path in front of it - hence the explicit switch.
    pub allow_nonlocal: bool,
}

impl IngressSettings {
    /// Whether the private ingress listener is configured.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.listen_addr.is_some()
    }

    /// Validate the ingress settings at the configuration boundary.
    fn validate(&self) -> Result<(), ConfigError> {
        let Some(ref addr) = self.listen_addr else {
            return Ok(());
        };

        let addr: SocketAddr = addr.parse().map_err(|_| {
            ConfigError::InvalidValue("ingress_listen_addr", "expected a numeric IP:port pair")
        })?;

        if !addr.ip().is_loopback() && !self.allow_nonlocal {
            return Err(ConfigError::InvalidValue(
                "ingress_listen_addr",
                "non-loopback ingress binds require ingress_allow_nonlocal \
                 (ORBITCAST_INGRESS_ALLOW_NONLOCAL=true) behind a protected network path",
            ));
        }

        // Never an unauthenticated publication path: the ingress does not
        // start without its credential.
        if self.token.as_ref().is_none_or(|token| token.as_bytes().is_empty()) {
            return Err(ConfigError::InvalidValue(
                "ingress_listen_addr",
                "the ingress requires ORBITCAST_INGRESS_TOKEN_BASE64 (strict base64 of the \
                 service token); it is never read from the config file",
            ));
        }

        Ok(())
    }
}

/// Whether the authority part of a URL points at this machine.
///
/// Conservative on purpose: anything it cannot prove is loopback is treated as
/// a remote host, which then requires TLS.
fn is_loopback_authority(rest: &str) -> bool {
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    // `[::1]` is a bracketed host with no port; `[::1]:8080` and `host:8080`
    // both split on their last colon, which is outside the brackets.
    let host = if authority.ends_with(']') {
        authority
    } else {
        match authority.rsplit_once(':') {
            Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) => host,
            _ => authority,
        }
    };

    matches!(host.trim_start_matches('[').trim_end_matches(']'), "localhost" | "127.0.0.1" | "::1")
}

/// Normalize an origin for comparison: trimmed, lowercased, no trailing slash.
///
/// Origins are ASCII scheme/host/port triples, so ASCII-lowercasing is the
/// correct (and allocation-cheap) normalization.
fn normalize_origin(origin: &str) -> String {
    origin.trim().trim_end_matches('/').to_ascii_lowercase()
}

/// Values loaded from the optional TOML file named by `ORBITCAST_CONFIG`.
///
/// Every field is optional; a present field sits between the environment and
/// the defaults in precedence (environment > file > defaults). Key names match
/// the docking (`Moored`) handshake names, plus `listen_addr`, which the
/// handshake deliberately does not accept. `deny_unknown_fields` makes a typo
/// a hard startup error instead of a silently ignored setting.
#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen_addr: Option<String>,
    allowed_origins: Option<Vec<String>>,
    max_message_size: Option<usize>,
    max_connections: Option<usize>,
    rate_connect_per_ip_per_min: Option<u32>,
    rate_commands_per_conn_per_sec: Option<u32>,
    rate_subscribes_per_conn_per_sec: Option<u32>,
    handshake_timeout_ms: Option<u64>,
    outbound_queue_messages: Option<usize>,
    outbound_queue_bytes: Option<usize>,
    max_header_bytes: Option<usize>,
    trusted_proxies: Option<Vec<String>>,
    metrics_public: Option<bool>,
    shutdown_timeout_ms: Option<u64>,
    namespace: Option<String>,
    database_url: Option<String>,
    listener_database_url: Option<String>,
    ping_interval: Option<u64>,
    overflow_enabled: Option<bool>,
    overflow_ttl_ms: Option<u64>,
    max_publication_size: Option<usize>,
    streams_secret: Option<String>,
    streams_previous_secrets: Option<Vec<String>>,
    public_streams: Option<bool>,
    public_streams_insecure: Option<bool>,
    turbo_streams: Option<bool>,
    turbo_streams_secret: Option<String>,
    turbo_previous_secrets: Option<Vec<String>>,
    cable_ready_streams: Option<bool>,
    cable_ready_secret: Option<String>,
    cable_ready_previous_secrets: Option<Vec<String>>,
    presence_ttl_ms: Option<u64>,
    presence_refresh_interval_ms: Option<u64>,
    broker_enabled: Option<bool>,
    broker_streams: Option<Vec<String>>,
    broker_retention_secs: Option<u64>,
    broker_max_messages_per_stream: Option<u64>,
    broker_max_bytes_per_stream: Option<u64>,
    broker_reconcile_interval_ms: Option<u64>,
    broker_replay_buffer: Option<usize>,
    broker_session_ttl_secs: Option<u64>,
    broker_max_messages_per_namespace: Option<u64>,
    broker_max_bytes_per_namespace: Option<u64>,
    recovery_secret: Option<String>,
    node_id: Option<String>,
    rpc_endpoint_url: Option<String>,
    rpc_deadline_ms: Option<u64>,
    rpc_max_concurrency: Option<usize>,
    rpc_mailbox_capacity: Option<usize>,
    rpc_cleanup_capacity: Option<usize>,
    rpc_failure_threshold: Option<usize>,
    rpc_half_open_timeout_ms: Option<u64>,
    rpc_forward_headers: Option<Vec<String>>,
    ingress_listen_addr: Option<String>,
    ingress_allow_nonlocal: Option<bool>,
}

impl std::fmt::Debug for FileConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Plain-text keys and the database password never reach a log line.
        let redacted = |secret: &Option<String>| secret.as_ref().map(|_| "[REDACTED]");
        f.debug_struct("FileConfig")
            .field("listen_addr", &self.listen_addr)
            .field("namespace", &self.namespace)
            .field("database_url", &self.database_url.as_deref().map(redact_url))
            .field("listener_database_url", &self.listener_database_url.as_deref().map(redact_url))
            .field("streams_secret", &redacted(&self.streams_secret))
            .field("turbo_streams_secret", &redacted(&self.turbo_streams_secret))
            .field("cable_ready_secret", &redacted(&self.cable_ready_secret))
            .field("recovery_secret", &redacted(&self.recovery_secret))
            .field("rpc_endpoint_url", &self.rpc_endpoint_url)
            .field("ingress_listen_addr", &self.ingress_listen_addr)
            .finish_non_exhaustive()
    }
}

impl FileConfig {
    /// Load the file named by `ORBITCAST_CONFIG`, if any.
    ///
    /// An explicitly named file that cannot be read or parsed is a hard
    /// boundary error: a mistyped path must never silently run on defaults.
    fn load(env: Env<'_>) -> Result<Self, ConfigError> {
        let Some(path) = env.non_empty("ORBITCAST_CONFIG") else {
            return Ok(Self::default());
        };

        let text = std::fs::read_to_string(&path).map_err(|e| ConfigError::ConfigFile {
            path: path.clone(),
            message: format!("cannot read: {e}"),
        })?;

        toml::from_str(&text).map_err(|e| ConfigError::ConfigFile { path, message: e.to_string() })
    }

    /// A plain-text secret from the file as raw key bytes.
    ///
    /// An explicitly written empty secret is a hard error, matching the strict
    /// treatment of the base64 environment form.
    fn secret(
        field: &'static str,
        value: Option<&str>,
    ) -> Result<Option<SecretBytes>, ConfigError> {
        match value {
            Some("") => Err(ConfigError::InvalidValue(field, "must not be empty")),
            Some(text) => Ok(Some(SecretBytes::new(text.as_bytes().to_vec()))),
            None => Ok(None),
        }
    }

    /// Plain-text verify-only previous keys from the file, as raw key bytes.
    ///
    /// Every entry must be non-empty and the list must fit the ring bound
    /// ([`crate::streams::MAX_PREVIOUS_KEYS`]); absent means no previous key.
    fn secret_list(
        field: &'static str,
        values: Option<&[String]>,
    ) -> Result<PreviousKeys, ConfigError> {
        let Some(values) = values else {
            return Ok(PreviousKeys::default());
        };
        let keys = values
            .iter()
            .map(|text| {
                Self::secret(field, Some(text))
                    .map(|key| key.expect("a Some(&str) input yields a Some key"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        previous_keys(field, keys)
    }
}

/// Bound a previous-key list, naming `field` when it is too long.
fn previous_keys(field: &'static str, keys: Vec<SecretBytes>) -> Result<PreviousKeys, ConfigError> {
    PreviousKeys::new(keys).map_err(|e| ConfigError::Invalid(field, e.to_string()))
}

/// Keys the environment or the TOML file set explicitly.
///
/// Recorded by [`Config::from_env`] under their docking-handshake names, so a
/// later handshake only fills what is still unset (environment > file >
/// handshake > defaults). A configuration built by hand (tests) starts empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExplicitKeys(BTreeSet<&'static str>);

impl ExplicitKeys {
    /// Record that `key` was set explicitly.
    pub fn mark(&mut self, key: &'static str) {
        self.0.insert(key);
    }

    /// Whether `key` was set explicitly.
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.0.contains(key)
    }
}

/// OrbitCast configuration
#[derive(Clone)]
pub struct Config {
    /// Mothership docking parameters; `None` disables the docking transport.
    pub docking: Option<DockingConfig>,
    /// Direct HTTP/WebSocket listener settings.
    pub http: HttpConfig,
    /// Deadline for draining sessions during shutdown (process-wide).
    pub shutdown_timeout: Duration,
    /// PostgreSQL connection string for publishing (and the overflow,
    /// presence, and broker sessions). Required by the PostgreSQL backend.
    pub database_url: Option<String>,
    /// Connection string for the `LISTEN` session, when it must differ from
    /// `database_url` (a direct connection beside a transaction pooler). Must
    /// address the same cable database. `None` uses `database_url`.
    pub listener_database_url: Option<String>,
    /// Deployment namespace for pub/sub routing.
    ///
    /// Part of every channel name and every publication envelope, and
    /// re-validated on receipt. Empty is invalid for the PostgreSQL backend:
    /// two deployments sharing a database must not share channels.
    pub namespace: String,
    /// Deadline for a `LISTEN`/`UNLISTEN` acknowledgement.
    pub pubsub_ack_timeout: Duration,
    /// Bound on the pub/sub intake and delivery queues.
    pub pubsub_queue_capacity: usize,
    /// Overflow relay settings for publications too large to inline.
    ///
    /// Disabled by default: the relay needs the `orbitcast_payloads` migration,
    /// and nothing here ever issues DDL.
    pub overflow: OverflowConfig,
    /// Ping interval in seconds (default: 3)
    pub ping_interval: u64,
    /// Lifetime of one distributed presence lease.
    ///
    /// Also the liveness window: a connection whose socket produced no inbound
    /// traffic within it is not refreshed. Only used in Rails-dispatch mode
    /// with a PostgreSQL backend; a grant may ask for less, never more.
    pub presence_ttl: Duration,
    /// How often live leases are refreshed, in one batched statement.
    ///
    /// Must be shorter than [`Config::presence_ttl`], with room for a missed
    /// pass: the default 10 s against a 45 s TTL survives three failures.
    pub presence_refresh_interval: Duration,
    /// Retained-history settings.
    ///
    /// Disabled by default: retention needs the broker migration, and nothing
    /// here ever issues DDL.
    pub broker: BrokerConfig,
    /// Dedicated recovery-token key, from `ORBITCAST_RECOVERY_SECRET_BASE64`.
    ///
    /// Never the streams key and never derived from it: without this, the
    /// `Orbitcast::RecoveryChannel` simply does not exist on this node.
    pub recovery_secret: Option<SecretBytes>,
    /// Signed streams configuration
    pub streams: StreamsConfig,
    /// Rails-dispatch RPC settings. Disabled unless `rpc_endpoint_url` is set.
    pub rpc: RpcSettings,
    /// Private HTTP ingress settings. Disabled unless `ingress_listen_addr`
    /// is set; the credential is environment-only.
    pub ingress: IngressSettings,
    /// Which keys the environment or the file set, so the docking handshake
    /// fills only what is still unset.
    pub explicit: ExplicitKeys,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("docking", &self.docking)
            .field("http", &self.http)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("database_url", &self.database_url.as_deref().map(redact_url))
            .field("listener_database_url", &self.listener_database_url.as_deref().map(redact_url))
            .field("namespace", &self.namespace)
            .field("pubsub_ack_timeout", &self.pubsub_ack_timeout)
            .field("pubsub_queue_capacity", &self.pubsub_queue_capacity)
            .field("overflow", &self.overflow)
            .field("ping_interval", &self.ping_interval)
            .field("presence_ttl", &self.presence_ttl)
            .field("presence_refresh_interval", &self.presence_refresh_interval)
            .field("broker", &self.broker)
            .field("recovery_secret", &self.recovery_secret)
            .field("streams", &self.streams)
            .field("rpc", &self.rpc)
            .field("ingress", &self.ingress)
            .field("explicit", &self.explicit)
            .finish()
    }
}

impl Config {
    /// Which authorization mode this configuration selects.
    #[must_use]
    pub const fn dispatch_mode(&self) -> DispatchMode {
        if self.rpc.is_enabled() { DispatchMode::RailsDispatch } else { DispatchMode::SignedOnly }
    }
}

impl Config {
    /// Create config from environment variables and the optional TOML file
    /// named by `ORBITCAST_CONFIG` (environment > file > defaults).
    ///
    /// The `MS_*` supervisor variables are optional: when none of them is set,
    /// the docking transport is simply not selected and the direct listener
    /// (`ORBITCAST_LISTEN_ADDR`) must be configured instead. A *partial* `MS_*`
    /// environment is an error rather than a silent downgrade.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MissingEnv`] / [`ConfigError::InvalidEnv`] for a partial
    /// `MS_*` environment or a malformed `ORBITCAST_*` number, boolean, or
    /// base64 key; [`ConfigError::ConfigFile`] when `ORBITCAST_CONFIG` names a
    /// file that cannot be read or parsed (an unknown key included); and
    /// [`ConfigError::InvalidValue`] for an empty secret in that file. Range
    /// checks are [`Config::validate`]'s job, not this one's.
    pub fn from_env() -> Result<Self, ConfigError> {
        let env = Env::process();
        Self::from_env_over(env, &FileConfig::load(env)?)
    }

    /// Layer the environment over `file` values over defaults.
    ///
    /// Sections are read in a fixed order, so the first malformed value is
    /// always the one reported.
    fn from_env_over(env: Env<'_>, file: &FileConfig) -> Result<Self, ConfigError> {
        let docking = Self::docking_from(env)?;

        // Mothership normally supplies the namespace in the MOORED config; the
        // environment variable keeps standalone runs and local testing usable.
        let namespace = env
            .non_empty("ORBITCAST_NAMESPACE")
            .or_else(|| file.namespace.clone())
            .unwrap_or_default();

        let streams = Self::streams_from(env, file)?;
        let http = Self::http_from(env, file)?;

        let shutdown_timeout = env
            .parse("ORBITCAST_SHUTDOWN_TIMEOUT_MS")?
            .or(file.shutdown_timeout_ms)
            .map_or(DEFAULT_SHUTDOWN_TIMEOUT, Duration::from_millis);

        let overflow = Self::overflow_from(env, file)?;
        let broker = Self::broker_from(env, file)?;

        // The recovery key follows the same rule as every other key: strict
        // base64 in the environment, raw UTF-8 bytes in the file, and no
        // fallback to any other profile's secret.
        let recovery_secret = env.secret_or_file(
            "ORBITCAST_RECOVERY_SECRET_BASE64",
            "recovery_secret",
            file.recovery_secret.as_deref(),
        )?;

        let rpc = Self::rpc_from(env, file)?;
        let ingress = Self::ingress_from(env, file)?;
        let explicit = Self::explicit_keys(env, file);

        Ok(Self {
            docking,
            http,
            shutdown_timeout,
            database_url: env
                .non_empty("ORBITCAST_DATABASE_URL")
                .or_else(|| file.database_url.clone()),
            listener_database_url: env
                .non_empty("ORBITCAST_LISTENER_DATABASE_URL")
                .or_else(|| file.listener_database_url.clone()),
            namespace,
            pubsub_ack_timeout: DEFAULT_ACK_TIMEOUT,
            pubsub_queue_capacity: DEFAULT_QUEUE_CAPACITY,
            overflow,
            ping_interval: file.ping_interval.unwrap_or(3),
            presence_ttl: Duration::from_millis(
                env.parse::<u64>("ORBITCAST_PRESENCE_TTL_MS")?
                    .or(file.presence_ttl_ms)
                    .unwrap_or(DEFAULT_PRESENCE_TTL_MS),
            ),
            presence_refresh_interval: Duration::from_millis(
                env.parse::<u64>("ORBITCAST_PRESENCE_REFRESH_INTERVAL_MS")?
                    .or(file.presence_refresh_interval_ms)
                    .unwrap_or(DEFAULT_PRESENCE_REFRESH_INTERVAL_MS),
            ),
            broker,
            recovery_secret,
            streams,
            rpc,
            ingress,
            explicit,
        })
    }

    /// Which handshake-settable keys the environment or the file supplied.
    ///
    /// One row per key the docking handshake may carry; a key is explicit when
    /// its environment variable is set (non-empty) or its file field is
    /// present. Keys the handshake never carries (binds, credentials) are not
    /// listed: they have nothing to be protected from.
    #[expect(clippy::too_many_lines, reason = "one row per handshake key: a table, not logic")]
    fn explicit_keys(env: Env<'_>, file: &FileConfig) -> ExplicitKeys {
        let set = |var: &'static str| env.non_empty(var).is_some();
        let rows: [(&'static str, bool); _] = [
            ("database_url", set("ORBITCAST_DATABASE_URL") || file.database_url.is_some()),
            (
                "listener_database_url",
                set("ORBITCAST_LISTENER_DATABASE_URL") || file.listener_database_url.is_some(),
            ),
            ("namespace", set("ORBITCAST_NAMESPACE") || file.namespace.is_some()),
            (
                "overflow_enabled",
                set("ORBITCAST_OVERFLOW_ENABLED") || file.overflow_enabled.is_some(),
            ),
            ("overflow_ttl_ms", set("ORBITCAST_OVERFLOW_TTL_MS") || file.overflow_ttl_ms.is_some()),
            (
                "max_publication_size",
                set("ORBITCAST_MAX_PUBLICATION_SIZE") || file.max_publication_size.is_some(),
            ),
            ("ping_interval", file.ping_interval.is_some()),
            ("presence_ttl_ms", set("ORBITCAST_PRESENCE_TTL_MS") || file.presence_ttl_ms.is_some()),
            (
                "presence_refresh_interval_ms",
                set("ORBITCAST_PRESENCE_REFRESH_INTERVAL_MS")
                    || file.presence_refresh_interval_ms.is_some(),
            ),
            ("allowed_origins", set("ORBITCAST_ALLOWED_ORIGINS") || file.allowed_origins.is_some()),
            (
                "max_message_size",
                set("ORBITCAST_MAX_MESSAGE_SIZE") || file.max_message_size.is_some(),
            ),
            ("max_connections", set("ORBITCAST_MAX_CONNECTIONS") || file.max_connections.is_some()),
            (
                "shutdown_timeout_ms",
                set("ORBITCAST_SHUTDOWN_TIMEOUT_MS") || file.shutdown_timeout_ms.is_some(),
            ),
            (
                "rate_connect_per_ip_per_min",
                set("ORBITCAST_RATE_CONNECT_PER_IP_PER_MIN")
                    || file.rate_connect_per_ip_per_min.is_some(),
            ),
            (
                "rate_commands_per_conn_per_sec",
                set("ORBITCAST_RATE_COMMANDS_PER_CONN_PER_SEC")
                    || file.rate_commands_per_conn_per_sec.is_some(),
            ),
            (
                "rate_subscribes_per_conn_per_sec",
                set("ORBITCAST_RATE_SUBSCRIBES_PER_CONN_PER_SEC")
                    || file.rate_subscribes_per_conn_per_sec.is_some(),
            ),
            (
                "handshake_timeout_ms",
                set("ORBITCAST_HANDSHAKE_TIMEOUT_MS") || file.handshake_timeout_ms.is_some(),
            ),
            (
                "outbound_queue_messages",
                set("ORBITCAST_OUTBOUND_QUEUE_MESSAGES") || file.outbound_queue_messages.is_some(),
            ),
            (
                "outbound_queue_bytes",
                set("ORBITCAST_OUTBOUND_QUEUE_BYTES") || file.outbound_queue_bytes.is_some(),
            ),
            (
                "max_header_bytes",
                set("ORBITCAST_MAX_HEADER_BYTES") || file.max_header_bytes.is_some(),
            ),
            ("trusted_proxies", set("ORBITCAST_TRUSTED_PROXIES") || file.trusted_proxies.is_some()),
            ("broker_enabled", set("ORBITCAST_BROKER_ENABLED") || file.broker_enabled.is_some()),
            ("broker_streams", set("ORBITCAST_BROKER_STREAMS") || file.broker_streams.is_some()),
            (
                "broker_retention_secs",
                set("ORBITCAST_BROKER_RETENTION_SECS") || file.broker_retention_secs.is_some(),
            ),
            (
                "broker_max_messages_per_stream",
                set("ORBITCAST_BROKER_MAX_MESSAGES_PER_STREAM")
                    || file.broker_max_messages_per_stream.is_some(),
            ),
            (
                "broker_max_bytes_per_stream",
                set("ORBITCAST_BROKER_MAX_BYTES_PER_STREAM")
                    || file.broker_max_bytes_per_stream.is_some(),
            ),
            (
                "broker_reconcile_interval_ms",
                set("ORBITCAST_BROKER_RECONCILE_INTERVAL_MS")
                    || file.broker_reconcile_interval_ms.is_some(),
            ),
            (
                "broker_replay_buffer",
                set("ORBITCAST_BROKER_REPLAY_BUFFER") || file.broker_replay_buffer.is_some(),
            ),
            (
                "broker_session_ttl_secs",
                set("ORBITCAST_BROKER_SESSION_TTL_SECS") || file.broker_session_ttl_secs.is_some(),
            ),
            (
                "broker_max_messages_per_namespace",
                set("ORBITCAST_BROKER_MAX_MESSAGES_PER_NAMESPACE")
                    || file.broker_max_messages_per_namespace.is_some(),
            ),
            (
                "broker_max_bytes_per_namespace",
                set("ORBITCAST_BROKER_MAX_BYTES_PER_NAMESPACE")
                    || file.broker_max_bytes_per_namespace.is_some(),
            ),
            (
                "recovery_secret",
                set("ORBITCAST_RECOVERY_SECRET_BASE64") || file.recovery_secret.is_some(),
            ),
            (
                "streams_secret",
                set("ORBITCAST_STREAMS_SECRET_BASE64") || file.streams_secret.is_some(),
            ),
            (
                "streams_previous_secrets",
                set("ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64")
                    || file.streams_previous_secrets.is_some(),
            ),
            ("public_streams", set("ORBITCAST_PUBLIC_STREAMS") || file.public_streams.is_some()),
            (
                "public_streams_insecure",
                set("ORBITCAST_PUBLIC_STREAMS_INSECURE") || file.public_streams_insecure.is_some(),
            ),
            ("turbo_streams", set("ORBITCAST_TURBO_STREAMS") || file.turbo_streams.is_some()),
            (
                "turbo_streams_secret",
                set("ORBITCAST_TURBO_SECRET_BASE64") || file.turbo_streams_secret.is_some(),
            ),
            (
                "turbo_previous_secrets",
                set("ORBITCAST_TURBO_PREVIOUS_SECRETS_BASE64")
                    || file.turbo_previous_secrets.is_some(),
            ),
            (
                "cable_ready_streams",
                set("ORBITCAST_CABLE_READY_STREAMS") || file.cable_ready_streams.is_some(),
            ),
            (
                "cable_ready_secret",
                set("ORBITCAST_CABLE_READY_SECRET_BASE64") || file.cable_ready_secret.is_some(),
            ),
            (
                "cable_ready_previous_secrets",
                set("ORBITCAST_CABLE_READY_PREVIOUS_SECRETS_BASE64")
                    || file.cable_ready_previous_secrets.is_some(),
            ),
            (
                "rpc_endpoint_url",
                set("ORBITCAST_RPC_ENDPOINT_URL") || file.rpc_endpoint_url.is_some(),
            ),
            ("node_id", set("ORBITCAST_NODE_ID") || file.node_id.is_some()),
            ("rpc_deadline_ms", set("ORBITCAST_RPC_DEADLINE_MS") || file.rpc_deadline_ms.is_some()),
            (
                "rpc_max_concurrency",
                set("ORBITCAST_RPC_MAX_CONCURRENCY") || file.rpc_max_concurrency.is_some(),
            ),
            (
                "rpc_mailbox_capacity",
                set("ORBITCAST_RPC_MAILBOX_CAPACITY") || file.rpc_mailbox_capacity.is_some(),
            ),
            (
                "rpc_cleanup_capacity",
                set("ORBITCAST_RPC_CLEANUP_CAPACITY") || file.rpc_cleanup_capacity.is_some(),
            ),
        ];
        let mut explicit = ExplicitKeys::default();
        for (key, present) in rows {
            if present {
                explicit.mark(key);
            }
        }
        // Three more that the array above would push past a comfortable size.
        if env.non_empty("ORBITCAST_RPC_FAILURE_THRESHOLD").is_some()
            || file.rpc_failure_threshold.is_some()
        {
            explicit.mark("rpc_failure_threshold");
        }
        if env.non_empty("ORBITCAST_RPC_HALF_OPEN_TIMEOUT_MS").is_some()
            || file.rpc_half_open_timeout_ms.is_some()
        {
            explicit.mark("rpc_half_open_timeout_ms");
        }
        if env.non_empty("ORBITCAST_RPC_FORWARD_HEADERS").is_some()
            || file.rpc_forward_headers.is_some()
        {
            explicit.mark("rpc_forward_headers");
        }
        explicit
    }

    /// Signed-stream keys and switches.
    fn streams_from(env: Env<'_>, file: &FileConfig) -> Result<StreamsConfig, ConfigError> {
        // Secret precedence: the gem Launcher exports key material as strict
        // base64 (`ORBITCAST_*_SECRET_BASE64`), which decodes to the exact raw
        // key bytes. These win over a plain-text file value (raw UTF-8 bytes)
        // and over any plain-text value later supplied by the Moored config
        // (see `apply_moored_config`, which only fills a secret it finds
        // unset). Invalid base64 is a hard boundary error rather than a
        // silently-empty key.
        //
        // Previous (verify-only) keys follow the same rule: a comma-separated
        // list of strict base64 in the environment, a plain-text array in the
        // file. The ring is bounded at the boundary (MAX_PREVIOUS_KEYS).
        let mut streams = StreamsConfig {
            secret: env.secret_or_file(
                "ORBITCAST_STREAMS_SECRET_BASE64",
                "streams_secret",
                file.streams_secret.as_deref(),
            )?,
            previous_secrets: env.secret_list_or_file(
                "ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64",
                "streams_previous_secrets",
                file.streams_previous_secrets.as_deref(),
            )?,
            turbo_secret: env.secret_or_file(
                "ORBITCAST_TURBO_SECRET_BASE64",
                "turbo_streams_secret",
                file.turbo_streams_secret.as_deref(),
            )?,
            turbo_previous_secrets: env.secret_list_or_file(
                "ORBITCAST_TURBO_PREVIOUS_SECRETS_BASE64",
                "turbo_previous_secrets",
                file.turbo_previous_secrets.as_deref(),
            )?,
            cable_ready_secret: env.secret_or_file(
                "ORBITCAST_CABLE_READY_SECRET_BASE64",
                "cable_ready_secret",
                file.cable_ready_secret.as_deref(),
            )?,
            cable_ready_previous_secrets: env.secret_list_or_file(
                "ORBITCAST_CABLE_READY_PREVIOUS_SECRETS_BASE64",
                "cable_ready_previous_secrets",
                file.cable_ready_previous_secrets.as_deref(),
            )?,
            ..StreamsConfig::default()
        };

        // Standalone runs have no MOORED config to enable a signing profile,
        // so the profile switches are also readable from the environment.
        if let Some(public) = env.flag("ORBITCAST_PUBLIC_STREAMS")?.or(file.public_streams) {
            streams.public = public;
        }
        if let Some(insecure) =
            env.flag("ORBITCAST_PUBLIC_STREAMS_INSECURE")?.or(file.public_streams_insecure)
        {
            streams.public_insecure = insecure;
        }
        if let Some(turbo) = env.flag("ORBITCAST_TURBO_STREAMS")?.or(file.turbo_streams) {
            streams.turbo = turbo;
        }
        if let Some(cable_ready) =
            env.flag("ORBITCAST_CABLE_READY_STREAMS")?.or(file.cable_ready_streams)
        {
            streams.cable_ready = cable_ready;
        }
        Ok(streams)
    }

    /// Direct listener settings.
    fn http_from(env: Env<'_>, file: &FileConfig) -> Result<HttpConfig, ConfigError> {
        Ok(HttpConfig {
            listen_addr: env
                .non_empty("ORBITCAST_LISTEN_ADDR")
                .or_else(|| file.listen_addr.clone()),
            allowed_origins: env
                .non_empty("ORBITCAST_ALLOWED_ORIGINS")
                .map(|value| parse_origins(&value))
                .or_else(|| {
                    file.allowed_origins.as_ref().map(|origins| {
                        origins
                            .iter()
                            .map(|origin| normalize_origin(origin))
                            .filter(|origin| !origin.is_empty())
                            .collect()
                    })
                })
                .unwrap_or_default(),
            max_message_size: env
                .parse("ORBITCAST_MAX_MESSAGE_SIZE")?
                .or(file.max_message_size)
                .unwrap_or(DEFAULT_MAX_MESSAGE_SIZE),
            max_connections: env
                .parse("ORBITCAST_MAX_CONNECTIONS")?
                .or(file.max_connections)
                .unwrap_or(DEFAULT_MAX_CONNECTIONS),
            rate_connect_per_ip_per_min: env
                .parse("ORBITCAST_RATE_CONNECT_PER_IP_PER_MIN")?
                .or(file.rate_connect_per_ip_per_min)
                .unwrap_or(DEFAULT_RATE_CONNECT_PER_IP_PER_MIN),
            rate_commands_per_conn_per_sec: env
                .parse("ORBITCAST_RATE_COMMANDS_PER_CONN_PER_SEC")?
                .or(file.rate_commands_per_conn_per_sec)
                .unwrap_or(DEFAULT_RATE_COMMANDS_PER_CONN_PER_SEC),
            rate_subscribes_per_conn_per_sec: env
                .parse("ORBITCAST_RATE_SUBSCRIBES_PER_CONN_PER_SEC")?
                .or(file.rate_subscribes_per_conn_per_sec)
                .unwrap_or(DEFAULT_RATE_SUBSCRIBES_PER_CONN_PER_SEC),
            handshake_timeout: env
                .parse::<u64>("ORBITCAST_HANDSHAKE_TIMEOUT_MS")?
                .or(file.handshake_timeout_ms)
                .map_or(DEFAULT_HANDSHAKE_TIMEOUT, Duration::from_millis),
            outbound_queue_messages: env
                .parse("ORBITCAST_OUTBOUND_QUEUE_MESSAGES")?
                .or(file.outbound_queue_messages)
                .unwrap_or(DEFAULT_OUTBOUND_QUEUE_MESSAGES),
            outbound_queue_bytes: env
                .parse("ORBITCAST_OUTBOUND_QUEUE_BYTES")?
                .or(file.outbound_queue_bytes)
                .unwrap_or(DEFAULT_OUTBOUND_QUEUE_BYTES),
            max_header_bytes: env
                .parse("ORBITCAST_MAX_HEADER_BYTES")?
                .or(file.max_header_bytes)
                .unwrap_or(DEFAULT_MAX_HEADER_BYTES),
            trusted_proxies: match env.non_empty("ORBITCAST_TRUSTED_PROXIES") {
                Some(value) => parse_cidrs(split_list(&value))?,
                None => parse_cidrs(file.trusted_proxies.clone().unwrap_or_default())?,
            },
            // Administrative surface: a typo must never quietly publish it on
            // the browser-facing listener, which is what `Env::flag` guarantees.
            metrics_public: env
                .flag("ORBITCAST_METRICS_PUBLIC")?
                .or(file.metrics_public)
                .unwrap_or(false),
        })
    }

    /// Overflow relay settings.
    ///
    /// A typo in the switch is a hard error (`Env::flag` /
    /// `deny_unknown_fields`): silently publishing inline-only would turn large
    /// Turbo broadcasts into runtime failures instead of a boot-time complaint.
    fn overflow_from(env: Env<'_>, file: &FileConfig) -> Result<OverflowConfig, ConfigError> {
        let mut overflow = OverflowConfig::default();
        if let Some(enabled) = env.flag("ORBITCAST_OVERFLOW_ENABLED")?.or(file.overflow_enabled) {
            overflow.enabled = enabled;
        }
        if let Some(ttl_ms) =
            env.parse::<u64>("ORBITCAST_OVERFLOW_TTL_MS")?.or(file.overflow_ttl_ms)
        {
            overflow.ttl = Duration::from_millis(ttl_ms);
        }
        if let Some(max) =
            env.parse::<usize>("ORBITCAST_MAX_PUBLICATION_SIZE")?.or(file.max_publication_size)
        {
            overflow.max_publication_size = max;
        }
        Ok(overflow)
    }

    /// Retained broker settings.
    ///
    /// Off unless explicitly enabled; every bound is re-checked by
    /// `BrokerConfig::validate` before anything connects.
    fn broker_from(env: Env<'_>, file: &FileConfig) -> Result<BrokerConfig, ConfigError> {
        let mut broker = BrokerConfig::default();
        if let Some(enabled) = env.flag("ORBITCAST_BROKER_ENABLED")?.or(file.broker_enabled) {
            broker.enabled = enabled;
        }
        if let Some(streams) = env
            .non_empty("ORBITCAST_BROKER_STREAMS")
            .map(|value| parse_prefixes(&value))
            .or_else(|| file.broker_streams.clone())
        {
            broker.streams = streams;
        }
        if let Some(secs) =
            env.parse::<u64>("ORBITCAST_BROKER_RETENTION_SECS")?.or(file.broker_retention_secs)
        {
            broker.retention = Duration::from_secs(secs);
        }
        if let Some(max) = env
            .parse::<u64>("ORBITCAST_BROKER_MAX_MESSAGES_PER_STREAM")?
            .or(file.broker_max_messages_per_stream)
        {
            broker.max_messages_per_stream = max;
        }
        if let Some(max) = env
            .parse::<u64>("ORBITCAST_BROKER_MAX_BYTES_PER_STREAM")?
            .or(file.broker_max_bytes_per_stream)
        {
            broker.max_bytes_per_stream = max;
        }
        if let Some(millis) = env
            .parse::<u64>("ORBITCAST_BROKER_RECONCILE_INTERVAL_MS")?
            .or(file.broker_reconcile_interval_ms)
        {
            broker.reconcile_interval = Duration::from_millis(millis);
        }
        if let Some(buffer) =
            env.parse::<usize>("ORBITCAST_BROKER_REPLAY_BUFFER")?.or(file.broker_replay_buffer)
        {
            broker.replay_buffer = buffer;
        }
        if let Some(secs) =
            env.parse::<u64>("ORBITCAST_BROKER_SESSION_TTL_SECS")?.or(file.broker_session_ttl_secs)
        {
            broker.session_ttl = Duration::from_secs(secs);
        }
        if let Some(max) = env
            .parse::<u64>("ORBITCAST_BROKER_MAX_MESSAGES_PER_NAMESPACE")?
            .or(file.broker_max_messages_per_namespace)
        {
            broker.max_messages_per_namespace = max;
        }
        if let Some(max) = env
            .parse::<u64>("ORBITCAST_BROKER_MAX_BYTES_PER_NAMESPACE")?
            .or(file.broker_max_bytes_per_namespace)
        {
            broker.max_bytes_per_namespace = max;
        }
        Ok(broker)
    }

    /// Rails-dispatch settings.
    ///
    /// The endpoint is the mode switch; the credential is environment-only, so
    /// a checked-in TOML file can never carry it.
    fn rpc_from(env: Env<'_>, file: &FileConfig) -> Result<RpcSettings, ConfigError> {
        let defaults = RpcSettings::default();
        let mut rpc = RpcSettings {
            endpoint_url: env
                .non_empty("ORBITCAST_RPC_ENDPOINT_URL")
                .or_else(|| file.rpc_endpoint_url.clone()),
            token: env.secret("ORBITCAST_RPC_TOKEN_BASE64")?,
            deadline: env
                .parse::<u64>("ORBITCAST_RPC_DEADLINE_MS")?
                .or(file.rpc_deadline_ms)
                .map_or(defaults.deadline, Duration::from_millis),
            max_concurrency: env
                .parse("ORBITCAST_RPC_MAX_CONCURRENCY")?
                .or(file.rpc_max_concurrency)
                .unwrap_or(defaults.max_concurrency),
            mailbox_capacity: env
                .parse("ORBITCAST_RPC_MAILBOX_CAPACITY")?
                .or(file.rpc_mailbox_capacity)
                .unwrap_or(defaults.mailbox_capacity),
            cleanup_capacity: env
                .parse("ORBITCAST_RPC_CLEANUP_CAPACITY")?
                .or(file.rpc_cleanup_capacity)
                .unwrap_or(defaults.cleanup_capacity),
            failure_threshold: env
                .parse("ORBITCAST_RPC_FAILURE_THRESHOLD")?
                .or(file.rpc_failure_threshold)
                .unwrap_or(defaults.failure_threshold),
            half_open_timeout: env
                .parse::<u64>("ORBITCAST_RPC_HALF_OPEN_TIMEOUT_MS")?
                .or(file.rpc_half_open_timeout_ms)
                .map_or(defaults.half_open_timeout, Duration::from_millis),
            forward_headers: defaults.forward_headers,
            node_id: env
                .non_empty("ORBITCAST_NODE_ID")
                .or_else(|| file.node_id.clone())
                .unwrap_or(defaults.node_id),
        };

        if let Some(headers) = env
            .non_empty("ORBITCAST_RPC_FORWARD_HEADERS")
            .map(|value| parse_header_names(&value))
            .or_else(|| file.rpc_forward_headers.as_deref().map(parse_header_list))
        {
            rpc.forward_headers = headers;
        }

        Ok(rpc)
    }

    /// Private HTTP ingress settings.
    ///
    /// The listener address is boot-time configuration (environment or file,
    /// like `listen_addr`); the credential is environment-only, so a
    /// checked-in TOML file can never carry it.
    fn ingress_from(env: Env<'_>, file: &FileConfig) -> Result<IngressSettings, ConfigError> {
        Ok(IngressSettings {
            listen_addr: env
                .non_empty("ORBITCAST_INGRESS_LISTEN_ADDR")
                .or_else(|| file.ingress_listen_addr.clone()),
            token: env.secret("ORBITCAST_INGRESS_TOKEN_BASE64")?,
            allow_nonlocal: env
                .flag("ORBITCAST_INGRESS_ALLOW_NONLOCAL")?
                .or(file.ingress_allow_nonlocal)
                .unwrap_or(false),
        })
    }

    /// Read the docking parameters, if the supervisor exported any.
    fn docking_from(env: Env<'_>) -> Result<Option<DockingConfig>, ConfigError> {
        let pid = env.var("MS_PID");
        let ship = env.var("MS_SHIP");
        let socket_dir = env.var("MS_SOCKET_DIR");

        if pid.is_none() && ship.is_none() && socket_dir.is_none() {
            return Ok(None);
        }

        let mothership_pid: u32 = pid
            .ok_or(ConfigError::MissingEnv("MS_PID"))?
            .parse()
            .map_err(|_| ConfigError::InvalidEnv("MS_PID", "expected u32"))?;
        let ship_name = ship.ok_or(ConfigError::MissingEnv("MS_SHIP"))?;
        let socket_dir = socket_dir.map_or_else(|| default_socket_dir(env), PathBuf::from);

        Ok(Some(DockingConfig { mothership_pid, ship_name, socket_dir, routes: Vec::new() }))
    }

    /// Validate the configuration at the process boundary.
    ///
    /// Fails when no transport is selected at all, and on any out-of-range
    /// listener limit. Called once, before anything is bound.
    ///
    /// # Errors
    ///
    /// [`ConfigError::NoTransport`] when neither docking nor the direct
    /// listener is configured. Otherwise [`ConfigError::InvalidValue`] (or
    /// [`ConfigError::Invalid`] for the broker) naming the first offending
    /// key: a listener limit ([`HttpConfig::validate`]), a zero shutdown
    /// deadline or ping interval, a signing profile enabled without its own
    /// secret, an unusable overflow or presence bound, a broker without a
    /// `database_url` or with bad bounds, a recovery key without the broker,
    /// Rails-dispatch or ingress settings that fail their own checks, or an
    /// ingress without a namespace or sharing the public listener's bind.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.docking.is_none() && !self.http.is_enabled() {
            return Err(ConfigError::NoTransport);
        }

        self.http.validate()?;

        self.validate_backend_and_bounds()?;

        // Retained history and recovery. Both are off by default, and each one
        // fails at the boundary rather than half-working at runtime.
        self.broker.validate().map_err(|e| ConfigError::Invalid("broker", e.to_string()))?;

        if self.broker.enabled && self.database_url.is_none() {
            return Err(ConfigError::InvalidValue(
                "broker_enabled",
                "requires a database_url: retained history lives in PostgreSQL",
            ));
        }

        // A recovery key without a broker would advertise a channel that can
        // replay nothing, which is worse than no channel at all.
        if self.recovery_secret.is_some() && !self.broker.enabled {
            return Err(ConfigError::InvalidValue(
                "recovery_secret",
                "requires broker_enabled: recovery replays retained history",
            ));
        }

        self.rpc.validate()?;

        self.ingress.validate()?;

        // Every accepted publication and control message is validated against
        // this node's namespace; without one, the ingress could accept nothing
        // and would only ever answer 403. Fail at boot instead.
        if self.ingress.is_enabled() && self.namespace.is_empty() {
            return Err(ConfigError::InvalidValue(
                "ingress_listen_addr",
                "the ingress requires a namespace (ORBITCAST_NAMESPACE or `namespace`): \
                 publications are validated against it",
            ));
        }

        // The ingress is a private surface: it must never share a bind with
        // the public `/cable` listener. (Distinct ports on the same IP are
        // fine; the same pair would fail the second bind anyway - refuse it
        // here with a real explanation. Port 0 is exempt: the kernel assigns
        // two distinct ephemeral ports, which tests rely on.)
        if let (Some(public), Some(private)) =
            (self.http.listen_addr.as_deref(), self.ingress.listen_addr.as_deref())
            && public == private
            && private.parse::<SocketAddr>().is_ok_and(|addr| addr.port() != 0)
        {
            return Err(ConfigError::InvalidValue(
                "ingress_listen_addr",
                "must differ from listen_addr: the ingress is never served on the public \
                 /cable listener",
            ));
        }

        Ok(())
    }

    /// The backend requirement plus the overflow and presence bounds.
    fn validate_backend_and_bounds(&self) -> Result<(), ConfigError> {
        // The PostgreSQL backend is what this build broadcasts through: a
        // node without a database or a namespace could only run with
        // broadcasting disabled, which is never allowed.
        #[cfg(all(feature = "postgres", not(feature = "memory")))]
        {
            if self.database_url.as_deref().is_none_or(|url| url.trim().is_empty()) {
                return Err(ConfigError::InvalidValue(
                    "database_url",
                    "the PostgreSQL backend requires ORBITCAST_DATABASE_URL (or `database_url`); \
                     the relay never runs with broadcasting disabled",
                ));
            }
            if self.namespace.is_empty() {
                return Err(ConfigError::InvalidValue(
                    "namespace",
                    "the PostgreSQL backend requires ORBITCAST_NAMESPACE (or `namespace`): two \
                     deployments sharing a database must not share channels",
                ));
            }
        }

        if self.listener_database_url.as_deref().is_some_and(|url| url.trim().is_empty()) {
            return Err(ConfigError::InvalidValue(
                "listener_database_url",
                "must not be empty when set; omit it to reuse database_url",
            ));
        }

        if self.shutdown_timeout.is_zero() {
            return Err(ConfigError::InvalidValue(
                "shutdown_timeout_ms",
                "expected a non-zero duration",
            ));
        }

        if self.ping_interval == 0 {
            return Err(ConfigError::InvalidValue("ping_interval", "expected at least 1 second"));
        }

        // Each signing profile requires its own explicitly configured secret:
        // there is no implicit fallback between unrelated profiles, so enabling
        // one without its key is a boundary error, not a
        // silent reuse of another profile's key.
        if self.streams.turbo && self.streams.turbo_secret.is_none() {
            return Err(ConfigError::InvalidValue(
                "turbo_streams",
                "requires its own secret (ORBITCAST_TURBO_SECRET_BASE64 or \
                 turbo_streams_secret); there is no fallback to streams_secret",
            ));
        }

        if self.streams.cable_ready && self.streams.cable_ready_secret.is_none() {
            return Err(ConfigError::InvalidValue(
                "cable_ready_streams",
                "requires its own secret (ORBITCAST_CABLE_READY_SECRET_BASE64 or \
                 cable_ready_secret); there is no fallback to streams_secret",
            ));
        }

        // Unsigned public streams let any client that can reach /cable read
        // any stream name it can guess. That is a development convenience,
        // never a production mode, so the switch alone is refused:
        // the operator must spell out that they accept the exposure.
        if self.streams.public && !self.streams.public_insecure {
            return Err(ConfigError::InvalidValue(
                "public_streams",
                "unsigned public streams let any client subscribe to any stream name without a \
                 signed capability; this is development-only. Set public_streams_insecure = true \
                 (ORBITCAST_PUBLIC_STREAMS_INSECURE=true) to acknowledge that, or use a signing \
                 profile instead",
            ));
        }

        // Overflow bounds are checked here as well as in the backend, so an
        // unusable relay setting fails before anything binds or connects.
        if self.overflow.ttl.is_zero() {
            return Err(ConfigError::InvalidValue(
                "overflow_ttl_ms",
                "expected a non-zero duration",
            ));
        }

        if !(crate::protocol::v1::publication::MAX_NOTIFY_PAYLOAD_LEN
            ..=crate::pubsub::MAX_PUBLICATION_SIZE_LIMIT)
            .contains(&self.overflow.max_publication_size)
        {
            return Err(ConfigError::InvalidValue(
                "max_publication_size",
                "expected 8000..=67108864 bytes",
            ));
        }

        // Presence bounds are checked whatever the mode: a node that later
        // gains a Rails endpoint must not discover its lease settings are
        // unusable at the first grant.
        let ttl_ms = self.presence_ttl.as_millis();
        if ttl_ms == 0 || ttl_ms > u128::from(MAX_PRESENCE_TTL_MS) {
            return Err(ConfigError::InvalidValue(
                "presence_ttl_ms",
                "expected 1..=600000 milliseconds",
            ));
        }

        // A refresh period at or above the TTL cannot keep a lease alive: every
        // roster would flicker once per TTL.
        if self.presence_refresh_interval.is_zero()
            || self.presence_refresh_interval >= self.presence_ttl
        {
            return Err(ConfigError::InvalidValue(
                "presence_refresh_interval_ms",
                "expected a non-zero duration shorter than presence_ttl_ms",
            ));
        }

        Ok(())
    }

    /// Apply configuration from the Moored handshake response.
    ///
    /// Precedence is environment > file > handshake: a key the environment or
    /// the file already set is left alone (logged at debug), whatever the
    /// handshake says. Everything else is parsed **strictly**: a key this
    /// build does not know, a key the handshake may not carry (`listen_addr`
    /// and the whole ingress block - transport selection happens before
    /// anything binds, and a supervisor can neither open the private ingress
    /// nor hand it a token; the environment-only credentials), an empty
    /// value, or a value that does not parse is an error, never silently
    /// ignored. `validate()` re-checks the result before anything uses it.
    ///
    /// # Errors
    ///
    /// [`ConfigError::UnknownHandshakeKey`] for an unknown or refused key, and
    /// [`ConfigError::InvalidHandshakeValue`] for a value that does not parse
    /// or is out of range for its key.
    pub fn apply_moored_config(
        &mut self,
        config: &HashMap<String, String>,
    ) -> Result<(), ConfigError> {
        // Sorted, so the first error is deterministic.
        let mut keys: Vec<&String> = config.keys().collect();
        keys.sort();

        for key in keys {
            let value = Moored { key, value: &config[key] };
            if !MOORED_KEYS.contains(&key.as_str()) {
                return Err(ConfigError::UnknownHandshakeKey(key.clone()));
            }
            if self.explicit.contains(key) {
                tracing::debug!(key, "handshake value ignored: set by the environment or file");
                continue;
            }
            self.apply_moored_key(value)?;
        }

        Ok(())
    }

    /// Apply one handshake key that is known and not explicitly set.
    fn apply_moored_key(&mut self, moored: Moored<'_>) -> Result<(), ConfigError> {
        match moored.key.as_str() {
            // Database, namespace, pub/sub bounds, the overflow relay, ping.
            "database_url" => self.database_url = Some(moored.non_empty()?.to_string()),
            "listener_database_url" => {
                self.listener_database_url = Some(moored.non_empty()?.to_string());
            }
            "namespace" => self.namespace = moored.non_empty()?.to_string(),
            "pubsub_ack_timeout_ms" => self.pubsub_ack_timeout = moored.millis()?,
            "pubsub_queue_capacity" => self.pubsub_queue_capacity = moored.positive()?,
            "overflow_enabled" => self.overflow.enabled = moored.flag()?,
            "overflow_ttl_ms" => self.overflow.ttl = moored.millis()?,
            "max_publication_size" => self.overflow.max_publication_size = moored.positive()?,
            "ping_interval" => self.ping_interval = moored.positive()?,

            // Presence tunables.
            "presence_ttl_ms" => self.presence_ttl = moored.millis()?,
            "presence_refresh_interval_ms" => self.presence_refresh_interval = moored.millis()?,

            // Listener limits and the drain deadline.
            "allowed_origins" => self.http.allowed_origins = parse_origins(moored.value),
            "max_message_size" => self.http.max_message_size = moored.positive()?,
            "max_connections" => self.http.max_connections = moored.positive()?,
            "shutdown_timeout_ms" => self.shutdown_timeout = moored.millis()?,
            "rate_connect_per_ip_per_min" => {
                self.http.rate_connect_per_ip_per_min = moored.number()?;
            }
            "rate_commands_per_conn_per_sec" => {
                self.http.rate_commands_per_conn_per_sec = moored.number()?;
            }
            "rate_subscribes_per_conn_per_sec" => {
                self.http.rate_subscribes_per_conn_per_sec = moored.number()?;
            }
            "handshake_timeout_ms" => self.http.handshake_timeout = moored.millis()?,
            "outbound_queue_messages" => self.http.outbound_queue_messages = moored.positive()?,
            "outbound_queue_bytes" => self.http.outbound_queue_bytes = moored.positive()?,
            "max_header_bytes" => self.http.max_header_bytes = moored.positive()?,
            "trusted_proxies" => {
                self.http.trusted_proxies =
                    parse_cidrs(split_list(moored.value)).map_err(|_| {
                        moored.invalid("expected comma-separated IP addresses or CIDR networks")
                    })?;
            }

            // Retained broker bounds and the recovery key.
            "broker_enabled" => self.broker.enabled = moored.flag()?,
            "broker_streams" => self.broker.streams = parse_prefixes(moored.value),
            "broker_retention_secs" => self.broker.retention = moored.secs()?,
            "broker_max_messages_per_stream" => {
                self.broker.max_messages_per_stream = moored.positive()?;
            }
            "broker_max_bytes_per_stream" => {
                self.broker.max_bytes_per_stream = moored.positive()?;
            }
            "broker_reconcile_interval_ms" => self.broker.reconcile_interval = moored.millis()?,
            "broker_replay_buffer" => self.broker.replay_buffer = moored.positive()?,
            "broker_session_ttl_secs" => self.broker.session_ttl = moored.secs()?,
            "broker_max_messages_per_namespace" => {
                self.broker.max_messages_per_namespace = moored.positive()?;
            }
            "broker_max_bytes_per_namespace" => {
                self.broker.max_bytes_per_namespace = moored.positive()?;
            }
            "recovery_secret" => self.recovery_secret = Some(moored_secret(moored.non_empty()?)),

            // Signed streams. Plain-text values pass through as their raw
            // UTF-8 bytes, for direct deployments that never go through the
            // gem Launcher; an env-decoded base64 key was marked explicit and
            // never reaches here.
            "streams_secret" => self.streams.secret = Some(moored_secret(moored.non_empty()?)),
            "streams_previous_secrets" => self.streams.previous_secrets = moored.secret_list()?,
            "public_streams" => self.streams.public = moored.flag()?,
            "public_streams_insecure" => self.streams.public_insecure = moored.flag()?,
            "turbo_streams" => self.streams.turbo = moored.flag()?,
            "turbo_streams_secret" => {
                self.streams.turbo_secret = Some(moored_secret(moored.non_empty()?));
            }
            "turbo_previous_secrets" => {
                self.streams.turbo_previous_secrets = moored.secret_list()?;
            }
            "cable_ready_streams" => self.streams.cable_ready = moored.flag()?,
            "cable_ready_secret" => {
                self.streams.cable_ready_secret = Some(moored_secret(moored.non_empty()?));
            }
            "cable_ready_previous_secrets" => {
                self.streams.cable_ready_previous_secrets = moored.secret_list()?;
            }

            // Rails dispatch. The credential is not among these: it stays
            // environment-only, so an endpoint arriving without one fails
            // `validate()` rather than calling Rails unauthenticated.
            "rpc_endpoint_url" => self.rpc.endpoint_url = Some(moored.non_empty()?.to_string()),
            "node_id" => self.rpc.node_id = moored.non_empty()?.to_string(),
            "rpc_deadline_ms" => self.rpc.deadline = moored.millis()?,
            "rpc_max_concurrency" => self.rpc.max_concurrency = moored.positive()?,
            "rpc_mailbox_capacity" => self.rpc.mailbox_capacity = moored.positive()?,
            "rpc_cleanup_capacity" => self.rpc.cleanup_capacity = moored.positive()?,
            "rpc_failure_threshold" => self.rpc.failure_threshold = moored.positive()?,
            "rpc_half_open_timeout_ms" => self.rpc.half_open_timeout = moored.millis()?,
            "rpc_forward_headers" => {
                let headers = parse_header_names(moored.value);
                if headers.is_empty() {
                    return Err(moored.invalid("expected at least one header name"));
                }
                self.rpc.forward_headers = headers;
            }
            _ => return Err(ConfigError::UnknownHandshakeKey(moored.key.clone())),
        }
        Ok(())
    }
}

/// Every key the docking handshake may carry.
///
/// Deliberately absent: `listen_addr`, `ingress_listen_addr`,
/// `ingress_allow_nonlocal`, and every credential the environment owns.
pub const MOORED_KEYS: &[&str] = &[
    "database_url",
    "listener_database_url",
    "namespace",
    "pubsub_ack_timeout_ms",
    "pubsub_queue_capacity",
    "overflow_enabled",
    "overflow_ttl_ms",
    "max_publication_size",
    "ping_interval",
    "presence_ttl_ms",
    "presence_refresh_interval_ms",
    "allowed_origins",
    "max_message_size",
    "max_connections",
    "shutdown_timeout_ms",
    "rate_connect_per_ip_per_min",
    "rate_commands_per_conn_per_sec",
    "rate_subscribes_per_conn_per_sec",
    "handshake_timeout_ms",
    "outbound_queue_messages",
    "outbound_queue_bytes",
    "max_header_bytes",
    "trusted_proxies",
    "broker_enabled",
    "broker_streams",
    "broker_retention_secs",
    "broker_max_messages_per_stream",
    "broker_max_bytes_per_stream",
    "broker_reconcile_interval_ms",
    "broker_replay_buffer",
    "broker_session_ttl_secs",
    "broker_max_messages_per_namespace",
    "broker_max_bytes_per_namespace",
    "recovery_secret",
    "streams_secret",
    "streams_previous_secrets",
    "public_streams",
    "public_streams_insecure",
    "turbo_streams",
    "turbo_streams_secret",
    "turbo_previous_secrets",
    "cable_ready_streams",
    "cable_ready_secret",
    "cable_ready_previous_secrets",
    "rpc_endpoint_url",
    "node_id",
    "rpc_deadline_ms",
    "rpc_max_concurrency",
    "rpc_mailbox_capacity",
    "rpc_cleanup_capacity",
    "rpc_failure_threshold",
    "rpc_half_open_timeout_ms",
    "rpc_forward_headers",
];

/// One handshake key with its raw value, read strictly.
///
/// The handshake is as strict as the environment: an unparsable value, a
/// zero where a positive bound is required, or a boolean outside the known
/// spellings is an error naming the key, never a value kept by accident.
#[derive(Clone, Copy)]
struct Moored<'a> {
    key: &'a String,
    value: &'a str,
}

impl<'a> Moored<'a> {
    fn invalid(self, message: &'static str) -> ConfigError {
        ConfigError::InvalidHandshakeValue { key: self.key.clone(), message }
    }

    /// A present, non-empty value.
    fn non_empty(self) -> Result<&'a str, ConfigError> {
        if self.value.trim().is_empty() {
            return Err(self.invalid("must not be empty"));
        }
        Ok(self.value)
    }

    /// A boolean in the same spellings the environment accepts.
    fn flag(self) -> Result<bool, ConfigError> {
        match self.value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(self.invalid("expected a boolean (true/false)")),
        }
    }

    /// A non-negative number (zero allowed: the documented "off" spelling of
    /// a rate limit).
    fn number<T: std::str::FromStr>(self) -> Result<T, ConfigError> {
        self.value.trim().parse().map_err(|_| self.invalid("expected a non-negative integer"))
    }

    /// A strictly positive number.
    fn positive<T: std::str::FromStr + Default + PartialOrd>(self) -> Result<T, ConfigError> {
        let value: T = self.number()?;
        if value <= T::default() {
            return Err(self.invalid("expected a positive integer"));
        }
        Ok(value)
    }

    /// A strictly positive millisecond count, as a duration.
    /// A comma-separated list of plain-text verify-only keys, each non-empty,
    /// bounded by the ring size.
    fn secret_list(self) -> Result<PreviousKeys, ConfigError> {
        let keys = self
            .value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(moored_secret)
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return Err(self.invalid("expected at least one key"));
        }
        PreviousKeys::new(keys).map_err(|_| self.invalid("too many previous keys"))
    }

    fn millis(self) -> Result<Duration, ConfigError> {
        self.positive().map(Duration::from_millis)
    }

    /// A strictly positive second count, as a duration.
    fn secs(self) -> Result<Duration, ConfigError> {
        self.positive().map(Duration::from_secs)
    }
}

/// Split a comma-separated header-name list into lowercase entries.
fn parse_header_names(value: &str) -> Vec<String> {
    parse_header_list(
        &value
            .split(',')
            .map(|entry| entry.trim_matches(['[', ']', '"', '\''].as_slice()).to_string())
            .collect::<Vec<_>>(),
    )
}

/// Normalize a header-name list: trimmed, lowercased, empties dropped.
fn parse_header_list(names: &[String]) -> Vec<String> {
    names
        .iter()
        .map(|name| name.trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .collect()
}

/// Split a comma-separated stream-prefix allowlist.
///
/// Empty entries are dropped rather than turned into a prefix that matches
/// every stream: widening an allowlist must be an explicit, empty list.
fn parse_prefixes(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|entry| entry.trim().trim_matches(['[', ']', '"', '\''].as_slice()).to_string())
        .filter(|entry| !entry.is_empty())
        .collect()
}

/// Split a comma-separated origin list into normalized entries.
///
/// Also accepts a TOML array that was flattened into a string by the exporter;
/// empty entries are dropped rather than becoming a match-anything hole.
fn parse_origins(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|entry| normalize_origin(entry.trim_matches(['[', ']', '"', '\''].as_slice())))
        .filter(|entry| !entry.is_empty())
        .collect()
}

/// Where configuration reads environment variables from.
///
/// [`Env::process`] is the real process environment. Tests pass their own
/// lookup instead of mutating the process environment, which would race every
/// other test thread reading it - the reason edition 2024 made `set_var`
/// `unsafe`.
#[derive(Clone, Copy)]
struct Env<'a> {
    lookup: &'a dyn Fn(&str) -> Option<String>,
}

impl Env<'static> {
    /// The process environment.
    fn process() -> Self {
        Self { lookup: &process_var }
    }
}

/// [`Env::process`]'s lookup. A value that is not valid UTF-8 counts as unset.
fn process_var(var: &str) -> Option<String> {
    std::env::var(var).ok()
}

impl Env<'_> {
    /// The raw value of `var`, if set.
    fn var(self, var: &str) -> Option<String> {
        (self.lookup)(var)
    }

    /// A non-empty value, treating empty as unset.
    fn non_empty(self, var: &'static str) -> Option<String> {
        self.var(var).filter(|value| !value.trim().is_empty())
    }

    /// Parse an unsigned-integer value; a malformed value is a hard error, so a
    /// typo in a limit never silently falls back to the default.
    ///
    /// Every tunable read through this is a count or a millisecond duration,
    /// hence the single "non-negative integer" expectation in the error.
    fn parse<T: std::str::FromStr>(self, var: &'static str) -> Result<Option<T>, ConfigError> {
        self.non_empty(var)
            .map(|value| {
                value
                    .trim()
                    .parse()
                    .map_err(|_| ConfigError::InvalidEnv(var, "expected a non-negative integer"))
            })
            .transpose()
    }

    /// Parse a boolean value. Anything but a known spelling is an error: a
    /// typo must never quietly disable a security-relevant switch.
    fn flag(self, var: &'static str) -> Result<Option<bool>, ConfigError> {
        self.non_empty(var)
            .map(|value| match value.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => Ok(true),
                "0" | "false" | "no" | "off" => Ok(false),
                _ => Err(ConfigError::InvalidEnv(var, "expected a boolean (true/false)")),
            })
            .transpose()
    }

    /// Decode a strict-base64 secret into raw key bytes.
    ///
    /// Absent or empty yields `None`; malformed base64 is a hard boundary
    /// error so a mistyped secret never silently degrades to an unconfigured
    /// key.
    fn secret(self, var: &'static str) -> Result<Option<SecretBytes>, ConfigError> {
        let Some(value) = self.var(var).filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        let bytes = STANDARD.decode(value.as_bytes()).map_err(|_| {
            ConfigError::InvalidEnv(var, "expected strict base64-encoded key bytes")
        })?;
        (!bytes.is_empty()).ok_or(ConfigError::InvalidEnv(var, "decoded key must not be empty"))?;
        Ok(Some(SecretBytes::new(bytes)))
    }

    /// Decode a comma-separated list of strict-base64 verify-only keys.
    ///
    /// Absent or empty yields no keys; any malformed or empty entry, or more
    /// than the ring bound, is a hard error.
    fn secret_list(self, var: &'static str) -> Result<Option<PreviousKeys>, ConfigError> {
        let Some(value) = self.non_empty(var) else {
            return Ok(None);
        };
        let keys = value
            .split(',')
            .map(|entry| {
                let entry = entry.trim();
                let bytes = STANDARD.decode(entry.as_bytes()).map_err(|_| {
                    ConfigError::InvalidEnv(var, "expected comma-separated strict base64 keys")
                })?;
                (!bytes.is_empty())
                    .ok_or(ConfigError::InvalidEnv(var, "decoded key must not be empty"))?;
                Ok(SecretBytes::new(bytes))
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        PreviousKeys::new(keys)
            .map(Some)
            .map_err(|_| ConfigError::InvalidEnv(var, "too many previous keys (max 8)"))
    }

    /// Previous keys from strict base64 in `var`, else the plain-text `file`
    /// list under `field`. The environment form wins.
    fn secret_list_or_file(
        self,
        var: &'static str,
        field: &'static str,
        file: Option<&[String]>,
    ) -> Result<PreviousKeys, ConfigError> {
        self.secret_list(var)?.map_or_else(|| FileConfig::secret_list(field, file), Ok)
    }

    /// A key from strict base64 in `var`, else the plain-text `file` value
    /// (raw UTF-8 bytes) under `field`. The environment form wins; there is no
    /// fallback to any other profile's key.
    fn secret_or_file(
        self,
        var: &'static str,
        field: &'static str,
        file: Option<&str>,
    ) -> Result<Option<SecretBytes>, ConfigError> {
        self.secret(var)?.map_or_else(|| FileConfig::secret(field, file), |secret| Ok(Some(secret)))
    }
}

/// A plain-text Moored/TOML secret as raw key bytes.
///
/// The value is passed through as its raw UTF-8 bytes, which is what a direct
/// TOML deployment signs with; base64 key material arrives instead through
/// [`Env::secret`].
fn moored_secret(value: &str) -> SecretBytes {
    SecretBytes::new(value.as_bytes().to_vec())
}

/// The supervisor's runtime socket directory, or the temp dir without one.
fn default_socket_dir(env: Env<'_>) -> PathBuf {
    env.var("XDG_RUNTIME_DIR").map_or_else(std::env::temp_dir, PathBuf::from).join("mothership")
}

/// Configuration error
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing environment variable: {0}")]
    MissingEnv(&'static str),
    #[error("invalid environment variable {0}: {1}")]
    InvalidEnv(&'static str, &'static str),
    #[error("invalid configuration value {0}: {1}")]
    InvalidValue(&'static str, &'static str),
    /// Same as [`ConfigError::InvalidValue`], for a message a subsystem
    /// produced at runtime (bounds it already knows how to describe).
    #[error("invalid configuration value {0}: {1}")]
    Invalid(&'static str, String),
    #[error("config file {path}: {message}")]
    ConfigFile { path: String, message: String },
    /// The docking handshake carried a key this build does not know, or one
    /// it refuses to take from a handshake (binds, credentials).
    #[error(
        "docking handshake key {0:?} is unknown or not accepted from the handshake (binds and \
         credentials are environment or file settings)"
    )]
    UnknownHandshakeKey(String),
    /// The docking handshake carried a value that does not parse for its key.
    #[error("docking handshake value for {key:?} is invalid: {message}")]
    InvalidHandshakeValue { key: String, message: &'static str },
    #[error(
        "no transport configured: set ORBITCAST_LISTEN_ADDR for the direct \
         HTTP/WebSocket listener, or run under Mothership (MS_PID, MS_SHIP)"
    )]
    NoTransport,
}

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
