//! Pooled HTTP/JSON caller for the private Rails endpoint.
//!
//! One [`RpcClient`] per process. Every call is
//! `POST {endpoint}/{operation}` with a bearer credential and a v1
//! [`RpcRequest`] body, and returns a **validated** [`RpcReply`].
//!
//! ```text
//! call(request)
//!   -> circuit breaker            (open   -> RpcError::CircuitOpen, no call)
//!   -> end-to-end deadline        (expiry -> RpcError::Deadline)
//!        -> concurrency permit    (queue wait is inside the deadline)
//!        -> pooled HTTP request   (connection reuse via rama's client pool)
//!        -> status check          (non-2xx -> RpcError::Http)
//!        -> bounded body read
//!        -> decode + validate + correlate (-> RpcError::InvalidReply)
//! ```
//!
//! # What is *not* an error
//!
//! A `denied` outcome is a decision, not a failure: it comes back as
//! `Ok(reply)` with [`Outcome::Denied`], and the caller turns it into a
//! standard rejection frame. Only transport, protocol, and availability
//! problems are [`RpcError`]s, and only those trip the circuit breaker - a
//! Rails application raising on one channel must not stop authentication for
//! every other session.
//!
//! # Retries
//!
//! There are none, deliberately. Authentication, channel, and action callbacks
//! have side effects, so an ambiguous timeout is reported rather than
//! replayed. Request IDs are for correlation and
//! deduplication, not for pretending arbitrary Ruby is idempotent.

use std::sync::Arc;
use std::time::Duration;

use breaker_machines::{AsyncCircuitBreaker, CircuitError};
use rama::Service as _;
use rama::http::body::CollectOptions;
use rama::http::client::DefaultHttpWebClient;
use rama::http::{Body, BodyExtractExt as _, HeaderValue, Request, header};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::protocol::Envelope as _;
use crate::protocol::ProtocolError;
use crate::protocol::v1::rpc::{DescribeInfo, Operation, Outcome, RpcReply, RpcRequest};
use crate::streams::SecretBytes;

/// Capabilities the Rails endpoint must advertise before this node admits
/// dispatch traffic. Each one names an operation the session reducer issues.
pub const REQUIRED_CAPABILITIES: &[&str] = &["identify", "open_channel", "run_action", "release"];

/// How often the endpoint is re-probed with `describe`.
pub const DESCRIBE_INTERVAL: Duration = Duration::from_secs(30);

/// Default end-to-end RPC deadline (one second).
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(1);

/// Default ceiling on in-flight RPC calls, tied to Rails capacity.
pub const DEFAULT_MAX_CONCURRENCY: usize = 64;

/// Default consecutive-failure count that opens the circuit.
pub const DEFAULT_FAILURE_THRESHOLD: usize = 5;

/// Default sliding window the failure count is measured over.
pub const DEFAULT_FAILURE_WINDOW: Duration = Duration::from_secs(10);

/// Default cooldown before a half-open probe is allowed.
pub const DEFAULT_HALF_OPEN_TIMEOUT: Duration = Duration::from_secs(5);

/// Default number of probe successes that close the circuit again.
pub const DEFAULT_SUCCESS_THRESHOLD: usize = 2;

/// Default ceiling on one reply body, in bytes.
pub const DEFAULT_MAX_REPLY_BYTES: usize = 2 * 1024 * 1024;

/// Circuit name used in breaker diagnostics.
const CIRCUIT_NAME: &str = "orbitcast.rails_rpc";

/// Everything [`RpcClient`] needs to reach the Rails endpoint.
///
/// The bearer credential is [`SecretBytes`]: it never lands in a `Debug`
/// rendering, and the assembled `Authorization` header is marked sensitive so
/// rama's tracing layers do not log it either.
#[derive(Debug, Clone)]
pub struct RpcConfig {
    /// Base URL of the Rails endpoint, e.g.
    /// `https://app.internal/_orbitcast/v1/rails`. The operation name is
    /// appended as the final path segment.
    pub endpoint_url: String,
    /// Service credential sent as `Authorization: Bearer <token>`.
    pub bearer_token: SecretBytes,
    /// End-to-end deadline covering queue wait, connection acquisition, and
    /// the response.
    pub deadline: Duration,
    /// Ceiling on simultaneously in-flight calls.
    pub max_concurrency: usize,
    /// Failures within [`Self::failure_window`] that open the circuit.
    pub failure_threshold: usize,
    /// Sliding window the failure count is measured over.
    pub failure_window: Duration,
    /// Cooldown before the circuit admits a half-open probe.
    pub half_open_timeout: Duration,
    /// Probe successes required to close the circuit again.
    pub success_threshold: usize,
    /// Ceiling on one reply body, in bytes.
    pub max_reply_bytes: usize,
}

impl RpcConfig {
    /// Build a configuration with the documented defaults.
    pub fn new(endpoint_url: impl Into<String>, bearer_token: SecretBytes) -> Self {
        Self {
            endpoint_url: endpoint_url.into(),
            bearer_token,
            deadline: DEFAULT_DEADLINE,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            failure_window: DEFAULT_FAILURE_WINDOW,
            half_open_timeout: DEFAULT_HALF_OPEN_TIMEOUT,
            success_threshold: DEFAULT_SUCCESS_THRESHOLD,
            max_reply_bytes: DEFAULT_MAX_REPLY_BYTES,
        }
    }
}

/// The process-wide caller for the Rails endpoint.
///
/// Cheap to share behind an `Arc`: the HTTP connection pool, the concurrency
/// permits, and the circuit breaker are all shared state, and *must* be, or the
/// bounds would be per-caller instead of per-process.
pub struct RpcClient {
    /// Base URL with any trailing slash removed.
    endpoint: String,
    /// Pre-built, sensitive `Authorization` header value.
    authorization: HeaderValue,
    deadline: Duration,
    max_reply_bytes: usize,
    /// rama's default web client, which includes its multiplexing connection
    /// pool: consecutive calls reuse the same upstream connection.
    http: DefaultHttpWebClient,
    permits: Arc<Semaphore>,
    breaker: AsyncCircuitBreaker,
    /// Where call outcomes and latencies are recorded. `None` for a client
    /// built outside a server (tests, and any future standalone caller): the
    /// counters are an observation, never a precondition for calling.
    metrics: Option<Arc<crate::metrics::Metrics>>,
}

impl RpcClient {
    /// Build a client, validating everything that cannot be fixed later.
    ///
    /// # Errors
    ///
    /// [`RpcError::Config`] when the endpoint URL is empty (after trailing
    /// slashes are removed) or not `http://`/`https://`, the deadline is zero,
    /// `max_concurrency` is zero, or the bearer token is empty or not a valid
    /// HTTP header value.
    pub fn new(config: RpcConfig) -> Result<Self, RpcError> {
        // Trim in place: the configured URL is owned, so no second copy.
        let mut endpoint = config.endpoint_url;
        endpoint.truncate(endpoint.trim_end_matches('/').len());
        if endpoint.is_empty() {
            return Err(RpcError::Config("rpc endpoint_url must not be empty"));
        }
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            return Err(RpcError::Config("rpc endpoint_url must be an http(s) URL"));
        }
        if config.deadline.is_zero() {
            return Err(RpcError::Config("rpc deadline must be non-zero"));
        }
        if config.max_concurrency == 0 {
            return Err(RpcError::Config("rpc max_concurrency must be at least 1"));
        }
        if config.bearer_token.as_bytes().is_empty() {
            return Err(RpcError::Config("rpc bearer token must not be empty"));
        }

        let mut authorization =
            HeaderValue::from_bytes(&[b"Bearer ", config.bearer_token.as_bytes()].concat())
                .map_err(|_| RpcError::Config("rpc bearer token is not a valid header value"))?;
        authorization.set_sensitive(true);

        let breaker = AsyncCircuitBreaker::builder(CIRCUIT_NAME)
            .failure_threshold(config.failure_threshold.max(1))
            .failure_window_secs(config.failure_window.as_secs_f64().max(0.001))
            .half_open_timeout_secs(config.half_open_timeout.as_secs_f64().max(0.001))
            .success_threshold(config.success_threshold.max(1))
            .build_async();

        Ok(Self {
            endpoint,
            authorization,
            deadline: config.deadline,
            max_reply_bytes: config.max_reply_bytes.max(1),
            http: DefaultHttpWebClient::default(),
            permits: Arc::new(Semaphore::new(config.max_concurrency)),
            breaker,
            metrics: None,
        })
    }

    /// Record this client's outcomes and latencies into `metrics`.
    ///
    /// A builder step rather than a constructor argument so every existing
    /// caller (and every test) keeps building a client in one expression.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// The base endpoint URL, trailing slash removed.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The end-to-end deadline applied to every call.
    pub const fn deadline(&self) -> Duration {
        self.deadline
    }

    /// Whether the circuit is currently refusing calls.
    pub fn is_circuit_open(&self) -> bool {
        self.breaker.is_open()
    }

    /// The breaker's current state name (`closed`, `open`, `half_open`).
    pub fn circuit_state(&self) -> &'static str {
        self.breaker.state_name()
    }

    /// Run the `describe` handshake and judge the endpoint's answer.
    ///
    /// `describe` carries no session (nil session UUID, sequence 0). The reply
    /// is validated like any other, and an accepted one is checked against
    /// what this node requires: envelope version 1 among
    /// `supported_versions`, every capability in [`REQUIRED_CAPABILITIES`],
    /// and `ready: true`. Software versions are informational only.
    ///
    /// # Errors
    ///
    /// Exactly those of [`RpcClient::call`]: the probe goes through the same
    /// breaker, deadline, and validation.
    pub async fn describe(&self, namespace: &str, node_id: &str) -> Result<Described, RpcError> {
        let request = RpcRequest::new(
            Operation::Describe,
            namespace,
            node_id,
            uuid::Uuid::nil().to_string(),
            0,
            0,
            u64::try_from(self.deadline.as_millis()).unwrap_or(u64::MAX).max(1),
        );
        match self.call(request).await {
            Ok(reply) => Ok(Described::judge(&reply)),
            // The envelope validator already refuses a describe that does not
            // list v1; that is a verdict about the endpoint, not a transport
            // failure, and `/readyz` names it as such.
            Err(RpcError::InvalidReply(ProtocolError::UnsupportedVersion(_))) => {
                Ok(Described::Refused(DescribeRefusal::UnsupportedVersion))
            }
            Err(error) => Err(error),
        }
    }

    /// Run one operation against the Rails endpoint.
    ///
    /// The returned reply has already passed [`Envelope::validate`] and
    /// [`RpcReply::correlates_with`], so callers only have to look at the
    /// outcome and the effects.
    ///
    /// # Errors
    ///
    /// - [`RpcError::InvalidRequest`] when `request` fails its own validation;
    ///   nothing is sent.
    /// - [`RpcError::CircuitOpen`] when the breaker refuses the call.
    /// - [`RpcError::Deadline`] when the deadline expires, permit wait
    ///   included.
    /// - [`RpcError::Http`] for a non-2xx status.
    /// - [`RpcError::Transport`] for connection, encoding, or body-read
    ///   failures, including a reply over the configured size limit.
    /// - [`RpcError::InvalidReply`] when the reply does not decode, validate,
    ///   or correlate with `request`.
    ///
    /// A `denied` or `fault` outcome is `Ok`: see the module docs.
    ///
    /// [`Envelope::validate`]: crate::protocol::Envelope::validate
    pub async fn call(&self, request: RpcRequest) -> Result<RpcReply, RpcError> {
        // One timer around the whole call, queue wait and circuit decision
        // included: that is the latency the session actually experienced.
        let started = std::time::Instant::now();
        let result = self.call_inner(request).await;

        if let Some(ref metrics) = self.metrics {
            let outcome = match result {
                Ok(ref reply) => match reply.outcome {
                    Outcome::Accepted => crate::metrics::RpcOutcome::Accepted,
                    Outcome::Denied => crate::metrics::RpcOutcome::Denied,
                    Outcome::Fault => crate::metrics::RpcOutcome::Fault,
                },
                Err(RpcError::Deadline) => crate::metrics::RpcOutcome::Deadline,
                Err(RpcError::CircuitOpen) => crate::metrics::RpcOutcome::CircuitOpen,
                Err(RpcError::Http(_)) => crate::metrics::RpcOutcome::Http,
                Err(RpcError::Transport(_)) => crate::metrics::RpcOutcome::Transport,
                Err(
                    RpcError::InvalidReply(_) | RpcError::InvalidRequest(_) | RpcError::Config(_),
                ) => crate::metrics::RpcOutcome::Invalid,
            };
            metrics.record_rpc(outcome, started.elapsed());
        }

        result
    }

    /// [`RpcClient::call`] without the measurement wrapper.
    async fn call_inner(&self, request: RpcRequest) -> Result<RpcReply, RpcError> {
        // Validate our own output before it leaves the process: an envelope
        // this node would reject on receipt must never be sent.
        request.validate().map_err(RpcError::InvalidRequest)?;

        let url = format!("{}/{}", self.endpoint, request.operation.as_str());
        let body = serde_json::to_vec(&request)
            .map_err(|e| RpcError::Transport(format!("cannot encode request: {e}")))?;

        let outcome = self
            .breaker
            .call(|| async {
                // The deadline lives inside the breaker call so an expiry is
                // recorded as a failure, not silently retried forever.
                tokio::time::timeout(self.deadline, self.send(&url, body, &request))
                    .await
                    .unwrap_or(Err(RpcError::Deadline))
            })
            .await;

        match outcome {
            Ok(reply) => Ok(reply),
            Err(CircuitError::Execution(error)) => {
                warn!(operation = %request.operation, error = %error, "Rails RPC failed");
                Err(error)
            }
            Err(other) => {
                debug!(
                    operation = %request.operation,
                    reason = %other,
                    "Rails RPC short-circuited"
                );
                Err(RpcError::CircuitOpen)
            }
        }
    }

    /// One HTTP round trip, from permit acquisition to a validated reply.
    async fn send(
        &self,
        url: &str,
        body: Vec<u8>,
        request: &RpcRequest,
    ) -> Result<RpcReply, RpcError> {
        // Queue wait is deliberately inside the caller's deadline: a call that
        // spends its whole budget waiting for capacity is an expired call, not
        // a call that runs late against a Rails process that gave up.
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| RpcError::Transport("rpc concurrency limiter closed".to_string()))?;

        let http_request = Request::post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json")
            .header(header::AUTHORIZATION, self.authorization.clone())
            .body(Body::from(body))
            .map_err(|e| RpcError::Transport(format!("cannot build request: {e}")))?;

        let response =
            self.http.serve(http_request).await.map_err(|e| RpcError::Transport(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            return Err(RpcError::Http(status.as_u16()));
        }

        let text = response
            .into_body()
            .try_into_string_with(CollectOptions::new().with_max_size(self.max_reply_bytes))
            .await
            .map_err(|e| RpcError::Transport(format!("cannot read reply body: {e}")))?;

        let reply: RpcReply = serde_json::from_str(&text)
            .map_err(|e| RpcError::InvalidReply(ProtocolError::Malformed(e)))?;
        reply.validate().map_err(RpcError::InvalidReply)?;
        reply.correlates_with(request).map_err(RpcError::InvalidReply)?;

        debug!(
            operation = %request.operation,
            outcome = %reply.outcome,
            effects = reply.effects.len(),
            "Rails RPC completed"
        );
        Ok(reply)
    }
}

impl std::fmt::Debug for RpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `authorization` is a credential and `http` is pool internals: both
        // stay out of the rendering on purpose.
        f.debug_struct("RpcClient")
            .field("endpoint", &self.endpoint)
            .field("deadline", &self.deadline)
            .field("max_reply_bytes", &self.max_reply_bytes)
            .field("available_permits", &self.permits.available_permits())
            .field("circuit_open", &self.breaker.is_open())
            .field("metrics", &self.metrics.is_some())
            .finish_non_exhaustive()
    }
}

/// What a validated `describe` reply says about the endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Described {
    /// The endpoint speaks v1, advertises every required capability, and
    /// reports itself ready.
    Admitted(DescribeInfo),
    /// The endpoint answered, but this node must not dispatch to it.
    Refused(DescribeRefusal),
}

/// Why an endpoint that answered `describe` is not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeRefusal {
    /// Outcome `denied`: the endpoint refuses this node (credential or
    /// namespace mismatch on its side).
    Denied,
    /// Outcome `fault`: the endpoint could not answer.
    Fault,
    /// `supported_versions` does not include 1.
    UnsupportedVersion,
    /// A capability in [`REQUIRED_CAPABILITIES`] is missing.
    MissingCapability,
    /// The endpoint reports `ready: false`.
    NotReady,
}

impl DescribeRefusal {
    /// The stable name `/readyz` reports for this refusal.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Denied => "denied",
            Self::Fault => "fault",
            Self::UnsupportedVersion => "unsupported_version",
            Self::MissingCapability => "missing_capability",
            Self::NotReady => "not_ready",
        }
    }
}

impl Described {
    /// Judge one validated reply.
    #[must_use]
    pub fn judge(reply: &RpcReply) -> Self {
        match reply.outcome {
            Outcome::Denied => return Self::Refused(DescribeRefusal::Denied),
            Outcome::Fault => return Self::Refused(DescribeRefusal::Fault),
            Outcome::Accepted => {}
        }
        // `RpcReply::validate` guarantees the payload on an accepted describe.
        let Some(info) = reply.describe.clone() else {
            return Self::Refused(DescribeRefusal::Fault);
        };
        if !info.supported_versions.contains(&crate::protocol::v1::VERSION) {
            return Self::Refused(DescribeRefusal::UnsupportedVersion);
        }
        if REQUIRED_CAPABILITIES.iter().any(|capability| !info.has_capability(capability)) {
            return Self::Refused(DescribeRefusal::MissingCapability);
        }
        if !info.ready {
            return Self::Refused(DescribeRefusal::NotReady);
        }
        Self::Admitted(info)
    }
}

/// Why a call could not produce a validated reply.
///
/// A `denied` outcome is **not** here: denial is a successful call with a
/// negative decision.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// The end-to-end deadline expired (queue wait included).
    #[error("rails rpc deadline expired")]
    Deadline,
    /// The circuit is open, so no call was made.
    #[error("rails rpc circuit is open")]
    CircuitOpen,
    /// The endpoint answered with a non-2xx status.
    #[error("rails rpc returned HTTP {0}")]
    Http(u16),
    /// Connection, DNS, TLS, or body-read failure.
    #[error("rails rpc transport failure: {0}")]
    Transport(String),
    /// The reply decoded but is not a usable v1 reply for this request.
    #[error("rails rpc returned an invalid reply: {0}")]
    InvalidReply(#[source] ProtocolError),
    /// This node tried to send an envelope it would itself reject.
    #[error("rails rpc request is invalid: {0}")]
    InvalidRequest(#[source] ProtocolError),
    /// The client could not be constructed from the given configuration.
    #[error("rails rpc configuration is invalid: {0}")]
    Config(&'static str),
}

impl RpcError {
    /// Whether this failure is a temporary backend problem.
    ///
    /// Temporary failures close affected sessions with `reconnect: true`;
    /// nothing ever downgrades to signed-only acceptance.
    #[must_use]
    pub const fn is_temporary(&self) -> bool {
        matches!(self, Self::Deadline | Self::CircuitOpen | Self::Http(_) | Self::Transport(_))
    }
}

#[cfg(test)]
mod tests;
