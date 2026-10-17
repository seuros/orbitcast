//! Private HTTP ingress listener: the alternative publication path to
//! PostgreSQL, plus the control ingress.
//!
//! A *separate* rama listener, bound by `ingress_listen_addr` and defaulting
//! to loopback-only binds ([`crate::config::IngressSettings`]): the public
//! `/cable` listener never serves these routes, and a node without the
//! ingress configured has no such surface at all.
//!
//! # Routes
//!
//! | route                   | behaviour |
//! |-------------------------|-----------|
//! | `POST /v1/publications` | accept one v1 [`Publication`]; route it through [`Backend::publish_publication`] (broker retention applies, the caller's id is preserved); reply `202` with an [`IngressAck`] |
//! | `POST /v1/control`      | accept one v1 [`ControlMessage`](crate::protocol::v1::control::ControlMessage); apply it exactly like the PostgreSQL control listener; reply `202` with a [`ControlAck`] |
//! | `GET /metrics`          | Prometheus text exposition for the whole process ([`crate::metrics`]) |
//! | wrong method on a known route | `405` with the allowed method |
//! | anything else           | `404` |
//!
//! Every non-`2xx` reply (the `404` included) carries an [`IngressError`]
//! body: `{"version":1,"error":"<code>","message":"<text>"}`.
//!
//! `/metrics` lives here, and not on the browser-facing listener, because this
//! listener is already private *and* already authenticated: the scrape
//! presents the same bearer credential the publisher does. A node without
//! `ingress_listen_addr` therefore exposes no metrics at all unless it opts
//! into `metrics_public` (see [`crate::metrics`]).
//!
//! # Statuses
//!
//! - `202`: the backend accepted (and, for retained streams, committed) the
//!   publication. Not a delivery receipt.
//! - `400`: unreadable, malformed, or semantically invalid envelope.
//! - `401`: missing or wrong bearer credential (compared in constant time).
//! - `403`: a valid envelope for another deployment's namespace.
//! - `413`: payload above `max_publication_size` (or a body above the
//!   transport cap).
//! - `405`: known route, wrong method.
//! - `429`: more than `MAX_IN_FLIGHT` requests already being processed.
//! - `503`: the pub/sub backend cannot accept publications right now.
//!
//! The Ruby client (`gem/lib/orbitcast/publisher/http.rb`) accepts
//! `200`/`201`/`202` and maps `401/403`, `413`, and everything else to its own
//! error classes; `202` is what this listener emits.
//!
//! # Authentication
//!
//! `Authorization: Bearer <token>`, checked before the body is read. The
//! expected token arrives as strict base64 in `ORBITCAST_INGRESS_TOKEN_BASE64`
//! (environment-only) and is compared in constant time. Browser cookies are
//! never ingress credentials.

use std::convert::Infallible;
use std::sync::Arc;

use rama::Service;
use rama::http::body::CollectOptions;
use rama::http::server::HttpServer;
use rama::http::{Body, BodyExtractExt as _, Method, Request, Response, StatusCode, header};
use rama::rt::Executor;
use rama::tcp::server::TcpListener;
use subtle::ConstantTimeEq as _;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::protocol::Envelope;
use crate::protocol::v1::publication::{
    ControlAck, IngressAck, IngressError, MAX_NOTIFY_PAYLOAD_LEN, Publication,
};
use crate::pubsub::PubSubError;
use crate::server::{Backend, ControlRejection, Shutdown};
use crate::streams::SecretBytes;
use crate::transport::http::BoundListener;

/// The publication ingress route, matching the Ruby client's default
/// `ingress_url` path.
pub const PUBLICATIONS_PATH: &str = "/v1/publications";

/// The control ingress route.
pub const CONTROL_PATH: &str = "/v1/control";

/// The Prometheus scrape route.
pub const METRICS_PATH: &str = "/metrics";

/// Ceiling on simultaneously processed ingress requests.
///
/// The ingress is one Rails application publishing over a handful of pooled
/// keep-alive connections; 64 concurrent requests is already far beyond that
/// shape. Beyond it the listener answers `429` instead of queueing without
/// bound.
const MAX_IN_FLIGHT: usize = 64;

/// Ceiling on one control message body.
///
/// The PostgreSQL control path is bounded by the `NOTIFY` payload limit, and
/// a valid control message is a few hundred bytes; the HTTP path gets the
/// same bound so both refuse the same messages.
const CONTROL_BODY_LIMIT: usize = MAX_NOTIFY_PAYLOAD_LEN;

/// Envelope overhead allowed on top of the payload cap for one request body.
///
/// `json_payload` travels JSON-escaped inside the envelope, which can inflate
/// it (quotes and backslashes double); the factor-of-two in
/// [`IngressTransport::publication_body_limit`] absorbs that, and this slack
/// covers the remaining fields. The authoritative `413` check is on the
/// *decoded* payload length against `max_publication_size`.
const ENVELOPE_SLACK: usize = 16 * 1024;

/// The assembled private ingress listener.
pub struct IngressTransport {
    backend: Arc<Backend>,
    /// Expected bearer credential (raw bytes; compared in constant time).
    token: SecretBytes,
    /// Payload ceiling, shared with the pub/sub overflow settings.
    max_publication_size: usize,
    /// Bounded concurrency; exhausted permits answer `429`.
    permits: Arc<Semaphore>,
    /// Process-wide drain signal: the listener stops with it.
    shutdown: Shutdown,
}

impl IngressTransport {
    /// Build the ingress from a validated configuration.
    ///
    /// Fails when the credential is absent - [`crate::config::Config::validate`]
    /// already guarantees it, and this refuses to serve unauthenticated even
    /// if it is bypassed.
    ///
    /// # Errors
    ///
    /// When `config.ingress.token` is unset or empty.
    pub fn new(
        backend: Arc<Backend>,
        config: &Config,
        shutdown: Shutdown,
    ) -> anyhow::Result<Arc<Self>> {
        let token =
            config.ingress.token.clone().filter(|token| !token.as_bytes().is_empty()).ok_or_else(
                || {
                    anyhow::anyhow!(
                        "the ingress requires ORBITCAST_INGRESS_TOKEN_BASE64; refusing to serve \
                     an unauthenticated publication endpoint"
                    )
                },
            )?;

        Ok(Arc::new(Self {
            backend,
            token,
            max_publication_size: config.overflow.max_publication_size,
            permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            shutdown,
        }))
    }

    /// Serve until the process drains.
    ///
    /// The ingress needs no drain phase of its own: in-flight requests are
    /// answered, and Rails treats a refused publish as a visible failure.
    pub async fn serve(self: Arc<Self>, listener: BoundListener) {
        let addr = listener.local_addr();
        info!(%addr, "OrbitCast private ingress listener started");

        let mut stop_rx = self.shutdown.subscribe();

        tokio::select! {
            () = Arc::clone(&self).accept(listener.into_listener()) => {
                info!(%addr, "Ingress listener stopped");
            }
            () = stop_rx.wait() => info!(%addr, "Ingress listener stopped accepting"),
        }
    }

    /// Serve HTTP on `listener` until it fails.
    async fn accept(self: Arc<Self>, listener: TcpListener) {
        listener
            .serve(HttpServer::auto(Executor::default()).service(IngressRouter { transport: self }))
            .await;
    }

    /// Whether the request carries the exact bearer credential.
    fn authorized(&self, req: &Request) -> bool {
        let presented = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));

        presented.is_some_and(|presented| token_matches(&self.token, presented.as_bytes()))
    }

    /// Transport-level cap on one publication request body.
    const fn publication_body_limit(&self) -> usize {
        self.max_publication_size.saturating_mul(2).saturating_add(ENVELOPE_SLACK)
    }

    /// Handle `POST /v1/publications`.
    async fn accept_publication(&self, req: Request) -> Response {
        let body = match read_body(req, self.publication_body_limit()).await {
            Ok(body) => body,
            Err(response) => return *response,
        };

        let publication: Publication = match serde_json::from_str(&body) {
            Ok(publication) => publication,
            Err(e) => {
                debug!(error = %e, "Rejecting unparseable publication");
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "malformed_envelope",
                    "malformed publication envelope",
                );
            }
        };

        if let Err(e) = publication.validate() {
            debug!(error = %e, "Rejecting invalid publication");
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_envelope",
                "invalid publication envelope",
            );
        }

        // The credential is bound to this deployment's namespace; a foreign
        // namespace is a refusal, not a routing decision.
        if publication.namespace != self.backend.namespace() {
            debug!("Rejecting publication for a foreign namespace");
            return error_response(StatusCode::FORBIDDEN, "foreign_namespace", "foreign namespace");
        }

        if publication.json_payload.len() > self.max_publication_size {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                "payload too large",
            );
        }

        // The Backend::publish_publication seam: retained streams commit
        // through the broker, everything else takes the ephemeral pub/sub
        // path. The caller's id travels unchanged, so the ack echoes exactly
        // the id the notice, the overflow row, and the broker row carry - and
        // a retried publication deduplicates instead of duplicating.
        let publication_id = publication.id.clone();
        match self.backend.publish_publication(publication).await {
            Ok(()) => accepted(
                serde_json::to_string(&IngressAck::new(publication_id))
                    .expect("ack serialization cannot fail"),
            ),
            Err(
                e @ (PubSubError::PayloadTooLarge { .. } | PubSubError::PublicationTooLarge { .. }),
            ) => {
                debug!(error = %e, "Publication refused as too large");
                error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "payload_too_large",
                    "payload too large",
                )
            }
            Err(PubSubError::Protocol(e)) => {
                debug!(error = %e, "Publication refused by the backend validator");
                error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid_envelope",
                    "invalid publication envelope",
                )
            }
            Err(e) => {
                warn!(error = %e, "Publication refused: backend unavailable");
                error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "backend_unavailable",
                    "backend unavailable",
                )
            }
        }
    }

    /// Handle `POST /v1/control`.
    ///
    /// Decoding, validation, namespace ownership, deduplication, and the
    /// disconnect itself are the *same* code the PostgreSQL control listener
    /// runs ([`Backend::decode_control`] / [`Backend::apply_control`]); only
    /// the status mapping lives here. A duplicate `control_id` is an
    /// idempotent `202`: the first arrival already acted.
    async fn accept_control(&self, req: Request) -> Response {
        let body = match read_body(req, CONTROL_BODY_LIMIT).await {
            Ok(body) => body,
            Err(response) => return *response,
        };

        let control = match self.backend.decode_control(&body) {
            Ok(control) => control,
            Err(rejection @ ControlRejection::ForeignNamespace { .. }) => {
                debug!(error = %rejection, "Rejecting control message");
                return error_response(
                    StatusCode::FORBIDDEN,
                    "foreign_namespace",
                    "foreign namespace",
                );
            }
            Err(rejection) => {
                debug!(error = %rejection, "Rejecting control message");
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid_control",
                    "invalid control message",
                );
            }
        };

        self.backend.apply_control(&control).await;
        accepted(
            serde_json::to_string(&ControlAck::new(control.control_id))
                .expect("ack serialization cannot fail"),
        )
    }

    /// Handle `GET /metrics`: one Prometheus scrape for the whole process.
    ///
    /// Rendered from the backend, so it covers the `/cable` listener and the
    /// docking transport too - this listener is only where the route is
    /// mounted, never the scope of what it reports.
    fn serve_metrics(&self) -> Response {
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, crate::metrics::CONTENT_TYPE)
            .body(Body::from(self.backend.render_metrics()))
            .expect("static response parts are valid")
    }
}

/// Constant-time bearer comparison.
///
/// `subtle` compares byte-for-byte without early exit; two slices of unequal
/// length compare unequal (the length itself is not secret - it is visible on
/// the wire anyway).
fn token_matches(expected: &SecretBytes, presented: &[u8]) -> bool {
    expected.as_bytes().ct_eq(presented).into()
}

/// Read one request body under `limit` bytes.
///
/// A `Content-Length` above the limit is answered `413` before any body byte
/// is read; a body that fails the bounded read (over-limit chunked transfer,
/// invalid UTF-8, or a broken stream) is `400`. (The error is boxed only to
/// keep the happy path's `Result` small.)
async fn read_body(req: Request, limit: usize) -> Result<String, Box<Response>> {
    let declared = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());

    if declared.is_some_and(|len| len > limit) {
        return Err(Box::new(error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_too_large",
            "body too large",
        )));
    }

    req.into_body().try_into_string_with(CollectOptions::new().with_max_size(limit)).await.map_err(
        |e| {
            debug!(error = %e, "Ingress body could not be read");
            Box::new(error_response(StatusCode::BAD_REQUEST, "unreadable_body", "unreadable body"))
        },
    )
}

/// A `202` with a JSON body.
fn accepted(body: String) -> Response {
    json_response(StatusCode::ACCEPTED, body)
}

/// A versioned JSON error envelope ([`IngressError`]); the message never
/// carries payload data.
fn error_response(code: StatusCode, error: &'static str, message: &str) -> Response {
    json_response(
        code,
        serde_json::to_string(&IngressError::new(error, message))
            .expect("error envelope serialization cannot fail"),
    )
}

fn json_response(code: StatusCode, body: String) -> Response {
    Response::builder()
        .status(code)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .expect("static response parts are valid")
}

/// The ingress router: two POST routes, everything else `404`/`405`.
#[derive(Clone)]
struct IngressRouter {
    transport: Arc<IngressTransport>,
}

impl Service<Request> for IngressRouter {
    type Output = Response;
    type Error = Infallible;

    /// Route one request, and count its reply.
    ///
    /// Every exit goes through [`IngressRouter::route`] so the counter is
    /// incremented in exactly one place: a status this module can produce but
    /// forgets to count would be an invisible failure mode.
    async fn serve(&self, req: Request<Body>) -> Result<Self::Output, Self::Error> {
        let response = self.route(req).await;

        if let Some(status) = crate::metrics::IngressStatus::from_code(response.status().as_u16()) {
            self.transport.backend.metrics().record_ingress(status);
        } else {
            warn!(status = %response.status(), "Ingress produced an unclassified status");
        }

        Ok(response)
    }
}

impl IngressRouter {
    async fn route(&self, req: Request<Body>) -> Response {
        let path = req.uri().path_or_root().into_owned();

        if !matches!(path.as_str(), PUBLICATIONS_PATH | CONTROL_PATH | METRICS_PATH) {
            return error_response(StatusCode::NOT_FOUND, "not_found", "not found");
        }

        let (expected, allowed) = match path.as_str() {
            METRICS_PATH => (Method::GET, "GET"),
            _ => (Method::POST, "POST"),
        };

        if req.method() != expected {
            let mut response = error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "method not allowed",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, rama::http::HeaderValue::from_static(allowed));
            return response;
        }

        // Credential first, before a single body byte is read - and before a
        // scrape is rendered: the metrics route is administrative, so it is
        // authenticated exactly like the publication routes.
        if !self.transport.authorized(&req) {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "invalid ingress credential",
            );
        }

        // Bounded concurrency: beyond the cap the caller is told to back off
        // rather than queued without bound.
        let Ok(_permit) = self.transport.permits.clone().try_acquire_owned() else {
            return error_response(
                StatusCode::TOO_MANY_REQUESTS,
                "at_capacity",
                "ingress at capacity",
            );
        };

        match path.as_str() {
            PUBLICATIONS_PATH => self.transport.accept_publication(req).await,
            CONTROL_PATH => self.transport.accept_control(req).await,
            METRICS_PATH => self.transport.serve_metrics(),
            _ => unreachable!("path already matched"),
        }
    }
}

#[cfg(test)]
mod tests;
