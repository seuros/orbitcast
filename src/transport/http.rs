//! Direct HTTP/WebSocket listener, built on rama.
//!
//! This is the transport that makes the binary runnable without a supervisor:
//! it serves the browser-facing `/cable` endpoint plus the two lifecycle
//! probes, and drives the *same* [`Connections`] logic the docking transport
//! uses.
//!
//! # Routes
//!
//! | route      | behaviour |
//! |------------|-----------|
//! | `GET /cable`   | WebSocket upgrade (see subprotocol/origin rules below) |
//! | `GET /healthz` | always `200`: process liveness only |
//! | `GET /readyz`  | `200` while the pub/sub listener is ready, the Rails endpoint (dispatch mode) passed `describe`, and the node is not draining; else `503`. JSON body names each component |
//! | `GET /metrics` | **only** when `metrics_public = true`; unauthenticated Prometheus text. `404` otherwise |
//! | anything else  | `404` |
//!
//! `/metrics` is off by default and belongs on the private ingress listener,
//! behind its bearer credential. See [`crate::metrics`] for the tradeoff the
//! public switch makes.
//!
//! # Subprotocol negotiation
//!
//! `@rails/actioncable` offers `["actioncable-v1-json", "actioncable-unsupported"]`.
//! When the offer contains `actioncable-v1-json` the response selects exactly
//! that. A client that offers nothing, or offers only protocols we do not
//! speak, still gets a connection — the response then carries no
//! `Sec-WebSocket-Protocol` header at all, which is what an ActionCable server
//! (websocket-driver) does, and is never an implicit claim to speak the
//! client's protocol.
//!
//! # Origin validation
//!
//! Enforced before the upgrade, by [`crate::config::HttpConfig::allows_origin`]:
//! a request without an `Origin` header is allowed (non-browser clients do not
//! send one), a request with one is allowed only when the normalized origin is
//! in `allowed_origins`. An empty list therefore rejects *every* browser
//! origin; `*` is an explicit development-only opt-out. Rejection is `403`
//! before any upgrade response is produced.
//!
//! # Limits
//!
//! - `max_connections`: further upgrades are refused with `503` before the
//!   handshake completes.
//! - `max_message_size`: enforced by the WebSocket layer on inbound messages
//!   and frames; an oversize message fails the connection instead of being
//!   buffered.
//! - `rate_connect_per_ip_per_min`: a token bucket per client address, burst
//!   equal to the rate. Over the limit is `429` with `Retry-After`, before the
//!   upgrade and before any session exists. The key is the **TCP peer**
//!   address unless that peer is in `trusted_proxies`, in which case the
//!   client address is taken from `Forwarded` / `X-Forwarded-For` (walking the
//!   chain from the proxy end past every trusted hop). An untrusted peer's
//!   forwarding headers are ignored: a header anyone can send is not an
//!   identity. A request with no peer address (there is none in unit tests)
//!   is not rate limited.
//! - `max_header_bytes`: the HTTP/1 read buffer, which bounds the header
//!   section of every request on this listener; a larger head fails the
//!   connection before any upgrade. (HTTP/2 gets the same bound as its header
//!   list size.)
//! - `handshake_timeout_ms`: a deadline on completing the upgrade, from the
//!   moment the request reaches this router until the WebSocket is
//!   established. A handshake that misses it is abandoned and counted
//!   (`orbitcast_connections_rejected_total{reason="handshake_timeout"}`); no
//!   session is ever created for it. (The TCP accept that preceded the request
//!   is bounded by rama's own server read timeouts, which this deliberately
//!   does not reimplement.)
//! - `rate_commands_per_conn_per_sec` / `rate_subscribes_per_conn_per_sec`:
//!   charged in [`Connections::handle_command`], not here - they apply
//!   identically to the docking transport. Over a limit the session is closed
//!   with a reconnectable `disconnect`.
//! - `outbound_queue_messages` / `outbound_queue_bytes`: each socket has its
//!   own bounded outbound queue (both limits), and the hub delivers every
//!   frame straight into it - there is no shared queue in between, so the only
//!   bound a frame can meet is its own socket's, and one slow reader never
//!   costs another socket a frame. Overflow is lag: the hub sends the socket
//!   `disconnect {reconnect: true}` and closes it, and
//!   `orbitcast_socket_lag_disconnects_total{reason="queue"}` is incremented.
//!   The close signal travels on a channel of its own, so it cannot be lost
//!   behind a full frame queue.
//!
//! # Liveness
//!
//! Every socket is sent a WebSocket **control ping** on
//! `WS_PING_INTERVAL`; browsers and rama answer it with a pong without any
//! application involvement. Every inbound message - that pong, or a real client
//! frame - marks the connection alive
//! ([`Connections::record_liveness`](crate::server::Connections::record_liveness)),
//! which is the only signal distributed presence refreshes a lease from. The
//! ActionCable JSON `ping` is *not* that signal: this node sends it, so it
//! proves nothing about the peer.
//!
//! # TLS
//!
//! There is none, deliberately. Public TLS is terminated by the deployment
//! proxy that forwards `/cable` here; this listener should be
//! bound to loopback or a private interface.

use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use rama::Service;
use rama::extensions::ExtensionsRef;
use rama::futures::{SinkExt, StreamExt};
use rama::http::headers::{HeaderMapExt, SecWebSocketProtocol};
use rama::http::io::upgrade;
use rama::http::layer::upgrade::UpgradeResponse;
use rama::http::server::HttpServer;
use rama::http::service::web::response::IntoResponse;
use rama::http::ws::handshake::server::WebSocketAcceptor;
use rama::http::ws::protocol::{CloseFrame, Message, ProtocolError, Role, WebSocketConfig};
use rama::http::ws::runtime::AsyncWebSocket;
use rama::http::{Body, HeaderValue, Method, Request, Response, StatusCode};
use rama::net::stream::SocketInfo;
use rama::rt::Executor;
use rama::tcp::server::TcpListener;
use rama::utils::str::non_empty_str;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::metrics::ConnectionRejection;
use crate::ratelimit::{DEFAULT_CAPACITY, KeyedRateLimit};
use crate::server::{Backend, Connections, DISCONNECT_REASON_SERVER_RESTART, Shutdown};
use crate::transport::docking::{Disembark, Outgoing};
use crate::transport::{LinkOutcome, OutboundLink};

/// The only ActionCable subprotocol this server speaks.
pub const ACTIONCABLE_SUBPROTOCOL: &str = "actioncable-v1-json";

/// How long a draining socket is given to flush and close politely.
const CLOSE_GRACE: Duration = Duration::from_secs(1);

/// RFC 6455 close code 1001: the server is going away.
const CLOSE_GOING_AWAY: u16 = 1001;

/// RFC 6455 close code 1009: an inbound message exceeded the size limit.
const CLOSE_MESSAGE_TOO_BIG: u16 = 1009;

/// How often the drain loop re-checks whether every session is gone.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// How often each socket is sent a WebSocket control ping.
///
/// Short enough that an idle browser tab produces a pong well inside the
/// default 45 s presence lease, and cheap enough to be unconditional: this is a
/// two-byte control frame answered by the peer's WebSocket stack, not an
/// application round trip.
const WS_PING_INTERVAL: Duration = Duration::from_secs(15);

/// A bound TCP listener plus its resolved local address.
///
/// Binding is separate from serving so a port conflict fails startup instead of
/// disappearing into a background task, and so tests can bind port 0 and learn
/// the assigned port.
pub struct BoundListener {
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl BoundListener {
    /// The address actually bound (with the real port when `:0` was requested).
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Take the raw listener (sibling transport modules only).
    pub(crate) fn into_listener(self) -> TcpListener {
        self.listener
    }
}

/// Bind the HTTP listener.
///
/// # Errors
///
/// When `addr` cannot be bound (malformed, unavailable, or already in use);
/// the error names `addr`. Also when the bound socket cannot report its local
/// address.
pub async fn bind(addr: &str) -> anyhow::Result<BoundListener> {
    let listener = TcpListener::build(Executor::default())
        .bind_address(addr)
        .await
        .map_err(|error| anyhow::anyhow!("failed to bind HTTP listener at {addr}: {error}"))?;
    let local_addr = listener.local_addr()?;

    Ok(BoundListener { listener, local_addr })
}

/// A server-initiated close, delivered on a channel of its own.
#[derive(Debug, Clone)]
struct CloseSignal {
    code: u16,
    reason: String,
    /// Write the frames already queued before closing (an ordered close:
    /// the ActionCable `disconnect` precedes the socket close). `false` for a
    /// lag close, whose queue is the problem.
    flush: bool,
    /// One last frame written right before the close - the reconnectable
    /// `disconnect` a lagging socket is owed, which its full queue could not
    /// carry.
    final_frame: Option<Bytes>,
}

/// Bounds of one socket's outbound queue.
#[derive(Debug, Clone, Copy)]
struct QueueLimits {
    messages: usize,
    bytes: usize,
}

/// The sending half of one socket's outbound queue.
#[derive(Debug)]
struct SocketSender {
    frames: mpsc::Sender<Bytes>,
    /// Bytes currently queued (added on enqueue, subtracted on dequeue).
    queued_bytes: Arc<AtomicUsize>,
    /// The close signal. A `watch` cannot be lost or refused: a full frame
    /// queue never stands between the server and a close.
    close: watch::Sender<Option<CloseSignal>>,
}

/// The receiving half, owned by the socket's pump.
struct SocketReceiver {
    frames: FrameQueue,
    close: watch::Receiver<Option<CloseSignal>>,
}

/// The frame half of a socket's queue, with its byte accounting.
struct FrameQueue {
    frames: mpsc::Receiver<Bytes>,
    queued_bytes: Arc<AtomicUsize>,
}

impl FrameQueue {
    /// Take the next queued frame, releasing its bytes from the accounting.
    async fn next(&mut self) -> Option<Bytes> {
        let frame = self.frames.recv().await?;
        self.queued_bytes.fetch_sub(frame.len(), Ordering::Relaxed);
        Some(frame)
    }

    /// Take a queued frame without waiting (close-time flush).
    fn try_next(&mut self) -> Option<Bytes> {
        let frame = self.frames.try_recv().ok()?;
        self.queued_bytes.fetch_sub(frame.len(), Ordering::Relaxed);
        Some(frame)
    }
}

/// `conn_id -> socket queue`: the hub's [`OutboundLink`] for this transport.
///
/// Each frame goes straight from the hub into the target socket's own bounded
/// queue; nothing is shared between sockets on the way.
struct SocketRegistry {
    sockets: DashMap<u32, SocketSender>,
    limits: QueueLimits,
}

impl SocketRegistry {
    fn new(limits: QueueLimits) -> Self {
        Self { sockets: DashMap::new(), limits }
    }

    fn insert(&self, conn_id: u32) -> SocketReceiver {
        let (frames_tx, frames_rx) = mpsc::channel(self.limits.messages);
        let (close_tx, close_rx) = watch::channel(None);
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        self.sockets.insert(
            conn_id,
            SocketSender {
                frames: frames_tx,
                queued_bytes: Arc::clone(&queued_bytes),
                close: close_tx,
            },
        );
        SocketReceiver { frames: FrameQueue { frames: frames_rx, queued_bytes }, close: close_rx }
    }

    fn remove(&self, conn_id: u32) {
        self.sockets.remove(&conn_id);
    }

    fn len(&self) -> usize {
        self.sockets.len()
    }

    fn is_empty(&self) -> bool {
        self.sockets.is_empty()
    }

    /// Signal an ordered close: queued frames are written first.
    fn close(&self, conn_id: u32, code: u16, reason: String) {
        let Some(socket) = self.sockets.get(&conn_id) else {
            debug!(conn_id, "No socket for close; dropped");
            return;
        };
        // A lag close already pending keeps precedence: its queue is the
        // problem, and flushing it would only prolong the stall.
        socket.close.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(CloseSignal { code, reason, flush: true, final_frame: None });
            true
        });
    }
}

#[async_trait]
impl OutboundLink for SocketRegistry {
    /// A queue past either bound is lag for that socket alone: the frame is
    /// not queued and [`LinkOutcome::Lagged`] tells the hub to close the
    /// socket (through [`Self::lag_close`], whose `disconnect` rides the
    /// close signal rather than the full queue). One slow reader never
    /// blocks, or costs a frame to, an unrelated socket.
    fn try_frame(&self, conn_id: u32, frame: Bytes) -> LinkOutcome {
        let Some(socket) = self.sockets.get(&conn_id) else {
            debug!(conn_id, "No socket for frame; dropped");
            return LinkOutcome::Closed;
        };

        let queued = socket.queued_bytes.load(Ordering::Relaxed);
        if queued.saturating_add(frame.len()) > self.limits.bytes {
            debug!(conn_id, limit = "bytes", "Socket outbound queue full");
            return LinkOutcome::Lagged;
        }

        socket.queued_bytes.fetch_add(frame.len(), Ordering::Relaxed);
        match socket.frames.try_send(frame) {
            Ok(()) => LinkOutcome::Sent,
            Err(mpsc::error::TrySendError::Full(frame)) => {
                socket.queued_bytes.fetch_sub(frame.len(), Ordering::Relaxed);
                debug!(conn_id, limit = "messages", "Socket outbound queue full");
                LinkOutcome::Lagged
            }
            Err(mpsc::error::TrySendError::Closed(frame)) => {
                socket.queued_bytes.fetch_sub(frame.len(), Ordering::Relaxed);
                debug!(conn_id, "Socket closed; frame dropped");
                LinkOutcome::Closed
            }
        }
    }

    /// Direct delivery never waits: the only room is the socket's own.
    async fn frame(&self, conn_id: u32, frame: Bytes) -> LinkOutcome {
        self.try_frame(conn_id, frame)
    }

    async fn disconnect(&self, close: Disembark) {
        self.close(close.conn_id, close.code, close.reason);
    }

    /// The close signal carries the `disconnect` frame as its final frame,
    /// and skips the backlog: that queue is the problem, and flushing it
    /// would only prolong the stall. A lag close already signalled, or an
    /// ordered close already pending, is left as it is.
    fn lag_close(&self, disconnect: Bytes, close: Disembark) {
        let Some(socket) = self.sockets.get(&close.conn_id) else {
            debug!(conn_id = close.conn_id, "No socket for lag close; dropped");
            return;
        };
        socket.close.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(CloseSignal {
                code: close.code,
                reason: close.reason,
                flush: false,
                final_frame: Some(disconnect),
            });
            true
        });
    }
}

/// The assembled HTTP transport: routing, upgrade policy, and socket lifetime.
pub struct HttpTransport {
    connections: Arc<Connections>,
    sockets: Arc<SocketRegistry>,
    /// Listener policy: origins, per-message limit, connection ceiling, rate
    /// limits, handshake deadline, metrics exposure.
    policy: crate::config::HttpConfig,
    /// Per-peer-address connection admission; `None` when disabled.
    connect_limit: Option<KeyedRateLimit<IpAddr>>,
    /// Connection ids for this transport's hub only. Each transport has its own
    /// hub, so these never need to be unique against docking connection ids.
    next_conn_id: AtomicU32,
    /// Process-wide drain signal: readiness flips and new upgrades are refused.
    shutdown: Shutdown,
    /// Listener-only stop signal, fired at the *end* of [`Self::drain`] so the
    /// probes stay answerable while sessions are being closed.
    listener_stop: Shutdown,
    /// Background tasks (ping, presence sweep).
    _tasks: crate::server::TaskGuard,
}

impl HttpTransport {
    /// Build the transport: connection driver and the socket registry the
    /// hub delivers into directly.
    ///
    /// # Errors
    ///
    /// [`crate::rpc_client::RpcError::Config`] when Rails dispatch is
    /// configured but its client cannot be built: an endpoint that is not an
    /// `http(s)` URL, a zero deadline or concurrency, or a token that is not a
    /// valid header value.
    pub fn new(
        backend: Arc<Backend>,
        config: &Config,
        shutdown: Shutdown,
    ) -> Result<Arc<Self>, crate::rpc_client::RpcError> {
        // `Connections::new` builds its hub around a docking queue sender.
        // This transport has no shared queue: the hub is pointed at the
        // socket registry before it can hold a session, so that queue is never
        // written to, and its receiver is dropped here.
        let (outgoing_tx, _never_read) = mpsc::channel::<Outgoing>(1);
        let connections = Connections::new(backend, config, outgoing_tx)?;
        let sockets = Arc::new(SocketRegistry::new(QueueLimits {
            messages: config.http.outbound_queue_messages.max(1),
            bytes: config.http.outbound_queue_bytes.max(1),
        }));
        connections.hub().attach_link(Arc::clone(&sockets) as Arc<dyn OutboundLink>);

        let tasks = connections.spawn_background_tasks();

        let connect_limit =
            KeyedRateLimit::per_minute(config.http.rate_connect_per_ip_per_min, DEFAULT_CAPACITY);
        if connect_limit.is_none() {
            warn!(
                "Per-address connection rate limiting is disabled (rate_connect_per_ip_per_min = 0)"
            );
        }
        if config.http.metrics_public {
            warn!(
                "Serving GET /metrics unauthenticated on the public listener \
                 (metrics_public); prefer the private ingress listener"
            );
        }

        Ok(Arc::new(Self {
            connections,
            sockets,
            policy: config.http.clone(),
            connect_limit,
            next_conn_id: AtomicU32::new(1),
            shutdown,
            listener_stop: Shutdown::new(),
            _tasks: tasks,
        }))
    }

    /// The connection driver behind this transport.
    pub const fn connections(&self) -> &Arc<Connections> {
        &self.connections
    }

    /// Serve until the listener is stopped by [`Self::drain`].
    ///
    /// The listener deliberately keeps answering during a drain: `/readyz`
    /// must be reachable and report `503` long enough for a load balancer to
    /// take this node out of rotation, and `/cable` must answer `503` rather
    /// than time out. The socket closes are what bound the shutdown.
    pub async fn serve(self: Arc<Self>, listener: BoundListener) {
        let addr = listener.local_addr();
        info!(%addr, "OrbitCast HTTP listener started");

        let mut stop_rx = self.listener_stop.subscribe();

        tokio::select! {
            () = Arc::clone(&self).accept(listener.into_listener()) => {
                info!(%addr, "HTTP listener stopped");
            }
            () = stop_rx.wait() => info!(%addr, "HTTP listener stopped accepting"),
        }
    }

    /// Serve HTTP on `listener` until it fails.
    async fn accept(self: Arc<Self>, listener: TcpListener) {
        let mut server = HttpServer::auto(Executor::default());
        // The HTTP/1 read buffer is what bounds a request head: a header
        // section larger than it fails the connection before any upgrade.
        // `Config::validate` keeps the value above rama's own floor.
        if let Err(error) = server.http1_mut().try_set_max_buf_size(self.policy.max_header_bytes) {
            warn!(
                error = %error,
                "max_header_bytes rejected by the HTTP/1 server; using its default"
            );
        }
        server.h2_mut().set_max_header_list_size(
            u32::try_from(self.policy.max_header_bytes).unwrap_or(u32::MAX),
        );
        listener.serve(server.service(Router { transport: self })).await;
    }

    /// Close every session and wait for them to go, up to `timeout`.
    ///
    /// The sequence is: advertise a standard reconnectable ActionCable
    /// `disconnect`, enqueue a close behind it on every socket (one ordered
    /// mailbox per socket, so the disconnect frame is always written first),
    /// wait for the sockets to finish, tear down whatever is left through the
    /// one teardown path, and only then stop the listener.
    pub async fn drain(&self, timeout: Duration) {
        self.shutdown.trigger();
        self.connections.disconnect_all(DISCONNECT_REASON_SERVER_RESTART, true);
        self.connections.close_all(CLOSE_GOING_AWAY, "going away").await;

        let deadline = tokio::time::Instant::now() + timeout;
        while !self.sockets.is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
        }

        if !self.sockets.is_empty() {
            warn!(remaining = self.sockets.len(), "Drain deadline reached; forcing teardown");
        }

        // Every session that outlived its socket still goes through the same
        // teardown: memberships, backend interest, presence.
        self.connections.drain().await;
        self.listener_stop.trigger();
    }

    /// Handle `GET /cable`: validate, negotiate, upgrade.
    ///
    /// Order matters: drain, origin, connection ceiling, then the per-address
    /// rate limit. The cheap, unconditional refusals come first, and a token is
    /// only ever spent on a request that would otherwise have been upgraded, so
    /// a client that keeps sending a forbidden `Origin` never exhausts its own
    /// connection budget.
    async fn serve_cable(self: &Arc<Self>, req: Request) -> Response {
        let metrics = Arc::clone(self.connections.backend().metrics());

        if self.shutdown.is_triggered() {
            metrics.record_connection_rejected(ConnectionRejection::Draining);
            return status(StatusCode::SERVICE_UNAVAILABLE, "draining");
        }

        let origin =
            req.headers().get("origin").and_then(|value| value.to_str().ok()).map(str::to_string);

        if !self.policy.allows_origin(origin.as_deref()) {
            warn!(origin = ?origin, "Rejected WebSocket upgrade: origin not allowed");
            metrics.record_connection_rejected(ConnectionRejection::Origin);
            return status(StatusCode::FORBIDDEN, "origin not allowed");
        }

        if self.sockets.len() >= self.policy.max_connections {
            warn!(
                max = self.policy.max_connections,
                "Rejected WebSocket upgrade: connection limit"
            );
            metrics.record_connection_rejected(ConnectionRejection::Limit);
            return status(StatusCode::SERVICE_UNAVAILABLE, "connection limit reached");
        }

        // The address a client is attributed to: the TCP peer, or - only
        // behind a trusted proxy - the forwarded client address.
        let client_ip =
            peer_ip(&req).map(|peer| self.policy.client_ip(peer, &forwarded_chain(&req)));

        if let Some(ref limit) = self.connect_limit
            && let Some(client) = client_ip
            && !limit.allow(client)
        {
            // The address is not logged: a peer address is request metadata,
            // and structured logs here carry no client identity.
            warn!("Rejected WebSocket upgrade: per-address connection rate limit");
            metrics.record_connection_rejected(ConnectionRejection::RateLimited);
            return too_many_requests();
        }

        let req = normalize_subprotocol_offer(req);

        // `protocols_flex` is what lets a client that offers no subprotocol
        // through; the allow-list is what stops us from echoing one we do not
        // speak (the offer was already normalized above).
        let acceptor = WebSocketAcceptor::default()
            .with_protocols_flex(true)
            .with_protocols(SecWebSocketProtocol::new(non_empty_str!(ACTIONCABLE_SUBPROTOCOL)));

        let UpgradeResponse { response, request, extensions } = match acceptor.serve(req).await {
            Ok(accepted) => accepted,
            Err(rejection) => {
                debug!("WebSocket handshake rejected by acceptor");
                return rejection;
            }
        };

        // Path *and* query: Rails connection authentication routinely reads the
        // query string of the original `/cable` URL. `?` with nothing after it
        // is preserved, because it is distinct from no query at all.
        let path = request.uri().query().map_or_else(
            || request.uri().path_or_root().into_owned(),
            |query| format!("{}?{}", request.uri().path_or_root(), query.as_encoded_str()),
        );
        let remote_addr = client_ip.map(|ip| ip.to_string()).unwrap_or_default();
        let headers = collect_headers(&request);
        let on_upgrade = upgrade::handle_upgrade(&request);

        let transport = Arc::clone(self);
        let ws_config = WebSocketConfig::default()
            .with_max_message_size(self.policy.max_message_size)
            .with_max_frame_size(self.policy.max_message_size);
        let handshake_timeout = self.policy.handshake_timeout;

        tokio::spawn(async move {
            // Everything between "the response is on its way" and "the socket
            // is usable" is on one deadline: a peer that opens a connection,
            // gets the 101, and then never finishes the upgrade must not hold
            // a task forever.
            let established = tokio::time::timeout(handshake_timeout, async {
                let upgraded = on_upgrade.await?;
                upgraded.extensions().extend(&extensions);
                Ok::<_, rama::error::BoxError>(
                    AsyncWebSocket::from_raw_socket(upgraded, Role::Server, Some(ws_config)).await,
                )
            })
            .await;

            let socket = match established {
                Ok(Ok(socket)) => socket,
                Ok(Err(e)) => {
                    warn!(error = %e, "WebSocket upgrade failed");
                    return;
                }
                Err(_) => {
                    warn!(
                        timeout_ms =
                            u64::try_from(handshake_timeout.as_millis()).unwrap_or(u64::MAX),
                        "WebSocket handshake did not complete within the deadline"
                    );
                    metrics.record_connection_rejected(ConnectionRejection::HandshakeTimeout);
                    return;
                }
            };

            transport.run_socket(socket, path, remote_addr, headers).await;
        });

        response
    }

    /// Own one upgraded socket for its whole lifetime.
    async fn run_socket(
        self: Arc<Self>,
        mut socket: AsyncWebSocket,
        path: String,
        remote_addr: String,
        headers: std::collections::HashMap<String, String>,
    ) {
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let mut outbound = self.sockets.insert(conn_id);

        debug!(conn_id, %path, remote = %remote_addr, "WebSocket connected");
        self.connections.accept(conn_id, path, remote_addr, headers).await;

        let close = self.pump(conn_id, &mut socket, &mut outbound).await;

        // Teardown first: stop routing before the socket goes away, so nothing
        // is queued for a connection that can no longer receive it.
        self.sockets.remove(conn_id);
        self.connections.teardown(conn_id).await;

        // Best effort: the session is already torn down, so a peer that will
        // not take the close frame changes nothing but deserves a trace.
        let close_frame =
            close.map(|(code, reason)| CloseFrame { code: code.into(), reason: reason.into() });
        match tokio::time::timeout(CLOSE_GRACE, socket.close(close_frame)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => debug!(conn_id, error = %e, "Close frame not delivered"),
            Err(_) => debug!(conn_id, "Close handshake exceeded its grace period"),
        }

        debug!(conn_id, "WebSocket disconnected");
    }

    /// Move messages both ways until the socket ends.
    ///
    /// Returns the close frame to send, if the server is the side closing.
    ///
    /// Shutdown is *not* a branch here on purpose: a drain enqueues the
    /// standard `disconnect` frame and then the close on the same ordered
    /// mailbox, so the client always sees the ActionCable disconnect before the
    /// WebSocket close. Reacting to the signal directly would race that order.
    async fn pump(
        &self,
        conn_id: u32,
        socket: &mut AsyncWebSocket,
        outbound: &mut SocketReceiver,
    ) -> Option<(u16, String)> {
        let mut ws_ping = tokio::time::interval(WS_PING_INTERVAL);
        ws_ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ws_ping.tick().await; // consume the immediate first tick

        let SocketReceiver { frames, close } = outbound;

        loop {
            tokio::select! {
                // The close signal first: it cannot be lost behind a full
                // frame queue, and an ordered close flushes that queue itself.
                biased;

                changed = close.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                    let signal = close.borrow_and_update().clone();
                    let Some(signal) = signal else { continue };
                    return close_socket(conn_id, socket, frames, signal).await;
                }

                frame = frames.next() => match frame {
                    Some(frame) => {
                        if !write_text(conn_id, socket, frame).await {
                            return None;
                        }
                    }
                    None => return None,
                },

                message = socket.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        // Inbound bytes from the peer: verified liveness, and
                        // recorded before the frame is even parsed.
                        self.connections.record_liveness(conn_id);
                        self.connections.handle_frame(conn_id, text.as_bytes()).await;
                    }
                    Some(Ok(Message::Binary(_))) => {
                        self.connections.record_liveness(conn_id);
                        debug!(
                            conn_id,
                            "Ignoring binary frame: the ActionCable protocol is JSON text"
                        );
                    }
                    // Control frames: rama answers Ping with Pong itself. The
                    // pong answering *our* ping is what keeps an idle tab's
                    // presence lease alive.
                    Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {
                        self.connections.record_liveness(conn_id);
                    }
                    Some(Ok(Message::Close(_))) | None => return None,
                    Some(Err(ProtocolError::MessageTooLong { size, max_size })) => {
                        warn!(
                            conn_id,
                            size, max_size, "Inbound message exceeds the configured limit"
                        );
                        return Some((CLOSE_MESSAGE_TOO_BIG, "message too big".to_string()));
                    }
                    Some(Err(e)) => {
                        debug!(conn_id, error = %e, "Socket read failed");
                        return None;
                    }
                },

                _ = ws_ping.tick() => {
                    // The peer's stack answers this without waking the page; a
                    // peer that cannot answer stops being refreshed, and its
                    // presence leases expire on schedule.
                    if let Err(e) = socket.send(Message::Ping(Vec::new().into())).await {
                        debug!(conn_id, error = %e, "Control ping failed; closing");
                        return None;
                    }
                }
            }
        }
    }
}

/// Apply one close signal: flush what was queued before it (an ordered
/// close), write the final frame a lag close carries, then report the close
/// code. Bounded by `CLOSE_GRACE`: a peer that will not read its backlog is
/// not waited on.
async fn close_socket(
    conn_id: u32,
    socket: &mut AsyncWebSocket,
    frames: &mut FrameQueue,
    signal: CloseSignal,
) -> Option<(u16, String)> {
    let flush = async {
        if signal.flush {
            while let Some(frame) = frames.try_next() {
                if !write_text(conn_id, socket, frame).await {
                    return;
                }
            }
        }
        if let Some(frame) = signal.final_frame
            && !write_text(conn_id, socket, frame).await
        {
            debug!(conn_id, "Final frame before close not delivered");
        }
    };
    if tokio::time::timeout(CLOSE_GRACE, flush).await.is_err() {
        debug!(conn_id, "Close-time flush exceeded its grace period");
    }
    Some((signal.code, signal.reason))
}

/// Write one text frame; `false` when the socket is gone.
async fn write_text(conn_id: u32, socket: &mut AsyncWebSocket, frame: Bytes) -> bool {
    let text = match std::str::from_utf8(&frame) {
        Ok(text) => text,
        Err(e) => {
            warn!(conn_id, error = %e, "Dropping non-UTF-8 outbound frame");
            return true;
        }
    };
    if let Err(e) = socket.send(Message::text(text)).await {
        debug!(conn_id, error = %e, "Socket write failed");
        return false;
    }
    true
}

/// The forwarded client chain, client first, from `Forwarded` (RFC 7239)
/// or, failing that, `X-Forwarded-For`.
///
/// Parsed, never trusted on its own: [`crate::config::HttpConfig::client_ip`]
/// decides whether the peer may supply it at all.
fn forwarded_chain(req: &Request) -> Vec<IpAddr> {
    let header = |name: &str| -> Vec<String> {
        req.headers()
            .get_all(name)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(',').map(|entry| entry.trim().to_string()))
            .filter(|entry| !entry.is_empty())
            .collect()
    };

    let forwarded: Vec<IpAddr> = header("forwarded")
        .iter()
        .filter_map(|element| {
            element.split(';').find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                key.trim().eq_ignore_ascii_case("for").then(|| parse_forwarded_node(value))?
            })
        })
        .collect();
    if !forwarded.is_empty() {
        return forwarded;
    }

    header("x-forwarded-for").iter().filter_map(|entry| parse_forwarded_node(entry)).collect()
}

/// One `for=` node or `X-Forwarded-For` entry: optionally quoted, optionally
/// bracketed (IPv6), optionally with a port. Obfuscated identifiers
/// (`_hidden`) and `unknown` parse as nothing.
fn parse_forwarded_node(value: &str) -> Option<IpAddr> {
    let value = value.trim().trim_matches('"');
    if let Some(rest) = value.strip_prefix('[') {
        let host = rest.split(']').next()?;
        return host.parse().ok();
    }
    if let Ok(ip) = value.parse::<IpAddr>() {
        return Some(ip);
    }
    // `host:port` for IPv4 only; an unbracketed IPv6 with a port is
    // ambiguous and is not guessed at.
    let (host, port) = value.rsplit_once(':')?;
    port.parse::<u16>().ok()?;
    host.parse::<std::net::Ipv4Addr>().ok().map(IpAddr::V4)
}

/// Rewrite the client's subprotocol offer to what this server can honestly
/// answer.
///
/// The offer is kept only when it contains `actioncable-v1-json`; any other
/// offer is removed, which makes the acceptor complete the handshake *without*
/// a `Sec-WebSocket-Protocol` header instead of rejecting the client or echoing
/// a protocol we do not speak.
fn normalize_subprotocol_offer(req: Request) -> Request {
    let offered: Option<SecWebSocketProtocol> = req.headers().typed_get();

    let keep = offered
        .as_ref()
        .is_some_and(|protocols| protocols.contains(ACTIONCABLE_SUBPROTOCOL).is_some());

    if keep || offered.is_none() {
        return req;
    }

    let (mut parts, body) = req.into_parts();
    parts.headers.remove(rama::http::header::SEC_WEBSOCKET_PROTOCOL);
    debug!("Client offered no supported subprotocol; upgrading without one");
    Request::from_parts(parts, body)
}

/// Copy request headers into the session's metadata map.
fn collect_headers(req: &Request) -> std::collections::HashMap<String, String> {
    req.headers()
        .iter()
        .filter_map(|(name, value)| {
            value.to_str().ok().map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}

/// A plain text response with an explicit status.
fn status(code: StatusCode, body: &'static str) -> Response {
    (code, body).into_response()
}

/// The peer address of the TCP connection carrying this request.
///
/// `None` when the request never came off a socket (unit tests build requests
/// directly), which is why the rate limiter treats a missing address as "not
/// limited" rather than inventing a shared bucket for it.
fn peer_ip(req: &Request) -> Option<IpAddr> {
    req.extensions().get_ref::<SocketInfo>().map(|info| info.peer_addr().ip_addr)
}

/// `429` with the conventional back-off hint.
///
/// `Retry-After: 1` is deliberately coarse: the exact refill instant is this
/// node's business, and a precise value would leak the limiter's state.
fn too_many_requests() -> Response {
    let mut response = status(StatusCode::TOO_MANY_REQUESTS, "connection rate limit");
    response.headers_mut().insert(rama::http::header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

/// The HTTP router: three routes, everything else `404`.
#[derive(Clone)]
struct Router {
    transport: Arc<HttpTransport>,
}

impl Service<Request> for Router {
    type Output = Response;
    type Error = Infallible;

    async fn serve(&self, req: Request<Body>) -> Result<Self::Output, Self::Error> {
        let path = req.uri().path_or_root().into_owned();

        if req.method() != Method::GET {
            return Ok(status(StatusCode::NOT_FOUND, "not found"));
        }

        Ok(match path.as_str() {
            "/cable" => self.transport.serve_cable(req).await,
            // Liveness: the process is running. Never reflects dependencies.
            "/healthz" => status(StatusCode::OK, "ok"),
            // Readiness: dependencies usable and not draining. The body names
            // each component, so a 503 says *what* is not ready.
            "/readyz" => {
                let readiness = self
                    .transport
                    .connections
                    .backend()
                    .readiness(self.transport.shutdown.is_triggered());
                let code =
                    if readiness.ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
                let body = serde_json::to_string(&readiness)
                    .expect("readiness is a plain struct of strings and booleans");
                Response::builder()
                    .status(code)
                    .header(rama::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .expect("static response parts are valid")
            }
            // Opt-in only, and unauthenticated when it is on: a node that has
            // not asked for this has no such route to probe.
            "/metrics" if self.transport.policy.metrics_public => {
                let body = self.transport.connections.backend().render_metrics();
                Response::builder()
                    .status(StatusCode::OK)
                    .header(rama::http::header::CONTENT_TYPE, crate::metrics::CONTENT_TYPE)
                    .body(Body::from(body))
                    .expect("static response parts are valid")
            }
            _ => status(StatusCode::NOT_FOUND, "not found"),
        })
    }
}

#[cfg(test)]
mod tests;
