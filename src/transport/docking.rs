//! Docking transport: the Mothership Unix-socket link, plus its frame codec.
//!
//! OrbitCast uses the standard Mothership docking protocol. Its framing and
//! version rules are owned by `mothership-docking-protocol` and are explicitly
//! out of scope for the OrbitCast envelope versioning in `crate::protocol`.
//!
//! The transport itself owns only bytes and connection lifetime: every
//! ActionCable decision (welcome, subscribe authorization, teardown) is made by
//! [`crate::server::Connections`], exactly as in the direct HTTP listener.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::{Config, DockingConfig};
use crate::metrics::Metrics;
use crate::server::{Backend, Connections, Shutdown, ShutdownRx};
use crate::transport::{LinkOutcome, OutboundLink};

pub use mothership_docking_protocol::{
    Boarding, Cargo, CargoKind, Disembark, Dock, MAX_FRAME_PAYLOAD, MessageType, Moored, VERSION,
    decode_cargo, decode_header, encode_cargo, encode_cargo_header, encode_disembark, encode_dock,
};

/// The link queue holds this many per-socket queues' worth of frames
/// (`outbound_queue_messages` × this), never fewer than
/// [`LINK_QUEUE_MIN_FRAMES`]. See [`link_queue_capacity`].
const LINK_QUEUE_FACTOR: usize = 32;

/// Floor on the link queue bound, in frames.
const LINK_QUEUE_MIN_FRAMES: usize = 1024;

/// Room kept free in the read buffer before each read.
const READ_BUFFER_SIZE: usize = 64 * 1024;

/// Frame header: message type (1) + payload length (4).
const HEADER_LEN: usize = 5;

/// Grace period granted to the writer task to flush the disconnect frames
/// queued during shutdown, before the link is torn down.
const DRAIN_FLUSH_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

/// RFC 6455 close code 1001: the server is going away.
const CLOSE_GOING_AWAY: u16 = 1001;

/// One ActionCable text frame addressed to a connection.
///
/// The bytes are shared ([`Bytes`]): a publication fanned out to a thousand
/// recipients with the same identifier is one allocation referenced a thousand
/// times, all the way to the socket writer.
#[derive(Debug, Clone)]
pub struct TextFrame {
    pub conn_id: u32,
    pub data: Bytes,
}

#[derive(Debug)]
pub enum Outgoing {
    /// A text frame for one connection.
    Cargo(TextFrame),
    /// Server-initiated close; emitted by `Hub::disconnect`.
    Disembark(Disembark),
}

/// Bound of one docking link's outbound queue, in frames, for a per-socket
/// bound of `outbound_queue_messages`.
///
/// One Unix socket carries every connection Mothership boards here, so the
/// queue is shared and sized as a multiple of the per-socket bound:
/// `outbound_queue_messages × 32`, at least 1 024 (the default `256` gives
/// 8 192). A fan-out burst to N members puts N frames on it at once, and the
/// link writer drains it at Unix-socket speed; a queue that still fills is
/// the supervisor not keeping up, and the connection whose frame did not
/// fit is closed with a reconnectable `disconnect` (see
/// [`DockingLink::try_frame`]).
#[must_use]
pub const fn link_queue_capacity(outbound_queue_messages: usize) -> usize {
    let scaled = outbound_queue_messages.saturating_mul(LINK_QUEUE_FACTOR);
    if scaled < LINK_QUEUE_MIN_FRAMES { LINK_QUEUE_MIN_FRAMES } else { scaled }
}

/// The hub's outbound link over one docking connection.
///
/// Every connection's frames and closes share one bounded queue, drained in
/// order by the link writer. The queue is the whole link's bound, not one
/// socket's: Mothership applies its own per-socket policy on the far side.
pub struct DockingLink {
    tx: mpsc::Sender<Outgoing>,
    /// Where a frame lost to a full link queue is counted
    /// (`orbitcast_hub_queue_overflow_total`).
    metrics: Arc<Metrics>,
}

impl DockingLink {
    /// A link that writes into `tx`, counting lost frames into `metrics`.
    #[must_use]
    pub const fn new(tx: mpsc::Sender<Outgoing>, metrics: Arc<Metrics>) -> Self {
        Self { tx, metrics }
    }
}

impl std::fmt::Debug for DockingLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DockingLink")
            .field("capacity", &self.tx.max_capacity())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl OutboundLink for DockingLink {
    /// A full link queue loses this frame for `conn_id`. It is counted
    /// (`orbitcast_hub_queue_overflow_total`, a docking-only counter) and
    /// reported as lag, so the hub closes that connection with a
    /// reconnectable `disconnect` rather than leaving a silent gap.
    fn try_frame(&self, conn_id: u32, frame: Bytes) -> LinkOutcome {
        match self.tx.try_send(Outgoing::Cargo(TextFrame { conn_id, data: frame })) {
            Ok(()) => LinkOutcome::Sent,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.record_hub_queue_overflow();
                warn!(conn_id, "docking link queue full; frame lost, closing the connection");
                LinkOutcome::Lagged
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!(conn_id, "docking link closed; frame dropped");
                LinkOutcome::Closed
            }
        }
    }

    async fn frame(&self, conn_id: u32, frame: Bytes) -> LinkOutcome {
        if self.tx.send(Outgoing::Cargo(TextFrame { conn_id, data: frame })).await.is_err() {
            debug!(conn_id, "docking link closed; frame dropped");
            return LinkOutcome::Closed;
        }
        LinkOutcome::Sent
    }

    async fn disconnect(&self, close: Disembark) {
        let conn_id = close.conn_id;
        if self.tx.send(Outgoing::Disembark(close)).await.is_err() {
            debug!(conn_id, "docking link closed; disembark dropped");
        }
    }

    /// The frame and the close go onto the ordered queue at once when it
    /// has room; otherwise a task of its own waits for room, so the close is
    /// delayed by a full queue but never lost to it.
    fn lag_close(&self, disconnect: Bytes, close: Disembark) {
        let conn_id = close.conn_id;
        let frame = Outgoing::Cargo(TextFrame { conn_id, data: disconnect });
        let close = Outgoing::Disembark(close);

        let frame = match self.tx.try_send(frame) {
            Ok(()) => None,
            Err(mpsc::error::TrySendError::Full(frame)) => Some(frame),
            Err(mpsc::error::TrySendError::Closed(_)) => return,
        };
        let close = if frame.is_some() {
            close
        } else {
            match self.tx.try_send(close) {
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => return,
                Err(mpsc::error::TrySendError::Full(close)) => close,
            }
        };

        let tx = self.tx.clone();
        tokio::spawn(async move {
            if let Some(frame) = frame
                && tx.send(frame).await.is_err()
            {
                return;
            }
            if tx.send(close).await.is_err() {
                debug!(conn_id, "docking link closed before the lag close was queued");
            }
        });
    }
}

/// Encode one outbound message as a complete docking frame.
///
/// Copies the frame data into the result: the link writer uses
/// [`write_outgoing`], which writes header and shared data separately.
#[must_use]
pub fn encode_outgoing(message: &Outgoing) -> Vec<u8> {
    match message {
        Outgoing::Cargo(frame) => encode_cargo(&Cargo {
            conn_id: frame.conn_id,
            kind: CargoKind::Text,
            data: frame.data.to_vec(),
        }),
        Outgoing::Disembark(disembark) => encode_disembark(disembark),
    }
}

/// Write one outbound message to the link without copying frame data.
///
/// # Errors
///
/// A frame larger than the docking protocol allows, or the link's write
/// error.
pub async fn write_outgoing<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &Outgoing,
) -> anyhow::Result<()> {
    match message {
        Outgoing::Cargo(frame) => {
            let header = encode_cargo_header(frame.conn_id, CargoKind::Text, frame.data.len())
                .map_err(|e| anyhow::anyhow!("cannot encode cargo header: {e}"))?;
            writer.write_all(&header).await?;
            writer.write_all(&frame.data).await?;
        }
        Outgoing::Disembark(disembark) => {
            writer.write_all(&encode_disembark(disembark)).await?;
        }
    }
    Ok(())
}

/// Bind the ship socket and serve docking sessions until shutdown.
///
/// Each disconnect drops all session state and waits for Mothership to dock
/// again; a fatal error (misconfiguration, protocol desync) returns so the
/// supervisor can relaunch the bay with a clean slate.
///
/// # Errors
///
/// When `base_config` has no docking settings; when the ship socket's
/// directory cannot be created, a stale socket cannot be removed, or the
/// socket cannot be bound or accept; and when a docked session ends fatally:
/// the configuration completed by MOORED fails validation or selects no
/// authorization mode, or the link desyncs.
pub async fn serve(
    base_config: Config,
    backend: Arc<Backend>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    let Some(ref docking) = base_config.docking else {
        anyhow::bail!("docking transport requested without MS_* configuration");
    };

    let socket_path = docking.socket_path();
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Remove a stale socket left by a previous process.
    if socket_path.exists() {
        tokio::fs::remove_file(&socket_path).await?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    info!(socket = %socket_path.display(), "Listening for Mothership connection");

    let mut shutdown_rx = shutdown.subscribe();

    loop {
        tokio::select! {
            () = shutdown_rx.wait() => {
                info!("Docking transport stopped accepting");
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                info!("Mothership connected");
                serve_session(stream, &base_config, docking, &backend, &shutdown).await?;
                if shutdown.is_triggered() {
                    return Ok(());
                }
                info!("Awaiting Mothership re-dock");
            }
        }
    }
}

/// Perform the docking handshake with Mothership, under `deadline`.
///
/// Returns MOORED and whatever followed it in the same reads: Mothership may
/// board a connection right after mooring, and those frames belong to the
/// link reader. A peer that connects and never completes MOORED within the
/// deadline is dropped so the ship socket is not held by a half-open link.
async fn dock<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    ship_name: &str,
    deadline: Duration,
) -> anyhow::Result<(Moored, BytesMut)> {
    tokio::time::timeout(deadline, dock_unbounded(stream, ship_name))
        .await
        .map_err(|_| anyhow::anyhow!("docking handshake did not complete within {deadline:?}"))?
}

async fn dock_unbounded<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    ship_name: &str,
) -> anyhow::Result<(Moored, BytesMut)> {
    let dock = Dock::new(ship_name);
    stream.write_all(&encode_dock(&dock)).await?;
    info!(version = VERSION, ship = %ship_name, "Sent DOCK");

    let mut pending = BytesMut::with_capacity(4096);
    loop {
        if let Some((msg_type, payload)) = take_frame(&mut pending)? {
            if msg_type != MessageType::Moored {
                anyhow::bail!("Expected MOORED, got {msg_type:?}");
            }

            let moored: Moored = serde_json::from_slice(&payload)?;
            info!(
                version = moored.version,
                config_keys = ?moored.config.keys().collect::<Vec<_>>(),
                "Docking complete - MOORED"
            );
            return Ok((moored, pending));
        }
        if stream.read_buf(&mut pending).await? == 0 {
            anyhow::bail!("Connection closed during docking");
        }
    }
}

/// Split one complete frame off `pending`, if it holds one. The payload
/// shares the read buffer: nothing is copied, and the rest of the buffer is
/// never shifted. Frames past [`MAX_FRAME_PAYLOAD`] are refused before any
/// room is reserved for them.
fn take_frame(pending: &mut BytesMut) -> anyhow::Result<Option<(MessageType, Bytes)>> {
    if pending.len() < HEADER_LEN {
        return Ok(None);
    }
    let (msg_type, payload_len) = decode_header(pending)?;
    if payload_len > MAX_FRAME_PAYLOAD {
        anyhow::bail!(
            "{msg_type:?} frame of {payload_len} bytes exceeds the {MAX_FRAME_PAYLOAD}-byte limit"
        );
    }
    let total_len = HEADER_LEN + payload_len;
    if pending.len() < total_len {
        pending.reserve(total_len - pending.len());
        return Ok(None);
    }
    pending.advance(HEADER_LEN);
    Ok(Some((msg_type, pending.split_to(payload_len).freeze())))
}

/// Serve one Mothership connection until the link drops.
///
/// Returns `Ok(())` when the connection ended (caller re-accepts); returns
/// `Err` on fatal misconfiguration or protocol desync (caller exits, letting
/// Mothership relaunch the bay).
async fn serve_session(
    mut stream: tokio::net::UnixStream,
    base_config: &Config,
    docking: &DockingConfig,
    backend: &Arc<Backend>,
    shutdown: &Shutdown,
) -> anyhow::Result<()> {
    // A failed handshake is not fatal — go back to listening.
    let frame_deadline = base_config.http.handshake_timeout;
    let (moored, pending) = match dock(&mut stream, &docking.ship_name, frame_deadline).await {
        Ok(docked) => docked,
        Err(e) => {
            warn!(error = %e, "Docking handshake failed");
            return Ok(());
        }
    };

    let mut config = base_config.clone();
    // A handshake naming a key this build does not know, or carrying a value
    // it cannot use, is a misconfigured supervisor: fatal, never ignored.
    config.apply_moored_config(&moored.config)?;
    config.validate()?;

    if !config.streams.is_enabled() && !config.rpc.is_enabled() {
        anyhow::bail!(
            "an authorization mode is required; configure Rails dispatch \
             (rpc_endpoint_url + ORBITCAST_RPC_TOKEN_BASE64) or signed streams \
             (streams_secret, turbo_streams + turbo_streams_secret, \
             cable_ready_streams + cable_ready_secret, public_streams)"
        );
    }

    if config.rpc.is_enabled() {
        info!("Rails-dispatch mode");
    } else {
        info!("Standalone mode (signed streams only)");
    }

    let (mut reader, mut writer) = stream.into_split();
    let (outgoing_tx, mut outgoing_rx) =
        mpsc::channel::<Outgoing>(link_queue_capacity(config.http.outbound_queue_messages));

    let connections = Connections::new(Arc::clone(backend), &config, outgoing_tx)?;
    let mut tasks = connections.spawn_background_tasks();

    tasks.push(tokio::spawn(async move {
        while let Some(message) = outgoing_rx.recv().await {
            if let Err(e) = write_outgoing(&mut writer, &message).await {
                error!(error = %e, "Failed to write to Mothership");
                break;
            }
        }
    }));

    let mut shutdown_rx = shutdown.subscribe();

    // Read messages from Mothership. The loop is wrapped so that *every* exit
    // path — clean disconnect, read error, protocol desync, shutdown — drains
    // the hub through the same per-connection teardown as an ordinary
    // disembark.
    let link_result = read_link(
        &mut reader,
        pending,
        &connections,
        &mut shutdown_rx,
        LinkDeadlines { shutdown: config.shutdown_timeout, frame: frame_deadline },
    )
    .await;

    connections.drain().await;
    // Background tasks hold their own handles until the guard aborts them.
    drop(connections);
    drop(tasks);

    link_result
}

/// The two deadlines the link reader applies.
#[derive(Clone, Copy)]
struct LinkDeadlines {
    /// Drain budget once shutdown fires.
    shutdown: Duration,
    /// How long the rest of a partially read frame may take to arrive.
    frame: Duration,
}

/// Read and dispatch docking frames until the link ends or shutdown fires.
///
/// `pending` holds bytes already read past MOORED. Frames are read straight
/// into it and split off without copying; a frame header that cannot be
/// decoded, or announces a payload past [`MAX_FRAME_PAYLOAD`], means the link
/// is desynced and ends it as a protocol error. A frame whose remaining bytes
/// do not arrive within the frame deadline ends the link the same way: a
/// half-written frame is a stalled supervisor, not something to wait out.
///
/// Dispatching never blocks on a session: every command goes onto that
/// session's own mailbox, so one slow Rails call or backend acknowledgement
/// delays only the connection that issued it.
async fn read_link<R>(
    reader: &mut R,
    mut pending: BytesMut,
    connections: &Arc<Connections>,
    shutdown_rx: &mut ShutdownRx,
    deadlines: LinkDeadlines,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
{
    loop {
        // Every complete frame already buffered goes out before the next read.
        while let Some((msg_type, payload)) =
            take_frame(&mut pending).inspect_err(|e| error!(error = %e, "Docking link desynced"))?
        {
            dispatch(connections, msg_type, &payload).await?;
        }

        pending.reserve(READ_BUFFER_SIZE);
        // Only a partial frame is on a deadline: an idle link may stay quiet.
        let frame_deadline = if pending.is_empty() { Duration::MAX } else { deadlines.frame };
        tokio::select! {
            () = shutdown_rx.wait() => {
                // Advertise a reconnectable disconnect, ask Mothership to close
                // each socket behind it (same ordered queue, so the disconnect
                // frame always goes first), and give the writer a moment to
                // flush before the link is torn down.
                connections
                    .disconnect_all(crate::server::DISCONNECT_REASON_SERVER_RESTART, true);
                connections.close_all(CLOSE_GOING_AWAY, "going away").await;
                tokio::time::sleep(deadlines.shutdown.min(DRAIN_FLUSH_GRACE)).await;
                info!("Docking link closing for shutdown");
                return Ok(());
            }
            read = tokio::time::timeout(
                frame_deadline,
                reader.read_buf(&mut pending),
            ) => match read {
                Ok(Ok(0)) => {
                    info!("Mothership disconnected");
                    return Ok(());
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    error!(error = %e, "Read error");
                    return Ok(());
                }
                Err(_) => {
                    error!(
                        buffered = pending.len(),
                        deadline_ms =
                            u64::try_from(deadlines.frame.as_millis()).unwrap_or(u64::MAX),
                        "Incomplete docking frame did not complete within the deadline"
                    );
                    anyhow::bail!(
                        "incomplete docking frame exceeded the {:?} deadline",
                        deadlines.frame
                    );
                }
            },
        }
    }
}

/// Apply one decoded docking frame to the connection driver.
async fn dispatch(
    connections: &Arc<Connections>,
    msg_type: MessageType,
    payload: &[u8],
) -> anyhow::Result<()> {
    match msg_type {
        MessageType::Boarding => {
            let boarding: Boarding = serde_json::from_slice(payload)?;
            debug!(
                conn_id = boarding.conn_id,
                path = %boarding.path,
                remote = %boarding.remote_addr,
                "New connection boarding"
            );

            connections
                .accept(boarding.conn_id, boarding.path, boarding.remote_addr, boarding.headers)
                .await;
        }

        MessageType::Cargo => {
            let cargo = decode_cargo(payload)?;
            debug!(
                conn_id = cargo.conn_id,
                kind = ?cargo.kind,
                len = cargo.data.len(),
                "Received cargo"
            );

            // Inbound bytes from the client, forwarded by the supervisor:
            // verified liveness for presence leases. Mothership terminates the
            // WebSocket, so a control pong never reaches this process - a
            // docked connection is refreshable only while the client is
            // actually sending frames (see `crate::presence`).
            connections.record_liveness(cargo.conn_id);

            if cargo.kind != CargoKind::Text {
                debug!(
                    conn_id = cargo.conn_id,
                    kind = ?cargo.kind,
                    len = cargo.data.len(),
                    "Ignoring non-text ActionCable cargo"
                );
                return Ok(());
            }

            // `cargo.data` borrows the read buffer: the client's bytes reach
            // the connection driver without a copy.
            connections.handle_frame(cargo.conn_id, cargo.data).await;
        }

        MessageType::Disembark => {
            let disembark: Disembark = serde_json::from_slice(payload)?;
            debug!(
                conn_id = disembark.conn_id,
                code = disembark.code,
                reason = %disembark.reason,
                "Connection disembarked"
            );

            connections.teardown(disembark.conn_id).await;
        }

        other => warn!(msg_type = ?other, "Unhandled message type"),
    }

    Ok(())
}

#[cfg(test)]
mod tests;
