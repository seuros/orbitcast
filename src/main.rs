//! OrbitCast - Real-time WebSocket server
//!
//! Runs as a ship under Mothership, communicating via Unix socket
//! using the Docking Protocol.

// Compile-time feature validation: exactly one pub/sub backend required
#[cfg(not(any(feature = "postgres", feature = "memory")))]
compile_error!(
    "OrbitCast requires a pub/sub backend. Enable: --features postgres OR --features memory"
);
#[cfg(all(feature = "postgres", feature = "memory"))]
compile_error!(
    "Only one pub/sub backend can be enabled. Use --no-default-features --features memory."
);

mod actioncable;
mod config;
mod hub;
mod protocol;
mod pubsub;
mod rpc;
mod rpc_client;
mod session;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::{
    actioncable::{ClientCommand, ServerMessage},
    config::Config,
    hub::Hub,
    protocol::{MessageType, Moored, Outgoing},
    pubsub::PubSub,
    rpc::anycable::{CommandMessage, Env, Status},
    rpc_client::AnyCableRpc,
    session::Session,
};

#[cfg(feature = "postgres")]
use crate::pubsub::PostgresPubSub;

#[cfg(feature = "memory")]
use crate::pubsub::MemoryPubSub;

#[cfg(feature = "postgres")]
type PubSubImpl = PostgresPubSub;
#[cfg(feature = "memory")]
type PubSubImpl = MemoryPubSub;

#[derive(Parser, Debug)]
#[command(name = "orbitcast")]
#[command(about = "Real-time WebSocket server with ActionCable protocol")]
#[command(version)]
struct Args {
    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

async fn init_pubsub(config: &Config) -> Option<Arc<PubSubImpl>> {
    #[cfg(feature = "memory")]
    {
        let _ = config;
        info!("Memory pub/sub initialized (single-node only)");
        return Some(Arc::new(MemoryPubSub::new()));
    }

    #[cfg(all(not(feature = "memory"), feature = "postgres"))]
    {
        if let Some(ref db_url) = config.database_url {
            match PostgresPubSub::new(db_url).await {
                Ok(ps) => {
                    info!("PostgreSQL pub/sub connected");
                    Some(Arc::new(ps))
                }
                Err(e) => {
                    error!(error = %e, "Failed to connect to PostgreSQL pub/sub");
                    None
                }
            }
        } else {
            warn!("No database_url in config - pub/sub disabled");
            None
        }
    }

    #[cfg(all(not(feature = "memory"), not(feature = "postgres")))]
    {
        let _ = config;
        None
    }
}

fn filter_headers(
    headers: &HashMap<String, String>,
    allowlist: &[String],
) -> HashMap<String, String> {
    if allowlist.is_empty() {
        return headers
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v.clone()))
            .collect();
    }

    let allowed: std::collections::HashSet<String> = allowlist
        .iter()
        .map(|h| h.to_lowercase())
        .collect();

    headers
        .iter()
        .filter(|(k, _)| allowed.contains(&k.to_lowercase()))
        .map(|(k, v)| (k.to_lowercase(), v.clone()))
        .collect()
}

fn header_value<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    let name_lower = name.to_lowercase();
    headers
        .iter()
        .find(|(k, _)| k.to_lowercase() == name_lower)
        .map(|(_, v)| v.as_str())
}

fn build_request_url(path: &str, headers: &HashMap<String, String>) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        return path.to_string();
    }

    let scheme = header_value(headers, "x-forwarded-proto")
        .or_else(|| header_value(headers, "x-forwarded-scheme"))
        .unwrap_or("http");

    let host = header_value(headers, "x-forwarded-host")
        .or_else(|| header_value(headers, "host"))
        .unwrap_or("localhost");

    format!("{}://{}{}", scheme, host, path)
}

fn build_env(
    path: &str,
    headers: &HashMap<String, String>,
    allowlist: &[String],
    cstate: HashMap<String, String>,
    istate: HashMap<String, String>,
) -> Env {
    Env {
        url: build_request_url(path, headers),
        headers: filter_headers(headers, allowlist),
        cstate,
        istate,
    }
}

fn status_from(code: i32) -> Status {
    Status::try_from(code).unwrap_or(Status::Error)
}

async fn send_transmissions(hub: &Hub, conn_id: u32, transmissions: Vec<String>) {
    for payload in transmissions {
        hub.send(conn_id, payload.as_bytes()).await;
    }
}

#[derive(Debug, Clone)]
struct SessionSnapshot {
    path: String,
    headers: HashMap<String, String>,
    subscriptions: Vec<String>,
    cstate: HashMap<String, String>,
    istate: HashMap<String, String>,
    identifiers: String,
}

fn snapshot_session(hub: &Hub, conn_id: u32) -> Option<SessionSnapshot> {
    let session = hub.get_session(conn_id)?;
    Some(SessionSnapshot {
        path: session.path.clone(),
        headers: session.headers.clone(),
        subscriptions: session.subscriptions.iter().cloned().collect(),
        cstate: session.cstate.clone(),
        istate: session.istate_encoded(),
        identifiers: session.connection_identifiers.clone().unwrap_or_default(),
    })
}

async fn rpc_disconnect(
    rpc: &AnyCableRpc,
    hub: &Hub,
    conn_id: u32,
    config: &Config,
) {
    let snapshot = match snapshot_session(hub, conn_id) {
        Some(snapshot) => snapshot,
        None => return,
    };

    let env = build_env(
        &snapshot.path,
        &snapshot.headers,
        &config.rpc_headers,
        snapshot.cstate,
        snapshot.istate,
    );

    let request = crate::rpc::anycable::DisconnectRequest {
        identifiers: snapshot.identifiers,
        subscriptions: snapshot.subscriptions,
        env: Some(env),
    };

    match rpc.disconnect(request).await {
        Ok(response) => {
            let status = status_from(response.status);
            if status != Status::Success {
                warn!(status = ?status, error = %response.error_msg, "RPC disconnect returned non-success");
            }
        }
        Err(e) => {
            warn!(error = %e, "RPC disconnect failed");
        }
    }
}

async fn close_connection(
    rpc: &AnyCableRpc,
    hub: &Hub,
    conn_id: u32,
    code: u16,
    reason: &str,
    config: &Config,
) {
    rpc_disconnect(rpc, hub, conn_id, config).await;
    hub.disconnect(conn_id, code, reason).await;
    hub.remove_session(conn_id);
}

/// Perform docking handshake with Mothership
async fn dock<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    stream: &mut S,
    ship_name: &str,
) -> anyhow::Result<Moored> {
    // Send DOCK message
    let dock = protocol::Dock {
        version: protocol::VERSION,
        ship: ship_name.to_string(),
    };
    let encoded = protocol::encode_dock(&dock);
    stream.write_all(&encoded).await?;
    info!(version = protocol::VERSION, ship = %ship_name, "Sent DOCK");

    // Read MOORED response
    let mut buf = vec![0u8; 4096];
    let mut pending = Vec::new();

    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            anyhow::bail!("Connection closed during docking");
        }
        pending.extend_from_slice(&buf[..n]);

        if pending.len() >= 5 {
            let (msg_type, payload_len) = protocol::decode_header(&pending)?;
            let total_len = 5 + payload_len;

            if pending.len() >= total_len {
                if msg_type != MessageType::Moored {
                    anyhow::bail!("Expected MOORED, got {:?}", msg_type);
                }

                let payload = &pending[5..total_len];
                let moored: Moored = serde_json::from_slice(payload)?;

                info!(
                    version = moored.version,
                    config_keys = ?moored.config.keys().collect::<Vec<_>>(),
                    "Docking complete - MOORED"
                );
                return Ok(moored);
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize JSON logging (consistent with Mothership)
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(&args.log_level)
        .init();

    info!("OrbitCast v{}", env!("CARGO_PKG_VERSION"));

    // Load config from environment
    let mut config = match Config::from_env() {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to load config: {}", e);
            error!("OrbitCast must be run as a Mothership ship");
            error!("Required env vars: MS_PID, MS_SHIP, MS_SOCKET_DIR");
            std::process::exit(1);
        }
    };

    info!(
        pid = config.mothership_pid,
        ship = %config.ship_name,
        socket = %config.socket_path().display(),
        "Starting OrbitCast"
    );

    // Create socket directory if needed
    if let Some(parent) = config.socket_path().parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Remove stale socket if exists
    let socket_path = config.socket_path();
    if socket_path.exists() {
        tokio::fs::remove_file(&socket_path).await?;
    }

    // Create Unix socket listener
    let listener = UnixListener::bind(&socket_path)?;
    info!(socket = %socket_path.display(), "Listening for Mothership connection");

    // Accept connection from Mothership
    let (mut stream, _) = listener.accept().await?;
    info!("Mothership connected");

    // Perform docking handshake
    let moored = dock(&mut stream, &config.ship_name).await?;

    // Apply config from Mothership
    config.apply_moored_config(&moored.config);

    let rpc_timeout = config
        .rpc_request_timeout_ms
        .map(Duration::from_millis);
    let rpc = Arc::new(AnyCableRpc::connect(&config.rpc_host, rpc_timeout).await?);
    info!(rpc_host = %config.rpc_host, "AnyCable RPC client ready");

    // Initialize pub/sub backend
    let pubsub = init_pubsub(&config).await;

    // Now split stream for bidirectional communication
    let (mut reader, mut writer) = stream.into_split();

    // Channel for outgoing cargo
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Outgoing>(1024);

    // Create hub
    let hub = Arc::new(Hub::new(outgoing_tx));

    // Spawn pub/sub listener if available
    if let Some(ref ps) = pubsub {
        let ps_clone = ps.clone();
        let hub_clone = hub.clone();
        tokio::spawn(async move {
            info!("Starting pub/sub listener");
            if let Err(e) = ps_clone
                .listen(move |stream, payload| {
                    let hub = hub_clone.clone();
                    // Wrap in message format and broadcast
                    if let Ok(message) = serde_json::from_slice::<serde_json::Value>(&payload) {
                        let broadcast = ServerMessage::Message {
                            identifier: stream.clone(),
                            message,
                        };
                        let encoded = actioncable::encode(&broadcast);
                        // O(1) broadcast via tokio::sync::broadcast
                        hub.broadcast(&stream, &encoded);
                    }
                })
                .await
            {
                error!(error = %e, "Pub/sub listener error");
            }
        });
    }

    // Spawn ping task
    let ping_hub = hub.clone();
    let ping_interval = config.ping_interval;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(ping_interval));
        loop {
            interval.tick().await;
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let ping = ServerMessage::Ping { timestamp };
            let payload = actioncable::encode(&ping);

            // O(1) broadcast ping to all sessions
            ping_hub.broadcast_ping(&payload);
            debug!(timestamp, sessions = ping_hub.session_count(), "Ping sent");
        }
    });

    // Spawn outgoing cargo sender
    let writer_handle = tokio::spawn(async move {
        while let Some(message) = outgoing_rx.recv().await {
            let encoded = protocol::encode_outgoing(&message);

            if let Err(e) = writer.write_all(&encoded).await {
                error!(error = %e, "Failed to write to Mothership");
                break;
            }
        }
    });

    // Read messages from Mothership
    let mut buf = vec![0u8; 64 * 1024]; // 64KB buffer
    let mut pending = Vec::new();

    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) => {
                info!("Mothership disconnected");
                break;
            }
            Ok(n) => n,
            Err(e) => {
                error!(error = %e, "Read error");
                break;
            }
        };

        pending.extend_from_slice(&buf[..n]);

        // Process complete messages
        while pending.len() >= 5 {
            let (msg_type, payload_len) = match protocol::decode_header(&pending) {
                Ok(header) => header,
                Err(e) => {
                    error!(error = %e, "Failed to decode header; dropping pending buffer");
                    pending.clear();
                    break;
                }
            };

            let total_len = 5 + payload_len;
            if pending.len() < total_len {
                break; // Need more data
            }

            let payload = &pending[5..total_len];

            match msg_type {
                MessageType::Boarding => {
                    let boarding: protocol::Boarding = serde_json::from_slice(payload)?;
                    debug!(
                        conn_id = boarding.conn_id,
                        path = %boarding.path,
                        remote = %boarding.remote_addr,
                        "New connection boarding"
                    );

                    let conn_id = boarding.conn_id;
                    let env = build_env(
                        &boarding.path,
                        &boarding.headers,
                        &config.rpc_headers,
                        HashMap::new(),
                        HashMap::new(),
                    );
                    let request = crate::rpc::anycable::ConnectionRequest { env: Some(env) };

                    let response = match rpc.connect_request(request).await {
                        Ok(response) => response,
                        Err(e) => {
                            error!(conn_id, error = %e, "RPC connect failed");
                            hub.disconnect(conn_id, 1011, "rpc_connect_failed").await;
                            continue;
                        }
                    };

                    let status = status_from(response.status);
                    if status != Status::Success {
                        let reason = if response.error_msg.is_empty() {
                            "rpc_rejected"
                        } else {
                            response.error_msg.as_str()
                        };
                        warn!(conn_id, status = ?status, reason, "RPC connect rejected");
                        hub.disconnect(conn_id, 1000, reason).await;
                        continue;
                    }

                    let mut session = Session::new(
                        boarding.conn_id,
                        boarding.path,
                        boarding.remote_addr,
                        boarding.headers,
                    );
                    if !response.identifiers.is_empty() {
                        session.set_connection_identifiers(response.identifiers.clone());
                    }
                    if let Some(env) = response.env {
                        session.set_cstate(env.cstate);
                    }
                    hub.add_session(session);

                    send_transmissions(&hub, conn_id, response.transmissions).await;
                    debug!(conn_id, "Connect transmissions sent");
                }

                MessageType::Cargo => {
                    let cargo = protocol::decode_cargo(payload)?;
                    let conn_id = cargo.conn_id;
                    debug!(conn_id, len = cargo.data.len(), "Received cargo");

                    let parsed = match actioncable::parse_command(&cargo.data) {
                        Ok(command) => command,
                        Err(e) => {
                            warn!(conn_id, error = %e, "Invalid ActionCable command");
                            continue;
                        }
                    };

                    match parsed {
                        ClientCommand::Subscribe { identifier } => {
                            let command = "subscribe";
                            let data = String::new();

                            let (path, headers, cstate, istate, connection_identifiers) =
                                match hub.get_session(conn_id) {
                                    Some(session) => (
                                        session.path.clone(),
                                        session.headers.clone(),
                                        session.cstate.clone(),
                                        session.istate_for(&identifier),
                                        session.connection_identifiers.clone().unwrap_or_default(),
                                    ),
                                    None => {
                                        warn!(conn_id, "Received command for unknown session");
                                        continue;
                                    }
                                };

                            let env = build_env(
                                &path,
                                &headers,
                                &config.rpc_headers,
                                cstate,
                                istate,
                            );

                            let message = CommandMessage {
                                command: command.to_string(),
                                identifier: identifier.clone(),
                                connection_identifiers,
                                data,
                                env: Some(env),
                            };

                            let response = match rpc.command(message).await {
                                Ok(response) => response,
                                Err(e) => {
                                    error!(conn_id, error = %e, "RPC command failed");
                                    close_connection(
                                        &rpc,
                                        &hub,
                                        conn_id,
                                        1011,
                                        "rpc_command_failed",
                                        &config,
                                    )
                                    .await;
                                    continue;
                                }
                            };

                            let status = status_from(response.status);
                            send_transmissions(&hub, conn_id, response.transmissions).await;

                            if response.disconnect || status == Status::Error {
                                let reason = if response.error_msg.is_empty() {
                                    "rpc_disconnect"
                                } else {
                                    response.error_msg.as_str()
                                };
                                close_connection(&rpc, &hub, conn_id, 1000, reason, &config).await;
                                continue;
                            }

                            if status == Status::Success {
                                let (added_streams, removed_streams) = if let Some(mut session) =
                                    hub.get_session_mut(conn_id)
                                {
                                    session.subscribe(identifier.clone());

                                    if let Some(env) = response.env {
                                        session.set_cstate(env.cstate);
                                        if !env.istate.is_empty() {
                                            session.set_istate(&identifier, env.istate);
                                        }
                                    }

                                    let mut removed = Vec::new();
                                    if response.stop_streams {
                                        removed = session.remove_all_subscription_streams(&identifier);
                                    } else if !response.stopped_streams.is_empty() {
                                        removed = session.remove_subscription_streams(
                                            &identifier,
                                            &response.stopped_streams,
                                        );
                                    }

                                    let added = if response.streams.is_empty() {
                                        Vec::new()
                                    } else {
                                        session.add_subscription_streams(&identifier, &response.streams)
                                    };

                                    (added, removed)
                                } else {
                                    (Vec::new(), Vec::new())
                                };

                                for stream in added_streams {
                                    hub.subscribe_to_stream(conn_id, &stream);
                                    if let Some(ref ps) = pubsub
                                        && hub.stream_subscriber_count(&stream) == 1
                                        && let Err(e) = ps.subscribe(&stream).await
                                    {
                                        warn!(
                                            stream = %stream,
                                            error = %e,
                                            "Failed to subscribe pubsub"
                                        );
                                    }
                                }

                                for stream in removed_streams {
                                    hub.unsubscribe_from_stream(conn_id, &stream);
                                    if hub.stream_subscriber_count(&stream) == 0
                                        && let Some(ref ps) = pubsub
                                        && let Err(e) = ps.unsubscribe(&stream).await
                                    {
                                        warn!(
                                            stream = %stream,
                                            error = %e,
                                            "Failed to unsubscribe pubsub"
                                        );
                                    }
                                }
                            }
                        }
                        ClientCommand::Unsubscribe { identifier } => {
                            let command = "unsubscribe";
                            let data = String::new();

                            let (path, headers, cstate, istate, connection_identifiers) =
                                match hub.get_session(conn_id) {
                                    Some(session) => (
                                        session.path.clone(),
                                        session.headers.clone(),
                                        session.cstate.clone(),
                                        session.istate_for(&identifier),
                                        session.connection_identifiers.clone().unwrap_or_default(),
                                    ),
                                    None => {
                                        warn!(conn_id, "Received command for unknown session");
                                        continue;
                                    }
                                };

                            let env = build_env(
                                &path,
                                &headers,
                                &config.rpc_headers,
                                cstate,
                                istate,
                            );

                            let message = CommandMessage {
                                command: command.to_string(),
                                identifier: identifier.clone(),
                                connection_identifiers,
                                data,
                                env: Some(env),
                            };

                            let response = match rpc.command(message).await {
                                Ok(response) => response,
                                Err(e) => {
                                    error!(conn_id, error = %e, "RPC command failed");
                                    close_connection(
                                        &rpc,
                                        &hub,
                                        conn_id,
                                        1011,
                                        "rpc_command_failed",
                                        &config,
                                    )
                                    .await;
                                    continue;
                                }
                            };

                            let status = status_from(response.status);
                            send_transmissions(&hub, conn_id, response.transmissions).await;

                            if response.disconnect || status == Status::Error {
                                let reason = if response.error_msg.is_empty() {
                                    "rpc_disconnect"
                                } else {
                                    response.error_msg.as_str()
                                };
                                close_connection(&rpc, &hub, conn_id, 1000, reason, &config).await;
                                continue;
                            }

                            if status == Status::Success {
                                let (added_streams, removed_streams) = if let Some(mut session) =
                                    hub.get_session_mut(conn_id)
                                {
                                    session.unsubscribe(&identifier);

                                    if let Some(env) = response.env {
                                        session.set_cstate(env.cstate);
                                        if !env.istate.is_empty() {
                                            session.set_istate(&identifier, env.istate);
                                        }
                                    }

                                    let removed = if response.stop_streams {
                                        session.remove_all_subscription_streams(&identifier)
                                    } else if !response.stopped_streams.is_empty() {
                                        session.remove_subscription_streams(
                                            &identifier,
                                            &response.stopped_streams,
                                        )
                                    } else {
                                        session.remove_all_subscription_streams(&identifier)
                                    };

                                    let added = if response.streams.is_empty() {
                                        Vec::new()
                                    } else {
                                        session.add_subscription_streams(&identifier, &response.streams)
                                    };

                                    (added, removed)
                                } else {
                                    (Vec::new(), Vec::new())
                                };

                                for stream in added_streams {
                                    hub.subscribe_to_stream(conn_id, &stream);
                                    if let Some(ref ps) = pubsub
                                        && hub.stream_subscriber_count(&stream) == 1
                                        && let Err(e) = ps.subscribe(&stream).await
                                    {
                                        warn!(
                                            stream = %stream,
                                            error = %e,
                                            "Failed to subscribe pubsub"
                                        );
                                    }
                                }

                                for stream in removed_streams {
                                    hub.unsubscribe_from_stream(conn_id, &stream);
                                    if hub.stream_subscriber_count(&stream) == 0
                                        && let Some(ref ps) = pubsub
                                        && let Err(e) = ps.unsubscribe(&stream).await
                                    {
                                        warn!(
                                            stream = %stream,
                                            error = %e,
                                            "Failed to unsubscribe pubsub"
                                        );
                                    }
                                }
                            }
                        }
                        ClientCommand::Message { identifier, data } => {
                            let command = "message";

                            let (path, headers, cstate, istate, connection_identifiers) =
                                match hub.get_session(conn_id) {
                                    Some(session) => (
                                        session.path.clone(),
                                        session.headers.clone(),
                                        session.cstate.clone(),
                                        session.istate_for(&identifier),
                                        session.connection_identifiers.clone().unwrap_or_default(),
                                    ),
                                    None => {
                                        warn!(conn_id, "Received command for unknown session");
                                        continue;
                                    }
                                };

                            let env = build_env(
                                &path,
                                &headers,
                                &config.rpc_headers,
                                cstate,
                                istate,
                            );

                            let message = CommandMessage {
                                command: command.to_string(),
                                identifier: identifier.clone(),
                                connection_identifiers,
                                data,
                                env: Some(env),
                            };

                            let response = match rpc.command(message).await {
                                Ok(response) => response,
                                Err(e) => {
                                    error!(conn_id, error = %e, "RPC command failed");
                                    close_connection(
                                        &rpc,
                                        &hub,
                                        conn_id,
                                        1011,
                                        "rpc_command_failed",
                                        &config,
                                    )
                                    .await;
                                    continue;
                                }
                            };

                            let status = status_from(response.status);
                            send_transmissions(&hub, conn_id, response.transmissions).await;

                            if response.disconnect || status == Status::Error {
                                let reason = if response.error_msg.is_empty() {
                                    "rpc_disconnect"
                                } else {
                                    response.error_msg.as_str()
                                };
                                close_connection(&rpc, &hub, conn_id, 1000, reason, &config).await;
                                continue;
                            }

                            if status == Status::Success
                                && let Some(mut session) = hub.get_session_mut(conn_id)
                                && let Some(env) = response.env
                            {
                                session.set_cstate(env.cstate);
                                if !env.istate.is_empty() {
                                    session.set_istate(&identifier, env.istate);
                                }
                            }
                        }
                    }
                }

                MessageType::Disembark => {
                    let disembark: protocol::Disembark = serde_json::from_slice(payload)?;
                    debug!(
                        conn_id = disembark.conn_id,
                        code = disembark.code,
                        reason = %disembark.reason,
                        "Connection disembarked"
                    );

                    rpc_disconnect(&rpc, &hub, disembark.conn_id, &config).await;
                    hub.remove_session(disembark.conn_id);
                }

                _ => {
                    warn!(msg_type = ?msg_type, "Unhandled message type");
                }
            }

            pending.drain(..total_len);
        }
    }

    writer_handle.abort();

    // Cleanup socket
    if socket_path.exists() {
        let _ = tokio::fs::remove_file(&socket_path).await;
    }

    info!("OrbitCast shutdown");
    Ok(())
}
