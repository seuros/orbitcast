//! OrbitCast - Real-time WebSocket server
//!
//! Runs as a ship under Mothership, communicating via Unix socket
//! using the Docking Protocol.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use orbitcast::{
    actioncable::{self, ClientCommand, ServerMessage},
    protocol::{self, MessageType, Moored},
    Config, Hub, PubSub, Session,
};

#[cfg(feature = "postgres")]
use orbitcast::PostgresPubSub;

#[cfg(feature = "memory")]
use orbitcast::MemoryPubSub;

#[derive(Parser, Debug)]
#[command(name = "orbitcast")]
#[command(about = "Real-time WebSocket server with ActionCable protocol")]
#[command(version)]
struct Args {
    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
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

    // Initialize pub/sub backend
    #[cfg(feature = "postgres")]
    let pubsub: Option<Arc<PostgresPubSub>> = if let Some(ref db_url) = config.database_url {
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
    };

    #[cfg(feature = "memory")]
    let pubsub: Option<Arc<MemoryPubSub>> = {
        info!("Memory pub/sub initialized (single-node only)");
        Some(Arc::new(MemoryPubSub::new()))
    };

    // Now split stream for bidirectional communication
    let (mut reader, mut writer) = stream.into_split();

    // Channel for outgoing cargo
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<protocol::Cargo>(1024);

    // Create hub
    let hub = Arc::new(Hub::new(outgoing_tx));

    // Spawn pub/sub listener if available
    if let Some(ref ps) = pubsub {
        let ps_clone = ps.clone();
        let hub_clone = hub.clone();
        tokio::spawn(async move {
            info!("Starting PostgreSQL pub/sub listener");
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
        while let Some(cargo) = outgoing_rx.recv().await {
            let encoded = protocol::encode_cargo(&cargo);

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
                    error!(error = %e, "Failed to decode header");
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

                    let session = Session::new(
                        boarding.conn_id,
                        boarding.path,
                        boarding.remote_addr,
                        boarding.headers,
                    );
                    let conn_id = session.conn_id;
                    hub.add_session(session);

                    // Send ActionCable welcome message
                    let welcome = ServerMessage::Welcome {
                        sid: conn_id.to_string(),
                    };
                    hub.send(conn_id, &actioncable::encode(&welcome)).await;
                    debug!(conn_id, "Sent welcome");
                }

                MessageType::Cargo => {
                    let cargo = protocol::decode_cargo(payload)?;
                    let conn_id = cargo.conn_id;
                    debug!(conn_id, len = cargo.data.len(), "Received cargo");

                    // Parse ActionCable command
                    match actioncable::parse_command(&cargo.data) {
                        Ok(ClientCommand::Subscribe { identifier }) => {
                            debug!(conn_id, identifier = %identifier, "Subscribe");

                            // Subscribe to stream (no backend RPC for MVP)
                            hub.subscribe_to_stream(conn_id, &identifier);

                            // Subscribe pubsub to this channel for cross-node broadcasts
                            if let Some(ref ps) = pubsub {
                                if let Err(e) = ps.subscribe(&identifier).await {
                                    warn!(
                                        identifier = %identifier,
                                        error = %e,
                                        "Failed to subscribe pubsub"
                                    );
                                }
                            }

                            // Send confirmation
                            let confirm = ServerMessage::ConfirmSubscription {
                                identifier: identifier.clone(),
                            };
                            hub.send(conn_id, &actioncable::encode(&confirm)).await;
                            debug!(conn_id, identifier = %identifier, "Confirmed subscription");
                        }

                        Ok(ClientCommand::Unsubscribe { identifier }) => {
                            debug!(conn_id, identifier = %identifier, "Unsubscribe");
                            hub.unsubscribe_from_stream(conn_id, &identifier);

                            // Unsubscribe from PostgreSQL if no more local subscribers
                            if hub.stream_subscriber_count(&identifier) == 0 {
                                if let Some(ref ps) = pubsub {
                                    let _ = ps.unsubscribe(&identifier).await;
                                    debug!(
                                        identifier = %identifier,
                                        "Unsubscribed from pubsub (no subscribers)"
                                    );
                                }
                            }
                        }

                        Ok(ClientCommand::Message { identifier, data }) => {
                            debug!(
                                conn_id,
                                identifier = %identifier,
                                data_len = data.len(),
                                "Message"
                            );
                            // TODO: RPC to backend for processing
                            // For now, broadcast to stream subscribers
                            if let Ok(message) = serde_json::from_str::<serde_json::Value>(&data) {
                                let broadcast = ServerMessage::Message {
                                    identifier: identifier.clone(),
                                    message,
                                };
                                hub.broadcast(&identifier, &actioncable::encode(&broadcast));
                            }
                        }

                        Err(e) => {
                            warn!(conn_id, error = %e, "Invalid ActionCable command");
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
