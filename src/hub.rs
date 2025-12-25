//! Connection Hub
//!
//! Central manager for all WebSocket sessions and stream subscriptions.
//! Uses tokio::sync::broadcast for efficient O(1) fan-out.

use std::collections::HashMap;

use dashmap::DashMap;
use tokio::sync::{broadcast, mpsc};
use tokio::task::AbortHandle;
use tracing::{debug, warn};

use crate::protocol::{Cargo, Outgoing};
use crate::session::Session;

/// Broadcast channel capacity per stream
const STREAM_CAPACITY: usize = 256;

/// Ping broadcast capacity
const PING_CAPACITY: usize = 16;

/// Hub manages all sessions and stream subscriptions
pub struct Hub {
    /// All sessions by connection ID
    sessions: DashMap<u32, Session>,

    /// Stream broadcast senders: stream_name -> broadcast::Sender
    streams: DashMap<String, broadcast::Sender<Vec<u8>>>,

    /// Subscription tasks: conn_id -> (stream -> AbortHandle)
    subscriptions: DashMap<u32, HashMap<String, AbortHandle>>,

    /// Identifier to connection IDs (for multi-device)
    identifiers: DashMap<String, Vec<u32>>,

    /// Channel to send cargo back to Mothership
    outgoing_tx: mpsc::Sender<Outgoing>,

    /// Ping broadcast sender (all sessions receive pings)
    ping_tx: broadcast::Sender<Vec<u8>>,
}

impl Hub {
    /// Create a new Hub
    pub fn new(outgoing_tx: mpsc::Sender<Outgoing>) -> Self {
        let (ping_tx, _) = broadcast::channel(PING_CAPACITY);

        Self {
            sessions: DashMap::new(),
            streams: DashMap::new(),
            subscriptions: DashMap::new(),
            identifiers: DashMap::new(),
            outgoing_tx,
            ping_tx,
        }
    }

    /// Register a new session
    pub fn add_session(&self, session: Session) {
        let conn_id = session.conn_id;
        debug!(conn_id, path = %session.path, "session registered");

        // Initialize subscription map for this session
        self.subscriptions.insert(conn_id, HashMap::new());

        // Subscribe session to ping channel
        self.subscribe_to_pings(conn_id);

        self.sessions.insert(conn_id, session);
    }

    /// Subscribe a session to the ping broadcast
    fn subscribe_to_pings(&self, conn_id: u32) {
        let mut rx = self.ping_tx.subscribe();
        let outgoing_tx = self.outgoing_tx.clone();

        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(payload) => {
                        let cargo = Cargo { conn_id, data: payload };
                        if outgoing_tx.send(Outgoing::Cargo(cargo)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        debug!(conn_id, lagged = n, "ping receiver lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        // Store ping task handle for cleanup on session removal
        if let Some(mut subs) = self.subscriptions.get_mut(&conn_id) {
            subs.insert("__ping__".to_string(), handle.abort_handle());
        }
    }

    /// Remove a session
    pub fn remove_session(&self, conn_id: u32) {
        // Abort all subscription tasks first
        if let Some((_, subs)) = self.subscriptions.remove(&conn_id) {
            for (stream, handle) in subs {
                handle.abort();
                debug!(conn_id, stream, "aborted subscription task");

                // Clean up empty streams
                if let Some(sender) = self.streams.get(&stream)
                    && sender.receiver_count() == 0
                {
                    drop(sender);
                    self.streams.remove(&stream);
                    debug!(stream, "removed empty stream");
                }
            }
        }

        if let Some((_, session)) = self.sessions.remove(&conn_id) {
            // Remove from identifiers
            if let Some(ref identifier) = session.identifier {
                self.identifiers.alter(identifier, |_, mut ids| {
                    ids.retain(|&id| id != conn_id);
                    ids
                });
            }

            debug!(conn_id, "session removed");
        }
    }

    /// Get session by connection ID
    #[allow(dead_code)]
    pub fn get_session(&self, conn_id: u32) -> Option<dashmap::mapref::one::Ref<'_, u32, Session>> {
        self.sessions.get(&conn_id)
    }

    /// Get mutable session by connection ID
    #[allow(dead_code)]
    pub fn get_session_mut(
        &self,
        conn_id: u32,
    ) -> Option<dashmap::mapref::one::RefMut<'_, u32, Session>> {
        self.sessions.get_mut(&conn_id)
    }

    /// Subscribe connection to a stream
    pub fn subscribe_to_stream(&self, conn_id: u32, stream: &str) {
        if let Some(subs) = self.subscriptions.get(&conn_id)
            && subs.contains_key(stream)
        {
            debug!(conn_id, stream, "already subscribed to stream");
            return;
        }

        // Get or create broadcast sender for this stream
        let sender = self
            .streams
            .entry(stream.to_string())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(STREAM_CAPACITY);
                tx
            })
            .clone();

        // Get a receiver and spawn forwarding task
        let mut rx = sender.subscribe();
        let outgoing_tx = self.outgoing_tx.clone();
        let stream_name = stream.to_string();

        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(payload) => {
                        let cargo = Cargo { conn_id, data: payload };
                        if outgoing_tx.send(Outgoing::Cargo(cargo)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(conn_id, stream = %stream_name, lagged = n, "receiver lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        // Store abort handle
        if let Some(mut subs) = self.subscriptions.get_mut(&conn_id) {
            subs.insert(stream.to_string(), handle.abort_handle());
        }

        if let Some(mut session) = self.sessions.get_mut(&conn_id) {
            session.subscribe_stream(stream.to_string());
        }

        debug!(conn_id, stream, "subscribed to stream");
    }

    /// Unsubscribe connection from a stream
    pub fn unsubscribe_from_stream(&self, conn_id: u32, stream: &str) {
        // Abort the forwarding task
        if let Some(mut subs) = self.subscriptions.get_mut(&conn_id)
            && let Some(handle) = subs.remove(stream)
        {
            handle.abort();
        }

        if let Some(mut session) = self.sessions.get_mut(&conn_id) {
            session.unsubscribe_stream(stream);
        }

        // Clean up empty streams
        if let Some(sender) = self.streams.get(stream)
            && sender.receiver_count() == 0
        {
            drop(sender);
            self.streams.remove(stream);
            debug!(stream, "removed empty stream");
        }

        debug!(conn_id, stream, "unsubscribed from stream");
    }

    /// Get subscriber count for a stream
    pub fn stream_subscriber_count(&self, stream: &str) -> usize {
        self.streams
            .get(stream)
            .map(|s| s.receiver_count())
            .unwrap_or(0)
    }

    /// Broadcast message to all subscribers of a stream (O(1) fan-out)
    pub fn broadcast(&self, stream: &str, payload: &[u8]) {
        if let Some(sender) = self.streams.get(stream) {
            match sender.send(payload.to_vec()) {
                Ok(n) => {
                    debug!(stream, receivers = n, "broadcast sent");
                }
                Err(_) => {
                    debug!(stream, "no receivers for broadcast");
                }
            }
        }
    }

    /// Broadcast message to all subscribers of a stream, excluding sender
    /// Used for whisper messages where the sender should not receive their own message
    pub async fn broadcast_excluding(&self, stream: &str, payload: &[u8], exclude_conn_id: u32) {
        let receivers: Vec<u32> = self
            .subscriptions
            .iter()
            .filter(|entry| {
                *entry.key() != exclude_conn_id && entry.value().contains_key(stream)
            })
            .map(|entry| *entry.key())
            .collect();

        for conn_id in &receivers {
            let cargo = Cargo {
                conn_id: *conn_id,
                data: payload.to_vec(),
            };
            let _ = self.outgoing_tx.send(Outgoing::Cargo(cargo)).await;
        }

        debug!(
            stream,
            receivers = receivers.len(),
            excluded = exclude_conn_id,
            "broadcast_excluding sent"
        );
    }

    /// Broadcast ping to all sessions
    pub fn broadcast_ping(&self, payload: &[u8]) {
        match self.ping_tx.send(payload.to_vec()) {
            Ok(n) => {
                debug!(receivers = n, "ping broadcast sent");
            }
            Err(_) => {
                debug!("no receivers for ping");
            }
        }
    }

    /// Send message to a specific connection
    pub async fn send(&self, conn_id: u32, payload: &[u8]) {
        let cargo = Cargo {
            conn_id,
            data: payload.to_vec(),
        };

        if let Err(e) = self.outgoing_tx.send(Outgoing::Cargo(cargo)).await {
            warn!(conn_id, error = %e, "failed to send cargo");
        }
    }

    /// Disconnect a specific connection
    pub async fn disconnect(&self, conn_id: u32, code: u16, reason: &str) {
        let disembark = crate::protocol::Disembark {
            conn_id,
            code,
            reason: reason.to_string(),
        };

        if let Err(e) = self.outgoing_tx.send(Outgoing::Disembark(disembark)).await {
            warn!(conn_id, error = %e, "failed to send disembark");
        }
    }

    /// Set identifier for a session (after authentication)
    #[allow(dead_code)]
    pub fn set_identifier(&self, conn_id: u32, identifier: String) {
        if let Some(mut session) = self.sessions.get_mut(&conn_id) {
            session.set_identifier(identifier.clone());
        }

        self.identifiers
            .entry(identifier)
            .or_default()
            .push(conn_id);
    }

    /// Get all connection IDs for an identifier
    #[allow(dead_code)]
    pub fn get_connections_by_identifier(&self, identifier: &str) -> Vec<u32> {
        self.identifiers
            .get(identifier)
            .map(|entry| entry.clone())
            .unwrap_or_default()
    }

    /// Get session count
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Get stream count
    #[allow(dead_code)]
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Get ping receiver count
    #[allow(dead_code)]
    pub fn ping_receiver_count(&self) -> usize {
        self.ping_tx.receiver_count()
    }
}

impl std::fmt::Debug for Hub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hub")
            .field("sessions", &self.sessions.len())
            .field("streams", &self.streams.len())
            .field("identifiers", &self.identifiers.len())
            .finish()
    }
}
