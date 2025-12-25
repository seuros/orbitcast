//! WebSocket session management
//!
//! A session represents a single WebSocket connection.

use std::collections::{HashMap, HashSet};

/// State key for presence stream (matches AnyCable's PRESENCE_STREAM_STATE)
pub const PRESENCE_STREAM_STATE: &str = "$p";

/// State key for whisper stream (matches AnyCable's WHISPER_STREAM_STATE)
pub const WHISPER_STREAM_STATE: &str = "$w";

/// WebSocket session
#[derive(Debug)]
#[allow(dead_code)]
pub struct Session {
    /// Unique connection ID (from Mothership)
    pub conn_id: u32,
    /// Request path (e.g., "/cable")
    pub path: String,
    /// Remote address
    pub remote_addr: String,
    /// Request headers
    pub headers: HashMap<String, String>,
    /// Identifier (set after authentication)
    pub identifier: Option<String>,
    /// Connection identifiers (set after authentication)
    pub connection_identifiers: Option<String>,
    /// Subscribed identifiers
    pub subscriptions: HashSet<String>,
    /// Subscribed streams
    pub streams: HashSet<String>,
    /// Streams per subscription identifier
    pub subscription_streams: HashMap<String, HashSet<String>>,
    /// Connection state from RPC
    pub cstate: HashMap<String, String>,
    /// Subscription state from RPC (identifier -> state map)
    pub istate: HashMap<String, HashMap<String, String>>,
    /// Presence streams per subscription identifier
    pub presence_streams: HashMap<String, String>,
    /// Whisper streams per subscription identifier
    pub whisper_streams: HashMap<String, String>,
}

#[allow(dead_code)]
impl Session {
    /// Create a new session from a Boarding message
    pub fn new(
        conn_id: u32,
        path: String,
        remote_addr: String,
        headers: HashMap<String, String>,
    ) -> Self {
        Self {
            conn_id,
            path,
            remote_addr,
            headers,
            identifier: None,
            connection_identifiers: None,
            subscriptions: HashSet::new(),
            streams: HashSet::new(),
            subscription_streams: HashMap::new(),
            cstate: HashMap::new(),
            istate: HashMap::new(),
            presence_streams: HashMap::new(),
            whisper_streams: HashMap::new(),
        }
    }

    /// Check if session is authenticated
    pub fn is_authenticated(&self) -> bool {
        self.connection_identifiers.is_some()
    }

    /// Set connection identifiers (after authentication)
    pub fn set_connection_identifiers(&mut self, identifiers: String) {
        self.connection_identifiers = Some(identifiers);
    }

    /// Set session identifier (after authentication)
    pub fn set_identifier(&mut self, identifier: String) {
        self.identifier = Some(identifier);
    }

    /// Subscribe to an identifier
    pub fn subscribe(&mut self, identifier: String) {
        self.subscriptions.insert(identifier);
    }

    /// Unsubscribe from an identifier
    pub fn unsubscribe(&mut self, identifier: &str) -> bool {
        self.subscriptions.remove(identifier)
    }

    /// Subscribe to a stream
    pub fn subscribe_stream(&mut self, stream: String) {
        self.streams.insert(stream);
    }

    /// Unsubscribe from a stream
    pub fn unsubscribe_stream(&mut self, stream: &str) -> bool {
        self.streams.remove(stream)
    }

    /// Check if subscribed to a stream
    pub fn is_subscribed_to_stream(&self, stream: &str) -> bool {
        self.streams.contains(stream)
    }

    /// Update connection state from RPC
    pub fn set_cstate(&mut self, cstate: HashMap<String, String>) {
        self.cstate = cstate;
    }

    /// Update subscription state from RPC
    pub fn set_istate(&mut self, identifier: &str, istate: HashMap<String, String>) {
        self.istate.insert(identifier.to_string(), istate);
    }

    /// Get subscription state for RPC
    pub fn istate_for(&self, identifier: &str) -> HashMap<String, String> {
        self.istate
            .get(identifier)
            .cloned()
            .unwrap_or_else(HashMap::new)
    }

    /// Encode subscription state map for disconnect request
    pub fn istate_encoded(&self) -> HashMap<String, String> {
        let mut encoded = HashMap::new();
        for (identifier, state) in &self.istate {
            if let Ok(json) = serde_json::to_string(state) {
                encoded.insert(identifier.clone(), json);
            }
        }
        encoded
    }

    /// Add streams for a subscription identifier, returning newly-added streams
    pub fn add_subscription_streams(
        &mut self,
        identifier: &str,
        streams: &[String],
    ) -> Vec<String> {
        let entry = self
            .subscription_streams
            .entry(identifier.to_string())
            .or_default();

        let mut added = Vec::new();
        for stream in streams {
            if entry.insert(stream.clone()) && self.streams.insert(stream.clone()) {
                added.push(stream.clone());
            }
        }

        added
    }

    /// Remove streams for a subscription identifier, returning fully-removed streams
    pub fn remove_subscription_streams(
        &mut self,
        identifier: &str,
        streams: &[String],
    ) -> Vec<String> {
        let mut candidates = Vec::new();

        if let Some(entry) = self.subscription_streams.get_mut(identifier) {
            for stream in streams {
                if entry.remove(stream) {
                    candidates.push(stream.clone());
                }
            }

            if entry.is_empty() {
                self.subscription_streams.remove(identifier);
            }
        }

        self.prune_streams(candidates)
    }

    /// Remove all streams for a subscription identifier, returning fully-removed streams
    pub fn remove_all_subscription_streams(&mut self, identifier: &str) -> Vec<String> {
        let candidates = self
            .subscription_streams
            .remove(identifier)
            .map(|streams| streams.into_iter().collect())
            .unwrap_or_default();

        self.prune_streams(candidates)
    }

    fn prune_streams(&mut self, streams: Vec<String>) -> Vec<String> {
        let mut removed = Vec::new();
        for stream in streams {
            let still_used = self
                .subscription_streams
                .values()
                .any(|set| set.contains(&stream));
            if !still_used && self.streams.remove(&stream) {
                removed.push(stream);
            }
        }

        removed
    }

    /// Get header value (case-insensitive)
    pub fn get_header(&self, name: &str) -> Option<&str> {
        let name_lower = name.to_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k.to_lowercase() == name_lower)
            .map(|(_, v)| v.as_str())
    }

    /// Set presence stream for a subscription identifier
    pub fn set_presence_stream(&mut self, identifier: &str, stream: String) {
        self.presence_streams.insert(identifier.to_string(), stream);
    }

    /// Get presence stream for a subscription identifier
    pub fn get_presence_stream(&self, identifier: &str) -> Option<&str> {
        self.presence_streams.get(identifier).map(|s| s.as_str())
    }

    /// Remove presence stream for a subscription identifier
    pub fn remove_presence_stream(&mut self, identifier: &str) -> Option<String> {
        self.presence_streams.remove(identifier)
    }

    /// Get all presence streams (for disconnect cleanup)
    pub fn all_presence_streams(&self) -> Vec<String> {
        self.presence_streams.values().cloned().collect()
    }

    /// Set whisper stream for a subscription identifier
    pub fn set_whisper_stream(&mut self, identifier: &str, stream: String) {
        self.whisper_streams.insert(identifier.to_string(), stream);
    }

    /// Get whisper stream for a subscription identifier
    pub fn get_whisper_stream(&self, identifier: &str) -> Option<&str> {
        self.whisper_streams.get(identifier).map(|s| s.as_str())
    }

    /// Remove whisper stream for a subscription identifier
    pub fn remove_whisper_stream(&mut self, identifier: &str) -> Option<String> {
        self.whisper_streams.remove(identifier)
    }
}
