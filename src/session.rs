//! WebSocket session management
//!
//! A session represents a single WebSocket connection.

use std::collections::{HashMap, HashSet};

/// WebSocket session
#[derive(Debug)]
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
    /// Subscribed channels
    pub channels: HashSet<String>,
    /// Subscribed streams
    pub streams: HashSet<String>,
}

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
            channels: HashSet::new(),
            streams: HashSet::new(),
        }
    }

    /// Check if session is authenticated
    pub fn is_authenticated(&self) -> bool {
        self.identifier.is_some()
    }

    /// Set session identifier (after authentication)
    pub fn set_identifier(&mut self, identifier: String) {
        self.identifier = Some(identifier);
    }

    /// Subscribe to a channel
    pub fn subscribe(&mut self, channel: String) {
        self.channels.insert(channel);
    }

    /// Unsubscribe from a channel
    pub fn unsubscribe(&mut self, channel: &str) -> bool {
        self.channels.remove(channel)
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

    /// Get header value (case-insensitive)
    pub fn get_header(&self, name: &str) -> Option<&str> {
        let name_lower = name.to_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k.to_lowercase() == name_lower)
            .map(|(_, v)| v.as_str())
    }
}
