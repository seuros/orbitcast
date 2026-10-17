//! WebSocket session management
//!
//! A session represents a single WebSocket connection.
//!
//! A session holds connection-scoped metadata only. Subscription memberships
//! (`identifier -> streams` and the reverse index) are owned exclusively by
//! [`crate::hub::Hub`]; duplicating them here is what previously let stream
//! bookkeeping drift out of sync with delivery. Presence grants live with the
//! Rails-dispatch session task that received them (`crate::presence`).

use std::collections::HashMap;

/// WebSocket session
#[derive(Debug)]
pub struct Session {
    /// Unique connection ID (from Mothership)
    pub conn_id: u32,
    /// Request path (e.g., "/cable")
    pub path: String,
    /// Identifier (set after authentication)
    pub identifier: Option<String>,
    /// Whisper streams per subscription identifier
    pub whisper_streams: HashMap<String, String>,
}

impl Session {
    /// Create a new session for an accepted connection.
    ///
    /// Only what the hub needs: Rails-dispatch mode keeps the request's
    /// address and headers with its own session task, for the RPC calls.
    #[must_use]
    pub fn new(conn_id: u32, path: String) -> Self {
        Self { conn_id, path, identifier: None, whisper_streams: HashMap::new() }
    }

    /// Set session identifier (after authentication)
    pub fn set_identifier(&mut self, identifier: String) {
        self.identifier = Some(identifier);
    }

    /// Set whisper stream for a subscription identifier
    pub fn set_whisper_stream(&mut self, identifier: &str, stream: String) {
        self.whisper_streams.insert(identifier.to_string(), stream);
    }

    /// Get whisper stream for a subscription identifier
    #[must_use]
    pub fn get_whisper_stream(&self, identifier: &str) -> Option<&str> {
        self.whisper_streams.get(identifier).map(String::as_str)
    }

    /// Remove whisper stream for a subscription identifier
    pub fn remove_whisper_stream(&mut self, identifier: &str) -> Option<String> {
        self.whisper_streams.remove(identifier)
    }
}
