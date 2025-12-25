//! Presence tracking for OrbitCast
//!
//! Tracks online users per stream with TTL-based expiration.
//! Compatible with AnyCable's presence protocol.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::hub::Hub;

/// Default presence TTL in seconds
pub const DEFAULT_PRESENCE_TTL: u64 = 15;

/// Presence event types
pub const PRESENCE_JOIN: &str = "join";
pub const PRESENCE_LEAVE: &str = "leave";
pub const PRESENCE_INFO: &str = "info";

/// A presence record for a user in a stream
#[derive(Debug, Clone)]
pub struct PresenceRecord {
    /// Presence ID (user identifier, not session ID)
    pub id: String,
    /// User-provided info (name, avatar, etc.)
    pub info: serde_json::Value,
    /// When this record expires
    pub expires_at: Instant,
    /// Session IDs that have joined with this presence ID
    pub sessions: Vec<String>,
}

/// Presence event to broadcast
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<serde_json::Value>,
}

/// Presence info response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceInfo {
    #[serde(rename = "type")]
    pub info_type: String,
    pub total: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records: Option<Vec<PresenceInfoRecord>>,
}

/// A record in presence info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceInfoRecord {
    pub id: String,
    pub info: serde_json::Value,
}

/// Presence store for a single OrbitCast node
///
/// Stores presence records per stream with automatic expiration.
/// When a user joins/leaves, events are broadcast to all stream subscribers.
pub struct PresenceStore {
    /// stream -> (presence_id -> PresenceRecord)
    records: DashMap<String, HashMap<String, PresenceRecord>>,
    /// TTL for presence records
    ttl: Duration,
}

impl PresenceStore {
    /// Create a new presence store with the given TTL
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            records: DashMap::new(),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    /// Add a presence record. Returns Some(event) if this is a new join.
    pub fn join(
        &self,
        stream: &str,
        session_id: &str,
        presence_id: &str,
        info: serde_json::Value,
    ) -> Option<PresenceEvent> {
        let expires_at = Instant::now() + self.ttl;
        let mut stream_records = self.records.entry(stream.to_string()).or_default();

        if let Some(record) = stream_records.get_mut(presence_id) {
            // Existing presence - add session if not already present
            if !record.sessions.contains(&session_id.to_string()) {
                record.sessions.push(session_id.to_string());
            }
            // Refresh expiration
            record.expires_at = expires_at;
            // Update info
            record.info = info;
            debug!(stream, presence_id, session_id, "presence refreshed");
            None // Not a new join
        } else {
            // New presence
            stream_records.insert(
                presence_id.to_string(),
                PresenceRecord {
                    id: presence_id.to_string(),
                    info: info.clone(),
                    expires_at,
                    sessions: vec![session_id.to_string()],
                },
            );
            debug!(stream, presence_id, session_id, "presence joined");
            Some(PresenceEvent {
                event_type: PRESENCE_JOIN.to_string(),
                id: presence_id.to_string(),
                info: Some(info),
            })
        }
    }

    /// Remove a presence record. Returns Some(event) if this was the last session.
    pub fn leave(&self, stream: &str, session_id: &str) -> Option<PresenceEvent> {
        let mut stream_records = self.records.get_mut(stream)?;

        // Find the record containing this session
        let mut to_remove = None;
        for (presence_id, record) in stream_records.iter_mut() {
            if let Some(pos) = record.sessions.iter().position(|s| s == session_id) {
                record.sessions.remove(pos);
                if record.sessions.is_empty() {
                    to_remove = Some((
                        presence_id.clone(),
                        PresenceEvent {
                            event_type: PRESENCE_LEAVE.to_string(),
                            id: presence_id.clone(),
                            info: None,
                        },
                    ));
                }
                break;
            }
        }

        if let Some((presence_id, event)) = to_remove {
            stream_records.remove(&presence_id);
            debug!(stream, presence_id, session_id, "presence left");
            return Some(event);
        }

        None
    }

    /// Get presence info for a stream
    pub fn info(&self, stream: &str) -> PresenceInfo {
        let stream_records = match self.records.get(stream) {
            Some(r) => r,
            None => {
                return PresenceInfo {
                    info_type: PRESENCE_INFO.to_string(),
                    total: 0,
                    records: Some(vec![]),
                }
            }
        };

        let now = Instant::now();
        let records: Vec<PresenceInfoRecord> = stream_records
            .values()
            .filter(|r| r.expires_at > now)
            .map(|r| PresenceInfoRecord {
                id: r.id.clone(),
                info: r.info.clone(),
            })
            .collect();

        PresenceInfo {
            info_type: PRESENCE_INFO.to_string(),
            total: records.len(),
            records: Some(records),
        }
    }

    /// Touch a session to refresh its TTL
    pub fn touch(&self, session_id: &str) {
        let expires_at = Instant::now() + self.ttl;

        for mut entry in self.records.iter_mut() {
            for record in entry.value_mut().values_mut() {
                if record.sessions.contains(&session_id.to_string()) {
                    record.expires_at = expires_at;
                }
            }
        }
    }

    /// Remove expired presence records, returning leave events
    pub fn expire(&self) -> Vec<(String, PresenceEvent)> {
        let now = Instant::now();
        let mut events = Vec::new();

        for mut entry in self.records.iter_mut() {
            let stream = entry.key().clone();
            let records = entry.value_mut();

            let expired: Vec<String> = records
                .iter()
                .filter(|(_, r)| r.expires_at <= now)
                .map(|(id, _)| id.clone())
                .collect();

            for presence_id in expired {
                records.remove(&presence_id);
                events.push((
                    stream.clone(),
                    PresenceEvent {
                        event_type: PRESENCE_LEAVE.to_string(),
                        id: presence_id,
                        info: None,
                    },
                ));
            }
        }

        events
    }

    /// Remove all presence records for a session (on disconnect)
    pub fn remove_session(&self, session_id: &str) -> Vec<(String, PresenceEvent)> {
        let mut events = Vec::new();

        for mut entry in self.records.iter_mut() {
            let stream = entry.key().clone();
            let records = entry.value_mut();

            let mut to_remove = Vec::new();
            for (presence_id, record) in records.iter_mut() {
                if let Some(pos) = record.sessions.iter().position(|s| s == session_id) {
                    record.sessions.remove(pos);
                    if record.sessions.is_empty() {
                        to_remove.push(presence_id.clone());
                    }
                }
            }

            for presence_id in to_remove {
                records.remove(&presence_id);
                events.push((
                    stream.clone(),
                    PresenceEvent {
                        event_type: PRESENCE_LEAVE.to_string(),
                        id: presence_id,
                        info: None,
                    },
                ));
            }
        }

        events
    }
}

impl Default for PresenceStore {
    fn default() -> Self {
        Self::new(DEFAULT_PRESENCE_TTL)
    }
}

/// Spawn the presence expiration task
pub fn spawn_expiration_task(
    presence: Arc<PresenceStore>,
    hub: Arc<Hub>,
    check_interval: Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(check_interval);
        loop {
            interval.tick().await;
            let events = presence.expire();
            for (stream, event) in events {
                broadcast_presence_event(&hub, &stream, &event).await;
            }
        }
    });
}

/// Broadcast a presence event to all subscribers of a stream
pub async fn broadcast_presence_event(hub: &Hub, stream: &str, event: &PresenceEvent) {
    let message = PresenceMessage {
        msg_type: "presence".to_string(),
        identifier: stream.to_string(),
        message: event.clone(),
    };

    if let Ok(payload) = serde_json::to_vec(&message) {
        hub.broadcast(stream, &payload);
    } else {
        warn!(stream, "failed to serialize presence event");
    }
}

#[derive(Serialize)]
struct PresenceMessage {
    #[serde(rename = "type")]
    msg_type: String,
    identifier: String,
    message: PresenceEvent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_new_presence() {
        let store = PresenceStore::new(60);
        let event = store.join(
            "chat:1",
            "session_1",
            "user_42",
            serde_json::json!({"name": "Marissa"}),
        );

        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.event_type, PRESENCE_JOIN);
        assert_eq!(event.id, "user_42");
    }

    #[test]
    fn test_join_existing_presence() {
        let store = PresenceStore::new(60);

        // First join
        let event1 = store.join(
            "chat:1",
            "session_1",
            "user_42",
            serde_json::json!({"name": "Marissa"}),
        );
        assert!(event1.is_some());

        // Second join (same presence ID, different session)
        let event2 = store.join(
            "chat:1",
            "session_2",
            "user_42",
            serde_json::json!({"name": "Marissa"}),
        );
        assert!(event2.is_none()); // Should not emit join for existing presence
    }

    #[test]
    fn test_leave_last_session() {
        let store = PresenceStore::new(60);

        store.join(
            "chat:1",
            "session_1",
            "user_42",
            serde_json::json!({"name": "Marissa"}),
        );

        let event = store.leave("chat:1", "session_1");
        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.event_type, PRESENCE_LEAVE);
        assert_eq!(event.id, "user_42");
    }

    #[test]
    fn test_leave_not_last_session() {
        let store = PresenceStore::new(60);

        store.join(
            "chat:1",
            "session_1",
            "user_42",
            serde_json::json!({"name": "Marissa"}),
        );
        store.join(
            "chat:1",
            "session_2",
            "user_42",
            serde_json::json!({"name": "Marissa"}),
        );

        let event = store.leave("chat:1", "session_1");
        assert!(event.is_none()); // Should not emit leave if other sessions remain
    }

    #[test]
    fn test_presence_info() {
        let store = PresenceStore::new(60);

        store.join(
            "chat:1",
            "session_1",
            "user_42",
            serde_json::json!({"name": "Marissa"}),
        );
        store.join(
            "chat:1",
            "session_2",
            "user_13",
            serde_json::json!({"name": "Marissa"}),
        );

        let info = store.info("chat:1");
        assert_eq!(info.total, 2);
        assert_eq!(info.records.as_ref().unwrap().len(), 2);
    }
}
