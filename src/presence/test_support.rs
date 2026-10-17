//! Fixtures shared by the presence store tests and the server's presence
//! reducer tests.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::presence::{PresenceError, PresencePublisher};
use crate::protocol::Envelope as _;
use crate::protocol::v1::presence::{PresenceEvent, PresenceMessage};

// MOCKED: the pub/sub side of a presence store. Announcements are recorded
// instead of published; nothing is delivered to any hub.
#[derive(Debug, Default)]
pub struct RecordingPublisher {
    published: Mutex<Vec<(String, String)>>,
}

#[async_trait]
impl PresencePublisher for RecordingPublisher {
    async fn publish_presence(
        &self,
        stream_key: &str,
        json_payload: &str,
    ) -> Result<(), PresenceError> {
        self.published
            .lock()
            .expect("recorder poisoned")
            .push((stream_key.to_string(), json_payload.to_string()));
        Ok(())
    }
}

impl RecordingPublisher {
    /// Every announcement, decoded and validated as a v1 presence message.
    pub fn messages(&self) -> Vec<(String, PresenceMessage)> {
        self.published
            .lock()
            .expect("recorder poisoned")
            .iter()
            .map(|(key, json)| {
                let message: PresenceMessage =
                    serde_json::from_str(json).expect("an announcement is a v1 message");
                message.validate().expect("an announcement is valid");
                (key.clone(), message)
            })
            .collect()
    }

    /// The announcements of one event kind, in order.
    pub fn events(&self, event: PresenceEvent) -> Vec<PresenceMessage> {
        self.messages()
            .into_iter()
            .filter(|(_, message)| message.event == event)
            .map(|(_, message)| message)
            .collect()
    }

    /// The event kinds announced so far, in order.
    pub fn event_kinds(&self) -> Vec<PresenceEvent> {
        self.messages().into_iter().map(|(_, message)| message.event).collect()
    }
}
