//! ActionCable Protocol Implementation
//!
//! Wire-compatible with Rails ActionCable clients.
//!
//! ## Client Commands
//! - `subscribe`: Join a channel
//! - `unsubscribe`: Leave a channel
//! - `message`: Send data to a channel
//! - `join`: Declare presence in a channel
//! - `leave`: Remove presence from a channel
//! - `presence`: Query current presence list
//! - `whisper`: Send ephemeral message (excluded from sender)
//!
//! ## Server Messages
//! - `welcome`: Connection established
//! - `ping`: Heartbeat
//! - `confirm_subscription`: Channel joined
//! - `reject_subscription`: Channel rejected
//! - `disconnect`: Connection closing

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Presence data for join command
#[derive(Debug, Clone, Deserialize)]
pub struct PresenceData {
    pub id: String,
    #[serde(default)]
    pub info: Option<Value>,
}

/// Client-to-server commands
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ClientCommand {
    Subscribe {
        identifier: String,
    },
    Unsubscribe {
        identifier: String,
    },
    Message {
        identifier: String,
        data: String,
    },
    Join {
        identifier: String,
        #[serde(default)]
        presence: Option<PresenceData>,
    },
    Leave {
        identifier: String,
    },
    Presence {
        identifier: String,
    },
    Whisper {
        identifier: String,
        data: String,
    },
}

/// Server-to-client messages
#[derive(Debug, Clone)]
pub enum ServerMessage {
    Welcome { sid: String },
    Ping { timestamp: i64 },
    ConfirmSubscription { identifier: String },
    RejectSubscription { identifier: String },
    Message { identifier: String, message: Value },
    Disconnect { reason: String, reconnect: bool },
}

/// Internal representation for serialization. Payloads borrow from the
/// [`ServerMessage`] being encoded, so no field is copied per frame.
#[derive(Serialize)]
struct WelcomePayload<'a> {
    #[serde(rename = "type")]
    msg_type: &'static str,
    sid: &'a str,
}

#[derive(Serialize)]
struct PingPayload {
    #[serde(rename = "type")]
    msg_type: &'static str,
    message: i64,
}

#[derive(Serialize)]
struct SubscriptionPayload<'a> {
    #[serde(rename = "type")]
    msg_type: &'static str,
    identifier: &'a str,
}

#[derive(Serialize)]
struct MessagePayload<'a> {
    identifier: &'a str,
    message: &'a Value,
}

#[derive(Serialize)]
struct DisconnectPayload<'a> {
    #[serde(rename = "type")]
    msg_type: &'static str,
    reason: &'a str,
    reconnect: bool,
}

/// Parse a client command from raw bytes
///
/// # Errors
///
/// [`ActionCableError::Parse`] when `data` is not a JSON object, names an
/// unknown `command`, or lacks a field that command requires.
pub fn parse_command(data: &[u8]) -> Result<ClientCommand, ActionCableError> {
    serde_json::from_slice(data).map_err(ActionCableError::Parse)
}

/// Encode a server message to bytes
#[must_use]
pub fn encode(msg: &ServerMessage) -> Vec<u8> {
    match msg {
        ServerMessage::Welcome { sid } => to_frame(&WelcomePayload { msg_type: "welcome", sid }),
        ServerMessage::Ping { timestamp } => {
            to_frame(&PingPayload { msg_type: "ping", message: *timestamp })
        }
        ServerMessage::ConfirmSubscription { identifier } => {
            to_frame(&SubscriptionPayload { msg_type: "confirm_subscription", identifier })
        }
        ServerMessage::RejectSubscription { identifier } => {
            to_frame(&SubscriptionPayload { msg_type: "reject_subscription", identifier })
        }
        ServerMessage::Message { identifier, message } => {
            to_frame(&MessagePayload { identifier, message })
        }
        ServerMessage::Disconnect { reason, reconnect } => {
            to_frame(&DisconnectPayload { msg_type: "disconnect", reason, reconnect: *reconnect })
        }
    }
}

/// Serialize one of the frame payloads above.
///
/// Only ever called with those derive-only payloads: strings, integers,
/// booleans, and a `serde_json::Value`, written into a `Vec`. None of them
/// can make `serde_json` fail (no fallible `Serialize` impl, no non-string map
/// key, no I/O), so the `expect` documents an invariant rather than a
/// reachable panic.
fn to_frame(payload: &impl Serialize) -> Vec<u8> {
    serde_json::to_vec(payload).expect("ActionCable frame payloads always serialize")
}

/// ActionCable protocol errors
#[derive(Debug, thiserror::Error)]
pub enum ActionCableError {
    #[error("failed to parse command: {0}")]
    Parse(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests;
