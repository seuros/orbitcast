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
    #[allow(dead_code)]
    Welcome {
        sid: String,
    },
    Ping {
        timestamp: i64,
    },
    #[allow(dead_code)]
    ConfirmSubscription {
        identifier: String,
    },
    #[allow(dead_code)]
    RejectSubscription {
        identifier: String,
    },
    Message {
        identifier: String,
        message: Value,
    },
    #[allow(dead_code)]
    Disconnect {
        reason: String,
        reconnect: bool,
    },
}

/// Internal representation for serialization
#[derive(Serialize)]
struct WelcomePayload {
    #[serde(rename = "type")]
    msg_type: &'static str,
    sid: String,
}

#[derive(Serialize)]
struct PingPayload {
    #[serde(rename = "type")]
    msg_type: &'static str,
    message: i64,
}

#[derive(Serialize)]
struct SubscriptionPayload {
    #[serde(rename = "type")]
    msg_type: &'static str,
    identifier: String,
}

#[derive(Serialize)]
struct MessagePayload {
    identifier: String,
    message: Value,
}

#[derive(Serialize)]
struct DisconnectPayload {
    #[serde(rename = "type")]
    msg_type: &'static str,
    reason: String,
    reconnect: bool,
}

/// Parse a client command from raw bytes
pub fn parse_command(data: &[u8]) -> Result<ClientCommand, ActionCableError> {
    serde_json::from_slice(data).map_err(ActionCableError::Parse)
}

/// Encode a server message to bytes
pub fn encode(msg: &ServerMessage) -> Vec<u8> {
    match msg {
        ServerMessage::Welcome { sid } => serde_json::to_vec(&WelcomePayload {
            msg_type: "welcome",
            sid: sid.clone(),
        })
        .unwrap(),
        ServerMessage::Ping { timestamp } => serde_json::to_vec(&PingPayload {
            msg_type: "ping",
            message: *timestamp,
        })
        .unwrap(),
        ServerMessage::ConfirmSubscription { identifier } => {
            serde_json::to_vec(&SubscriptionPayload {
                msg_type: "confirm_subscription",
                identifier: identifier.clone(),
            })
            .unwrap()
        }
        ServerMessage::RejectSubscription { identifier } => {
            serde_json::to_vec(&SubscriptionPayload {
                msg_type: "reject_subscription",
                identifier: identifier.clone(),
            })
            .unwrap()
        }
        ServerMessage::Message {
            identifier,
            message,
        } => serde_json::to_vec(&MessagePayload {
            identifier: identifier.clone(),
            message: message.clone(),
        })
        .unwrap(),
        ServerMessage::Disconnect { reason, reconnect } => serde_json::to_vec(&DisconnectPayload {
            msg_type: "disconnect",
            reason: reason.clone(),
            reconnect: *reconnect,
        })
        .unwrap(),
    }
}

/// ActionCable protocol errors
#[derive(Debug, thiserror::Error)]
pub enum ActionCableError {
    #[error("failed to parse command: {0}")]
    Parse(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_subscribe() {
        let json = br#"{"command":"subscribe","identifier":"chat_1"}"#;
        let cmd = parse_command(json).unwrap();
        match cmd {
            ClientCommand::Subscribe { identifier } => {
                assert_eq!(identifier, "chat_1");
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn test_parse_unsubscribe() {
        let json = br#"{"command":"unsubscribe","identifier":"chat_1"}"#;
        let cmd = parse_command(json).unwrap();
        match cmd {
            ClientCommand::Unsubscribe { identifier } => {
                assert_eq!(identifier, "chat_1");
            }
            _ => panic!("expected Unsubscribe"),
        }
    }

    #[test]
    fn test_parse_message() {
        let json =
            br#"{"command":"message","identifier":"chat_1","data":"{\"action\":\"speak\"}"}"#;
        let cmd = parse_command(json).unwrap();
        match cmd {
            ClientCommand::Message { identifier, data } => {
                assert_eq!(identifier, "chat_1");
                assert!(data.contains("speak"));
            }
            _ => panic!("expected Message"),
        }
    }

    #[test]
    fn test_encode_welcome() {
        let msg = ServerMessage::Welcome {
            sid: "abc123".to_string(),
        };
        let encoded = encode(&msg);
        let json: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["type"], "welcome");
        assert_eq!(json["sid"], "abc123");
    }

    #[test]
    fn test_encode_ping() {
        let msg = ServerMessage::Ping {
            timestamp: 1234567890,
        };
        let encoded = encode(&msg);
        let json: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["type"], "ping");
        assert_eq!(json["message"], 1234567890);
    }

    #[test]
    fn test_encode_confirm_subscription() {
        let msg = ServerMessage::ConfirmSubscription {
            identifier: "chat_1".to_string(),
        };
        let encoded = encode(&msg);
        let json: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["type"], "confirm_subscription");
        assert_eq!(json["identifier"], "chat_1");
    }

    #[test]
    fn test_encode_message() {
        let msg = ServerMessage::Message {
            identifier: "chat_1".to_string(),
            message: serde_json::json!({"text": "hello"}),
        };
        let encoded = encode(&msg);
        let json: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["identifier"], "chat_1");
        assert_eq!(json["message"]["text"], "hello");
    }

    #[test]
    fn test_encode_disconnect() {
        let msg = ServerMessage::Disconnect {
            reason: "server_restart".to_string(),
            reconnect: true,
        };
        let encoded = encode(&msg);
        let json: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["type"], "disconnect");
        assert_eq!(json["reason"], "server_restart");
        assert_eq!(json["reconnect"], true);
    }

    #[test]
    fn test_parse_join_with_presence() {
        let json = br#"{"command":"join","identifier":"chat_1","presence":{"id":"user_42","info":{"name":"Marissa"}}}"#;
        let cmd = parse_command(json).unwrap();
        match cmd {
            ClientCommand::Join {
                identifier,
                presence,
            } => {
                assert_eq!(identifier, "chat_1");
                let p = presence.unwrap();
                assert_eq!(p.id, "user_42");
                assert_eq!(p.info.unwrap()["name"], "Marissa");
            }
            _ => panic!("expected Join"),
        }
    }

    #[test]
    fn test_parse_join_without_presence() {
        let json = br#"{"command":"join","identifier":"chat_1"}"#;
        let cmd = parse_command(json).unwrap();
        match cmd {
            ClientCommand::Join {
                identifier,
                presence,
            } => {
                assert_eq!(identifier, "chat_1");
                assert!(presence.is_none());
            }
            _ => panic!("expected Join"),
        }
    }

    #[test]
    fn test_parse_leave() {
        let json = br#"{"command":"leave","identifier":"chat_1"}"#;
        let cmd = parse_command(json).unwrap();
        match cmd {
            ClientCommand::Leave { identifier } => {
                assert_eq!(identifier, "chat_1");
            }
            _ => panic!("expected Leave"),
        }
    }

    #[test]
    fn test_parse_presence() {
        let json = br#"{"command":"presence","identifier":"chat_1"}"#;
        let cmd = parse_command(json).unwrap();
        match cmd {
            ClientCommand::Presence { identifier } => {
                assert_eq!(identifier, "chat_1");
            }
            _ => panic!("expected Presence"),
        }
    }

    #[test]
    fn test_parse_whisper() {
        let json = br#"{"command":"whisper","identifier":"chat_1","data":"{\"type\":\"typing\"}"}"#;
        let cmd = parse_command(json).unwrap();
        match cmd {
            ClientCommand::Whisper { identifier, data } => {
                assert_eq!(identifier, "chat_1");
                assert!(data.contains("typing"));
            }
            _ => panic!("expected Whisper"),
        }
    }
}
