use super::*;

use std::assert_matches;

#[test]
fn test_parse_subscribe() {
    let json = br#"{"command":"subscribe","identifier":"chat_1"}"#;
    let cmd = parse_command(json).unwrap();
    assert_matches!(cmd, ClientCommand::Subscribe { identifier } if identifier == "chat_1");
}

#[test]
fn test_parse_unsubscribe() {
    let json = br#"{"command":"unsubscribe","identifier":"chat_1"}"#;
    let cmd = parse_command(json).unwrap();
    assert_matches!(cmd, ClientCommand::Unsubscribe { identifier } if identifier == "chat_1");
}

#[test]
fn test_parse_message() {
    let json = br#"{"command":"message","identifier":"chat_1","data":"{\"action\":\"speak\"}"}"#;
    let cmd = parse_command(json).unwrap();
    assert_matches!(
        cmd,
        ClientCommand::Message { identifier, data }
            if identifier == "chat_1" && data.contains("speak")
    );
}

/// Every server frame, byte for byte: field order and escaping are part of
/// what the ActionCable client receives, not just the decoded values.
#[test]
fn encode_produces_exact_frames() {
    let cases = [
        (
            ServerMessage::Welcome { sid: "abc123".to_string() },
            r#"{"type":"welcome","sid":"abc123"}"#,
        ),
        (
            ServerMessage::Ping { timestamp: 1_234_567_890 },
            r#"{"type":"ping","message":1234567890}"#,
        ),
        (
            ServerMessage::ConfirmSubscription { identifier: "chat_1".to_string() },
            r#"{"type":"confirm_subscription","identifier":"chat_1"}"#,
        ),
        (
            ServerMessage::RejectSubscription { identifier: "chat_1".to_string() },
            r#"{"type":"reject_subscription","identifier":"chat_1"}"#,
        ),
        (
            ServerMessage::Message {
                identifier: r#"{"channel":"ChatChannel"}"#.to_string(),
                message: serde_json::json!({"text": "hello"}),
            },
            r#"{"identifier":"{\"channel\":\"ChatChannel\"}","message":{"text":"hello"}}"#,
        ),
        (
            ServerMessage::Disconnect { reason: "server_restart".to_string(), reconnect: true },
            r#"{"type":"disconnect","reason":"server_restart","reconnect":true}"#,
        ),
    ];

    for (message, expected) in cases {
        let encoded = encode(&message);
        assert_eq!(String::from_utf8(encoded).unwrap(), expected, "message: {message:?}");
    }
}

#[test]
fn test_parse_join_with_presence() {
    let json = br#"{
            "command":"join",
            "identifier":"chat_1",
            "presence":{"id":"user_42","info":{"name":"Marissa"}}
        }"#;
    let cmd = parse_command(json).unwrap();
    let ClientCommand::Join { identifier, presence } = cmd else {
        panic!("expected Join, got {cmd:?}");
    };
    assert_eq!(identifier, "chat_1");
    let p = presence.unwrap();
    assert_eq!(p.id, "user_42");
    assert_eq!(p.info.unwrap()["name"], "Marissa");
}

#[test]
fn test_parse_join_without_presence() {
    let json = br#"{"command":"join","identifier":"chat_1"}"#;
    let cmd = parse_command(json).unwrap();
    assert_matches!(
        cmd,
        ClientCommand::Join { identifier, presence: None } if identifier == "chat_1"
    );
}

#[test]
fn test_parse_leave() {
    let json = br#"{"command":"leave","identifier":"chat_1"}"#;
    let cmd = parse_command(json).unwrap();
    assert_matches!(cmd, ClientCommand::Leave { identifier } if identifier == "chat_1");
}

#[test]
fn test_parse_presence() {
    let json = br#"{"command":"presence","identifier":"chat_1"}"#;
    let cmd = parse_command(json).unwrap();
    assert_matches!(cmd, ClientCommand::Presence { identifier } if identifier == "chat_1");
}

#[test]
fn test_parse_whisper() {
    let json = br#"{"command":"whisper","identifier":"chat_1","data":"{\"type\":\"typing\"}"}"#;
    let cmd = parse_command(json).unwrap();
    assert_matches!(
        cmd,
        ClientCommand::Whisper { identifier, data }
            if identifier == "chat_1" && data.contains("typing")
    );
}
