use super::*;
use serde_json::Value;

#[test]
fn addressed_frame_echoes_identifier_and_payload() {
    let identifier = r#"{"channel":"Turbo::StreamsChannel","signed_stream_name":"abc"}"#;
    let frame = addressed_frame(&encode_identifier(identifier), br#"{"text":"hello"}"#);

    let parsed: Value = serde_json::from_slice(&frame).unwrap();
    assert_eq!(parsed["identifier"], identifier);
    assert_eq!(parsed["message"]["text"], "hello");
}

#[test]
fn identifier_escaping_survives_the_roundtrip() {
    const IDENTIFIERS: &[&str] = &[
        "quote\" backslash\\ newline\n unicode ✨",
        "control \u{0} \u{1f} tab\t",
        "line separators \u{2028} \u{2029}",
        r#"{"channel":"Turbo::StreamsChannel","signed_stream_name":"abc"}"#,
        "",
    ];

    for identifier in IDENTIFIERS {
        let encoded = encode_identifier(identifier);
        // Byte-identical to the serializer the rest of the crate uses.
        assert_eq!(&*encoded, serde_json::to_string(identifier).unwrap(), "{identifier:?}");

        let frame = addressed_frame(&encoded, b"null");
        let parsed: Value = serde_json::from_slice(&frame).unwrap();
        assert_eq!(parsed["identifier"], *identifier, "{identifier:?}");
        assert!(parsed["message"].is_null(), "{identifier:?}");
    }
}

#[test]
fn payload_bytes_are_spliced_verbatim() {
    // Key order and spacing of the application payload are preserved.
    let payload = br#"{"z":1,  "a":[1,2,3]}"#;
    let frame = addressed_frame(&encode_identifier("id"), payload);

    let text = std::str::from_utf8(&frame).unwrap();
    assert_eq!(text, r#"{"identifier":"id","message":{"z":1,  "a":[1,2,3]}}"#);
}
