//! Addressed ActionCable frame encoding with shared payload bytes.
//!
//! The ActionCable client protocol addresses every server message with the
//! *subscriber's own* identifier: the byte-for-byte JSON string the client sent
//! in its `subscribe` command. A stream name is a server-side routing key and
//! must never be substituted for it.
//!
//! A publication is therefore encoded once into its message bytes, and each
//! member of the stream gets a frame that differs only in the `identifier`
//! field. The payload bytes are spliced in verbatim — never re-parsed or
//! re-serialized per recipient — and finished frames are shared as [`Bytes`]
//! between members whose identifiers are byte-identical (the common Turbo case,
//! where every browser on a page subscribes with the same identifier string),
//! all the way down to the socket writer: no per-recipient copy exists.

use std::sync::Arc;

use bytes::{BufMut as _, Bytes, BytesMut};

const FRAME_PREFIX: &[u8] = br#"{"identifier":"#;
const FRAME_MESSAGE: &[u8] = br#","message":"#;
const FRAME_SUFFIX: u8 = b'}';

/// Pre-encode a client identifier as a JSON string literal (quotes and escapes
/// included), ready to splice into a frame.
///
/// Done once per subscription so fan-out never re-escapes the identifier.
#[must_use]
pub fn encode_identifier(identifier: &str) -> Arc<str> {
    // `Value`'s `Display` writes the same compact JSON as
    // `serde_json::to_string`, but through an infallible API: a string cannot
    // fail to encode, so there is no error (or panic) path to carry.
    Arc::from(serde_json::Value::from(identifier).to_string())
}

/// Build `{"identifier":<identifier_json>,"message":<payload>}`.
///
/// `identifier_json` must come from [`encode_identifier`] and `payload` must
/// already be a valid JSON value; both are spliced in verbatim, so the
/// application's exact bytes reach the browser.
#[must_use]
pub fn addressed_frame(identifier_json: &str, payload: &[u8]) -> Bytes {
    let mut frame = BytesMut::with_capacity(
        FRAME_PREFIX.len() + identifier_json.len() + FRAME_MESSAGE.len() + payload.len() + 1,
    );
    frame.put_slice(FRAME_PREFIX);
    frame.put_slice(identifier_json.as_bytes());
    frame.put_slice(FRAME_MESSAGE);
    frame.put_slice(payload);
    frame.put_u8(FRAME_SUFFIX);
    frame.freeze()
}

#[cfg(test)]
mod tests;
