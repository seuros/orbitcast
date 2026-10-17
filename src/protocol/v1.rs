//! Version 1 envelopes: defaults and semantic validation helpers.
//!
//! Every v1 envelope carries `version: 1` on the wire, defaults an absent
//! `version` to `1` on parse, and defaults authorization-bearing fields to
//! invalid values so a truncated envelope fails closed instead of widening
//! access. The helpers in this module are the shared semantic checks the
//! envelope types run from `Envelope::validate`.

pub mod claims;
pub mod control;
pub mod presence;
pub mod publication;
pub mod recovery;
pub mod rpc;

use crate::protocol::ProtocolError;

/// `Display` for a wire enum: exactly its wire spelling, from `as_str`.
macro_rules! display_as_wire_str {
    ($($ty:ty),+ $(,)?) => {$(
        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    )+};
}
pub(crate) use display_as_wire_str;

/// The v1 wire version integer.
pub const VERSION: u32 = 1;

/// Maximum accepted namespace length, in bytes.
pub const MAX_NAMESPACE_LEN: usize = 128;

/// Maximum accepted logical stream name length, in bytes.
pub const MAX_STREAM_LEN: usize = 1024;

/// Maximum accepted publication payload length, in bytes (1 MiB default).
pub const MAX_PAYLOAD_LEN: usize = 1024 * 1024;

/// `#[serde(default = ...)]` hook: an absent `version` reads as v1.
#[must_use]
pub const fn default_version() -> u32 {
    VERSION
}

/// Reject any envelope that does not declare v1.
///
/// # Errors
///
/// [`ProtocolError::UnsupportedVersion`] for any `version` other than
/// [`VERSION`].
pub const fn validate_version(version: u32) -> Result<(), ProtocolError> {
    if version == VERSION { Ok(()) } else { Err(ProtocolError::UnsupportedVersion(version)) }
}

/// Reject empty or oversized values for a required string field.
///
/// # Errors
///
/// [`ProtocolError::EmptyField`] for an empty `value`, and
/// [`ProtocolError::FieldTooLong`] when it is longer than `max` bytes.
pub const fn validate_bounded(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), ProtocolError> {
    if value.is_empty() {
        return Err(ProtocolError::EmptyField { field });
    }
    let len = value.len();
    if len > max {
        return Err(ProtocolError::FieldTooLong { field, len, max });
    }
    Ok(())
}

/// A namespace must be present and bounded; an absent one defaults to empty
/// and is rejected here rather than treated as "any namespace".
///
/// # Errors
///
/// [`ProtocolError::EmptyField`] or [`ProtocolError::FieldTooLong`] (over
/// [`MAX_NAMESPACE_LEN`]), as in [`validate_bounded`].
pub const fn validate_namespace(namespace: &str) -> Result<(), ProtocolError> {
    validate_bounded("namespace", namespace, MAX_NAMESPACE_LEN)
}

/// A logical stream name must be present and bounded.
///
/// # Errors
///
/// [`ProtocolError::EmptyField`] or [`ProtocolError::FieldTooLong`] (over
/// [`MAX_STREAM_LEN`]), as in [`validate_bounded`].
pub const fn validate_stream(stream: &str) -> Result<(), ProtocolError> {
    validate_bounded("stream", stream, MAX_STREAM_LEN)
}

/// Publication IDs are UUID strings; anything else is a routing hazard.
///
/// # Errors
///
/// [`ProtocolError::EmptyField`] for an empty `value`, and
/// [`ProtocolError::InvalidUuid`] when it does not parse as a UUID.
pub fn validate_uuid(field: &'static str, value: &str) -> Result<(), ProtocolError> {
    if value.is_empty() {
        return Err(ProtocolError::EmptyField { field });
    }
    uuid::Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| ProtocolError::InvalidUuid { field, value: value.to_string() })
}

/// Check that an embedded application payload is valid JSON *without*
/// decoding it into values: the exact Rails-encoded text is preserved byte for
/// byte and must never be re-encoded.
///
/// # Errors
///
/// [`ProtocolError::EmptyField`] for empty `text`,
/// [`ProtocolError::FieldTooLong`] above [`MAX_PAYLOAD_LEN`] bytes, and
/// [`ProtocolError::InvalidJson`] when it is not exactly one JSON value.
pub fn validate_json_text(field: &'static str, text: &str) -> Result<(), ProtocolError> {
    if text.is_empty() {
        return Err(ProtocolError::EmptyField { field });
    }
    let len = text.len();
    if len > MAX_PAYLOAD_LEN {
        return Err(ProtocolError::FieldTooLong { field, len, max: MAX_PAYLOAD_LEN });
    }
    serde_json::from_str::<serde::de::IgnoredAny>(text)
        .map(|_| ())
        .map_err(|source| ProtocolError::InvalidJson { field, source })
}

/// Reject expired (or absent, i.e. `0`) expiry claims.
///
/// # Errors
///
/// [`ProtocolError::Expired`] unless `expires_at` is strictly after `now`.
pub const fn validate_not_expired(expires_at: i64, now: i64) -> Result<(), ProtocolError> {
    if expires_at > now { Ok(()) } else { Err(ProtocolError::Expired { expires_at, now }) }
}

/// Current wall-clock time in unix seconds.
///
/// A pre-epoch clock yields `0`, which expires every claim: fail closed.
#[must_use]
pub fn now_unix() -> i64 {
    jiff::Timestamp::now().as_second().max(0)
}

#[cfg(test)]
mod tests;
