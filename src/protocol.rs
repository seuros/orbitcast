//! Canonical OrbitCast wire contract.
//!
//! Every OrbitCast-owned envelope (publications, notices, token claims, and
//! later RPC/control/presence/recovery) is defined here with `serde` and is
//! the single authority for its shape. Runtime modules consume these types
//! instead of declaring competing envelopes. Standard ActionCable frames,
//! Turbo's signing format, application payloads, and the docking transport in
//! `crate::transport::docking` are *not* covered by these rules.
//!
//! # Versioning rules
//!
//! 1. Serializers always emit `version`; it is `1` for every envelope today.
//! 2. An absent `version` parses as `1` (`#[serde(default = ...)]`) for
//!    tolerant reading during rolling deploys.
//! 3. Unknown fields are ignored - normal serde behavior, never
//!    `deny_unknown_fields` on wire types.
//! 4. Absent fields are defaulted, and authorization-bearing fields default to
//!    *invalid* values (empty namespace/stream/purpose, `expires_at = 0`) so
//!    they fail closed.
//! 5. Wrong field types, unsupported versions, and unknown tagged `kind`
//!    values are hard errors, not tolerated unknowns.
//! 6. Parsing defaults never bypass semantic validation: call
//!    [`Envelope::validate`] before acting on any decoded envelope.

pub mod v1;

/// Shared behavior for every OrbitCast-owned envelope.
pub trait Envelope {
    /// The version this envelope will serialize with.
    fn version(&self) -> u32;

    /// Semantic validation, run after parsing and before acting on the value.
    ///
    /// # Errors
    ///
    /// The first rule the envelope breaks, as a [`ProtocolError`]: an
    /// unsupported `version`, a required field that is empty or over its
    /// bound, an embedded value that does not decode, or fields that
    /// contradict each other. Each implementation documents its own rules.
    fn validate(&self) -> Result<(), ProtocolError>;
}

/// Envelope validation failures.
///
/// Decode errors (wrong types, malformed JSON, unknown tagged `kind`) surface
/// as [`ProtocolError::Malformed`]; everything else is a semantic rejection of
/// a structurally valid envelope.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("unsupported envelope version: {0}")]
    UnsupportedVersion(u32),
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("{field} exceeds maximum length ({len} > {max} bytes)")]
    FieldTooLong { field: &'static str, len: usize, max: usize },
    #[error("{field} is not a valid UUID: {value:?}")]
    InvalidUuid { field: &'static str, value: String },
    #[error("{field} does not contain valid JSON: {source}")]
    InvalidJson { field: &'static str, source: serde_json::Error },
    #[error("invalid purpose: expected {expected:?}, got {actual:?}")]
    InvalidPurpose { expected: &'static str, actual: String },
    #[error("claims expired at {expires_at} (now {now})")]
    Expired { expires_at: i64, now: i64 },
    #[error("{field} has a value that is not allowed: {value:?}")]
    DisallowedValue { field: &'static str, value: String },
    #[error("too many entries in {field}: {count} > {max}")]
    TooMany { field: &'static str, count: usize, max: usize },
    #[error("{field} is out of range: {value} (expected 1..={max})")]
    OutOfRange { field: &'static str, value: u64, max: u64 },
    #[error("{operation} requires a {field} payload")]
    MissingPayload { operation: &'static str, field: &'static str },
    #[error("{field} is not allowed on a {outcome} reply")]
    NotAllowedForOutcome { field: &'static str, outcome: &'static str },
    #[error("{field} does not match the request: expected {expected:?}, got {actual:?}")]
    Mismatch { field: &'static str, expected: String, actual: String },
    #[error("malformed envelope: {0}")]
    Malformed(#[from] serde_json::Error),
}
