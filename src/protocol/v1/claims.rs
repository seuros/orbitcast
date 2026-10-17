//! v1 signed-token claims (OrbitCast signing profile).
//!
//! This module owns the *shape and semantics* of the claims only. Signing and
//! HMAC verification stay in `crate::streams`: the signature is verified over
//! the original encoded bytes first, and only then are the claims decoded here
//! and validated.
//!
//! Every authorization-bearing claim defaults to an invalid value, so a
//! truncated or hostile token fails closed instead of granting broader access.
//! The additive optional claims (`whisper`, `kid`) default the other way —
//! to *no* extra capability — so an older validator that ignores them can
//! only be stricter than a newer one, never more permissive.

use serde::{Deserialize, Serialize};

use crate::protocol::v1::{
    self, default_version, now_unix, validate_namespace, validate_not_expired, validate_stream,
    validate_version,
};
use crate::protocol::{Envelope, ProtocolError};

/// The only purpose a subscribe token may carry.
pub const PURPOSE_SUBSCRIBE: &str = "subscribe";

/// Claims carried by an OrbitCast-profile signed stream token.
///
/// Wire form: `base64(JSON(claims))--hex(HMAC-SHA256(encoded))`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamClaims {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Namespace the token is scoped to. Absent defaults to empty (invalid).
    #[serde(default)]
    pub namespace: String,
    /// Logical stream granted. Absent defaults to empty (invalid).
    #[serde(default)]
    pub stream: String,
    /// Granted purpose; must be [`PURPOSE_SUBSCRIBE`]. Absent defaults to
    /// empty, which grants nothing.
    #[serde(default)]
    pub purpose: String,
    /// Expiry in unix seconds. Absent defaults to `0`, i.e. already expired.
    #[serde(default)]
    pub expires_at: i64,
    /// Whether the holder may also *whisper* (client-originated fanout) on
    /// the stream. Absent means `false`: a signed name is a read capability
    /// unless the producer explicitly widened it. Only emitted
    /// when set, so tokens without it keep their exact bytes.
    #[serde(default, skip_serializing_if = "is_false")]
    pub whisper: bool,
    /// Advisory key identifier. Carried through for operators correlating a
    /// token with a rotation; the verifier never selects a key from it (the
    /// signature is checked before the claims are readable, so the whole
    /// ring is tried in order regardless).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kid: Option<String>,
}

/// `skip_serializing_if` hook: an unset capability is not written.
#[expect(clippy::trivially_copy_pass_by_ref, reason = "serde's skip_serializing_if signature")]
const fn is_false(value: &bool) -> bool {
    !*value
}

impl StreamClaims {
    /// Build subscribe claims expiring at `expires_at` (unix seconds).
    pub fn subscribe(
        namespace: impl Into<String>,
        stream: impl Into<String>,
        expires_at: i64,
    ) -> Self {
        Self {
            version: v1::VERSION,
            namespace: namespace.into(),
            stream: stream.into(),
            purpose: PURPOSE_SUBSCRIBE.to_string(),
            expires_at,
            whisper: false,
            kid: None,
        }
    }

    /// The same claims, additionally granting whisper.
    #[must_use]
    pub const fn with_whisper(mut self, whisper: bool) -> Self {
        self.whisper = whisper;
        self
    }

    /// Validate against an explicit clock reading, in unix seconds.
    ///
    /// # Errors
    ///
    /// The first failed check, in order: [`ProtocolError::UnsupportedVersion`],
    /// an empty or oversized `namespace` or `stream`,
    /// [`ProtocolError::InvalidPurpose`] for anything but
    /// [`PURPOSE_SUBSCRIBE`], and [`ProtocolError::Expired`] when
    /// `expires_at` is not after `now`.
    pub fn validate_at(&self, now: i64) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        validate_namespace(&self.namespace)?;
        validate_stream(&self.stream)?;
        if self.purpose != PURPOSE_SUBSCRIBE {
            return Err(ProtocolError::InvalidPurpose {
                expected: PURPOSE_SUBSCRIBE,
                actual: self.purpose.clone(),
            });
        }
        validate_not_expired(self.expires_at, now)
    }

    /// Whether the claims authorize subscribing to `stream` in `namespace`.
    ///
    /// Semantic validation runs first: a token is never trusted because its
    /// stream happens to match.
    #[must_use]
    pub fn authorizes(&self, namespace: &str, stream: &str, now: i64) -> bool {
        self.validate_at(now).is_ok() && self.namespace == namespace && self.stream == stream
    }
}

impl Envelope for StreamClaims {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        self.validate_at(now_unix())
    }
}

#[cfg(test)]
mod tests;
