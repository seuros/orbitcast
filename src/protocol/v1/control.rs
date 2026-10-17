//! v1 Rails-initiated control messages.
//!
//! A control message is how Rails reaches *sessions it does not hold*: a
//! multi-device sign-out publishes one disconnect control message, every node
//! looks the identity key up in its local index, and only the matching sessions
//! are closed. No node scans every session, and no control data is ever visible
//! to a browser.
//!
//! Control messages arrive over the private HTTP control ingress and over a
//! namespaced PostgreSQL control notification; the same envelope covers both,
//! and `control_id` deduplicates a message that arrives twice.

use serde::{Deserialize, Serialize};

use crate::protocol::v1::rpc::validate_close_reason;
use crate::protocol::v1::{
    self, default_version, validate_bounded, validate_namespace, validate_uuid, validate_version,
};
use crate::protocol::{Envelope, ProtocolError};

/// Maximum accepted identity-key length, in bytes.
pub const MAX_IDENTITY_KEY_LEN: usize = 512;

/// The tagged body of a control message. Unknown `kind` values are hard decode
/// errors: a node that cannot understand a control message must not apply a
/// guessed-at fraction of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    /// Close every session whose canonical identity key matches.
    Disconnect,
}

impl ControlKind {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disconnect => "disconnect",
        }
    }
}

super::display_as_wire_str!(ControlKind);

/// One Rails-initiated control message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlMessage {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// What to do. Required: there is no default control action.
    pub kind: ControlKind,
    /// Namespace this message applies to. Re-validated on receipt; a message
    /// for another deployment's namespace is dropped.
    #[serde(default)]
    pub namespace: String,
    /// UUID of this control message, for deduplication across the ingress and
    /// the notification path.
    #[serde(default)]
    pub control_id: String,
    /// The canonical identity key Rust indexes sessions by, exactly as it was
    /// derived from the accepted `identify` identity attributes.
    #[serde(default)]
    pub identity_key: String,
    /// Disconnect reason, from [`crate::protocol::v1::rpc::ALLOWED_CLOSE_REASONS`].
    #[serde(default)]
    pub reason: String,
    /// Reconnect policy handed to the ActionCable client.
    ///
    /// Absent defaults to `false`: a control message that fails to say
    /// otherwise ends the session rather than inviting an immediate reconnect
    /// with credentials the application just revoked.
    #[serde(default)]
    pub reconnect: bool,
}

impl ControlMessage {
    /// Build a disconnect control message with a fresh control UUID.
    pub fn disconnect(
        namespace: impl Into<String>,
        identity_key: impl Into<String>,
        reason: impl Into<String>,
        reconnect: bool,
    ) -> Self {
        Self {
            version: v1::VERSION,
            kind: ControlKind::Disconnect,
            namespace: namespace.into(),
            control_id: uuid::Uuid::new_v4().to_string(),
            identity_key: identity_key.into(),
            reason: reason.into(),
            reconnect,
        }
    }

    /// Whether this message applies to `namespace`.
    ///
    /// Validation runs first: a message is never applied because its namespace
    /// happens to match.
    #[must_use]
    pub fn applies_to(&self, namespace: &str) -> bool {
        self.validate().is_ok() && self.namespace == namespace
    }
}

impl Envelope for ControlMessage {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        validate_namespace(&self.namespace)?;
        validate_uuid("control_id", &self.control_id)?;
        validate_bounded("identity_key", &self.identity_key, MAX_IDENTITY_KEY_LEN)?;
        validate_close_reason(&self.reason)
    }
}

#[cfg(test)]
mod tests;
