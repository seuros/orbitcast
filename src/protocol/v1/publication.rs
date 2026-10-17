//! v1 publication transport records.
//!
//! [`Publication`] is one broadcast produced by Rails; [`PublicationNotice`]
//! is what actually travels over a PostgreSQL `NOTIFY` channel - either the
//! publication inline, or a reference to an overflow row when the encoded
//! notice would exceed PostgreSQL's payload limit.
//!
//! These are transport records, not browser frames: the delivered application
//! value is `json_payload`, carried as the exact Rails-encoded JSON *text*.
//! Quoting it inside the envelope is envelope encoding; the bytes are never
//! re-encoded, re-ordered, or coerced.

use serde::{Deserialize, Serialize};

use crate::protocol::v1::{
    self, default_version, validate_json_text, validate_namespace, validate_stream, validate_uuid,
    validate_version,
};
use crate::protocol::{Envelope, ProtocolError};

/// PostgreSQL refuses `NOTIFY` payloads of 8000 bytes or more.
pub const MAX_NOTIFY_PAYLOAD_LEN: usize = 8000;

/// A single Rails broadcast, addressed by `(namespace, stream)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Publication {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Routing namespace. Absent defaults to empty, which fails validation.
    #[serde(default)]
    pub namespace: String,
    /// Publication UUID, unique per publish so identical messages in one
    /// transaction are not folded together.
    #[serde(default)]
    pub id: String,
    /// Logical stream name (never the hashed channel name).
    #[serde(default)]
    pub stream: String,
    /// Exact Rails-encoded JSON text of the application payload.
    #[serde(default)]
    pub json_payload: String,
}

impl Publication {
    /// Build a publication with a fresh UUID.
    pub fn new(
        namespace: impl Into<String>,
        stream: impl Into<String>,
        json_payload: impl Into<String>,
    ) -> Self {
        Self {
            version: v1::VERSION,
            namespace: namespace.into(),
            id: uuid::Uuid::new_v4().to_string(),
            stream: stream.into(),
            json_payload: json_payload.into(),
        }
    }
}

impl Envelope for Publication {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        validate_namespace(&self.namespace)?;
        validate_uuid("id", &self.id)?;
        validate_stream(&self.stream)?;
        validate_json_text("json_payload", &self.json_payload)
    }
}

/// Reference to an `orbitcast_payloads` row holding an oversized publication.
///
/// The reference is not authorization: every listening node re-validates the
/// namespace and stream of the fetched row against this reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverflowRef {
    /// Primary key of the overflow row, equal to the publication UUID.
    #[serde(default)]
    pub id: String,
    /// Expected namespace of the overflow row.
    #[serde(default)]
    pub namespace: String,
    /// Expected logical stream of the overflow row.
    #[serde(default)]
    pub stream: String,
}

impl OverflowRef {
    /// Build a reference to the overflow row of `publication`.
    #[must_use]
    pub fn for_publication(publication: &Publication) -> Self {
        Self {
            id: publication.id.clone(),
            namespace: publication.namespace.clone(),
            stream: publication.stream.clone(),
        }
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_uuid("id", &self.id)?;
        validate_namespace(&self.namespace)?;
        validate_stream(&self.stream)
    }
}

/// The tagged body of a notice. Unknown `kind` values are hard decode errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NoticeKind {
    /// The publication travels inside the notification payload.
    Inline { publication: Publication },
    /// The publication is stored in the overflow table; fetch it by reference.
    Overflow {
        #[serde(default)]
        id: String,
        #[serde(default)]
        namespace: String,
        #[serde(default)]
        stream: String,
    },
}

/// The record carried by one PostgreSQL `NOTIFY`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationNotice {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Inline publication or overflow reference, selected by `kind`.
    #[serde(flatten)]
    pub kind: NoticeKind,
}

impl PublicationNotice {
    /// Wrap a publication for inline delivery.
    #[must_use]
    pub const fn inline(publication: Publication) -> Self {
        Self { version: v1::VERSION, kind: NoticeKind::Inline { publication } }
    }

    /// Reference an overflow row instead of inlining the payload.
    #[must_use]
    pub fn overflow(reference: OverflowRef) -> Self {
        Self {
            version: v1::VERSION,
            kind: NoticeKind::Overflow {
                id: reference.id,
                namespace: reference.namespace,
                stream: reference.stream,
            },
        }
    }

    /// The namespace this notice routes into.
    #[must_use]
    pub fn namespace(&self) -> &str {
        match &self.kind {
            NoticeKind::Inline { publication } => &publication.namespace,
            NoticeKind::Overflow { namespace, .. } => namespace,
        }
    }

    /// The logical stream this notice routes to.
    #[must_use]
    pub fn stream(&self) -> &str {
        match &self.kind {
            NoticeKind::Inline { publication } => &publication.stream,
            NoticeKind::Overflow { stream, .. } => stream,
        }
    }

    /// Whether the encoded form fits in a PostgreSQL `NOTIFY` payload.
    ///
    /// Checked against the encoded UTF-8 byte length, envelope escaping
    /// included, before any SQL runs.
    #[must_use]
    pub const fn fits_notify_payload(encoded: &str) -> bool {
        encoded.len() < MAX_NOTIFY_PAYLOAD_LEN
    }
}

/// The `202` body of the private HTTP publication ingress.
///
/// Deliberately minimal: the version every Orbitcast envelope carries, plus
/// the publication UUID the caller supplied - a `202` means the backend
/// accepted and committed the publication, not that a browser saw it. Pinned
/// by `tests/fixtures/ingress/ack.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressAck {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// UUID of the accepted publication, exactly as it arrived.
    #[serde(default)]
    pub publication_id: String,
}

impl IngressAck {
    /// Acknowledge one accepted publication.
    pub fn new(publication_id: impl Into<String>) -> Self {
        Self { version: v1::VERSION, publication_id: publication_id.into() }
    }
}

impl Envelope for IngressAck {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        validate_uuid("publication_id", &self.publication_id)
    }
}

/// A refusal from the private HTTP ingress.
///
/// Every error body the ingress emits is this envelope: the version every
/// Orbitcast envelope carries, a stable machine-readable `error` code, and a
/// human-readable `message` that never carries payload data. The HTTP status
/// says what happened; the body says why, in a shape a client can match on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressError {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Stable code: `malformed_envelope`, `invalid_envelope`,
    /// `foreign_namespace`, `payload_too_large`, `body_too_large`,
    /// `unreadable_body`, `unauthorized`, `method_not_allowed`, `not_found`,
    /// `at_capacity`, `backend_unavailable`, `invalid_control`.
    #[serde(default)]
    pub error: String,
    /// Diagnostic text for operators; never application data.
    #[serde(default)]
    pub message: String,
}

impl IngressError {
    /// Build one refusal.
    pub fn new(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self { version: v1::VERSION, error: error.into(), message: message.into() }
    }
}

impl Envelope for IngressError {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        v1::validate_bounded("error", &self.error, 64)
    }
}

/// The `202` body of `POST /v1/control`.
///
/// Echoes the control UUID the caller supplied. A duplicate `control_id` is
/// acknowledged the same way: the first arrival already acted, and the
/// acknowledgement is idempotent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlAck {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// UUID of the accepted control message, exactly as it arrived.
    #[serde(default)]
    pub control_id: String,
}

impl ControlAck {
    /// Acknowledge one accepted control message.
    pub fn new(control_id: impl Into<String>) -> Self {
        Self { version: v1::VERSION, control_id: control_id.into() }
    }
}

impl Envelope for ControlAck {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        validate_uuid("control_id", &self.control_id)
    }
}

impl Envelope for PublicationNotice {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        match &self.kind {
            NoticeKind::Inline { publication } => publication.validate(),
            NoticeKind::Overflow { id, namespace, stream } => {
                OverflowRef { id: id.clone(), namespace: namespace.clone(), stream: stream.clone() }
                    .validate()
            }
        }
    }
}

#[cfg(test)]
mod tests;
