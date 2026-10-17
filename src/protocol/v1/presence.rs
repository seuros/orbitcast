//! v1 presence envelopes.
//!
//! Presence needs **no transport extension**: every frame below rides an
//! ordinary ActionCable `message`/`perform` command in one direction and an
//! ordinary addressed application message in the other. A stock
//! `@rails/actioncable` client speaks it without a patch.
//!
//! ```text
//! client -> server   {"command":"message","identifier":"{...}",
//!                     "data":"{\"action\":\"orbitcast_presence\",\"op\":\"join\"}"}
//! server -> client   {"identifier":"{...}","message":{"version":1,
//!                     "type":"presence","event":"join","stream":"chat:42",
//!                     "subject_id":"user:7","metadata":{},"total":1}}
//! server -> client   {"identifier":"{...}","message":{"version":1,
//!                     "type":"presence","event":"error","reason":"not_granted",
//!                     "op":"join"}}
//! ```
//!
//! A refused operation is answered on the same subscription with an `error`
//! message whose `reason` is the stable code of the refusal
//! ([`crate::presence::PresenceError::code`]); `op` names the operation when
//! it decoded and `stream` the roster when exactly one was addressed. A
//! refusal never closes the connection.
//!
//! # What the client may say
//!
//! `orbitcast_presence` is a **reserved action namespace**: an application
//! channel may not define an action with that name, because the server answers
//! it instead of Rails. The only client-controlled fields are the operation and
//! the snapshot page window.
//!
//! Identity is deliberately *not* one of them. A [`PresenceCommand`] carries no
//! `subject_id` and no `metadata`: both come from the `presence_grant` effect
//! Rails returned for this `(connection, identifier, stream)`, so a client
//! cannot name, impersonate, or decorate a subject. Any such field a client
//! sends anyway is an unknown field and is ignored on decode, never
//! promoted into the lease.
//!
//! # Tolerance and failure
//!
//! | wire condition | result |
//! | --- | --- |
//! | unknown field | ignored |
//! | absent `version` | reads as `1` |
//! | unknown `op` value | hard decode error |
//! | absent `op` | hard decode error |
//! | `action` other than `orbitcast_presence` | semantic rejection |
//! | `limit` of `0` or above [`MAX_PAGE_SIZE`] | semantic rejection |
//! | metadata above [`MAX_METADATA_LEN`] | semantic rejection |

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::protocol::v1::{
    self, default_version, validate_bounded, validate_stream, validate_version,
};
use crate::protocol::{Envelope, ProtocolError};

/// The reserved action name presence operations travel under.
pub const PRESENCE_ACTION: &str = "orbitcast_presence";

/// The `type` of every server-to-client presence message.
pub const PRESENCE_MESSAGE_TYPE: &str = "presence";

/// Maximum accepted presence subject-id length, in bytes.
///
/// Same bound as the `presence_grant` effect's: a subject that cannot be
/// granted can never be joined.
pub const MAX_SUBJECT_ID_LEN: usize = v1::rpc::MAX_SUBJECT_ID_LEN;

/// Maximum encoded size of one subject's public metadata, in bytes (4 KiB).
pub const MAX_METADATA_LEN: usize = v1::rpc::MAX_PRESENCE_METADATA_LEN;

/// Maximum number of members one snapshot page may carry.
pub const MAX_PAGE_SIZE: u32 = 100;

/// Page size used when a client asks for a snapshot without one.
pub const DEFAULT_PAGE_SIZE: u32 = 50;

/// Maximum accepted snapshot page offset.
///
/// Deep paging over a roster is a scan; the ceiling keeps one client from
/// walking an unbounded `OFFSET` across the table.
pub const MAX_PAGE_OFFSET: u64 = 100_000;

/// Reject an empty or oversized subject id.
///
/// # Errors
///
/// [`ProtocolError::EmptyField`] for an empty id, and
/// [`ProtocolError::FieldTooLong`] above [`MAX_SUBJECT_ID_LEN`] bytes.
pub const fn validate_subject_id(subject_id: &str) -> Result<(), ProtocolError> {
    validate_bounded("subject_id", subject_id, MAX_SUBJECT_ID_LEN)
}

/// Reject metadata whose *encoded* size exceeds [`MAX_METADATA_LEN`].
///
/// The encoded size is what travels and what is stored, so a single huge value
/// is bounded exactly like many small ones.
///
/// # Errors
///
/// [`ProtocolError::FieldTooLong`] when the encoded map is over the limit,
/// and [`ProtocolError::Malformed`] if it cannot be encoded at all.
pub fn validate_metadata(metadata: &Map<String, Value>) -> Result<(), ProtocolError> {
    let encoded = serde_json::to_string(metadata).map_err(ProtocolError::Malformed)?;
    if encoded.len() > MAX_METADATA_LEN {
        return Err(ProtocolError::FieldTooLong {
            field: "metadata",
            len: encoded.len(),
            max: MAX_METADATA_LEN,
        });
    }
    Ok(())
}

/// One presence operation a client may ask for.
///
/// Serialized `snake_case`. An unknown value is a hard decode error: a typo must
/// fail rather than fall through to a default operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PresenceOp {
    /// Claim (or refresh) this session's lease on the granted subject.
    Join,
    /// Drop this session's lease.
    Leave,
    /// Read one page of the current roster.
    Snapshot,
}

impl PresenceOp {
    /// The wire spelling, which is also the spelling used in a grant's
    /// `operations` allowlist.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Join => "join",
            Self::Leave => "leave",
            Self::Snapshot => "snapshot",
        }
    }

    /// Resolve a wire/allowlist spelling; `None` for anything unknown.
    #[must_use]
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "join" => Some(Self::Join),
            "leave" => Some(Self::Leave),
            "snapshot" => Some(Self::Snapshot),
            _ => None,
        }
    }
}

super::display_as_wire_str!(PresenceOp);

/// Cheap probe: does this `perform` payload address the presence namespace?
///
/// Used to route a command *before* it is strictly parsed, so a malformed
/// presence command is reported as a presence error instead of being forwarded
/// to Rails as an application action. Anything that does not decode, or that
/// names another action, is not ours.
#[must_use]
pub fn is_presence_action(json_data: &str) -> bool {
    #[derive(Deserialize)]
    struct ActionProbe {
        #[serde(default)]
        action: String,
    }

    serde_json::from_str::<ActionProbe>(json_data)
        .is_ok_and(|probe| probe.action == PRESENCE_ACTION)
}

/// The page window of a snapshot request, already clamped to the limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PresencePage {
    /// Members per page, from 1 up to [`MAX_PAGE_SIZE`].
    pub limit: u32,
    /// Members to skip, from 0 up to [`MAX_PAGE_OFFSET`].
    pub offset: u64,
}

impl Default for PresencePage {
    fn default() -> Self {
        Self { limit: DEFAULT_PAGE_SIZE, offset: 0 }
    }
}

/// One presence operation, as it arrives inside a `perform` command's `data`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceCommand {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// The reserved action name. Anything else is rejected.
    pub action: String,
    /// The operation. Unknown values are a hard decode error.
    pub op: PresenceOp,
    /// Snapshot page size. Absent means [`DEFAULT_PAGE_SIZE`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Snapshot page offset. Absent means `0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
}

impl PresenceCommand {
    /// Build a command for `op` (used by tests, fixtures, and the Ruby
    /// helper's shape documentation).
    #[must_use]
    pub fn new(op: PresenceOp) -> Self {
        Self {
            version: v1::VERSION,
            action: PRESENCE_ACTION.to_string(),
            op,
            limit: None,
            offset: None,
        }
    }

    /// Attach a snapshot page window.
    #[must_use]
    pub const fn with_page(mut self, limit: u32, offset: u64) -> Self {
        self.limit = Some(limit);
        self.offset = Some(offset);
        self
    }

    /// Decode one `perform` payload as a presence command.
    ///
    /// Strict: this runs only after [`is_presence_action`] said the payload is
    /// ours, so a decode failure is a client protocol error rather than an
    /// application action that merely looks unfamiliar.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::Malformed`] when the payload does not decode (wrong
    /// types, absent or unknown `op`), and the [`Envelope::validate`] error
    /// for a foreign `action`, an unsupported `version`, or a page window out
    /// of range.
    pub fn decode(json_data: &str) -> Result<Self, ProtocolError> {
        let command: Self = serde_json::from_str(json_data).map_err(ProtocolError::Malformed)?;
        command.validate()?;
        Ok(command)
    }

    /// The requested page, defaulted and (already validated) in range.
    #[must_use]
    pub fn page(&self) -> PresencePage {
        PresencePage {
            limit: self.limit.unwrap_or(DEFAULT_PAGE_SIZE),
            offset: self.offset.unwrap_or(0),
        }
    }
}

impl Envelope for PresenceCommand {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;

        if self.action != PRESENCE_ACTION {
            return Err(ProtocolError::DisallowedValue {
                field: "action",
                value: self.action.clone(),
            });
        }

        if let Some(limit) = self.limit
            && (limit == 0 || limit > MAX_PAGE_SIZE)
        {
            return Err(ProtocolError::OutOfRange {
                field: "limit",
                value: u64::from(limit),
                max: u64::from(MAX_PAGE_SIZE),
            });
        }

        if let Some(offset) = self.offset
            && offset > MAX_PAGE_OFFSET
        {
            return Err(ProtocolError::OutOfRange {
                field: "offset",
                value: offset,
                max: MAX_PAGE_OFFSET,
            });
        }

        Ok(())
    }
}

/// What a server-to-client presence message reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PresenceEvent {
    /// A subject gained its first live lease on this stream.
    Join,
    /// A subject lost its last live lease on this stream.
    Leave,
    /// One page of the roster, answering a `snapshot` request.
    Snapshot,
    /// A refused operation, answering the command that was refused. Carries
    /// the refusal's `reason` code; never a roster.
    Error,
}

impl PresenceEvent {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Join => "join",
            Self::Leave => "leave",
            Self::Snapshot => "snapshot",
            Self::Error => "error",
        }
    }
}

/// Maximum accepted length of an error `reason` code, in bytes.
pub const MAX_REASON_LEN: usize = 64;

super::display_as_wire_str!(PresenceEvent);

/// One subject in a snapshot page.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceMember {
    /// The granted subject id. Never client-chosen.
    #[serde(default)]
    pub subject_id: String,
    /// The public metadata Rails granted with the subject.
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

impl PresenceMember {
    /// Semantic validation of one member entry.
    ///
    /// # Errors
    ///
    /// The [`validate_subject_id`] error for an empty or oversized subject,
    /// then the [`validate_metadata`] error for oversized metadata.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_subject_id(&self.subject_id)?;
        validate_metadata(&self.metadata)
    }
}

/// The page window a snapshot answer was produced with.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresencePageInfo {
    /// The page size actually applied.
    #[serde(default)]
    pub limit: u32,
    /// The offset actually applied.
    #[serde(default)]
    pub offset: u64,
    /// Whether more members exist past this page.
    #[serde(default)]
    pub has_more: bool,
}

/// One addressed presence message, carried as the `message` body of an
/// ordinary ActionCable frame.
///
/// Join and leave are *subject-level* transitions: multiple sessions (tabs,
/// devices) of one subject produce exactly one join and, after the last one
/// goes, exactly one leave.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceMessage {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Always [`PRESENCE_MESSAGE_TYPE`]; lets a client dispatch on `type`
    /// exactly as it does for every other Orbitcast application message.
    #[serde(rename = "type")]
    pub message_type: String,
    /// Which transition this message reports.
    pub event: PresenceEvent,
    /// The logical stream the roster belongs to. Required for every roster
    /// event; present on an `error` only when exactly one roster was
    /// addressed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stream: String,
    /// The subject that joined or left. Absent on a snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_id: Option<String>,
    /// The joining subject's public metadata. Absent on leave and snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Map<String, Value>>,
    /// Distinct live subjects on the stream after this transition.
    ///
    /// Eventually consistent by construction: it is a count taken under the
    /// membership lock at transition time, not a subscription-time invariant.
    #[serde(default)]
    pub total: u64,
    /// Snapshot members, in stable `subject_id` order. Empty for join/leave.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<PresenceMember>,
    /// Page window of a snapshot. Absent for join/leave.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<PresencePageInfo>,
    /// The refusal code of an `error`: one of the
    /// [`crate::presence::PresenceError::code`] values. Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The operation an `error` refused, when the command decoded far enough
    /// to name one. Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<PresenceOp>,
}

impl PresenceMessage {
    fn base(event: PresenceEvent, stream: impl Into<String>) -> Self {
        Self {
            version: v1::VERSION,
            message_type: PRESENCE_MESSAGE_TYPE.to_string(),
            event,
            stream: stream.into(),
            subject_id: None,
            metadata: None,
            total: 0,
            members: Vec::new(),
            page: None,
            reason: None,
            op: None,
        }
    }

    /// A subject-level join.
    pub fn join(
        stream: impl Into<String>,
        subject_id: impl Into<String>,
        metadata: Map<String, Value>,
        total: u64,
    ) -> Self {
        Self {
            subject_id: Some(subject_id.into()),
            metadata: Some(metadata),
            total,
            ..Self::base(PresenceEvent::Join, stream)
        }
    }

    /// A subject-level leave. Metadata is not repeated: the subject is gone.
    pub fn leave(stream: impl Into<String>, subject_id: impl Into<String>, total: u64) -> Self {
        Self {
            subject_id: Some(subject_id.into()),
            total,
            ..Self::base(PresenceEvent::Leave, stream)
        }
    }

    /// One roster page.
    pub fn snapshot(
        stream: impl Into<String>,
        members: Vec<PresenceMember>,
        total: u64,
        page: PresencePageInfo,
    ) -> Self {
        Self { members, total, page: Some(page), ..Self::base(PresenceEvent::Snapshot, stream) }
    }

    /// A refused operation. `stream` is the roster addressed, when exactly
    /// one was; `op` the operation, when the command decoded far enough to
    /// name it.
    pub fn error(reason: impl Into<String>, op: Option<PresenceOp>, stream: Option<&str>) -> Self {
        Self {
            reason: Some(reason.into()),
            op,
            ..Self::base(PresenceEvent::Error, stream.unwrap_or_default())
        }
    }

    /// Encode as the exact JSON text of one ActionCable `message` body.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::Malformed`] if `serde_json` refuses the message. Every
    /// field is a string, integer, or string-keyed map, so that does not
    /// happen in practice; the error path keeps an encoding bug off the panic
    /// path.
    pub fn to_json(&self) -> Result<String, ProtocolError> {
        serde_json::to_string(self).map_err(ProtocolError::Malformed)
    }
}

impl Envelope for PresenceMessage {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;

        if self.message_type != PRESENCE_MESSAGE_TYPE {
            return Err(ProtocolError::DisallowedValue {
                field: "type",
                value: self.message_type.clone(),
            });
        }

        // Only an error may leave the stream out: a refusal of a command that
        // never resolved to one roster has none to name.
        if self.event != PresenceEvent::Error || !self.stream.is_empty() {
            validate_stream(&self.stream)?;
        }

        match self.event {
            PresenceEvent::Error => {
                let reason =
                    self.reason.as_deref().ok_or(ProtocolError::EmptyField { field: "reason" })?;
                validate_bounded("reason", reason, MAX_REASON_LEN)?;
                if !self.members.is_empty() {
                    return Err(ProtocolError::NotAllowedForOutcome {
                        field: "members",
                        outcome: "error",
                    });
                }
            }
            PresenceEvent::Join | PresenceEvent::Leave => {
                let subject_id = self
                    .subject_id
                    .as_deref()
                    .ok_or(ProtocolError::EmptyField { field: "subject_id" })?;
                validate_subject_id(subject_id)?;
                if let Some(ref metadata) = self.metadata {
                    validate_metadata(metadata)?;
                }
                if !self.members.is_empty() {
                    return Err(ProtocolError::NotAllowedForOutcome {
                        field: "members",
                        outcome: "transition",
                    });
                }
            }
            PresenceEvent::Snapshot => {
                let page = self.page.ok_or(ProtocolError::MissingPayload {
                    operation: "snapshot",
                    field: "page",
                })?;
                if page.limit == 0 || page.limit > MAX_PAGE_SIZE {
                    return Err(ProtocolError::OutOfRange {
                        field: "limit",
                        value: u64::from(page.limit),
                        max: u64::from(MAX_PAGE_SIZE),
                    });
                }
                if self.members.len() > page.limit as usize {
                    return Err(ProtocolError::TooMany {
                        field: "members",
                        count: self.members.len(),
                        max: page.limit as usize,
                    });
                }
                for member in &self.members {
                    member.validate()?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
