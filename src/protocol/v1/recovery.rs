//! v1 recovery envelopes.
//!
//! Recovery is **opt-in** and rides ordinary ActionCable frames, exactly like
//! presence: there is no transport extension, and an unchanged ActionCable or
//! Turbo client never sees any of this. Everything below only exists on a
//! subscription to the reserved `Orbitcast::RecoveryChannel`.
//!
//! ```text
//! client -> server   {"command":"subscribe",
//!                     "identifier":"{\"channel\":\"Orbitcast::RecoveryChannel\",
//!                                     \"token\":\"<recovery token>\"}"}
//! client -> server   {"command":"message","identifier":"{...}",
//!                     "data":"{\"action\":\"orbitcast_recovery\",\"op\":\"resume\",
//!                              \"cursors\":{\"chat:42\":\"<opaque>\"}}"}
//! server -> client   {"identifier":"{...}","message":{"version":1,
//!                     "type":"orbitcast_delivery","stream":"chat:42",
//!                     "publication_id":"<uuid>","generation":"1","sequence":"7",
//!                     "cursor":"<opaque>","json_payload":"{\"body\":\"hi\"}"}}
//! ```
//!
//! # Three wire envelopes, one stored one
//!
//! | envelope | direction | purpose |
//! | --- | --- | --- |
//! | [`DeliveryEnvelope`] | server -> client | one retained publication, with its identity and order |
//! | [`RecoveryCommand`] | client -> server | `resume` / `ack`, carrying opaque cursors |
//! | [`RecoveryMessage`] | server -> client | `replay_start` / `replay_done` / `gap` |
//! | [`SessionSnapshot`] | server -> store | the cursors a session last acknowledged |
//!
//! A [`SessionSnapshot`] never travels to a browser: it is the versioned JSON
//! the broker stores per recovery session (`orbitcast_sessions.snapshot`) and
//! reads back when a client resumes without cursors of its own.
//!
//! # Cursors are opaque
//!
//! A [`Cursor`] is a `(generation, sequence)` pair for one stream, and it
//! travels as `base64(compact JSON)`:
//!
//! ```text
//! {"v":1,"g":"1","s":"7"}   ->   eyJ2IjoxLCJnIjoiMSIsInMiOiI3In0=
//! ```
//!
//! Both numbers are **decimal strings**, not JSON numbers: a 64-bit sequence
//! exceeds `Number.MAX_SAFE_INTEGER`, and a browser that round-trips the cursor
//! through `JSON.parse`/`JSON.stringify` must not silently round it. The helper
//! never inspects the value at all - it stores and returns the string - so the
//! encoding stays a server-side detail that can change without a client change.
//!
//! Key order (`v`, `g`, `s`) is fixed so the same `(generation, sequence)`
//! always encodes to the same bytes and fixtures stay deterministic.
//!
//! # At-least-once, within retention
//!
//! An `ack` is a *receipt*, not proof that the browser applied anything. Replay
//! is at-least-once inside the retention window: the same publication may be
//! delivered twice across a replay-to-live handoff, and
//! [`DeliveryEnvelope::publication_id`] is the stable id a client deduplicates
//! on. History outside retention is not recoverable and is reported as a
//! [`GapReason`], which the application resolves by resynchronizing.
//!
//! # Tolerance and failure
//!
//! | wire condition | result |
//! | --- | --- |
//! | unknown field | ignored |
//! | absent `version` | reads as `1` |
//! | unknown `op` / `event` / `reason` | hard decode error |
//! | `action` other than `orbitcast_recovery` | semantic rejection |
//! | malformed or foreign-version cursor | semantic rejection |
//! | more than [`MAX_RECOVERY_STREAMS`] streams | semantic rejection |

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

use crate::protocol::v1::{
    self, default_version, now_unix, validate_bounded, validate_json_text, validate_namespace,
    validate_not_expired, validate_stream, validate_uuid, validate_version,
};
use crate::protocol::{Envelope, ProtocolError};

/// The only purpose a recovery token may carry.
///
/// Deliberately *not* [`crate::protocol::v1::claims::PURPOSE_SUBSCRIBE`]: a
/// subscribe token can never be replayed as a recovery capability, and the two
/// are signed with different keys besides.
pub const PURPOSE_RECOVER: &str = "recover";

/// The reserved channel recovery negotiation happens on.
pub const RECOVERY_CHANNEL: &str = "Orbitcast::RecoveryChannel";

/// The reserved action name recovery commands travel under.
pub const RECOVERY_ACTION: &str = "orbitcast_recovery";

/// The `type` of every server-to-client recovery control message.
pub const RECOVERY_MESSAGE_TYPE: &str = "orbitcast_recovery";

/// The `type` of every retained-publication delivery.
pub const DELIVERY_MESSAGE_TYPE: &str = "orbitcast_delivery";

/// Wire version of the encoded cursor payload.
pub const CURSOR_VERSION: u32 = 1;

/// Maximum accepted encoded cursor length, in bytes.
pub const MAX_CURSOR_LEN: usize = 256;

/// Maximum number of streams one recovery token (or one command) may name.
pub const MAX_RECOVERY_STREAMS: usize = 64;

/// Maximum accepted recovery session id length, in bytes.
pub const MAX_SESSION_ID_LEN: usize = 256;

/// A position in one stream's retained history.
///
/// `generation` guards against a retention wipe: when a stream's history is
/// dropped the generation is bumped, so an old cursor is recognizably stale
/// instead of silently matching sequence numbers from a new era.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Cursor {
    /// History generation this position belongs to.
    pub generation: u64,
    /// Last sequence the client acknowledges having received.
    pub sequence: u64,
}

/// The compact JSON a cursor decodes from. Numbers travel as decimal strings.
///
/// Decode-only: [`Cursor::encode`] writes the same fixed shape directly.
#[derive(Debug, Deserialize)]
struct CursorPayload {
    #[serde(rename = "v", default = "default_cursor_version")]
    version: u32,
    #[serde(rename = "g")]
    generation: String,
    #[serde(rename = "s")]
    sequence: String,
}

const fn default_cursor_version() -> u32 {
    CURSOR_VERSION
}

impl Cursor {
    /// Build a cursor for `(generation, sequence)`.
    #[must_use]
    pub const fn new(generation: u64, sequence: u64) -> Self {
        Self { generation, sequence }
    }

    /// Encode as the opaque string a client stores and echoes back.
    #[must_use]
    pub fn encode(&self) -> String {
        // Fixed field order (v, g, s) keeps the encoding deterministic. Written
        // directly rather than through a serializer: the values are decimal
        // digits, which never need JSON escaping, so this cannot fail and is
        // byte-identical to the serde form `CursorPayload` decodes.
        let json = format!(
            r#"{{"v":{CURSOR_VERSION},"g":"{}","s":"{}"}}"#,
            self.generation, self.sequence
        );
        BASE64.encode(json)
    }

    /// Decode an opaque cursor string.
    ///
    /// Every failure is a semantic rejection: a client that sends a truncated,
    /// hand-written, or foreign-version cursor is told to resynchronize rather
    /// than being replayed from a guessed position.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::EmptyField`] for empty text,
    /// [`ProtocolError::FieldTooLong`] above [`MAX_CURSOR_LEN`] bytes,
    /// [`ProtocolError::UnsupportedVersion`] for a cursor version other than
    /// [`CURSOR_VERSION`], and [`ProtocolError::DisallowedValue`] for anything
    /// that is not strict base64 of the documented JSON with decimal-string
    /// `u64` fields.
    pub fn decode(text: &str) -> Result<Self, ProtocolError> {
        if text.is_empty() {
            return Err(ProtocolError::EmptyField { field: "cursor" });
        }
        if text.len() > MAX_CURSOR_LEN {
            return Err(ProtocolError::FieldTooLong {
                field: "cursor",
                len: text.len(),
                max: MAX_CURSOR_LEN,
            });
        }

        let invalid =
            || ProtocolError::DisallowedValue { field: "cursor", value: text.to_string() };

        let decoded = BASE64.decode(text).map_err(|_| invalid())?;
        let payload: CursorPayload = serde_json::from_slice(&decoded).map_err(|_| invalid())?;
        if payload.version != CURSOR_VERSION {
            return Err(ProtocolError::UnsupportedVersion(payload.version));
        }

        Ok(Self {
            generation: payload.generation.parse::<u64>().map_err(|_| invalid())?,
            sequence: payload.sequence.parse::<u64>().map_err(|_| invalid())?,
        })
    }
}

/// Validate a map of `stream -> encoded cursor`.
///
/// Checks the stream names, the cursor encodings, and the map size, so a
/// resume request is fully understood before any history is read.
///
/// # Errors
///
/// [`ProtocolError::TooMany`] above [`MAX_RECOVERY_STREAMS`] entries, the
/// [`v1::validate_stream`] error for a bad stream name, and the
/// [`Cursor::decode`] error for the first cursor that does not decode.
pub fn validate_cursors(cursors: &BTreeMap<String, String>) -> Result<(), ProtocolError> {
    if cursors.len() > MAX_RECOVERY_STREAMS {
        return Err(ProtocolError::TooMany {
            field: "cursors",
            count: cursors.len(),
            max: MAX_RECOVERY_STREAMS,
        });
    }

    for (stream, cursor) in cursors {
        validate_stream(stream)?;
        Cursor::decode(cursor)?;
    }

    Ok(())
}

/// Claims carried by a recovery token.
///
/// Same wire shape as a subscribe token - `base64(JSON(claims))--hex(HMAC)` -
/// but signed with the **dedicated recovery key** and carrying
/// `purpose: "recover"` plus the exact stream set it may replay. A recovery
/// token is a capability Rails minted; it can never widen access, because the
/// streams are named in the signed claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryClaims {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Namespace the token is scoped to. Absent defaults to empty (invalid).
    #[serde(default)]
    pub namespace: String,
    /// Application-chosen session identity. Absent defaults to empty (invalid).
    ///
    /// Identity *reference* only: never a cookie, never Marshal data, never
    /// sensitive connection state.
    #[serde(default)]
    pub session_id: String,
    /// Streams this token may replay. Absent defaults to empty, which replays
    /// nothing.
    #[serde(default)]
    pub streams: Vec<String>,
    /// Granted purpose; must be [`PURPOSE_RECOVER`]. Absent defaults to empty,
    /// which grants nothing.
    #[serde(default)]
    pub purpose: String,
    /// Expiry in unix seconds. Absent defaults to `0`, i.e. already expired.
    #[serde(default)]
    pub expires_at: i64,
}

impl RecoveryClaims {
    /// Build recovery claims expiring at `expires_at` (unix seconds).
    pub fn recover(
        namespace: impl Into<String>,
        session_id: impl Into<String>,
        streams: Vec<String>,
        expires_at: i64,
    ) -> Self {
        Self {
            version: v1::VERSION,
            namespace: namespace.into(),
            session_id: session_id.into(),
            streams,
            purpose: PURPOSE_RECOVER.to_string(),
            expires_at,
        }
    }

    /// Validate against an explicit clock reading, in unix seconds.
    ///
    /// # Errors
    ///
    /// The first failed check, in order: [`ProtocolError::UnsupportedVersion`],
    /// an empty or oversized `namespace` or `session_id`, an empty
    /// ([`ProtocolError::EmptyField`]) or oversized
    /// ([`ProtocolError::TooMany`]) stream list or an invalid stream name,
    /// [`ProtocolError::InvalidPurpose`] for anything but [`PURPOSE_RECOVER`],
    /// and [`ProtocolError::Expired`] when `expires_at` is not after `now`.
    pub fn validate_at(&self, now: i64) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        validate_namespace(&self.namespace)?;
        validate_bounded("session_id", &self.session_id, MAX_SESSION_ID_LEN)?;

        if self.streams.is_empty() {
            return Err(ProtocolError::EmptyField { field: "streams" });
        }
        if self.streams.len() > MAX_RECOVERY_STREAMS {
            return Err(ProtocolError::TooMany {
                field: "streams",
                count: self.streams.len(),
                max: MAX_RECOVERY_STREAMS,
            });
        }
        for stream in &self.streams {
            validate_stream(stream)?;
        }

        if self.purpose != PURPOSE_RECOVER {
            return Err(ProtocolError::InvalidPurpose {
                expected: PURPOSE_RECOVER,
                actual: self.purpose.clone(),
            });
        }

        validate_not_expired(self.expires_at, now)
    }

    /// Whether the claims authorize replaying `stream` in `namespace`.
    ///
    /// Semantic validation runs first: a token is never trusted because one of
    /// its stream names happens to match.
    #[must_use]
    pub fn authorizes(&self, namespace: &str, stream: &str, now: i64) -> bool {
        self.validate_at(now).is_ok()
            && self.namespace == namespace
            && self.streams.iter().any(|granted| granted == stream)
    }
}

impl Envelope for RecoveryClaims {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        self.validate_at(now_unix())
    }
}

/// One recovery operation a client may ask for.
///
/// Serialized `snake_case`. An unknown value is a hard decode error: a typo must
/// fail rather than fall through to a default operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOp {
    /// Replay everything committed strictly after the supplied cursors, then
    /// continue live.
    Resume,
    /// Record receipt up to the supplied cursors. Best effort, never a
    /// precondition for delivery.
    Ack,
}

impl RecoveryOp {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resume => "resume",
            Self::Ack => "ack",
        }
    }
}

super::display_as_wire_str!(RecoveryOp);

/// Cheap probe: does this `perform` payload address the recovery namespace?
///
/// Used to route a command *before* it is strictly parsed, so a malformed
/// recovery command is reported as a recovery error instead of being forwarded
/// as an application action.
#[must_use]
pub fn is_recovery_action(json_data: &str) -> bool {
    #[derive(Deserialize)]
    struct ActionProbe {
        #[serde(default)]
        action: String,
    }

    serde_json::from_str::<ActionProbe>(json_data)
        .is_ok_and(|probe| probe.action == RECOVERY_ACTION)
}

/// One recovery operation, as it arrives inside a `perform` command's `data`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCommand {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// The reserved action name. Anything else is rejected.
    pub action: String,
    /// The operation. Unknown values are a hard decode error.
    pub op: RecoveryOp,
    /// `stream -> opaque cursor`. Absent means "no known position".
    ///
    /// A `resume` without a cursor for a granted stream starts that stream at
    /// the live watermark: it asks for no history at all, rather than for all
    /// of it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cursors: BTreeMap<String, String>,
}

impl RecoveryCommand {
    /// Build a command for `op` with no cursors.
    #[must_use]
    pub fn new(op: RecoveryOp) -> Self {
        Self {
            version: v1::VERSION,
            action: RECOVERY_ACTION.to_string(),
            op,
            cursors: BTreeMap::new(),
        }
    }

    /// Attach one stream's cursor.
    #[must_use]
    pub fn with_cursor(mut self, stream: impl Into<String>, cursor: &Cursor) -> Self {
        self.cursors.insert(stream.into(), cursor.encode());
        self
    }

    /// Decode one `perform` payload as a recovery command.
    ///
    /// Strict: this runs only after [`is_recovery_action`] said the payload is
    /// ours, so a decode failure is a client protocol error.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::Malformed`] when the payload does not decode (wrong
    /// types, absent or unknown `op`), and the [`Envelope::validate`] error
    /// for a foreign `action`, an unsupported `version`, or a bad cursor map
    /// (see [`validate_cursors`]).
    pub fn decode(json_data: &str) -> Result<Self, ProtocolError> {
        let command: Self = serde_json::from_str(json_data).map_err(ProtocolError::Malformed)?;
        command.validate()?;
        Ok(command)
    }

    /// The requested cursors, decoded. Only call after [`Envelope::validate`].
    ///
    /// # Errors
    ///
    /// The [`Cursor::decode`] error for the first cursor that does not decode,
    /// which cannot happen once [`Envelope::validate`] has passed.
    pub fn decoded_cursors(&self) -> Result<BTreeMap<String, Cursor>, ProtocolError> {
        self.cursors
            .iter()
            .map(|(stream, cursor)| Cursor::decode(cursor).map(|c| (stream.clone(), c)))
            .collect()
    }
}

impl Envelope for RecoveryCommand {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;

        if self.action != RECOVERY_ACTION {
            return Err(ProtocolError::DisallowedValue {
                field: "action",
                value: self.action.clone(),
            });
        }

        validate_cursors(&self.cursors)
    }
}

/// What a server-to-client recovery control message reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryEvent {
    /// Replay has been fenced in and is about to start for these streams.
    ReplayStart,
    /// Every retained message up to the captured watermark has been sent;
    /// delivery continues live from the carried cursors.
    ReplayDone,
    /// One stream cannot be replayed from the supplied cursor. The application
    /// must resynchronize; nothing will be invented to fill the hole.
    Gap,
}

impl RecoveryEvent {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReplayStart => "replay_start",
            Self::ReplayDone => "replay_done",
            Self::Gap => "gap",
        }
    }
}

super::display_as_wire_str!(RecoveryEvent);

/// Why a stream could not be replayed.
///
/// Unknown values are a hard decode error: a client must never treat an
/// unrecognized reason as "probably fine".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    /// The requested position fell out of the retention window.
    Expired,
    /// The stream's history was wiped and restarted under a new generation.
    UnknownGeneration,
    /// Live traffic outran the bounded replay buffer.
    BufferOverflow,
    /// Another resume of the same session token won the fencing race.
    Fenced,
    /// The token does not grant this stream (any more).
    Unauthorized,
    /// The retained store could not be read.
    Unavailable,
}

impl GapReason {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::UnknownGeneration => "unknown_generation",
            Self::BufferOverflow => "buffer_overflow",
            Self::Fenced => "fenced",
            Self::Unauthorized => "unauthorized",
            Self::Unavailable => "unavailable",
        }
    }
}

super::display_as_wire_str!(GapReason);

/// One recovery control message, carried as the `message` body of an ordinary
/// ActionCable frame addressed with the recovery subscription's identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryMessage {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Always [`RECOVERY_MESSAGE_TYPE`].
    #[serde(rename = "type")]
    pub message_type: String,
    /// Which stage this message reports.
    pub event: RecoveryEvent,
    /// Streams a `replay_start` covers. Empty for the other events.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub streams: Vec<String>,
    /// The stream a `gap` is about. Absent for the other events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    /// Why a `gap` happened. Absent for the other events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<GapReason>,
    /// `stream -> opaque cursor` the client should hold after this message.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cursors: BTreeMap<String, String>,
}

impl RecoveryMessage {
    fn base(event: RecoveryEvent) -> Self {
        Self {
            version: v1::VERSION,
            message_type: RECOVERY_MESSAGE_TYPE.to_string(),
            event,
            streams: Vec::new(),
            stream: None,
            reason: None,
            cursors: BTreeMap::new(),
        }
    }

    /// Replay is starting for `streams`.
    #[must_use]
    pub fn replay_start(streams: Vec<String>) -> Self {
        Self { streams, ..Self::base(RecoveryEvent::ReplayStart) }
    }

    /// Replay finished; `cursors` is the position live delivery continues from.
    #[must_use]
    pub fn replay_done(cursors: BTreeMap<String, String>) -> Self {
        Self { cursors, ..Self::base(RecoveryEvent::ReplayDone) }
    }

    /// One stream cannot be replayed.
    pub fn gap(stream: impl Into<String>, reason: GapReason) -> Self {
        Self { stream: Some(stream.into()), reason: Some(reason), ..Self::base(RecoveryEvent::Gap) }
    }

    /// Encode as the exact JSON text of one ActionCable `message` body.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::Malformed`] if `serde_json` refuses the message. Every
    /// field is a string, integer, enum, or string-keyed map, so that does not
    /// happen in practice; the error path keeps an encoding bug off the panic
    /// path.
    pub fn to_json(&self) -> Result<String, ProtocolError> {
        serde_json::to_string(self).map_err(ProtocolError::Malformed)
    }
}

impl Envelope for RecoveryMessage {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;

        if self.message_type != RECOVERY_MESSAGE_TYPE {
            return Err(ProtocolError::DisallowedValue {
                field: "type",
                value: self.message_type.clone(),
            });
        }

        match self.event {
            RecoveryEvent::ReplayStart => {
                if self.streams.is_empty() {
                    return Err(ProtocolError::EmptyField { field: "streams" });
                }
                if self.streams.len() > MAX_RECOVERY_STREAMS {
                    return Err(ProtocolError::TooMany {
                        field: "streams",
                        count: self.streams.len(),
                        max: MAX_RECOVERY_STREAMS,
                    });
                }
                for stream in &self.streams {
                    validate_stream(stream)?;
                }
            }
            RecoveryEvent::ReplayDone => {}
            RecoveryEvent::Gap => {
                let stream =
                    self.stream.as_deref().ok_or(ProtocolError::EmptyField { field: "stream" })?;
                validate_stream(stream)?;
                if self.reason.is_none() {
                    return Err(ProtocolError::MissingPayload {
                        operation: "gap",
                        field: "reason",
                    });
                }
            }
        }

        validate_cursors(&self.cursors)
    }
}

/// One retained publication, delivered with the identity and order a client
/// needs to deduplicate and to resume.
///
/// The application payload travels as [`Self::json_payload`] - the exact
/// Rails-encoded JSON *text*, byte for byte, never re-encoded - matching
/// [`crate::protocol::v1::publication::Publication`]. A client parses it
/// itself, which is what keeps float and unicode escaping intact end to end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryEnvelope {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Always [`DELIVERY_MESSAGE_TYPE`].
    #[serde(rename = "type")]
    pub message_type: String,
    /// Logical stream this publication belongs to.
    #[serde(default)]
    pub stream: String,
    /// The publication's stable UUID: the deduplication key across a
    /// replay-to-live handoff and across reconnects.
    #[serde(default)]
    pub publication_id: String,
    /// History generation, as a decimal string (JS-safe).
    #[serde(default)]
    pub generation: String,
    /// Per-stream sequence, as a decimal string (JS-safe).
    #[serde(default)]
    pub sequence: String,
    /// The opaque cursor a client stores after accepting this delivery.
    #[serde(default)]
    pub cursor: String,
    /// Exact Rails-encoded JSON text of the application payload.
    #[serde(default)]
    pub json_payload: String,
}

impl DeliveryEnvelope {
    /// Build a delivery envelope, deriving the cursor from the position.
    pub fn new(
        stream: impl Into<String>,
        publication_id: impl Into<String>,
        position: Cursor,
        json_payload: impl Into<String>,
    ) -> Self {
        Self {
            version: v1::VERSION,
            message_type: DELIVERY_MESSAGE_TYPE.to_string(),
            stream: stream.into(),
            publication_id: publication_id.into(),
            generation: position.generation.to_string(),
            sequence: position.sequence.to_string(),
            cursor: position.encode(),
            json_payload: json_payload.into(),
        }
    }

    /// The position this delivery occupies. Only call after validation.
    ///
    /// # Errors
    ///
    /// The [`Cursor::decode`] error when [`Self::cursor`] does not decode,
    /// which cannot happen once [`Envelope::validate`] has passed.
    pub fn position(&self) -> Result<Cursor, ProtocolError> {
        Cursor::decode(&self.cursor)
    }

    /// Encode as the exact JSON text of one ActionCable `message` body.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::Malformed`] if `serde_json` refuses the message. Every
    /// field is a string, integer, enum, or string-keyed map, so that does not
    /// happen in practice; the error path keeps an encoding bug off the panic
    /// path.
    pub fn to_json(&self) -> Result<String, ProtocolError> {
        serde_json::to_string(self).map_err(ProtocolError::Malformed)
    }
}

impl Envelope for DeliveryEnvelope {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;

        if self.message_type != DELIVERY_MESSAGE_TYPE {
            return Err(ProtocolError::DisallowedValue {
                field: "type",
                value: self.message_type.clone(),
            });
        }

        validate_stream(&self.stream)?;
        validate_uuid("publication_id", &self.publication_id)?;
        validate_json_text("json_payload", &self.json_payload)?;

        let numeric = |field: &'static str, value: &str| -> Result<u64, ProtocolError> {
            value
                .parse::<u64>()
                .map_err(|_| ProtocolError::DisallowedValue { field, value: value.to_string() })
        };
        let generation = numeric("generation", &self.generation)?;
        let sequence = numeric("sequence", &self.sequence)?;

        // The cursor is not a second opinion: it must name the same position
        // the envelope already reports, or a client resuming from it would ask
        // for a different place than the one it just received.
        let cursor = Cursor::decode(&self.cursor)?;
        if cursor != Cursor::new(generation, sequence) {
            return Err(ProtocolError::Mismatch {
                field: "cursor",
                expected: format!("{generation}/{sequence}"),
                actual: format!("{}/{}", cursor.generation, cursor.sequence),
            });
        }

        Ok(())
    }
}

/// The versioned JSON stored as a recovery session snapshot.
///
/// Cursors and the session identity *reference* only. Never browser cookies,
/// never Ruby Marshal data, never sensitive connection attributes
/// a snapshot is restored into a fresh authorization decision, it
/// does not carry one. Because it holds nothing but stream names and opaque
/// positions, it is stored in clear: there is nothing in it worth encrypting,
/// and a leaked snapshot grants no access - replay still needs a valid
/// recovery token *and* a live subscription to every stream it names.
///
/// Read back on a `resume` that carries no cursors: the server-side memory of
/// where a reconnecting session left off (see `crate::recovery`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// The session identity the token named. Absent defaults to empty
    /// (invalid).
    #[serde(default)]
    pub session_id: String,
    /// `stream -> opaque cursor`, exactly as the client acknowledged it.
    #[serde(default)]
    pub cursors: BTreeMap<String, String>,
}

impl SessionSnapshot {
    /// Build a snapshot for `session_id`.
    pub fn new(session_id: impl Into<String>, cursors: BTreeMap<String, String>) -> Self {
        Self { version: v1::VERSION, session_id: session_id.into(), cursors }
    }

    /// Decode and validate one stored snapshot.
    ///
    /// A stored row gets the same validation an inbound envelope gets: a
    /// snapshot that does not decode is not "probably fine", it is ignored
    /// and the session resumes from the live edge.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::Malformed`] when the text is not the documented JSON,
    /// and the [`Envelope::validate`] error for an unsupported `version`, an
    /// empty or oversized `session_id`, or an unusable cursor map (see
    /// [`validate_cursors`]).
    pub fn decode(json: &str) -> Result<Self, ProtocolError> {
        let snapshot: Self = serde_json::from_str(json).map_err(ProtocolError::Malformed)?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Encode for storage.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::Malformed`] if `serde_json` fails to encode the
    /// snapshot; with string keys and string values only, it does not.
    pub fn to_json(&self) -> Result<String, ProtocolError> {
        serde_json::to_string(self).map_err(ProtocolError::Malformed)
    }

    /// The stored cursors, decoded. Only call after [`Envelope::validate`].
    ///
    /// # Errors
    ///
    /// The [`Cursor::decode`] error for the first cursor that does not decode,
    /// which cannot happen once [`Envelope::validate`] has passed.
    pub fn decoded_cursors(&self) -> Result<BTreeMap<String, Cursor>, ProtocolError> {
        self.cursors
            .iter()
            .map(|(stream, cursor)| Cursor::decode(cursor).map(|c| (stream.clone(), c)))
            .collect()
    }
}

impl Envelope for SessionSnapshot {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        validate_bounded("session_id", &self.session_id, MAX_SESSION_ID_LEN)?;
        validate_cursors(&self.cursors)
    }
}

#[cfg(test)]
mod tests;
