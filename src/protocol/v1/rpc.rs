//! v1 server-to-Rails RPC envelopes.
//!
//! One request envelope and one reply envelope cover every operation. The
//! operation is a tagged value (`identify`, `open_channel`, ...) and selects
//! which payload the request must carry; the reply carries an explicit
//! `outcome`, a new state revision, an optional state replacement, and an
//! **ordered** list of typed effects.
//!
//! # What is authoritative here
//!
//! Rust is the strict validator of Rails replies. Parsing is tolerant in
//! exactly the documented ways - unknown fields ignored, absent fields
//! defaulted - and everything else fails closed:
//!
//! | wire condition | result |
//! | --- | --- |
//! | unknown field | ignored |
//! | absent `version` | reads as `1` |
//! | absent `outcome` | reads as `fault` (never `accepted`) |
//! | unknown `outcome` value | hard decode error |
//! | unknown effect `kind` | hard decode error |
//! | unknown `operation` value | hard decode error |
//! | absent `state` / explicit `null` | state unchanged |
//! | `state: {}` | state cleared |
//! | effects on a non-accepted reply | rejected |
//!
//! Ownership of the identifiers an effect names is *not* checked here: the
//! envelope only guarantees the identifiers are present and bounded. The
//! session reducer in `crate::server` is what verifies that this connection
//! actually owns them.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::protocol::v1::{
    self, default_version, validate_bounded, validate_json_text, validate_namespace,
    validate_stream, validate_uuid, validate_version,
};
use crate::protocol::{Envelope, ProtocolError};

/// Maximum number of effects one reply may carry.
pub const MAX_EFFECTS: usize = 64;

/// Maximum accepted ActionCable identifier length, in bytes.
pub const MAX_IDENTIFIER_LEN: usize = 4096;

/// Maximum accepted node-id length, in bytes.
pub const MAX_NODE_ID_LEN: usize = 128;

/// Maximum accepted original-request URL length, in bytes.
pub const MAX_URL_LEN: usize = 8192;

/// Maximum accepted client-address / origin length, in bytes.
pub const MAX_ADDRESS_LEN: usize = 256;

/// Maximum number of forwarded (allowlisted) headers.
pub const MAX_HEADERS: usize = 32;

/// Maximum accepted header name length, in bytes.
pub const MAX_HEADER_NAME_LEN: usize = 128;

/// Maximum accepted header value length, in bytes.
pub const MAX_HEADER_VALUE_LEN: usize = 8192;

/// Maximum number of entries in one declared-state or identity map.
pub const MAX_STATE_ENTRIES: usize = 64;

/// Maximum accepted state/identity key length, in bytes.
pub const MAX_STATE_KEY_LEN: usize = 128;

/// Maximum encoded size of one declared-state or identity map, in bytes.
pub const MAX_STATE_LEN: usize = 64 * 1024;

/// Maximum accepted `transmit` payload size, in bytes.
pub const MAX_TRANSMIT_LEN: usize = v1::MAX_PAYLOAD_LEN;

/// Maximum number of streams one subscription snapshot may report.
pub const MAX_SUBSCRIPTION_STREAMS: usize = 64;

/// Maximum number of subscriptions a release snapshot may report.
pub const MAX_RELEASE_SUBSCRIPTIONS: usize = 256;

/// Maximum accepted diagnostic `reason` length, in bytes.
pub const MAX_REASON_LEN: usize = 256;

/// Maximum accepted presence subject-id length, in bytes.
pub const MAX_SUBJECT_ID_LEN: usize = 256;

/// Maximum encoded size of presence metadata, in bytes.
pub const MAX_PRESENCE_METADATA_LEN: usize = 4096;

/// Maximum number of allowed presence operations in one grant.
pub const MAX_PRESENCE_OPERATIONS: usize = 16;

/// Maximum accepted end-to-end RPC deadline, in milliseconds.
pub const MAX_DEADLINE_MS: u64 = 60_000;

/// Maximum number of capabilities a `describe` reply may advertise.
pub const MAX_CAPABILITIES: usize = 64;

/// Maximum accepted software-version / capability string length, in bytes.
pub const MAX_LABEL_LEN: usize = 128;

/// Standard ActionCable disconnect reason: authentication or authorization
/// refused. Paired with `reconnect: false`.
pub const CLOSE_REASON_UNAUTHORIZED: &str = "unauthorized";

/// Standard ActionCable disconnect reason: the client sent something invalid.
pub const CLOSE_REASON_INVALID_REQUEST: &str = "invalid_request";

/// Standard ActionCable disconnect reason: this node is going away.
pub const CLOSE_REASON_SERVER_RESTART: &str = "server_restart";

/// Standard ActionCable disconnect reason: closed on the application's behalf
/// (remote sign-out, multi-device logout).
pub const CLOSE_REASON_REMOTE: &str = "remote";

/// The only disconnect reasons Rails may ask for.
///
/// Rails does not get to invent wire-visible close reasons: the browser-facing
/// `disconnect` frame is framed by Rust, and the ActionCable client only
/// understands these.
pub const ALLOWED_CLOSE_REASONS: &[&str] = &[
    CLOSE_REASON_UNAUTHORIZED,
    CLOSE_REASON_INVALID_REQUEST,
    CLOSE_REASON_SERVER_RESTART,
    CLOSE_REASON_REMOTE,
];

/// Headers that are never forwarded to Rails, whatever the allowlist says.
///
/// These describe *this* hop's connection, and reconstructing them at the
/// Rails end would be either meaningless or a request-smuggling hazard.
pub const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Reject a close reason outside the ActionCable allowlist.
///
/// # Errors
///
/// [`ProtocolError::EmptyField`] for an empty reason, and
/// [`ProtocolError::DisallowedValue`] for one not in [`ALLOWED_CLOSE_REASONS`].
pub fn validate_close_reason(reason: &str) -> Result<(), ProtocolError> {
    if reason.is_empty() {
        return Err(ProtocolError::EmptyField { field: "reason" });
    }
    if ALLOWED_CLOSE_REASONS.contains(&reason) {
        Ok(())
    } else {
        Err(ProtocolError::DisallowedValue { field: "reason", value: reason.to_string() })
    }
}

/// Reject an unbounded declared-state or identity map.
///
/// Bounds the entry count, each key, and the *encoded* size: a single huge
/// value is as expensive as many small ones.
///
/// # Errors
///
/// [`ProtocolError::TooMany`] above [`MAX_STATE_ENTRIES`] entries, an empty
/// or oversized (over [`MAX_STATE_KEY_LEN`]) key, and
/// [`ProtocolError::FieldTooLong`] when the encoded map exceeds
/// [`MAX_STATE_LEN`] bytes.
pub fn validate_state_map(
    field: &'static str,
    map: &Map<String, Value>,
) -> Result<(), ProtocolError> {
    if map.len() > MAX_STATE_ENTRIES {
        return Err(ProtocolError::TooMany { field, count: map.len(), max: MAX_STATE_ENTRIES });
    }
    for key in map.keys() {
        validate_bounded("state_key", key, MAX_STATE_KEY_LEN)?;
    }

    let encoded = serde_json::to_string(map).map_err(ProtocolError::Malformed)?;
    if encoded.len() > MAX_STATE_LEN {
        return Err(ProtocolError::FieldTooLong { field, len: encoded.len(), max: MAX_STATE_LEN });
    }
    Ok(())
}

/// Reject an empty or oversized ActionCable identifier.
const fn validate_identifier(identifier: &str) -> Result<(), ProtocolError> {
    validate_bounded("identifier", identifier, MAX_IDENTIFIER_LEN)
}

/// The operations the server may ask Rails to run.
///
/// Serialized `snake_case`, and the same string is the last path segment of the
/// HTTP route (`POST {endpoint}/{operation}`). An unknown value is a hard
/// decode error, never a tolerated unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// Capability and readiness handshake; carries no session.
    Describe,
    /// Connection authentication and identification.
    Identify,
    /// Subscription authorization plus the `subscribed` callback.
    OpenChannel,
    /// The `unsubscribed` callback for a subscription the client already left.
    CloseChannel,
    /// Dispatch of one decoded `data["action"]`.
    RunAction,
    /// Remaining unsubscribe callbacks plus the disconnect callback.
    ReleaseConnection,
}

impl Operation {
    /// The wire (and URL path) spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Describe => "describe",
            Self::Identify => "identify",
            Self::OpenChannel => "open_channel",
            Self::CloseChannel => "close_channel",
            Self::RunAction => "run_action",
            Self::ReleaseConnection => "release_connection",
        }
    }

    /// Whether this operation participates in session state and revisions.
    ///
    /// `describe` does not: it is a node-level handshake with no session.
    #[must_use]
    pub const fn is_stateful(self) -> bool {
        !matches!(self, Self::Describe)
    }
}

super::display_as_wire_str!(Operation);

/// The authorization outcome of one operation.
///
/// `Fault` is the [`Default`], which is what an absent `outcome` reads as: a
/// truncated reply must never be mistaken for an acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Rails ran the operation and authorized it.
    Accepted,
    /// Rails ran the operation and refused it. Not an error: a decision.
    Denied,
    /// Rails could not run the operation (exception, timeout, overload).
    #[default]
    Fault,
}

impl Outcome {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Denied => "denied",
            Self::Fault => "fault",
        }
    }

    /// Whether effects and state replacements may be applied.
    #[must_use]
    pub const fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted)
    }
}

super::display_as_wire_str!(Outcome);

/// Everything Rails is allowed to know about the original WebSocket request.
///
/// Reconstructed from validated metadata only: the browser cannot submit this,
/// and a header that is not on the configured allowlist never appears here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionSnapshot {
    /// The original WebSocket request URL, query string included.
    #[serde(default)]
    pub url: String,
    /// The verified `Origin` header, if the client sent one.
    #[serde(default)]
    pub origin: Option<String>,
    /// The verified client address.
    #[serde(default)]
    pub client_address: String,
    /// Allowlisted request headers, names lowercased.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Identity attributes Rails returned from a previous `identify`.
    #[serde(default)]
    pub identity: Map<String, Value>,
    /// Declared connection state Rails returned from a previous operation.
    #[serde(default)]
    pub state: Map<String, Value>,
}

impl ConnectionSnapshot {
    /// Semantic validation of one connection snapshot.
    ///
    /// # Errors
    ///
    /// An empty or oversized `url`, `client_address`, or `origin`;
    /// [`ProtocolError::TooMany`] above [`MAX_HEADERS`] headers; an empty,
    /// oversized, non-lowercase, or hop-by-hop header name
    /// ([`HOP_BY_HOP_HEADERS`]); a header value over [`MAX_HEADER_VALUE_LEN`];
    /// and the [`validate_state_map`] error for `identity` or `state`.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_bounded("url", &self.url, MAX_URL_LEN)?;
        validate_bounded("client_address", &self.client_address, MAX_ADDRESS_LEN)?;
        if let Some(ref origin) = self.origin {
            validate_bounded("origin", origin, MAX_ADDRESS_LEN)?;
        }

        if self.headers.len() > MAX_HEADERS {
            return Err(ProtocolError::TooMany {
                field: "headers",
                count: self.headers.len(),
                max: MAX_HEADERS,
            });
        }
        for (name, value) in &self.headers {
            validate_bounded("header_name", name, MAX_HEADER_NAME_LEN)?;
            if name.chars().any(|c| c.is_ascii_uppercase() || c.is_whitespace()) {
                return Err(ProtocolError::DisallowedValue {
                    field: "header_name",
                    value: name.clone(),
                });
            }
            if HOP_BY_HOP_HEADERS.contains(&name.as_str()) {
                return Err(ProtocolError::DisallowedValue {
                    field: "header_name",
                    value: name.clone(),
                });
            }
            if value.len() > MAX_HEADER_VALUE_LEN {
                return Err(ProtocolError::FieldTooLong {
                    field: "header_value",
                    len: value.len(),
                    max: MAX_HEADER_VALUE_LEN,
                });
            }
        }

        validate_state_map("identity", &self.identity)?;
        validate_state_map("state", &self.state)
    }
}

/// One subscription as this server currently holds it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionSnapshot {
    /// The client's original identifier, byte for byte.
    #[serde(default)]
    pub identifier: String,
    /// The identifier decoded as channel params, for Rails' convenience. The
    /// `identifier` field remains authoritative.
    #[serde(default)]
    pub params: Map<String, Value>,
    /// Streams currently installed for this subscription.
    #[serde(default)]
    pub streams: Vec<String>,
    /// Declared channel state Rails returned from a previous operation.
    #[serde(default)]
    pub state: Map<String, Value>,
}

impl SubscriptionSnapshot {
    /// Build a snapshot from an identifier, decoding params when possible.
    ///
    /// A non-object identifier yields empty params rather than an error: the
    /// identifier itself is what addresses frames, and Rails decides whether
    /// the params are usable.
    pub fn new(identifier: impl Into<String>) -> Self {
        let identifier = identifier.into();
        let params = serde_json::from_str::<Map<String, Value>>(&identifier).unwrap_or_default();
        Self { identifier, params, streams: Vec::new(), state: Map::new() }
    }

    /// Semantic validation of one subscription snapshot.
    ///
    /// # Errors
    ///
    /// An empty or oversized `identifier`; [`ProtocolError::TooMany`] above
    /// [`MAX_SUBSCRIPTION_STREAMS`] streams or an invalid stream name; and the
    /// [`validate_state_map`] error for `params` or `state`.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_identifier(&self.identifier)?;
        if self.streams.len() > MAX_SUBSCRIPTION_STREAMS {
            return Err(ProtocolError::TooMany {
                field: "streams",
                count: self.streams.len(),
                max: MAX_SUBSCRIPTION_STREAMS,
            });
        }
        for stream in &self.streams {
            validate_stream(stream)?;
        }
        validate_state_map("params", &self.params)?;
        validate_state_map("state", &self.state)
    }
}

/// The `message`/`perform` payload of a `run_action` request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionPayload {
    /// The subscription the action targets.
    #[serde(default)]
    pub identifier: String,
    /// The exact client-encoded JSON text of the command's `data` field.
    ///
    /// Carried as text, not as a decoded value: Rails reads `data["action"]`
    /// out of the bytes the client actually sent.
    #[serde(default)]
    pub json_data: String,
}

impl ActionPayload {
    /// Semantic validation of one action payload.
    ///
    /// # Errors
    ///
    /// An empty or oversized `identifier`, and the
    /// [`v1::validate_json_text`] error when `json_data` is empty, oversized,
    /// or not one JSON value.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_identifier(&self.identifier)?;
        validate_json_text("json_data", &self.json_data)
    }
}

/// The final snapshot handed to `release_connection`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleasePayload {
    /// Why the connection went away, from [`ALLOWED_CLOSE_REASONS`].
    #[serde(default)]
    pub reason: String,
    /// The reconnect policy that was advertised to the client.
    #[serde(default)]
    pub reconnect: bool,
    /// Subscriptions still installed when the connection ended.
    #[serde(default)]
    pub subscriptions: Vec<SubscriptionSnapshot>,
}

impl ReleasePayload {
    /// Semantic validation of one release payload.
    ///
    /// # Errors
    ///
    /// The [`validate_close_reason`] error for `reason`,
    /// [`ProtocolError::TooMany`] above [`MAX_RELEASE_SUBSCRIPTIONS`]
    /// subscriptions, and the first [`SubscriptionSnapshot::validate`] error.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_close_reason(&self.reason)?;
        if self.subscriptions.len() > MAX_RELEASE_SUBSCRIPTIONS {
            return Err(ProtocolError::TooMany {
                field: "subscriptions",
                count: self.subscriptions.len(),
                max: MAX_RELEASE_SUBSCRIPTIONS,
            });
        }
        for subscription in &self.subscriptions {
            subscription.validate()?;
        }
        Ok(())
    }
}

/// One request from this node to the Rails endpoint.
///
/// `POST {endpoint}/{operation}`; the envelope's `operation` must match the
/// route, which is what stops a request being replayed against a different
/// operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcRequest {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// The operation being requested.
    pub operation: Operation,
    /// Routing/authorization namespace.
    #[serde(default)]
    pub namespace: String,
    /// Stable identifier of the node making the call.
    #[serde(default)]
    pub node_id: String,
    /// Server-generated session UUID. Nil UUID for `describe`.
    #[serde(default)]
    pub session_id: String,
    /// UUID of this request, for correlation and deduplication.
    #[serde(default)]
    pub request_id: String,
    /// Monotonic per-session command sequence.
    #[serde(default)]
    pub sequence: u64,
    /// The state revision this node believes the session is at.
    #[serde(default)]
    pub expected_revision: u64,
    /// End-to-end deadline, in milliseconds.
    #[serde(default)]
    pub deadline_ms: u64,
    /// Connection snapshot; required by every stateful operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<ConnectionSnapshot>,
    /// Subscription snapshot; required by the subscription operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription: Option<SubscriptionSnapshot>,
    /// Action payload; required by `run_action`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<ActionPayload>,
    /// Release payload; required by `release_connection`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<ReleasePayload>,
}

impl RpcRequest {
    /// Build a request envelope with a fresh request UUID.
    pub fn new(
        operation: Operation,
        namespace: impl Into<String>,
        node_id: impl Into<String>,
        session_id: impl Into<String>,
        sequence: u64,
        expected_revision: u64,
        deadline_ms: u64,
    ) -> Self {
        Self {
            version: v1::VERSION,
            operation,
            namespace: namespace.into(),
            node_id: node_id.into(),
            session_id: session_id.into(),
            request_id: uuid::Uuid::new_v4().to_string(),
            sequence,
            expected_revision,
            deadline_ms,
            connection: None,
            subscription: None,
            action: None,
            release: None,
        }
    }

    /// Attach the connection snapshot.
    #[must_use]
    pub fn with_connection(mut self, connection: ConnectionSnapshot) -> Self {
        self.connection = Some(connection);
        self
    }

    /// Attach the subscription snapshot.
    #[must_use]
    pub fn with_subscription(mut self, subscription: SubscriptionSnapshot) -> Self {
        self.subscription = Some(subscription);
        self
    }

    /// Attach the action payload.
    #[must_use]
    pub fn with_action(mut self, action: ActionPayload) -> Self {
        self.action = Some(action);
        self
    }

    /// Attach the release payload.
    #[must_use]
    pub fn with_release(mut self, release: ReleasePayload) -> Self {
        self.release = Some(release);
        self
    }

    /// The payload a given operation must carry.
    const fn require(&self, present: bool, field: &'static str) -> Result<(), ProtocolError> {
        if present {
            Ok(())
        } else {
            Err(ProtocolError::MissingPayload { operation: self.operation.as_str(), field })
        }
    }
}

impl Envelope for RpcRequest {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        validate_namespace(&self.namespace)?;
        validate_bounded("node_id", &self.node_id, MAX_NODE_ID_LEN)?;
        validate_uuid("session_id", &self.session_id)?;
        validate_uuid("request_id", &self.request_id)?;

        if self.deadline_ms == 0 || self.deadline_ms > MAX_DEADLINE_MS {
            return Err(ProtocolError::OutOfRange {
                field: "deadline_ms",
                value: self.deadline_ms,
                max: MAX_DEADLINE_MS,
            });
        }

        match self.operation {
            Operation::Describe => {}
            Operation::Identify => self.require(self.connection.is_some(), "connection")?,
            Operation::OpenChannel | Operation::CloseChannel => {
                self.require(self.connection.is_some(), "connection")?;
                self.require(self.subscription.is_some(), "subscription")?;
            }
            Operation::RunAction => {
                self.require(self.connection.is_some(), "connection")?;
                self.require(self.subscription.is_some(), "subscription")?;
                self.require(self.action.is_some(), "action")?;
            }
            Operation::ReleaseConnection => {
                self.require(self.connection.is_some(), "connection")?;
                self.require(self.release.is_some(), "release")?;
            }
        }

        // Payloads that are present but not required are still validated: a
        // tolerated field is never an unvalidated field.
        if let Some(ref connection) = self.connection {
            connection.validate()?;
        }
        if let Some(ref subscription) = self.subscription {
            subscription.validate()?;
        }
        if let Some(ref action) = self.action {
            action.validate()?;
        }
        if let Some(ref release) = self.release {
            release.validate()?;
        }
        Ok(())
    }
}

/// Optional replacement of a declared-state map.
///
/// The absent / `null` / `{}` trichotomy, as one type:
///
/// | wire | value |
/// | --- | --- |
/// | field absent | [`StateReplacement::Unchanged`] |
/// | `"state": null` | [`StateReplacement::Unchanged`] |
/// | `"state": {}` | [`StateReplacement::Replace`] with an empty map (*clear*) |
/// | `"state": {"a":1}` | [`StateReplacement::Replace`] |
///
/// Anything else (a list, a number, a string) is a decode error.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum StateReplacement {
    /// Keep whatever state the session already has.
    #[default]
    Unchanged,
    /// Replace the whole state map. An empty map clears it.
    Replace(Map<String, Value>),
}

impl StateReplacement {
    /// An explicit "drop every key".
    #[must_use]
    pub fn clear() -> Self {
        Self::Replace(Map::new())
    }

    /// Whether the session state is left alone.
    #[must_use]
    pub const fn is_unchanged(&self) -> bool {
        matches!(self, Self::Unchanged)
    }

    /// Whether this replacement clears the state.
    #[must_use]
    pub fn is_clear(&self) -> bool {
        matches!(self, Self::Replace(map) if map.is_empty())
    }

    /// The replacement map, if one was sent.
    #[must_use]
    pub const fn map(&self) -> Option<&Map<String, Value>> {
        match self {
            Self::Unchanged => None,
            Self::Replace(map) => Some(map),
        }
    }
}

impl Serialize for StateReplacement {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            // Reached only when a caller serializes the field explicitly;
            // `RpcReply` skips an unchanged state entirely.
            Self::Unchanged => serializer.serialize_none(),
            Self::Replace(map) => map.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for StateReplacement {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<Map<String, Value>>::deserialize(deserializer)
            .map(|state| state.map_or(Self::Unchanged, Self::Replace))
    }
}

/// One typed effect of a successful operation.
///
/// Applied in order. Rails does not return raw WebSocket frames: `welcome`,
/// `confirm_subscription`, `reject_subscription`, and `disconnect` framing are
/// owned by Rust. An unknown `kind` is a hard decode error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Effect {
    /// `stream_from` / `stream_for`: add one stream to an owned subscription.
    AddStream {
        #[serde(default)]
        identifier: String,
        #[serde(default)]
        stream: String,
    },
    /// `stop_stream_from`: drop one stream from an owned subscription.
    RemoveStream {
        #[serde(default)]
        identifier: String,
        #[serde(default)]
        stream: String,
    },
    /// `stop_all_streams`: drop every stream, keeping the subscription.
    RemoveAllStreams {
        #[serde(default)]
        identifier: String,
    },
    /// `transmit`: deliver one application payload to an owned subscription.
    Transmit {
        #[serde(default)]
        identifier: String,
        /// Exact Rails-encoded JSON text; never re-encoded.
        #[serde(default)]
        json_payload: String,
    },
    /// Close the connection with an allowlisted reason and reconnect policy.
    Close {
        #[serde(default)]
        reason: String,
        #[serde(default)]
        reconnect: bool,
    },
    /// Grant an authorized presence membership.
    PresenceGrant {
        #[serde(default)]
        identifier: String,
        #[serde(default)]
        stream: String,
        #[serde(default)]
        subject_id: String,
        #[serde(default)]
        metadata: Map<String, Value>,
        #[serde(default)]
        ttl_ms: u64,
        #[serde(default)]
        operations: Vec<String>,
    },
    /// Revoke a presence membership.
    PresenceRevoke {
        #[serde(default)]
        identifier: String,
        #[serde(default)]
        stream: String,
        #[serde(default)]
        subject_id: String,
    },
}

impl Effect {
    /// The wire `kind` of this effect.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::AddStream { .. } => "add_stream",
            Self::RemoveStream { .. } => "remove_stream",
            Self::RemoveAllStreams { .. } => "remove_all_streams",
            Self::Transmit { .. } => "transmit",
            Self::Close { .. } => "close",
            Self::PresenceGrant { .. } => "presence_grant",
            Self::PresenceRevoke { .. } => "presence_revoke",
        }
    }

    /// The subscription identifier this effect addresses, if any.
    ///
    /// `close` addresses the connection, not a subscription.
    #[must_use]
    pub fn identifier(&self) -> Option<&str> {
        match self {
            Self::AddStream { identifier, .. }
            | Self::RemoveStream { identifier, .. }
            | Self::RemoveAllStreams { identifier }
            | Self::Transmit { identifier, .. }
            | Self::PresenceGrant { identifier, .. }
            | Self::PresenceRevoke { identifier, .. } => Some(identifier),
            Self::Close { .. } => None,
        }
    }

    /// Semantic validation of one effect.
    ///
    /// # Errors
    ///
    /// An empty or oversized identifier, stream, or subject id; a `transmit`
    /// payload over [`MAX_TRANSMIT_LEN`] or not one JSON value; a `close`
    /// reason outside [`ALLOWED_CLOSE_REASONS`]; and, for `presence_grant`, a
    /// zero `ttl_ms` ([`ProtocolError::OutOfRange`]), more than
    /// [`MAX_PRESENCE_OPERATIONS`] operations, or metadata encoding to more
    /// than [`MAX_PRESENCE_METADATA_LEN`] bytes.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::AddStream { identifier, stream } | Self::RemoveStream { identifier, stream } => {
                validate_identifier(identifier)?;
                validate_stream(stream)
            }
            Self::RemoveAllStreams { identifier } => validate_identifier(identifier),
            Self::Transmit { identifier, json_payload } => {
                validate_identifier(identifier)?;
                if json_payload.len() > MAX_TRANSMIT_LEN {
                    return Err(ProtocolError::FieldTooLong {
                        field: "json_payload",
                        len: json_payload.len(),
                        max: MAX_TRANSMIT_LEN,
                    });
                }
                validate_json_text("json_payload", json_payload)
            }
            Self::Close { reason, .. } => validate_close_reason(reason),
            Self::PresenceGrant {
                identifier,
                stream,
                subject_id,
                metadata,
                ttl_ms,
                operations,
            } => {
                validate_identifier(identifier)?;
                validate_stream(stream)?;
                validate_bounded("subject_id", subject_id, MAX_SUBJECT_ID_LEN)?;
                if *ttl_ms == 0 {
                    return Err(ProtocolError::OutOfRange {
                        field: "ttl_ms",
                        value: *ttl_ms,
                        max: MAX_DEADLINE_MS,
                    });
                }
                if operations.len() > MAX_PRESENCE_OPERATIONS {
                    return Err(ProtocolError::TooMany {
                        field: "operations",
                        count: operations.len(),
                        max: MAX_PRESENCE_OPERATIONS,
                    });
                }
                let encoded = serde_json::to_string(metadata).map_err(ProtocolError::Malformed)?;
                if encoded.len() > MAX_PRESENCE_METADATA_LEN {
                    return Err(ProtocolError::FieldTooLong {
                        field: "metadata",
                        len: encoded.len(),
                        max: MAX_PRESENCE_METADATA_LEN,
                    });
                }
                Ok(())
            }
            Self::PresenceRevoke { identifier, stream, subject_id } => {
                validate_identifier(identifier)?;
                validate_stream(stream)?;
                validate_bounded("subject_id", subject_id, MAX_SUBJECT_ID_LEN)
            }
        }
    }
}

/// What a `describe` reply reports.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeInfo {
    /// Envelope versions the Rails endpoint can decode.
    #[serde(default)]
    pub supported_versions: Vec<u32>,
    /// The gem's software version. Informational: admission never requires
    /// build equality.
    #[serde(default)]
    pub software_version: String,
    /// Named capabilities the endpoint provides.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Whether the endpoint is ready to serve dispatch traffic.
    #[serde(default)]
    pub ready: bool,
}

impl DescribeInfo {
    /// Whether the endpoint advertises a named capability.
    #[must_use]
    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|entry| entry == capability)
    }

    /// Semantic validation: the endpoint must speak a version we emit.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::UnsupportedVersion`] when `supported_versions` lacks
    /// [`v1::VERSION`], [`ProtocolError::TooMany`] above [`MAX_CAPABILITIES`]
    /// capabilities, and an empty or oversized (over [`MAX_LABEL_LEN`])
    /// capability or non-empty software version.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if !self.supported_versions.contains(&v1::VERSION) {
            return Err(ProtocolError::UnsupportedVersion(
                self.supported_versions.first().copied().unwrap_or_default(),
            ));
        }
        if self.capabilities.len() > MAX_CAPABILITIES {
            return Err(ProtocolError::TooMany {
                field: "capabilities",
                count: self.capabilities.len(),
                max: MAX_CAPABILITIES,
            });
        }
        for capability in &self.capabilities {
            validate_bounded("capability", capability, MAX_LABEL_LEN)?;
        }
        if !self.software_version.is_empty() {
            validate_bounded("software_version", &self.software_version, MAX_LABEL_LEN)?;
        }
        Ok(())
    }
}

/// Rails' answer to one request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcReply {
    /// Always emitted; absent on the wire means v1.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Echo of the operation. A mismatch is rejected by
    /// [`RpcReply::correlates_with`].
    pub operation: Operation,
    /// The authorization outcome. Absent reads as `fault`.
    #[serde(default)]
    pub outcome: Outcome,
    /// Echo of the request UUID.
    #[serde(default)]
    pub request_id: String,
    /// Echo of the command sequence.
    #[serde(default)]
    pub sequence: u64,
    /// The state revision after this operation.
    #[serde(default)]
    pub new_revision: u64,
    /// Optional connection-state replacement. Absent/null: unchanged.
    #[serde(default, skip_serializing_if = "StateReplacement::is_unchanged")]
    pub state: StateReplacement,
    /// Optional channel-state replacement for the request's subscription.
    #[serde(default, skip_serializing_if = "StateReplacement::is_unchanged")]
    pub subscription_state: StateReplacement,
    /// Identity attributes (`identify` only). An empty map is still a valid
    /// accepted anonymous connection.
    #[serde(default)]
    pub identity: Map<String, Value>,
    /// Ordered effects to apply after an accepted operation.
    #[serde(default)]
    pub effects: Vec<Effect>,
    /// Diagnostic reason for a denial or fault. Never shown to the browser.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// `describe` payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub describe: Option<DescribeInfo>,
}

impl RpcReply {
    /// Build an accepted reply for a request (used by tests and stubs).
    #[must_use]
    pub fn accepted(request: &RpcRequest) -> Self {
        Self {
            version: v1::VERSION,
            operation: request.operation,
            outcome: Outcome::Accepted,
            request_id: request.request_id.clone(),
            sequence: request.sequence,
            new_revision: request.expected_revision.saturating_add(1),
            state: StateReplacement::Unchanged,
            subscription_state: StateReplacement::Unchanged,
            identity: Map::new(),
            effects: Vec::new(),
            reason: None,
            describe: None,
        }
    }

    /// Build a denied reply for a request.
    pub fn denied(request: &RpcRequest, reason: impl Into<String>) -> Self {
        Self {
            outcome: Outcome::Denied,
            new_revision: request.expected_revision,
            reason: Some(reason.into()),
            ..Self::accepted(request)
        }
    }

    /// Whether this reply may be applied to session state.
    #[must_use]
    pub const fn is_accepted(&self) -> bool {
        self.outcome.is_accepted()
    }

    /// Check that this reply answers `request`.
    ///
    /// Run *after* [`Envelope::validate`]: a reply for another request, another
    /// operation, or a revision that moved backwards is discarded rather than
    /// applied.
    ///
    /// # Errors
    ///
    /// [`ProtocolError::Mismatch`] naming the first field that does not
    /// answer `request`: `operation`, `request_id`, `sequence`, or (for an
    /// accepted stateful reply) a `new_revision` below the request's
    /// `expected_revision`.
    pub fn correlates_with(&self, request: &RpcRequest) -> Result<(), ProtocolError> {
        if self.operation != request.operation {
            return Err(ProtocolError::Mismatch {
                field: "operation",
                expected: request.operation.to_string(),
                actual: self.operation.to_string(),
            });
        }
        if self.request_id != request.request_id {
            return Err(ProtocolError::Mismatch {
                field: "request_id",
                expected: request.request_id.clone(),
                actual: self.request_id.clone(),
            });
        }
        if self.sequence != request.sequence {
            return Err(ProtocolError::Mismatch {
                field: "sequence",
                expected: request.sequence.to_string(),
                actual: self.sequence.to_string(),
            });
        }
        if self.operation.is_stateful()
            && self.is_accepted()
            && self.new_revision < request.expected_revision
        {
            return Err(ProtocolError::Mismatch {
                field: "new_revision",
                expected: format!(">= {}", request.expected_revision),
                actual: self.new_revision.to_string(),
            });
        }
        Ok(())
    }

    /// Reject state/identity/effects on a reply that was not accepted.
    fn validate_outcome_scope(&self) -> Result<(), ProtocolError> {
        if self.is_accepted() {
            return Ok(());
        }
        let outcome = self.outcome.as_str();
        if !self.effects.is_empty() {
            return Err(ProtocolError::NotAllowedForOutcome { field: "effects", outcome });
        }
        if !self.state.is_unchanged() {
            return Err(ProtocolError::NotAllowedForOutcome { field: "state", outcome });
        }
        if !self.subscription_state.is_unchanged() {
            return Err(ProtocolError::NotAllowedForOutcome {
                field: "subscription_state",
                outcome,
            });
        }
        if !self.identity.is_empty() {
            return Err(ProtocolError::NotAllowedForOutcome { field: "identity", outcome });
        }
        Ok(())
    }
}

impl Envelope for RpcReply {
    fn version(&self) -> u32 {
        self.version
    }

    fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        validate_uuid("request_id", &self.request_id)?;

        if let Some(ref reason) = self.reason {
            validate_bounded("reason", reason, MAX_REASON_LEN)?;
        }

        self.validate_outcome_scope()?;

        if self.effects.len() > MAX_EFFECTS {
            return Err(ProtocolError::TooMany {
                field: "effects",
                count: self.effects.len(),
                max: MAX_EFFECTS,
            });
        }
        for effect in &self.effects {
            effect.validate()?;
        }

        validate_state_map("identity", &self.identity)?;
        if let Some(state) = self.state.map() {
            validate_state_map("state", state)?;
        }
        if let Some(state) = self.subscription_state.map() {
            validate_state_map("subscription_state", state)?;
        }

        match (self.operation, self.is_accepted()) {
            (Operation::Describe, true) => self.describe.as_ref().map_or_else(
                || {
                    Err(ProtocolError::MissingPayload {
                        operation: Operation::Describe.as_str(),
                        field: "describe",
                    })
                },
                DescribeInfo::validate,
            ),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;
