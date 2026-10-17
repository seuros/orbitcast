//! Signed streams support for OrbitCast.
//!
//! Allows subscribing to streams without an application round-trip by verifying
//! signed stream names. Three independent profiles, each with its own format
//! and its own key ring — there is no fallback between them:
//!
//! - **`Orbitcast::StreamsChannel` — Orbitcast v1 claims profile.** The token is
//!   `base64_strict(JSON(claims))--hex(HMAC-SHA256(key, encoded))`, where the
//!   claims are `{version, namespace, stream, purpose:"subscribe",
//!   expires_at}` plus the optional `whisper` capability. The HMAC is verified
//!   over the encoded bytes *before* the payload is ever parsed, then the
//!   claims are decoded with the tolerant serde rules and validated
//!   semantically ([`crate::protocol::v1::claims`]).
//! - **`Turbo::StreamsChannel` — Turbo profile.** A bare stream string signed
//!   with Turbo's own verifier key. Parsed separately from the claims profile;
//!   an `ActiveSupport::Messages::Metadata` envelope (`{"_rails": ...}`) is
//!   rejected explicitly rather than read as a stream name.
//! - **`CableReady::Stream` — CableReady profile.** Same string-signing shape
//!   as Turbo, with its own key.
//!
//! # Key rings
//!
//! Every profile verifies against a [`KeyRing`]: the current key first, then
//! up to [`MAX_PREVIOUS_KEYS`] verify-only previous keys in order. Nothing
//! here ever signs (Rails is the producer), so a previous key can only widen
//! what verifies, never what is minted. Which key matched is counted in
//! `orbitcast_signature_verifications_total{profile,key}`.
//!
//! # Capabilities
//!
//! A verified name is a *read* capability. Whisper (client-originated fanout)
//! is granted only by an Orbitcast claims token carrying `whisper: true`;
//! Turbo, CableReady, and public streams never authorize it.

use std::sync::Arc;

use hmac::{Hmac, KeyInit, Mac};
use serde::Deserialize;
use sha2::Sha256;
use tracing::debug;

use crate::metrics::{Metrics, SignatureKey, SignatureProfile};
use crate::protocol::ProtocolError;
use crate::protocol::v1::claims::StreamClaims;
use crate::protocol::v1::{now_unix, validate_stream};

type HmacSha256 = Hmac<Sha256>;

/// Known channel identifiers for signed streams
pub const ORBITCAST_CHANNEL: &str = "Orbitcast::StreamsChannel";
pub const TURBO_CHANNEL: &str = "Turbo::StreamsChannel";
pub const CABLE_READY_CHANNEL: &str = "CableReady::Stream";

/// Upper bound on an accepted signed-stream token, in bytes. Guards the
/// verifier against pathological input before any base64/JSON work.
pub const MAX_TOKEN_LEN: usize = 8 * 1024;

/// Upper bound on verify-only previous keys in one profile's ring.
///
/// A rotation needs two keys (new and old); eight leaves room for a
/// deployment that rotates faster than its longest-lived page expires without
/// turning verification into a scan of every key ever issued.
pub const MAX_PREVIOUS_KEYS: usize = 8;

/// Byte length of an HMAC-SHA256 signature.
const SIGNATURE_LEN: usize = 32;

/// HMAC key bytes for a signing profile.
///
/// Kept as raw bytes (not `String`): the gem exports key material as strict
/// base64 that Rust decodes to bytes, and a plain-text config value is passed
/// through as its raw UTF-8 bytes. The [`Debug`] impl never reveals the key.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretBytes(Vec<u8>);

impl SecretBytes {
    /// Wrap raw key bytes.
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The raw key bytes, for HMAC keying only.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretBytes([REDACTED; {} bytes])", self.0.len())
    }
}

/// Verify-only previous keys for one profile, bounded at construction.
///
/// The bound is enforced where the list is parsed (the configuration
/// boundary), so a [`KeyRing`] can be built without a failure path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreviousKeys(Vec<SecretBytes>);

impl PreviousKeys {
    /// Wrap `keys`, refusing more than [`MAX_PREVIOUS_KEYS`].
    ///
    /// # Errors
    ///
    /// [`KeyRingError::TooManyPreviousKeys`] past the bound.
    pub fn new(keys: Vec<SecretBytes>) -> Result<Self, KeyRingError> {
        let count = keys.len();
        if count > MAX_PREVIOUS_KEYS {
            return Err(KeyRingError::TooManyPreviousKeys { count, max: MAX_PREVIOUS_KEYS });
        }
        Ok(Self(keys))
    }

    /// Number of previous keys.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the ring holds no previous key.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The keys, in verification order.
    #[must_use]
    pub fn as_slice(&self) -> &[SecretBytes] {
        &self.0
    }
}

/// Key-ring construction errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyRingError {
    #[error("too many previous keys ({count} > {max})")]
    TooManyPreviousKeys { count: usize, max: usize },
}

/// Which ring key verified a signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyMatch {
    /// The current signing key.
    Current,
    /// A verify-only previous key, by its position in the ring.
    Previous(usize),
}

impl KeyMatch {
    /// The metric label this match is counted under.
    #[must_use]
    pub const fn metric_key(self) -> SignatureKey {
        match self {
            Self::Current => SignatureKey::Current,
            Self::Previous(_) => SignatureKey::Previous,
        }
    }
}

/// One profile's verification keys: the current key plus bounded previous
/// keys. The current key is always tried first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRing {
    current: SecretBytes,
    previous: PreviousKeys,
}

impl KeyRing {
    /// A ring with only the current key.
    #[must_use]
    pub const fn new(current: SecretBytes) -> Self {
        Self { current, previous: PreviousKeys(Vec::new()) }
    }

    /// A ring with the current key plus verify-only previous keys.
    #[must_use]
    pub const fn with_previous(current: SecretBytes, previous: PreviousKeys) -> Self {
        Self { current, previous }
    }

    /// Number of verify-only previous keys.
    #[must_use]
    pub const fn previous_len(&self) -> usize {
        self.previous.len()
    }

    /// Every key in verification order: current, then previous.
    fn keys(&self) -> impl Iterator<Item = (KeyMatch, &SecretBytes)> {
        std::iter::once((KeyMatch::Current, &self.current)).chain(
            self.previous
                .as_slice()
                .iter()
                .enumerate()
                .map(|(i, key)| (KeyMatch::Previous(i), key)),
        )
    }

    /// Constant-time HMAC-SHA256 check of `signature` over `data`, trying the
    /// current key first and then each previous key in order.
    ///
    /// `None` when no key in the ring produced the signature. The comparison
    /// is on the decoded signature bytes (`Mac::verify_slice`), never on hex
    /// text.
    fn verify(
        &self,
        data: &[u8],
        signature: &[u8; SIGNATURE_LEN],
    ) -> Result<Option<KeyMatch>, VerifyError> {
        for (which, key) in self.keys() {
            let mut mac =
                HmacSha256::new_from_slice(key.as_bytes()).map_err(|_| VerifyError::InvalidKey)?;
            mac.update(data);
            if mac.verify_slice(signature).is_ok() {
                return Ok(Some(which));
            }
        }
        Ok(None)
    }
}

/// Configuration for signed streams.
///
/// Every switch here is off and every key unset by default: no profile and
/// no public stream exists until something in the configuration asks for it.
/// Presence is not a signed-stream capability at all: it is granted by Rails
/// per subscription in dispatch mode (`crate::presence`).
#[expect(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent operator switch read 1:1 from configuration, not \
              encoded state; folding them into an enum would invent combinations"
)]
#[derive(Debug, Clone, Default)]
pub struct StreamsConfig {
    /// Current key for verifying `Orbitcast::StreamsChannel` (Orbitcast v1
    /// claims profile).
    pub secret: Option<SecretBytes>,
    /// Verify-only previous keys for the Orbitcast profile.
    pub previous_secrets: PreviousKeys,
    /// Allow public (unsigned) streams.
    pub public: bool,
    /// Explicit acknowledgement that public streams let any client read any
    /// stream name. Required alongside [`Self::public`]
    /// (`Config::validate` refuses one without the other).
    pub public_insecure: bool,
    /// Enable Turbo Streams support
    pub turbo: bool,
    /// Current key for Turbo Streams. Required when `turbo` is enabled; there
    /// is no fallback to the main secret.
    pub turbo_secret: Option<SecretBytes>,
    /// Verify-only previous keys for the Turbo profile.
    pub turbo_previous_secrets: PreviousKeys,
    /// Enable CableReady support
    pub cable_ready: bool,
    /// Current key for CableReady. Required when `cable_ready` is enabled;
    /// there is no fallback to the main secret.
    pub cable_ready_secret: Option<SecretBytes>,
    /// Verify-only previous keys for the CableReady profile.
    pub cable_ready_previous_secrets: PreviousKeys,
}

impl StreamsConfig {
    /// Check if any signed streams feature is enabled
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.secret.is_some() || self.public || self.turbo || self.cable_ready
    }

    /// The Orbitcast profile's ring, when a current key is configured.
    #[must_use]
    pub fn ring(&self) -> Option<KeyRing> {
        self.secret.clone().map(|key| KeyRing::with_previous(key, self.previous_secrets.clone()))
    }

    /// The Turbo profile's ring, when a current key is configured.
    #[must_use]
    pub fn turbo_ring(&self) -> Option<KeyRing> {
        self.turbo_secret
            .clone()
            .map(|key| KeyRing::with_previous(key, self.turbo_previous_secrets.clone()))
    }

    /// The CableReady profile's ring, when a current key is configured.
    #[must_use]
    pub fn cable_ready_ring(&self) -> Option<KeyRing> {
        self.cable_ready_secret
            .clone()
            .map(|key| KeyRing::with_previous(key, self.cable_ready_previous_secrets.clone()))
    }
}

/// A verified value together with the ring key that authenticated it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified<T> {
    /// What the token carried.
    pub value: T,
    /// Which ring key produced the signature.
    pub key: KeyMatch,
}

/// Split a `payload--signature` token into its encoded half and the decoded
/// signature bytes.
///
/// Shared prologue of both verifiers: an oversized token, a missing separator,
/// an empty payload, or a signature that is not exactly 64 lowercase/uppercase
/// hex characters is a format error before any HMAC work. No key can produce
/// such a signature, so it is never presented to the ring.
fn split_token(token: &str) -> Result<(&str, [u8; SIGNATURE_LEN]), VerifyError> {
    if token.len() > MAX_TOKEN_LEN {
        return Err(VerifyError::TokenTooLong { len: token.len(), max: MAX_TOKEN_LEN });
    }
    let (encoded, signature_hex) = token.split_once("--").ok_or(VerifyError::InvalidFormat)?;
    if encoded.is_empty() || signature_hex.len() != SIGNATURE_LEN * 2 {
        return Err(VerifyError::InvalidFormat);
    }
    let mut signature = [0u8; SIGNATURE_LEN];
    hex::decode_to_slice(signature_hex, &mut signature).map_err(|_| VerifyError::InvalidFormat)?;
    Ok((encoded, signature))
}

/// Authenticate `token` against `ring`: the encoded half and the key that
/// signed it, or [`VerifyError::InvalidSignature`] when no key did.
fn authenticate<'t>(ring: &KeyRing, token: &'t str) -> Result<(&'t str, KeyMatch), VerifyError> {
    let (encoded, signature) = split_token(token)?;
    let key = ring.verify(encoded.as_bytes(), &signature)?.ok_or(VerifyError::InvalidSignature)?;
    Ok((encoded, key))
}

/// Verifier for the Orbitcast v1 `Orbitcast::StreamsChannel` claims profile.
///
/// Signature verification happens over the original encoded bytes before the
/// payload is parsed; semantic validation ([`StreamClaims::validate_at`]) and
/// the namespace check both run afterwards, all fail-closed.
pub struct ClaimsVerifier {
    ring: KeyRing,
}

impl ClaimsVerifier {
    /// Create a verifier keyed with a single current key.
    #[must_use]
    pub const fn new(key: SecretBytes) -> Self {
        Self { ring: KeyRing::new(key) }
    }

    /// Create a verifier over a full key ring.
    #[must_use]
    pub const fn with_ring(ring: KeyRing) -> Self {
        Self { ring }
    }

    /// Verify, decode, and semantically validate an Orbitcast v1 subscribe
    /// token against `namespace` at clock reading `now` (unix seconds).
    ///
    /// `now` is injected rather than read from a global so tests can pin a
    /// fixed clock. The order is deliberate and fail-closed:
    /// 1. constant-time HMAC over the encoded bytes (current key, then each
    ///    previous key),
    /// 2. strict base64 decode + tolerant serde parse,
    /// 3. version/namespace/stream/purpose/expiry validation,
    /// 4. namespace must equal the configured namespace.
    ///
    /// An optional `kid` claim is carried through but never consulted: the
    /// signature is checked before the claims are readable, so a key hint
    /// inside the payload cannot steer key selection.
    ///
    /// # Errors
    ///
    /// [`VerifyError::TokenTooLong`] past [`MAX_TOKEN_LEN`],
    /// [`VerifyError::InvalidFormat`] without a `payload--signature` split or
    /// a 64-hex-character signature, [`VerifyError::InvalidSignature`] when no
    /// ring key produced the HMAC, [`VerifyError::Base64`] /
    /// [`VerifyError::Json`] for an undecodable payload,
    /// [`VerifyError::Claims`] when the claims fail validation at `now`
    /// (expired, wrong purpose, ...), and [`VerifyError::NamespaceMismatch`]
    /// for another deployment's token.
    pub fn verify(
        &self,
        token: &str,
        namespace: &str,
        now: i64,
    ) -> Result<StreamClaims, VerifyError> {
        self.verify_keyed(token, namespace, now).map(|verified| verified.value)
    }

    /// As [`Self::verify`], also reporting which ring key matched.
    ///
    /// # Errors
    ///
    /// As [`Self::verify`].
    pub fn verify_keyed(
        &self,
        token: &str,
        namespace: &str,
        now: i64,
    ) -> Result<Verified<StreamClaims>, VerifyError> {
        // Authenticate the token before interpreting anything inside it.
        let (encoded, key) = authenticate(&self.ring, token)?;

        let claims_bytes = base64_decode(encoded)?;
        let claims: StreamClaims = serde_json::from_slice(&claims_bytes)?;

        // Semantic validation: version, nonempty/bounded namespace + stream,
        // purpose == "subscribe", and expiry strictly in the future.
        claims.validate_at(now)?;

        // A token only authorizes its own namespace; a signed name from another
        // deployment must never be honored here.
        if claims.namespace != namespace {
            return Err(VerifyError::NamespaceMismatch {
                expected: namespace.to_string(),
                actual: claims.namespace,
            });
        }

        Ok(Verified { value: claims, key })
    }

    /// Produce a token the same way the Ruby producer does: strict base64 of
    /// the fixed-key-order JSON, `--`, lowercase-hex HMAC over the encoded
    /// bytes, always with the *current* key. Test-only; the gem is the
    /// authoritative producer.
    #[cfg(test)]
    pub(crate) fn sign(&self, claims: &StreamClaims) -> String {
        let json = serde_json::to_string(claims).expect("claims serialize");
        sign_encoded(&self.ring.current, &base64_encode(json.as_bytes()))
    }
}

/// Message verifier using HMAC-SHA256 (Rails `MessageVerifier` compatible).
///
/// Used by the Turbo and CableReady profiles, which sign a bare stream string
/// rather than an Orbitcast claims object.
pub struct MessageVerifier {
    ring: KeyRing,
}

impl MessageVerifier {
    /// Create a new verifier with a single current key.
    #[must_use]
    pub fn new(key: &[u8]) -> Self {
        Self { ring: KeyRing::new(SecretBytes::new(key.to_vec())) }
    }

    /// Create a verifier over a full key ring.
    #[must_use]
    pub const fn with_ring(ring: KeyRing) -> Self {
        Self { ring }
    }

    /// Verify a signed message and return the decoded stream string.
    ///
    /// Expected format: `base64(json(string))--hex(hmac_sha256(base64_part))`
    ///
    /// # Errors
    ///
    /// [`VerifyError::TokenTooLong`] past [`MAX_TOKEN_LEN`],
    /// [`VerifyError::InvalidFormat`] without a `payload--signature` split or
    /// a 64-hex-character signature, [`VerifyError::InvalidSignature`] when no
    /// ring key produced the HMAC, [`VerifyError::Base64`] /
    /// [`VerifyError::Utf8`] / [`VerifyError::Json`] when the payload is not
    /// base64 JSON, [`VerifyError::MetadataEnvelope`] for an
    /// `ActiveSupport::Messages::Metadata` (`_rails`) envelope,
    /// [`VerifyError::PayloadNotAString`] for any other non-string JSON, and
    /// [`VerifyError::Stream`] when the string is empty or over the stream
    /// length limit.
    pub fn verify(&self, signed: &str) -> Result<String, VerifyError> {
        self.verify_keyed(signed).map(|verified| verified.value)
    }

    /// As [`Self::verify`], also reporting which ring key matched.
    ///
    /// # Errors
    ///
    /// As [`Self::verify`].
    pub fn verify_keyed(&self, signed: &str) -> Result<Verified<String>, VerifyError> {
        // Verify signature over the encoded bytes before parsing.
        let (encoded, key) = authenticate(&self.ring, signed)?;

        let json_bytes = base64_decode(encoded)?;
        let json_str = String::from_utf8(json_bytes)?;
        let stream = match serde_json::from_str::<serde_json::Value>(&json_str)? {
            serde_json::Value::String(stream) => stream,
            // A correctly signed MessageVerifier metadata envelope (purpose
            // and/or expiry) is not a stream name: the Turbo profile carries
            // no metadata and must not grow some by reading `data` out of it.
            serde_json::Value::Object(object) if object.contains_key("_rails") => {
                return Err(VerifyError::MetadataEnvelope);
            }
            _ => return Err(VerifyError::PayloadNotAString),
        };

        validate_stream(&stream).map_err(VerifyError::Stream)?;

        Ok(Verified { value: stream, key })
    }

    /// Generate a signed stream name with the current key. Test-only; Rails
    /// is the producer.
    #[cfg(test)]
    pub(crate) fn sign(&self, stream: &str) -> Result<String, VerifyError> {
        let json = serde_json::to_string(stream)?;
        Ok(sign_encoded(&self.ring.current, &base64_encode(json.as_bytes())))
    }
}

/// Errors during message verification
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("invalid message format")]
    InvalidFormat,
    #[error("token exceeds maximum length ({len} > {max} bytes)")]
    TokenTooLong { len: usize, max: usize },
    #[error("invalid signature")]
    InvalidSignature,
    #[error("invalid key")]
    InvalidKey,
    #[error("payload is an ActiveSupport metadata envelope, not a stream name")]
    MetadataEnvelope,
    #[error("payload is not a JSON string")]
    PayloadNotAString,
    #[error("stream rejected: {0}")]
    Stream(#[source] ProtocolError),
    #[error("token namespace {actual:?} does not match configured namespace {expected:?}")]
    NamespaceMismatch { expected: String, actual: String },
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("claims validation failed: {0}")]
    Claims(#[from] ProtocolError),
}

/// Every field a signed-stream identifier may carry, across the three
/// channels. Parsed exactly once per resolution.
#[derive(Debug, Deserialize)]
struct SubscribeRequest {
    #[serde(default)]
    channel: Option<String>,
    /// `Orbitcast::StreamsChannel`: an unsigned public stream name.
    #[serde(default)]
    stream_name: Option<String>,
    /// `Orbitcast::StreamsChannel` / `Turbo::StreamsChannel`: the signed name.
    #[serde(default)]
    signed_stream_name: Option<String>,
    /// `CableReady::Stream`: the signed name, under its `identifier` key.
    #[serde(default, rename = "identifier")]
    signed_identifier: Option<String>,
}

/// Result of stream resolution
#[derive(Debug)]
pub struct StreamResult {
    /// The resolved stream name
    pub stream: String,
    /// Whether the verified capability authorizes whisper on this stream
    /// (Orbitcast claims tokens with `whisper: true` only).
    pub whisper: bool,
}

/// Streams controller for handling signed stream subscriptions
pub struct StreamsController {
    config: StreamsConfig,
    /// Namespace every `Orbitcast::StreamsChannel` claims token must be scoped to.
    namespace: String,
    pubsub_verifier: Option<ClaimsVerifier>,
    turbo_verifier: Option<MessageVerifier>,
    cable_ready_verifier: Option<MessageVerifier>,
    /// Where verification outcomes are counted; `None` for a bare controller
    /// (tests) records nothing.
    metrics: Option<Arc<Metrics>>,
}

impl StreamsController {
    /// Create a new streams controller for the configured `namespace`.
    ///
    /// Each profile is keyed with its own ring only: an unset Turbo or
    /// CableReady key yields no verifier for that profile rather than a
    /// fallback to the main secret. Configuration
    /// validation rejects an enabled profile without its key before this
    /// controller is ever built.
    #[must_use]
    pub fn new(config: StreamsConfig, namespace: String) -> Self {
        let pubsub_verifier = config.ring().map(ClaimsVerifier::with_ring);
        let turbo_verifier = config.turbo_ring().map(MessageVerifier::with_ring);
        let cable_ready_verifier = config.cable_ready_ring().map(MessageVerifier::with_ring);

        Self {
            config,
            namespace,
            pubsub_verifier,
            turbo_verifier,
            cable_ready_verifier,
            metrics: None,
        }
    }

    /// Count verification outcomes in `metrics`.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Check if an identifier should be handled by this controller
    #[must_use]
    pub fn handles(&self, identifier: &str) -> bool {
        let Ok(parsed) = serde_json::from_str::<SubscribeRequest>(identifier) else {
            return false;
        };
        match parsed.channel.as_deref() {
            Some(ORBITCAST_CHANNEL) => self.config.secret.is_some() || self.config.public,
            Some(TURBO_CHANNEL) => self.config.turbo,
            Some(CABLE_READY_CHANNEL) => self.config.cable_ready,
            _ => false,
        }
    }

    /// Resolve a subscription request to a stream, using the wall clock.
    ///
    /// # Errors
    ///
    /// As [`Self::resolve_at`].
    pub fn resolve(&self, identifier: &str) -> Result<StreamResult, StreamError> {
        self.resolve_at(identifier, now_unix())
    }

    /// Resolve a subscription request at an explicit clock reading (unix
    /// seconds). The injected clock keeps expiry checks deterministic in tests.
    ///
    /// The identifier is parsed once; the channel then selects the profile.
    ///
    /// # Errors
    ///
    /// [`StreamError::Json`] for an identifier that is not the expected JSON,
    /// [`StreamError::MissingChannel`] / [`StreamError::UnknownChannel`] when
    /// it names no signed-stream channel, the `*NotEnabled` /
    /// [`StreamError::PublicNotAllowed`] / [`StreamError::NoSecret`] variants
    /// when the matching profile is not configured,
    /// [`StreamError::MissingStream`] when no stream is named,
    /// [`StreamError::InvalidStream`] for an empty or oversized public stream
    /// name, and [`StreamError::Verify`] when the signed name fails
    /// verification.
    pub fn resolve_at(&self, identifier: &str, now: i64) -> Result<StreamResult, StreamError> {
        let parsed: SubscribeRequest = serde_json::from_str(identifier)?;
        let channel = parsed.channel.as_deref().ok_or(StreamError::MissingChannel)?;

        match channel {
            ORBITCAST_CHANNEL => self.resolve_pubsub(&parsed, now),
            TURBO_CHANNEL => self.resolve_turbo(&parsed),
            CABLE_READY_CHANNEL => self.resolve_cable_ready(&parsed),
            _ => Err(StreamError::UnknownChannel(channel.to_string())),
        }
    }

    fn resolve_pubsub(
        &self,
        request: &SubscribeRequest,
        now: i64,
    ) -> Result<StreamResult, StreamError> {
        // Public (unsigned) streams are a separate opt-in, not part of the
        // signed claims profile. They are read-only: no capability token means
        // no whisper.
        if let Some(stream) = request.stream_name.as_deref() {
            if !self.config.public {
                return Err(StreamError::PublicNotAllowed);
            }
            validate_stream(stream)?;
            debug!(stream, "public stream subscription");
            return Ok(StreamResult { stream: stream.to_string(), whisper: false });
        }

        // Signed Orbitcast v1 claims token.
        if let Some(token) = request.signed_stream_name.as_deref() {
            let verifier = self.pubsub_verifier.as_ref().ok_or(StreamError::NoSecret)?;
            let outcome = verifier.verify_keyed(token, &self.namespace, now);
            self.record(SignatureProfile::Orbitcast, &outcome);
            let Verified { value: claims, key } = outcome?;
            debug!(
                stream = %claims.stream,
                ?key,
                whisper = claims.whisper,
                "verified Orbitcast claims stream"
            );
            return Ok(StreamResult { stream: claims.stream, whisper: claims.whisper });
        }

        Err(StreamError::MissingStream)
    }

    fn resolve_turbo(&self, request: &SubscribeRequest) -> Result<StreamResult, StreamError> {
        if !self.config.turbo {
            return Err(StreamError::TurboNotEnabled);
        }
        let signed = request.signed_stream_name.as_deref().ok_or(StreamError::MissingStream)?;
        self.resolve_string_profile(self.turbo_verifier.as_ref(), signed, SignatureProfile::Turbo)
    }

    fn resolve_cable_ready(&self, request: &SubscribeRequest) -> Result<StreamResult, StreamError> {
        if !self.config.cable_ready {
            return Err(StreamError::CableReadyNotEnabled);
        }
        let signed = request.signed_identifier.as_deref().ok_or(StreamError::MissingStream)?;
        self.resolve_string_profile(
            self.cable_ready_verifier.as_ref(),
            signed,
            SignatureProfile::CableReady,
        )
    }

    /// Resolve a string-signing profile (Turbo, CableReady) to its stream.
    ///
    /// Both profiles sign a bare stream string with their own key and carry
    /// no claims, so neither can authorize whisper: that is a property of the
    /// Orbitcast claims profile.
    fn resolve_string_profile(
        &self,
        verifier: Option<&MessageVerifier>,
        signed: &str,
        profile: SignatureProfile,
    ) -> Result<StreamResult, StreamError> {
        let verifier = verifier.ok_or(StreamError::NoSecret)?;
        let outcome = verifier.verify_keyed(signed);
        self.record(profile, &outcome);
        let Verified { value: stream, key } = outcome?;

        debug!(stream, ?key, "verified {} stream", profile.label());
        Ok(StreamResult { stream, whisper: false })
    }

    /// Count one verification outcome: the key that matched, or `failed`
    /// when the HMAC was checked against every ring key and none produced
    /// it. Format and decode errors involve no key and are not counted.
    fn record<T>(&self, profile: SignatureProfile, outcome: &Result<Verified<T>, VerifyError>) {
        let Some(metrics) = &self.metrics else { return };
        match outcome {
            Ok(verified) => {
                metrics.record_signature_verification(profile, verified.key.metric_key());
            }
            Err(VerifyError::InvalidSignature) => {
                metrics.record_signature_verification(profile, SignatureKey::Failed);
            }
            Err(_) => {}
        }
    }
}

/// Errors during stream resolution
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("missing channel in identifier")]
    MissingChannel,
    #[error("unknown channel: {0}")]
    UnknownChannel(String),
    #[error("missing stream name or signed stream")]
    MissingStream,
    #[error("public streams not allowed")]
    PublicNotAllowed,
    #[error("turbo streams not enabled")]
    TurboNotEnabled,
    #[error("cable_ready not enabled")]
    CableReadyNotEnabled,
    #[error("no secret configured")]
    NoSecret,
    #[error("invalid stream name: {0}")]
    InvalidStream(#[from] ProtocolError),
    #[error("verification failed: {0}")]
    Verify(#[from] VerifyError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// `encoded--hex(HMAC-SHA256(key, encoded))` (the test-only signers above).
#[cfg(test)]
fn sign_encoded(key: &SecretBytes, encoded: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(key.as_bytes()).expect("hmac accepts any key length");
    mac.update(encoded.as_bytes());
    format!("{encoded}--{}", hex::encode(mac.finalize().into_bytes()))
}

/// Standard base64 encode (the test-only signers above).
#[cfg(test)]
fn base64_encode(data: &[u8]) -> String {
    use base64::{Engine, engine::general_purpose::STANDARD};
    STANDARD.encode(data)
}

/// Standard base64 decode
fn base64_decode(data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    STANDARD.decode(data)
}

#[cfg(test)]
mod tests;
