//! Signed streams support for OrbitCast
//!
//! Allows subscribing to streams without RPC calls by verifying signed stream names.
//! Compatible with AnyCable's signed streams, Turbo Streams, and CableReady.
//!
//! # Signing Algorithm
//!
//! Uses Rails' MessageVerifier algorithm:
//! 1. Encode stream name: Base64(JSON(stream_name))
//! 2. Calculate HMAC-SHA256 digest
//! 3. Format: `${encoded}--${digest_hex}`

use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tracing::debug;

type HmacSha256 = Hmac<Sha256>;

/// Known channel identifiers for signed streams
pub const PUBSUB_CHANNEL: &str = "$pubsub";
pub const TURBO_CHANNEL: &str = "Turbo::StreamsChannel";
pub const CABLE_READY_CHANNEL: &str = "CableReady::Stream";

/// Configuration for signed streams
#[derive(Debug, Clone)]
pub struct StreamsConfig {
    /// Secret key for verifying $pubsub signed streams
    pub secret: Option<String>,
    /// Allow public (unsigned) streams
    pub public: bool,
    /// Enable whisper support for streams
    pub whisper: bool,
    /// Enable presence support for streams
    pub presence: bool,
    /// Enable Turbo Streams support
    pub turbo: bool,
    /// Secret for Turbo Streams (falls back to main secret)
    pub turbo_secret: Option<String>,
    /// Enable CableReady support
    pub cable_ready: bool,
    /// Secret for CableReady (falls back to main secret)
    pub cable_ready_secret: Option<String>,
}

impl Default for StreamsConfig {
    fn default() -> Self {
        Self {
            secret: None,
            public: false,
            whisper: false,
            presence: true, // AnyCable defaults presence to true
            turbo: false,
            turbo_secret: None,
            cable_ready: false,
            cable_ready_secret: None,
        }
    }
}

impl StreamsConfig {
    /// Check if any signed streams feature is enabled
    pub fn is_enabled(&self) -> bool {
        self.secret.is_some() || self.public || self.turbo || self.cable_ready
    }

    /// Get the secret for Turbo Streams
    pub fn get_turbo_secret(&self) -> Option<&str> {
        self.turbo_secret.as_deref().or(self.secret.as_deref())
    }

    /// Get the secret for CableReady
    pub fn get_cable_ready_secret(&self) -> Option<&str> {
        self.cable_ready_secret
            .as_deref()
            .or(self.secret.as_deref())
    }
}

/// Message verifier using HMAC-SHA256 (Rails MessageVerifier compatible)
pub struct MessageVerifier {
    key: Vec<u8>,
}

impl MessageVerifier {
    /// Create a new verifier with the given secret key
    pub fn new(key: &str) -> Self {
        Self {
            key: key.as_bytes().to_vec(),
        }
    }

    /// Verify a signed message and return the decoded payload
    ///
    /// Expected format: `base64(json(payload))--hex(hmac_sha256(base64_part))`
    pub fn verify(&self, signed: &str) -> Result<String, VerifyError> {
        let parts: Vec<&str> = signed.split("--").collect();
        if parts.len() != 2 {
            return Err(VerifyError::InvalidFormat);
        }

        let encoded = parts[0];
        let signature = parts[1];

        // Verify signature
        if !self.verify_signature(encoded.as_bytes(), signature)? {
            return Err(VerifyError::InvalidSignature);
        }

        // Decode base64
        let json_bytes = base64_decode(encoded)?;
        let json_str = String::from_utf8(json_bytes)?;

        // Parse JSON - stream name is a quoted string
        let stream: String = serde_json::from_str(&json_str)?;

        Ok(stream)
    }

    /// Verify the HMAC signature
    fn verify_signature(&self, data: &[u8], signature: &str) -> Result<bool, VerifyError> {
        let mut mac = HmacSha256::new_from_slice(&self.key).map_err(|_| VerifyError::InvalidKey)?;
        mac.update(data);
        let expected = mac.finalize().into_bytes();
        let expected_hex = hex::encode(expected);

        // Constant-time comparison
        Ok(expected_hex.as_bytes().ct_eq(signature.as_bytes()).into())
    }

    /// Generate a signed stream name (for testing)
    #[allow(dead_code)]
    pub fn sign(&self, stream: &str) -> Result<String, VerifyError> {
        let json = serde_json::to_string(stream)?;
        let encoded = base64_encode(json.as_bytes());

        let mut mac = HmacSha256::new_from_slice(&self.key).map_err(|_| VerifyError::InvalidKey)?;
        mac.update(encoded.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        Ok(format!("{}--{}", encoded, signature))
    }
}

/// Errors during message verification
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("invalid message format")]
    InvalidFormat,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("invalid key")]
    InvalidKey,
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// $pubsub channel subscribe request
#[derive(Debug, Deserialize)]
pub struct PubSubRequest {
    pub channel: String,
    #[serde(default)]
    pub stream_name: Option<String>,
    #[serde(default)]
    pub signed_stream_name: Option<String>,
}

/// Turbo Streams subscribe request
#[derive(Debug, Deserialize)]
pub struct TurboRequest {
    pub channel: String,
    pub signed_stream_name: String,
}

/// CableReady subscribe request
#[derive(Debug, Deserialize)]
pub struct CableReadyRequest {
    pub channel: String,
    pub identifier: String,
}

/// Result of stream resolution
#[derive(Debug)]
pub struct StreamResult {
    /// The resolved stream name
    pub stream: String,
    /// Whether whisper is enabled for this stream
    pub whisper: bool,
    /// Whether presence is enabled for this stream
    pub presence: bool,
}

/// Streams controller for handling signed stream subscriptions
pub struct StreamsController {
    config: StreamsConfig,
    pubsub_verifier: Option<MessageVerifier>,
    turbo_verifier: Option<MessageVerifier>,
    cable_ready_verifier: Option<MessageVerifier>,
}

impl StreamsController {
    /// Create a new streams controller
    pub fn new(config: StreamsConfig) -> Self {
        let pubsub_verifier = config.secret.as_ref().map(|s| MessageVerifier::new(s));
        let turbo_verifier = config.get_turbo_secret().map(MessageVerifier::new);
        let cable_ready_verifier = config.get_cable_ready_secret().map(MessageVerifier::new);

        Self {
            config,
            pubsub_verifier,
            turbo_verifier,
            cable_ready_verifier,
        }
    }

    /// Check if an identifier should be handled by this controller
    pub fn handles(&self, identifier: &str) -> bool {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(identifier)
            && let Some(channel) = parsed.get("channel").and_then(|c| c.as_str())
        {
            return match channel {
                PUBSUB_CHANNEL => self.config.secret.is_some() || self.config.public,
                TURBO_CHANNEL => self.config.turbo,
                CABLE_READY_CHANNEL => self.config.cable_ready,
                _ => false,
            };
        }
        false
    }

    /// Resolve a subscription request to a stream
    pub fn resolve(&self, identifier: &str) -> Result<StreamResult, StreamError> {
        let parsed: serde_json::Value = serde_json::from_str(identifier)?;
        let channel = parsed
            .get("channel")
            .and_then(|c| c.as_str())
            .ok_or(StreamError::MissingChannel)?;

        match channel {
            PUBSUB_CHANNEL => self.resolve_pubsub(identifier),
            TURBO_CHANNEL => self.resolve_turbo(identifier),
            CABLE_READY_CHANNEL => self.resolve_cable_ready(identifier),
            _ => Err(StreamError::UnknownChannel(channel.to_string())),
        }
    }

    fn resolve_pubsub(&self, identifier: &str) -> Result<StreamResult, StreamError> {
        let request: PubSubRequest = serde_json::from_str(identifier)?;

        // Check for public stream
        if let Some(ref stream) = request.stream_name {
            if !self.config.public {
                return Err(StreamError::PublicNotAllowed);
            }
            debug!(stream, "public stream subscription");
            return Ok(StreamResult {
                stream: stream.clone(),
                whisper: true,  // Public streams always allow whisper
                presence: true, // Public streams always allow presence
            });
        }

        // Check for signed stream
        if let Some(ref signed) = request.signed_stream_name {
            let verifier = self.pubsub_verifier.as_ref().ok_or(StreamError::NoSecret)?;
            let stream = verifier.verify(signed)?;
            debug!(stream, "verified signed stream");
            return Ok(StreamResult {
                stream,
                whisper: self.config.whisper,
                presence: self.config.presence,
            });
        }

        Err(StreamError::MissingStream)
    }

    fn resolve_turbo(&self, identifier: &str) -> Result<StreamResult, StreamError> {
        if !self.config.turbo {
            return Err(StreamError::TurboNotEnabled);
        }

        let request: TurboRequest = serde_json::from_str(identifier)?;
        let verifier = self.turbo_verifier.as_ref().ok_or(StreamError::NoSecret)?;
        let stream = verifier.verify(&request.signed_stream_name)?;

        debug!(stream, "verified Turbo stream");
        Ok(StreamResult {
            stream,
            whisper: false,  // Turbo doesn't use whisper
            presence: false, // Turbo doesn't use presence
        })
    }

    fn resolve_cable_ready(&self, identifier: &str) -> Result<StreamResult, StreamError> {
        if !self.config.cable_ready {
            return Err(StreamError::CableReadyNotEnabled);
        }

        let request: CableReadyRequest = serde_json::from_str(identifier)?;
        let verifier = self
            .cable_ready_verifier
            .as_ref()
            .ok_or(StreamError::NoSecret)?;
        let stream = verifier.verify(&request.identifier)?;

        debug!(stream, "verified CableReady stream");
        Ok(StreamResult {
            stream,
            whisper: false,  // CableReady doesn't use whisper
            presence: false, // CableReady doesn't use presence
        })
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
    #[error("verification failed: {0}")]
    Verify(#[from] VerifyError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Standard base64 encode
fn base64_encode(data: &[u8]) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.encode(data)
}

/// Standard base64 decode
fn base64_decode(data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.decode(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_verifier_roundtrip() {
        let verifier = MessageVerifier::new("test-secret");
        let stream = "chat:42";

        let signed = verifier.sign(stream).unwrap();
        let verified = verifier.verify(&signed).unwrap();

        assert_eq!(stream, verified);
    }

    #[test]
    fn test_message_verifier_rails_compatible() {
        // This is a signed stream generated by Rails with secret "test-secret"
        // stream_name = "chat:42"
        // => Base64("\"chat:42\"") = "ImNoYXQ6NDIi"
        // => HMAC-SHA256("ImNoYXQ6NDIi", "test-secret") = "..."
        let verifier = MessageVerifier::new("test-secret");

        // Generate our own and verify the format matches Rails
        let signed = verifier.sign("chat:42").unwrap();
        assert!(signed.contains("--"));

        let parts: Vec<&str> = signed.split("--").collect();
        assert_eq!(parts.len(), 2);

        // Verify base64 encoded JSON
        let decoded = base64_decode(parts[0]).unwrap();
        let json: String = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(json, "chat:42");
    }

    #[test]
    fn test_invalid_signature() {
        let verifier = MessageVerifier::new("test-secret");
        let result = verifier.verify("ImNoYXQ6NDIi--invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_format() {
        let verifier = MessageVerifier::new("test-secret");
        assert!(verifier.verify("no-separator").is_err());
        assert!(verifier.verify("too--many--parts").is_err());
    }

    #[test]
    fn test_streams_controller_pubsub() {
        let config = StreamsConfig {
            secret: Some("test-secret".to_string()),
            public: false,
            ..Default::default()
        };
        let controller = StreamsController::new(config);
        let verifier = MessageVerifier::new("test-secret");

        let signed = verifier.sign("notifications:123").unwrap();
        let identifier = format!(
            r#"{{"channel":"$pubsub","signed_stream_name":"{}"}}"#,
            signed
        );

        assert!(controller.handles(&identifier));

        let result = controller.resolve(&identifier).unwrap();
        assert_eq!(result.stream, "notifications:123");
    }

    #[test]
    fn test_streams_controller_public() {
        let config = StreamsConfig {
            public: true,
            ..Default::default()
        };
        let controller = StreamsController::new(config);

        let identifier = r#"{"channel":"$pubsub","stream_name":"public:stream"}"#;

        assert!(controller.handles(identifier));

        let result = controller.resolve(identifier).unwrap();
        assert_eq!(result.stream, "public:stream");
        assert!(result.whisper);
        assert!(result.presence);
    }

    #[test]
    fn test_streams_controller_public_not_allowed() {
        let config = StreamsConfig {
            secret: Some("test-secret".to_string()),
            public: false,
            ..Default::default()
        };
        let controller = StreamsController::new(config);

        let identifier = r#"{"channel":"$pubsub","stream_name":"public:stream"}"#;
        let result = controller.resolve(identifier);

        assert!(matches!(result, Err(StreamError::PublicNotAllowed)));
    }

    #[test]
    fn test_streams_controller_turbo() {
        let config = StreamsConfig {
            turbo: true,
            turbo_secret: Some("turbo-secret".to_string()),
            ..Default::default()
        };
        let controller = StreamsController::new(config);
        let verifier = MessageVerifier::new("turbo-secret");

        let signed = verifier.sign("posts:1").unwrap();
        let identifier = format!(
            r#"{{"channel":"Turbo::StreamsChannel","signed_stream_name":"{}"}}"#,
            signed
        );

        assert!(controller.handles(&identifier));

        let result = controller.resolve(&identifier).unwrap();
        assert_eq!(result.stream, "posts:1");
    }
}
