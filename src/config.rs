//! OrbitCast configuration
//!
//! Configuration comes from:
//! 1. Environment variables (MS_PID, MS_SHIP, MS_SOCKET_DIR)
//! 2. Handshake with Mothership (Moored.config)

use std::collections::HashMap;
use std::path::PathBuf;

use crate::streams::StreamsConfig;

/// OrbitCast configuration
#[derive(Debug, Clone)]
pub struct Config {
    /// Mothership PID
    pub mothership_pid: u32,
    /// Ship name
    pub ship_name: String,
    /// Socket directory
    pub socket_dir: PathBuf,
    /// Routes this ship handles (received from Mothership)
    #[allow(dead_code)]
    pub routes: Vec<String>,
    /// PostgreSQL connection string (from Moored.config)
    pub database_url: Option<String>,
    /// Ping interval in seconds (default: 3)
    pub ping_interval: u64,
    /// AnyCable RPC host (default: 127.0.0.1:50051)
    pub rpc_host: String,
    /// RPC request timeout in milliseconds (optional)
    pub rpc_request_timeout_ms: Option<u64>,
    /// Header allowlist for RPC (lowercased). Empty means forward all.
    pub rpc_headers: Vec<String>,
    /// Signed streams configuration
    pub streams: StreamsConfig,
}

impl Config {
    /// Create config from environment variables
    pub fn from_env() -> Result<Self, ConfigError> {
        let mothership_pid: u32 = std::env::var("MS_PID")
            .map_err(|_| ConfigError::MissingEnv("MS_PID"))?
            .parse()
            .map_err(|_| ConfigError::InvalidEnv("MS_PID", "expected u32"))?;

        let ship_name = std::env::var("MS_SHIP").map_err(|_| ConfigError::MissingEnv("MS_SHIP"))?;

        let socket_dir = std::env::var("MS_SOCKET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| default_socket_dir());

        Ok(Self {
            mothership_pid,
            ship_name,
            socket_dir,
            routes: Vec::new(),
            database_url: None,
            ping_interval: 3,
            rpc_host: "127.0.0.1:50051".to_string(),
            rpc_request_timeout_ms: None,
            rpc_headers: vec!["cookie".to_string()],
            streams: StreamsConfig::default(),
        })
    }

    /// Apply configuration from Moored handshake response
    pub fn apply_moored_config(&mut self, config: &HashMap<String, String>) {
        if let Some(url) = config.get("database_url") {
            self.database_url = Some(url.clone());
        }

        if let Some(interval) = config.get("ping_interval")
            && let Ok(secs) = interval.parse()
        {
            self.ping_interval = secs;
        }

        if let Some(host) = config.get("rpc_host") {
            self.rpc_host = host.clone();
        }

        if let Some(timeout) = config.get("rpc_request_timeout_ms")
            && let Ok(ms) = timeout.parse::<u64>()
        {
            self.rpc_request_timeout_ms = if ms == 0 { None } else { Some(ms) };
        }

        if let Some(headers) = config.get("rpc_headers") {
            let trimmed = headers.trim();
            if trimmed == "*" || trimmed.eq_ignore_ascii_case("all") {
                self.rpc_headers.clear();
            } else {
                self.rpc_headers = trimmed
                    .split(',')
                    .map(|h| h.trim().to_lowercase())
                    .filter(|h| !h.is_empty())
                    .collect();
            }
        }

        // Signed streams configuration
        if let Some(secret) = config.get("streams_secret")
            && !secret.is_empty()
        {
            self.streams.secret = Some(secret.clone());
        }

        if let Some(public) = config.get("public_streams") {
            self.streams.public = public.eq_ignore_ascii_case("true") || public == "1";
        }

        if let Some(whisper) = config.get("streams_whisper") {
            self.streams.whisper = whisper.eq_ignore_ascii_case("true") || whisper == "1";
        }

        if let Some(presence) = config.get("streams_presence") {
            self.streams.presence = presence.eq_ignore_ascii_case("true") || presence == "1";
        }

        // Turbo Streams
        if let Some(turbo) = config.get("turbo_streams") {
            self.streams.turbo = turbo.eq_ignore_ascii_case("true") || turbo == "1";
        }

        if let Some(secret) = config.get("turbo_streams_secret")
            && !secret.is_empty()
        {
            self.streams.turbo_secret = Some(secret.clone());
        }

        // CableReady
        if let Some(cable_ready) = config.get("cable_ready_streams") {
            self.streams.cable_ready = cable_ready.eq_ignore_ascii_case("true") || cable_ready == "1";
        }

        if let Some(secret) = config.get("cable_ready_secret")
            && !secret.is_empty()
        {
            self.streams.cable_ready_secret = Some(secret.clone());
        }
    }

    /// Get the Unix socket path
    pub fn socket_path(&self) -> PathBuf {
        self.socket_dir.join(format!("{}.sock", self.ship_name))
    }
}

/// Get default socket directory
fn default_socket_dir() -> PathBuf {
    std::env::var("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir())
        .join("mothership")
}

/// Configuration error
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing environment variable: {0}")]
    MissingEnv(&'static str),
    #[error("invalid environment variable {0}: {1}")]
    InvalidEnv(&'static str, &'static str),
}
