//! `orbitcast doctor`: what this binary is, what it was told, and whether the
//! database answers — without secrets.
//!
//! The report diagnoses; it never gates. A node whose database is down, or
//! whose configuration selects no transport, still produces a full report
//! and exits `0` so an operator (or the gem's binary verification, which
//! runs `doctor --json` with no configuration at all) can read what is
//! there. Only a configuration that cannot be *parsed* is an error.
//!
//! Key material never appears: every key is reported as configured or not
//! plus the number of verify-only previous keys in its ring, and database
//! URLs go through [`redact_url`]. A connection error is scrubbed of the
//! password as well, in case a driver ever echoes the DSN back.

use std::fmt::Write as _;
use std::time::Duration;

use serde::Serialize;

use crate::config::{Config, DispatchMode, redact_url};
use crate::streams::StreamsConfig;

/// Wire envelope versions this binary speaks. The gem
/// requires its own version to be listed.
pub const ENVELOPE_VERSIONS: &[u32] = &[crate::protocol::v1::VERSION];

/// The Rust target triple this binary was compiled for (set by `build.rs`).
pub const TARGET: &str = env!("ORBITCAST_TARGET");

/// How long the database probe waits for a connection and a version query.
pub const DATABASE_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// The pub/sub backend features this binary was built with.
#[must_use]
pub fn features() -> Vec<&'static str> {
    [cfg!(feature = "postgres").then_some("postgres"), cfg!(feature = "memory").then_some("memory")]
        .into_iter()
        .flatten()
        .collect()
}

/// The whole report, rendered as one JSON object by `--json`.
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    /// Crate version; the gem requires an exact match for an overridden binary.
    pub version: &'static str,
    /// Rust target triple ([`TARGET`]).
    pub target: &'static str,
    /// Backend features ([`features`]).
    pub features: Vec<&'static str>,
    /// [`ENVELOPE_VERSIONS`].
    pub envelope_versions: &'static [u32],
    /// Deployment namespace; `None` when unset.
    pub namespace: Option<String>,
    /// `signed_only` or `rails_dispatch`.
    pub dispatch_mode: &'static str,
    /// Outcome of `Config::validate`: diagnostic only.
    pub validation: Validation,
    pub transports: Transports,
    /// Allowed browser origins, normalized. Empty refuses every browser
    /// origin; `*` allows any.
    pub origins: Vec<String>,
    pub keys: Keys,
    pub database: Database,
}

/// Whether the parsed configuration would start a relay.
#[derive(Debug, Clone, Serialize)]
pub struct Validation {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Which listeners the configuration selects.
#[derive(Debug, Clone, Serialize)]
pub struct Transports {
    /// Direct HTTP/WebSocket listener bind.
    pub listen_addr: Option<String>,
    /// Mothership docking, when the `MS_*` environment is present.
    pub docking: Option<Docking>,
    /// Private publication ingress bind.
    pub ingress_listen_addr: Option<String>,
    /// Whether `/metrics` is also mounted, unauthenticated, on the public listener.
    pub metrics_public: bool,
}

/// Docking transport parameters.
#[derive(Debug, Clone, Serialize)]
pub struct Docking {
    pub ship: String,
    pub socket: String,
}

/// Key configuration per signing profile and service credential.
#[derive(Debug, Clone, Serialize)]
pub struct Keys {
    pub streams: KeyStatus,
    pub turbo: KeyStatus,
    pub cable_ready: KeyStatus,
    pub recovery: KeyStatus,
    pub rpc_token: KeyStatus,
    pub ingress_token: KeyStatus,
    /// Unsigned public streams, with the explicit insecure acknowledgement.
    pub public_streams: PublicStreams,
}

/// One key slot: never the bytes, only whether they are there.
#[derive(Debug, Clone, Serialize)]
pub struct KeyStatus {
    /// The profile switch, for profiles that have one (`turbo`, `cable_ready`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Whether a current key is configured.
    pub configured: bool,
    /// Verify-only previous keys in the ring.
    pub previous_keys: usize,
}

/// Public (unsigned) stream switches.
#[derive(Debug, Clone, Serialize)]
pub struct PublicStreams {
    pub enabled: bool,
    pub insecure_acknowledged: bool,
}

/// Database configuration and reachability.
#[derive(Debug, Clone, Serialize)]
pub struct Database {
    /// `database_url`, password redacted.
    pub url: Option<String>,
    /// `listener_database_url`, password redacted.
    pub listener_url: Option<String>,
    pub status: DatabaseStatus,
    /// `server_version` reported by PostgreSQL, when reachable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_version: Option<String>,
    /// The connection error, password scrubbed, when unreachable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Outcome of the database probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseStatus {
    /// Connected and answered `SHOW server_version` within the timeout.
    Reachable,
    /// A `database_url` is configured but the probe failed or timed out.
    Unreachable,
    /// No `database_url` configured.
    NotConfigured,
    /// A `database_url` is configured but this build has no PostgreSQL
    /// backend (`memory` feature only).
    UnsupportedByBuild,
}

impl DatabaseStatus {
    /// The wire/label spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reachable => "reachable",
            Self::Unreachable => "unreachable",
            Self::NotConfigured => "not_configured",
            Self::UnsupportedByBuild => "unsupported_by_build",
        }
    }
}

impl Report {
    /// Describe `config` and probe its database (bounded by
    /// [`DATABASE_PROBE_TIMEOUT`]).
    pub async fn gather(config: &Config) -> Self {
        let database = match config.database_url.as_deref() {
            Some(url) => probe_database(url, DATABASE_PROBE_TIMEOUT).await,
            None => Database::not_configured(),
        };
        Self::describe(config, database)
    }

    /// Describe `config` with an already-known database outcome (no probe).
    #[must_use]
    pub fn describe(config: &Config, mut database: Database) -> Self {
        database.url = config.database_url.as_deref().map(redact_url);
        database.listener_url = config.listener_database_url.as_deref().map(redact_url);

        let validation = match config.validate() {
            Ok(()) => Validation { ok: true, error: None },
            Err(error) => Validation { ok: false, error: Some(error.to_string()) },
        };

        Self {
            version: env!("CARGO_PKG_VERSION"),
            target: TARGET,
            features: features(),
            envelope_versions: ENVELOPE_VERSIONS,
            namespace: (!config.namespace.is_empty()).then(|| config.namespace.clone()),
            dispatch_mode: match config.dispatch_mode() {
                DispatchMode::SignedOnly => "signed_only",
                DispatchMode::RailsDispatch => "rails_dispatch",
            },
            validation,
            transports: Transports {
                listen_addr: config.http.listen_addr.clone(),
                docking: config.docking.as_ref().map(|docking| Docking {
                    ship: docking.ship_name.clone(),
                    socket: docking.socket_path().display().to_string(),
                }),
                ingress_listen_addr: config.ingress.listen_addr.clone(),
                metrics_public: config.http.metrics_public,
            },
            origins: config.http.allowed_origins.clone(),
            keys: Keys::describe(config),
            database,
        }
    }

    /// The human-readable rendering (default output).
    #[must_use]
    pub fn render_text(&self) -> String {
        let mut out = String::with_capacity(1024);
        let none = "(none)";
        let line = |out: &mut String, label: &str, value: &str| {
            // `fmt::Write` for `String` has no error path.
            let _ = writeln!(out, "{label:<22}{value}");
        };

        // `fmt::Write` for `String` has no error path.
        let _ = writeln!(out, "orbitcast {} ({})", self.version, self.target);
        line(&mut out, "features:", &self.features.join(", "));
        line(&mut out, "envelope versions:", &join(self.envelope_versions));
        line(&mut out, "namespace:", self.namespace.as_deref().unwrap_or("(unset)"));
        line(&mut out, "mode:", self.dispatch_mode);
        match &self.validation.error {
            None => line(&mut out, "configuration:", "ok"),
            Some(error) => line(&mut out, "configuration:", &format!("INVALID: {error}")),
        }

        // `fmt::Write` for `String` has no error path.
        let _ = writeln!(out, "transports:");
        line(&mut out, "  listen_addr:", self.transports.listen_addr.as_deref().unwrap_or(none));
        match &self.transports.docking {
            Some(docking) => {
                line(
                    &mut out,
                    "  docking:",
                    &format!("ship {} at {}", docking.ship, docking.socket),
                );
            }
            None => line(&mut out, "  docking:", none),
        }
        line(
            &mut out,
            "  ingress:",
            self.transports.ingress_listen_addr.as_deref().unwrap_or(none),
        );
        line(
            &mut out,
            "  metrics_public:",
            if self.transports.metrics_public { "true" } else { "false" },
        );

        if self.origins.is_empty() {
            line(&mut out, "origins:", "(none: every browser Origin is refused)");
        } else {
            line(&mut out, "origins:", &self.origins.join(", "));
        }

        // `fmt::Write` for `String` has no error path.
        let _ = writeln!(out, "keys:");
        line(&mut out, "  streams:", &self.keys.streams.render());
        line(&mut out, "  turbo:", &self.keys.turbo.render());
        line(&mut out, "  cable_ready:", &self.keys.cable_ready.render());
        line(&mut out, "  recovery:", &self.keys.recovery.render());
        line(&mut out, "  rpc_token:", &self.keys.rpc_token.render());
        line(&mut out, "  ingress_token:", &self.keys.ingress_token.render());
        let public = &self.keys.public_streams;
        line(
            &mut out,
            "  public_streams:",
            match (public.enabled, public.insecure_acknowledged) {
                (false, _) => "disabled",
                (true, true) => "ENABLED (insecure acknowledged)",
                (true, false) => "enabled WITHOUT public_streams_insecure (refused by validation)",
            },
        );

        // `fmt::Write` for `String` has no error path.
        let _ = writeln!(out, "database:");
        line(&mut out, "  url:", self.database.url.as_deref().unwrap_or("(not configured)"));
        if let Some(listener) = &self.database.listener_url {
            line(&mut out, "  listener_url:", listener);
        }
        let status = match (&self.database.server_version, &self.database.error) {
            (Some(version), _) => {
                format!("{} (PostgreSQL {version})", self.database.status.as_str())
            }
            (None, Some(error)) => format!("{}: {error}", self.database.status.as_str()),
            (None, None) => self.database.status.as_str().to_string(),
        };
        line(&mut out, "  status:", &status);
        out
    }

    /// The JSON rendering (`--json`): one object, pretty-printed.
    ///
    /// # Panics
    ///
    /// Never in practice: the report is plain strings, booleans, and
    /// integers, which `serde_json` always serializes.
    #[must_use]
    pub fn render_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("the report holds only serializable values")
    }
}

impl Keys {
    const fn describe(config: &Config) -> Self {
        let StreamsConfig {
            secret,
            previous_secrets,
            public,
            public_insecure,
            turbo,
            turbo_secret,
            turbo_previous_secrets,
            cable_ready,
            cable_ready_secret,
            cable_ready_previous_secrets,
            ..
        } = &config.streams;
        Self {
            streams: KeyStatus {
                enabled: None,
                configured: secret.is_some(),
                previous_keys: previous_secrets.len(),
            },
            turbo: KeyStatus {
                enabled: Some(*turbo),
                configured: turbo_secret.is_some(),
                previous_keys: turbo_previous_secrets.len(),
            },
            cable_ready: KeyStatus {
                enabled: Some(*cable_ready),
                configured: cable_ready_secret.is_some(),
                previous_keys: cable_ready_previous_secrets.len(),
            },
            recovery: KeyStatus::single(config.recovery_secret.is_some()),
            rpc_token: KeyStatus::single(config.rpc.token.is_some()),
            ingress_token: KeyStatus::single(config.ingress.token.is_some()),
            public_streams: PublicStreams {
                enabled: *public,
                insecure_acknowledged: *public_insecure,
            },
        }
    }
}

impl KeyStatus {
    const fn single(configured: bool) -> Self {
        Self { enabled: None, configured, previous_keys: 0 }
    }

    fn render(&self) -> String {
        let mut text = match self.enabled {
            Some(true) => "enabled, ".to_string(),
            Some(false) => "disabled, ".to_string(),
            None => String::new(),
        };
        text.push_str(if self.configured { "configured" } else { "not configured" });
        if self.previous_keys > 0 {
            // `fmt::Write` for `String` has no error path.
            let _ = write!(text, " (+{} previous)", self.previous_keys);
        }
        text
    }
}

impl Database {
    const fn not_configured() -> Self {
        Self {
            url: None,
            listener_url: None,
            status: DatabaseStatus::NotConfigured,
            server_version: None,
            error: None,
        }
    }

    /// A probe that produced no version: `status` plus `error`, with the
    /// password of `url` scrubbed out of the text should a driver echo it.
    fn failed(status: DatabaseStatus, url: &str, error: impl std::fmt::Display) -> Self {
        Self {
            url: None,
            listener_url: None,
            status,
            server_version: None,
            error: Some(scrub_password(url, &error.to_string())),
        }
    }

    #[cfg(all(feature = "postgres", not(feature = "memory")))]
    fn unreachable(url: &str, error: impl std::fmt::Display) -> Self {
        Self::failed(DatabaseStatus::Unreachable, url, error)
    }
}

/// Connect to `url` and ask for its version, giving up after `timeout`.
///
/// Never panics and never prints the password: the only text that leaves
/// here is the status, the server version, or a scrubbed error.
#[cfg_attr(
    not(all(feature = "postgres", not(feature = "memory"))),
    expect(clippy::unused_async, reason = "the signature is the same whichever backend was built")
)]
pub async fn probe_database(url: &str, timeout: Duration) -> Database {
    #[cfg(all(feature = "postgres", not(feature = "memory")))]
    {
        match tokio::time::timeout(timeout, query_server_version(url)).await {
            Ok(Ok(version)) => Database {
                url: None,
                listener_url: None,
                status: DatabaseStatus::Reachable,
                server_version: Some(version),
                error: None,
            },
            Ok(Err(error)) => Database::unreachable(url, error),
            Err(_) => Database::unreachable(
                url,
                format_args!("no answer within {} s", timeout.as_secs_f64()),
            ),
        }
    }
    #[cfg(not(all(feature = "postgres", not(feature = "memory"))))]
    {
        Database::failed(
            DatabaseStatus::UnsupportedByBuild,
            url,
            format_args!(
                "this build ({}) has no PostgreSQL backend; {} is ignored (probe timeout {} s \
                 unused)",
                features().join(","),
                redact_url(url),
                timeout.as_secs_f64()
            ),
        )
    }
}

#[cfg(all(feature = "postgres", not(feature = "memory")))]
async fn query_server_version(url: &str) -> anyhow::Result<String> {
    let open = crate::pg::OpenClient::connect(url, "doctor").await?;
    let row = open.client.query_one("SHOW server_version", &[]).await?;
    Ok(row.try_get::<_, String>(0)?)
}

/// Remove the password of `url` from `text`, should a driver echo it.
fn scrub_password(url: &str, text: &str) -> String {
    match password_of(url) {
        Some(password) if !password.is_empty() => text.replace(password, "[REDACTED]"),
        _ => text.to_string(),
    }
}

/// The password in a `scheme://user:password@host` URL, if any.
fn password_of(url: &str) -> Option<&str> {
    let (_, rest) = url.split_once("://")?;
    let (userinfo, _) = rest.rsplit_once('@')?;
    userinfo.split_once(':').map(|(_, password)| password)
}

fn join(values: &[u32]) -> String {
    values.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
}

#[cfg(test)]
mod tests;
