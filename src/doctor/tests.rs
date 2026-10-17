use super::*;
use crate::config::{Config, DockingConfig};
use crate::streams::{PreviousKeys, SecretBytes};

/// A configuration built by hand, the way `Config::from_env` would with the
/// listed settings: a direct listener, a database with a password, one
/// previous Turbo key, and public streams without the acknowledgement.
fn sample_config() -> Config {
    let mut config = crate::config::test_support::minimal_config();
    config.http.listen_addr = Some("127.0.0.1:8080".to_string());
    config.http.allowed_origins = vec!["https://app.example".to_string()];
    config.database_url = Some("postgres://cable:hunter2@db.internal:5432/orbitcast".to_string());
    config.namespace = "example_production".to_string();
    config.streams.secret = Some(SecretBytes::new(b"streams-key".to_vec()));
    config.streams.turbo = true;
    config.streams.turbo_secret = Some(SecretBytes::new(b"turbo-key".to_vec()));
    config.streams.turbo_previous_secrets =
        PreviousKeys::new(vec![SecretBytes::new(b"old-turbo-key".to_vec())]).unwrap();
    config.streams.public = true;
    config.docking = Some(DockingConfig {
        mothership_pid: 1,
        ship_name: "orbitcast".to_string(),
        socket_dir: std::path::PathBuf::from("/run/mothership"),
        routes: Vec::new(),
    });
    config
}

fn unreachable() -> Database {
    Database::failed(
        DatabaseStatus::Unreachable,
        "postgres://cable:hunter2@db.internal:5432/orbitcast",
        "connection refused (postgres://cable:hunter2@db.internal:5432/orbitcast)",
    )
}

#[test]
fn the_report_carries_what_the_gem_and_the_manifest_compare() {
    let report = Report::describe(&sample_config(), unreachable());
    assert_eq!(report.version, env!("CARGO_PKG_VERSION"));
    assert!(report.target.contains('-'), "a target triple: {}", report.target);
    assert_eq!(report.envelope_versions, &[1]);
    assert_ne!(report.features.len(), 0);
    for feature in &report.features {
        assert!(["postgres", "memory"].contains(feature), "{feature}");
    }
}

#[test]
fn keys_are_described_without_their_bytes() {
    let report = Report::describe(&sample_config(), unreachable());
    assert!(report.keys.streams.configured);
    assert_eq!(report.keys.streams.previous_keys, 0);
    assert_eq!(report.keys.turbo.enabled, Some(true));
    assert!(report.keys.turbo.configured);
    assert_eq!(report.keys.turbo.previous_keys, 1);
    assert_eq!(report.keys.cable_ready.enabled, Some(false));
    assert!(!report.keys.cable_ready.configured);
    assert!(!report.keys.recovery.configured);
    assert!(report.keys.public_streams.enabled);
    assert!(!report.keys.public_streams.insecure_acknowledged);

    for rendered in [report.render_json(), report.render_text()] {
        for secret in ["streams-key", "turbo-key", "old-turbo-key", "hunter2"] {
            assert!(!rendered.contains(secret), "{secret} leaked:\n{rendered}");
        }
    }
}

#[test]
fn validation_is_reported_not_enforced() {
    // Public streams without the acknowledgement fail validation; the report
    // still comes out whole.
    let report = Report::describe(&sample_config(), unreachable());
    assert!(!report.validation.ok);
    let error = report.validation.error.as_deref().unwrap();
    assert!(error.contains("public_streams_insecure"), "{error}");
    assert_eq!(report.transports.listen_addr.as_deref(), Some("127.0.0.1:8080"));
    assert_eq!(
        report.transports.docking.as_ref().unwrap().socket,
        "/run/mothership/orbitcast.sock"
    );
    assert_eq!(report.origins, vec!["https://app.example".to_string()]);
    assert!(report.render_text().contains("configuration:        INVALID"));

    let mut config = sample_config();
    config.streams.public_insecure = true;
    let report = Report::describe(&config, unreachable());
    assert!(report.validation.ok, "{:?}", report.validation.error);
}

#[test]
fn an_empty_configuration_still_reports() {
    let config = crate::config::test_support::minimal_config();
    let report = Report::describe(&config, Database::not_configured());
    assert_eq!(report.namespace, None);
    assert_eq!(report.database.status, DatabaseStatus::NotConfigured);
    assert_eq!(report.database.url, None);
    assert!(!report.validation.ok, "no transport is a validation error, not a crash");
    let json: serde_json::Value = serde_json::from_str(&report.render_json()).unwrap();
    assert_eq!(json["envelope_versions"], serde_json::json!([1]));
    assert_eq!(json["database"]["status"], "not_configured");
    assert!(json["database"].get("server_version").is_none(), "absent, not null");
}

#[test]
fn database_urls_are_redacted_and_errors_scrubbed() {
    let report = Report::describe(&sample_config(), unreachable());
    assert_eq!(
        report.database.url.as_deref(),
        Some("postgres://cable:[REDACTED]@db.internal:5432/orbitcast")
    );
    assert_eq!(report.database.status, DatabaseStatus::Unreachable);
    let error = report.database.error.as_deref().unwrap();
    assert!(!error.contains("hunter2"), "{error}");
    assert!(error.contains("[REDACTED]"), "{error}");
    assert!(report.render_text().contains("unreachable: connection refused"));
}

#[test]
fn password_extraction_handles_every_url_shape() {
    const CASES: &[(&str, Option<&str>)] = &[
        ("postgres://u:p@h/db", Some("p")),
        ("postgres://u:p%40ss@h/db", Some("p%40ss")),
        ("postgres://u@h/db", None),
        ("postgres://h/db", None),
        ("not a url", None),
        ("postgres://u:@h/db", Some("")),
    ];
    for (url, expected) in CASES {
        assert_eq!(password_of(url), *expected, "{url}");
    }
    // An empty password scrubs nothing (it would replace every position).
    assert_eq!(scrub_password("postgres://u:@h/db", "boom"), "boom");
}

#[tokio::test]
async fn probing_a_closed_port_is_unreachable_within_the_timeout() {
    // A bound-then-dropped listener gives a port nothing answers on.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let url = format!("postgres://orbitcast:s3cret@127.0.0.1:{port}/x");
    let started = std::time::Instant::now();
    let database = probe_database(&url, DATABASE_PROBE_TIMEOUT).await;
    assert!(started.elapsed() < DATABASE_PROBE_TIMEOUT);
    #[cfg(all(feature = "postgres", not(feature = "memory")))]
    assert_eq!(database.status, DatabaseStatus::Unreachable);
    #[cfg(not(all(feature = "postgres", not(feature = "memory"))))]
    assert_eq!(database.status, DatabaseStatus::UnsupportedByBuild);
    let error = database.error.unwrap();
    assert!(!error.contains("s3cret"), "{error}");
}
