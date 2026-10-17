//! TOML config file (`ORBITCAST_CONFIG`)

use super::*;

#[test]
fn file_config_fills_values_the_environment_left_unset() {
    let file: FileConfig = toml::from_str(
        r#"
            listen_addr = "127.0.0.1:9443"
            allowed_origins = ["https://App.Example/"]
            namespace = "file_namespace"
            database_url = "postgres://localhost/file_db"
            max_message_size = 2048
            shutdown_timeout_ms = 5000
            ping_interval = 7
            overflow_enabled = true
            overflow_ttl_ms = 60000
            turbo_streams = true
            turbo_streams_secret = "turbo-file-key"
            turbo_previous_secrets = ["turbo-old-1", "turbo-old-2"]
            "#,
    )
    .unwrap();

    // With an empty environment the file values land as-is over the defaults.
    let config = with_env(&[], |env| Config::from_env_over(env, &file)).unwrap();
    assert_eq!(config.http.listen_addr.as_deref(), Some("127.0.0.1:9443"));
    assert_eq!(config.http.allowed_origins, vec!["https://app.example".to_string()]);
    assert_eq!(config.namespace, "file_namespace");
    assert_eq!(config.database_url.as_deref(), Some("postgres://localhost/file_db"));
    assert_eq!(config.http.max_message_size, 2048);
    assert_eq!(config.shutdown_timeout, Duration::from_secs(5));
    assert_eq!(config.ping_interval, 7);
    assert!(config.overflow.enabled);
    assert_eq!(config.overflow.ttl, Duration::from_mins(1));
    assert!(config.streams.turbo);
    assert_eq!(
        config.streams.turbo_secret.as_ref().map(SecretBytes::as_bytes),
        Some(&b"turbo-file-key"[..])
    );
    assert_eq!(
        config.streams.turbo_previous_secrets.as_slice(),
        &[SecretBytes::new(b"turbo-old-1".to_vec()), SecretBytes::new(b"turbo-old-2".to_vec())],
        "file previous keys pass through as raw UTF-8 bytes, in order"
    );
}

#[test]
fn file_config_rejects_unknown_keys() {
    let err = toml::from_str::<FileConfig>("listen_adr = \"127.0.0.1:8080\"").unwrap_err();
    assert!(err.to_string().contains("listen_adr"), "{err}");
}

#[test]
fn file_config_rejects_empty_secrets() {
    let file: FileConfig = toml::from_str("streams_secret = \"\"").unwrap();
    assert_matches!(
        with_env(&[], |env| Config::from_env_over(env, &file)),
        Err(ConfigError::InvalidValue("streams_secret", _))
    );
}

#[test]
fn missing_named_config_file_is_a_hard_error() {
    let result = with_env(&[("ORBITCAST_CONFIG", "/nonexistent/orbitcast.toml")], FileConfig::load);
    assert_matches!(result, Err(ConfigError::ConfigFile { .. }));
}
