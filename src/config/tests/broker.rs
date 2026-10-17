//! retained broker and recovery

use super::*;

#[test]
fn the_broker_and_recovery_are_off_by_default() {
    let config = base_config();
    assert!(!config.broker.enabled, "retained history needs a migration");
    assert!(config.recovery_secret.is_none(), "recovery needs its own key");
    assert!(!config.broker.handles("chat:42"));
    config.validate().unwrap();
}

#[test]
fn validate_rejects_recovery_without_a_broker() {
    // A recovery key without retained history would advertise a channel
    // that can replay nothing, which is worse than no channel at all.
    let mut config = base_config();
    config.recovery_secret = Some(SecretBytes::new(b"recovery".to_vec()));
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("recovery_secret", _)));
}

#[cfg(not(feature = "memory"))]
#[test]
fn validate_rejects_a_broker_without_a_database_and_accepts_a_complete_one() {
    // Without a database the PostgreSQL backend itself refuses first; the
    // broker's own check is what a memory build would hit.
    let mut config = base_config();
    config.broker.enabled = true;
    config.database_url = None;
    assert_matches!(
        config.validate(),
        Err(ConfigError::InvalidValue("database_url" | "broker_enabled", _))
    );

    // Enabled, with a database and a key: usable.
    let mut config = base_config();
    config.database_url = Some("postgres://localhost/cable".to_string());
    config.broker.enabled = true;
    config.recovery_secret = Some(SecretBytes::new(b"recovery".to_vec()));
    config.validate().unwrap();
}

#[cfg(not(feature = "memory"))]
#[test]
fn validate_rejects_unusable_broker_bounds() {
    let mut config = base_config();
    config.database_url = Some("postgres://localhost/cable".to_string());
    config.broker.enabled = true;
    config.broker.replay_buffer = 0;
    let err = config.validate().unwrap_err();
    assert_matches!(err, ConfigError::Invalid("broker", _), "{err:?}");
    assert!(err.to_string().contains("broker_replay_buffer"), "{err}");
}

#[cfg(feature = "memory")]
#[test]
fn a_memory_build_refuses_the_broker_at_the_configuration_boundary() {
    // Retained history needs PostgreSQL; a single-node memory build must
    // fail at boot rather than accept recovery it can never serve.
    let mut config = base_config();
    config.database_url = Some("postgres://localhost/cable".to_string());
    config.broker.enabled = true;
    let err = config.validate().unwrap_err();
    assert_matches!(err, ConfigError::Invalid("broker", _), "{err:?}");
    assert!(err.to_string().contains("postgres"), "{err}");
}

#[test]
fn moored_config_sets_broker_bounds_and_the_recovery_key() {
    let mut config = base_config();
    config
        .apply_moored_config(&moored(&[
            ("broker_enabled", "true"),
            ("broker_streams", "chat:, orders:"),
            ("broker_retention_secs", "60"),
            ("broker_max_messages_per_stream", "25"),
            ("broker_max_bytes_per_stream", "65536"),
            ("broker_reconcile_interval_ms", "1500"),
            ("broker_replay_buffer", "16"),
            ("broker_session_ttl_secs", "120"),
            ("broker_max_messages_per_namespace", "5000"),
            ("broker_max_bytes_per_namespace", "1048576"),
            ("recovery_secret", "moored-recovery-key"),
        ]))
        .unwrap();

    assert!(config.broker.enabled);
    assert_eq!(config.broker.streams, vec!["chat:".to_string(), "orders:".to_string()]);
    assert!(config.broker.handles("chat:42"));
    assert!(!config.broker.handles("telemetry:cpu"));
    assert_eq!(config.broker.retention, Duration::from_mins(1));
    assert_eq!(config.broker.max_messages_per_stream, 25);
    assert_eq!(config.broker.max_bytes_per_stream, 65_536);
    assert_eq!(config.broker.reconcile_interval, Duration::from_millis(1500));
    assert_eq!(config.broker.replay_buffer, 16);
    assert_eq!(config.broker.session_ttl, Duration::from_mins(2));
    assert_eq!(config.broker.max_messages_per_namespace, 5000);
    assert_eq!(config.broker.max_bytes_per_namespace, 1_048_576);
    assert_eq!(
        config.recovery_secret.as_ref().map(|s| s.as_bytes().to_vec()),
        Some(b"moored-recovery-key".to_vec()),
        "a plain-text handshake secret is used as raw UTF-8 key bytes"
    );
}

#[test]
fn moored_config_refuses_unusable_broker_values() {
    for (key, value) in [
        ("broker_enabled", "maybe"),
        ("broker_retention_secs", "0"),
        ("broker_replay_buffer", "nonsense"),
    ] {
        let mut config = base_config();
        assert_matches!(
            config.apply_moored_config(&moored(&[(key, value)])),
            Err(ConfigError::InvalidHandshakeValue { .. }),
            "{key}={value:?}"
        );
    }

    // An empty prefix list is an explicit "retain everything", never an error.
    let mut config = base_config();
    config.apply_moored_config(&moored(&[("broker_streams", "")])).unwrap();
    assert_matches!(config.broker.streams.as_slice(), []);
}

#[test]
fn an_environment_recovery_key_wins_over_the_handshake() {
    let encoded = STANDARD.encode(b"env-recovery-key");
    let config = with_env(&[("ORBITCAST_RECOVERY_SECRET_BASE64", encoded.as_str())], |env| {
        Config::from_env_over(env, &FileConfig::default())
    });

    let mut config = config.expect("config loads");
    config.apply_moored_config(&moored(&[("recovery_secret", "moored-key")])).unwrap();
    assert_eq!(
        config.recovery_secret.as_ref().map(|s| s.as_bytes().to_vec()),
        Some(b"env-recovery-key".to_vec())
    );
}

#[test]
fn stream_prefix_lists_drop_empty_entries() {
    assert_eq!(parse_prefixes("chat:, orders:"), vec!["chat:", "orders:"]);
    assert_eq!(parse_prefixes(r#"["chat:","orders:"]"#), vec!["chat:", "orders:"]);
    assert!(parse_prefixes(" , ,").is_empty(), "an empty entry never becomes a wildcard");
}
