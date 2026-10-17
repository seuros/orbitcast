//! presence

use super::*;

#[test]
fn presence_leases_default_to_the_documented_bounds() {
    let config = base_config();
    assert_eq!(config.presence_ttl, Duration::from_secs(45));
    assert_eq!(config.presence_refresh_interval, Duration::from_secs(10));
    assert!(
        config.presence_refresh_interval * 4 <= config.presence_ttl,
        "the default survives three consecutive missed refresh passes"
    );
    config.validate().unwrap();
}

#[test]
fn validate_rejects_unusable_presence_bounds() {
    let mut config = base_config();
    config.presence_ttl = Duration::ZERO;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("presence_ttl_ms", _)));

    let mut config = base_config();
    config.presence_ttl = Duration::from_millis(MAX_PRESENCE_TTL_MS + 1);
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("presence_ttl_ms", _)));

    // A refresh at or above the TTL cannot keep a lease alive.
    let mut config = base_config();
    config.presence_refresh_interval = config.presence_ttl;
    assert_matches!(
        config.validate(),
        Err(ConfigError::InvalidValue("presence_refresh_interval_ms", _))
    );

    let mut config = base_config();
    config.presence_refresh_interval = Duration::ZERO;
    assert_matches!(
        config.validate(),
        Err(ConfigError::InvalidValue("presence_refresh_interval_ms", _))
    );
}

#[test]
fn moored_config_sets_presence_bounds() {
    let mut config = base_config();
    config
        .apply_moored_config(&moored(&[
            ("presence_ttl_ms", "20000"),
            ("presence_refresh_interval_ms", "5000"),
        ]))
        .unwrap();

    assert_eq!(config.presence_ttl, Duration::from_secs(20));
    assert_eq!(config.presence_refresh_interval, Duration::from_secs(5));
    config.validate().unwrap();
}

#[test]
fn moored_config_refuses_unusable_presence_values() {
    for (key, value) in [("presence_ttl_ms", "0"), ("presence_refresh_interval_ms", "soon")] {
        let mut config = base_config();
        assert_matches!(
            config.apply_moored_config(&moored(&[(key, value)])),
            Err(ConfigError::InvalidHandshakeValue { .. }),
            "{key}={value:?}"
        );
        assert_eq!(config.presence_ttl, Duration::from_millis(DEFAULT_PRESENCE_TTL_MS));
        assert_eq!(
            config.presence_refresh_interval,
            Duration::from_millis(DEFAULT_PRESENCE_REFRESH_INTERVAL_MS)
        );
    }
}

// --- the former node-local tracker switch is gone ---

#[test]
fn streams_presence_is_no_longer_a_configuration_key() {
    // Presence is a Rails grant in dispatch mode; the signed-mode node-local
    // tracker and its switch were removed. A file
    // still carrying the key is refused rather than silently ignored, and the
    // handshake refuses it the same way.
    let err = toml::from_str::<FileConfig>("streams_presence = true").unwrap_err();
    assert!(err.to_string().contains("streams_presence"), "{err}");

    let mut config = base_config();
    assert_matches!(
        config.apply_moored_config(&moored(&[("streams_presence", "true")])),
        Err(ConfigError::UnknownHandshakeKey(_))
    );
}
