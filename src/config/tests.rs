use std::assert_matches;

use super::*;

/// A docked node with a database and a namespace: the smallest configuration
/// that validates in a PostgreSQL build.
fn base_config() -> Config {
    let mut config = super::test_support::minimal_config();
    config.docking = Some(DockingConfig {
        mothership_pid: 1,
        ship_name: "orbitcast".to_string(),
        socket_dir: PathBuf::from("/tmp"),
        routes: Vec::new(),
    });
    config.database_url = Some("postgres://cable:s3cret@localhost/orbitcast".to_string());
    config.namespace = "example_production".to_string();
    config
}

/// Run `f` against an [`Env`] holding exactly `pairs`.
///
/// Never the process environment: these tests stay hermetic and race-free
/// under parallel test threads.
fn with_env<R>(pairs: &[(&str, &str)], f: impl FnOnce(Env<'_>) -> R) -> R {
    let lookup = |var: &str| {
        pairs.iter().find(|(name, _)| *name == var).map(|(_, value)| (*value).to_string())
    };
    f(Env { lookup: &lookup })
}

/// A dispatch-mode configuration: loopback endpoint plus a credential.
fn dispatch_config() -> Config {
    let mut config = base_config();
    config.rpc.endpoint_url = Some("http://127.0.0.1:3000/_orbitcast/v1/rails".to_string());
    config.rpc.token = Some(SecretBytes::new(b"service-token".to_vec()));
    config
}

fn moored(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn the_admission_limits_have_documented_defaults() {
    let http = HttpConfig::default();
    assert_eq!(http.rate_connect_per_ip_per_min, DEFAULT_RATE_CONNECT_PER_IP_PER_MIN);
    assert_eq!(http.rate_commands_per_conn_per_sec, DEFAULT_RATE_COMMANDS_PER_CONN_PER_SEC);
    assert_eq!(http.rate_subscribes_per_conn_per_sec, DEFAULT_RATE_SUBSCRIBES_PER_CONN_PER_SEC);
    assert_eq!(http.handshake_timeout, DEFAULT_HANDSHAKE_TIMEOUT);
    assert_eq!(http.outbound_queue_messages, DEFAULT_OUTBOUND_QUEUE_MESSAGES);
    assert_eq!(http.outbound_queue_bytes, DEFAULT_OUTBOUND_QUEUE_BYTES);
    assert_eq!(http.max_header_bytes, DEFAULT_MAX_HEADER_BYTES);
    assert!(http.trusted_proxies.is_empty(), "no proxy is trusted until listed");
    assert!(!http.metrics_public, "an administrative route is never on by default");
}

#[test]
fn queue_and_header_bounds_are_validated() {
    let mut config = base_config();
    config.http.outbound_queue_messages = 0;
    assert_matches!(
        config.validate(),
        Err(ConfigError::InvalidValue("outbound_queue_messages", _))
    );

    let mut config = base_config();
    config.http.outbound_queue_bytes = 0;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("outbound_queue_bytes", _)));

    let mut config = base_config();
    config.http.rate_subscribes_per_conn_per_sec = MAX_RATE_LIMIT + 1;
    assert_matches!(
        config.validate(),
        Err(ConfigError::InvalidValue("rate_subscribes_per_conn_per_sec", _))
    );

    for bytes in [MIN_MAX_HEADER_BYTES - 1, MAX_MAX_HEADER_BYTES + 1] {
        let mut config = base_config();
        config.http.max_header_bytes = bytes;
        assert_matches!(
            config.validate(),
            Err(ConfigError::InvalidValue("max_header_bytes", _)),
            "{bytes}"
        );
    }

    let mut config = base_config();
    config.listener_database_url = Some("  ".to_string());
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("listener_database_url", _)));
}

#[cfg(all(feature = "postgres", not(feature = "memory")))]
#[test]
fn the_postgres_backend_requires_a_database_url_and_a_namespace() {
    let mut config = base_config();
    config.database_url = None;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("database_url", _)));

    let mut config = base_config();
    config.namespace = String::new();
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("namespace", _)));
}

#[test]
fn the_new_listener_keys_are_read_from_the_environment() {
    let config = with_env(
        &[
            ("ORBITCAST_OUTBOUND_QUEUE_MESSAGES", "16"),
            ("ORBITCAST_OUTBOUND_QUEUE_BYTES", "65536"),
            ("ORBITCAST_RATE_SUBSCRIBES_PER_CONN_PER_SEC", "5"),
            ("ORBITCAST_MAX_HEADER_BYTES", "32768"),
            ("ORBITCAST_TRUSTED_PROXIES", "10.0.0.0/8, fd00::/8,192.0.2.1"),
            ("ORBITCAST_LISTENER_DATABASE_URL", "postgres://cable@db-primary/orbitcast"),
            ("ORBITCAST_LISTEN_ADDR", "127.0.0.1:0"),
        ],
        |env| Config::from_env_over(env, &FileConfig::default()),
    )
    .expect("config loads");

    assert_eq!(config.http.outbound_queue_messages, 16);
    assert_eq!(config.http.outbound_queue_bytes, 65_536);
    assert_eq!(config.http.rate_subscribes_per_conn_per_sec, 5);
    assert_eq!(config.http.max_header_bytes, 32_768);
    assert_eq!(
        config.http.trusted_proxies.iter().map(ToString::to_string).collect::<Vec<_>>(),
        vec!["10.0.0.0/8", "fd00::/8", "192.0.2.1/32"]
    );
    assert_eq!(
        config.listener_database_url.as_deref(),
        Some("postgres://cable@db-primary/orbitcast")
    );
    for key in ["outbound_queue_messages", "trusted_proxies", "listener_database_url"] {
        assert!(config.explicit.contains(key), "{key} is recorded as explicit");
    }
    assert!(!config.explicit.contains("namespace"));

    let error = with_env(&[("ORBITCAST_TRUSTED_PROXIES", "10.0.0.0/99")], |env| {
        Config::from_env_over(env, &FileConfig::default())
    })
    .expect_err("a malformed network is never a narrower trust list");
    assert_matches!(error, ConfigError::InvalidValue("trusted_proxies", _));
}

#[test]
fn cidr_membership_is_exact_and_family_scoped() {
    let cases: &[(&str, &str, bool)] = &[
        ("10.0.0.0/8", "10.200.3.4", true),
        ("10.0.0.0/8", "11.0.0.1", false),
        ("192.0.2.1", "192.0.2.1", true),
        ("192.0.2.1", "192.0.2.2", false),
        ("0.0.0.0/0", "203.0.113.9", true),
        ("fd00::/8", "fd12::1", true),
        ("fd00::/8", "fe80::1", false),
        ("::/0", "2001:db8::1", true),
        ("10.0.0.0/8", "::ffff:10.0.0.1", false),
    ];
    for (network, ip, expected) in cases {
        let cidr = Cidr::parse(network).expect(network);
        assert_eq!(cidr.contains(ip.parse().unwrap()), *expected, "{ip} in {network}");
    }
    assert!(Cidr::parse("10.0.0.0/33").is_err());
    assert!(Cidr::parse("not-an-ip").is_err());
    assert!(Cidr::parse("fd00::/129").is_err());
}

#[test]
fn only_a_trusted_proxy_may_supply_the_client_address() {
    let ip = |text: &str| text.parse::<IpAddr>().unwrap();
    let mut http = HttpConfig::default();
    let client = ip("203.0.113.9");
    let proxy = ip("10.0.0.2");
    let edge = ip("10.0.0.3");

    // Untrusted peer: the header is ignored, whatever it says.
    assert_eq!(http.client_ip(proxy, &[client]), proxy);

    http.trusted_proxies = vec![Cidr::parse("10.0.0.0/8").unwrap()];
    // Trusted peer, one hop: the forwarded client.
    assert_eq!(http.client_ip(proxy, &[client]), client);
    // Trusted chain: walk past every trusted hop to the first untrusted one.
    assert_eq!(http.client_ip(proxy, &[client, edge]), client);
    // A chain made only of trusted proxies, or no header: the peer.
    assert_eq!(http.client_ip(proxy, &[edge]), proxy);
    assert_eq!(http.client_ip(proxy, &[]), proxy);
    // A spoofed inner hop cannot push the attribution to an arbitrary address
    // behind the real client: the rightmost untrusted address wins.
    assert_eq!(http.client_ip(proxy, &[ip("198.51.100.1"), client]), client);
}

#[test]
fn debug_output_redacts_passwords_and_plain_text_secrets() {
    assert_eq!(
        redact_url("postgres://cable:hunter2@db.internal:5432/orbitcast?sslmode=require"),
        "postgres://cable:[REDACTED]@db.internal:5432/orbitcast?sslmode=require"
    );
    assert_eq!(redact_url("postgres://db.internal/orbitcast"), "postgres://db.internal/orbitcast");
    assert_eq!(
        redact_url("postgres://cable@db.internal/orbitcast"),
        "postgres://cable@db.internal/orbitcast"
    );

    let mut config = base_config();
    config.listener_database_url = Some("postgres://listener:topsecret@db/orbitcast".to_string());
    config.streams.secret = Some(SecretBytes::new(b"raw-key".to_vec()));
    let rendered = format!("{config:?}");
    assert!(!rendered.contains("s3cret"), "{rendered}");
    assert!(!rendered.contains("topsecret"), "{rendered}");
    assert!(!rendered.contains("raw-key"), "{rendered}");
    assert!(rendered.contains("[REDACTED]"), "{rendered}");

    let file: FileConfig =
        toml::from_str("streams_secret = \"plain\"\ndatabase_url = \"postgres://u:pw@h/db\"")
            .unwrap();
    let rendered = format!("{file:?}");
    assert!(!rendered.contains("plain") && !rendered.contains("pw@"), "{rendered}");
}

#[test]
fn a_zero_rate_limit_validates_as_the_disabled_spelling() {
    let mut config = base_config();
    config.http.rate_connect_per_ip_per_min = 0;
    config.http.rate_commands_per_conn_per_sec = 0;
    assert!(config.validate().is_ok(), "0 is the documented off switch, not an error");
}

#[test]
fn out_of_range_admission_limits_are_boundary_errors() {
    let mut config = base_config();
    config.http.rate_connect_per_ip_per_min = MAX_RATE_LIMIT + 1;
    assert_matches!(
        config.validate(),
        Err(ConfigError::InvalidValue("rate_connect_per_ip_per_min", _))
    );

    let mut config = base_config();
    config.http.rate_commands_per_conn_per_sec = MAX_RATE_LIMIT + 1;
    assert_matches!(
        config.validate(),
        Err(ConfigError::InvalidValue("rate_commands_per_conn_per_sec", _))
    );

    // Zero is *not* an off switch here: it would refuse every handshake.
    let mut config = base_config();
    config.http.handshake_timeout = Duration::ZERO;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("handshake_timeout_ms", _)));

    let mut config = base_config();
    config.http.handshake_timeout = MAX_HANDSHAKE_TIMEOUT + Duration::from_millis(1);
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("handshake_timeout_ms", _)));
}

#[test]
fn moored_config_enables_turbo_streams() {
    let mut config = base_config();
    config
        .apply_moored_config(&moored(&[
            ("turbo_streams", "true"),
            ("turbo_streams_secret", "s3cret"),
        ]))
        .unwrap();
    assert!(config.streams.turbo);
    // Plain-text config passes through as its raw UTF-8 bytes.
    assert_eq!(
        config.streams.turbo_secret.as_ref().map(SecretBytes::as_bytes),
        Some(&b"s3cret"[..])
    );
    assert!(config.streams.is_enabled());
}

#[test]
fn moored_config_stores_pubsub_secret_as_raw_bytes() {
    let mut config = base_config();
    config.apply_moored_config(&moored(&[("streams_secret", "plain-text-key")])).unwrap();
    assert_eq!(
        config.streams.secret.as_ref().map(SecretBytes::as_bytes),
        Some(&b"plain-text-key"[..])
    );
}

#[test]
fn env_base64_secret_wins_over_moored_plain_text() {
    // Env-decoded key bytes must not be overwritten by a plain-text
    // Moored value (precedence: base64 env > plain-text config).
    let mut config = base_config();
    config.streams.secret = Some(SecretBytes::new(b"env-key-bytes".to_vec()));
    config.streams.turbo_secret = Some(SecretBytes::new(b"env-turbo".to_vec()));
    config.explicit.mark("streams_secret");
    config.explicit.mark("turbo_streams_secret");
    config
        .apply_moored_config(&moored(&[
            ("streams_secret", "should-be-ignored"),
            ("turbo_streams_secret", "also-ignored"),
        ]))
        .unwrap();
    assert_eq!(
        config.streams.secret.as_ref().map(SecretBytes::as_bytes),
        Some(&b"env-key-bytes"[..])
    );
    assert_eq!(
        config.streams.turbo_secret.as_ref().map(SecretBytes::as_bytes),
        Some(&b"env-turbo"[..])
    );
}

#[test]
fn env_secret_rejects_invalid_base64() {
    // "!!!" is not valid base64.
    let result = with_env(&[("SECRET", "!!!not-base64!!!")], |env| env.secret("SECRET"));
    assert_matches!(result, Err(ConfigError::InvalidEnv("SECRET", _)));
}

#[test]
fn env_secret_decodes_strict_base64_to_key_bytes() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let encoded = STANDARD.encode(b"raw-key-bytes");
    let result = with_env(&[("SECRET", encoded.as_str())], |env| env.secret("SECRET"));
    assert_eq!(result.unwrap().as_ref().map(SecretBytes::as_bytes), Some(&b"raw-key-bytes"[..]));
}

#[test]
fn env_secret_treats_empty_and_absent_as_unset() {
    assert_matches!(with_env(&[("SECRET", "")], |env| env.secret("SECRET")), Ok(None));
    assert_matches!(with_env(&[], |env| env.secret("SECRET")), Ok(None));
}

#[test]
fn moored_config_sets_pubsub_namespace_and_bounds() {
    let mut config = base_config();
    config
        .apply_moored_config(&moored(&[
            ("namespace", "handshake_ns"),
            ("pubsub_ack_timeout_ms", "1500"),
            ("pubsub_queue_capacity", "64"),
            ("listener_database_url", "postgres://listener@primary/orbitcast"),
        ]))
        .unwrap();
    assert_eq!(config.namespace, "handshake_ns");
    assert_eq!(config.pubsub_ack_timeout, Duration::from_millis(1500));
    assert_eq!(config.pubsub_queue_capacity, 64);
    assert_eq!(
        config.listener_database_url.as_deref(),
        Some("postgres://listener@primary/orbitcast")
    );
}

#[test]
fn moored_config_rejects_empty_zero_and_unparsable_values() {
    // The handshake is as strict as the environment: nothing is kept "by
    // accident" and nothing is silently dropped.
    let invalid: &[(&str, &str)] = &[
        ("namespace", ""),
        ("pubsub_ack_timeout_ms", "0"),
        ("pubsub_queue_capacity", "0"),
        ("pubsub_ack_timeout_ms", "soon"),
        ("pubsub_queue_capacity", "lots"),
        ("overflow_enabled", "maybe"),
        ("overflow_ttl_ms", "0"),
        ("max_publication_size", "lots"),
        ("max_message_size", "0"),
        ("max_connections", "0"),
        ("shutdown_timeout_ms", "0"),
        ("ping_interval", "0"),
        ("rate_connect_per_ip_per_min", "-1"),
        ("trusted_proxies", "10.0.0.0/99"),
        ("rpc_forward_headers", ", ,"),
    ];
    for (key, value) in invalid {
        let mut config = base_config();
        let error = config
            .apply_moored_config(&moored(&[(key, value)]))
            .expect_err(&format!("{key}={value:?} must be refused"));
        assert_matches!(
            error,
            ConfigError::InvalidHandshakeValue { ref key, .. } if key == key,
            "{key}={value:?}: {error}"
        );
    }
}

#[test]
fn moored_config_refuses_unknown_and_reserved_keys() {
    for key in [
        "listen_addr",
        "ingress_listen_addr",
        "ingress_allow_nonlocal",
        "rpc_token_base64",
        "typo_key",
    ] {
        let mut config = base_config();
        assert_matches!(
            config.apply_moored_config(&moored(&[(key, "value")])),
            Err(ConfigError::UnknownHandshakeKey(ref unknown)) if unknown == key,
            "{key}"
        );
    }
}

#[test]
fn moored_config_fills_only_keys_the_environment_and_file_left_unset() {
    // Precedence: environment > file > handshake. The handshake value for a
    // key that is explicit (set by either) is ignored, even when it parses.
    let mut config = base_config();
    config.namespace = "from_env".to_string();
    config.explicit.mark("namespace");
    config.http.max_connections = 42;
    config.explicit.mark("max_connections");
    config
        .apply_moored_config(&moored(&[
            ("namespace", "from_handshake"),
            ("max_connections", "7"),
            ("max_message_size", "2048"),
        ]))
        .unwrap();
    assert_eq!(config.namespace, "from_env", "an explicit key keeps its value");
    assert_eq!(config.http.max_connections, 42);
    assert_eq!(config.http.max_message_size, 2048, "an unset key is filled");

    // An explicit key is skipped even when the handshake value is unusable:
    // the value that counts was already validated by its own source.
    let mut config = base_config();
    config.explicit.mark("max_connections");
    config.apply_moored_config(&moored(&[("max_connections", "lots")])).unwrap();

    // Loaded from the environment, the provenance is recorded automatically.
    let mut config = with_env(
        &[("ORBITCAST_NAMESPACE", "env_ns"), ("ORBITCAST_LISTEN_ADDR", "127.0.0.1:0")],
        |env| {
            Config::from_env_over(
                env,
                &FileConfig { max_connections: Some(5), ..FileConfig::default() },
            )
        },
    )
    .unwrap();
    config
        .apply_moored_config(&moored(&[
            ("namespace", "hs"),
            ("max_connections", "9"),
            ("ping_interval", "7"),
        ]))
        .unwrap();
    assert_eq!(config.namespace, "env_ns");
    assert_eq!(config.http.max_connections, 5, "the file value beats the handshake");
    assert_eq!(config.ping_interval, 7, "the handshake fills what nothing else set");
}

// --- overflow relay ----------------------------------------------------

#[test]
fn overflow_relay_is_off_by_default() {
    let config = base_config();
    assert!(!config.overflow.enabled, "the relay requires a migration, so it is opt-in");
    assert_eq!(config.overflow.ttl, crate::pubsub::DEFAULT_OVERFLOW_TTL);
    assert_eq!(config.overflow.max_publication_size, crate::pubsub::DEFAULT_MAX_PUBLICATION_SIZE);
    config.validate().unwrap();
}

#[test]
fn moored_config_sets_overflow_relay_settings() {
    let mut config = base_config();
    config
        .apply_moored_config(&moored(&[
            ("overflow_enabled", "true"),
            ("overflow_ttl_ms", "60000"),
            ("max_publication_size", "262144"),
        ]))
        .unwrap();

    assert!(config.overflow.enabled);
    assert_eq!(config.overflow.ttl, Duration::from_mins(1));
    assert_eq!(config.overflow.max_publication_size, 262_144);
    config.validate().unwrap();
}

#[test]
fn validate_rejects_unusable_overflow_bounds() {
    let mut config = base_config();
    config.overflow.ttl = Duration::ZERO;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("overflow_ttl_ms", _)));

    let mut config = base_config();
    // Below the notification limit the relay would reject payloads that
    // need no relay at all.
    config.overflow.max_publication_size =
        crate::protocol::v1::publication::MAX_NOTIFY_PAYLOAD_LEN - 1;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("max_publication_size", _)));

    let mut config = base_config();
    config.overflow.max_publication_size = crate::pubsub::MAX_PUBLICATION_SIZE_LIMIT + 1;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("max_publication_size", _)));
}

// --- transport selection and listener limits ---------------------------

#[test]
fn validate_requires_at_least_one_transport() {
    let mut config = base_config();
    config.docking = None;
    assert_matches!(config.validate(), Err(ConfigError::NoTransport));

    // The direct listener alone is a complete transport selection.
    config.http.listen_addr = Some("127.0.0.1:8080".to_string());
    config.validate().unwrap();

    // So is docking alone.
    config.http.listen_addr = None;
    config.docking = base_config().docking;
    config.validate().unwrap();
}

#[test]
fn validate_rejects_unusable_listener_limits() {
    let mut config = base_config();
    config.http.listen_addr = Some("localhost:8080".to_string());
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("listen_addr", _)));

    config.http.listen_addr = Some("0.0.0.0:8080".to_string());
    config.http.max_message_size = 0;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("max_message_size", _)));

    config.http.max_message_size = MAX_MESSAGE_SIZE_LIMIT + 1;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("max_message_size", _)));

    config.http.max_message_size = DEFAULT_MAX_MESSAGE_SIZE;
    config.http.max_connections = 0;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("max_connections", _)));

    config.http.max_connections = DEFAULT_MAX_CONNECTIONS;
    config.shutdown_timeout = Duration::ZERO;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("shutdown_timeout_ms", _)));
}

#[test]
fn validate_rejects_malformed_origin_entries() {
    let mut config = base_config();
    config.http.allowed_origins = vec!["example.com".to_string()];
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("allowed_origins", _)));

    config.http.allowed_origins = vec!["https://example.com".to_string()];
    config.validate().unwrap();
}

#[test]
fn origin_policy_allows_missing_origin_and_listed_origins_only() {
    let mut http = HttpConfig::default();

    // Empty allow-list: only clients that send no Origin at all.
    assert!(http.allows_origin(None));
    assert!(!http.allows_origin(Some("https://example.com")));

    http.allowed_origins = parse_origins("https://example.com/, HTTPS://Other.Example:8443");
    assert!(http.allows_origin(Some("https://example.com")));
    // Normalization covers case and a trailing slash, not a different port.
    assert!(http.allows_origin(Some("HTTPS://EXAMPLE.COM/")));
    assert!(http.allows_origin(Some("https://other.example:8443")));
    assert!(!http.allows_origin(Some("https://example.com:8443")));
    assert!(!http.allows_origin(Some("http://example.com")));
    assert!(!http.allows_origin(Some("null")));

    // Explicit wildcard is an opt-in escape hatch.
    http.allowed_origins = vec!["*".to_string()];
    assert!(http.allows_origin(Some("https://anything.example")));
}

#[test]
fn parse_origins_normalizes_and_drops_empty_entries() {
    assert_eq!(
        parse_origins("[\"https://A.example/\", \"\", https://b.example]"),
        vec!["https://a.example".to_string(), "https://b.example".to_string()]
    );
    assert_matches!(parse_origins(" , ").as_slice(), []);
}

#[test]
fn moored_config_sets_listener_limits() {
    let mut config = base_config();
    config
        .apply_moored_config(&moored(&[
            ("allowed_origins", "https://app.example"),
            ("max_message_size", "2048"),
            ("max_connections", "42"),
            ("shutdown_timeout_ms", "5000"),
            ("outbound_queue_messages", "16"),
            ("outbound_queue_bytes", "4096"),
            ("rate_subscribes_per_conn_per_sec", "0"),
            ("max_header_bytes", "16384"),
            ("trusted_proxies", "10.0.0.0/8"),
        ]))
        .unwrap();

    assert_eq!(config.http.listen_addr, None, "bind address is a startup-time setting");
    assert_eq!(config.http.allowed_origins, vec!["https://app.example".to_string()]);
    assert_eq!(config.http.max_message_size, 2048);
    assert_eq!(config.http.max_connections, 42);
    assert_eq!(config.shutdown_timeout, Duration::from_secs(5));
    assert_eq!(config.http.outbound_queue_messages, 16);
    assert_eq!(config.http.outbound_queue_bytes, 4096);
    assert_eq!(config.http.rate_subscribes_per_conn_per_sec, 0, "zero is the off switch");
    assert_eq!(config.http.max_header_bytes, 16_384);
    assert_eq!(config.http.trusted_proxies, vec![Cidr::parse("10.0.0.0/8").unwrap()]);
}

#[test]
fn env_flag_rejects_typos_instead_of_disabling_silently() {
    let flag = |value| with_env(&[("SWITCH", value)], |env| env.flag("SWITCH"));

    assert_matches!(flag("ture"), Err(ConfigError::InvalidEnv("SWITCH", _)));
    assert_matches!(flag("YES"), Ok(Some(true)));
    assert_matches!(flag(" off "), Ok(Some(false)));
    assert_matches!(flag(""), Ok(None), "empty means unset");
    assert_matches!(with_env(&[], |env| env.flag("SWITCH")), Ok(None));
}

#[test]
fn env_parse_rejects_malformed_numbers_instead_of_using_defaults() {
    let parse = |value| with_env(&[("LIMIT", value)], |env| env.parse::<u64>("LIMIT"));

    assert_matches!(parse(" 42 "), Ok(Some(42)));
    assert_matches!(parse("-1"), Err(ConfigError::InvalidEnv("LIMIT", _)));
    assert_matches!(parse("4k"), Err(ConfigError::InvalidEnv("LIMIT", _)));
    assert_matches!(parse("  "), Ok(None), "blank means unset");
}

#[test]
fn docking_socket_path_is_derived_from_ship_name() {
    let docking = base_config().docking.unwrap();
    assert_eq!(docking.socket_path(), PathBuf::from("/tmp/orbitcast.sock"));
}

// --- no cross-profile secret fallback

#[test]
fn validate_rejects_turbo_enabled_without_its_own_secret() {
    let mut config = base_config();
    config.streams.turbo = true;
    // The claims secret must not stand in for the Turbo key.
    config.streams.secret = Some(SecretBytes::new(b"claims-key".to_vec()));
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("turbo_streams", _)));

    config.streams.turbo_secret = Some(SecretBytes::new(b"turbo-key".to_vec()));
    config.validate().unwrap();
}

#[test]
fn validate_rejects_cable_ready_enabled_without_its_own_secret() {
    let mut config = base_config();
    config.streams.cable_ready = true;
    config.streams.secret = Some(SecretBytes::new(b"claims-key".to_vec()));
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("cable_ready_streams", _)));

    config.streams.cable_ready_secret = Some(SecretBytes::new(b"cr-key".to_vec()));
    config.validate().unwrap();
}

mod broker;
mod dispatch;
mod file;
mod ingress;
mod presence;
mod streams;
