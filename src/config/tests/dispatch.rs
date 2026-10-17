//! Rails dispatch mode configuration.

use super::*;

#[test]
fn signed_only_is_the_default_mode() {
    let config = base_config();
    assert!(!config.rpc.is_enabled());
    assert_eq!(config.dispatch_mode(), DispatchMode::SignedOnly);
    assert!(config.rpc.to_rpc_config().is_none());
    config.validate().unwrap();
}

#[test]
fn an_endpoint_selects_dispatch_mode() {
    let config = dispatch_config();
    assert_eq!(config.dispatch_mode(), DispatchMode::RailsDispatch);
    config.validate().unwrap();

    let rpc = config.rpc.to_rpc_config().expect("dispatch mode yields a caller config");
    assert_eq!(rpc.endpoint_url, "http://127.0.0.1:3000/_orbitcast/v1/rails");
    assert_eq!(rpc.deadline, DEFAULT_DEADLINE);
    assert_eq!(rpc.max_concurrency, DEFAULT_MAX_CONCURRENCY);
}

#[test]
fn dispatch_without_a_credential_fails_at_the_boundary() {
    // Never fall back to signed-only: an unauthenticated endpoint is a
    // boot-time error, not a quieter mode.
    let mut config = dispatch_config();
    config.rpc.token = None;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_endpoint_url", _)));

    config.rpc.token = Some(SecretBytes::new(Vec::new()));
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_endpoint_url", _)));
}

#[test]
fn plain_http_is_only_allowed_to_a_loopback_endpoint() {
    let mut config = dispatch_config();

    for endpoint in
        ["http://127.0.0.1:3000/rails", "http://localhost/rails", "http://[::1]:3000/rails"]
    {
        config.rpc.endpoint_url = Some(endpoint.to_string());
        config.validate().unwrap_or_else(|err| panic!("{endpoint} must be allowed: {err}"));
    }

    config.rpc.endpoint_url = Some("http://app.internal/rails".to_string());
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_endpoint_url", _)));

    // TLS to a remote host is always fine.
    config.rpc.endpoint_url = Some("https://app.internal/rails".to_string());
    config.validate().unwrap();

    // A non-HTTP scheme is not an endpoint at all.
    config.rpc.endpoint_url = Some("unix:///tmp/rails.sock".to_string());
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_endpoint_url", _)));
}

#[test]
fn validate_rejects_unusable_rpc_bounds() {
    let mut config = dispatch_config();
    config.rpc.deadline = Duration::ZERO;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_deadline_ms", _)));

    let mut config = dispatch_config();
    config.rpc.deadline = Duration::from_millis(MAX_DEADLINE_MS + 1);
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_deadline_ms", _)));

    let mut config = dispatch_config();
    config.rpc.max_concurrency = 0;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_max_concurrency", _)));

    let mut config = dispatch_config();
    config.rpc.mailbox_capacity = 0;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_mailbox_capacity", _)));

    let mut config = dispatch_config();
    config.rpc.cleanup_capacity = 0;
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_cleanup_capacity", _)));

    let mut config = dispatch_config();
    config.rpc.half_open_timeout = Duration::ZERO;
    assert_matches!(
        config.validate(),
        Err(ConfigError::InvalidValue("rpc_half_open_timeout_ms", _))
    );
}

#[test]
fn forward_headers_must_be_lowercase_and_never_hop_by_hop() {
    let mut config = dispatch_config();
    config.rpc.forward_headers = vec!["Cookie".to_string()];
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_forward_headers", _)));

    config.rpc.forward_headers = vec!["transfer-encoding".to_string()];
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("rpc_forward_headers", _)));

    config.rpc.forward_headers = vec!["cookie".to_string(), "x-request-id".to_string()];
    config.validate().unwrap();
}

#[test]
fn only_allowlisted_headers_are_forwarded() {
    let settings = RpcSettings::default();
    let headers = HashMap::from([
        ("Cookie".to_string(), "_session=abc".to_string()),
        ("Authorization".to_string(), "Bearer user".to_string()),
        ("X-Forwarded-For".to_string(), "10.0.0.1".to_string()),
        ("host".to_string(), "app.example".to_string()),
        ("connection".to_string(), "upgrade".to_string()),
    ]);

    let forwarded = settings.allowlisted_headers(&headers);
    assert_eq!(
        forwarded,
        BTreeMap::from([
            ("authorization".to_string(), "Bearer user".to_string()),
            ("cookie".to_string(), "_session=abc".to_string()),
        ]),
        "the default allowlist is credentials only, names lowercased"
    );

    // Even an explicitly allowlisted hop-by-hop header is dropped.
    let settings = RpcSettings { forward_headers: vec!["host".to_string()], ..settings };
    assert!(settings.allowlisted_headers(&headers).is_empty());
}

#[test]
fn file_config_carries_rpc_tunables_but_never_the_token() {
    let file: FileConfig = toml::from_str(
        r#"
            node_id = "node-7"
            rpc_endpoint_url = "https://app.internal/_orbitcast/v1/rails"
            rpc_deadline_ms = 2500
            rpc_max_concurrency = 8
            rpc_mailbox_capacity = 4
            rpc_cleanup_capacity = 16
            rpc_failure_threshold = 3
            rpc_half_open_timeout_ms = 1500
            rpc_forward_headers = ["Cookie", "X-Api-Key"]
            "#,
    )
    .unwrap();

    let config = with_env(&[], |env| Config::from_env_over(env, &file)).unwrap();
    assert_eq!(config.rpc.node_id, "node-7");
    assert_eq!(
        config.rpc.endpoint_url.as_deref(),
        Some("https://app.internal/_orbitcast/v1/rails")
    );
    assert_eq!(config.rpc.deadline, Duration::from_millis(2500));
    assert_eq!(config.rpc.max_concurrency, 8);
    assert_eq!(config.rpc.mailbox_capacity, 4);
    assert_eq!(config.rpc.cleanup_capacity, 16);
    assert_eq!(config.rpc.failure_threshold, 3);
    assert_eq!(config.rpc.half_open_timeout, Duration::from_millis(1500));
    assert_eq!(
        config.rpc.forward_headers,
        vec!["cookie".to_string(), "x-api-key".to_string()],
        "file header names are normalized to lowercase"
    );

    // There is no file key for the token at all: a typo'd attempt to put
    // one there is a hard error, not a silently ignored secret.
    let err = toml::from_str::<FileConfig>(r#"rpc_token = "hunter2""#).unwrap_err();
    assert!(err.to_string().contains("rpc_token"), "{err}");
}

#[test]
fn moored_config_sets_rpc_tunables_but_not_the_token() {
    let mut config = base_config();
    config
        .apply_moored_config(&moored(&[
            ("rpc_endpoint_url", "https://app.internal/rails"),
            ("node_id", "node-3"),
            ("rpc_deadline_ms", "750"),
            ("rpc_max_concurrency", "12"),
            ("rpc_mailbox_capacity", "8"),
            ("rpc_cleanup_capacity", "64"),
            ("rpc_failure_threshold", "4"),
            ("rpc_half_open_timeout_ms", "2000"),
            ("rpc_forward_headers", "cookie, x-api-key"),
        ]))
        .unwrap();

    assert_eq!(config.rpc.endpoint_url.as_deref(), Some("https://app.internal/rails"));
    assert_eq!(config.rpc.node_id, "node-3");
    assert_eq!(config.rpc.deadline, Duration::from_millis(750));
    assert_eq!(config.rpc.max_concurrency, 12);
    assert_eq!(config.rpc.mailbox_capacity, 8);
    assert_eq!(config.rpc.cleanup_capacity, 64);
    assert_eq!(config.rpc.failure_threshold, 4);
    assert_eq!(config.rpc.half_open_timeout, Duration::from_secs(2));
    assert_eq!(config.rpc.forward_headers, vec!["cookie".to_string(), "x-api-key".to_string()]);

    assert!(config.rpc.token.is_none(), "a Moored handshake must not carry the token");
    assert_matches!(
        config.validate(),
        Err(ConfigError::InvalidValue("rpc_endpoint_url", _)),
        "an endpoint without an environment credential must fail closed"
    );

    // The token itself is not a handshake key at all.
    let mut config = base_config();
    assert_matches!(
        config.apply_moored_config(&moored(&[("rpc_token", "hunter2")])),
        Err(ConfigError::UnknownHandshakeKey(_))
    );
}

#[test]
fn moored_config_refuses_zero_and_unparsable_rpc_values() {
    for (key, value) in [
        ("rpc_endpoint_url", ""),
        ("rpc_deadline_ms", "0"),
        ("rpc_max_concurrency", "0"),
        ("rpc_mailbox_capacity", "lots"),
        ("rpc_forward_headers", " , "),
    ] {
        let mut config = dispatch_config();
        assert_matches!(
            config.apply_moored_config(&moored(&[(key, value)])),
            Err(ConfigError::InvalidHandshakeValue { .. }),
            "{key}={value:?}"
        );
        assert_eq!(
            config.rpc.endpoint_url.as_deref(),
            Some("http://127.0.0.1:3000/_orbitcast/v1/rails"),
            "a refused handshake changes nothing"
        );
        assert_eq!(config.rpc.deadline, DEFAULT_DEADLINE);
        assert_eq!(config.rpc.max_concurrency, DEFAULT_MAX_CONCURRENCY);
        assert_eq!(config.rpc.mailbox_capacity, DEFAULT_RPC_MAILBOX_CAPACITY);
        assert_eq!(
            config.rpc.forward_headers,
            DEFAULT_RPC_FORWARD_HEADERS.iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    }
}
