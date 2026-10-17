//! private HTTP ingress

use super::*;

fn ingress_config(addr: &str) -> Config {
    let mut config = base_config();
    config.namespace = "example_production".to_string();
    config.ingress.listen_addr = Some(addr.to_string());
    config.ingress.token = Some(SecretBytes::new(b"ingress-token".to_vec()));
    config
}

#[test]
fn the_ingress_is_disabled_by_default() {
    let config = base_config();
    assert!(!config.ingress.is_enabled());
    assert!(config.ingress.token.is_none());
    assert!(!config.ingress.allow_nonlocal);
    config.validate().unwrap();
}

#[test]
fn a_loopback_ingress_with_a_token_validates() {
    ingress_config("127.0.0.1:28081").validate().unwrap();
    ingress_config("[::1]:28081").validate().unwrap();
}

#[test]
fn the_ingress_requires_a_token() {
    let mut config = ingress_config("127.0.0.1:28081");
    config.ingress.token = None;
    let err = config.validate().unwrap_err();
    assert_matches!(err, ConfigError::InvalidValue("ingress_listen_addr", _), "{err:?}");
    assert!(err.to_string().contains("ORBITCAST_INGRESS_TOKEN_BASE64"), "{err}");
}

#[test]
fn the_ingress_requires_a_namespace() {
    let mut config = ingress_config("127.0.0.1:28081");
    config.namespace = String::new();
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("namespace"), "{err}");
}

#[test]
fn a_nonlocal_ingress_bind_fails_closed_without_the_explicit_switch() {
    let mut config = ingress_config("0.0.0.0:28081");
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("ingress_allow_nonlocal"), "{err}");

    // The explicit switch is what widens the bind - nothing else.
    config.ingress.allow_nonlocal = true;
    config.validate().unwrap();
}

#[test]
fn the_ingress_rejects_hostnames_and_the_public_listener_address() {
    // Names are not resolved, so a typo can never widen the bind.
    let config = ingress_config("localhost:28081");
    assert_matches!(
        config.validate().unwrap_err(),
        ConfigError::InvalidValue("ingress_listen_addr", _)
    );

    // Never served on the public /cable listener.
    let mut config = ingress_config("127.0.0.1:8080");
    config.http.listen_addr = Some("127.0.0.1:8080".to_string());
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("listen_addr"), "{err}");
}

#[test]
fn the_moored_handshake_cannot_open_the_ingress() {
    // Transport selection is boot-time only and the credential is
    // environment-only: each key is refused outright, never quietly dropped.
    for key in ["ingress_listen_addr", "ingress_allow_nonlocal", "ingress_token"] {
        let mut config = base_config();
        assert_matches!(
            config.apply_moored_config(&moored(&[(key, "value")])),
            Err(ConfigError::UnknownHandshakeKey(_)),
            "{key}"
        );
        assert!(!config.ingress.is_enabled());
        assert!(config.ingress.token.is_none());
        assert!(!config.ingress.allow_nonlocal);
    }
}
