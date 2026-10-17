//! Signed-stream keys: rings, the CableReady environment path, and the
//! public-stream acknowledgement.

use super::*;
use crate::streams::MAX_PREVIOUS_KEYS;

fn b64(bytes: &[u8]) -> String {
    use base64::{Engine, engine::general_purpose::STANDARD};
    STANDARD.encode(bytes)
}

fn keys(config: &PreviousKeys) -> Vec<&[u8]> {
    config.as_slice().iter().map(SecretBytes::as_bytes).collect()
}

#[test]
fn previous_keys_are_read_from_comma_separated_strict_base64() {
    let streams = format!("{},{}", b64(b"old-1"), b64(b"old-2"));
    let turbo = format!(" {} , {} ", b64(b"turbo-old"), b64(b"turbo-older"));
    let cable_ready = b64(b"cr-old");
    let config = with_env(
        &[
            ("ORBITCAST_STREAMS_SECRET_BASE64", &b64(b"current")),
            ("ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64", &streams),
            ("ORBITCAST_TURBO_STREAMS", "true"),
            ("ORBITCAST_TURBO_SECRET_BASE64", &b64(b"turbo")),
            ("ORBITCAST_TURBO_PREVIOUS_SECRETS_BASE64", &turbo),
            ("ORBITCAST_CABLE_READY_STREAMS", "1"),
            ("ORBITCAST_CABLE_READY_SECRET_BASE64", &b64(b"cr")),
            ("ORBITCAST_CABLE_READY_PREVIOUS_SECRETS_BASE64", &cable_ready),
        ],
        |env| Config::from_env_over(env, &FileConfig::default()),
    )
    .unwrap();

    assert_eq!(keys(&config.streams.previous_secrets), [&b"old-1"[..], b"old-2"]);
    assert_eq!(keys(&config.streams.turbo_previous_secrets), [&b"turbo-old"[..], b"turbo-older"]);
    assert!(config.streams.cable_ready);
    assert_eq!(
        config.streams.cable_ready_secret.as_ref().map(SecretBytes::as_bytes),
        Some(&b"cr"[..])
    );
    assert_eq!(keys(&config.streams.cable_ready_previous_secrets), [&b"cr-old"[..]]);

    for key in [
        "streams_previous_secrets",
        "turbo_previous_secrets",
        "cable_ready_streams",
        "cable_ready_secret",
        "cable_ready_previous_secrets",
    ] {
        assert!(config.explicit.contains(key), "{key} is marked explicit");
    }
}

#[test]
fn previous_keys_reject_malformed_entries_and_an_oversized_ring() {
    let malformed = format!("{},!!!", b64(b"ok"));
    assert_matches!(
        with_env(&[("ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64", &malformed)], |env| {
            Config::from_env_over(env, &FileConfig::default())
        }),
        Err(ConfigError::InvalidEnv("ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64", _))
    );

    let empty_entry = format!("{},", b64(b"ok"));
    assert_matches!(
        with_env(&[("ORBITCAST_TURBO_PREVIOUS_SECRETS_BASE64", &empty_entry)], |env| {
            Config::from_env_over(env, &FileConfig::default())
        }),
        Err(ConfigError::InvalidEnv("ORBITCAST_TURBO_PREVIOUS_SECRETS_BASE64", _))
    );

    let too_many = (0..=MAX_PREVIOUS_KEYS)
        .map(|i| b64(&[u8::try_from(i).unwrap()]))
        .collect::<Vec<_>>()
        .join(",");
    assert_matches!(
        with_env(&[("ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64", &too_many)], |env| {
            Config::from_env_over(env, &FileConfig::default())
        }),
        Err(ConfigError::InvalidEnv("ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64", msg))
            if msg.contains("too many")
    );

    // The file is bounded the same way, and refuses an empty entry.
    let file: FileConfig = toml::from_str(&format!(
        "streams_previous_secrets = [{}]",
        (0..=MAX_PREVIOUS_KEYS).map(|i| format!("\"k{i}\"")).collect::<Vec<_>>().join(", ")
    ))
    .unwrap();
    assert_matches!(
        with_env(&[], |env| Config::from_env_over(env, &file)),
        Err(ConfigError::Invalid("streams_previous_secrets", _))
    );
    let file: FileConfig = toml::from_str(r#"cable_ready_previous_secrets = ["ok", ""]"#).unwrap();
    assert_matches!(
        with_env(&[], |env| Config::from_env_over(env, &file)),
        Err(ConfigError::InvalidValue("cable_ready_previous_secrets", _))
    );
}

#[test]
fn env_previous_keys_win_over_file_and_handshake_previous_keys() {
    let file: FileConfig = toml::from_str(r#"streams_previous_secrets = ["file-old"]"#).unwrap();
    let env_keys = b64(b"env-old");
    let mut config = with_env(&[("ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64", &env_keys)], |env| {
        Config::from_env_over(env, &file)
    })
    .unwrap();
    assert_eq!(keys(&config.streams.previous_secrets), [&b"env-old"[..]]);

    config.apply_moored_config(&moored(&[("streams_previous_secrets", "moored-old")])).unwrap();
    assert_eq!(keys(&config.streams.previous_secrets), [&b"env-old"[..]], "explicit keys are kept");
}

#[test]
fn moored_config_fills_previous_keys_from_plain_text_lists() {
    let mut config = base_config();
    config
        .apply_moored_config(&moored(&[
            ("streams_previous_secrets", "a, b"),
            ("turbo_previous_secrets", "t1"),
            ("cable_ready_previous_secrets", "c1,c2,c3"),
        ]))
        .unwrap();
    assert_eq!(keys(&config.streams.previous_secrets), [&b"a"[..], b"b"]);
    assert_eq!(keys(&config.streams.turbo_previous_secrets), [&b"t1"[..]]);
    assert_eq!(keys(&config.streams.cable_ready_previous_secrets), [&b"c1"[..], b"c2", b"c3"]);

    for value in ["", " , ", &["k"; MAX_PREVIOUS_KEYS + 1].join(",")] {
        let mut config = base_config();
        assert_matches!(
            config.apply_moored_config(&moored(&[("turbo_previous_secrets", value)])),
            Err(ConfigError::InvalidHandshakeValue { .. }),
            "{value:?}"
        );
    }
}

#[test]
fn the_handshake_no_longer_carries_a_whisper_switch() {
    // Whisper is a per-token claim now; a handshake still sending the old
    // global switch is refused rather than silently ignored.
    let mut config = base_config();
    assert_matches!(
        config.apply_moored_config(&moored(&[("streams_whisper", "true")])),
        Err(ConfigError::UnknownHandshakeKey(key)) if key == "streams_whisper"
    );
    assert!(toml::from_str::<FileConfig>("streams_whisper = true").is_err());
}

#[test]
fn public_streams_require_the_insecure_acknowledgement() {
    let mut config = base_config();
    config.streams.public = true;
    assert_matches!(
        config.validate(),
        Err(ConfigError::InvalidValue("public_streams", msg))
            if msg.contains("public_streams_insecure")
    );

    config.streams.public_insecure = true;
    config.validate().unwrap();

    // The acknowledgement alone changes nothing.
    let mut config = base_config();
    config.streams.public_insecure = true;
    config.validate().unwrap();
    assert!(!config.streams.public);

    // Readable from all three surfaces.
    let config = with_env(
        &[("ORBITCAST_PUBLIC_STREAMS", "yes"), ("ORBITCAST_PUBLIC_STREAMS_INSECURE", "yes")],
        |env| Config::from_env_over(env, &FileConfig::default()),
    )
    .unwrap();
    assert!(config.streams.public && config.streams.public_insecure);
    assert!(config.explicit.contains("public_streams_insecure"));

    let file: FileConfig = toml::from_str("public_streams_insecure = true").unwrap();
    let config = with_env(&[], |env| Config::from_env_over(env, &file)).unwrap();
    assert!(config.streams.public_insecure);

    let mut config = base_config();
    config.apply_moored_config(&moored(&[("public_streams_insecure", "true")])).unwrap();
    assert!(config.streams.public_insecure);
}

#[test]
fn cable_ready_enabled_from_the_environment_still_needs_its_own_key() {
    let config = with_env(&[("ORBITCAST_CABLE_READY_STREAMS", "true")], |env| {
        let mut config = Config::from_env_over(env, &FileConfig::default()).unwrap();
        config.docking = base_config().docking;
        config.database_url = base_config().database_url;
        config.namespace = base_config().namespace;
        config
    });
    assert_matches!(config.validate(), Err(ConfigError::InvalidValue("cable_ready_streams", _)));
}
