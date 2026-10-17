use std::assert_matches;

use super::*;

const NAMESPACE: &str = "example_production";

/// Decode and unwrap an inline notice, or fail the test.
fn decode_inline(namespace: &str, channel: &str, payload: &str) -> Publication {
    match decode_notice(namespace, channel, payload).unwrap() {
        DecodedNotice::Inline(publication) => publication,
        DecodedNotice::Overflow(reference) => {
            panic!("expected an inline notice, got overflow {reference:?}")
        }
    }
}

#[test]
fn channel_matches_ruby_golden_vectors() {
    // Cross-language golden vectors: the Ruby publisher derives these same
    // names. Changing any of them is a wire break.
    assert_eq!(
        channel_for("example_production", "chat:42"),
        "orbitcast_75ba535bd0c12e8f0990bf756607b4f5ddb148f2e22070e1"
    );
    assert_eq!(
        channel_for("demo", "posts:1"),
        "orbitcast_c5748d585378540c557a6893ad3d8866d449a7673b2590b9"
    );
    assert_eq!(
        channel_for("café", "stream:ünïcode"),
        "orbitcast_b630c21a8012cd05b047cec754aad9601a46823df496aa85"
    );
}

#[test]
fn control_channel_matches_the_ruby_derivation() {
    // `channel_for(namespace, CONTROL_STREAM)`, exactly as
    // `Orbitcast::Publisher::Control.channel_for` derives it. Pinned as
    // golden hex: changing either side is a wire break.
    assert_eq!(
        control_channel_for("example_production"),
        "orbitcast_d76a6eca57219572da6545dba39e8ad3e95b1cf4a2d07458"
    );
    assert_eq!(
        control_channel_for("demo"),
        "orbitcast_3f89827f4ec2ef321dd76721774889fa9d026f3e4a91da8d"
    );
    assert_eq!(
        control_channel_for("example_production"),
        channel_for("example_production", CONTROL_STREAM)
    );
}

#[test]
fn channel_fits_postgres_identifier_limit() {
    let channel = channel_for(NAMESPACE, "chat:42");
    assert_eq!(channel.len(), 58);
    assert!(channel.is_ascii());
    assert!(channel.starts_with(CHANNEL_PREFIX));
    assert!(channel[CHANNEL_PREFIX.len()..].chars().all(|c| c.is_ascii_hexdigit()));
    assert!(channel[CHANNEL_PREFIX.len()..].chars().all(|c| !c.is_ascii_uppercase()));
}

#[test]
fn channel_length_prefix_prevents_boundary_collisions() {
    // Without length prefixing, ("ab", "c") and ("a", "bc") would collide.
    assert_ne!(channel_for("ab", "c"), channel_for("a", "bc"));
}

#[test]
fn notice_roundtrip_preserves_payload_and_channel() {
    let payload = r#"{"b":1.0000000000000002,"a":"é"}"#;
    let (channel, encoded) = encode_notice(NAMESPACE, "chat:42", payload).unwrap();
    assert_eq!(channel, channel_for(NAMESPACE, "chat:42"));

    let decoded = decode_inline(NAMESPACE, &channel, &encoded);
    assert_eq!(decoded.version, 1);
    assert_eq!(decoded.namespace, NAMESPACE);
    assert_eq!(decoded.stream, "chat:42");
    assert_eq!(decoded.json_payload, payload);
    assert_ne!(decoded.id, "");
}

#[test]
fn notice_encodes_as_the_documented_v1_shape() {
    let (_, encoded) = encode_notice(NAMESPACE, "chat:42", r#"{"body":"hi"}"#).unwrap();
    let json: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["kind"], "inline");
    assert_eq!(json["publication"]["version"], 1);
    assert_eq!(json["publication"]["namespace"], NAMESPACE);
    assert_eq!(json["publication"]["stream"], "chat:42");
    assert_eq!(json["publication"]["json_payload"], r#"{"body":"hi"}"#);
}

#[test]
fn each_encode_gets_a_unique_publication_id() {
    let (_, first) = encode_notice(NAMESPACE, "chat:42", "{}").unwrap();
    let (_, second) = encode_notice(NAMESPACE, "chat:42", "{}").unwrap();
    assert_ne!(first, second, "identical broadcasts must not fold together");
}

#[test]
fn encode_rejects_oversized_payload_without_truncating() {
    let payload = format!(r#"{{"body":"{}"}}"#, "x".repeat(MAX_NOTIFY_PAYLOAD_LEN));
    let err = encode_notice(NAMESPACE, "chat:42", &payload).unwrap_err();
    match err {
        PubSubError::PayloadTooLarge { len, max } => {
            assert!(len >= max);
            assert_eq!(max, MAX_NOTIFY_PAYLOAD_LEN);
        }
        other => panic!("expected PayloadTooLarge, got {other:?}"),
    }
}

#[test]
fn encode_rejects_empty_namespace_and_non_json_payload() {
    assert_matches!(
        encode_notice("", "chat:42", "{}").unwrap_err(),
        PubSubError::Protocol(ProtocolError::EmptyField { field: "namespace" })
    );
    assert_matches!(
        encode_notice(NAMESPACE, "chat:42", "{not json").unwrap_err(),
        PubSubError::Protocol(ProtocolError::InvalidJson { field: "json_payload", .. })
    );
}

#[test]
fn decode_rejects_malformed_notice() {
    let channel = channel_for(NAMESPACE, "chat:42");
    for payload in ["", "not json", "{", "[1,2,3]", r#"{"version":1}"#] {
        let err = decode_notice(NAMESPACE, &channel, payload).unwrap_err();
        assert_matches!(
            err,
            PubSubError::Protocol(ProtocolError::Malformed(_)),
            "payload {payload:?}"
        );
    }
}

#[test]
fn decode_rejects_unknown_kind() {
    let channel = channel_for(NAMESPACE, "chat:42");
    let payload = r#"{"version":1,"kind":"telepathy","publication":{}}"#;
    let err = decode_notice(NAMESPACE, &channel, payload).unwrap_err();
    assert_matches!(err, PubSubError::Protocol(ProtocolError::Malformed(_)));
    assert!(err.to_string().contains("unknown variant"), "{err}");
}

#[test]
fn decode_rejects_unsupported_version() {
    let (channel, encoded) = encode_notice(NAMESPACE, "chat:42", "{}").unwrap();
    let bumped = encoded.replacen(r#"{"version":1"#, r#"{"version":99"#, 1);
    let err = decode_notice(NAMESPACE, &channel, &bumped).unwrap_err();
    assert_matches!(err, PubSubError::Protocol(ProtocolError::UnsupportedVersion(99)));
}

#[test]
fn decode_rejects_oversized_notification() {
    let channel = channel_for(NAMESPACE, "chat:42");
    let payload = "x".repeat(MAX_NOTIFY_PAYLOAD_LEN);
    assert_matches!(
        decode_notice(NAMESPACE, &channel, &payload).unwrap_err(),
        PubSubError::PayloadTooLarge { .. }
    );
}

#[test]
fn decode_rejects_foreign_namespace() {
    let (channel, encoded) = encode_notice("other_app", "chat:42", "{}").unwrap();
    let err = decode_notice(NAMESPACE, &channel, &encoded).unwrap_err();
    match err {
        PubSubError::NamespaceMismatch { expected, actual } => {
            assert_eq!(expected, NAMESPACE);
            assert_eq!(actual, "other_app");
        }
        other => panic!("expected NamespaceMismatch, got {other:?}"),
    }
}

#[test]
fn decode_rejects_channel_content_mismatch() {
    let (_, encoded) = encode_notice(NAMESPACE, "chat:42", "{}").unwrap();
    let wrong_channel = channel_for(NAMESPACE, "chat:43");
    let err = decode_notice(NAMESPACE, &wrong_channel, &encoded).unwrap_err();
    assert_matches!(err, PubSubError::ChannelMismatch { .. });
}

#[test]
fn decode_returns_an_overflow_reference_not_a_publication() {
    let id = uuid::Uuid::new_v4().to_string();
    let channel = channel_for(NAMESPACE, "chat:42");
    let payload = format!(
        concat!(
            r#"{{"version":1,"kind":"overflow","id":"{id}","#,
            r#""namespace":"{NAMESPACE}","stream":"chat:42"}}"#
        ),
        id = id,
        NAMESPACE = NAMESPACE
    );
    match decode_notice(NAMESPACE, &channel, &payload).unwrap() {
        DecodedNotice::Overflow(reference) => {
            assert_eq!(reference.id, id);
            assert_eq!(reference.namespace, NAMESPACE);
            assert_eq!(reference.stream, "chat:42");
        }
        other @ DecodedNotice::Inline(_) => {
            panic!("expected an overflow reference, got {other:?}")
        }
    }
}

#[test]
fn decode_rejects_an_overflow_reference_from_a_foreign_namespace() {
    // Namespace and channel agreement are checked before the kind: an
    // overflow reference is not a way around routing validation.
    let id = uuid::Uuid::new_v4().to_string();
    let channel = channel_for("other_app", "chat:42");
    let payload = format!(
        concat!(
            r#"{{"version":1,"kind":"overflow","id":"{id}","#,
            r#""namespace":"other_app","stream":"chat:42"}}"#
        ),
        id = id
    );
    assert_matches!(
        decode_notice(NAMESPACE, &channel, &payload).unwrap_err(),
        PubSubError::NamespaceMismatch { .. }
    );
}

// --- overflow preparation ----------------------------------------------

/// A payload that cannot fit inline whatever the envelope overhead is.
fn oversized_payload() -> String {
    format!(r#"{{"html":"{}"}}"#, "x".repeat(MAX_NOTIFY_PAYLOAD_LEN))
}

fn overflow_enabled() -> OverflowConfig {
    OverflowConfig { enabled: true, ..OverflowConfig::default() }
}

#[test]
fn prepare_keeps_small_publications_inline_even_with_overflow_enabled() {
    let prepared =
        prepare_notice(NAMESPACE, "chat:42", r#"{"body":"hi"}"#, overflow_enabled()).unwrap();
    assert!(prepared.relay_row.is_none(), "small publications never touch the relay table");
    let json: serde_json::Value = serde_json::from_str(&prepared.encoded).unwrap();
    assert_eq!(json["kind"], "inline");
}

#[test]
fn prepare_emits_the_documented_overflow_reference_shape() {
    let payload = oversized_payload();
    let prepared = prepare_notice(NAMESPACE, "chat:42", &payload, overflow_enabled()).unwrap();

    let row = prepared.relay_row.as_ref().expect("oversized publication needs a relay row");
    assert_eq!(row.json_payload, payload, "the stored row carries the exact payload text");

    // Matches tests/fixtures/publications/valid_overflow_ref.json: the
    // reference is exactly version/kind/id/namespace/stream.
    let json: serde_json::Value = serde_json::from_str(&prepared.encoded).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["kind"], "overflow");
    assert_eq!(json["id"], row.id);
    assert_eq!(json["namespace"], NAMESPACE);
    assert_eq!(json["stream"], "chat:42");
    let mut keys: Vec<&str> = json.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["id", "kind", "namespace", "stream", "version"]);
    assert_eq!(prepared.channel, channel_for(NAMESPACE, "chat:42"));
    assert!(PublicationNotice::fits_notify_payload(&prepared.encoded));
}

#[test]
fn prepare_reference_decodes_back_into_the_same_reference() {
    let prepared =
        prepare_notice(NAMESPACE, "chat:42", &oversized_payload(), overflow_enabled()).unwrap();
    let row = prepared.relay_row.unwrap();

    match decode_notice(NAMESPACE, &prepared.channel, &prepared.encoded).unwrap() {
        DecodedNotice::Overflow(reference) => {
            assert_eq!(reference, OverflowRef::for_publication(&row));
        }
        other @ DecodedNotice::Inline(_) => {
            panic!("expected an overflow reference, got {other:?}")
        }
    }
}

#[test]
fn prepare_without_overflow_still_refuses_oversized_publications() {
    let err = prepare_notice(NAMESPACE, "chat:42", &oversized_payload(), OverflowConfig::default())
        .unwrap_err();
    assert_matches!(err, PubSubError::PayloadTooLarge { .. });
    assert!(err.to_string().contains("overflow"), "{err}");
}

#[test]
fn prepare_rejects_publications_above_the_publication_ceiling() {
    let overflow =
        OverflowConfig { enabled: true, max_publication_size: 9000, ..Default::default() };
    let payload = format!(r#"{{"html":"{}"}}"#, "x".repeat(9000));

    match prepare_notice(NAMESPACE, "chat:42", &payload, overflow).unwrap_err() {
        PubSubError::PublicationTooLarge { len, max } => {
            assert_eq!(len, payload.len());
            assert_eq!(max, 9000);
        }
        other => panic!("expected PublicationTooLarge, got {other:?}"),
    }
}

#[test]
fn overflow_config_defaults_are_off_with_the_documented_bounds() {
    let overflow = OverflowConfig::default();
    assert!(!overflow.enabled, "the relay needs a migration, so it is opt-in");
    assert_eq!(overflow.ttl, DEFAULT_OVERFLOW_TTL);
    assert_eq!(overflow.max_publication_size, DEFAULT_MAX_PUBLICATION_SIZE);
    overflow.validate().unwrap();
}

#[test]
fn overflow_config_rejects_unusable_bounds() {
    let zero_ttl = OverflowConfig { ttl: Duration::ZERO, ..Default::default() };
    assert!(zero_ttl.validate().is_err());

    // Below the notification limit the relay would reject publications
    // that need no relay at all.
    let too_small =
        OverflowConfig { max_publication_size: MAX_NOTIFY_PAYLOAD_LEN - 1, ..Default::default() };
    assert!(too_small.validate().is_err());

    let too_large = OverflowConfig {
        max_publication_size: MAX_PUBLICATION_SIZE_LIMIT + 1,
        ..Default::default()
    };
    assert!(too_large.validate().is_err());
}

#[test]
fn stats_snapshot_counts_every_category() {
    let stats = PubSubStats::default();
    stats.record_intake_drop();
    stats.record_delivery_drop();
    stats.record_delivery_drop();
    stats.record_rejected();
    stats.record_overflow();
    stats.record_overflow_gap();
    stats.record_reconnect();
    stats.record_intake_depth(3);
    stats.record_intake_depth(9);
    stats.record_intake_depth(4);

    assert_eq!(
        stats.snapshot(),
        StatsSnapshot {
            intake_dropped: 1,
            delivery_dropped: 2,
            rejected: 1,
            overflow: 1,
            overflow_gaps: 1,
            reconnects: 1,
            intake_depth_max: 9,
        }
    );
}
