use super::*;
use std::time::Duration;

fn render_default(metrics: &Metrics) -> String {
    render(Exposition {
        metrics,
        gauges: Gauges::default(),
        pubsub: None,
        presence: None,
        broker: None,
    })
}

fn value_of(text: &str, series: &str) -> Option<f64> {
    text.lines()
        .find(|line| line.starts_with(series) && line.as_bytes().get(series.len()) == Some(&b' '))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
}

#[test]
fn label_sets_are_closed_and_index_uniquely() {
    // The whole no-unbounded-labels guarantee rests on these arrays being
    // dense and collision-free.
    for (position, reason) in ConnectionRejection::ALL.iter().enumerate() {
        assert_eq!(reason.index(), position, "{}", reason.label());
    }
    for (position, outcome) in RpcOutcome::ALL.iter().enumerate() {
        assert_eq!(outcome.index(), position, "{}", outcome.label());
    }
    for (position, status) in IngressStatus::ALL.iter().enumerate() {
        assert_eq!(status.index(), position, "{}", status.label());
    }
    for (position, class) in CommandClass::ALL.iter().enumerate() {
        assert_eq!(class.index(), position, "{}", class.label());
    }
    for (position, command) in UnsupportedCommand::ALL.iter().enumerate() {
        assert_eq!(command.index(), position, "{}", command.label());
    }
    for (position, reason) in LagReason::ALL.iter().enumerate() {
        assert_eq!(reason.index(), position, "{}", reason.label());
    }
    for (position, profile) in SignatureProfile::ALL.iter().enumerate() {
        assert_eq!(profile.index(), position, "{}", profile.label());
    }
    for (position, key) in SignatureKey::ALL.iter().enumerate() {
        assert_eq!(key.index(), position, "{}", key.label());
    }
}

#[test]
fn every_always_present_series_exists_at_zero() {
    let text = render_default(&Metrics::new());

    for series in [
        "orbitcast_connections_current",
        "orbitcast_connections_total",
        "orbitcast_subscriptions_current",
        "orbitcast_streams_with_interest",
        "orbitcast_hub_queue_overflow_total",
        "orbitcast_session_reuse_stale_entries_total",
        "orbitcast_pubsub_published_total",
        "orbitcast_pubsub_received_total",
        "orbitcast_pubsub_delivered_total",
        "orbitcast_presence_leases_current",
        "orbitcast_rpc_latency_seconds_count",
    ] {
        assert_eq!(value_of(&text, series), Some(0.0), "{series} must exist at zero");
    }

    // Absent subsystems emit nothing at all, rather than a misleading zero.
    assert!(!text.contains("orbitcast_broker_retained_total"));
    assert!(!text.contains("orbitcast_pubsub_dropped_total"));
    assert!(!text.contains("orbitcast_presence_joins_total"));
    assert!(!text.contains("orbitcast_rails_ready"), "no Rails endpoint, no readiness series");

    // Labelled families exist at zero for every label value.
    for series in [
        "orbitcast_commands_rate_limited_total{class=\"subscribe\"} 0",
        "orbitcast_commands_rate_limited_total{class=\"action\"} 0",
        "orbitcast_unsupported_commands_total{command=\"message\"} 0",
        "orbitcast_socket_lag_disconnects_total{reason=\"queue\"} 0",
        "orbitcast_socket_lag_disconnects_total{reason=\"pending_buffer\"} 0",
        "orbitcast_signature_verifications_total{profile=\"orbitcast\",key=\"current\"} 0",
        "orbitcast_signature_verifications_total{profile=\"turbo\",key=\"previous\"} 0",
        "orbitcast_signature_verifications_total{profile=\"cable_ready\",key=\"failed\"} 0",
    ] {
        assert!(text.contains(series), "{series} must exist at zero:\n{text}");
    }
}

#[test]
fn signature_verifications_count_per_profile_and_key() {
    let metrics = Metrics::new();
    metrics.record_signature_verification(SignatureProfile::Orbitcast, SignatureKey::Current);
    metrics.record_signature_verification(SignatureProfile::Orbitcast, SignatureKey::Current);
    metrics.record_signature_verification(SignatureProfile::Turbo, SignatureKey::Previous);
    metrics.record_signature_verification(SignatureProfile::CableReady, SignatureKey::Failed);

    assert_eq!(
        metrics.signature_verifications(SignatureProfile::Orbitcast, SignatureKey::Current),
        2
    );
    assert_eq!(
        metrics.signature_verifications(SignatureProfile::Orbitcast, SignatureKey::Failed),
        0
    );

    let text = render_default(&metrics);
    assert!(text.contains(
        "orbitcast_signature_verifications_total{profile=\"orbitcast\",key=\"current\"} 2"
    ));
    assert!(
        text.contains(
            "orbitcast_signature_verifications_total{profile=\"turbo\",key=\"previous\"} 1"
        )
    );
    assert!(text.contains(
        "orbitcast_signature_verifications_total{profile=\"cable_ready\",key=\"failed\"} 1"
    ));
}

#[test]
fn counters_and_gauges_reach_the_exposition() {
    let metrics = Metrics::new();
    metrics.record_connection();
    metrics.record_connection();
    metrics.record_connection_rejected(ConnectionRejection::RateLimited);
    metrics.record_command_rate_limited(CommandClass::Action);
    metrics.record_unsupported_command(UnsupportedCommand::Message);
    metrics.record_socket_lag_disconnect(LagReason::Queue);
    metrics.record_hub_queue_overflow();
    metrics.record_session_reuse_stale_entries(2);
    metrics.record_pubsub_gap_disconnects(3);
    metrics.record_published();
    metrics.record_received();
    metrics.record_delivered(7);
    metrics.record_ingress(IngressStatus::Accepted);

    let text = render(Exposition {
        metrics: &metrics,
        gauges: Gauges {
            connections: 3,
            subscriptions: 5,
            streams_with_interest: 2,
            presence_leases: 1,
            rails_ready: Some(true),
        },
        pubsub: Some(StatsSnapshot {
            intake_dropped: 1,
            delivery_dropped: 2,
            rejected: 3,
            overflow: 4,
            overflow_gaps: 5,
            reconnects: 6,
            intake_depth_max: 7,
        }),
        presence: Some(PresenceMetrics { joins: 9, ..PresenceMetrics::default() }),
        broker: Some(BrokerExposition {
            namespace: "example_production",
            stats: BrokerStatsSnapshot {
                retained: 11,
                duplicates_identical: 2,
                duplicates_conflicting: 1,
                messages_current: 40,
                bytes_current: 4096,
                ..BrokerStatsSnapshot::default()
            },
        }),
    });

    assert_eq!(value_of(&text, "orbitcast_connections_total"), Some(2.0));
    assert_eq!(value_of(&text, "orbitcast_connections_current"), Some(3.0));
    assert_eq!(value_of(&text, "orbitcast_subscriptions_current"), Some(5.0));
    assert_eq!(value_of(&text, "orbitcast_streams_with_interest"), Some(2.0));
    assert_eq!(value_of(&text, "orbitcast_presence_leases_current"), Some(1.0));
    assert!(text.contains("orbitcast_commands_rate_limited_total{class=\"action\"} 1"));
    assert!(text.contains("orbitcast_commands_rate_limited_total{class=\"subscribe\"} 0"));
    assert_eq!(value_of(&text, "orbitcast_rails_ready"), Some(1.0));
    assert_eq!(value_of(&text, "orbitcast_pubsub_intake_queue_depth_max"), Some(7.0));
    assert_eq!(value_of(&text, "orbitcast_pubsub_delivered_total"), Some(7.0));
    assert_eq!(value_of(&text, "orbitcast_presence_joins_total"), Some(9.0));
    assert_eq!(value_of(&text, "orbitcast_broker_retained_total"), Some(11.0));
    assert!(text.contains("orbitcast_broker_duplicates_total{kind=\"identical\"} 2"));
    assert!(text.contains("orbitcast_broker_duplicates_total{kind=\"conflicting\"} 1"));
    assert!(
        text.contains("orbitcast_broker_messages_current{namespace=\"example_production\"} 40")
    );
    assert!(text.contains("orbitcast_broker_bytes_current{namespace=\"example_production\"} 4096"));
    assert!(text.contains("orbitcast_connections_rejected_total{reason=\"rate_limited\"} 1"));
    assert!(text.contains("orbitcast_pubsub_dropped_total{queue=\"intake\"} 1"));
    assert!(text.contains("orbitcast_ingress_requests_total{status=\"202\"} 1"));
    assert!(text.contains("orbitcast_unsupported_commands_total{command=\"message\"} 1"));
    assert!(text.contains("orbitcast_socket_lag_disconnects_total{reason=\"queue\"} 1"));
    assert_eq!(value_of(&text, "orbitcast_hub_queue_overflow_total"), Some(1.0));
    assert_eq!(value_of(&text, "orbitcast_session_reuse_stale_entries_total"), Some(2.0));
    assert_eq!(value_of(&text, "orbitcast_pubsub_gap_disconnects_total"), Some(3.0));
}

#[test]
fn the_latency_histogram_is_cumulative_and_sums_exactly() {
    let metrics = Metrics::new();
    metrics.record_rpc(RpcOutcome::Accepted, Duration::from_millis(3));
    metrics.record_rpc(RpcOutcome::Denied, Duration::from_millis(400));
    // Beyond the last bound: only `+Inf` counts it.
    metrics.record_rpc(RpcOutcome::Deadline, Duration::from_secs(30));

    let text = render_default(&metrics);

    // 3 ms lands in the 0.005 bucket and every bucket above it.
    assert!(text.contains("orbitcast_rpc_latency_seconds_bucket{le=\"0.0025\"} 0"));
    assert!(text.contains("orbitcast_rpc_latency_seconds_bucket{le=\"0.005\"} 1"));
    assert!(text.contains("orbitcast_rpc_latency_seconds_bucket{le=\"0.5\"} 2"));
    assert!(text.contains("orbitcast_rpc_latency_seconds_bucket{le=\"5\"} 2"));
    assert!(text.contains("orbitcast_rpc_latency_seconds_bucket{le=\"+Inf\"} 3"));
    assert_eq!(value_of(&text, "orbitcast_rpc_latency_seconds_count"), Some(3.0));
    assert!(text.contains("orbitcast_rpc_latency_seconds_sum 30.403000\n"), "{text}");
    assert!(text.contains("orbitcast_rpc_calls_total{outcome=\"accepted\"} 1"));

    // Past 2^53 microseconds an `f64` can no longer hold the total: the sum
    // is printed from the integer count, so the last digit still survives.
    let metrics = Metrics::new();
    metrics.record_rpc(RpcOutcome::Accepted, Duration::from_micros(9_007_199_254_740_993));
    let text = render_default(&metrics);
    assert!(text.contains("orbitcast_rpc_latency_seconds_sum 9007199254.740993\n"), "{text}");
}

#[test]
fn ingress_statuses_map_from_codes_without_inventing_labels() {
    assert_eq!(IngressStatus::from_code(202), Some(IngressStatus::Accepted));
    assert_eq!(
        IngressStatus::from_code(200),
        Some(IngressStatus::Ok),
        "a scrape is not an accepted publication"
    );
    assert_eq!(IngressStatus::from_code(429), Some(IngressStatus::TooManyRequests));
    assert_eq!(IngressStatus::from_code(418), None, "an unexpected status adds no series");
    assert_eq!(IngressStatus::from_code(201), None);
}
