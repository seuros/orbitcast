//! Fixtures shared by the transport unit tests.

use std::sync::Arc;

use crate::config::{Config, HttpConfig, IngressSettings, RpcSettings};
use crate::server::Backend;

/// The deployment namespace every transport fixture runs under.
pub(super) const NAMESPACE: &str = "example_production";

/// A configuration with every transport and optional subsystem off; each test
/// switches on only what it exercises.
pub(super) fn config() -> Config {
    Config {
        docking: None,
        http: HttpConfig::default(),
        shutdown_timeout: crate::config::DEFAULT_SHUTDOWN_TIMEOUT,
        database_url: None,
        listener_database_url: None,
        namespace: NAMESPACE.to_string(),
        pubsub_ack_timeout: crate::pubsub::DEFAULT_ACK_TIMEOUT,
        pubsub_queue_capacity: crate::pubsub::DEFAULT_QUEUE_CAPACITY,
        overflow: crate::pubsub::OverflowConfig::default(),
        ping_interval: 3,
        presence_ttl: std::time::Duration::from_millis(crate::presence::DEFAULT_PRESENCE_TTL_MS),
        presence_refresh_interval: std::time::Duration::from_millis(
            crate::presence::DEFAULT_PRESENCE_REFRESH_INTERVAL_MS,
        ),
        broker: crate::broker::BrokerConfig::default(),
        recovery_secret: None,
        streams: crate::streams::StreamsConfig::default(),
        rpc: RpcSettings::default(),
        ingress: IngressSettings::default(),
        explicit: crate::config::ExplicitKeys::default(),
    }
}

/// A backend over a pub/sub that never becomes ready: every publication and
/// subscription is refused, and `/readyz` reports 503.
pub(super) fn backend_unready() -> Arc<Backend> {
    Arc::new(
        Backend::with_pubsub(crate::pubsub::test_support::ScriptedPubSub::unready())
            .with_namespace(NAMESPACE),
    )
}
