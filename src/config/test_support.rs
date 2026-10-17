//! Configuration fixtures shared by the config and doctor unit tests.

use std::time::Duration;

use super::*;

/// A configuration with nothing selected: no transport, no database, no
/// namespace, no key. The shape `Config::from_env` yields in an empty
/// environment, built by hand so tests never touch the process environment.
pub fn minimal_config() -> Config {
    Config {
        docking: None,
        http: HttpConfig::default(),
        shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        database_url: None,
        listener_database_url: None,
        namespace: String::new(),
        pubsub_ack_timeout: DEFAULT_ACK_TIMEOUT,
        pubsub_queue_capacity: DEFAULT_QUEUE_CAPACITY,
        overflow: OverflowConfig::default(),
        ping_interval: 3,
        presence_ttl: Duration::from_millis(DEFAULT_PRESENCE_TTL_MS),
        presence_refresh_interval: Duration::from_millis(DEFAULT_PRESENCE_REFRESH_INTERVAL_MS),
        broker: BrokerConfig::default(),
        recovery_secret: None,
        streams: StreamsConfig::default(),
        rpc: RpcSettings::default(),
        ingress: IngressSettings::default(),
        explicit: ExplicitKeys::default(),
    }
}
