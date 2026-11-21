//! Broker-specific live-PostgreSQL fixtures for `#[ignore]` integration tests.
//! Shared by the broker and recovery tests; the connection fixtures come from
//! [`crate::pubsub::postgres::test_support`].
//!
//! Run with:
//!
//! ```text
//! DATABASE_URL=postgres://... cargo test -p orbitcast -- --ignored
//! ```

use std::sync::Arc;

use super::{PostgresBroker, PostgresBrokerConfig};
use crate::broker::{BrokerConfig, MESSAGES_TABLE, POSITIONS_TABLE, SESSIONS_TABLE};
use crate::pubsub::OverflowConfig;
use crate::pubsub::postgres::test_support::{
    batch_execute, integration_client, integration_dsn, integration_namespace,
};

/// The broker tables exactly as the gem migrations leave them. One copy,
/// shared with the `tests/` integration crate.
pub const SCHEMA_SQL: &str = include_str!("../../../tests/support/broker_schema.sql");

/// Create the broker (and relay) tables exactly as the gem migrations do.
pub async fn ensure_broker_tables() {
    let mut client = integration_client().await;

    // Parallel `CREATE TABLE IF NOT EXISTS` can still collide in pg_type,
    // so the postcondition - not this statement's result - is the check.
    let attempted = batch_execute(&mut client, SCHEMA_SQL).await;

    for table in [MESSAGES_TABLE, POSITIONS_TABLE, SESSIONS_TABLE, "orbitcast_payloads"] {
        let present: Option<String> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT to_regclass('{table}')::text")))
                .fetch_one(&mut client)
                .await
                .expect("query to_regclass");
        assert!(present.is_some(), "{table} must exist for broker tests: {attempted:?}");
    }
}

/// A connected broker in its own per-test namespace, tables ensured, relay
/// off.
pub async fn integration_broker(label: &str, broker: BrokerConfig) -> Arc<PostgresBroker> {
    integration_broker_with_relay(label, broker, OverflowConfig::default()).await
}

/// [`integration_broker`] with explicit overflow relay settings.
pub async fn integration_broker_with_relay(
    label: &str,
    broker: BrokerConfig,
    overflow: OverflowConfig,
) -> Arc<PostgresBroker> {
    ensure_broker_tables().await;
    let mut config =
        PostgresBrokerConfig::new(integration_dsn(), integration_namespace(label), broker);
    config.overflow = overflow;
    PostgresBroker::connect(config).await.expect("connect to DATABASE_URL")
}
