//! History must survive the relay: a broker opened by a fresh process
//! reads what the previous one retained, at the
//! same positions and with the same ids, and the fencing generation continues
//! rather than restarting.
//!
//! The one recovery property a memory store cannot show, so this is the
//! PostgreSQL half of `tests/recovery.rs`, built with the default feature set
//! and run against a throwaway database:
//!
//! ```text
//! DATABASE_URL=postgres://... cargo test --test recovery_postgres -- --ignored
//! ```
#![cfg(all(feature = "postgres", not(feature = "memory")))]

use std::str::FromStr;
use std::sync::Arc;

use orbitcast::broker::postgres::{PostgresBroker, PostgresBrokerConfig};
use orbitcast::broker::{Broker, BrokerConfig};
use orbitcast::protocol::v1::publication::Publication;
use orbitcast::protocol::v1::recovery::Cursor;
use sqlx::Connection as _;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgSslMode};

/// The broker tables exactly as the gem migrations leave them; one copy
/// shared with the crate's own `#[ignore]` tests.
const SCHEMA_SQL: &str = include_str!("support/broker_schema.sql");

async fn open(namespace: &str) -> Arc<PostgresBroker> {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL for the #[ignore] test");
    let options =
        PgConnectOptions::from_str(&url).expect("DATABASE_URL").ssl_mode(PgSslMode::Disable);
    let mut client = PgConnection::connect_with(&options).await.expect("connect");
    sqlx::raw_sql(SCHEMA_SQL).execute(&mut client).await.expect("schema");

    let config = PostgresBrokerConfig::new(
        url,
        namespace,
        BrokerConfig { enabled: true, ..BrokerConfig::default() },
    );
    PostgresBroker::connect(config).await.expect("broker connects")
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn retained_history_survives_a_relay_restart() {
    let namespace = format!("itest_restart_{}", std::process::id());

    // Relay A retains three publications and takes a session lease.
    let first = open(&namespace).await;
    let mut ids = Vec::new();
    for n in 1..=3 {
        let publication = Publication::new(&namespace, "chat:42", format!(r#"{{"n":{n}}}"#));
        ids.push(publication.id.clone());
        first.retain(&publication).await.expect("retained");
    }
    assert_eq!(first.fence("session:7").await.unwrap(), 1);
    drop(first);

    // Relay B - a new process as far as PostgreSQL is concerned - sees
    // the same history at the same positions, and the lease continues
    // rather than restarting.
    let second = open(&namespace).await;
    let range = second.range("chat:42").await.unwrap();
    assert_eq!(range.watermark, Cursor::new(1, 3));
    assert_eq!(range.oldest, Some(1));

    let rows = second.fetch_after("chat:42", Cursor::new(1, 0), 10).await.unwrap();
    let read: Vec<(u64, String)> =
        rows.iter().map(|m| (m.position.sequence, m.publication_id.clone())).collect();
    let expected: Vec<(u64, String)> = (1..=3).zip(ids).collect();
    assert_eq!(read, expected, "same order, same ids, after the restart");

    assert_eq!(second.fencing_generation("session:7").await.unwrap(), 1);
    assert_eq!(second.fence("session:7").await.unwrap(), 2, "the lease is durable");
}
