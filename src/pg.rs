//! PostgreSQL plumbing shared by the pub/sub, broker, and presence backends.
//!
//! - [`connect_with_sslmode`] honours the DSN's `sslmode` the way libpq does
//!   (`disable`, `allow`, `prefer`, `require`, `verify-*`; default `prefer`).
//! - [`PgSlot`] is one persistent session: a single client serialized by a
//!   mutex and reopened when its connection dies. Each backend layers its own
//!   reconnect-once retry and error type on top.
//! - [`describe_pg_error`] adds a backend's migration hint to a missing-table
//!   error.

use std::task::{Context, Poll};

use native_tls::TlsConnector;
use postgres_native_tls::{MakeTlsConnector, TlsStream};
use tokio::sync::{Mutex, MutexGuard};
use tokio::task::JoinHandle;
use tokio_postgres::{AsyncMessage, NoTls};
use tracing::warn;

type TlsConnection =
    tokio_postgres::Connection<tokio_postgres::Socket, TlsStream<tokio_postgres::Socket>>;
type NoTlsConnection =
    tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>;

/// The connection half of a client, whichever transport it negotiated.
pub enum PgConnection {
    Tls(TlsConnection),
    NoTls(NoTlsConnection),
}

impl PgConnection {
    /// Poll for the next asynchronous message (a notification or notice),
    /// driving the connection as a side effect.
    pub fn poll_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AsyncMessage, tokio_postgres::Error>>> {
        match self {
            Self::Tls(conn) => conn.poll_message(cx),
            Self::NoTls(conn) => conn.poll_message(cx),
        }
    }

    /// Drive the connection until it closes.
    ///
    /// # Errors
    ///
    /// The I/O or protocol error that ended the connection.
    pub async fn run(self) -> Result<(), tokio_postgres::Error> {
        match self {
            Self::Tls(conn) => conn.await,
            Self::NoTls(conn) => conn.await,
        }
    }
}

/// A DSN's `sslmode`, as libpq spells it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

/// The `sslmode` query parameter of a DSN, if present and recognized.
///
/// The key is case-insensitive; an unrecognized value reads as absent, which
/// [`connect_with_sslmode`] treats as libpq's default, `prefer`.
#[must_use]
pub fn sslmode_from_url(connection_string: &str) -> Option<SslMode> {
    let query = connection_string.split_once('?')?.1;
    let (_, value) = query
        .split('&')
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .find(|(key, _)| key.eq_ignore_ascii_case("sslmode"))?;

    match value.to_ascii_lowercase().as_str() {
        "disable" => Some(SslMode::Disable),
        "allow" => Some(SslMode::Allow),
        "prefer" => Some(SslMode::Prefer),
        "require" => Some(SslMode::Require),
        "verify-ca" | "verify_ca" => Some(SslMode::VerifyCa),
        "verify-full" | "verify_full" => Some(SslMode::VerifyFull),
        _ => None,
    }
}

async fn connect_tls(
    connection_string: &str,
) -> anyhow::Result<(tokio_postgres::Client, PgConnection)> {
    let connector = TlsConnector::builder().build()?;
    let tls = MakeTlsConnector::new(connector);
    let (client, connection) = tokio_postgres::connect(connection_string, tls).await?;
    Ok((client, PgConnection::Tls(connection)))
}

async fn connect_no_tls(
    connection_string: &str,
) -> anyhow::Result<(tokio_postgres::Client, PgConnection)> {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
    Ok((client, PgConnection::NoTls(connection)))
}

/// Connect, honouring the DSN's `sslmode` (default `prefer`).
///
/// `allow` tries plain text first and falls back to TLS; `prefer` the reverse;
/// `require` and the `verify-*` modes accept TLS only.
///
/// # Errors
///
/// The connection error; when both transports were tried, both errors.
pub async fn connect_with_sslmode(
    connection_string: &str,
) -> anyhow::Result<(tokio_postgres::Client, PgConnection)> {
    match sslmode_from_url(connection_string).unwrap_or(SslMode::Prefer) {
        SslMode::Disable => connect_no_tls(connection_string).await,
        SslMode::Allow => match connect_no_tls(connection_string).await {
            Ok(conn) => Ok(conn),
            Err(no_tls_err) => connect_tls(connection_string).await.map_err(|tls_err| {
                anyhow::anyhow!(
                    "non-TLS connection failed ({no_tls_err}); TLS connection failed ({tls_err})"
                )
            }),
        },
        SslMode::Prefer => match connect_tls(connection_string).await {
            Ok(conn) => Ok(conn),
            Err(tls_err) => connect_no_tls(connection_string).await.map_err(|no_tls_err| {
                anyhow::anyhow!(
                    "TLS connection failed ({tls_err}); non-TLS connection failed ({no_tls_err})"
                )
            }),
        },
        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => {
            connect_tls(connection_string).await
        }
    }
}

/// Aborts a background task when the owner is dropped.
pub struct AbortOnDrop(pub JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Render a PostgreSQL error, prefixed with the backend's migration hint when
/// the error is a missing table.
pub fn describe_pg_error(
    error: &tokio_postgres::Error,
    missing_table_hint: impl FnOnce() -> String,
) -> String {
    match error.code() {
        Some(code) if *code == tokio_postgres::error::SqlState::UNDEFINED_TABLE => {
            format!("{}: {error}", missing_table_hint())
        }
        _ => error.to_string(),
    }
}

/// A client plus the task driving its connection; dropping it ends both.
pub struct OpenClient {
    pub client: tokio_postgres::Client,
    _driver: AbortOnDrop,
}

impl OpenClient {
    /// Connect and spawn the connection driver. `role` names the session in
    /// the warning logged if the connection fails.
    ///
    /// # Errors
    ///
    /// Whatever [`connect_with_sslmode`] reports.
    pub async fn connect(database_url: &str, role: &'static str) -> anyhow::Result<Self> {
        let (client, connection) = connect_with_sslmode(database_url).await?;
        let driver = tokio::spawn(async move {
            if let Err(e) = connection.run().await {
                warn!(error = %e, role, "PostgreSQL session connection closed");
            }
        });
        Ok(Self { client, _driver: AbortOnDrop(driver) })
    }
}

/// One persistent PostgreSQL session: a single client serialized by a mutex,
/// reopened when its connection dies.
///
/// The mutex is what lets a caller run a multi-statement transaction without
/// another caller's statement interleaving on the same connection.
pub struct PgSlot {
    database_url: String,
    role: &'static str,
    open: Mutex<OpenClient>,
}

impl PgSlot {
    /// Open the session's first connection.
    ///
    /// # Errors
    ///
    /// Whatever [`connect_with_sslmode`] reports.
    pub async fn connect(database_url: String, role: &'static str) -> anyhow::Result<Self> {
        let open = OpenClient::connect(&database_url, role).await?;
        Ok(Self { database_url, role, open: Mutex::new(open) })
    }

    /// Lock the session, reconnecting first if its connection has closed.
    ///
    /// # Errors
    ///
    /// A failed reconnect. The dead client stays in place, so the next call
    /// tries again.
    pub async fn acquire(&self) -> anyhow::Result<MutexGuard<'_, OpenClient>> {
        let mut open = self.open.lock().await;
        if open.client.is_closed() {
            self.reopen(&mut open).await?;
        }
        Ok(open)
    }

    /// Replace a locked client with a fresh connection.
    ///
    /// # Errors
    ///
    /// A failed connect; `open` is left as it was.
    pub async fn reopen(&self, open: &mut OpenClient) -> anyhow::Result<()> {
        *open = OpenClient::connect(&self.database_url, self.role).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
