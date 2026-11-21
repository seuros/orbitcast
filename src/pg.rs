//! PostgreSQL plumbing shared by the pub/sub, broker, and presence backends.
//!
//! - [`connect_with_sslmode`] and [`listen_with_sslmode`] honour the DSN's
//!   `sslmode` (see [`SslMode`]; default `prefer`).
//! - [`PgSlot`] is one persistent session: a single connection serialized by
//!   a mutex and reopened when it breaks. Each backend layers its own
//!   reconnect-once retry and error type on top.
//! - [`describe_pg_error`] adds a backend's migration hint to a missing-table
//!   error.
//!
//! # TLS
//!
//! rustls with the platform's native root store. Every TLS attempt verifies
//! the server certificate against those roots and, except under
//! `verify-ca`, the host name: the checks the system TLS library made before
//! this crate moved to rustls. `require` therefore verifies like
//! `verify-full` (stricter than libpq, unchanged from earlier releases), and a
//! `sslrootcert` in the DSN adds its CAs to the native roots.

use std::borrow::Cow;
use std::future::Future;
use std::str::FromStr;

use sqlx::ConnectOptions as _;
use sqlx::Connection as _;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgListener, PgPoolOptions, PgSslMode};
use tokio::sync::{Mutex, MutexGuard};
use tokio::task::JoinHandle;
use tracing::warn;

/// SQLSTATE `undefined_table`.
const UNDEFINED_TABLE: &str = "42P01";

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

/// One way of reaching the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transport {
    Plain,
    /// TLS, the certificate verified against the trusted roots and, with
    /// `verify_host`, issued to the host connected to.
    Tls {
        verify_host: bool,
    },
}

impl Transport {
    const fn label(self) -> &'static str {
        match self {
            Self::Plain => "non-TLS",
            Self::Tls { .. } => "TLS",
        }
    }

    const fn ssl_mode(self) -> PgSslMode {
        match self {
            Self::Plain => PgSslMode::Disable,
            Self::Tls { verify_host: true } => PgSslMode::VerifyFull,
            Self::Tls { verify_host: false } => PgSslMode::VerifyCa,
        }
    }
}

/// The transports `mode` tries, in order.
///
/// `allow` tries plain text first and falls back to TLS; `prefer` the reverse;
/// `require` and the `verify-*` modes accept TLS only.
const fn transports(mode: SslMode) -> &'static [Transport] {
    const VERIFIED: Transport = Transport::Tls { verify_host: true };
    match mode {
        SslMode::Disable => &[Transport::Plain],
        SslMode::Allow => &[Transport::Plain, VERIFIED],
        SslMode::Prefer => &[VERIFIED, Transport::Plain],
        SslMode::Require | SslMode::VerifyFull => &[VERIFIED],
        SslMode::VerifyCa => &[Transport::Tls { verify_host: false }],
    }
}

/// The DSN as connect options, before a transport is chosen.
///
/// Statements are never cached server-side (each runs as an unnamed
/// statement, so a transaction pooler never sees a prepared statement it does
/// not know), and the driver's own statement log is off: every backend logs
/// its operations itself, never their SQL.
fn base_options(connection_string: &str) -> anyhow::Result<PgConnectOptions> {
    Ok(PgConnectOptions::from_str(connection_string)?
        .statement_cache_capacity(0)
        .disable_statement_logging())
}

/// Run `connect` once per transport the DSN's `sslmode` allows (default
/// `prefer`), returning the first success.
///
/// # Errors
///
/// An unparseable DSN, or the connection error; when two transports were
/// tried, both errors.
async fn with_sslmode<T, F, Fut>(connection_string: &str, mut connect: F) -> anyhow::Result<T>
where
    F: FnMut(PgConnectOptions) -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>,
{
    let base = base_options(connection_string)?;
    let mode = sslmode_from_url(connection_string).unwrap_or(SslMode::Prefer);

    let mut failures: Vec<String> = Vec::new();
    for &transport in transports(mode) {
        match connect(base.clone().ssl_mode(transport.ssl_mode())).await {
            Ok(value) => return Ok(value),
            Err(error) if transports(mode).len() == 1 => return Err(error.into()),
            Err(error) => {
                failures.push(format!("{} connection failed ({error})", transport.label()));
            }
        }
    }
    Err(anyhow::anyhow!(failures.join("; ")))
}

/// Connect one session, honouring the DSN's `sslmode` (default `prefer`).
///
/// # Errors
///
/// See [`with_sslmode`].
pub async fn connect_with_sslmode(connection_string: &str) -> anyhow::Result<PgConnection> {
    with_sslmode(connection_string, |options| async move { options.connect().await }).await
}

/// Open a `LISTEN` session, honouring the DSN's `sslmode` (default `prefer`).
///
/// [`PgListener`] needs a pool to draw its connection from. The pool holds at
/// most that one connection, never expires it, and is never asked for another:
/// the listener does not reconnect by itself (`eager_reconnect(false)`), so a
/// lost connection surfaces as `Ok(None)` from [`PgListener::try_recv`] and
/// the caller's supervisor owns reconnect and backoff.
///
/// The transport is chosen with a direct connection first: the pool retries a
/// refused connection until its acquire timeout and then reports only the
/// timeout, which would hide the real error and stall the `prefer`/`allow`
/// fallback.
///
/// # Errors
///
/// See [`with_sslmode`].
pub async fn listen_with_sslmode(connection_string: &str) -> anyhow::Result<PgListener> {
    with_sslmode(connection_string, |options| async move {
        options.connect().await?.close().await?;

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .min_connections(0)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        let mut listener = PgListener::connect_with(&pool).await?;
        listener.ignore_pool_close_event(true);
        listener.eager_reconnect(false);
        Ok(listener)
    })
    .await
}

/// Aborts a background task when the owner is dropped.
pub struct AbortOnDrop(pub JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Whether PostgreSQL itself reported `error` (a missing table, a constraint
/// violation): the session is fine and re-running would fail the same way.
/// Everything else - I/O, TLS, protocol, decoding - may be a dead session.
#[must_use]
pub const fn is_server_error(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(_))
}

/// Whether `error` means the connection itself can no longer be used.
const fn is_transport_error(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::WorkerCrashed
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
    )
}

/// The SQLSTATE of a server-reported error.
#[must_use]
pub fn sqlstate(error: &sqlx::Error) -> Option<Cow<'_, str>> {
    error.as_database_error().and_then(sqlx::error::DatabaseError::code)
}

/// Render a PostgreSQL error, prefixed with the backend's migration hint when
/// the error is a missing table.
pub fn describe_pg_error(
    error: &sqlx::Error,
    missing_table_hint: impl FnOnce() -> String,
) -> String {
    match sqlstate(error) {
        Some(code) if code == UNDEFINED_TABLE => format!("{}: {error}", missing_table_hint()),
        _ => error.to_string(),
    }
}

/// One open connection, plus whether a statement found it broken.
pub struct OpenClient {
    pub client: PgConnection,
    role: &'static str,
    broken: bool,
}

impl OpenClient {
    /// Connect. `role` names the session in the warning logged if it breaks.
    ///
    /// # Errors
    ///
    /// Whatever [`connect_with_sslmode`] reports.
    pub async fn connect(database_url: &str, role: &'static str) -> anyhow::Result<Self> {
        Ok(Self { client: connect_with_sslmode(database_url).await?, role, broken: false })
    }

    /// Pass a statement's result through, remembering a transport failure so
    /// the next [`PgSlot::acquire`] reconnects before handing the session out.
    ///
    /// # Errors
    ///
    /// `result`'s own error, unchanged.
    pub fn note<T>(&mut self, result: Result<T, sqlx::Error>) -> Result<T, sqlx::Error> {
        if let Err(error) = &result
            && is_transport_error(error)
            && !self.broken
        {
            self.broken = true;
            warn!(error = %error, role = self.role, "PostgreSQL session connection closed");
        }
        result
    }
}

/// One persistent PostgreSQL session: a single connection serialized by a
/// mutex, reopened when it breaks.
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

    /// Lock the session, reconnecting first if a statement found it broken
    /// (see [`OpenClient::note`]).
    ///
    /// # Errors
    ///
    /// A failed reconnect. The broken connection stays in place, so the next
    /// call tries again.
    pub async fn acquire(&self) -> anyhow::Result<MutexGuard<'_, OpenClient>> {
        let mut open = self.open.lock().await;
        if open.broken {
            self.reopen(&mut open).await?;
        }
        Ok(open)
    }

    /// Replace a locked connection with a fresh one.
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
