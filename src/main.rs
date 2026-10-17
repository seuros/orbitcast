//! OrbitCast CLI/bootstrap.
//!
//! Everything of substance lives in the `orbitcast` library; this binary only
//! parses arguments, loads and validates configuration, starts the selected
//! transports, and runs the shutdown sequence.
//!
//! # Transports
//!
//! - **Docking**: selected by the `MS_*` supervisor environment. OrbitCast
//!   binds its ship socket and waits for Mothership to dock.
//! - **HTTP/WebSocket**: selected by `ORBITCAST_LISTEN_ADDR`. The listener is
//!   bound before startup completes, so a port conflict is a startup failure.
//!
//! Both may run at once; at least one is required.
//!
//! # Subcommands
//!
//! - `serve` (the default): run the relay.
//! - `doctor [--json]`: describe the build and configuration and probe the
//!   database, without binding anything ([`orbitcast::doctor`]). Exits `0`
//!   whatever it finds — it diagnoses, it does not gate — except `2` when the
//!   configuration cannot be parsed at all.
//!
//! # Shutdown
//!
//! On SIGTERM or ctrl-c: stop accepting, advertise the standard ActionCable
//! `disconnect {reconnect: true}`, drain sessions until `shutdown_timeout`,
//! tear every remaining session down through the one teardown path, and stop
//! the pub/sub backend.
//!
//! # Metrics
//!
//! `GET /metrics` is served by the **private ingress listener** when one is
//! configured, behind the same bearer credential as the publication routes.
//! `metrics_public` additionally mounts it, unauthenticated, on the public
//! listener. With neither, the process exports nothing - fail closed. The
//! resolved choice is logged once at startup; see [`orbitcast::metrics`].

use std::sync::Arc;
use std::time::Duration;

use orbitcast::config::{Config, ConfigError};
use orbitcast::doctor::Report;
use orbitcast::server::{Backend, Shutdown};
use orbitcast::transport::{docking, http, ingress};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

#[derive(usage_rs::Cli, Debug)]
#[usage(name = "orbitcast")]
#[usage(about = "Real-time WebSocket server with ActionCable protocol")]
#[usage(version)]
#[usage(unknown_flags = "error", args_override_self = false)]
struct Args {
    /// Log level (trace, debug, info, warn, error)
    ///
    /// Precedence: this flag > `ORBITCAST_LOG` > `info`.
    #[usage(short, long, global, env = "ORBITCAST_LOG", default = "info")]
    log_level: String,
    /// Optional subcommand; a bare `orbitcast` behaves exactly like
    /// `orbitcast serve` (backward compatible with supervisor manifests that
    /// launch the binary with no arguments).
    #[usage(subcommand)]
    command: Option<Command>,
}

#[derive(usage_rs::Subcommands, Debug)]
enum Command {
    /// Run the relay (default when no subcommand is given)
    Serve,
    /// Report build, configuration, and database reachability; never serves
    Doctor(DoctorArgs),
}

#[derive(usage_rs::Args, Debug)]
struct DoctorArgs {
    /// Print one JSON object instead of text
    #[usage(long)]
    json: bool,
}

/// Exit status of `doctor` when the configuration cannot be parsed.
const EXIT_CONFIG_PARSE: i32 = 2;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // `serve` is byte-for-byte the default run. The exhaustive match keeps a
    // future subcommand from silently falling through into serving.
    match args.command {
        None | Some(Command::Serve) => {}
        Some(Command::Doctor(DoctorArgs { json })) => std::process::exit(doctor(json).await),
    }

    // Initialize JSON logging (consistent with Mothership)
    tracing_subscriber::fmt().json().with_env_filter(&args.log_level).init();

    info!("OrbitCast v{}", env!("CARGO_PKG_VERSION"));

    let config = match load_config() {
        Ok(config) => config,
        Err(e) => {
            error!(error = %e, "Failed to load config");
            std::process::exit(1);
        }
    };

    serve(&config).await
}

/// Run every configured transport until a shutdown signal, then drain.
async fn serve(config: &Config) -> anyhow::Result<()> {
    let shutdown = Shutdown::new();
    Running::start(config, &shutdown)
        .await?
        .run_until_signal(&shutdown, config.shutdown_timeout)
        .await;
    info!("OrbitCast stopped");
    Ok(())
}

/// Everything [`serve`] started, stopped in reverse by
/// [`Running::run_until_signal`].
struct Running {
    /// This process' own backend handle, released last: after every transport
    /// has stopped and the pub/sub listener was aborted.
    _backend: Arc<Backend>,
    /// The pub/sub delivery listener, if the backend has one.
    listener_task: Option<JoinHandle<()>>,
    /// The direct listener, which has a drain phase of its own.
    http: Option<Arc<http::HttpTransport>>,
    /// One task per transport.
    transports: Vec<JoinHandle<()>>,
}

impl Running {
    /// Initialize the backend, bind every configured listener, and spawn the
    /// transports.
    async fn start(config: &Config, shutdown: &Shutdown) -> anyhow::Result<Self> {
        // A backend that cannot be established is a startup failure: the
        // process exits non-zero rather than running with broadcasting off.
        let backend = match Backend::init(config).await {
            Ok(backend) => backend,
            Err(error) => {
                error!(error = %error, "Pub/sub backend failed to start; exiting");
                return Err(error);
            }
        };
        let listener_task = backend.spawn_listener();

        // Bind before anything else can succeed: a port conflict is a startup
        // failure, not a background surprise.
        let http_listener = match config.http.listen_addr {
            Some(ref addr) => Some(http::bind(addr).await?),
            None => None,
        };

        // The private ingress binds its own listener (never the public /cable
        // one); absent configuration means the ingress simply does not exist.
        let ingress_listener = match config.ingress.listen_addr {
            Some(ref addr) => Some(http::bind(addr).await?),
            None => None,
        };

        let mut transports = Vec::new();
        let mut http_transport = None;

        if let Some(listener) = http_listener {
            if !config.streams.is_enabled() && !config.rpc.is_enabled() {
                anyhow::bail!(
                    "the direct listener requires an authorization mode; configure Rails \
                     dispatch (ORBITCAST_RPC_ENDPOINT_URL + ORBITCAST_RPC_TOKEN_BASE64) or \
                     signed streams (ORBITCAST_STREAMS_SECRET_BASE64, ORBITCAST_TURBO_STREAMS + \
                     ORBITCAST_TURBO_SECRET_BASE64, ORBITCAST_PUBLIC_STREAMS)"
                );
            }

            let transport =
                http::HttpTransport::new(Arc::clone(&backend), config, shutdown.clone())?;
            http_transport = Some(Arc::clone(&transport));
            transports.push(tokio::spawn(transport.serve(listener)));
        }

        if let Some(listener) = ingress_listener {
            let transport =
                ingress::IngressTransport::new(Arc::clone(&backend), config, shutdown.clone())?;
            transports.push(tokio::spawn(transport.serve(listener)));
        }

        log_metrics_exposure(config);

        if let Some(ref docking_config) = config.docking {
            info!(
                pid = docking_config.mothership_pid,
                ship = %docking_config.ship_name,
                socket = %docking_config.socket_path().display(),
                "Starting OrbitCast docking transport"
            );

            let config = config.clone();
            let backend = Arc::clone(&backend);
            let shutdown = shutdown.clone();
            transports.push(tokio::spawn(async move {
                if let Err(e) = docking::serve(config, backend, shutdown.clone()).await {
                    error!(error = %e, "Docking transport failed");
                    // A fatal docking error takes the process down so the
                    // supervisor can relaunch the bay with a clean slate.
                    shutdown.trigger();
                }
            }));
        }

        Ok(Self { _backend: backend, listener_task, http: http_transport, transports })
    }

    /// Serve until SIGTERM or ctrl-c, then drain: advertise the shutdown,
    /// drain the direct listener, give every transport `timeout` to finish,
    /// and stop the pub/sub listener.
    async fn run_until_signal(self, shutdown: &Shutdown, timeout: Duration) {
        wait_for_signal().await;
        info!("Shutdown signal received; draining");
        shutdown.trigger();

        if let Some(transport) = self.http {
            transport.drain(timeout).await;
        }

        for task in self.transports {
            match tokio::time::timeout(timeout, task).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!(error = %e, "Transport task ended abnormally"),
                Err(_) => warn!("Transport task did not stop within the drain deadline"),
            }
        }

        if let Some(handle) = self.listener_task {
            handle.abort();
        }
    }
}

/// Say once where `/metrics` ended up.
///
/// A node with neither the private ingress nor `metrics_public` exports
/// nothing at all, which is the fail-closed default and worth stating rather
/// than leaving an operator to discover by scraping a 404.
fn log_metrics_exposure(config: &Config) {
    match (config.ingress.is_enabled(), config.http.metrics_public) {
        (true, false) => info!("Metrics: GET /metrics on the private ingress listener (bearer)"),
        (true, true) => {
            info!(
                "Metrics: GET /metrics on the private ingress listener (bearer) and, \
                 unauthenticated, on the public listener"
            );
        }
        (false, true) => {
            info!("Metrics: GET /metrics on the public listener, unauthenticated (metrics_public)");
        }
        (false, false) => info!(
            "Metrics: not exported; configure ORBITCAST_INGRESS_LISTEN_ADDR (recommended) or \
             ORBITCAST_METRICS_PUBLIC"
        ),
    }
}

/// `orbitcast doctor`: print the report and return the exit status.
///
/// No logging is initialized, so stdout carries nothing but the report. A
/// configuration that does not parse is the one failure: it is reported (as
/// JSON too, under `--json`) with [`EXIT_CONFIG_PARSE`].
async fn doctor(json: bool) -> i32 {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            if json {
                let report = serde_json::json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "target": orbitcast::doctor::TARGET,
                    "features": orbitcast::doctor::features(),
                    "envelope_versions": orbitcast::doctor::ENVELOPE_VERSIONS,
                    "config_error": error.to_string(),
                });
                println!("{report}");
            }
            eprintln!("orbitcast doctor: configuration cannot be parsed: {error}");
            return EXIT_CONFIG_PARSE;
        }
    };

    let report = Report::gather(&config).await;
    if json {
        println!("{}", report.render_json());
    } else {
        print!("{}", report.render_text());
    }
    0
}

/// Load and validate configuration, explaining what a missing transport means.
fn load_config() -> Result<Config, ConfigError> {
    let config = Config::from_env()?;

    if let Err(e) = config.validate() {
        if matches!(e, ConfigError::NoTransport) {
            error!(
                "OrbitCast needs a transport: set ORBITCAST_LISTEN_ADDR (e.g. 127.0.0.1:8080) \
                 to serve WebSockets directly, or run it as a Mothership ship (MS_PID, MS_SHIP, \
                 MS_SOCKET_DIR)"
            );
        }
        return Err(e);
    }

    Ok(config)
}

/// Resolve on SIGTERM or ctrl-c.
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut term = match signal(SignalKind::terminate()) {
            Ok(term) => term,
            Err(e) => {
                warn!(error = %e, "Cannot listen for SIGTERM; ctrl-c only");
                ctrl_c().await;
                return;
            }
        };

        tokio::select! {
            _ = term.recv() => info!("SIGTERM received"),
            () = ctrl_c() => info!("Interrupt received"),
        }
    }

    #[cfg(not(unix))]
    ctrl_c().await;
}

/// Resolve on ctrl-c.
///
/// A handler that cannot be installed is logged and then never resolves: the
/// default disposition still ends the process on an interrupt (without a
/// drain), which beats mistaking the failure for an interrupt and shutting a
/// healthy relay down at boot.
async fn ctrl_c() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        error!(error = %e, "Cannot listen for ctrl-c; an interrupt will not drain");
        std::future::pending::<()>().await;
    }
}
