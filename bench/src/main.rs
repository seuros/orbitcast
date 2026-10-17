//! Load generator for the OrbitCast benchmark scripts.
//!
//! Two workloads, both driving a **separately started** `orbitcast` binary
//! over its real listeners - there is no in-process shortcut, because the
//! thing being measured is the process, not the library:
//!
//! - `fanout`: subscribe N clients across one or more streams, publish M
//!   messages (or publish for a duration) through the private HTTP ingress,
//!   and report per-message client-receive latency, bytes/sec, and - by
//!   sequence number - lost, duplicate, and reordered frames.
//! - `churn`: connect and disconnect repeatedly, and report the rate.
//!
//! # What the latency number means
//!
//! Every published payload carries a nanosecond timestamp taken from **this
//! process'** monotonic clock, and the receiving task subtracts it from a
//! reading of the same clock. Generator and measurer are one process, so there
//! is no clock skew to correct and no NTP story to tell. The interval measured
//! is *publish-call to client-receive*: it includes the ingress HTTP round
//! trip's request half, the backend commit, the fanout, and the client's own
//! read - which is what an application observes, and is deliberately not the
//! server's internal handling time.
//!
//! Percentiles are exact (the full sample is kept and sorted), not estimated
//! from buckets: at bench scale the memory is irrelevant and an exact p99 is
//! worth more than a cheap one.
//!
//! # What it is not
//!
//! It is not a capacity claim, and nothing here should be quoted as one.

// The derived async dispatch below is generic over no context with no `Send`
// bound; clippy::nursery's lint fires on the generated impl and only a
// module-level allow reaches it (an attribute on the enum does not).
#![allow(clippy::future_not_send)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use usage_rs::{Args, Cli, RunAsync, Subcommands};

/// The ActionCable subprotocol the relay speaks.
const SUBPROTOCOL: &str = "actioncable-v1-json";

/// The channel a public (unsigned) stream subscription names.
const PUBLIC_CHANNEL: &str = "Orbitcast::StreamsChannel";

/// Load generator for the OrbitCast bench scripts
#[derive(Cli, Debug)]
#[usage(bin = "orbitcast-bench", unknown_flags = "error", args_override_self = false)]
struct BenchCli {
    #[usage(subcommand)]
    workload: Workload,
}

#[derive(Subcommands, Debug)]
#[usage(run_async)]
enum Workload {
    /// Subscribe N clients to one stream and publish M messages to it.
    Fanout(FanoutArgs),
    /// Connect and disconnect repeatedly.
    Churn(ChurnArgs),
}

#[derive(Args, Debug, Clone)]
struct FanoutArgs {
    /// WebSocket URL of the relay's `/cable` listener.
    #[usage(long, default = "ws://127.0.0.1:28080/cable")]
    url: String,
    /// Private ingress publication URL.
    #[usage(long, default = "http://127.0.0.1:28081/v1/publications")]
    ingress: String,
    /// Bearer credential for the ingress (raw, not base64).
    #[usage(long, default = "bench-token")]
    token: String,
    /// Deployment namespace; must match the relay's.
    #[usage(long, default = "bench")]
    namespace: String,
    /// Stream name (with `--streams N > 1`, the prefix: `<stream>:<i>`).
    #[usage(long, default = "bench:fanout")]
    stream: String,
    /// Number of distinct streams. Clients and publications are spread over
    /// them round-robin: one hot stream (1) versus many small streams (N).
    #[usage(long, default = "1")]
    streams: usize,
    /// Number of subscribed clients.
    #[usage(long, default = "50")]
    clients: usize,
    /// Number of messages published in total (ignored with `--duration-secs`).
    #[usage(long, default = "100")]
    messages: usize,
    /// Publish for this many seconds instead of a fixed message count
    /// (soak runs: bench/soak.sh).
    #[usage(long)]
    duration_secs: Option<u64>,
    /// Extra payload bytes past the sequence/timestamp, as filler.
    #[usage(long, default = "0")]
    payload_bytes: usize,
    /// Delay between publishes, in milliseconds. `0` publishes as fast as the
    /// ingress accepts, which measures saturation rather than latency.
    #[usage(long, default = "2")]
    publish_interval_ms: u64,
    /// How long to wait for stragglers after the last publish, in
    /// milliseconds.
    #[usage(long, default = "2000")]
    settle_ms: u64,
    /// A client that hears nothing for this long gives up (default: the
    /// larger of `--settle-ms` and 1000). Raise it for a run that expects an
    /// outage in the middle, so a database restart is measured as loss and
    /// recovery rather than as every client disconnecting.
    #[usage(long)]
    idle_timeout_ms: Option<u64>,
}

#[derive(Args, Debug, Clone)]
struct ChurnArgs {
    /// WebSocket URL of the relay's `/cable` listener.
    #[usage(long, default = "ws://127.0.0.1:28080/cable")]
    url: String,
    /// Connect/disconnect cycles per worker.
    #[usage(long, default = "100")]
    cycles: usize,
    /// Concurrent workers.
    #[usage(long, default = "8")]
    concurrency: usize,
    /// Subscribe once per connection before closing it.
    #[usage(long, bool_value, default = "true")]
    subscribe: bool,
    /// Stream to subscribe to when `--subscribe` is set.
    #[usage(long, default = "bench:churn")]
    stream: String,
}

impl RunAsync for FanoutArgs {
    type Output = Result<(), Box<dyn std::error::Error>>;
    async fn run_async(self) -> Self::Output {
        fanout(self).await
    }
}

impl RunAsync for ChurnArgs {
    type Output = Result<(), Box<dyn std::error::Error>>;
    async fn run_async(self) -> Self::Output {
        churn(self).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    BenchCli::parse().workload.run_async().await
}

// --- fanout ------------------------------------------------------------------

/// Per-stream count of publications the ingress accepted, broadcast to the
/// receivers once the publisher is done so they know what "complete" means.
type Published = Arc<Vec<u64>>;

/// What one subscribed client observed.
#[derive(Default)]
struct ClientResult {
    /// Index of the stream this client subscribed to.
    stream: usize,
    /// Latency of every frame that carried a timestamp, in nanoseconds
    /// (duplicates included: a duplicate still has a real arrival time).
    latencies: Vec<u64>,
    /// Distinct sequence numbers seen.
    unique: u64,
    /// Frames whose sequence number had already been seen.
    duplicates: u64,
    /// Frames whose sequence number was lower than one already seen (and not
    /// a duplicate): the stream's order was not preserved for this socket.
    reordered: u64,
    /// Frames that carried no parseable `seq` (never expected; reported so a
    /// payload change cannot silently turn into "100% delivery").
    unparseable: u64,
    /// Payload bytes received (text frame length), all frames.
    bytes: u64,
    /// Highest sequence number seen.
    max_seq: Option<u64>,
}

async fn fanout(args: FanoutArgs) -> Result<(), Box<dyn std::error::Error>> {
    if args.streams == 0 {
        return Err("--streams must be at least 1".into());
    }
    if args.clients == 0 {
        return Err("--clients must be at least 1".into());
    }

    let clock = Arc::new(Instant::now());
    let stream_names: Arc<Vec<String>> = Arc::new(
        (0..args.streams)
            .map(|i| {
                if args.streams == 1 { args.stream.clone() } else { format!("{}:{i}", args.stream) }
            })
            .collect(),
    );

    // Every client must be *confirmed* before the first publish: a publication
    // that lands before a subscription is confirmed is correctly not
    // delivered, and counting it as a loss would measure the harness.
    let ready = Arc::new(Barrier::new(args.clients + 1));
    let (done_tx, done_rx) = tokio::sync::watch::channel::<Option<Published>>(None);
    let mut receivers = Vec::with_capacity(args.clients);

    for index in 0..args.clients {
        let args = args.clone();
        let stream = index % args.streams;
        let identifier = public_identifier(&stream_names[stream]);
        let clock = Arc::clone(&clock);
        let ready = Arc::clone(&ready);
        let done_rx = done_rx.clone();

        receivers.push(tokio::spawn(async move {
            match subscriber(index, stream, args, identifier, clock, ready, done_rx).await {
                Ok(result) => Some(result),
                Err(e) => {
                    eprintln!("client {index} failed: {e}");
                    None
                }
            }
        }));
    }

    ready.wait().await;
    eprintln!(
        "{} clients subscribed across {} stream(s) ({})",
        args.clients,
        args.streams,
        if args.streams == 1 {
            stream_names[0].clone()
        } else {
            format!("{}:0..{}", args.stream, args.streams - 1)
        }
    );

    let filler = "x".repeat(args.payload_bytes);
    let mut ingress = IngressClient::connect(&args.ingress, &args.token).await?;

    // Per-stream sequence numbers. A failed publish does not consume a
    // sequence number, so every accepted publication is contiguous per stream
    // and a receiver can tell loss from a publish failure.
    let mut published: Vec<u64> = vec![0; args.streams];
    let mut failed: u64 = 0;
    let publish_started = Instant::now();
    let duration = args.duration_secs.map(Duration::from_secs);
    let mut round: usize = 0;

    loop {
        let keep_going = match duration {
            Some(limit) => publish_started.elapsed() < limit,
            None => round < args.messages,
        };
        if !keep_going {
            break;
        }

        let stream = round % args.streams;
        let payload = serde_json::json!({
            "seq": published[stream],
            "s": stream,
            "t": clock.elapsed().as_nanos() as u64,
            "filler": filler,
        })
        .to_string();

        match ingress.publish(&args.namespace, &stream_names[stream], &payload).await {
            Ok(()) => published[stream] += 1,
            Err(e) => {
                failed += 1;
                if failed <= 10 || failed.is_multiple_of(100) {
                    eprintln!("publish #{round} (stream {stream}) failed: {e}");
                }
            }
        }
        round += 1;

        if args.publish_interval_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.publish_interval_ms)).await;
        }
    }
    let publish_elapsed = publish_started.elapsed();
    let published: Published = Arc::new(published);
    // Dropped receiver means every client already finished on its own.
    let _ = done_tx.send(Some(Arc::clone(&published)));

    // Receivers stop on their own once they have everything (or the settle
    // allowance after the last publish runs out); this only joins them.
    let mut results = Vec::with_capacity(args.clients);
    for receiver in receivers {
        if let Ok(Some(result)) = receiver.await {
            results.push(result);
        }
    }
    let total_elapsed = publish_started.elapsed();

    report_fanout(&args, &published, failed, &results, publish_elapsed, total_elapsed);
    Ok(())
}

/// One subscribed client: connect, subscribe, confirm, then record arrivals
/// until it has every accepted publication for its stream or the settle
/// allowance after the last publish runs out.
async fn subscriber(
    index: usize,
    stream: usize,
    args: FanoutArgs,
    identifier: String,
    clock: Arc<Instant>,
    ready: Arc<Barrier>,
    mut done: tokio::sync::watch::Receiver<Option<Published>>,
) -> Result<ClientResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut socket = connect(&args.url).await?;

    // Welcome, then subscribe, then confirmation - in that order, every time.
    wait_for(&mut socket, "welcome").await?;
    socket
        .send(Message::Text(
            serde_json::json!({ "command": "subscribe", "identifier": identifier })
                .to_string()
                .into(),
        ))
        .await?;
    wait_for(&mut socket, "confirm_subscription").await?;

    ready.wait().await;

    let mut result = ClientResult { stream, ..ClientResult::default() };
    let mut seen = SeqSet::default();
    let idle = Duration::from_millis(args.idle_timeout_ms.unwrap_or(args.settle_ms.max(1_000)));
    let settle = Duration::from_millis(args.settle_ms);
    let mut last_frame = Instant::now();
    let mut done_at: Option<Instant> = None;
    let mut expected: Option<u64> = None;

    loop {
        // Completion: once the publisher is done we know how many frames this
        // stream should deliver; stop as soon as they are all here, or when
        // the settle allowance after the last publish is spent.
        if done_at.is_none()
            && let Some(published) = done.borrow_and_update().as_ref()
        {
            expected = Some(published[stream]);
            done_at = Some(Instant::now());
        }
        if let (Some(expected), Some(done_at)) = (expected, done_at)
            && (result.unique >= expected || done_at.elapsed() >= settle)
        {
            break;
        }
        if last_frame.elapsed() >= idle {
            break;
        }

        // Poll the socket in short slices so the done signal is noticed even
        // when nothing arrives; the slice is bounded by whichever deadline is
        // nearer.
        let slice = Duration::from_millis(200);
        let message = tokio::select! {
            message = socket.next() => message,
            _ = done.changed() => continue,
            () = tokio::time::sleep(slice) => continue,
        };
        let Some(Ok(message)) = message else { break };

        let Message::Text(text) = message else { continue };
        let Ok(frame) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
        if frame.get("type").is_some() {
            continue; // ping / welcome / disconnect
        }

        last_frame = Instant::now();
        result.bytes += text.len() as u64;

        let Some(seq) = frame.pointer("/message/seq").and_then(serde_json::Value::as_u64) else {
            result.unparseable += 1;
            continue;
        };
        if let Some(sent) = frame.pointer("/message/t").and_then(serde_json::Value::as_u64) {
            result.latencies.push((clock.elapsed().as_nanos() as u64).saturating_sub(sent));
        }

        if !seen.insert(seq) {
            result.duplicates += 1;
            continue;
        }
        result.unique += 1;
        match result.max_seq {
            Some(max) if seq < max => result.reordered += 1,
            Some(max) if seq > max => result.max_seq = Some(seq),
            Some(_) => {}
            None => result.max_seq = Some(seq),
        }
    }

    if let Some(expected) = expected
        && result.unique < expected
    {
        eprintln!(
            "client {index} (stream {stream}) received {}/{expected} distinct frames \
             ({} duplicate, {} reordered)",
            result.unique, result.duplicates, result.reordered
        );
    }

    // Best-effort close: the peer may already be gone.
    let _ = socket.close(None).await;
    Ok(result)
}

/// Set of sequence numbers, as a bit vector: a soak run publishes millions
/// of frames and a hash set per client would dominate the generator's memory.
#[derive(Default)]
struct SeqSet {
    words: Vec<u64>,
}

impl SeqSet {
    /// Insert `seq`; `false` if it was already present.
    fn insert(&mut self, seq: u64) -> bool {
        let word = usize::try_from(seq / 64).expect("sequence number fits in memory");
        let bit = 1u64 << (seq % 64);
        if word >= self.words.len() {
            self.words.resize(word + 1, 0);
        }
        if self.words[word] & bit != 0 {
            return false;
        }
        self.words[word] |= bit;
        true
    }
}

fn report_fanout(
    args: &FanoutArgs,
    published: &[u64],
    failed: u64,
    results: &[ClientResult],
    publish_elapsed: Duration,
    total_elapsed: Duration,
) {
    let published_total: u64 = published.iter().sum();
    let expected: u64 = results.iter().map(|r| published[r.stream]).sum();
    let unique: u64 = results.iter().map(|r| r.unique).sum();
    let duplicates: u64 = results.iter().map(|r| r.duplicates).sum();
    let reordered: u64 = results.iter().map(|r| r.reordered).sum();
    let unparseable: u64 = results.iter().map(|r| r.unparseable).sum();
    let bytes: u64 = results.iter().map(|r| r.bytes).sum();
    let received: u64 = unique + duplicates + unparseable;
    let missing = expected.saturating_sub(unique);
    let clients_complete = results.iter().filter(|r| r.unique >= published[r.stream]).count();
    let clients_with_data = results.iter().filter(|r| r.unique > 0).count();

    let mut latencies: Vec<u64> =
        results.iter().flat_map(|r| r.latencies.iter().copied()).collect();
    latencies.sort_unstable();

    let publish_seconds = publish_elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let total_seconds = total_elapsed.as_secs_f64().max(f64::MIN_POSITIVE);

    let mut table = Table::new("fanout");
    table.row("clients", args.clients.to_string());
    table.row("clients connected", results.len().to_string());
    table.row("streams", args.streams.to_string());
    table.row(
        "run",
        match args.duration_secs {
            Some(secs) => format!("{secs} s"),
            None => format!("{} messages", args.messages),
        },
    );
    table.row("messages published", published_total.to_string());
    table.row("publish failures", failed.to_string());
    table.row("frames expected", expected.to_string());
    table.row("frames received", received.to_string());
    table.row("frames distinct", unique.to_string());
    table.row("frames duplicate", duplicates.to_string());
    table.row("frames reordered", reordered.to_string());
    table.row("frames unparseable", unparseable.to_string());
    table.row("frames missing", missing.to_string());
    table.row(
        "delivery",
        if expected == 0 {
            "n/a".to_string()
        } else {
            format!("{:.2}%", unique as f64 * 100.0 / expected as f64)
        },
    );
    table.row("clients complete", format!("{clients_complete}/{}", results.len()));
    table.row("clients receiving", format!("{clients_with_data}/{}", results.len()));
    table.row("payload filler", format!("{} B", args.payload_bytes));
    table.row("bytes received", format!("{bytes} B"));
    table.row("publish window", format!("{:.3} s", publish_elapsed.as_secs_f64()));
    table.row("total window", format!("{:.3} s", total_elapsed.as_secs_f64()));
    table.row("publish rate", format!("{:.1} msg/s", published_total as f64 / publish_seconds));
    table.row("delivered rate", format!("{:.1} frames/s", received as f64 / total_seconds));
    table.row("bytes/sec", format!("{:.0} B/s", bytes as f64 / total_seconds));
    table.row("latency p50", millis(percentile(&latencies, 50.0)));
    table.row("latency p95", millis(percentile(&latencies, 95.0)));
    table.row("latency p99", millis(percentile(&latencies, 99.0)));
    table.row("latency max", millis(latencies.last().copied()));
    table.print();

    if missing > 0 || duplicates > 0 || reordered > 0 {
        eprintln!(
            "note: {missing} frames missing, {duplicates} duplicate, {reordered} reordered; \
             a shortfall is a result, not an error - record it"
        );
    }
    if results.len() < args.clients {
        eprintln!(
            "note: {} clients failed to subscribe; their frames are not counted",
            args.clients - results.len()
        );
    }
}

// --- churn -------------------------------------------------------------------

async fn churn(args: ChurnArgs) -> Result<(), Box<dyn std::error::Error>> {
    let identifier = public_identifier(&args.stream);
    let failures = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));

    let started = Instant::now();
    let mut workers = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let args = args.clone();
        let identifier = identifier.clone();
        let failures = Arc::clone(&failures);
        let completed = Arc::clone(&completed);

        workers.push(tokio::spawn(async move {
            for _ in 0..args.cycles {
                match one_cycle(&args, &identifier).await {
                    Ok(()) => completed.fetch_add(1, Ordering::Relaxed),
                    Err(_) => failures.fetch_add(1, Ordering::Relaxed),
                };
            }
        }));
    }

    for worker in workers {
        // Each worker records its own outcome in `completed`/`failures`.
        let _ = worker.await;
    }

    let elapsed = started.elapsed();
    let completed = completed.load(Ordering::Relaxed);
    let failures = failures.load(Ordering::Relaxed);
    let seconds = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);

    let mut table = Table::new("churn");
    table.row("workers", args.concurrency.to_string());
    table.row("cycles requested", (args.concurrency * args.cycles).to_string());
    table.row("cycles completed", completed.to_string());
    table.row("cycles failed", failures.to_string());
    table.row("subscribe per cycle", args.subscribe.to_string());
    table.row("elapsed", format!("{:.3} s", elapsed.as_secs_f64()));
    table.row("rate", format!("{:.1} cycles/s", completed as f64 / seconds));
    table.print();

    if failures > 0 {
        eprintln!(
            "note: {failures} cycles failed; with the default per-address connection rate limit \
             that is expected - set ORBITCAST_RATE_CONNECT_PER_IP_PER_MIN=0 for load runs"
        );
    }

    Ok(())
}

async fn one_cycle(
    args: &ChurnArgs,
    identifier: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut socket = connect(&args.url).await?;
    wait_for(&mut socket, "welcome").await?;

    if args.subscribe {
        socket
            .send(Message::Text(
                serde_json::json!({ "command": "subscribe", "identifier": identifier })
                    .to_string()
                    .into(),
            ))
            .await?;
        wait_for(&mut socket, "confirm_subscription").await?;
    }

    socket.close(None).await?;
    Ok(())
}

// --- websocket helpers -------------------------------------------------------

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

async fn connect(url: &str) -> Result<Socket, Box<dyn std::error::Error + Send + Sync>> {
    let mut request = url.into_client_request()?;
    request.headers_mut().insert("sec-websocket-protocol", SUBPROTOCOL.parse()?);
    let (socket, _response) = tokio_tungstenite::connect_async(request).await?;
    Ok(socket)
}

/// Read frames until one of `type` arrives, skipping heartbeats.
async fn wait_for(
    socket: &mut Socket,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let deadline = Duration::from_secs(10);

    loop {
        let message = tokio::time::timeout(deadline, socket.next())
            .await
            .map_err(|_| format!("timed out waiting for {expected}"))?
            .ok_or_else(|| format!("socket closed waiting for {expected}"))??;

        let Message::Text(text) = message else { continue };
        let frame: serde_json::Value = serde_json::from_str(&text)?;

        match frame.get("type").and_then(serde_json::Value::as_str) {
            Some(kind) if kind == expected => return Ok(()),
            Some("ping") | None => continue,
            Some("reject_subscription") => return Err("subscription rejected".into()),
            Some("disconnect") => return Err("server disconnected".into()),
            Some(_) => continue,
        }
    }
}

/// A public-stream ActionCable identifier, exactly as a browser sends it.
fn public_identifier(stream: &str) -> String {
    serde_json::json!({ "channel": PUBLIC_CHANNEL, "stream_name": stream }).to_string()
}

// --- ingress client ----------------------------------------------------------

/// A minimal keep-alive HTTP/1.1 client for `POST /v1/publications`.
///
/// Hand-rolled rather than pulling a full HTTP stack into a load generator:
/// the request shape is fixed, and the only thing read back is the status line
/// plus exactly `Content-Length` body bytes. A response without a
/// `Content-Length` is treated as unparseable and the connection is rebuilt,
/// so a server that ever switched to chunked encoding would slow this down
/// rather than silently mis-measure it.
struct IngressClient {
    host: String,
    port: u16,
    path: String,
    authorization: String,
    stream: Option<tokio::io::BufReader<TcpStream>>,
}

impl IngressClient {
    async fn connect(url: &str, token: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let rest = url.strip_prefix("http://").ok_or("ingress URL must be http:// (no TLS)")?;
        let (authority, path) = match rest.find('/') {
            Some(index) => (&rest[..index], &rest[index..]),
            None => (rest, "/"),
        };
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => (host.to_string(), port.parse()?),
            None => (authority.to_string(), 80),
        };

        let mut client = Self {
            host,
            port,
            path: path.to_string(),
            authorization: format!("Bearer {token}"),
            stream: None,
        };
        client.reconnect().await?;
        Ok(client)
    }

    async fn reconnect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream = TcpStream::connect((self.host.as_str(), self.port)).await?;
        stream.set_nodelay(true)?;
        self.stream = Some(tokio::io::BufReader::new(stream));
        Ok(())
    }

    async fn publish(
        &mut self,
        namespace: &str,
        stream: &str,
        json_payload: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let body = serde_json::json!({
            "version": 1,
            "namespace": namespace,
            "id": uuid::Uuid::new_v4().to_string(),
            "stream": stream,
            "json_payload": json_payload,
        })
        .to_string();

        match self.round_trip(&body).await {
            Ok(status) if (200..300).contains(&status) => Ok(()),
            Ok(status) => Err(format!("ingress answered HTTP {status}").into()),
            Err(e) => {
                // One retry on a broken keep-alive connection, and only that:
                // a retry loop would hide the failure this is here to surface.
                self.reconnect().await?;
                let status = self.round_trip(&body).await.map_err(|_| e)?;
                if (200..300).contains(&status) {
                    Ok(())
                } else {
                    Err(format!("ingress answered HTTP {status}").into())
                }
            }
        }
    }

    async fn round_trip(&mut self, body: &str) -> Result<u16, Box<dyn std::error::Error>> {
        use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};

        let stream = self.stream.as_mut().ok_or("ingress connection is not established")?;
        let request = format!(
            "POST {} HTTP/1.1\r\nHost: {}:{}\r\nAuthorization: {}\r\nContent-Type: \
             application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
            self.path,
            self.host,
            self.port,
            self.authorization,
            body.len(),
            body
        );
        stream.write_all(request.as_bytes()).await?;
        stream.flush().await?;

        let mut status_line = String::new();
        if stream.read_line(&mut status_line).await? == 0 {
            return Err("ingress closed the connection".into());
        }
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .ok_or("malformed status line")?
            .parse()
            .map_err(|_| "malformed status code")?;

        let mut content_length: Option<usize> = None;
        loop {
            let mut header = String::new();
            if stream.read_line(&mut header).await? == 0 {
                return Err("ingress closed mid-headers".into());
            }
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                content_length = value.trim().parse().ok();
            }
        }

        let length = content_length.ok_or("ingress reply carried no Content-Length")?;
        let mut discard = vec![0u8; length];
        stream.read_exact(&mut discard).await?;

        Ok(status)
    }
}

// --- reporting ---------------------------------------------------------------

/// Exact percentile over a sorted sample; `None` for an empty sample.
fn percentile(sorted: &[u64], percent: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }

    // Nearest-rank: the smallest value at or above the requested rank, which
    // is the definition that cannot report a latency nobody observed.
    let rank = (percent / 100.0 * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted.get(rank.min(sorted.len()) - 1).copied()
}

fn millis(nanos: Option<u64>) -> String {
    match nanos {
        Some(nanos) => format!("{:.3} ms", nanos as f64 / 1_000_000.0),
        None => "n/a".to_string(),
    }
}

/// A two-column table, printed to stdout so the scripts can capture it.
struct Table {
    title: &'static str,
    rows: Vec<(String, String)>,
}

impl Table {
    fn new(title: &'static str) -> Self {
        Self { title, rows: Vec::new() }
    }

    fn row(&mut self, label: &str, value: String) {
        self.rows.push((label.to_string(), value));
    }

    fn print(&self) {
        let width =
            self.rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0).max(self.title.len());
        let value_width = self.rows.iter().map(|(_, value)| value.len()).max().unwrap_or(0).max(5);

        let rule = format!("+-{}-+-{}-+", "-".repeat(width), "-".repeat(value_width));
        println!("{rule}");
        println!("| {:<width$} | {:<value_width$} |", self.title, "value");
        println!("{rule}");
        for (label, value) in &self.rows {
            println!("| {label:<width$} | {value:<value_width$} |");
        }
        println!("{rule}");
    }
}
