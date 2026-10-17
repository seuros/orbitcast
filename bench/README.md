# OrbitCast benchmark harness

Two reproducible workloads and the load generator that drives them. Nothing
here is part of the OrbitCast build: `bench/` is its own crate, with its own
`Cargo.lock` and its own `target/`, listed under `workspace.exclude` in the
root `Cargo.toml`.

**There are no published numbers.** These scripts produce measurements of one
machine on one run; treat anything they print as that, not a capacity claim.

## Layout

| file | what it is |
|------|------------|
| `Cargo.toml`, `src/main.rs` | the generator (`orbitcast-bench`): WebSocket clients, ingress publisher, percentile reporting |
| `lib.sh` | shared plumbing: build, start/stop the relay, read RSS |
| `ws_fanout.sh` | fanout latency, throughput, and per-sequence loss/duplicate/reorder accounting |
| `churn.sh` | connect/disconnect rate and footprint |
| `soak.sh` | long fanout against a Docker PostgreSQL that is restarted mid-run; asserts reconnect, re-LISTEN, and idle counts from `/metrics` |

## Running

Both scripts build a **release** relay, start it themselves, and stop it on
exit. Nothing else needs to be running.

```sh
bench/ws_fanout.sh                          # 50 clients, 100 messages
CLIENTS=1000 MESSAGES=500 bench/ws_fanout.sh
PAYLOAD_BYTES=32768 bench/ws_fanout.sh      # Turbo-sized payloads
PUBLISH_INTERVAL_MS=0 bench/ws_fanout.sh    # saturate instead of measuring idle latency
STREAMS=16 CLIENTS=160 bench/ws_fanout.sh   # many small streams instead of one hot stream
DURATION_SECS=60 bench/ws_fanout.sh         # publish for a duration instead of a count

bench/churn.sh                              # 8 workers x 100 cycles
CYCLES=1000 CONCURRENCY=32 bench/churn.sh

bench/soak.sh                               # 120 s, 100 clients, 4 streams, PG restarted at 60 s
DURATION_SECS=600 CLIENTS=500 STREAMS=16 RESTART_AT_SECS=120 bench/soak.sh
```

`soak.sh` needs Docker (it starts and restarts `postgres:18` itself) and is
what `.github/workflows/soak.yml` runs weekly. It exits non-zero when the relay
did not reconnect, did not re-`LISTEN` (publications stop arriving after the
restart), or did not return to its idle connection/subscription/interest
counts and thread count once the generator is gone. Frames lost *during* the
outage are reported, not asserted: delivery is notification-only.

Against PostgreSQL instead of the in-process memory backend:

```sh
BACKEND=postgres DATABASE_URL=postgres://cable@localhost/orbitcast_bench bench/ws_fanout.sh
```

`RELAY_BIN=/path/to/orbitcast` skips the relay build and measures that binary
instead (a packaged gem's vendored executable, for instance).

Knobs shared by the scripts: `BACKEND` (`memory` | `postgres`), `ADDR`,
`INGRESS_ADDR`, `NAMESPACE`, `TOKEN`, `STREAM`, `MAX_CONNECTIONS`,
`ORBITCAST_LOG`. Fanout-only: `STREAMS`, `DURATION_SECS`, `IDLE_TIMEOUT_MS`.

## What the numbers mean

- **Latency** is *publish call to client receive*, measured on one monotonic
  clock inside the generator process: the request half of the ingress HTTP
  round trip, the backend commit, the fanout, and the client's own read. It is
  not the relay's internal handling time, and it is not comparable to a
  server-side span.
- **Percentiles** are exact (full sample, sorted, nearest-rank), not estimated
  from buckets.
- **Delivery** is counted by **sequence number**, per stream, per client:
  every payload carries `seq` (contiguous per stream; a failed publish does
  not consume one), and a client stops only when it has every accepted
  sequence number or the settle allowance after the last publish is spent —
  never merely after *N* frames, so a duplicate can no longer hide a loss.
  The table separates `distinct`, `duplicate` (a `seq` seen twice on one
  socket), `reordered` (a `seq` lower than one already seen), `unparseable`
  (no `seq` at all), and `missing`. Delivery is `distinct / expected`.
  Below 100% is a result, not a harness error: every client is confirmed
  subscribed before the first publish, so a missing frame is a real loss and
  not a race with subscription. Any non-zero duplicate or reordered count is
  a relay bug and must be recorded as such.
- **bytes/sec** is text-frame bytes received by all clients over the total
  window (publish window plus settle), i.e. the fanout output rate, not the
  ingress input rate.
- **Streams.** `--streams N` spreads clients and publications round-robin over
  `<stream>:0..N-1`, which is the "one hot stream versus many small streams"
  row of the workload table. Expected frames are computed per stream.
- **`publish rate`** with the default `PUBLISH_INTERVAL_MS=2` is paced, not
  maximal — the pacing is what makes the latency figure meaningful. Set
  `PUBLISH_INTERVAL_MS=0` to measure saturation instead, and expect the
  latency columns to become a queueing measurement.
- **RSS** comes from `ps`, which is what an operator sees and what a container
  limit enforces. It is not the allocator's own accounting; if you publish a
  memory figure, say which one it is.

## Two things the scripts deliberately do

1. **They disable the admission limits.** Every client comes from one address,
   so `ORBITCAST_RATE_CONNECT_PER_IP_PER_MIN=0` and
   `ORBITCAST_RATE_COMMANDS_PER_CONN_PER_SEC=0` are set for the run. Without
   that, a fanout run past 60 clients measures the rate limiter. Never copy
   those values into a deployment.
2. **They use a different WebSocket implementation than the server.**
   `tokio-tungstenite` on the client side, rama on the server side: a
   generator that shares the implementation under test cannot expose a bug in
   it.

## Limits of the generator

- One process generates all the load. Past a few thousand sockets, the
  generator's own scheduling shows up in the latency tail — record the
  generator's CPU alongside the relay's, and treat a run where the generator is
  saturated as invalid.
- It speaks the public (unsigned) stream profile only. Signed streams and
  Rails dispatch add authorization work per subscribe that this does not
  measure.
- `churn.sh` reports rate and RSS. File descriptors and task counts are
  **not** collected here yet; `soak.sh` reads the backend `LISTEN` count
  (`orbitcast_streams_with_interest`) and
  the thread count from `/metrics` and `ps`, which is the closest observable
  the relay exports.
