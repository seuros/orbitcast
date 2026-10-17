#!/usr/bin/env bash
#
# Shared plumbing for bench/ws_fanout.sh and bench/churn.sh: build the release
# binary and the generator, start and stop the relay, and read its footprint.
#
# Sourced, never executed. Every variable it reads (BACKEND, ADDR,
# INGRESS_ADDR, NAMESPACE, TOKEN, ROOT) is set by the calling script.

RELAY_PID=""
RELAY_LOG=""
GENERATOR=""

# Build the relay in release mode for the requested backend.
#
# Release, always: a debug build measures the absence of optimization. The
# `memory` backend is the default so a fanout run needs no database; `postgres`
# is the shipped default feature set and needs DATABASE_URL.
build_relay() {
  local backend="$1"

  # A prebuilt relay (a packaged gem's vendored binary, a cross-built target,
  # or a checkout that does not compile right now) can be measured as-is.
  if [ -n "${RELAY_BIN:-}" ]; then
    [ -x "$RELAY_BIN" ] || { echo "RELAY_BIN=$RELAY_BIN is not executable" >&2; exit 1; }
    echo "# using prebuilt relay $RELAY_BIN (backend=$backend not rebuilt)" >&2
    return 0
  fi

  case "$backend" in
    memory)
      echo "# building orbitcast (release, memory backend)" >&2
      cargo build --release --locked --no-default-features --features memory >&2
      ;;
    postgres)
      : "${DATABASE_URL:?BACKEND=postgres needs DATABASE_URL}"
      echo "# building orbitcast (release, postgres backend)" >&2
      cargo build --release --locked >&2
      ;;
    *)
      echo "unknown BACKEND: $backend (expected memory or postgres)" >&2
      exit 2
      ;;
  esac

  RELAY_BIN="$ROOT/target/release/orbitcast"
  [ -x "$RELAY_BIN" ] || { echo "relay binary not found at $RELAY_BIN" >&2; exit 1; }
}

# Build the load generator. Its own workspace, its own lockfile, its own
# target directory - it never enters the relay's build graph.
build_generator() {
  echo "# building the load generator" >&2
  cargo build --release --manifest-path "$ROOT/bench/Cargo.toml" >&2
  GENERATOR="$ROOT/bench/target/release/orbitcast-bench"
  [ -x "$GENERATOR" ] || { echo "generator not found at $GENERATOR" >&2; exit 1; }
}

# Start the relay with the admission limits turned *off*.
#
# Every client in a bench run comes from one address, so the default per-address
# connection limit (60/min) and the per-session command limit would be measuring
# the limiter rather than the relay. Turning them off is a benchmark decision
# and must never be copied into a deployment.
start_relay() {
  RELAY_LOG="$(mktemp -t orbitcast-bench-XXXXXX)"

  # Built as an array and passed through `env`: a conditional
  # `${VAR:+NAME=value}` prefix does not work, because bash classifies
  # assignment words before expansion and would run the expansion as the
  # command instead.
  local -a relay_env=(
    ORBITCAST_LISTEN_ADDR="$ADDR"
    ORBITCAST_INGRESS_LISTEN_ADDR="$INGRESS_ADDR"
    ORBITCAST_INGRESS_TOKEN_BASE64="$(printf '%s' "$TOKEN" | base64 | tr -d '\n')"
    ORBITCAST_NAMESPACE="$NAMESPACE"
    ORBITCAST_PUBLIC_STREAMS=true
    # The relay refuses unsigned public streams without this explicit
    # acknowledgment (src/config.rs); the generator speaks the public profile.
    ORBITCAST_PUBLIC_STREAMS_INSECURE=true
    ORBITCAST_RATE_CONNECT_PER_IP_PER_MIN=0
    ORBITCAST_RATE_COMMANDS_PER_CONN_PER_SEC=0
    ORBITCAST_MAX_CONNECTIONS="${MAX_CONNECTIONS:-200000}"
    ORBITCAST_LOG="${ORBITCAST_LOG:-warn}"
  )
  if [ -n "${DATABASE_URL:-}" ]; then
    relay_env+=(ORBITCAST_DATABASE_URL="$DATABASE_URL")
  fi
  env "${relay_env[@]}" "$RELAY_BIN" serve >"$RELAY_LOG" 2>&1 &
  RELAY_PID=$!

  # Readiness, not liveness: the backend must be able to accept publications
  # before the first client connects.
  for _ in $(seq 1 100); do
    if curl -fsS "http://$ADDR/readyz" >/dev/null 2>&1; then
      echo "# relay ready on $ADDR (pid $RELAY_PID, log $RELAY_LOG)" >&2
      return 0
    fi
    if ! kill -0 "$RELAY_PID" 2>/dev/null; then
      echo "relay exited during startup; log follows:" >&2
      cat "$RELAY_LOG" >&2
      RELAY_PID=""
      exit 1
    fi
    sleep 0.1
  done

  # Stop it here: the caller installs its EXIT trap only after start_relay
  # returns, so a relay that is up but never ready would otherwise outlive
  # the script and hold the ports for the next run.
  echo "relay did not become ready within 10s; log follows:" >&2
  cat "$RELAY_LOG" >&2
  stop_relay
  exit 1
}

stop_relay() {
  [ -n "$RELAY_PID" ] || return 0
  kill "$RELAY_PID" 2>/dev/null || true
  wait "$RELAY_PID" 2>/dev/null || true
  RELAY_PID=""
}

# Resident set size and thread count, straight from ps.
#
# RSS is what an operator sees and what a container limit enforces. It is not
# the allocator's own accounting, and the two do not have to agree - record
# which one a published figure came from.
report_relay_footprint() {
  [ -n "$RELAY_PID" ] || return 0

  local rss_kb
  rss_kb="$(ps -o rss= -p "$RELAY_PID" 2>/dev/null | tr -d ' ')"
  [ -n "$rss_kb" ] || return 0

  printf '+-%s-+-%s-+\n' "-------------" "--------------"
  printf '| %-13s | %-14s |\n' "footprint" "value"
  printf '+-%s-+-%s-+\n' "-------------" "--------------"
  printf '| %-13s | %-14s |\n' "rss" "$((rss_kb / 1024)) MiB"
  printf '| %-13s | %-14s |\n' "rss (raw)" "$rss_kb KiB"
  printf '+-%s-+-%s-+\n' "-------------" "--------------"
}

# The relay's own counters at the end of the run, from the private ingress
# listener: a run without this scrape is missing half its evidence.
# Histogram buckets and build info are elided;
# every other orbitcast_* sample is printed verbatim.
report_relay_metrics() {
  [ -n "$RELAY_PID" ] || return 0
  echo "# /metrics scrape at the end of the run:"
  curl -fsS -H "Authorization: Bearer $TOKEN" "http://$INGRESS_ADDR/metrics" 2>/dev/null \
    | grep -E '^orbitcast_' \
    | grep -vE '^orbitcast_(build_info|[a-z_]+_bucket)' \
    || echo "# (scrape failed)"
}
