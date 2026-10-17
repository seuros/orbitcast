#!/usr/bin/env bash
#
# Soak / chaos run: the fanout workload for a configurable duration against a
# Docker PostgreSQL that is RESTARTED mid-run, with assertions read from
# /metrics: repeated churn leaves no growing stream/task/backend-subscription
# count.
#
#   bench/soak.sh                                   # 120 s, 100 clients, 4 streams
#   DURATION_SECS=600 CLIENTS=500 STREAMS=16 bench/soak.sh
#   RESTART_AT_SECS=30 bench/soak.sh                # restart the database earlier
#
# What is asserted (any failure exits non-zero, after printing everything):
#   1. the relay's listener reconnected: orbitcast_pubsub_reconnects_total grew;
#   2. /readyz returned to 200 after the restart (recovery time is printed);
#   3. subscriptions re-LISTENed: orbitcast_streams_with_interest is unchanged
#      across the restart AND orbitcast_pubsub_received_total keeps growing
#      afterwards (publications arrive through the re-established LISTENs);
#   4. no "re-LISTEN ... failed" line in the relay log;
#   5. after the generator exits, connections / subscriptions / streams-with-
#      interest are back at their idle values and the relay's thread count did
#      not grow (there is no task-count metric; threads are the observable).
#
# Frames lost DURING the outage are expected: delivery is notification-only
# and the retained broker is the opt-in answer. The generator reports them;
# they are printed, not asserted.
#
# Not a benchmark. Nothing it prints is a baseline.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DURATION_SECS="${DURATION_SECS:-120}"
CLIENTS="${CLIENTS:-100}"
STREAMS="${STREAMS:-4}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-256}"
PUBLISH_INTERVAL_MS="${PUBLISH_INTERVAL_MS:-5}"
RESTART_AT_SECS="${RESTART_AT_SECS:-$((DURATION_SECS / 2))}"
# Clients must survive the outage: idle timeout well above the restart window.
IDLE_TIMEOUT_MS="${IDLE_TIMEOUT_MS:-90000}"
SETTLE_MS="${SETTLE_MS:-3000}"
RECOVERY_TIMEOUT_SECS="${RECOVERY_TIMEOUT_SECS:-90}"

PG_CONTAINER="${PG_CONTAINER:-orbitcast-soak-pg}"
PG_IMAGE="${PG_IMAGE:-postgres:18}"
PG_PORT="${PG_PORT:-54331}"
KEEP_PG="${KEEP_PG:-0}"

BACKEND=postgres
DATABASE_URL="postgres://orbitcast:orbitcast@127.0.0.1:${PG_PORT}/orbitcast_soak"
ADDR="${ADDR:-127.0.0.1:28080}"
INGRESS_ADDR="${INGRESS_ADDR:-127.0.0.1:28081}"
NAMESPACE="${NAMESPACE:-soak}"
TOKEN="${TOKEN:-bench-token}"
STREAM="${STREAM:-soak:fanout}"
SOAK_LOG_DIR="${SOAK_LOG_DIR:-$(mktemp -d -t orbitcast-soak-XXXXXX)}"
mkdir -p "$SOAK_LOG_DIR"

export DATABASE_URL BACKEND ADDR INGRESS_ADDR NAMESPACE TOKEN ROOT

# shellcheck source=bench/lib.sh
source "$ROOT/bench/lib.sh"

command -v docker >/dev/null || { echo "docker is required (PostgreSQL runs in a container)" >&2; exit 2; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 2; }

FAILURES=()
fail() { FAILURES+=("$1"); echo "ASSERTION FAILED: $1" >&2; }
note() { echo "# $*" >&2; }

# --- PostgreSQL container ----------------------------------------------------

start_postgres() {
  docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
  note "starting $PG_IMAGE as $PG_CONTAINER on 127.0.0.1:$PG_PORT"
  docker run -d --name "$PG_CONTAINER" \
    -e POSTGRES_USER=orbitcast -e POSTGRES_PASSWORD=orbitcast -e POSTGRES_DB=orbitcast_soak \
    -p "127.0.0.1:${PG_PORT}:5432" "$PG_IMAGE" >/dev/null
  wait_postgres
}

wait_postgres() {
  # Over TCP, deliberately: the image's first-start entrypoint runs a
  # temporary unix-socket-only server while it initializes, and a
  # socket-based pg_isready reports that one as ready before the real
  # server (the one the relay can reach) is listening.
  for _ in $(seq 1 120); do
    if docker exec "$PG_CONTAINER" pg_isready -h 127.0.0.1 -U orbitcast -d orbitcast_soak >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "PostgreSQL did not become ready" >&2
  exit 1
}

cleanup() {
  local status=$?
  if [ -n "${GEN_PID:-}" ] && kill -0 "$GEN_PID" 2>/dev/null; then
    kill "$GEN_PID" 2>/dev/null || true
    wait "$GEN_PID" 2>/dev/null || true
  fi
  stop_relay
  if [ -n "${RELAY_LOG:-}" ] && [ -f "$RELAY_LOG" ]; then
    cp "$RELAY_LOG" "$SOAK_LOG_DIR/relay.log"
  fi
  if [ "$KEEP_PG" != "1" ]; then
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
  fi
  note "logs in $SOAK_LOG_DIR"
  exit "$status"
}

# --- metrics -----------------------------------------------------------------

scrape() {
  curl -fsS -H "Authorization: Bearer $TOKEN" "http://$INGRESS_ADDR/metrics"
}

# Value of an unlabelled metric family from a saved scrape.
metric() {
  local file="$1" name="$2"
  awk -v n="$name" '$1 == n { print $2; found = 1 } END { if (!found) print "missing" }' "$file"
}

ready_code() {
  curl -s -o /dev/null -w '%{http_code}' "http://$ADDR/readyz" || true
}

relay_threads() {
  case "$(uname -s)" in
    Linux) ps -o nlwp= -p "$RELAY_PID" | tr -d ' ' ;;
    Darwin) ps -M -p "$RELAY_PID" | tail -n +2 | wc -l | tr -d ' ' ;;
    *) echo "n/a" ;;
  esac
}

assert_eq() { # label expected actual
  if [ "$2" != "$3" ]; then fail "$1: expected $2, got $3"; else note "ok: $1 = $3"; fi
}

assert_gt() { # label after before
  if [ "$2" = "missing" ] || [ "$3" = "missing" ]; then fail "$1: metric missing ($3 -> $2)"
  elif [ "$2" -gt "$3" ]; then note "ok: $1 grew $3 -> $2"
  else fail "$1: did not grow ($3 -> $2)"; fi
}

# --- run ---------------------------------------------------------------------

build_relay postgres
build_generator
start_postgres
start_relay
trap cleanup EXIT

note "soak: duration=${DURATION_SECS}s clients=$CLIENTS streams=$STREAMS payload=${PAYLOAD_BYTES}B interval=${PUBLISH_INTERVAL_MS}ms restart_at=${RESTART_AT_SECS}s"

scrape > "$SOAK_LOG_DIR/metrics-idle.txt"
IDLE_CONNECTIONS="$(metric "$SOAK_LOG_DIR/metrics-idle.txt" orbitcast_connections_current)"
IDLE_SUBSCRIPTIONS="$(metric "$SOAK_LOG_DIR/metrics-idle.txt" orbitcast_subscriptions_current)"
IDLE_INTEREST="$(metric "$SOAK_LOG_DIR/metrics-idle.txt" orbitcast_streams_with_interest)"
IDLE_RECONNECTS="$(metric "$SOAK_LOG_DIR/metrics-idle.txt" orbitcast_pubsub_reconnects_total)"
IDLE_THREADS="$(relay_threads)"
note "idle: connections=$IDLE_CONNECTIONS subscriptions=$IDLE_SUBSCRIPTIONS interest=$IDLE_INTEREST reconnects=$IDLE_RECONNECTS threads=$IDLE_THREADS"
report_relay_footprint

"$GENERATOR" fanout \
  --url "ws://$ADDR/cable" \
  --ingress "http://$INGRESS_ADDR/v1/publications" \
  --token "$TOKEN" \
  --namespace "$NAMESPACE" \
  --stream "$STREAM" \
  --streams "$STREAMS" \
  --clients "$CLIENTS" \
  --duration-secs "$DURATION_SECS" \
  --payload-bytes "$PAYLOAD_BYTES" \
  --publish-interval-ms "$PUBLISH_INTERVAL_MS" \
  --settle-ms "$SETTLE_MS" \
  --idle-timeout-ms "$IDLE_TIMEOUT_MS" \
  > "$SOAK_LOG_DIR/fanout.txt" 2> "$SOAK_LOG_DIR/fanout.err" &
GEN_PID=$!

# Let the clients subscribe and traffic flow before pulling the plug.
for _ in $(seq 1 "$RESTART_AT_SECS"); do
  sleep 1
  if ! kill -0 "$GEN_PID" 2>/dev/null; then
    echo "generator exited before the restart; stderr follows:" >&2
    cat "$SOAK_LOG_DIR/fanout.err" >&2
    exit 1
  fi
done

scrape > "$SOAK_LOG_DIR/metrics-before-restart.txt"
RUN_INTEREST="$(metric "$SOAK_LOG_DIR/metrics-before-restart.txt" orbitcast_streams_with_interest)"
RUN_CONNECTIONS="$(metric "$SOAK_LOG_DIR/metrics-before-restart.txt" orbitcast_connections_current)"
RECEIVED_BEFORE="$(metric "$SOAK_LOG_DIR/metrics-before-restart.txt" orbitcast_pubsub_received_total)"
note "before restart: connections=$RUN_CONNECTIONS interest=$RUN_INTEREST received=$RECEIVED_BEFORE"
assert_eq "streams with interest during the run" "$STREAMS" "$RUN_INTEREST"

note "restarting $PG_CONTAINER"
RESTART_STARTED="$(date +%s)"
docker restart "$PG_CONTAINER" >/dev/null
wait_postgres
note "PostgreSQL back after $(( $(date +%s) - RESTART_STARTED ))s; waiting for /readyz"

RECOVERED=""
for _ in $(seq 1 "$((RECOVERY_TIMEOUT_SECS * 2))"); do
  if [ "$(ready_code)" = "200" ]; then RECOVERED="$(( $(date +%s) - RESTART_STARTED ))"; break; fi
  sleep 0.5
done
if [ -n "$RECOVERED" ]; then
  note "ok: /readyz 200 again ${RECOVERED}s after the restart began"
else
  fail "/readyz did not return to 200 within ${RECOVERY_TIMEOUT_SECS}s of the restart"
fi

scrape > "$SOAK_LOG_DIR/metrics-after-restart.txt"
RECONNECTS_AFTER="$(metric "$SOAK_LOG_DIR/metrics-after-restart.txt" orbitcast_pubsub_reconnects_total)"
INTEREST_AFTER="$(metric "$SOAK_LOG_DIR/metrics-after-restart.txt" orbitcast_streams_with_interest)"
assert_gt "listener reconnects" "$RECONNECTS_AFTER" "$IDLE_RECONNECTS"
assert_eq "streams with interest after reconnect (re-LISTEN kept the interest set)" "$RUN_INTEREST" "$INTEREST_AFTER"

# Publications must flow through the re-established LISTENs: give the
# generator a few seconds past recovery, then compare the delivery counter.
sleep 5
scrape > "$SOAK_LOG_DIR/metrics-resumed.txt"
RECEIVED_RESUMED="$(metric "$SOAK_LOG_DIR/metrics-resumed.txt" orbitcast_pubsub_received_total)"
RECEIVED_AT_RECOVERY="$(metric "$SOAK_LOG_DIR/metrics-after-restart.txt" orbitcast_pubsub_received_total)"
assert_gt "publications received after the restart (re-LISTEN delivers)" "$RECEIVED_RESUMED" "$RECEIVED_AT_RECOVERY"

note "waiting for the generator to finish"
GEN_STATUS=0
wait "$GEN_PID" || GEN_STATUS=$?
GEN_PID=""
cat "$SOAK_LOG_DIR/fanout.txt"
if [ "$GEN_STATUS" -ne 0 ]; then
  fail "generator exited $GEN_STATUS"
  cat "$SOAK_LOG_DIR/fanout.err" >&2
fi
grep -E "^(client|publish)" "$SOAK_LOG_DIR/fanout.err" | head -20 >&2 || true

if grep -q "re-LISTEN" "$RELAY_LOG" && grep "re-LISTEN" "$RELAY_LOG" | grep -q "failed"; then
  fail "relay log reports a failed re-LISTEN"
else
  note "ok: no failed re-LISTEN in the relay log"
fi

# Idle again: every count must be back where it started.
sleep "$((SETTLE_MS / 1000 + 2))"
scrape > "$SOAK_LOG_DIR/metrics-after.txt"
assert_eq "connections after the run" "$IDLE_CONNECTIONS" "$(metric "$SOAK_LOG_DIR/metrics-after.txt" orbitcast_connections_current)"
assert_eq "subscriptions after the run" "$IDLE_SUBSCRIPTIONS" "$(metric "$SOAK_LOG_DIR/metrics-after.txt" orbitcast_subscriptions_current)"
assert_eq "streams with interest (LISTENs) after the run" "$IDLE_INTEREST" "$(metric "$SOAK_LOG_DIR/metrics-after.txt" orbitcast_streams_with_interest)"
AFTER_THREADS="$(relay_threads)"
if [ "$IDLE_THREADS" != "n/a" ] && [ "$AFTER_THREADS" -gt "$IDLE_THREADS" ]; then
  fail "relay thread count grew $IDLE_THREADS -> $AFTER_THREADS"
else
  note "ok: relay threads $IDLE_THREADS -> $AFTER_THREADS"
fi
assert_eq "/readyz at the end" "200" "$(ready_code)"
for family in orbitcast_pubsub_dropped_total orbitcast_pubsub_overflow_gaps_total; do
  grep -E "^${family}" "$SOAK_LOG_DIR/metrics-after.txt" >&2 || true
done
report_relay_footprint

if [ "${#FAILURES[@]}" -gt 0 ]; then
  echo "soak FAILED: ${#FAILURES[@]} assertion(s)" >&2
  printf '  - %s\n' "${FAILURES[@]}" >&2
  exit 1
fi
echo "soak OK: relay reconnected, re-LISTENed, and returned to idle counts" >&2
