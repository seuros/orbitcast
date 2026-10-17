#!/usr/bin/env bash
#
# Fanout benchmark: N subscribed clients on one public stream, M publications
# through the private HTTP ingress, reporting p50/p95/p99 client-receive
# latency and throughput.
#
#   bench/ws_fanout.sh                      # defaults: 50 clients, 100 messages
#   CLIENTS=1000 MESSAGES=500 bench/ws_fanout.sh
#   PAYLOAD_BYTES=32768 bench/ws_fanout.sh  # Turbo-sized payloads
#
# The relay is started by this script (memory backend by default, so no
# database is involved) and stopped on exit. To measure against PostgreSQL,
# set BACKEND=postgres and DATABASE_URL.
#
# Every number this prints is a measurement of *one machine on one run*. It is
# not a capacity claim.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLIENTS="${CLIENTS:-50}"
MESSAGES="${MESSAGES:-100}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-0}"
PUBLISH_INTERVAL_MS="${PUBLISH_INTERVAL_MS:-2}"
SETTLE_MS="${SETTLE_MS:-2000}"
STREAMS="${STREAMS:-1}"
# DURATION_SECS publishes for a duration instead of MESSAGES; IDLE_TIMEOUT_MS
# is how long a client waits in silence before giving up (both optional).
DURATION_SECS="${DURATION_SECS:-}"
IDLE_TIMEOUT_MS="${IDLE_TIMEOUT_MS:-}"

BACKEND="${BACKEND:-memory}"
ADDR="${ADDR:-127.0.0.1:28080}"
INGRESS_ADDR="${INGRESS_ADDR:-127.0.0.1:28081}"
NAMESPACE="${NAMESPACE:-bench}"
TOKEN="${TOKEN:-bench-token}"
STREAM="${STREAM:-bench:fanout}"

# shellcheck source=bench/lib.sh
source "$ROOT/bench/lib.sh"

build_relay "$BACKEND"
build_generator
start_relay
trap stop_relay EXIT

echo "# fanout: clients=$CLIENTS streams=$STREAMS messages=$MESSAGES duration_secs=${DURATION_SECS:-n/a} payload_bytes=$PAYLOAD_BYTES backend=$BACKEND" >&2

# Optional generator flags, appended only when set.
extra_args=()
[ -n "$DURATION_SECS" ] && extra_args+=(--duration-secs "$DURATION_SECS")
[ -n "$IDLE_TIMEOUT_MS" ] && extra_args+=(--idle-timeout-ms "$IDLE_TIMEOUT_MS")

"$GENERATOR" fanout \
  --url "ws://$ADDR/cable" \
  --ingress "http://$INGRESS_ADDR/v1/publications" \
  --token "$TOKEN" \
  --namespace "$NAMESPACE" \
  --stream "$STREAM" \
  --streams "$STREAMS" \
  --clients "$CLIENTS" \
  --messages "$MESSAGES" \
  --payload-bytes "$PAYLOAD_BYTES" \
  --publish-interval-ms "$PUBLISH_INTERVAL_MS" \
  --settle-ms "$SETTLE_MS" \
  ${extra_args[@]+"${extra_args[@]}"}

report_relay_footprint
report_relay_metrics
