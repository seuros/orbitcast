#!/usr/bin/env bash
#
# Churn benchmark: repeated connect / subscribe / disconnect cycles, reporting
# the completed rate and the relay's resident set size before and after.
#
#   bench/churn.sh                              # 8 workers x 100 cycles
#   CYCLES=1000 CONCURRENCY=32 bench/churn.sh
#
# The RSS delta is the point: after the churn the relay should return to
# roughly its idle footprint. A steadily growing "after" figure across runs is
# the leak this workload exists to catch.
#
# Not a capacity claim: one machine, one run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CYCLES="${CYCLES:-100}"
CONCURRENCY="${CONCURRENCY:-8}"

BACKEND="${BACKEND:-memory}"
ADDR="${ADDR:-127.0.0.1:28080}"
INGRESS_ADDR="${INGRESS_ADDR:-127.0.0.1:28081}"
NAMESPACE="${NAMESPACE:-bench}"
TOKEN="${TOKEN:-bench-token}"
STREAM="${STREAM:-bench:churn}"

# shellcheck source=bench/lib.sh
source "$ROOT/bench/lib.sh"

build_relay "$BACKEND"
build_generator
start_relay
trap stop_relay EXIT

echo "# churn: workers=$CONCURRENCY cycles=$CYCLES backend=$BACKEND" >&2
echo "# idle footprint before the run:" >&2
report_relay_footprint

"$GENERATOR" churn \
  --url "ws://$ADDR/cable" \
  --cycles "$CYCLES" \
  --concurrency "$CONCURRENCY" \
  --stream "$STREAM"

# Give teardown a moment to finish before reading the footprint back.
sleep 2
echo "# footprint after the run (compare with the idle figure above):" >&2
report_relay_footprint
report_relay_metrics
