#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB="${1:-./stress-save-64w}"
SNAP="${2:-0}"

SCRIPT_DIR_PARENT="$(dirname "$SCRIPT_DIR")"
echo "Converting $DB to B-tree..."
"$SCRIPT_DIR_PARENT/target/release/bxdb-convert" to-btree "$DB"

echo "Starting 64 timing-serve processes against $DB (snapshot $SNAP)..."

pids=()
for i in $(seq 0 63); do
    taskset -c 0-63 "$SCRIPT_DIR/timing-serve" "$DB" "$SNAP" "$i" &
    pids+=($!)
done

for pid in "${pids[@]}"; do
    wait "$pid"
done

echo "All 64 processes done. CSV files: timing_latencies_*.csv"
