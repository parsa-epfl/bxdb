#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB="${1:-./stress-save-64w}"
SNAP="${2:-0}"
BTREE_NAME="${DB}"

SCRIPT_DIR_PARENT="$(dirname "$SCRIPT_DIR")"

# Build the timing-serve binary (needs -lunwind for Rust FFI unwinding support).
echo "Building timing-serve..."
zig build-exe "$SCRIPT_DIR/timing-serve.zig" "$SCRIPT_DIR_PARENT/target/release/libbxdb.so" \
    -I "$SCRIPT_DIR" -lunwind -lc -lpthread -ldl -lm \
    -femit-bin="$SCRIPT_DIR/timing-serve"

echo "Converting $DB to B-tree..."
"$SCRIPT_DIR_PARENT/target/release/bxdb" convert to-btree "$DB"

echo "--- Attempting to read WITHOUT cache (should fail) ---"
if "$SCRIPT_DIR/timing-serve" "$BTREE_NAME" "$SNAP" 0 2>&1; then
    echo "ERROR: read succeeded without cache — unexpected"
    exit 1
else
    echo "Read failed as expected (no cache)."
fi

echo ""
echo "Creating shared-memory page cache..."
"$SCRIPT_DIR_PARENT/target/release/bxdb" cache create "$BTREE_NAME"

echo "Starting 64 timing-serve processes against $BTREE_NAME (snapshot $SNAP)..."

pids=()
for i in $(seq 0 63); do
    taskset -c 0-63 "$SCRIPT_DIR/timing-serve" "$BTREE_NAME" "$SNAP" "$i" &
    pids+=($!)
done

for pid in "${pids[@]}"; do
    wait "$pid"
done

echo "All 64 processes done. CSV files: timing_latencies_*.csv"

echo ""
echo "Cleaning up cache..."
"$SCRIPT_DIR_PARENT/target/release/bxdb" cache delete "$BTREE_NAME"

echo ""
echo "Done."
