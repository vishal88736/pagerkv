#!/usr/bin/env bash
# Start Node B (replica) on 8002 and Node A (primary) on 8001.
# Run from anywhere; paths are resolved relative to this script.
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python}"

"$PYTHON" -m uvicorn node_b.app:app --host 127.0.0.1 --port 8002 &
B_PID=$!

"$PYTHON" -m uvicorn node_a.app:app --host 127.0.0.1 --port 8001 &
A_PID=$!

cleanup() {
    kill "$A_PID" "$B_PID" 2>/dev/null || true
    wait "$A_PID" "$B_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Node B (replica) on http://127.0.0.1:8002"
echo "Node A (primary) on http://127.0.0.1:8001"
wait -n "$B_PID" "$A_PID"
