#!/bin/bash
# Start AlphaScout API server
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$SCRIPT_DIR/.api.pid"
LOG_FILE="$SCRIPT_DIR/../logs/api.log"

# Load env from repo root
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)
fi

# Set defaults for paths
export WORKSPACE="${WORKSPACE:-$(cd "$SCRIPT_DIR/.." && pwd)}"
export DB_PATH="${DB_PATH:-$WORKSPACE/data/alphascout.db}"
export DATA_DIR="${DATA_DIR:-$WORKSPACE/data}"

# Check if already running
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "API server already running (PID $(cat "$PID_FILE"))"
    exit 0
fi

# Start
cd "$SCRIPT_DIR"
mkdir -p "$(dirname "$LOG_FILE")"
nohup python3 -u api.py > "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "API server started (PID $!, log: $LOG_FILE)"
