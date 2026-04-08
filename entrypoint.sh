#!/bin/bash
set -e

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"

export WORKSPACE="${WORKSPACE:-$REPO_ROOT}"
export DB_PATH="${DB_PATH:-$REPO_ROOT/data/alphascout.db}"
export DATA_DIR="${DATA_DIR:-$REPO_ROOT/data}"

# Install cron jobs if running in production
if [ "${ENABLE_CRON:-true}" = "true" ]; then
    DAILY_SCRIPT="$REPO_ROOT/scripts/daily_cron.sh"
    LOG_FILE="$REPO_ROOT/logs/daily_pipeline.log"
    chmod +x "$DAILY_SCRIPT"
    mkdir -p "$REPO_ROOT/logs"

    echo "0 22 * * 1-5 $DAILY_SCRIPT >> $LOG_FILE 2>&1" | crontab -
    cron
    echo "Cron installed: daily pipeline at 22:00 UTC Mon-Fri"
fi

# Start API server
echo "Starting AlphaScout API on port 8090..."
cd "$REPO_ROOT/server"
exec python3 -u api.py
