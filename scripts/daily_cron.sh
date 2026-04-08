#!/bin/bash
# =============================================================================
# AlphaScout Daily Pipeline
# =============================================================================
# Runs all daily tasks in order:
#   1. Refresh market data (prices, fundamentals, earnings)
#   2. Refresh macro data (FRED series)
#   3. Sync universe profiles into DB
#   4. Evaluate all live deployments
#
# Usage:
#   bash scripts/daily_cron.sh           # run everything
#   bash scripts/daily_cron.sh evaluate  # just evaluate deployments
#
# Installed by: scripts/install_crons.sh
# =============================================================================

set -euo pipefail

# Resolve repo root (parent of scripts/)
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPTS_DIR="$REPO_ROOT/scripts"
SERVER_DIR="$REPO_ROOT/server"
LOG_DIR="$REPO_ROOT/logs"

# Load secrets and config from .env (gitignored)
if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    source "$REPO_ROOT/.env"
    set +a
fi

export WORKSPACE="${WORKSPACE:-$REPO_ROOT}"
export DB_PATH="${DB_PATH:-$REPO_ROOT/data/alphascout.db}"
export DATA_DIR="${DATA_DIR:-$REPO_ROOT/data}"

mkdir -p "$LOG_DIR"

timestamp() { date -u "+%Y-%m-%d %H:%M:%S UTC"; }

log() { echo "$(timestamp) $1"; }

# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

run_data_refresh() {
    log "[1/2] Refreshing all data (prices, fundamentals, earnings, macro, news → JSON + DB)..."
    cd "$SCRIPTS_DIR"
    python3 daily_update.py 2>&1 || log "[1/2] WARNING: daily_update.py failed (continuing)"
}

run_evaluate() {
    log "[2/2] Evaluating live deployments..."
    cd "$SCRIPTS_DIR"
    python3 deploy_engine.py evaluate 2>&1 || log "[2/2] ERROR: deployment evaluation failed"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if [ "${1:-all}" = "evaluate" ]; then
    # Just evaluate (useful for manual re-runs)
    run_evaluate
elif [ "${1:-all}" = "data" ]; then
    # Just refresh data (useful for manual re-runs)
    run_data_refresh
else
    log "========== AlphaScout Daily Pipeline Start =========="
    run_data_refresh
    run_evaluate
    log "========== AlphaScout Daily Pipeline Complete =========="
fi
