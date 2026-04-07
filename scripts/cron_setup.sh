#!/bin/bash
# =============================================================================
# AlphaScout Cron Setup
# =============================================================================
# Installs system crontab entries for:
#   1. API server auto-start on reboot + watchdog every 5 min
#   2. Daily data refresh (22:00 UTC) — pipeline + macro + universe sync
#   3. Daily deployment evaluation (22:30 UTC, Mon-Fri)
#
# Run: bash scripts/cron_setup.sh
# View: crontab -l
# Remove: crontab -r
# =============================================================================

set -euo pipefail

# Auto-detect workspace root from this script's location
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE="$(cd "$SCRIPT_DIR/.." && pwd)"
SCRIPTS="$WORKSPACE/scripts"
SERVER="$WORKSPACE/server"
LOGS="$WORKSPACE/logs"
PYTHON="/usr/bin/python3"

# Create logs directory
mkdir -p "$LOGS"

# Build the crontab
CRON=$(cat <<'CRONTAB'
# =============================================================================
# AlphaScout Crons
# =============================================================================

# --- API Server ---

# Start API server on reboot
@reboot cd SERVER_DIR && bash start.sh >> LOGS_DIR/api_watchdog.log 2>&1

# Every 5 min — restart API if it's down (silent when healthy)
*/5 * * * * curl -sf http://localhost:8090/health > /dev/null 2>&1 || (cd SERVER_DIR && bash start.sh >> LOGS_DIR/api_watchdog.log 2>&1)

# --- Daily Data Refresh ---

# 22:00 UTC — Data refresh: prices, fundamentals, earnings, analyst, macro
0 22 * * * cd SCRIPTS_DIR && PYTHON_BIN pipeline.py --refresh >> LOGS_DIR/pipeline.log 2>&1

# 22:10 UTC — Macro data refresh (FRED series, derived indicators)
10 22 * * * cd SCRIPTS_DIR && PYTHON_BIN macro_data.py daily >> LOGS_DIR/macro.log 2>&1

# 22:25 UTC — Sync universe profiles into DB (after pipeline refreshes profile JSONs)
25 22 * * * cd SCRIPTS_DIR && PYTHON_BIN -c "import sys; sys.path.insert(0,'../server'); from api import _sync_universe_profiles; n=_sync_universe_profiles(); print(f'{n} profiles synced')" >> LOGS_DIR/universe_sync.log 2>&1

# --- Deployment Evaluation ---

# 22:30 UTC Mon-Fri — Evaluate all live deployments (strategies, portfolios, regimes)
30 22 * * 1-5 cd SCRIPTS_DIR && PYTHON_BIN deploy_engine.py evaluate >> LOGS_DIR/evaluate.log 2>&1

# --- Maintenance ---

# Weekly log rotation (Sunday 00:00) — truncate logs over 10MB
0 0 * * 0 find LOGS_DIR -name "*.log" -size +10M -exec truncate -s 0 {} \;
CRONTAB
)

# Replace placeholders with actual paths
CRON="${CRON//SCRIPTS_DIR/$SCRIPTS}"
CRON="${CRON//SERVER_DIR/$SERVER}"
CRON="${CRON//PYTHON_BIN/$PYTHON}"
CRON="${CRON//LOGS_DIR/$LOGS}"

# Install
echo "$CRON" | crontab -

echo "Crontab installed:"
echo ""
crontab -l
echo ""
echo "Logs: $LOGS/"
echo "  api_watchdog.log   — API server start/restart events"
echo "  pipeline.log       — data refresh"
echo "  macro.log          — macro/FRED refresh"
echo "  universe_sync.log  — universe DB sync"
echo "  evaluate.log       — deployment evaluation"
