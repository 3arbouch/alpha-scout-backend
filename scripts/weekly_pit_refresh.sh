#!/bin/bash
# =============================================================================
# AlphaScout Weekly PIT Refresh
# =============================================================================
# Pulls the latest historical-constituent change log for S&P 500 / NASDAQ-100 /
# DJIA and the delisted-companies catalog from FMP, and refreshes the
# universe_profiles ISIN/CUSIP for all live tickers.
#
# Why weekly: index membership changes are rare (a few times a month) and
# ISINs are immutable per issuer. Once a week is sufficient to keep PIT
# backtests accurate. Running daily would be wasteful FMP traffic.
#
# Scheduled via cron — typically Sunday 21:00 UTC, an hour before the daily
# cron's next prod run (Mon 22:00 UTC) so the fresh data is in place before
# the next live-trading evaluation.
#
# Idempotent. Safe to re-run.
# =============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPTS_DIR="$REPO_ROOT/scripts"
LOG_DIR="$REPO_ROOT/logs"

if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    source "$REPO_ROOT/.env"
    set +a
fi

mkdir -p "$LOG_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
log() { echo "[$(date -u +%H:%M:%S)] $*"; }

log "=========================================================================="
log "weekly_pit_refresh start"
log "  REPO_ROOT=$REPO_ROOT"
log "  MARKET_DB_PATH=${MARKET_DB_PATH:-default}"
log "=========================================================================="

cd "$SCRIPTS_DIR"

log "[1/3] Refreshing historical index membership log (sp500/nasdaq/dowjones)..."
python3 -c "
import sqlite3
from db_config import MARKET_DB_PATH
from universe_history import ingest_index_history
conn = sqlite3.connect(MARKET_DB_PATH)
result = ingest_index_history(conn)
print(f'  rows written: {result}')
" || log "WARNING: index_history ingest failed (continuing)"

log "[2/3] Refreshing delisted-companies catalog..."
python3 -c "
import sqlite3
from db_config import MARKET_DB_PATH
from universe_history import ingest_delisted_catalog
conn = sqlite3.connect(MARKET_DB_PATH)
n = ingest_delisted_catalog(conn, max_pages=50)
print(f'  delisted rows touched: {n}')
" || log "WARNING: delisted_catalog ingest failed (continuing)"

log "[3/3] Refreshing ISIN/CUSIP for symbols missing them (only-missing mode)..."
python3 backfill_isin.py --only-missing || log "WARNING: isin backfill failed (continuing)"

log "=========================================================================="
log "weekly_pit_refresh complete"
log "=========================================================================="
