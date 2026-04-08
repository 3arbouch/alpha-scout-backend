#!/bin/bash
# =============================================================================
# Install AlphaScout cron jobs
# =============================================================================
# Run once: bash scripts/install_crons.sh
#
# Installs a crontab entry that runs the daily pipeline at 22:00 UTC (Mon-Fri).
# All paths are relative to this repo — no hardcoded /app or ~/.openclaw paths.
# =============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DAILY_SCRIPT="$REPO_ROOT/scripts/daily_cron.sh"
LOG_FILE="$REPO_ROOT/logs/daily_pipeline.log"

chmod +x "$DAILY_SCRIPT"
mkdir -p "$REPO_ROOT/logs"

# Build the new crontab entry
CRON_LINE="0 22 * * 1-5 $DAILY_SCRIPT >> $LOG_FILE 2>&1"
CRON_MARKER="# alphascout-daily-pipeline"

# Remove any existing AlphaScout cron entries, then add the new one
(crontab -l 2>/dev/null | grep -v "$CRON_MARKER" | grep -v "alphascout\|deploy_engine\|pipeline\.py.*refresh\|macro_data\.py.*daily\|universe_sync\|api_watchdog"; echo "$CRON_LINE $CRON_MARKER") | crontab -

echo "Cron installed:"
echo "  Schedule: 22:00 UTC Mon-Fri"
echo "  Script:   $DAILY_SCRIPT"
echo "  Log:      $LOG_FILE"
echo ""
echo "Verify with: crontab -l"
echo "Test with:   bash $DAILY_SCRIPT evaluate"
