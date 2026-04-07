#!/bin/bash
set -e

# Install cron jobs if running in production
if [ "${ENABLE_CRON:-true}" = "true" ]; then
    # Build crontab inline (no bash variable expansion issues)
    cat > /tmp/alphascout-cron <<'EOF'
# AlphaScout Daily Crons

# 22:00 UTC — Data refresh
0 22 * * * cd /app/scripts && python3 pipeline.py --refresh >> /app/logs/pipeline.log 2>&1

# 22:10 UTC — Macro data refresh
10 22 * * * cd /app/scripts && python3 macro_data.py daily >> /app/logs/macro.log 2>&1

# 22:25 UTC — Sync universe profiles into DB
25 22 * * * cd /app/server && python3 -c "from api import _sync_universe_profiles; print(f'{_sync_universe_profiles()} profiles synced')" >> /app/logs/universe_sync.log 2>&1

# 22:30 UTC Mon-Fri — Evaluate live deployments
30 22 * * 1-5 cd /app/scripts && python3 deploy_engine.py evaluate >> /app/logs/evaluate.log 2>&1

# Weekly log rotation
0 0 * * 0 find /app/logs -name "*.log" -size +10M -exec truncate -s 0 {} \;
EOF
    crontab /tmp/alphascout-cron
    rm /tmp/alphascout-cron
    cron
    echo "Cron jobs installed and daemon started"
fi

# Start API server
echo "Starting AlphaScout API on port 8080..."
cd /app/server
exec python3 -u api.py
