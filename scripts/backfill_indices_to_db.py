#!/usr/bin/env python3
"""
Backfill indices (^GSPC, ^DJI, ^IXIC) from JSON into the prices table.

Historical index data is stored in data/prices/indices/*.json but was
never synced to the prices DB table. This script populates them under
caret-less symbols (GSPC, DJI, IXIC).

Idempotent via INSERT OR REPLACE. Safe to run multiple times.

Usage:
    set -a && source .env && set +a && python3 scripts/backfill_indices_to_db.py
"""
import sys
import json
import sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from db_config import MARKET_DB_PATH

# Allow DATA_DIR override via env (dev repo has no data/, prod repo does)
import os
_default_data_dir = Path(__file__).parent.parent / "data"
DATA_DIR = Path(os.environ.get("DATA_DIR", str(_default_data_dir)))


def map_prices(symbol, data):
    """Same mapper as daily_update.py."""
    rows = []
    for r in data:
        rows.append((
            symbol, r.get("date"),
            r.get("open"), r.get("high"), r.get("low"), r.get("close"),
            r.get("volume"), r.get("changePercent"), r.get("vwap"),
        ))
    return rows


def backfill():
    indices_dir = DATA_DIR / "prices" / "indices"
    if not indices_dir.exists():
        print(f"No indices dir at {indices_dir}")
        return

    conn = sqlite3.connect(str(MARKET_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    insert = ("INSERT OR REPLACE INTO prices "
              "(symbol,date,open,high,low,close,volume,change_pct,vwap) "
              "VALUES (?,?,?,?,?,?,?,?,?)")

    total = 0
    for idx_file in sorted(indices_dir.glob("*.json")):
        safe = idx_file.stem  # GSPC, DJI, IXIC
        try:
            content = json.loads(idx_file.read_text())
            data = content.get("data") if isinstance(content, dict) else content
        except (json.JSONDecodeError, OSError) as e:
            print(f"  {safe}: skip — {e}")
            continue

        if not isinstance(data, list) or not data:
            print(f"  {safe}: no data")
            continue

        rows = map_prices(safe, data)
        conn.executemany(insert, rows)
        conn.commit()

        latest = max((r[1] for r in rows if r[1]), default="?")
        print(f"  {safe}: {len(rows)} rows inserted, latest={latest}")
        total += len(rows)

    print(f"\nTotal: {total} rows upserted")

    # Verify
    for safe in ["GSPC", "DJI", "IXIC"]:
        r = conn.execute("SELECT MAX(date), COUNT(*) FROM prices WHERE symbol = ?", (safe,)).fetchone()
        print(f"  {safe}: latest={r[0]}, rows={r[1]}")

    conn.close()


if __name__ == "__main__":
    backfill()
