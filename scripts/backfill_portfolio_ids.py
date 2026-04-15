#!/usr/bin/env python3
"""
Backfill portfolio_id on existing experiments and deployments.

For each experiment with a portfolio_config blob but no portfolio_id:
  - Compute portfolio_id hash
  - INSERT OR IGNORE into portfolios table
  - UPDATE experiment.portfolio_id = computed hash

Same for deployments using config_json.

Idempotent — safe to run multiple times.

Usage:
    set -a && source .env && set +a && python3 scripts/backfill_portfolio_ids.py
"""
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent))

from db_config import APP_DB_PATH
from portfolio_engine import compute_portfolio_id


def backfill():
    conn = sqlite3.connect(str(APP_DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    # --- Experiments ---
    print("\n=== Backfilling experiments ===")
    rows = cursor.execute("""
        SELECT id, portfolio_config FROM experiments
        WHERE portfolio_id IS NULL AND portfolio_config IS NOT NULL AND portfolio_config != ''
    """).fetchall()
    print(f"  Found {len(rows)} experiments needing backfill")

    updated_exp = 0
    skipped_exp = 0
    for row in rows:
        try:
            config = json.loads(row["portfolio_config"])
            if not config or not isinstance(config, dict):
                skipped_exp += 1
                continue
            # Need at least name + sleeves/strategies to be a valid portfolio
            if "sleeves" not in config and "strategies" not in config:
                skipped_exp += 1
                continue
            pid = compute_portfolio_id(config)
            # Save portfolio (idempotent)
            cursor.execute(
                "INSERT OR IGNORE INTO portfolios (portfolio_id, name, config, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (pid, config.get("name", "Unnamed"), json.dumps(config), now, now),
            )
            # Link experiment
            cursor.execute("UPDATE experiments SET portfolio_id = ? WHERE id = ?", (pid, row["id"]))
            updated_exp += 1
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"    Skipping {row['id']}: {e}")
            skipped_exp += 1

    print(f"  Updated: {updated_exp}, skipped: {skipped_exp}")

    # --- Deployments ---
    print("\n=== Backfilling deployments ===")
    rows = cursor.execute("""
        SELECT id, config_json FROM deployments
        WHERE portfolio_id IS NULL AND config_json IS NOT NULL AND config_json != ''
    """).fetchall()
    print(f"  Found {len(rows)} deployments needing backfill")

    updated_dep = 0
    skipped_dep = 0
    for row in rows:
        try:
            config = json.loads(row["config_json"])
            if not config or not isinstance(config, dict):
                skipped_dep += 1
                continue
            if "sleeves" not in config and "strategies" not in config:
                skipped_dep += 1
                continue
            pid = compute_portfolio_id(config)
            cursor.execute(
                "INSERT OR IGNORE INTO portfolios (portfolio_id, name, config, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (pid, config.get("name", "Unnamed"), json.dumps(config), now, now),
            )
            cursor.execute("UPDATE deployments SET portfolio_id = ? WHERE id = ?", (pid, row["id"]))
            updated_dep += 1
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"    Skipping {row['id']}: {e}")
            skipped_dep += 1

    print(f"  Updated: {updated_dep}, skipped: {skipped_dep}")

    conn.commit()

    # Summary
    print("\n=== Summary ===")
    total_portfolios = cursor.execute("SELECT COUNT(*) FROM portfolios").fetchone()[0]
    linked_exp = cursor.execute("SELECT COUNT(*) FROM experiments WHERE portfolio_id IS NOT NULL").fetchone()[0]
    unlinked_exp = cursor.execute("SELECT COUNT(*) FROM experiments WHERE portfolio_id IS NULL").fetchone()[0]
    linked_dep = cursor.execute("SELECT COUNT(*) FROM deployments WHERE portfolio_id IS NOT NULL").fetchone()[0]
    unlinked_dep = cursor.execute("SELECT COUNT(*) FROM deployments WHERE portfolio_id IS NULL").fetchone()[0]
    print(f"  Portfolios:  {total_portfolios}")
    print(f"  Experiments: {linked_exp} linked, {unlinked_exp} unlinked")
    print(f"  Deployments: {linked_dep} linked, {unlinked_dep} unlinked")

    conn.close()


if __name__ == "__main__":
    backfill()
