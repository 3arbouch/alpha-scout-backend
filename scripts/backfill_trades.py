#!/usr/bin/env python3
"""Backfill the unified trades table from existing deployments and backtests."""

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from deploy_engine import persist_trades, get_db, DB_PATH, DEPLOYMENTS_DIR

WORKSPACE = Path(__file__).parent.parent


def backfill_deployments():
    """Backfill from deployment results.json files."""
    conn = get_db()
    total = 0

    # Strategy deployments
    rows = conn.execute("SELECT id FROM deployed_strategies").fetchall()
    for row in rows:
        did = row["id"]
        rpath = DEPLOYMENTS_DIR / did / "results.json"
        if not rpath.exists():
            continue
        try:
            data = json.loads(rpath.read_text())
            trades = data.get("trades", [])
            if trades:
                n = persist_trades("deployment", did, trades,
                                   deployment_type="strategy", conn=conn)
                total += n
                print(f"  {did}: {n}/{len(trades)} trades")
        except Exception as e:
            print(f"  {did}: ERROR {e}")

    # Portfolio deployments
    rows = conn.execute("SELECT id FROM portfolio_deployments").fetchall()
    for row in rows:
        did = row["id"]
        rpath = DEPLOYMENTS_DIR / did / "results.json"
        if not rpath.exists():
            continue
        try:
            data = json.loads(rpath.read_text())
            sleeve_results = data.get("sleeve_results", [])
            per_sleeve = data.get("per_sleeve", [])
            for i, sr in enumerate(sleeve_results):
                label = per_sleeve[i].get("label") if i < len(per_sleeve) else sr.get("strategy", f"sleeve_{i}")
                trades = sr.get("trades", [])
                if trades:
                    n = persist_trades("deployment", did, trades,
                                       deployment_type="portfolio",
                                       sleeve_label=label, conn=conn)
                    total += n
                    print(f"  {did}/{label}: {n}/{len(trades)} trades")
        except Exception as e:
            print(f"  {did}: ERROR {e}")

    conn.close()
    return total


def backfill_backtests():
    """Backfill from backtest results files."""
    conn = get_db()
    total = 0

    # Strategy backtests
    rows = conn.execute("SELECT run_id FROM backtest_runs").fetchall()
    results_dir = WORKSPACE / "backtest" / "results"
    for row in rows:
        rid = row["run_id"]
        rpath = results_dir / f"{rid}.json"
        if not rpath.exists():
            continue
        try:
            data = json.loads(rpath.read_text())
            trades = data.get("trades", [])
            if trades:
                n = persist_trades("backtest", rid, trades, conn=conn)
                total += n
                print(f"  {rid}: {n}/{len(trades)} trades")
        except Exception as e:
            print(f"  {rid}: ERROR {e}")

    # Portfolio backtests
    rows = conn.execute("SELECT run_id, results_path FROM portfolio_backtest_runs").fetchall()
    for row in rows:
        rid = row["run_id"]
        rpath = WORKSPACE / row["results_path"] if row["results_path"] else None
        if not rpath or not rpath.exists():
            continue
        try:
            data = json.loads(rpath.read_text())
            for sleeve in data.get("per_sleeve", []):
                trades = sleeve.get("trades", [])
                if trades:
                    n = persist_trades("backtest", rid, trades,
                                       deployment_type="portfolio",
                                       sleeve_label=sleeve.get("label"), conn=conn)
                    total += n
                    print(f"  {rid}/{sleeve.get('label')}: {n}/{len(trades)} trades")
        except Exception as e:
            print(f"  {rid}: ERROR {e}")

    conn.close()
    return total


if __name__ == "__main__":
    print("=== Backfilling deployment trades ===")
    d = backfill_deployments()
    print(f"\nDeployment trades: {d}")

    print("\n=== Backfilling backtest trades ===")
    b = backfill_backtests()
    print(f"\nBacktest trades: {b}")

    print(f"\n=== Total: {d + b} trades backfilled ===")

    # Summary
    conn = sqlite3.connect(str(DB_PATH))
    count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    by_source = conn.execute("SELECT source_type, COUNT(*) FROM trades GROUP BY source_type").fetchall()
    conn.close()
    print(f"\nTrades table total: {count}")
    for row in by_source:
        print(f"  {row[0]}: {row[1]}")
