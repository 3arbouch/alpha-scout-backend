#!/usr/bin/env python3
"""One-off backfill: regenerate all trade_alerts with enriched data (signal_detail, pnl, days_held).

1. Clears trade_alerts + trade_executions tables
2. Re-evaluates all active deployments (refreshes results.json with signal_detail)
3. Generates alerts for ALL historical trades (not just today)
"""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from db_config import APP_DB_PATH as DB_PATH
DEPLOYMENTS_DIR = Path(__file__).parent.parent / "deployments"


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def generate_alerts_all_dates(conn, deploy_id: str, trades: list, sleeve_name: str = None):
    """Insert alerts for ALL trades, not just today."""
    now = datetime.now(timezone.utc).isoformat()
    count = 0

    for trade in trades:
        date = trade.get("date")
        if not date:
            continue

        prefix = f"{sleeve_name}:" if sleeve_name else ""
        alert_id = hashlib.md5(
            f"{deploy_id}:{date}:{prefix}{trade['symbol']}:{trade['action']}".encode()
        ).hexdigest()[:12]

        reason = trade.get("reason", "")
        if sleeve_name and not reason.startswith(f"[{sleeve_name}]"):
            reason = f"[{sleeve_name}] {reason}"

        sig_detail = trade.get("signal_detail")
        sig_detail_json = json.dumps(sig_detail) if sig_detail else None

        conn.execute(
            """INSERT OR IGNORE INTO trade_alerts
               (id, deployment_id, date, action, symbol, shares, target_price,
                amount, reason, signal_detail, entry_date, entry_price,
                pnl_pct, pnl, days_held, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (alert_id, deploy_id, date, trade["action"], trade["symbol"],
             trade["shares"], trade["price"], trade.get("amount"),
             reason, sig_detail_json, trade.get("entry_date"),
             trade.get("entry_price"), trade.get("pnl_pct"),
             trade.get("pnl"), trade.get("days_held"), now),
        )

        # Create pending execution record
        exec_id = hashlib.md5(f"exec:{alert_id}".encode()).hexdigest()[:12]
        conn.execute(
            """INSERT OR IGNORE INTO trade_executions
               (id, alert_id, status, updated_at)
               VALUES (?, ?, 'pending', ?)""",
            (exec_id, alert_id, now),
        )
        count += 1

    return count


def main():
    conn = get_db()

    # Step 1: Clear existing alerts
    print("Clearing trade_alerts and trade_executions...")
    conn.execute("DELETE FROM trade_executions")
    conn.execute("DELETE FROM trade_alerts")
    conn.commit()
    print("  ✓ Cleared")

    # Step 2: Re-evaluate all active strategy deployments
    print("\n=== Re-evaluating strategy deployments ===")
    from deploy_engine import evaluate_one, evaluate_portfolio_one

    strategy_rows = conn.execute(
        "SELECT id, strategy_name, alert_mode FROM deployed_strategies WHERE status = 'active'"
    ).fetchall()

    for row in strategy_rows:
        deploy_id = row["id"]
        name = row["strategy_name"]
        alert_on = bool(row["alert_mode"])
        print(f"\n{'📢' if alert_on else '⚪'} {name} ({deploy_id})")

        # Re-evaluate (this updates results.json with signal_detail)
        result = evaluate_one(deploy_id)
        if not result:
            print(f"  ✗ Evaluation failed")
            continue

        if not alert_on:
            print(f"  ⏭ Alerts not enabled, skipping alert generation")
            continue

        # Generate alerts for ALL trades
        trades = result.get("trades", [])
        count = generate_alerts_all_dates(conn, deploy_id, trades)
        conn.commit()
        print(f"  ✓ {count} alerts generated ({len([t for t in trades if t['action']=='BUY'])} BUY, {len([t for t in trades if t['action']=='SELL'])} SELL)")

    # Step 3: Re-evaluate all active portfolio deployments
    print("\n=== Re-evaluating portfolio deployments ===")
    portfolio_rows = conn.execute(
        "SELECT id, portfolio_name, alert_mode FROM portfolio_deployments WHERE status = 'active'"
    ).fetchall()

    for row in portfolio_rows:
        deploy_id = row["id"]
        name = row["portfolio_name"]
        alert_on = bool(row["alert_mode"])
        print(f"\n{'📢' if alert_on else '⚪'} {name} ({deploy_id})")

        result = evaluate_portfolio_one(deploy_id)
        if not result:
            print(f"  ✗ Evaluation failed")
            continue

        if not alert_on:
            print(f"  ⏭ Alerts not enabled, skipping alert generation")
            continue

        # Generate alerts from each sleeve
        sleeve_results = result.get("sleeve_results", [])
        sleeves = result.get("per_sleeve", [])
        total = 0
        for i, sr in enumerate(sleeve_results):
            sleeve_name = sleeves[i].get("label") or sleeves[i].get("name", f"sleeve_{i}") if i < len(sleeves) else f"sleeve_{i}"
            trades = sr.get("trades", [])
            count = generate_alerts_all_dates(conn, deploy_id, trades, sleeve_name=sleeve_name)
            total += count
        conn.commit()
        print(f"  ✓ {total} alerts generated across {len(sleeve_results)} sleeves")

    # Summary
    total_alerts = conn.execute("SELECT COUNT(*) FROM trade_alerts").fetchone()[0]
    total_with_detail = conn.execute("SELECT COUNT(*) FROM trade_alerts WHERE signal_detail IS NOT NULL").fetchone()[0]
    total_buys = conn.execute("SELECT COUNT(*) FROM trade_alerts WHERE action = 'BUY'").fetchone()[0]
    total_sells = conn.execute("SELECT COUNT(*) FROM trade_alerts WHERE action = 'SELL'").fetchone()[0]

    print(f"\n{'='*50}")
    print(f"BACKFILL COMPLETE")
    print(f"  Total alerts: {total_alerts} ({total_buys} BUY, {total_sells} SELL)")
    print(f"  With signal_detail: {total_with_detail}")
    print(f"  Without signal_detail: {total_alerts - total_with_detail}")
    print(f"{'='*50}")

    conn.close()


if __name__ == "__main__":
    main()
