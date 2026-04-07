#!/usr/bin/env python3
"""
AlphaScout Deployment Engine
=============================
Deploy a backtested strategy for live paper-trading.

Deployment = re-run the backtest engine daily with end_date = today.
This script manages the deployment lifecycle:
  - Create deployments (from strategy config)
  - Evaluate all active deployments (daily cron)
  - Query deployment state

Storage:
  - `deployed_strategies` SQLite table for metadata
  - `deployments/{id}/latest.json` for full engine output
  - `deployments/{id}/config.json` for the strategy config

Usage:
    # Deploy a strategy
    python3 deploy_engine.py deploy strategies/tech_30pct_drop_abfb8975.json \
        --start 2026-03-15 --capital 100000

    # Evaluate all active deployments (run after daily data refresh)
    python3 deploy_engine.py evaluate

    # List deployments
    python3 deploy_engine.py list

    # Stop a deployment
    python3 deploy_engine.py stop <deployment_id>
"""

import os
import sys
import re
import json
import hashlib
import sqlite3
import shutil
import argparse
from pathlib import Path
from datetime import datetime, timezone

SCRIPT_DIR = Path(__file__).parent
WORKSPACE = Path(os.environ.get("WORKSPACE", "/app"))
DEPLOYMENTS_DIR = WORKSPACE / "deployments"
DB_PATH = Path(os.environ.get("DB_PATH", "/app/data/alphascout.db"))

sys.path.insert(0, str(SCRIPT_DIR))

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS deployed_strategies (
    id TEXT PRIMARY KEY,
    strategy_name TEXT NOT NULL,
    config_json TEXT NOT NULL,
    start_date TEXT NOT NULL,
    initial_capital REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',  -- active, paused, stopped
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_evaluated TEXT,
    last_nav REAL,
    last_return_pct REAL,
    total_trades INTEGER DEFAULT 0,
    open_positions INTEGER DEFAULT 0,
    error TEXT,
    last_alpha_pct REAL,
    last_benchmark_return_pct REAL,
    current_utilization_pct REAL,
    last_sharpe_ratio REAL,
    last_ann_volatility_pct REAL,
    peak_utilized_capital REAL,
    avg_utilized_capital REAL,
    utilization_pct REAL,
    return_on_utilized_capital_pct REAL,
    rolling_vol_30d_pct REAL,
    alert_mode INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS trade_alerts (
    id TEXT PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    date TEXT NOT NULL,
    action TEXT NOT NULL,          -- BUY or SELL
    symbol TEXT NOT NULL,
    shares REAL NOT NULL,
    target_price REAL NOT NULL,
    amount REAL,
    reason TEXT,
    signal_detail TEXT,            -- JSON: entry signal metadata (conditions that fired)
    entry_date TEXT,               -- for SELL: original entry date
    entry_price REAL,              -- for SELL: original entry price
    pnl_pct REAL,                  -- for SELL: paper P&L %
    pnl REAL,                      -- for SELL: paper P&L $
    days_held INTEGER,             -- for SELL: holding period in trading days
    created_at TEXT NOT NULL,
    FOREIGN KEY (deployment_id) REFERENCES deployed_strategies(id)
);

CREATE TABLE IF NOT EXISTS trade_executions (
    id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, executed, skipped
    fill_price REAL,
    fill_time TEXT,
    fill_shares REAL,
    broker TEXT DEFAULT 'manual',            -- manual, ib
    slippage_pct REAL,
    notes TEXT,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (alert_id) REFERENCES trade_alerts(id)
);

CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    deployment_type TEXT,
    sleeve_label TEXT,
    date TEXT NOT NULL,
    action TEXT NOT NULL,
    symbol TEXT NOT NULL,
    shares REAL NOT NULL,
    price REAL NOT NULL,
    amount REAL,
    reason TEXT,
    signal_detail TEXT,
    entry_date TEXT,
    entry_price REAL,
    pnl REAL,
    pnl_pct REAL,
    days_held INTEGER,
    linked_trade_id TEXT,
    created_at TEXT NOT NULL
);
"""

SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_alerts_deploy_date ON trade_alerts(deployment_id, date);
CREATE INDEX IF NOT EXISTS idx_alerts_date ON trade_alerts(date);
CREATE INDEX IF NOT EXISTS idx_executions_alert ON trade_executions(alert_id);
CREATE INDEX IF NOT EXISTS idx_executions_status ON trade_executions(status);
CREATE INDEX IF NOT EXISTS idx_trades_source ON trades(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol, date);
CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(date);
CREATE INDEX IF NOT EXISTS idx_trades_deployment ON trades(source_id, date);
"""


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.executescript(SCHEMA_INDEXES)
    # Migrate: add columns if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(deployed_strategies)").fetchall()}
    for col, typ in [("rolling_vol_30d_pct", "REAL"), ("alert_mode", "INTEGER DEFAULT 0")]:
        if col not in cols:
            conn.execute(f"ALTER TABLE deployed_strategies ADD COLUMN {col} {typ}")
    # Migrate trade_alerts: add new columns if missing
    alert_cols = {r[1] for r in conn.execute("PRAGMA table_info(trade_alerts)").fetchall()}
    for col, typ in [("signal_detail", "TEXT"), ("pnl", "REAL"), ("days_held", "INTEGER")]:
        if col not in alert_cols:
            conn.execute(f"ALTER TABLE trade_alerts ADD COLUMN {col} {typ}")
    conn.commit()
    return conn


def generate_id(strategy_name: str) -> str:
    """Generate a deployment ID from strategy name + timestamp."""
    import hashlib
    slug = re.sub(r'[^a-z0-9_]', '', strategy_name.lower().replace(" ", "_"))[:40]
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    h = hashlib.md5(f"{slug}_{ts}".encode()).hexdigest()[:8]
    return f"{slug}_{h}"


# ---------------------------------------------------------------------------
# Trades persistence
# ---------------------------------------------------------------------------
def _trade_id(source_id: str, date: str, symbol: str, action: str,
              sleeve_label: str = None, seq: int = 0) -> str:
    """Deterministic trade ID. seq distinguishes multiple same-day trades for the same symbol."""
    raw = f"{source_id}:{date}:{symbol}:{action}:{sleeve_label or ''}:{seq}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def persist_trades(source_type: str, source_id: str, trades_list: list,
                   deployment_type: str = None, sleeve_label: str = None,
                   conn=None) -> int:
    """Persist a list of trades to the unified trades table. Returns count inserted."""
    close_conn = False
    if conn is None:
        conn = get_db()
        close_conn = True

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0

    # Build a lookup of BUY trades by (symbol, date) for linking
    # Use sequence counters to distinguish multiple same-day trades for the same symbol
    buy_lookup = {}  # (symbol, date) -> trade_id
    action_seq = {}  # (date, symbol, action) -> next sequence number
    for t in trades_list:
        key = (t["date"], t["symbol"], t.get("action", ""))
        seq = action_seq.get(key, 0)
        action_seq[key] = seq + 1
        if t.get("action") == "BUY":
            tid = _trade_id(source_id, t["date"], t["symbol"], "BUY", sleeve_label, seq)
            buy_lookup[(t["symbol"], t["date"])] = tid

    action_seq = {}  # reset for second pass
    for t in trades_list:
        key = (t["date"], t["symbol"], t.get("action", ""))
        seq = action_seq.get(key, 0)
        action_seq[key] = seq + 1
        tid = _trade_id(source_id, t["date"], t["symbol"], t["action"], sleeve_label, seq)

        # Link SELL to its BUY via entry_date
        linked = None
        if t.get("action") == "SELL" and t.get("entry_date"):
            linked = buy_lookup.get((t["symbol"], t["entry_date"]))

        sig = t.get("signal_detail")
        sig_json = json.dumps(sig) if sig and not isinstance(sig, str) else sig

        try:
            cur = conn.execute(
                """INSERT OR IGNORE INTO trades
                   (id, source_type, source_id, deployment_type, sleeve_label,
                    date, action, symbol, shares, price, amount, reason,
                    signal_detail, entry_date, entry_price, pnl, pnl_pct,
                    days_held, linked_trade_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (tid, source_type, source_id, deployment_type, sleeve_label,
                 t["date"], t["action"], t["symbol"], t["shares"], t["price"],
                 t.get("amount"), t.get("reason"), sig_json,
                 t.get("entry_date"), t.get("entry_price"),
                 t.get("pnl"), t.get("pnl_pct"), t.get("days_held"),
                 linked, now),
            )
            if cur.rowcount > 0:
                inserted += 1
        except Exception:
            pass  # INSERT OR IGNORE handles dupes

    conn.commit()

    # Now back-link BUYs to their SELLs (using same sequence as insert pass)
    backlink_seq = {}
    for t in trades_list:
        if t.get("action") == "SELL" and t.get("entry_date"):
            key = (t["date"], t["symbol"], "SELL")
            seq = backlink_seq.get(key, 0)
            backlink_seq[key] = seq + 1
            buy_id = buy_lookup.get((t["symbol"], t["entry_date"]))
            sell_id = _trade_id(source_id, t["date"], t["symbol"], "SELL", sleeve_label, seq)
            if buy_id:
                conn.execute("UPDATE trades SET linked_trade_id = ? WHERE id = ? AND linked_trade_id IS NULL",
                             (sell_id, buy_id))
    conn.commit()

    if close_conn:
        conn.close()
    return inserted


def _sleeve_id(source_id: str, label: str) -> str:
    """Deterministic sleeve ID."""
    raw = f"{source_id}:{label}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def persist_sleeves(source_type: str, source_id: str, portfolio_result: dict,
                    portfolio_id: str = None, deployment_id: str = None,
                    conn=None) -> int:
    """Persist sleeve data from a portfolio backtest/deployment result.

    Args:
        source_type: "backtest" or "deployment"
        source_id: run_id or deployment_id
        portfolio_result: full result dict from run_portfolio_backtest()
        portfolio_id: canonical portfolio ID (from DB)
        deployment_id: deployment ID (for live deployments)
        conn: optional DB connection

    Returns: number of sleeves persisted
    """
    close_conn = False
    if conn is None:
        conn = _get_portfolio_db()
        close_conn = True

    now = datetime.now(timezone.utc).isoformat()
    per_sleeve = portfolio_result.get("per_sleeve", [])
    sleeve_results = portfolio_result.get("sleeve_results", [])
    config = portfolio_result.get("config", {})
    strategies_cfg = config.get("strategies", [])
    initial_capital = config.get("backtest", {}).get("initial_capital", 1000000)

    inserted = 0
    for i, ps in enumerate(per_sleeve):
        label = ps.get("label", f"sleeve_{i}")
        sid = _sleeve_id(source_id, label)

        # Get config from strategies list
        strat_cfg = strategies_cfg[i] if i < len(strategies_cfg) else {}
        strategy_id = strat_cfg.get("strategy_id")
        weight = strat_cfg.get("weight", ps.get("weight", 0))
        regime_gate = strat_cfg.get("regime_gate", ["*"])
        allocated_capital = weight * initial_capital

        # Get inline config if present
        inline_config = strat_cfg.get("config")
        config_json = json.dumps(inline_config) if inline_config else None

        # Metrics from per_sleeve summary
        total_return = ps.get("total_return_pct", 0)
        sharpe = ps.get("sharpe_ratio", ps.get("sharpe"))
        max_dd = ps.get("max_drawdown_pct")
        pf = ps.get("profit_factor")
        win_rate = ps.get("win_rate_pct")
        total_trades = ps.get("total_entries", 0)
        closed = ps.get("closed_trades", 0)
        wins = ps.get("wins", 0)
        losses = ps.get("losses", 0)
        active_days = ps.get("active_days", 0)
        gated_off_days = ps.get("gated_off_days", 0)

        # Get detailed metrics from sleeve_results if available
        sr = sleeve_results[i] if i < len(sleeve_results) else {}
        sr_metrics = sr.get("metrics", {})
        if sr_metrics:
            sharpe = sharpe or sr_metrics.get("sharpe")
            max_dd = max_dd or sr_metrics.get("max_drawdown_pct")
            pf = pf or sr_metrics.get("profit_factor")
            win_rate = win_rate or sr_metrics.get("win_rate_pct")

        # Compute last_nav from sleeve NAV history or per_sleeve summary
        last_nav = ps.get("final_nav", allocated_capital)

        # Is currently active? Check last combined_nav entry
        is_active = 1
        nav_hist = portfolio_result.get("combined_nav_history", [])
        if nav_hist:
            last_entry = nav_hist[-1]
            for s in last_entry.get("sleeves", []):
                if s.get("label") == label:
                    is_active = 1 if s.get("active", True) else 0
                    last_nav = s.get("nav", last_nav)
                    break

        try:
            conn.execute(
                """INSERT OR REPLACE INTO sleeves
                   (sleeve_id, portfolio_id, deployment_id, source_type, source_id,
                    label, strategy_id, config_json, weight, regime_gate,
                    allocated_capital, is_active, last_nav, last_return_pct,
                    sharpe, max_drawdown_pct, profit_factor, win_rate_pct,
                    total_trades, closed_trades, wins, losses,
                    active_days, gated_off_days, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sid, portfolio_id, deployment_id, source_type, source_id,
                 label, strategy_id, config_json, weight, json.dumps(regime_gate),
                 allocated_capital, is_active, last_nav, total_return,
                 sharpe, max_dd, pf, win_rate,
                 total_trades, closed, wins, losses,
                 active_days, gated_off_days, now, now),
            )
            inserted += 1
        except Exception as e:
            print(f"  ⚠ Sleeve persist failed for '{label}': {e}")

    conn.commit()
    if close_conn:
        conn.close()
    return inserted


# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------
def deploy(config_path: str, start_date: str, capital: float, name: str | None = None) -> dict:
    """Create a new deployment from a strategy config."""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Strategy config not found: {config_path}")

    config = json.loads(config_file.read_text())
    # Stamp strategy_id if missing
    if "strategy_id" not in config:
        from backtest_engine import compute_strategy_id
        config["strategy_id"] = compute_strategy_id(config)
    strategy_name = name or config.get("name", config_file.stem)
    deploy_id = generate_id(strategy_name)

    # Override backtest dates and capital in the config
    config["backtest"]["start"] = start_date
    config["backtest"]["end"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    config["sizing"]["initial_allocation"] = capital

    # Save deployment directory
    deploy_dir = DEPLOYMENTS_DIR / deploy_id
    deploy_dir.mkdir(parents=True, exist_ok=True)
    (deploy_dir / "config.json").write_text(json.dumps(config, indent=2))

    # Also keep original config path for reference
    meta = {
        "original_config": str(config_file),
        "start_date": start_date,
        "initial_capital": capital,
    }
    (deploy_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    now = datetime.now(timezone.utc).isoformat()

    conn = get_db()
    conn.execute(
        """INSERT INTO deployed_strategies
           (id, strategy_id, strategy_name, config_json, start_date, initial_capital, status, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)""",
        (deploy_id, config.get("strategy_id"), strategy_name, json.dumps(config), start_date, capital, now, now),
    )
    conn.commit()
    conn.close()

    print(f"Deployed: {deploy_id}")
    print(f"  Strategy: {strategy_name}")
    print(f"  Start: {start_date}, Capital: ${capital:,.0f}")
    print(f"  Dir: {deploy_dir}")

    # Run initial evaluation
    evaluate_one(deploy_id)

    return {"id": deploy_id, "strategy_name": strategy_name, "start_date": start_date, "capital": capital}


# ---------------------------------------------------------------------------
# Evaluate
# ---------------------------------------------------------------------------
def evaluate_one(deploy_id: str) -> dict | None:
    """Re-run the backtest engine for a single deployment with end_date = today."""
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM deployed_strategies WHERE id = ?", (deploy_id,)).fetchone()
        if not row:
            print(f"Deployment not found: {deploy_id}")
            return None
        if row["status"] != "active":
            print(f"Skipping {deploy_id} (status={row['status']})")
            return None

        deploy_dir = DEPLOYMENTS_DIR / deploy_id
        config = json.loads(row["config_json"])

        # Update end_date to today
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        config["backtest"]["end"] = today

        # Write updated config for the engine
        tmp_config = deploy_dir / "config.json"
        tmp_config.write_text(json.dumps(config, indent=2))

        # Run the backtest engine
        try:
            from backtest_engine import run_backtest, save_results
            result = run_backtest(config, force_close_at_end=False)

            # Save results to deployment directory
            # Use a dummy source path to avoid shutil.copy2 same-file error
            save_results(result, "/dev/null", output_dir=str(deploy_dir))

            # Extract key metrics for the DB row
            metrics = result.get("metrics", {})
            nav = metrics.get("final_nav", row["initial_capital"])
            total_return = metrics.get("total_return_pct", 0)
            total_trades = len(result.get("trades", []))
            open_positions = len(result.get("open_positions", []))
            alpha = metrics.get("alpha_ann_pct", 0)
            bench_return = metrics.get("benchmark_return_pct", 0)
            sharpe = metrics.get("sharpe_ratio", 0)
            ann_vol = metrics.get("annualized_volatility_pct", 0)
            # Rolling 30-day annualized volatility from NAV history
            nav_history = result.get("nav_history", [])
            rolling_vol_30d = None
            if len(nav_history) >= 2:
                import math
                navs = [p["nav"] for p in nav_history if p.get("nav")]
                # Use last 30 data points (trading days)
                window = navs[-30:] if len(navs) >= 30 else navs
                if len(window) >= 2:
                    daily_returns = [(window[i] / window[i-1]) - 1 for i in range(1, len(window))]
                    if daily_returns:
                        mean_r = sum(daily_returns) / len(daily_returns)
                        var = sum((r - mean_r) ** 2 for r in daily_returns) / len(daily_returns)
                        rolling_vol_30d = round(math.sqrt(var) * math.sqrt(252) * 100, 2)
            # Current utilization from last nav_history point
            if nav_history:
                last_point = nav_history[-1]
                last_invested = last_point.get("positions_value", 0)
                last_nav_val = last_point.get("nav", 0)
                current_util = round((last_invested / last_nav_val * 100), 2) if last_nav_val > 0 else 0
            else:
                current_util = 0

            peak_utilized = metrics.get("peak_utilized_capital", 0)
            avg_utilized = metrics.get("avg_utilized_capital", 0)
            utilization = metrics.get("utilization_pct", 0)
            rouc = metrics.get("return_on_utilized_capital_pct", 0)

            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """UPDATE deployed_strategies SET
                   updated_at = ?, last_evaluated = ?, last_nav = ?,
                   last_return_pct = ?, total_trades = ?, open_positions = ?,
                   last_alpha_pct = ?, last_benchmark_return_pct = ?,
                   current_utilization_pct = ?,
                   last_sharpe_ratio = ?, last_ann_volatility_pct = ?,
                   rolling_vol_30d_pct = ?,
                   peak_utilized_capital = ?, avg_utilized_capital = ?,
                   utilization_pct = ?, return_on_utilized_capital_pct = ?,
                   error = NULL
                   WHERE id = ?""",
                (now, today, nav, total_return, total_trades, open_positions,
                 alpha, bench_return, current_util, sharpe, ann_vol,
                 rolling_vol_30d,
                 peak_utilized, avg_utilized, utilization, rouc, deploy_id),
            )
            conn.commit()
            print(f"  ✓ {deploy_id}: NAV ${nav:,.0f} ({total_return:+.1f}%), {open_positions} open, {total_trades} trades")

            # Persist all trades to unified trades table
            all_trades = result.get("trades", [])
            if all_trades:
                n = persist_trades("deployment", deploy_id, all_trades,
                                   deployment_type="strategy", conn=conn)
                if n:
                    print(f"    💾 {n} trade(s) persisted")

            # Generate trade alerts if alert_mode is on
            if row["alert_mode"]:
                alerts = _generate_alerts(conn, deploy_id, today, result)
                if alerts:
                    print(f"    📢 {len(alerts)} alert(s) generated for {today}")

            return result

        except Exception as e:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE deployed_strategies SET updated_at = ?, error = ? WHERE id = ?",
                (now, str(e), deploy_id),
            )
            conn.commit()
            print(f"  ✗ {deploy_id}: {e}")
            return None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Trade Alerts
# ---------------------------------------------------------------------------
def _generate_alerts(conn, deploy_id: str, today: str, result: dict) -> list[dict]:
    """Extract today's new trades from engine result and create alerts.

    Compares the engine's full trade log to find trades dated today.
    Each trade becomes a BUY or SELL alert with an associated pending execution.
    Idempotent: skips if alerts already exist for this deployment + date.
    """
    import hashlib

    # Check if alerts already generated for today
    existing = conn.execute(
        "SELECT COUNT(*) FROM trade_alerts WHERE deployment_id = ? AND date = ?",
        (deploy_id, today),
    ).fetchone()[0]
    if existing > 0:
        return []  # Already generated

    trades = result.get("trades", [])
    now = datetime.now(timezone.utc).isoformat()
    alerts = []

    for trade in trades:
        if trade.get("date") != today:
            continue

        # Generate deterministic alert ID
        alert_id = hashlib.md5(
            f"{deploy_id}:{today}:{trade['symbol']}:{trade['action']}".encode()
        ).hexdigest()[:12]

        # Serialize signal_detail if present
        sig_detail = trade.get("signal_detail")
        sig_detail_json = json.dumps(sig_detail) if sig_detail else None

        alert = {
            "id": alert_id,
            "deployment_id": deploy_id,
            "date": today,
            "action": trade["action"],  # BUY or SELL
            "symbol": trade["symbol"],
            "shares": trade["shares"],
            "target_price": trade["price"],
            "amount": trade.get("amount"),
            "reason": trade.get("reason"),
            "signal_detail": sig_detail_json,
            "entry_date": trade.get("entry_date"),
            "entry_price": trade.get("entry_price"),
            "pnl_pct": trade.get("pnl_pct"),
            "pnl": trade.get("pnl"),
            "days_held": trade.get("days_held"),
            "created_at": now,
        }

        conn.execute(
            """INSERT OR IGNORE INTO trade_alerts
               (id, deployment_id, date, action, symbol, shares, target_price,
                amount, reason, signal_detail, entry_date, entry_price,
                pnl_pct, pnl, days_held, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (alert_id, deploy_id, today, trade["action"], trade["symbol"],
             trade["shares"], trade["price"], trade.get("amount"),
             trade.get("reason"), sig_detail_json, trade.get("entry_date"),
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

        alerts.append(alert)

    if alerts:
        conn.commit()

    return alerts


def set_alert_mode(deploy_id: str, enabled: bool) -> dict:
    """Enable or disable alert mode for a deployment."""
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE deployed_strategies SET alert_mode = ?, updated_at = ? WHERE id = ?",
        (1 if enabled else 0, now, deploy_id),
    )
    conn.commit()
    row = conn.execute("SELECT id, strategy_name, alert_mode FROM deployed_strategies WHERE id = ?", (deploy_id,)).fetchone()
    conn.close()
    if not row:
        return {"error": "Deployment not found"}
    return {"id": row["id"], "strategy_name": row["strategy_name"], "alert_mode": bool(row["alert_mode"])}


def get_alerts(deploy_id: str = None, date: str = None, status: str = None,
               limit: int = 50) -> list[dict]:
    """Get trade alerts with optional filters."""
    conn = get_db()
    where = []
    params = []

    if deploy_id:
        where.append("a.deployment_id = ?")
        params.append(deploy_id)
    if date:
        where.append("a.date = ?")
        params.append(date)
    if status:
        where.append("e.status = ?")
        params.append(status)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    params.append(limit)

    rows = conn.execute(f"""
        SELECT a.*, e.id as execution_id, e.status as execution_status,
               e.fill_price, e.fill_time, e.fill_shares, e.broker,
               e.slippage_pct, e.notes,
               COALESCE(ds.strategy_name, pd.portfolio_name) as strategy_name
        FROM trade_alerts a
        LEFT JOIN trade_executions e ON e.alert_id = a.id
        LEFT JOIN deployed_strategies ds ON ds.id = a.deployment_id
        LEFT JOIN portfolio_deployments pd ON pd.id = a.deployment_id
        {where_sql}
        ORDER BY a.date DESC, a.created_at DESC
        LIMIT ?
    """, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        # Parse signal_detail from JSON string to object
        if d.get("signal_detail") and isinstance(d["signal_detail"], str):
            try:
                d["signal_detail"] = json.loads(d["signal_detail"])
            except (json.JSONDecodeError, TypeError):
                pass
        results.append(d)
    return results


def execute_alert(alert_id: str, fill_price: float = None, fill_shares: float = None,
                  broker: str = "manual", notes: str = None) -> dict:
    """Mark an alert as executed."""
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()

    # Get the alert to calculate slippage
    alert = conn.execute("SELECT * FROM trade_alerts WHERE id = ?", (alert_id,)).fetchone()
    if not alert:
        conn.close()
        return {"error": "Alert not found"}

    slippage = None
    actual_price = fill_price
    if actual_price and alert["target_price"]:
        slippage = round(((actual_price - alert["target_price"]) / alert["target_price"]) * 100, 4)

    conn.execute(
        """UPDATE trade_executions SET
           status = 'executed', fill_price = ?, fill_time = ?,
           fill_shares = ?, broker = ?, slippage_pct = ?, notes = ?, updated_at = ?
           WHERE alert_id = ?""",
        (actual_price, now, fill_shares or alert["shares"],
         broker, slippage, notes, now, alert_id),
    )
    conn.commit()

    result = conn.execute("""
        SELECT a.*, e.status as execution_status, e.fill_price, e.fill_time,
               e.fill_shares, e.broker, e.slippage_pct, e.notes
        FROM trade_alerts a
        JOIN trade_executions e ON e.alert_id = a.id
        WHERE a.id = ?
    """, (alert_id,)).fetchone()
    conn.close()
    return dict(result)


def skip_alert(alert_id: str, notes: str = None) -> dict:
    """Mark an alert as skipped."""
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()

    alert = conn.execute("SELECT id FROM trade_alerts WHERE id = ?", (alert_id,)).fetchone()
    if not alert:
        conn.close()
        return {"error": "Alert not found"}

    conn.execute(
        """UPDATE trade_executions SET
           status = 'skipped', notes = ?, updated_at = ?
           WHERE alert_id = ?""",
        (notes, now, alert_id),
    )
    conn.commit()

    result = conn.execute("""
        SELECT a.*, e.status as execution_status, e.fill_price, e.notes
        FROM trade_alerts a
        JOIN trade_executions e ON e.alert_id = a.id
        WHERE a.id = ?
    """, (alert_id,)).fetchone()
    conn.close()
    return dict(result)


def get_execution_summary(deploy_id: str = None) -> dict:
    """Get execution tracking summary: paper vs real P&L, slippage, follow-through rate."""
    conn = get_db()
    where = "WHERE a.deployment_id = ?" if deploy_id else ""
    params = [deploy_id] if deploy_id else []

    total = conn.execute(f"SELECT COUNT(*) FROM trade_alerts a {where}", params).fetchone()[0]
    executed = conn.execute(f"""
        SELECT COUNT(*) FROM trade_alerts a
        JOIN trade_executions e ON e.alert_id = a.id
        {where} {"AND" if where else "WHERE"} e.status = 'executed'
    """, params).fetchone()[0]
    skipped = conn.execute(f"""
        SELECT COUNT(*) FROM trade_alerts a
        JOIN trade_executions e ON e.alert_id = a.id
        {where} {"AND" if where else "WHERE"} e.status = 'skipped'
    """, params).fetchone()[0]
    pending = total - executed - skipped

    # Average slippage on executed trades
    avg_slippage = conn.execute(f"""
        SELECT AVG(e.slippage_pct) FROM trade_alerts a
        JOIN trade_executions e ON e.alert_id = a.id
        {where} {"AND" if where else "WHERE"} e.status = 'executed' AND e.slippage_pct IS NOT NULL
    """, params).fetchone()[0]

    conn.close()
    return {
        "total_alerts": total,
        "executed": executed,
        "skipped": skipped,
        "pending": pending,
        "follow_through_pct": round(executed / total * 100, 1) if total > 0 else 0,
        "avg_slippage_pct": round(avg_slippage, 4) if avg_slippage else None,
    }


def evaluate_all() -> list[str]:
    """Evaluate all active deployments. Returns list of evaluated IDs."""
    conn = get_db()
    rows = conn.execute("SELECT id FROM deployed_strategies WHERE status = 'active'").fetchall()
    conn.close()

    evaluated = []
    print(f"Evaluating {len(rows)} active deployment(s)...")
    for row in rows:
        result = evaluate_one(row["id"])
        if result:
            evaluated.append(row["id"])
    return evaluated


# ---------------------------------------------------------------------------
# Control
# ---------------------------------------------------------------------------
def stop_deployment(deploy_id: str):
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE deployed_strategies SET status = 'stopped', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()
    print(f"Stopped: {deploy_id}")


def pause_deployment(deploy_id: str):
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE deployed_strategies SET status = 'paused', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()
    print(f"Paused: {deploy_id}")


def resume_deployment(deploy_id: str):
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE deployed_strategies SET status = 'active', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()
    print(f"Resumed: {deploy_id}")


def list_deployments(include_stopped: bool = False, strategy_id: str | None = None) -> list[dict]:
    conn = get_db()
    where = []
    params = []
    if not include_stopped:
        where.append("status != 'stopped'")
    if strategy_id:
        where.append("strategy_id = ?")
        params.append(strategy_id)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(f"SELECT * FROM deployed_strategies {where_sql} ORDER BY created_at DESC", params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_deployment(deploy_id: str) -> dict | None:
    """Get full deployment state including latest results."""
    conn = get_db()
    row = conn.execute("SELECT * FROM deployed_strategies WHERE id = ?", (deploy_id,)).fetchone()
    conn.close()
    if not row:
        return None

    result = dict(row)

    # Load latest engine output if available
    latest_path = DEPLOYMENTS_DIR / deploy_id / "results.json"
    if latest_path.exists():
        try:
            latest = json.loads(latest_path.read_text())
            result["latest"] = latest
        except (json.JSONDecodeError, OSError):
            pass

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Portfolio Deployment
# ---------------------------------------------------------------------------
from portfolio_engine import run_portfolio_backtest as _run_portfolio_bt
from regime import evaluate_regimes as _eval_regimes

PORTFOLIO_DEPLOY_SCHEMA = """
CREATE TABLE IF NOT EXISTS portfolio_deployments (
    id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL,
    portfolio_name TEXT NOT NULL,
    config_json TEXT NOT NULL,
    start_date TEXT NOT NULL,
    initial_capital REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    last_evaluated TEXT,
    last_nav REAL,
    last_return_pct REAL,
    last_alpha_pct REAL,
    last_benchmark_return_pct REAL,
    last_sharpe_ratio REAL,
    last_max_drawdown_pct REAL,
    active_regimes TEXT,
    sleeve_summary TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sleeves (
    sleeve_id TEXT PRIMARY KEY,
    portfolio_id TEXT,
    deployment_id TEXT,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    label TEXT NOT NULL,
    strategy_id TEXT,
    config_json TEXT,
    weight REAL NOT NULL,
    regime_gate TEXT,
    allocated_capital REAL,
    is_active INTEGER DEFAULT 1,
    last_nav REAL,
    last_return_pct REAL,
    sharpe REAL,
    max_drawdown_pct REAL,
    profit_factor REAL,
    win_rate_pct REAL,
    total_trades INTEGER DEFAULT 0,
    closed_trades INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    active_days INTEGER DEFAULT 0,
    gated_off_days INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sleeves_portfolio ON sleeves(portfolio_id);
CREATE INDEX IF NOT EXISTS idx_sleeves_deployment ON sleeves(deployment_id);
CREATE INDEX IF NOT EXISTS idx_sleeves_source ON sleeves(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_sleeves_strategy ON sleeves(strategy_id);
"""


def _get_portfolio_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)  # Ensures trade_alerts + trade_executions exist
    conn.executescript(SCHEMA_INDEXES)
    conn.executescript(PORTFOLIO_DEPLOY_SCHEMA)
    # Migrate: add alert_mode if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(portfolio_deployments)").fetchall()}
    if "alert_mode" not in cols:
        conn.execute("ALTER TABLE portfolio_deployments ADD COLUMN alert_mode INTEGER DEFAULT 0")
    conn.commit()
    return conn


def deploy_portfolio(portfolio_config: dict, start_date: str, capital: float,
                     name: str | None = None) -> dict:
    """Deploy a portfolio for live paper-trading."""
    portfolio_name = name or portfolio_config.get("name", "Unnamed Portfolio")
    deploy_id = generate_id(portfolio_name)

    # Inject backtest params
    portfolio_config["backtest"] = {
        "start": start_date,
        "end": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "initial_capital": capital,
    }

    # Save deployment directory
    deploy_dir = DEPLOYMENTS_DIR / deploy_id
    deploy_dir.mkdir(parents=True, exist_ok=True)
    (deploy_dir / "config.json").write_text(json.dumps(portfolio_config, indent=2))

    portfolio_id = portfolio_config.get("portfolio_id", hashlib.md5(
        json.dumps(portfolio_config.get("strategies", []), sort_keys=True).encode()
    ).hexdigest()[:12])

    now = datetime.now(timezone.utc).isoformat()
    conn = _get_portfolio_db()
    conn.execute(
        """INSERT INTO portfolio_deployments
           (id, portfolio_id, portfolio_name, config_json, start_date, initial_capital, status, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)""",
        (deploy_id, portfolio_id, portfolio_name, json.dumps(portfolio_config),
         start_date, capital, now, now),
    )
    conn.commit()
    conn.close()

    print(f"Deployed portfolio: {deploy_id}")
    print(f"  Name: {portfolio_name}")
    print(f"  Start: {start_date}, Capital: ${capital:,.0f}")
    print(f"  Sleeves: {len(portfolio_config.get('strategies', []))}")

    # Run initial evaluation
    evaluate_portfolio_one(deploy_id)

    return {"id": deploy_id, "portfolio_name": portfolio_name, "start_date": start_date, "capital": capital}


def evaluate_portfolio_one(deploy_id: str) -> dict | None:
    """Re-run portfolio backtest with end_date = today."""
    conn = _get_portfolio_db()
    try:
        row = conn.execute("SELECT * FROM portfolio_deployments WHERE id = ?", (deploy_id,)).fetchone()
        if not row:
            print(f"Portfolio deployment not found: {deploy_id}")
            return None
        if row["status"] != "active":
            print(f"Skipping {deploy_id} (status={row['status']})")
            return None

        config = json.loads(row["config_json"])
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        config["backtest"]["end"] = today

        try:
            result = _run_portfolio_bt(config, force_close_at_end=False)

            # Save results
            deploy_dir = DEPLOYMENTS_DIR / deploy_id
            deploy_dir.mkdir(parents=True, exist_ok=True)

            # Save full results (NAV histories, regime timeline, per-sleeve trades)
            (deploy_dir / "results.json").write_text(json.dumps(result, indent=2, default=str))

            metrics = result.get("metrics", {})
            per_sleeve = result.get("per_sleeve", [])

            # Get current active regimes
            regime_history = result.get("regime_history", [])
            active_regimes = regime_history[-1]["active_regimes"] if regime_history else []

            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """UPDATE portfolio_deployments SET
                   updated_at = ?, last_evaluated = ?,
                   last_nav = ?, last_return_pct = ?,
                   last_alpha_pct = ?, last_benchmark_return_pct = ?,
                   last_sharpe_ratio = ?, last_max_drawdown_pct = ?,
                   active_regimes = ?, sleeve_summary = ?,
                   error = NULL
                   WHERE id = ?""",
                (now, today,
                 metrics.get("final_nav"), metrics.get("total_return_pct"),
                 metrics.get("alpha_ann_pct"), metrics.get("benchmark_return_pct"),
                 metrics.get("sharpe_ratio"), metrics.get("max_drawdown_pct"),
                 json.dumps(active_regimes), json.dumps(per_sleeve),
                 deploy_id),
            )
            conn.commit()

            print(f"  ✓ {deploy_id}: NAV ${metrics.get('final_nav', 0):,.0f} "
                  f"({metrics.get('total_return_pct', 0):+.1f}%), "
                  f"regimes: {active_regimes or '(none)'}")

            # Persist trades per sleeve to unified trades table
            sleeve_results = result.get("sleeve_results", [])
            for i, sr in enumerate(sleeve_results):
                label = per_sleeve[i].get("label") if i < len(per_sleeve) else sr.get("strategy", f"sleeve_{i}")
                sleeve_trades = sr.get("trades", [])
                if sleeve_trades:
                    n = persist_trades("deployment", deploy_id, sleeve_trades,
                                       deployment_type="portfolio",
                                       sleeve_label=label)
                    if n:
                        print(f"    💾 {n} trade(s) persisted for sleeve '{label}'")

            # Persist sleeve-level data
            portfolio_id = row["portfolio_id"] if "portfolio_id" in row.keys() else None
            n_sleeves = persist_sleeves("deployment", deploy_id, result,
                                        portfolio_id=portfolio_id,
                                        deployment_id=deploy_id)
            if n_sleeves:
                print(f"    📊 {n_sleeves} sleeve(s) persisted")

            # Generate trade alerts if alert_mode is on
            if row["alert_mode"]:
                alerts = _generate_portfolio_alerts(conn, deploy_id, today, result)
                if alerts:
                    print(f"    📢 {len(alerts)} portfolio alert(s) generated for {today}")

            return result

        except Exception as e:
            now = datetime.now(timezone.utc).isoformat()
            try:
                conn.execute(
                    "UPDATE portfolio_deployments SET updated_at = ?, error = ? WHERE id = ?",
                    (now, str(e), deploy_id),
                )
                conn.commit()
            except Exception:
                pass
            print(f"  ✗ {deploy_id}: {e}")
            import traceback
            traceback.print_exc()
            return None
    finally:
        conn.close()


def evaluate_all_portfolios() -> list[str]:
    """Evaluate all active portfolio deployments."""
    conn = _get_portfolio_db()
    rows = conn.execute("SELECT id FROM portfolio_deployments WHERE status = 'active'").fetchall()
    conn.close()

    evaluated = []
    print(f"Evaluating {len(rows)} active portfolio deployment(s)...")
    for row in rows:
        result = evaluate_portfolio_one(row["id"])
        if result:
            evaluated.append(row["id"])
    return evaluated


def _generate_portfolio_alerts(conn, deploy_id: str, today: str, result: dict) -> list[dict]:
    """Generate trade alerts from all sleeves in a portfolio result.

    Iterates over sleeve_results, extracts today's trades from each sleeve,
    and creates alerts tagged with the sleeve name in the reason field.
    Uses the same trade_alerts / trade_executions tables as strategy alerts.
    """
    import hashlib

    # Check if alerts already generated for today
    existing = conn.execute(
        "SELECT COUNT(*) FROM trade_alerts WHERE deployment_id = ? AND date = ?",
        (deploy_id, today),
    ).fetchone()[0]
    if existing > 0:
        return []

    sleeve_results = result.get("sleeve_results", [])
    sleeves = result.get("per_sleeve", [])
    now = datetime.now(timezone.utc).isoformat()
    alerts = []

    for i, sr in enumerate(sleeve_results):
        sleeve_name = sleeves[i].get("label") or sleeves[i].get("name", f"sleeve_{i}") if i < len(sleeves) else f"sleeve_{i}"
        trades = sr.get("trades", [])

        for trade in trades:
            if trade.get("date") != today:
                continue

            alert_id = hashlib.md5(
                f"{deploy_id}:{today}:{sleeve_name}:{trade['symbol']}:{trade['action']}".encode()
            ).hexdigest()[:12]

            reason = f"[{sleeve_name}] {trade.get('reason', '')}"
            sig_detail = trade.get("signal_detail")
            sig_detail_json = json.dumps(sig_detail) if sig_detail else None

            conn.execute(
                """INSERT OR IGNORE INTO trade_alerts
                   (id, deployment_id, date, action, symbol, shares, target_price,
                    amount, reason, signal_detail, entry_date, entry_price,
                    pnl_pct, pnl, days_held, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (alert_id, deploy_id, today, trade["action"], trade["symbol"],
                 trade["shares"], trade["price"], trade.get("amount"),
                 reason, sig_detail_json, trade.get("entry_date"),
                 trade.get("entry_price"), trade.get("pnl_pct"),
                 trade.get("pnl"), trade.get("days_held"), now),
            )

            exec_id = hashlib.md5(f"exec:{alert_id}".encode()).hexdigest()[:12]
            conn.execute(
                """INSERT OR IGNORE INTO trade_executions
                   (id, alert_id, status, updated_at)
                   VALUES (?, ?, 'pending', ?)""",
                (exec_id, alert_id, now),
            )

            alerts.append({
                "id": alert_id, "deployment_id": deploy_id, "date": today,
                "action": trade["action"], "symbol": trade["symbol"],
                "shares": trade["shares"], "target_price": trade["price"],
                "sleeve": sleeve_name, "reason": reason,
            })

    if alerts:
        conn.commit()

    return alerts


def set_portfolio_alert_mode(deploy_id: str, enabled: bool) -> dict:
    """Enable or disable alert mode for a portfolio deployment."""
    conn = _get_portfolio_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE portfolio_deployments SET alert_mode = ?, updated_at = ? WHERE id = ?",
        (1 if enabled else 0, now, deploy_id),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id, portfolio_name, alert_mode FROM portfolio_deployments WHERE id = ?",
        (deploy_id,),
    ).fetchone()
    conn.close()
    if not row:
        return {"error": "Portfolio deployment not found"}
    return {"id": row["id"], "portfolio_name": row["portfolio_name"], "alert_mode": bool(row["alert_mode"])}


def stop_portfolio(deploy_id: str):
    conn = _get_portfolio_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE portfolio_deployments SET status = 'stopped', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()
    print(f"Stopped portfolio: {deploy_id}")


def pause_portfolio(deploy_id: str):
    conn = _get_portfolio_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE portfolio_deployments SET status = 'paused', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()
    print(f"Paused portfolio: {deploy_id}")


def resume_portfolio(deploy_id: str):
    conn = _get_portfolio_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE portfolio_deployments SET status = 'active', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()
    print(f"Resumed portfolio: {deploy_id}")


def list_portfolio_deployments(include_stopped: bool = False, portfolio_id: str = None) -> list[dict]:
    conn = _get_portfolio_db()
    clauses = []
    params = []
    if not include_stopped:
        clauses.append("status != 'stopped'")
    if portfolio_id:
        clauses.append("portfolio_id = ?")
        params.append(portfolio_id)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(f"SELECT * FROM portfolio_deployments {where} ORDER BY created_at DESC", params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_portfolio_deployment(deploy_id: str) -> dict | None:
    conn = _get_portfolio_db()
    row = conn.execute("SELECT * FROM portfolio_deployments WHERE id = ?", (deploy_id,)).fetchone()
    conn.close()
    if not row:
        return None
    result = dict(row)
    # Parse JSON fields
    for field in ("active_regimes", "sleeve_summary"):
        if result.get(field):
            try:
                result[field] = json.loads(result[field])
            except (json.JSONDecodeError, TypeError):
                pass
    # Load latest results and structure for frontend
    latest_path = DEPLOYMENTS_DIR / deploy_id / "results.json"
    if latest_path.exists():
        try:
            full = json.loads(latest_path.read_text())

            # Full metrics (match strategy deployment richness)
            result["metrics"] = full.get("metrics", {})

            # Combined NAV history for equity curve chart
            result["nav_history"] = full.get("combined_nav_history", [])

            # Benchmark NAV for overlay
            result["benchmark"] = full.get("benchmark", {})

            # Regime timeline (daily active/inactive per regime)
            result["regime_history"] = full.get("regime_history", [])

            # Per-sleeve detail: open positions, recent trades, metrics
            sleeve_results = full.get("sleeve_results", [])
            per_sleeve = full.get("per_sleeve", [])
            sleeves_detail = []
            for i, ps in enumerate(per_sleeve):
                sleeve = dict(ps)
                if i < len(sleeve_results):
                    sr = sleeve_results[i]
                    sleeve["metrics"] = sr.get("metrics", {})
                    sleeve["open_positions"] = sr.get("open_positions", [])
                    sleeve["closed_trades"] = sr.get("closed_trades", [])
                    trades = sr.get("trades", [])
                    sleeve["trades"] = trades
                sleeves_detail.append(sleeve)
            result["sleeves"] = sleeves_detail

        except (json.JSONDecodeError, OSError):
            pass
    return result


# ---------------------------------------------------------------------------
# Regime Deployments
# ---------------------------------------------------------------------------
REGIME_DEPLOY_SCHEMA = """
CREATE TABLE IF NOT EXISTS regime_deployments (
    id TEXT PRIMARY KEY,
    regime_id TEXT NOT NULL,
    regime_name TEXT NOT NULL,
    config_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    alert_mode INTEGER DEFAULT 0,
    is_active INTEGER,
    last_evaluated TEXT,
    last_detail TEXT,
    total_evaluated_days INTEGER DEFAULT 0,
    total_active_days INTEGER DEFAULT 0,
    last_activated_date TEXT,
    last_deactivated_date TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS regime_state_history (
    deployment_id TEXT NOT NULL,
    date TEXT NOT NULL,
    is_active INTEGER NOT NULL,
    PRIMARY KEY (deployment_id, date),
    FOREIGN KEY (deployment_id) REFERENCES regime_deployments(id)
);

CREATE TABLE IF NOT EXISTS regime_alerts (
    id TEXT PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    date TEXT NOT NULL,
    transition TEXT NOT NULL,
    regime_name TEXT NOT NULL,
    detail TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (deployment_id) REFERENCES regime_deployments(id)
);

CREATE INDEX IF NOT EXISTS idx_regime_alerts_deploy ON regime_alerts(deployment_id, date);
"""


def _get_regime_deploy_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(REGIME_DEPLOY_SCHEMA)
    # Migrate: add new columns if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(regime_deployments)").fetchall()}
    for col, typ in [
        ("total_evaluated_days", "INTEGER DEFAULT 0"),
        ("total_active_days", "INTEGER DEFAULT 0"),
        ("last_activated_date", "TEXT"),
        ("last_deactivated_date", "TEXT"),
    ]:
        if col not in cols:
            conn.execute(f"ALTER TABLE regime_deployments ADD COLUMN {col} {typ}")
    conn.commit()
    return conn


def deploy_regime(regime_id: str, name: str | None = None) -> dict:
    """Deploy a regime for live monitoring."""
    conn = _get_regime_deploy_db()

    # Load regime from regimes table
    row = conn.execute("SELECT regime_id, name, config FROM regimes WHERE regime_id = ?", (regime_id,)).fetchone()
    if not row:
        conn.close()
        raise ValueError(f"Regime {regime_id} not found")

    regime_name = name or row["name"]
    config = json.loads(row["config"])
    deploy_id = f"regime_{regime_id}"

    # Check if already deployed
    existing = conn.execute("SELECT id, status FROM regime_deployments WHERE id = ?", (deploy_id,)).fetchone()
    if existing:
        if existing["status"] == "active":
            conn.close()
            return {"id": deploy_id, "regime_name": regime_name, "status": "already_active"}
        # Re-activate stopped deployment
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE regime_deployments SET status = 'active', updated_at = ? WHERE id = ?",
            (now, deploy_id),
        )
        conn.commit()
        conn.close()
        print(f"Re-activated regime deployment: {deploy_id}")
        return {"id": deploy_id, "regime_name": regime_name, "status": "reactivated"}

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO regime_deployments
           (id, regime_id, regime_name, config_json, status, created_at, updated_at)
           VALUES (?, ?, ?, ?, 'active', ?, ?)""",
        (deploy_id, regime_id, regime_name, json.dumps(config), now, now),
    )
    conn.commit()
    conn.close()

    print(f"Deployed regime: {deploy_id} ({regime_name})")

    # Run initial evaluation
    evaluate_regime_one(deploy_id)

    return {"id": deploy_id, "regime_name": regime_name, "regime_id": regime_id, "status": "active"}


def evaluate_regime_one(deploy_id: str) -> dict | None:
    """Evaluate a single regime deployment. Detect transitions and generate alerts."""
    conn = _get_regime_deploy_db()
    row = conn.execute("SELECT * FROM regime_deployments WHERE id = ?", (deploy_id,)).fetchone()
    if not row:
        conn.close()
        return None
    if row["status"] != "active":
        conn.close()
        return None

    config = json.loads(row["config_json"])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    previous_state = bool(row["is_active"]) if row["is_active"] is not None else None

    try:
        # Evaluate regime for today with full detail
        from regime import get_regime_details
        detail_result = get_regime_details(today, [config])
        regime_name = config["name"]
        regime_detail = detail_result["regimes"].get(regime_name, {})
        current_state = regime_detail.get("active", False)
        # Save full regime detail (entry conditions, exit conditions, actual values)
        detail = json.dumps(regime_detail)

        now = datetime.now(timezone.utc).isoformat()

        # Record state history
        conn.execute(
            "INSERT OR REPLACE INTO regime_state_history (deployment_id, date, is_active) VALUES (?, ?, ?)",
            (deploy_id, today, 1 if current_state else 0),
        )

        # Update stats
        total_days = (row["total_evaluated_days"] or 0) + 1
        total_active = (row["total_active_days"] or 0) + (1 if current_state else 0)
        last_activated = row["last_activated_date"]
        last_deactivated = row["last_deactivated_date"]

        if previous_state is not None and current_state and not previous_state:
            last_activated = today
        if previous_state is not None and not current_state and previous_state:
            last_deactivated = today

        # First evaluation — set initial activation date if active
        if previous_state is None and current_state:
            last_activated = today

        conn.execute(
            """UPDATE regime_deployments SET
               updated_at = ?, last_evaluated = ?,
               is_active = ?, last_detail = ?,
               total_evaluated_days = ?, total_active_days = ?,
               last_activated_date = ?, last_deactivated_date = ?,
               error = NULL
               WHERE id = ?""",
            (now, today, 1 if current_state else 0, detail,
             total_days, total_active,
             last_activated, last_deactivated,
             deploy_id),
        )

        # Detect transition and generate alert
        if row["alert_mode"] and previous_state is not None and current_state != previous_state:
            transition = "activated" if current_state else "deactivated"
            alert_id = hashlib.md5(f"{deploy_id}:{today}:{transition}".encode()).hexdigest()[:12]

            # Check idempotency
            existing = conn.execute(
                "SELECT id FROM regime_alerts WHERE id = ?", (alert_id,)
            ).fetchone()
            if not existing:
                conn.execute(
                    """INSERT INTO regime_alerts
                       (id, deployment_id, date, transition, regime_name, detail, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (alert_id, deploy_id, today, transition, row["regime_name"], detail, now),
                )
                print(f"    🔔 Regime alert: {row['regime_name']} {transition}")

        conn.commit()
        conn.close()

        status_icon = "🟢" if current_state else "⚪"
        print(f"  {status_icon} {deploy_id}: {row['regime_name']} = {'ACTIVE' if current_state else 'INACTIVE'}")

        return {"id": deploy_id, "regime_name": row["regime_name"], "is_active": current_state, "date": today}

    except Exception as e:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE regime_deployments SET updated_at = ?, error = ? WHERE id = ?",
            (now, str(e), deploy_id),
        )
        conn.commit()
        conn.close()
        print(f"  ✗ {deploy_id}: {e}")
        return None


def evaluate_all_regimes() -> list[str]:
    """Evaluate all active regime deployments."""
    conn = _get_regime_deploy_db()
    rows = conn.execute("SELECT id FROM regime_deployments WHERE status = 'active'").fetchall()
    conn.close()

    evaluated = []
    if not rows:
        return evaluated
    print(f"Evaluating {len(rows)} active regime deployment(s)...")
    for row in rows:
        result = evaluate_regime_one(row["id"])
        if result:
            evaluated.append(row["id"])
    return evaluated


def stop_regime_deployment(deploy_id: str):
    conn = _get_regime_deploy_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE regime_deployments SET status = 'stopped', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()


def pause_regime_deployment(deploy_id: str):
    conn = _get_regime_deploy_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE regime_deployments SET status = 'paused', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()


def resume_regime_deployment(deploy_id: str):
    conn = _get_regime_deploy_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE regime_deployments SET status = 'active', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()


def set_regime_alert_mode(deploy_id: str, enabled: bool) -> dict:
    """Enable or disable alerts for a regime deployment."""
    conn = _get_regime_deploy_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE regime_deployments SET alert_mode = ?, updated_at = ? WHERE id = ?",
        (1 if enabled else 0, now, deploy_id),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id, regime_name, alert_mode FROM regime_deployments WHERE id = ?",
        (deploy_id,),
    ).fetchone()
    conn.close()
    if not row:
        return {"error": "Regime deployment not found"}
    return {"id": row["id"], "regime_name": row["regime_name"], "alert_mode": bool(row["alert_mode"])}


def list_regime_deployments(include_stopped: bool = False) -> list[dict]:
    conn = _get_regime_deploy_db()
    where = "WHERE status != 'stopped'" if not include_stopped else ""
    rows = conn.execute(f"SELECT * FROM regime_deployments {where} ORDER BY created_at DESC").fetchall()
    conn.close()
    results = []
    for r in rows:
        d = dict(r)
        total = d.get("total_evaluated_days") or 0
        active = d.get("total_active_days") or 0
        d["active_pct"] = round(active / max(total, 1) * 100, 1)
        if d.get("last_detail"):
            try:
                d["last_detail"] = json.loads(d["last_detail"])
            except (json.JSONDecodeError, TypeError):
                pass
        results.append(d)
    return results


def get_regime_deployment(deploy_id: str, include_history: bool = False) -> dict | None:
    conn = _get_regime_deploy_db()
    row = conn.execute("SELECT * FROM regime_deployments WHERE id = ?", (deploy_id,)).fetchone()
    if not row:
        conn.close()
        return None
    result = dict(row)
    if result.get("last_detail"):
        try:
            result["last_detail"] = json.loads(result["last_detail"])
        except (json.JSONDecodeError, TypeError):
            pass

    # Compute summary stats
    total_days = result.get("total_evaluated_days") or 0
    total_active = result.get("total_active_days") or 0
    result["active_pct"] = round(total_active / max(total_days, 1) * 100, 1)

    # Include state history if requested (for charts)
    if include_history:
        rows = conn.execute(
            "SELECT date, is_active FROM regime_state_history WHERE deployment_id = ? ORDER BY date",
            (deploy_id,),
        ).fetchall()
        result["state_history"] = [{"date": r["date"], "is_active": bool(r["is_active"])} for r in rows]

    conn.close()
    return result


def get_regime_alerts(deploy_id: str = None, date: str = None, limit: int = 50) -> list[dict]:
    """Get regime transition alerts."""
    conn = _get_regime_deploy_db()
    where_parts = []
    params = []
    if deploy_id:
        where_parts.append("deployment_id = ?")
        params.append(deploy_id)
    if date:
        where_parts.append("date = ?")
        params.append(date)
    where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    rows = conn.execute(
        f"SELECT * FROM regime_alerts {where} ORDER BY date DESC, created_at DESC LIMIT ?",
        params + [limit],
    ).fetchall()
    conn.close()
    results = []
    for r in rows:
        d = dict(r)
        if d.get("detail"):
            try:
                d["detail"] = json.loads(d["detail"])
            except (json.JSONDecodeError, TypeError):
                pass
        results.append(d)
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="AlphaScout Deployment Engine")
    sub = parser.add_subparsers(dest="command")

    # deploy
    p_deploy = sub.add_parser("deploy", help="Deploy a strategy for live tracking")
    p_deploy.add_argument("config", help="Path to strategy config JSON")
    p_deploy.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p_deploy.add_argument("--capital", type=float, required=True, help="Initial capital")
    p_deploy.add_argument("--name", help="Override strategy name")

    # evaluate
    sub.add_parser("evaluate", help="Evaluate all active deployments")

    # list
    p_list = sub.add_parser("list", help="List deployments")
    p_list.add_argument("--all", action="store_true", help="Include stopped")

    # stop/pause/resume
    p_stop = sub.add_parser("stop", help="Stop a deployment")
    p_stop.add_argument("id", help="Deployment ID")

    p_pause = sub.add_parser("pause", help="Pause a deployment")
    p_pause.add_argument("id", help="Deployment ID")

    p_resume = sub.add_parser("resume", help="Resume a deployment")
    p_resume.add_argument("id", help="Deployment ID")

    # status
    p_status = sub.add_parser("status", help="Get deployment details")
    p_status.add_argument("id", help="Deployment ID")

    args = parser.parse_args()

    if args.command == "deploy":
        deploy(args.config, args.start, args.capital, args.name)
    elif args.command == "evaluate":
        evaluate_all_regimes()
        evaluate_all()
        evaluate_all_portfolios()
    elif args.command == "list":
        deployments = list_deployments(include_stopped=args.all)
        if not deployments:
            print("No deployments found.")
            return
        for d in deployments:
            nav_str = f"${d['last_nav']:,.0f}" if d['last_nav'] else "—"
            ret_str = f"{d['last_return_pct']:+.1f}%" if d['last_return_pct'] else "—"
            print(f"  [{d['status']:>7}] {d['id']}")
            print(f"           {d['strategy_name']} | Start: {d['start_date']} | Capital: ${d['initial_capital']:,.0f}")
            util_str = f"{d['utilization_pct']:.0f}%" if d.get('utilization_pct') else "—"
            rouc_str = f"{d['return_on_utilized_capital_pct']:+.1f}%" if d.get('return_on_utilized_capital_pct') else "—"
            print(f"           NAV: {nav_str} ({ret_str}) | Trades: {d['total_trades']} | Open: {d['open_positions']}")
            print(f"           Utilization: {util_str} | ROUC: {rouc_str}")
            if d['error']:
                print(f"           ⚠ Error: {d['error']}")
    elif args.command == "stop":
        stop_deployment(args.id)
    elif args.command == "pause":
        pause_deployment(args.id)
    elif args.command == "resume":
        resume_deployment(args.id)
    elif args.command == "status":
        d = get_deployment(args.id)
        if not d:
            print(f"Not found: {args.id}")
            return
        latest = d.pop("latest", None)
        print(json.dumps(d, indent=2))
        if latest and latest.get("metrics"):
            m = latest["metrics"]
            print(f"\nLatest metrics:")
            print(f"  NAV: ${m.get('final_nav', 0):,.0f}")
            print(f"  Return: {m.get('total_return_pct', 0):+.1f}%")
            print(f"  Alpha: {m.get('alpha_ann_pct', 0):+.1f}%")
            print(f"  Sharpe: {m.get('sharpe', 0):.2f}")
            print(f"  Win rate: {m.get('win_rate_pct', 0):.0f}%")
            print(f"  Max DD: {m.get('max_drawdown_pct', 0):.1f}%")
            print(f"  Peak Utilized: ${m.get('peak_utilized_capital', 0):,.0f}")
            print(f"  Avg Utilized: ${m.get('avg_utilized_capital', 0):,.0f}")
            print(f"  Utilization: {m.get('utilization_pct', 0):.1f}%")
            print(f"  ROUC: {m.get('return_on_utilized_capital_pct', 0):+.1f}%")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
