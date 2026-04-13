#!/usr/bin/env python3
"""
AlphaScout Deployment Engine (v2 — unified model)
===================================================
Every deployment is a portfolio. A single-strategy deployment is auto-wrapped
as a one-sleeve portfolio with weight=1.0 and no regime gating.

Storage:
  - `deployments` SQLite table for all deployment state
  - `deployments/{id}/` on disk for full engine output
  - `sleeves` table for per-sleeve metrics
  - `trades` table for unified trade log
  - `trade_alerts` + `trade_executions` for daily signals

Usage:
    python3 deploy_engine.py deploy strategies/tech_30pct_drop.json --start 2026-01-01 --capital 100000
    python3 deploy_engine.py deploy-portfolio portfolio.json --start 2026-01-01 --capital 1000000
    python3 deploy_engine.py evaluate
    python3 deploy_engine.py list
    python3 deploy_engine.py stop <id>
"""

import os
import sys
import re
import json
import hashlib
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime, timezone

SCRIPT_DIR = Path(__file__).parent
WORKSPACE = Path(os.environ.get("WORKSPACE", "/app"))
DEPLOYMENTS_DIR = WORKSPACE / "deployments"
from db_config import APP_DB_PATH as DB_PATH

sys.path.insert(0, str(SCRIPT_DIR))


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS deployments (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL DEFAULT 'portfolio',  -- 'strategy' or 'portfolio' (display only)
    name TEXT NOT NULL,
    config_json TEXT NOT NULL,               -- always portfolio format (frozen at deploy time)
    start_date TEXT NOT NULL,
    initial_capital REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',   -- active, paused, stopped
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    -- Latest evaluation metrics
    last_evaluated TEXT,
    last_nav REAL,
    last_return_pct REAL,
    last_alpha_pct REAL,
    last_benchmark_return_pct REAL,
    last_sharpe_ratio REAL,
    last_max_drawdown_pct REAL,
    last_ann_volatility_pct REAL,
    rolling_vol_30d_pct REAL,
    total_trades INTEGER DEFAULT 0,
    open_positions INTEGER DEFAULT 0,
    current_utilization_pct REAL,
    peak_utilized_capital REAL,
    avg_utilized_capital REAL,
    utilization_pct REAL,
    return_on_utilized_capital_pct REAL,
    -- Dual benchmark
    alpha_vs_market_pct REAL,
    alpha_vs_sector_pct REAL,
    market_benchmark_return_pct REAL,
    sector_benchmark_return_pct REAL,
    -- Portfolio-specific
    active_regimes TEXT,                     -- JSON array of active regime names
    sleeve_summary TEXT,                     -- JSON array of per-sleeve summaries
    num_sleeves INTEGER DEFAULT 1,
    -- Alerts
    alert_mode INTEGER DEFAULT 0,
    error TEXT
);

CREATE TABLE IF NOT EXISTS trade_alerts (
    id TEXT PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    date TEXT NOT NULL,
    action TEXT NOT NULL,
    symbol TEXT NOT NULL,
    shares REAL NOT NULL,
    target_price REAL NOT NULL,
    amount REAL,
    reason TEXT,
    signal_detail TEXT,
    entry_date TEXT,
    entry_price REAL,
    pnl_pct REAL,
    pnl REAL,
    days_held INTEGER,
    sleeve_label TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (deployment_id) REFERENCES deployments(id)
);

CREATE TABLE IF NOT EXISTS trade_executions (
    id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    fill_price REAL,
    fill_time TEXT,
    fill_shares REAL,
    broker TEXT DEFAULT 'manual',
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

CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id TEXT PRIMARY KEY,
    type TEXT NOT NULL DEFAULT 'portfolio',  -- 'strategy' or 'portfolio'
    name TEXT NOT NULL,
    config_json TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    initial_capital REAL NOT NULL,
    final_nav REAL,
    total_return_pct REAL,
    annualized_return_pct REAL,
    annualized_volatility_pct REAL,
    max_drawdown_pct REAL,
    max_drawdown_date TEXT,
    sharpe_ratio REAL,
    sortino_ratio REAL,
    calmar_ratio REAL,
    profit_factor REAL,
    total_entries INTEGER,
    closed_trades INTEGER,
    wins INTEGER,
    losses INTEGER,
    win_rate_pct REAL,
    avg_holding_days REAL,
    utilization_pct REAL,
    trading_days INTEGER,
    benchmark_return_pct REAL,
    alpha_ann_pct REAL,
    num_sleeves INTEGER DEFAULT 1,
    per_sleeve_json TEXT,
    results_path TEXT,
    created_at TEXT NOT NULL
);
"""

SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_deployments_status ON deployments(status);
CREATE INDEX IF NOT EXISTS idx_deployments_type ON deployments(type);
CREATE INDEX IF NOT EXISTS idx_alerts_deploy_date ON trade_alerts(deployment_id, date);
CREATE INDEX IF NOT EXISTS idx_alerts_date ON trade_alerts(date);
CREATE INDEX IF NOT EXISTS idx_executions_alert ON trade_executions(alert_id);
CREATE INDEX IF NOT EXISTS idx_executions_status ON trade_executions(status);
CREATE INDEX IF NOT EXISTS idx_trades_source ON trades(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol, date);
CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(date);
CREATE INDEX IF NOT EXISTS idx_trades_deployment ON trades(source_id, date);
CREATE INDEX IF NOT EXISTS idx_sleeves_deployment ON sleeves(deployment_id);
CREATE INDEX IF NOT EXISTS idx_sleeves_source ON sleeves(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_sleeves_strategy ON sleeves(strategy_id);
CREATE INDEX IF NOT EXISTS idx_backtest_runs_type ON backtest_runs(type);
CREATE INDEX IF NOT EXISTS idx_backtest_runs_created ON backtest_runs(created_at);
"""


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    # Create indexes individually (some may reference columns not yet migrated in old tables)
    for line in SCHEMA_INDEXES.strip().split(";"):
        line = line.strip()
        if line:
            try:
                conn.execute(line)
            except sqlite3.OperationalError:
                pass  # index references a column from an older schema version — skip
    conn.commit()
    return conn


def generate_id(name: str) -> str:
    """Generate a unique deployment ID from name + timestamp."""
    slug = re.sub(r'[^a-z0-9]+', '_', name.lower().strip())[:40]
    suffix = hashlib.md5(
        f"{name}:{datetime.now(timezone.utc).isoformat()}".encode()
    ).hexdigest()[:8]
    return f"{slug}_{suffix}"


# ---------------------------------------------------------------------------
# Strategy → Portfolio wrapping
# ---------------------------------------------------------------------------
def _is_strategy_config(config: dict) -> bool:
    """Detect whether a config is a raw strategy (not a portfolio)."""
    # A portfolio has "strategies" or "sleeves" at top level
    # A strategy has "universe" + "entry" + "sizing"
    return "universe" in config and "entry" in config and "sizing" in config


def wrap_strategy_as_portfolio(strategy_config: dict, capital: float,
                                start_date: str, end_date: str) -> dict:
    """Wrap a single strategy config as a one-sleeve portfolio config."""
    return {
        "name": strategy_config.get("name", "Unnamed Strategy"),
        "sleeves": [{
            "strategy_config": strategy_config,
            "weight": 1.0,
            "regime_gate": ["*"],
            "label": strategy_config.get("name", "Main"),
        }],
        "regime_filter": False,
        "capital_when_gated_off": "to_cash",
        "backtest": {
            "start": start_date,
            "end": end_date,
            "initial_capital": capital,
        },
    }


# ---------------------------------------------------------------------------
# Trade ID helpers
# ---------------------------------------------------------------------------
def _trade_id(source_id: str, date: str, symbol: str, action: str,
              sleeve_label: str = None, seq: int = 0) -> str:
    """Deterministic trade ID. seq distinguishes multiple same-day trades."""
    raw = f"{source_id}:{date}:{symbol}:{action}:{sleeve_label or ''}:{seq}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _sleeve_id(source_id: str, label: str) -> str:
    """Deterministic sleeve ID."""
    raw = f"{source_id}:{label}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Persist trades
# ---------------------------------------------------------------------------
def persist_trades(source_type: str, source_id: str, trades_list: list,
                   deployment_type: str = None, sleeve_label: str = None,
                   conn=None) -> int:
    """Persist trades to the unified trades table. Returns count inserted."""
    close_conn = False
    if conn is None:
        conn = get_db()
        close_conn = True

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0

    # Build BUY lookup for linking, with sequence counters
    buy_lookup = {}
    action_seq = {}
    for t in trades_list:
        key = (t["date"], t["symbol"], t.get("action", ""))
        seq = action_seq.get(key, 0)
        action_seq[key] = seq + 1
        if t.get("action") == "BUY":
            tid = _trade_id(source_id, t["date"], t["symbol"], "BUY", sleeve_label, seq)
            buy_lookup[(t["symbol"], t["date"])] = tid

    action_seq = {}
    for t in trades_list:
        key = (t["date"], t["symbol"], t.get("action", ""))
        seq = action_seq.get(key, 0)
        action_seq[key] = seq + 1
        tid = _trade_id(source_id, t["date"], t["symbol"], t["action"], sleeve_label, seq)

        linked = None
        if t.get("action") == "SELL" and t.get("entry_date"):
            linked = buy_lookup.get((t["symbol"], t["entry_date"]))

        sig = t.get("signal_detail")
        sig_json = json.dumps(sig) if sig and not isinstance(sig, str) else sig

        try:
            conn.execute(
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
            if conn.total_changes:
                inserted += 1
        except Exception:
            pass

    conn.commit()

    # Back-link BUYs to SELLs
    for t in trades_list:
        if t.get("action") == "SELL" and t.get("entry_date"):
            buy_id = buy_lookup.get((t["symbol"], t["entry_date"]))
            sell_id = _trade_id(source_id, t["date"], t["symbol"], "SELL", sleeve_label)
            if buy_id:
                conn.execute(
                    "UPDATE trades SET linked_trade_id = ? WHERE id = ? AND linked_trade_id IS NULL",
                    (sell_id, buy_id),
                )
    conn.commit()

    if close_conn:
        conn.close()
    return inserted


# ---------------------------------------------------------------------------
# Persist sleeves
# ---------------------------------------------------------------------------
def persist_sleeves(source_type: str, source_id: str, portfolio_result: dict,
                    deployment_id: str = None, conn=None) -> int:
    """Persist per-sleeve data from a portfolio result."""
    close_conn = False
    if conn is None:
        conn = get_db()
        close_conn = True

    per_sleeve = portfolio_result.get("per_sleeve", [])
    sleeve_results = portfolio_result.get("sleeve_results", [])
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0

    for i, ps in enumerate(per_sleeve):
        label = ps.get("label", f"sleeve_{i}")
        sid = _sleeve_id(source_id, label)
        sr = sleeve_results[i] if i < len(sleeve_results) else {}
        sr_metrics = sr.get("metrics", {})
        sr_config = sr.get("config", {})

        try:
            conn.execute(
                """INSERT OR REPLACE INTO sleeves
                   (sleeve_id, deployment_id, source_type, source_id,
                    label, strategy_id, config_json, weight, regime_gate,
                    allocated_capital, is_active, last_nav, last_return_pct,
                    sharpe, max_drawdown_pct, profit_factor, win_rate_pct,
                    total_trades, closed_trades, wins, losses,
                    active_days, gated_off_days, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sid, deployment_id, source_type, source_id,
                 label, sr_config.get("strategy_id"),
                 json.dumps(sr_config) if sr_config else None,
                 ps.get("weight", 0),
                 json.dumps(ps.get("regime_gate", ["*"])),
                 ps.get("allocated_capital", 0),
                 1 if ps.get("active_days", 0) > 0 else 0,
                 sr_metrics.get("final_nav"), sr_metrics.get("total_return_pct"),
                 sr_metrics.get("sharpe_ratio"), sr_metrics.get("max_drawdown_pct"),
                 sr_metrics.get("profit_factor"), sr_metrics.get("win_rate_pct"),
                 sr_metrics.get("total_entries", 0),
                 ps.get("closed_trades", 0), ps.get("wins", 0), ps.get("losses", 0),
                 ps.get("active_days", 0), ps.get("gated_off_days", 0),
                 now, now),
            )
            inserted += 1
        except Exception as e:
            print(f"  Warning: sleeve persist failed for '{label}': {e}")

    conn.commit()
    if close_conn:
        conn.close()
    return inserted


# ---------------------------------------------------------------------------
# Deploy (unified)
# ---------------------------------------------------------------------------
def deploy(config_or_path, start_date: str, capital: float,
           name: str = None) -> dict:
    """
    Deploy a strategy or portfolio for live paper-trading.

    Accepts either:
      - A file path to a strategy JSON config
      - A portfolio config dict
      - A file path to a portfolio JSON config

    Single strategies are auto-wrapped as one-sleeve portfolios.
    """
    # Load config
    if isinstance(config_or_path, (str, Path)):
        config_file = Path(config_or_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config not found: {config_or_path}")
        config = json.loads(config_file.read_text())
    else:
        config = config_or_path

    # Determine type and wrap if needed
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _is_strategy_config(config):
        deploy_type = "strategy"
        # Stamp strategy_id
        if "strategy_id" not in config:
            from backtest_engine import compute_strategy_id
            config["strategy_id"] = compute_strategy_id(config)
        deploy_name = name or config.get("name", "Unnamed Strategy")
        portfolio_config = wrap_strategy_as_portfolio(config, capital, start_date, today)
    else:
        deploy_type = "portfolio"
        deploy_name = name or config.get("name", "Unnamed Portfolio")
        portfolio_config = config
        # Inject backtest params
        portfolio_config["backtest"] = {
            "start": start_date,
            "end": today,
            "initial_capital": capital,
        }

    deploy_id = generate_id(deploy_name)
    num_sleeves = len(portfolio_config.get("sleeves",
                      portfolio_config.get("strategies", [])))

    # Save deployment directory
    deploy_dir = DEPLOYMENTS_DIR / deploy_id
    deploy_dir.mkdir(parents=True, exist_ok=True)
    (deploy_dir / "config.json").write_text(json.dumps(portfolio_config, indent=2))

    # Save to DB
    now = datetime.now(timezone.utc).isoformat()
    conn = get_db()
    conn.execute(
        """INSERT INTO deployments
           (id, type, name, config_json, start_date, initial_capital,
            num_sleeves, status, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)""",
        (deploy_id, deploy_type, deploy_name, json.dumps(portfolio_config),
         start_date, capital, num_sleeves, now, now),
    )
    conn.commit()
    conn.close()

    print(f"Deployed ({deploy_type}): {deploy_id}")
    print(f"  Name: {deploy_name}")
    print(f"  Start: {start_date}, Capital: ${capital:,.0f}, Sleeves: {num_sleeves}")

    # Run initial evaluation
    evaluate_one(deploy_id)

    return {"id": deploy_id, "name": deploy_name, "type": deploy_type,
            "start_date": start_date, "capital": capital}


# ---------------------------------------------------------------------------
# Evaluate (unified — always runs portfolio engine)
# ---------------------------------------------------------------------------
def evaluate_one(deploy_id: str) -> dict | None:
    """Re-run portfolio backtest with end_date = today."""
    conn = get_db()
    row = conn.execute("SELECT * FROM deployments WHERE id = ?", (deploy_id,)).fetchone()
    if not row:
        print(f"Deployment not found: {deploy_id}")
        conn.close()
        return None
    if row["status"] != "active":
        print(f"Skipping {deploy_id} (status={row['status']})")
        conn.close()
        return None

    config = json.loads(row["config_json"])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    config["backtest"]["end"] = today
    deploy_type = row["type"]

    deploy_dir = DEPLOYMENTS_DIR / deploy_id
    deploy_dir.mkdir(parents=True, exist_ok=True)
    (deploy_dir / "config.json").write_text(json.dumps(config, indent=2))

    try:
        from portfolio_engine import run_portfolio_backtest
        result = run_portfolio_backtest(config, force_close_at_end=False)

        # Save full results to disk
        (deploy_dir / "results.json").write_text(
            json.dumps(result, indent=2, default=str))

        # Extract metrics
        metrics = result.get("metrics", {})
        per_sleeve = result.get("per_sleeve", [])
        regime_history = result.get("regime_history", [])
        active_regimes = regime_history[-1]["active_regimes"] if regime_history else []

        # Count total trades across all sleeves
        total_trades = sum(
            len(sr.get("trades", []))
            for sr in result.get("sleeve_results", [])
        )
        # Count open positions across all sleeves
        open_positions = sum(
            len(sr.get("open_positions", []))
            for sr in result.get("sleeve_results", [])
        )

        # Rolling 30-day volatility from combined NAV
        rolling_vol_30d = None
        nav_history = result.get("combined_nav_history", [])
        if len(nav_history) >= 2:
            import math
            navs = [p["nav"] for p in nav_history if p.get("nav")]
            window = navs[-30:] if len(navs) >= 30 else navs
            if len(window) >= 2:
                daily_returns = [(window[i] / window[i-1]) - 1 for i in range(1, len(window))]
                if daily_returns:
                    mean_r = sum(daily_returns) / len(daily_returns)
                    var = sum((r - mean_r) ** 2 for r in daily_returns) / len(daily_returns)
                    rolling_vol_30d = round(math.sqrt(var) * math.sqrt(252) * 100, 2)

        # Current utilization from per-sleeve active NAV
        utilized = sum(s["nav"] for entry in nav_history[-1:] for s in entry.get("sleeves", []) if s.get("active"))
        last_nav_val = nav_history[-1]["nav"] if nav_history else 0
        current_util = round((utilized / last_nav_val * 100), 2) if last_nav_val > 0 else 0

        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """UPDATE deployments SET
               updated_at = ?, last_evaluated = ?,
               last_nav = ?, last_return_pct = ?,
               last_alpha_pct = ?, last_benchmark_return_pct = ?,
               last_sharpe_ratio = ?, last_max_drawdown_pct = ?,
               last_ann_volatility_pct = ?, rolling_vol_30d_pct = ?,
               total_trades = ?, open_positions = ?,
               current_utilization_pct = ?,
               peak_utilized_capital = ?, avg_utilized_capital = ?,
               utilization_pct = ?, return_on_utilized_capital_pct = ?,
               alpha_vs_market_pct = ?, alpha_vs_sector_pct = ?,
               market_benchmark_return_pct = ?, sector_benchmark_return_pct = ?,
               active_regimes = ?, sleeve_summary = ?,
               error = NULL
               WHERE id = ?""",
            (now, today,
             metrics.get("final_nav"), metrics.get("total_return_pct"),
             metrics.get("alpha_ann_pct"), metrics.get("benchmark_return_pct"),
             metrics.get("sharpe_ratio"), metrics.get("max_drawdown_pct"),
             metrics.get("annualized_volatility_pct"), rolling_vol_30d,
             total_trades, open_positions, current_util,
             metrics.get("peak_utilized_capital"),
             metrics.get("avg_utilized_capital"),
             metrics.get("utilization_pct"),
             metrics.get("return_on_utilized_capital_pct"),
             metrics.get("alpha_vs_market_pct"), metrics.get("alpha_vs_sector_pct"),
             metrics.get("market_benchmark_return_pct"), metrics.get("sector_benchmark_return_pct"),
             json.dumps(active_regimes), json.dumps(per_sleeve),
             deploy_id),
        )
        conn.commit()

        type_label = "strategy" if deploy_type == "strategy" else "portfolio"
        print(f"  ✓ {deploy_id} ({type_label}): NAV ${metrics.get('final_nav', 0):,.0f} "
              f"({metrics.get('total_return_pct', 0):+.1f}%), "
              f"{total_trades} trades, {open_positions} open")

        # Persist trades per sleeve
        sleeve_results = result.get("sleeve_results", [])
        for i, sr in enumerate(sleeve_results):
            label = per_sleeve[i].get("label") if i < len(per_sleeve) else f"sleeve_{i}"
            sleeve_trades = sr.get("trades", [])
            if sleeve_trades:
                n = persist_trades("deployment", deploy_id, sleeve_trades,
                                   deployment_type=deploy_type,
                                   sleeve_label=label, conn=conn)
                if n:
                    print(f"    {n} trade(s) persisted for sleeve '{label}'")

        # Persist sleeves
        n_sleeves = persist_sleeves("deployment", deploy_id, result,
                                    deployment_id=deploy_id, conn=conn)
        if n_sleeves:
            print(f"    {n_sleeves} sleeve(s) persisted")

        # Generate alerts if enabled
        if row["alert_mode"]:
            alerts = _generate_alerts(conn, deploy_id, today, result)
            if alerts:
                print(f"    {len(alerts)} alert(s) generated for {today}")

        conn.close()
        return result

    except Exception as e:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE deployments SET updated_at = ?, error = ? WHERE id = ?",
            (now, str(e), deploy_id),
        )
        conn.commit()
        conn.close()
        print(f"  ✗ {deploy_id}: {e}")
        import traceback
        traceback.print_exc()
        return None


def evaluate_all() -> list[str]:
    """Evaluate all active deployments (strategies + portfolios)."""
    conn = get_db()
    rows = conn.execute("SELECT id, type FROM deployments WHERE status = 'active'").fetchall()
    conn.close()

    evaluated = []
    print(f"Evaluating {len(rows)} active deployment(s)...")
    for row in rows:
        result = evaluate_one(row["id"])
        if result:
            evaluated.append(row["id"])
    return evaluated


# ---------------------------------------------------------------------------
# Alert generation (unified — handles sleeves)
# ---------------------------------------------------------------------------
def _generate_alerts(conn, deploy_id: str, today: str, result: dict) -> list[dict]:
    """Generate trade alerts from all sleeves. Idempotent."""
    # Check if already generated
    existing = conn.execute(
        "SELECT COUNT(*) FROM trade_alerts WHERE deployment_id = ? AND date = ?",
        (deploy_id, today),
    ).fetchone()[0]
    if existing > 0:
        return []

    sleeve_results = result.get("sleeve_results", [])
    per_sleeve = result.get("per_sleeve", [])
    now = datetime.now(timezone.utc).isoformat()
    alerts = []

    for i, sr in enumerate(sleeve_results):
        sleeve_name = per_sleeve[i].get("label", f"sleeve_{i}") if i < len(per_sleeve) else f"sleeve_{i}"
        trades = sr.get("trades", [])

        for trade in trades:
            if trade.get("date") != today:
                continue

            alert_id = hashlib.md5(
                f"{deploy_id}:{today}:{sleeve_name}:{trade['symbol']}:{trade['action']}".encode()
            ).hexdigest()[:12]

            reason = f"[{sleeve_name}] {trade.get('reason', '')}"
            sig_detail = trade.get("signal_detail")
            sig_json = json.dumps(sig_detail) if sig_detail else None

            conn.execute(
                """INSERT OR IGNORE INTO trade_alerts
                   (id, deployment_id, date, action, symbol, shares, target_price,
                    amount, reason, signal_detail, entry_date, entry_price,
                    pnl_pct, pnl, days_held, sleeve_label, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (alert_id, deploy_id, today, trade["action"], trade["symbol"],
                 trade["shares"], trade["price"], trade.get("amount"),
                 reason, sig_json, trade.get("entry_date"),
                 trade.get("entry_price"), trade.get("pnl_pct"),
                 trade.get("pnl"), trade.get("days_held"),
                 sleeve_name, now),
            )

            exec_id = hashlib.md5(f"exec:{alert_id}".encode()).hexdigest()[:12]
            conn.execute(
                """INSERT OR IGNORE INTO trade_executions
                   (id, alert_id, status, updated_at) VALUES (?, ?, 'pending', ?)""",
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


# ---------------------------------------------------------------------------
# Control (unified)
# ---------------------------------------------------------------------------
def stop_deployment(deploy_id: str):
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE deployments SET status = 'stopped', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()
    print(f"Stopped: {deploy_id}")


def pause_deployment(deploy_id: str):
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE deployments SET status = 'paused', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()
    print(f"Paused: {deploy_id}")


def resume_deployment(deploy_id: str):
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE deployments SET status = 'active', updated_at = ? WHERE id = ?", (now, deploy_id))
    conn.commit()
    conn.close()
    print(f"Resumed: {deploy_id}")


def set_alert_mode(deploy_id: str, enabled: bool) -> dict:
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE deployments SET alert_mode = ?, updated_at = ? WHERE id = ?",
        (1 if enabled else 0, now, deploy_id),
    )
    conn.commit()
    row = conn.execute("SELECT id, name, alert_mode FROM deployments WHERE id = ?", (deploy_id,)).fetchone()
    conn.close()
    if not row:
        return {"error": "Deployment not found"}
    return {"id": row["id"], "name": row["name"], "alert_mode": bool(row["alert_mode"])}


# ---------------------------------------------------------------------------
# Queries (unified)
# ---------------------------------------------------------------------------
def list_deployments(include_stopped: bool = False, deploy_type: str = None) -> list[dict]:
    conn = get_db()
    clauses = []
    params = []
    if not include_stopped:
        clauses.append("status != 'stopped'")
    if deploy_type:
        clauses.append("type = ?")
        params.append(deploy_type)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(f"SELECT * FROM deployments {where} ORDER BY created_at DESC", params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_deployment(deploy_id: str) -> dict | None:
    conn = get_db()
    row = conn.execute("SELECT * FROM deployments WHERE id = ?", (deploy_id,)).fetchone()
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

    # Load latest results from disk
    latest_path = DEPLOYMENTS_DIR / deploy_id / "results.json"
    if latest_path.exists():
        try:
            full = json.loads(latest_path.read_text())
            result["metrics"] = full.get("metrics", {})
            result["nav_history"] = full.get("combined_nav_history", [])
            result["benchmark"] = full.get("benchmark", {})
            result["regime_history"] = full.get("regime_history", [])

            # Per-sleeve detail
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
                    sleeve["trades"] = sr.get("trades", [])
                sleeves_detail.append(sleeve)
            result["sleeves"] = sleeves_detail
        except (json.JSONDecodeError, OSError):
            pass

    return result


def get_alerts(deploy_id: str = None, date: str = None, status: str = None,
               limit: int = 50) -> list[dict]:
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
               d.name as deployment_name, d.type as deployment_type
        FROM trade_alerts a
        LEFT JOIN trade_executions e ON e.alert_id = a.id
        LEFT JOIN deployments d ON d.id = a.deployment_id
        {where_sql}
        ORDER BY a.date DESC, a.created_at DESC
        LIMIT ?
    """, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        if d.get("signal_detail") and isinstance(d["signal_detail"], str):
            try:
                d["signal_detail"] = json.loads(d["signal_detail"])
            except (json.JSONDecodeError, TypeError):
                pass
        results.append(d)
    return results


def execute_alert(alert_id: str, fill_price: float = None, fill_shares: float = None,
                  broker: str = "manual", notes: str = None) -> dict:
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    alert = conn.execute("SELECT * FROM trade_alerts WHERE id = ?", (alert_id,)).fetchone()
    if not alert:
        conn.close()
        return {"error": "Alert not found"}

    slippage = None
    if fill_price and alert["target_price"]:
        slippage = round(((fill_price - alert["target_price"]) / alert["target_price"]) * 100, 4)

    conn.execute(
        """UPDATE trade_executions SET
           status = 'executed', fill_price = ?, fill_time = ?,
           fill_shares = ?, broker = ?, slippage_pct = ?, notes = ?, updated_at = ?
           WHERE alert_id = ?""",
        (fill_price, now, fill_shares or alert["shares"], broker, slippage, notes, now, alert_id),
    )
    conn.commit()
    result = conn.execute("""
        SELECT a.*, e.status as execution_status, e.fill_price, e.fill_time,
               e.fill_shares, e.broker, e.slippage_pct, e.notes
        FROM trade_alerts a JOIN trade_executions e ON e.alert_id = a.id
        WHERE a.id = ?
    """, (alert_id,)).fetchone()
    conn.close()
    return dict(result)


def skip_alert(alert_id: str, notes: str = None) -> dict:
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE trade_executions SET status = 'skipped', notes = ?, updated_at = ? WHERE alert_id = ?",
        (notes, now, alert_id),
    )
    conn.commit()
    result = conn.execute("""
        SELECT a.*, e.status as execution_status, e.fill_price, e.notes
        FROM trade_alerts a JOIN trade_executions e ON e.alert_id = a.id
        WHERE a.id = ?
    """, (alert_id,)).fetchone()
    conn.close()
    return dict(result) if result else {"error": "Alert not found"}


def get_execution_summary(deploy_id: str = None) -> dict:
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

    avg_slippage = conn.execute(f"""
        SELECT AVG(e.slippage_pct) FROM trade_alerts a
        JOIN trade_executions e ON e.alert_id = a.id
        {where} {"AND" if where else "WHERE"} e.status = 'executed' AND e.slippage_pct IS NOT NULL
    """, params).fetchone()[0]

    conn.close()
    return {
        "total_alerts": total, "executed": executed, "skipped": skipped, "pending": pending,
        "follow_through_pct": round(executed / total * 100, 1) if total > 0 else 0,
        "avg_slippage_pct": round(avg_slippage, 4) if avg_slippage else None,
    }


# ---------------------------------------------------------------------------
# Migration: copy data from old tables to new unified tables
# ---------------------------------------------------------------------------
def migrate_from_v1():
    """One-time migration: copy deployed_strategies + portfolio_deployments → deployments."""
    conn = get_db()

    # Check if old tables exist
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}

    migrated = 0

    if "deployed_strategies" in tables:
        rows = conn.execute("SELECT * FROM deployed_strategies").fetchall()
        for r in rows:
            r = dict(r)
            # Wrap strategy config as portfolio
            strategy_config = json.loads(r["config_json"])
            portfolio_config = wrap_strategy_as_portfolio(
                strategy_config, r["initial_capital"],
                r["start_date"], r.get("last_evaluated") or r["start_date"],
            )
            # Check if already migrated
            existing = conn.execute("SELECT id FROM deployments WHERE id = ?", (r["id"],)).fetchone()
            if existing:
                continue
            conn.execute(
                """INSERT OR IGNORE INTO deployments
                   (id, type, name, config_json, start_date, initial_capital,
                    num_sleeves, status, created_at, updated_at,
                    last_evaluated, last_nav, last_return_pct,
                    last_alpha_pct, last_benchmark_return_pct,
                    last_sharpe_ratio, last_ann_volatility_pct,
                    rolling_vol_30d_pct, total_trades, open_positions,
                    current_utilization_pct, peak_utilized_capital,
                    avg_utilized_capital, utilization_pct,
                    return_on_utilized_capital_pct, alert_mode, error)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (r["id"], "strategy", r["strategy_name"], json.dumps(portfolio_config),
                 r["start_date"], r["initial_capital"], 1,
                 r["status"], r["created_at"], r["updated_at"],
                 r.get("last_evaluated"), r.get("last_nav"), r.get("last_return_pct"),
                 r.get("last_alpha_pct"), r.get("last_benchmark_return_pct"),
                 r.get("last_sharpe_ratio"), r.get("last_ann_volatility_pct"),
                 r.get("rolling_vol_30d_pct"), r.get("total_trades"),
                 r.get("open_positions"), r.get("current_utilization_pct"),
                 r.get("peak_utilized_capital"), r.get("avg_utilized_capital"),
                 r.get("utilization_pct"), r.get("return_on_utilized_capital_pct"),
                 r.get("alert_mode", 0), r.get("error")),
            )
            migrated += 1
        print(f"Migrated {migrated} strategy deployments")

    migrated_p = 0
    if "portfolio_deployments" in tables:
        rows = conn.execute("SELECT * FROM portfolio_deployments").fetchall()
        for r in rows:
            r = dict(r)
            existing = conn.execute("SELECT id FROM deployments WHERE id = ?", (r["id"],)).fetchone()
            if existing:
                continue
            config = json.loads(r["config_json"])
            num_sleeves = len(config.get("strategies", config.get("sleeves", [])))
            conn.execute(
                """INSERT OR IGNORE INTO deployments
                   (id, type, name, config_json, start_date, initial_capital,
                    num_sleeves, status, created_at, updated_at,
                    last_evaluated, last_nav, last_return_pct,
                    last_alpha_pct, last_benchmark_return_pct,
                    last_sharpe_ratio, last_max_drawdown_pct,
                    active_regimes, sleeve_summary,
                    alert_mode, error)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (r["id"], "portfolio", r["portfolio_name"], r["config_json"],
                 r["start_date"], r["initial_capital"], num_sleeves,
                 r["status"], r["created_at"], r["updated_at"],
                 r.get("last_evaluated"), r.get("last_nav"), r.get("last_return_pct"),
                 r.get("last_alpha_pct"), r.get("last_benchmark_return_pct"),
                 r.get("last_sharpe_ratio"), r.get("last_max_drawdown_pct"),
                 r.get("active_regimes"), r.get("sleeve_summary"),
                 r.get("alert_mode", 0), r.get("error")),
            )
            migrated_p += 1
        print(f"Migrated {migrated_p} portfolio deployments")

    # Migrate backtest runs
    migrated_bt = 0

    # Add 'type' column to old backtest_runs if it exists without it
    if "backtest_runs" in tables:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(backtest_runs)").fetchall()}
        for col, typ in [("type", "TEXT DEFAULT 'strategy'"), ("name", "TEXT DEFAULT ''"),
                         ("config_json", "TEXT"), ("num_sleeves", "INTEGER DEFAULT 1"),
                         ("per_sleeve_json", "TEXT"), ("start_date", "TEXT"),
                         ("end_date", "TEXT"), ("initial_capital", "REAL"),
                         ("created_at", "TEXT")]:
            if col not in cols:
                try:
                    conn.execute(f"ALTER TABLE backtest_runs ADD COLUMN {col} {typ}")
                except sqlite3.OperationalError:
                    pass
        conn.commit()

    if "portfolio_backtest_runs" in tables:
        try:
            rows = conn.execute("SELECT * FROM portfolio_backtest_runs").fetchall()
            for r in rows:
                r = dict(r)
                existing = conn.execute("SELECT run_id FROM backtest_runs WHERE run_id = ?", (r["run_id"],)).fetchone()
                if existing:
                    continue
                conn.execute(
                    """INSERT OR IGNORE INTO backtest_runs
                       (run_id, type, name, config_json, start_date, end_date,
                        initial_capital, final_nav, total_return_pct,
                        annualized_return_pct, annualized_volatility_pct,
                        max_drawdown_pct, max_drawdown_date,
                        sharpe_ratio, sortino_ratio, calmar_ratio, profit_factor,
                        total_entries, closed_trades, wins, losses, win_rate_pct,
                        avg_holding_days, utilization_pct, trading_days,
                        benchmark_return_pct, alpha_ann_pct,
                        num_sleeves, per_sleeve_json, results_path, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (r["run_id"], "portfolio", r["portfolio_name"], r.get("config_json"),
                     r["start_date"], r["end_date"], r["initial_capital"],
                     r.get("final_nav"), r.get("total_return_pct"),
                     r.get("annualized_return_pct"), r.get("annualized_volatility_pct"),
                     r.get("max_drawdown_pct"), r.get("max_drawdown_date"),
                     r.get("sharpe_ratio"), r.get("sortino_ratio"),
                     r.get("calmar_ratio"), r.get("profit_factor"),
                     r.get("total_entries"), r.get("closed_trades"),
                     r.get("wins"), r.get("losses"), r.get("win_rate_pct"),
                     r.get("avg_holding_days"), r.get("utilization_pct"),
                     r.get("trading_days"), r.get("benchmark_return_pct"),
                     r.get("alpha_ann_pct"), r.get("num_sleeves"),
                     r.get("per_sleeve_json"), r.get("results_path"),
                     r["created_at"]),
                )
                migrated_bt += 1
            print(f"Migrated {migrated_bt} portfolio backtest runs")
        except sqlite3.OperationalError as e:
            print(f"  Skipped backtest_runs migration (schema mismatch: {e})"
                  f" — old backtest_runs table needs manual migration or drop+recreate")

    conn.commit()
    conn.close()
    print(f"Migration complete: {migrated + migrated_p} deployments, {migrated_bt} backtest runs")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="AlphaScout Deployment Engine v2")
    sub = parser.add_subparsers(dest="command")

    p_deploy = sub.add_parser("deploy", help="Deploy a strategy or portfolio")
    p_deploy.add_argument("config", help="Path to strategy or portfolio config JSON")
    p_deploy.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p_deploy.add_argument("--capital", type=float, required=True, help="Initial capital")
    p_deploy.add_argument("--name", help="Override name")

    sub.add_parser("evaluate", help="Evaluate all active deployments")

    p_list = sub.add_parser("list", help="List deployments")
    p_list.add_argument("--all", action="store_true", help="Include stopped")
    p_list.add_argument("--type", choices=["strategy", "portfolio"], help="Filter by type")

    p_stop = sub.add_parser("stop", help="Stop a deployment")
    p_stop.add_argument("id")

    p_pause = sub.add_parser("pause", help="Pause a deployment")
    p_pause.add_argument("id")

    p_resume = sub.add_parser("resume", help="Resume a deployment")
    p_resume.add_argument("id")

    p_status = sub.add_parser("status", help="Get deployment details")
    p_status.add_argument("id")

    sub.add_parser("migrate", help="Migrate data from v1 tables")

    args = parser.parse_args()

    if args.command == "deploy":
        deploy(args.config, args.start, args.capital, args.name)
    elif args.command == "evaluate":
        evaluate_all_regimes()
        evaluate_all()
    elif args.command == "list":
        deployments = list_deployments(include_stopped=getattr(args, 'all', False),
                                        deploy_type=getattr(args, 'type', None))
        if not deployments:
            print("No deployments found.")
            return
        for d in deployments:
            nav_str = f"${d['last_nav']:,.0f}" if d.get('last_nav') else "—"
            ret_str = f"{d['last_return_pct']:+.1f}%" if d.get('last_return_pct') else "—"
            print(f"  [{d['status']:>7}] [{d['type']:>9}] {d['id']}")
            print(f"           {d['name']} | Start: {d['start_date']} | Capital: ${d['initial_capital']:,.0f} | Sleeves: {d['num_sleeves']}")
            print(f"           NAV: {nav_str} ({ret_str}) | Trades: {d.get('total_trades', 0)} | Open: {d.get('open_positions', 0)}")
            if d.get('error'):
                print(f"           Error: {d['error']}")
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
        print(json.dumps(d, indent=2, default=str))
    elif args.command == "migrate":
        migrate_from_v1()
    else:
        parser.print_help()


# ---------------------------------------------------------------------------
# Regime Deployments (standalone regime monitoring)
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
    row = conn.execute("SELECT regime_id, name, config FROM regimes WHERE regime_id = ?", (regime_id,)).fetchone()
    if not row:
        conn.close()
        raise ValueError(f"Regime {regime_id} not found")

    regime_name = name or row["name"]
    config = json.loads(row["config"])
    deploy_id = f"regime_{regime_id}"

    existing = conn.execute("SELECT id, status FROM regime_deployments WHERE id = ?", (deploy_id,)).fetchone()
    if existing:
        if existing["status"] == "active":
            conn.close()
            return {"id": deploy_id, "regime_name": regime_name, "status": "already_active"}
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("UPDATE regime_deployments SET status = 'active', updated_at = ? WHERE id = ?", (now, deploy_id))
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
    evaluate_regime_one(deploy_id)
    return {"id": deploy_id, "regime_name": regime_name, "regime_id": regime_id, "status": "active"}


def evaluate_regime_one(deploy_id: str) -> dict | None:
    """Evaluate a single regime deployment."""
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
        from regime import get_regime_details
        detail_result = get_regime_details(today, [config])
        regime_name = config["name"]
        regime_detail = detail_result["regimes"].get(regime_name, {})
        current_state = regime_detail.get("active", False)
        detail = json.dumps(regime_detail)

        now = datetime.now(timezone.utc).isoformat()

        conn.execute(
            "INSERT OR REPLACE INTO regime_state_history (deployment_id, date, is_active) VALUES (?, ?, ?)",
            (deploy_id, today, 1 if current_state else 0),
        )

        total_days = (row["total_evaluated_days"] or 0) + 1
        total_active = (row["total_active_days"] or 0) + (1 if current_state else 0)
        last_activated = row["last_activated_date"]
        last_deactivated = row["last_deactivated_date"]

        if previous_state is not None and current_state and not previous_state:
            last_activated = today
        if previous_state is not None and not current_state and previous_state:
            last_deactivated = today
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
             total_days, total_active, last_activated, last_deactivated, deploy_id),
        )

        if row["alert_mode"] and previous_state is not None and current_state != previous_state:
            transition = "activated" if current_state else "deactivated"
            alert_id = hashlib.md5(f"{deploy_id}:{today}:{transition}".encode()).hexdigest()[:12]
            existing_alert = conn.execute("SELECT id FROM regime_alerts WHERE id = ?", (alert_id,)).fetchone()
            if not existing_alert:
                conn.execute(
                    """INSERT INTO regime_alerts
                       (id, deployment_id, date, transition, regime_name, detail, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (alert_id, deploy_id, today, transition, row["regime_name"], detail, now),
                )

        conn.commit()
        conn.close()

        status_icon = "🟢" if current_state else "⚪"
        print(f"  {status_icon} {deploy_id}: {row['regime_name']} = {'ACTIVE' if current_state else 'INACTIVE'}")
        return {"id": deploy_id, "regime_name": row["regime_name"], "is_active": current_state, "date": today}

    except Exception as e:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("UPDATE regime_deployments SET updated_at = ?, error = ? WHERE id = ?", (now, str(e), deploy_id))
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
    conn = _get_regime_deploy_db()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE regime_deployments SET alert_mode = ?, updated_at = ? WHERE id = ?",
                 (1 if enabled else 0, now, deploy_id))
    conn.commit()
    row = conn.execute("SELECT id, regime_name, alert_mode FROM regime_deployments WHERE id = ?", (deploy_id,)).fetchone()
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
    total_days = result.get("total_evaluated_days") or 0
    total_active = result.get("total_active_days") or 0
    result["active_pct"] = round(total_active / max(total_days, 1) * 100, 1)
    if include_history:
        rows = conn.execute(
            "SELECT date, is_active FROM regime_state_history WHERE deployment_id = ? ORDER BY date",
            (deploy_id,),
        ).fetchall()
        result["state_history"] = [{"date": r["date"], "is_active": bool(r["is_active"])} for r in rows]
    conn.close()
    return result


def get_regime_alerts(deploy_id: str = None, date: str = None, limit: int = 50) -> list[dict]:
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
# Compatibility aliases for api.py (v1 portfolio function names → v2 unified)
# ---------------------------------------------------------------------------

def deploy_portfolio(portfolio_config, start_date, capital, name=None):
    """Deploy a portfolio. Alias for deploy()."""
    return deploy(portfolio_config, start_date, capital, name)

def evaluate_portfolio_one(deploy_id):
    """Evaluate a portfolio deployment. Alias for evaluate_one()."""
    return evaluate_one(deploy_id)

def list_portfolio_deployments(include_stopped=False, portfolio_id=None):
    """List portfolio deployments. Alias for list_deployments()."""
    return list_deployments(include_stopped=include_stopped, deploy_type="portfolio")

def get_portfolio_deployment(deploy_id):
    """Get a portfolio deployment. Alias for get_deployment()."""
    return get_deployment(deploy_id)

def stop_portfolio(deploy_id):
    return stop_deployment(deploy_id)

def pause_portfolio(deploy_id):
    return pause_deployment(deploy_id)

def resume_portfolio(deploy_id):
    return resume_deployment(deploy_id)

def set_portfolio_alert_mode(deploy_id, enabled):
    return set_alert_mode(deploy_id, enabled)

def _get_portfolio_db():
    """Alias for get_db()."""
    return get_db()


if __name__ == "__main__":
    main()
