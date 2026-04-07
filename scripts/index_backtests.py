#!/usr/bin/env python3
"""
Index backtest results into SQLite for cross-run querying.

Usage:
    python3 index_backtests.py              # Index all results
    python3 index_backtests.py --status     # Show table stats
    python3 index_backtests.py --reindex    # Drop and rebuild
"""

import os, sys, json, sqlite3, argparse, time
from pathlib import Path
from datetime import datetime

DB_PATH = Path(os.environ.get("DB_PATH", "/app/data/alphascout.db"))
RESULTS_DIR = Path(os.environ.get("WORKSPACE", "/app")) / "backtest" / "results"

SCHEMA = """
CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id              TEXT PRIMARY KEY,
    strategy_name       TEXT,
    author_id           TEXT,
    author_name         TEXT,
    created_at          TEXT,
    universe_type       TEXT,
    universe_detail     TEXT,
    entry_type          TEXT,
    entry_threshold     REAL,
    entry_window        INTEGER,
    stop_loss           REAL,
    take_profit         REAL,
    time_stop           INTEGER,
    max_positions       INTEGER,
    capital             REAL,
    rebalance_freq      TEXT,
    start_date          TEXT,
    end_date            TEXT,
    slippage_bps        REAL,
    total_return        REAL,
    ann_return          REAL,
    alpha               REAL,
    max_drawdown        REAL,
    max_drawdown_date   TEXT,
    sharpe              REAL,
    sortino             REAL,
    win_rate            REAL,
    profit_factor       REAL,
    total_trades        INTEGER,
    wins                INTEGER,
    losses              INTEGER,
    avg_win_pct         REAL,
    avg_loss_pct        REAL,
    avg_holding_days    REAL,
    final_nav           REAL,
    benchmark_return    REAL,
    peak_utilized_capital REAL,
    avg_utilized_capital REAL,
    utilization_pct     REAL,
    return_on_utilized_capital_pct REAL,
    has_report          INTEGER DEFAULT 0,
    has_analysis        INTEGER DEFAULT 0,
    has_charts          INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_br_strategy ON backtest_runs(strategy_name);
CREATE INDEX IF NOT EXISTS idx_br_universe ON backtest_runs(universe_type, universe_detail);
CREATE INDEX IF NOT EXISTS idx_br_alpha ON backtest_runs(alpha);
CREATE INDEX IF NOT EXISTS idx_br_sharpe ON backtest_runs(sharpe);
CREATE INDEX IF NOT EXISTS idx_br_created ON backtest_runs(created_at);
"""

INSERT_SQL = """
INSERT OR REPLACE INTO backtest_runs (
    run_id, strategy_id, strategy_name, author_id, author_name, created_at,
    universe_type, universe_detail, entry_type, entry_threshold, entry_window,
    stop_loss, take_profit, time_stop, max_positions, capital, rebalance_freq,
    start_date, end_date, slippage_bps,
    total_return, ann_return, alpha, max_drawdown, max_drawdown_date,
    sharpe, sortino, win_rate, profit_factor,
    total_trades, wins, losses, avg_win_pct, avg_loss_pct, avg_holding_days,
    final_nav, benchmark_return,
    peak_utilized_capital, avg_utilized_capital, utilization_pct, return_on_utilized_capital_pct,
    has_report, has_analysis, has_charts
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def extract_run_at(data: dict, run_id: str) -> str:
    """Extract timestamp from results or infer from run_id."""
    if data.get("run_at"):
        return data["run_at"]
    # Try to parse from run_id: ..._YYYYMMDD_HHMMSS
    parts = run_id.rsplit("_", 2)
    if len(parts) >= 3:
        try:
            dt = datetime.strptime(f"{parts[-2]}_{parts[-1]}", "%Y%m%d_%H%M%S")
            return dt.isoformat()
        except ValueError:
            pass
    return ""


def universe_detail(config: dict) -> str:
    """Extract universe detail string from config."""
    u = config.get("universe", {})
    utype = u.get("type", "all")
    if utype == "sector":
        return u.get("sector", "")
    elif utype == "symbols":
        symbols = u.get("symbols", [])
        return ",".join(symbols[:20])
    return "all"


def index_result(conn: sqlite3.Connection, filepath: Path):
    """Index a single backtest result JSON into the table."""
    try:
        data = json.loads(filepath.read_text())
    except (json.JSONDecodeError, OSError):
        return False

    run_id = filepath.stem
    config = data.get("config", {})
    metrics = data.get("metrics", {})
    entry = config.get("entry", {}).get("trigger", {})
    sizing = config.get("sizing", {})
    sl = config.get("stop_loss") or {}
    tp = config.get("take_profit") or {}
    ts = config.get("time_stop") or {}
    rebal = config.get("rebalancing", {})
    bt = config.get("backtest", {})
    author = config.get("author", {})

    has_report = 1 if (filepath.parent / f"{run_id}_report.md").exists() else 0
    has_analysis = 1 if (filepath.parent / f"{run_id}_analysis.json").exists() else 0
    has_charts = 1 if (filepath.parent / f"{run_id}_equity.png").exists() else 0

    # Compute strategy_id if not in config
    sid = config.get("strategy_id")
    if not sid:
        try:
            from backtest_engine import compute_strategy_id
            sid = compute_strategy_id(config)
        except ImportError:
            sid = None

    row = (
        run_id,
        sid,
        data.get("strategy", ""),
        author.get("id", ""),
        author.get("name", ""),
        extract_run_at(data, run_id),
        config.get("universe", {}).get("type", "all"),
        universe_detail(config),
        entry.get("type", ""),
        entry.get("threshold"),
        entry.get("window_days"),
        sl.get("value"),
        tp.get("value"),
        ts.get("max_days"),
        sizing.get("max_positions"),
        sizing.get("initial_allocation"),
        rebal.get("frequency", "none"),
        bt.get("start", ""),
        bt.get("end", ""),
        bt.get("slippage_bps"),
        metrics.get("total_return_pct"),
        metrics.get("annualized_return_pct"),
        metrics.get("alpha_ann_pct"),
        metrics.get("max_drawdown_pct"),
        metrics.get("max_drawdown_date", ""),
        metrics.get("sharpe_ratio"),
        metrics.get("sortino_ratio"),
        metrics.get("win_rate_pct"),
        metrics.get("profit_factor"),
        metrics.get("total_trades"),
        metrics.get("wins"),
        metrics.get("losses"),
        metrics.get("avg_win_pct"),
        metrics.get("avg_loss_pct"),
        metrics.get("avg_holding_days"),
        metrics.get("final_nav"),
        metrics.get("benchmark_return_pct"),
        metrics.get("peak_utilized_capital"),
        metrics.get("avg_utilized_capital"),
        metrics.get("utilization_pct"),
        metrics.get("return_on_utilized_capital_pct"),
        has_report,
        has_analysis,
        has_charts,
    )

    conn.execute(INSERT_SQL, row)
    return True


def index_all(conn: sqlite3.Connection):
    """Index all backtest results."""
    if not RESULTS_DIR.exists():
        print("No results directory found.")
        return

    count = 0
    skipped = 0
    for f in sorted(RESULTS_DIR.glob("*.json")):
        if f.name.endswith("_daily.json") or f.name.endswith("_analysis.json"):
            continue
        if index_result(conn, f):
            count += 1
        else:
            skipped += 1

    conn.commit()
    print(f"Indexed {count} backtest runs ({skipped} skipped)")


def show_status(conn: sqlite3.Connection):
    """Show backtest_runs table stats."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM backtest_runs")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT strategy_name) FROM backtest_runs")
        strategies = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT universe_detail) FROM backtest_runs")
        universes = cur.fetchone()[0]
        cur.execute("SELECT AVG(total_return), AVG(alpha), AVG(sharpe) FROM backtest_runs")
        avg_ret, avg_alpha, avg_sharpe = cur.fetchone()

        print(f"  === Backtest Runs Index ===")
        print(f"  Total runs:       {total}")
        print(f"  Strategies:       {strategies}")
        print(f"  Universes:        {universes}")
        print(f"  Avg return:       {avg_ret:.1f}%" if avg_ret else "  Avg return:       —")
        print(f"  Avg alpha:        {avg_alpha:.1f}%" if avg_alpha else "  Avg alpha:        —")
        print(f"  Avg Sharpe:       {avg_sharpe:.2f}" if avg_sharpe else "  Avg Sharpe:       —")
    except sqlite3.OperationalError:
        print("  Table not created yet.")


def main():
    parser = argparse.ArgumentParser(description="Index backtest results into SQLite")
    parser.add_argument("--status", action="store_true", help="Show table stats")
    parser.add_argument("--reindex", action="store_true", help="Drop and rebuild")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))

    if args.status:
        show_status(conn)
        conn.close()
        return

    if args.reindex:
        conn.execute("DROP TABLE IF EXISTS backtest_runs")

    conn.executescript(SCHEMA)
    index_all(conn)
    show_status(conn)
    conn.close()


if __name__ == "__main__":
    main()
