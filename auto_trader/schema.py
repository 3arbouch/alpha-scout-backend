"""
Experiment logging schema and helpers.

Every auto-trader iteration is logged as an experiment row.
Stores the thesis, portfolio config, backtest metrics, and KEEP/DISCARD decision.
"""

import os
import sys
import json
import sqlite3
import hashlib
from datetime import datetime, timezone
from pathlib import Path

APP_DB_PATH = Path(os.environ.get("APP_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "app.db")))

def get_db():
    """Get a connection to the app database with all tables ensured."""
    sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
    from schema import init_db
    conn = sqlite3.connect(str(APP_DB_PATH))
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def generate_experiment_id(run_id: str, iteration: int) -> str:
    raw = f"{run_id}:{iteration}:{datetime.now(timezone.utc).isoformat()}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def log_experiment(
    run_id: str,
    iteration: int,
    thesis: str,
    assumptions: list[str],
    portfolio_config: dict,
    metrics: dict,
    target_metric: str,
    target_value: float,
    conditions: list[dict],
    conditions_met: bool,
    decision: str,
    best_value_so_far: float,
    backtest_start: str,
    backtest_end: str,
    initial_capital: float,
    model: str = None,
    session_id: str = None,
    tokens_used: int = None,
    duration_seconds: float = None,
    error: str = None,
    portfolio_id: str = None,
    lessons: str = None,
) -> str:
    """Log a single experiment. Returns the experiment ID.

    `lessons` is the agent's free-text reflection on prior experiments. Stored
    for UI display only — it is NOT surfaced in build_history_context, so
    subsequent iterations are not anchored by prior self-interpretation.
    """
    exp_id = generate_experiment_id(run_id, iteration)
    now = datetime.now(timezone.utc).isoformat()

    improvement = None
    if best_value_so_far and best_value_so_far != 0 and target_value is not None:
        improvement = ((target_value - best_value_so_far) / abs(best_value_so_far)) * 100

    conn = get_db()
    conn.execute(
        """INSERT INTO experiments
           (id, run_id, iteration, thesis, assumptions, lessons, portfolio_id, portfolio_config,
            target_metric, target_value, conditions, conditions_met,
            total_return_pct, annualized_return_pct,
            sharpe_ratio, sharpe_basis, sharpe_ratio_annualized, sharpe_ratio_period,
            sortino_ratio,
            max_drawdown_pct, annualized_volatility_pct, alpha_ann_pct,
            alpha_vs_market_pct, alpha_vs_sector_pct,
            market_benchmark_return_pct, market_benchmark_ann_return_pct,
            sector_benchmark_return_pct, sector_benchmark_ann_return_pct,
            profit_factor, win_rate_pct, total_trades,
            decision, best_value_so_far, improvement_pct,
            backtest_start, backtest_end, initial_capital,
            model, session_id, tokens_used, duration_seconds, error, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (exp_id, run_id, iteration,
         thesis, json.dumps(assumptions), lessons, portfolio_id, json.dumps(portfolio_config),
         target_metric, target_value, json.dumps(conditions), 1 if conditions_met else 0,
         metrics.get("total_return_pct"), metrics.get("annualized_return_pct"),
         metrics.get("sharpe_ratio"),
         metrics.get("sharpe_basis"),
         metrics.get("sharpe_ratio_annualized"),
         metrics.get("sharpe_ratio_period"),
         metrics.get("sortino_ratio"),
         metrics.get("max_drawdown_pct"), metrics.get("annualized_volatility_pct"),
         metrics.get("alpha_ann_pct"),
         metrics.get("alpha_vs_market_pct"), metrics.get("alpha_vs_sector_pct"),
         metrics.get("market_benchmark_return_pct"), metrics.get("market_benchmark_ann_return_pct"),
         metrics.get("sector_benchmark_return_pct"), metrics.get("sector_benchmark_ann_return_pct"),
         metrics.get("profit_factor"),
         metrics.get("win_rate_pct"), metrics.get("total_trades"),
         decision, best_value_so_far, improvement,
         backtest_start, backtest_end, initial_capital,
         model, session_id, tokens_used, duration_seconds, error, now),
    )
    conn.commit()
    conn.close()
    return exp_id


def get_experiment_history(run_id: str, limit: int = 20) -> list[dict]:
    """Get past experiments for a run, most recent first."""
    conn = get_db()
    rows = conn.execute(
        """SELECT id, iteration, thesis, assumptions, portfolio_config,
                  target_metric, target_value, conditions_met,
                  sharpe_ratio, alpha_ann_pct, annualized_volatility_pct,
                  max_drawdown_pct, total_return_pct, annualized_return_pct,
                  decision, best_value_so_far, improvement_pct, error
           FROM experiments
           WHERE run_id = ?
           ORDER BY iteration DESC
           LIMIT ?""",
        (run_id, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_lessons(run_id: str, limit: int = 3) -> list[dict]:
    """Most recent N experiments' lessons (skips rows where lessons is NULL/empty).

    Separated from get_experiment_history (which deliberately excludes the
    lessons column to avoid biasing the agent on aggregated self-interpretation):
    this helper surfaces only the LAST few lessons so the agent has its most
    recent reflections in context for the next iteration. Returns most-recent
    first; iterations are not necessarily contiguous if some had null lessons.
    """
    conn = get_db()
    rows = conn.execute(
        """SELECT iteration, lessons
           FROM experiments
           WHERE run_id = ?
             AND lessons IS NOT NULL
             AND TRIM(lessons) != ''
           ORDER BY iteration DESC
           LIMIT ?""",
        (run_id, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_best_experiment(run_id: str, higher_is_better: bool = True) -> dict | None:
    """Get the best KEEP experiment for a run."""
    order = "DESC" if higher_is_better else "ASC"
    conn = get_db()
    row = conn.execute(
        f"""SELECT * FROM experiments
           WHERE run_id = ? AND decision = 'keep'
           ORDER BY target_value {order}
           LIMIT 1""",
        (run_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_run_summary(run_id: str) -> dict:
    """Summary stats for a run."""
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM experiments WHERE run_id = ?", (run_id,)).fetchone()[0]
    keeps = conn.execute("SELECT COUNT(*) FROM experiments WHERE run_id = ? AND decision = 'keep'", (run_id,)).fetchone()[0]
    discards = conn.execute("SELECT COUNT(*) FROM experiments WHERE run_id = ? AND decision = 'discard'", (run_id,)).fetchone()[0]
    errors = conn.execute("SELECT COUNT(*) FROM experiments WHERE run_id = ? AND error IS NOT NULL", (run_id,)).fetchone()[0]
    best = conn.execute(
        "SELECT MAX(target_value) FROM experiments WHERE run_id = ? AND decision = 'keep'",
        (run_id,),
    ).fetchone()[0]
    conn.close()
    return {
        "run_id": run_id,
        "total_experiments": total,
        "keeps": keeps,
        "discards": discards,
        "errors": errors,
        "best_value": best,
    }
