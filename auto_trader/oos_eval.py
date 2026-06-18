#!/usr/bin/env python3
"""
Run OOS evaluation
==================
Re-run a run's *kept* experiments (decision='keep') over a single user-selected
window and compare each survivor's out-of-sample result to its in-sample
(training) result.

Two phases:
  - create_eval(): synchronous. Inserts the batch + one pending result row per
    survivor, returns the eval_id immediately (the API returns this).
  - run_eval(): the worker. Backtests every survivor over the window IN PARALLEL
    (ProcessPoolExecutor — backtests are CPU-bound), writes results, computes the
    cohort diagnostics (IS↔OOS Spearman rank-corr, OOS Sharpe mean/std), flips
    the batch to 'done'. Spawned as a detached subprocess by the API.

Engine is always v2 — no engine_version branching.
"""

import os
import sys
import json
import math
import uuid
import sqlite3
import contextlib
import io
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))
sys.path.insert(0, str(SCRIPT_DIR.parent / "scripts"))

from auto_trader.schema import get_db, APP_DB_PATH  # noqa: E402

# The 19 canonical metrics promoted to typed columns — mirrors the `experiments`
# table so IS↔OOS is a direct column-to-column comparison.
CANON_METRICS = [
    "total_return_pct", "annualized_return_pct",
    "sharpe_ratio", "sharpe_basis", "sharpe_ratio_annualized", "sharpe_ratio_period",
    "sortino_ratio", "max_drawdown_pct", "annualized_volatility_pct", "alpha_ann_pct",
    "alpha_vs_market_pct", "alpha_vs_sector_pct",
    "market_benchmark_return_pct", "market_benchmark_ann_return_pct",
    "sector_benchmark_return_pct", "sector_benchmark_ann_return_pct",
    "profit_factor", "win_rate_pct", "total_trades",
]
# Series kept in raw_result_json (out of summary responses) — what a frontend
# needs to plot OOS equity curves + trades. We omit ranking_history (per-date
# candidate leaderboards) and combined_nav_history (duplicates nav_history), and
# slim each nav_history point to its time-series scalars (see NAV_POINT_FIELDS) —
# the full per-day position book is the bloat (~900KB) and isn't an OOS result.
RAW_SERIES_KEYS = [
    "nav_history", "trades",
    "benchmark", "benchmark_market", "benchmark_sector",
    "regime_history", "allocation_history",
]
NAV_POINT_FIELDS = ["date", "nav", "cash", "positions_value", "num_positions",
                    "daily_pnl", "daily_pnl_pct"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finite(x):
    """Non-finite floats (NaN/Inf) → None so the row is JSON-serializable."""
    if isinstance(x, float) and not math.isfinite(x):
        return None
    return x


def _clean(obj):
    """Recursively replace non-finite floats with None so stored JSON is strict-
    valid (FastAPI / strict parsers reject NaN/Inf)."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(v) for v in obj]
    return obj


def _spearman(xs, ys):
    """Spearman rank correlation. Manual (rank → Pearson on ranks) to avoid a
    scipy dependency. Returns None if < 3 points or no variance."""
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(pairs) < 3:
        return None

    def rank(vals):
        order = sorted(range(len(vals)), key=lambda i: vals[i])
        r = [0.0] * len(vals)
        i = 0
        while i < len(vals):
            j = i
            while j + 1 < len(vals) and vals[order[j + 1]] == vals[order[i]]:
                j += 1
            avg = (i + j) / 2.0 + 1.0  # 1-based average rank for ties
            for k in range(i, j + 1):
                r[order[k]] = avg
            i = j + 1
        return r

    rx, ry = rank([p[0] for p in pairs]), rank([p[1] for p in pairs])
    n = len(pairs)
    mx, my = sum(rx) / n, sum(ry) / n
    cov = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    vx = math.sqrt(sum((a - mx) ** 2 for a in rx))
    vy = math.sqrt(sum((b - my) ** 2 for b in ry))
    if vx == 0 or vy == 0:
        return None
    return round(cov / (vx * vy), 4)


def _load_config(conn, exp) -> dict | None:
    """Resolve a survivor's portfolio config — prefer the linked portfolios row,
    fall back to the legacy inline portfolio_config."""
    pid = exp["portfolio_id"]
    if pid:
        row = conn.execute("SELECT config FROM portfolios WHERE portfolio_id = ?", (pid,)).fetchone()
        if row and row["config"]:
            return json.loads(row["config"])
    if exp["portfolio_config"]:
        return json.loads(exp["portfolio_config"])
    return None


def _windows_overlap(a_start, a_end, b_start, b_end) -> bool:
    """Strict overlap — windows that merely touch at a boundary (OOS starting on
    the training end date) are NOT overlapping; that's a clean forward window."""
    if not all([a_start, a_end, b_start, b_end]):
        return False
    return a_start < b_end and b_start < a_end


# ---------------------------------------------------------------------------
# Phase 1 — create (synchronous; API returns the eval_id)
# ---------------------------------------------------------------------------
def create_eval(run_id: str, eval_start: str, eval_end: str,
                recompute_is: bool = False) -> dict:
    """Create the batch + one pending result row per kept survivor.

    Raises ValueError on a missing run or a run with no kept survivors.
    """
    conn = get_db()
    try:
        run = conn.execute("SELECT id FROM auto_trader_runs WHERE id = ?", (run_id,)).fetchone()
        if not run:
            raise ValueError(f"Run '{run_id}' not found")

        survivors = conn.execute(
            """SELECT id, iteration, portfolio_id, portfolio_config,
                      backtest_start, backtest_end
               FROM experiments
               WHERE run_id = ? AND decision = 'keep'
               ORDER BY iteration""",
            (run_id,),
        ).fetchall()
        if not survivors:
            raise ValueError(f"Run '{run_id}' has no kept (decision='keep') experiments")

        eval_id = uuid.uuid4().hex[:16]
        now = _now()
        conn.execute(
            """INSERT INTO run_oos_evals
               (id, run_id, eval_start, eval_end, recompute_is, status,
                n_experiments, n_done, n_error, engine_version, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (eval_id, run_id, eval_start, eval_end, 1 if recompute_is else 0,
             "running", len(survivors), 0, 0, "v2", now, now),
        )
        for ex in survivors:
            overlaps = _windows_overlap(eval_start, eval_end,
                                        ex["backtest_start"], ex["backtest_end"])
            conn.execute(
                """INSERT INTO experiment_oos_results
                   (id, eval_id, run_id, experiment_id, portfolio_id, iteration,
                    overlaps_training, status, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (uuid.uuid4().hex[:16], eval_id, run_id, ex["id"], ex["portfolio_id"],
                 ex["iteration"], 1 if overlaps else 0, "pending", now, now),
            )
        conn.commit()
        return {"eval_id": eval_id, "run_id": run_id, "n_experiments": len(survivors),
                "status": "running"}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Parallel worker (module-level → picklable for ProcessPoolExecutor)
# ---------------------------------------------------------------------------
def _backtest_one(task: dict) -> dict:
    """Run one survivor's OOS backtest (and optionally recompute IS). Returns a
    result dict; never raises — failures come back as {'error': ...}."""
    try:
        sys.path.insert(0, str(SCRIPT_DIR.parent / "scripts"))
        from portfolio_engine_v2 import run_portfolio_backtest  # v2 only
        import copy

        base = task["config"]
        bt = dict(base.get("backtest") or {})
        capital = bt.get("initial_capital", 10_000_000)
        slippage = bt.get("slippage_bps", 10)

        def _run(start, end):
            c = copy.deepcopy(base)
            c["engine_version"] = "v2"
            c["backtest"] = {"start": start, "end": end,
                             "initial_capital": capital, "slippage_bps": slippage}
            with contextlib.redirect_stdout(io.StringIO()):
                return run_portfolio_backtest(c, force_close_at_end=False)

        oos = _run(task["eval_start"], task["eval_end"])
        raw = {k: oos.get(k) for k in RAW_SERIES_KEYS if k in oos}
        if isinstance(raw.get("nav_history"), list):
            raw["nav_history"] = [{f: pt.get(f) for f in NAV_POINT_FIELDS}
                                  for pt in raw["nav_history"]]
        out = {
            "result_id": task["result_id"],
            "oos_metrics": oos.get("metrics", {}),
            "raw": raw,
        }
        if task.get("recompute_is") and task.get("is_start") and task.get("is_end"):
            is_res = _run(task["is_start"], task["is_end"])
            out["is_metrics"] = is_res.get("metrics", {})
        return out
    except Exception as e:  # noqa: BLE001 — failures are per-survivor, not fatal
        import traceback
        return {"result_id": task["result_id"], "error": f"{e}\n{traceback.format_exc()}"}


# ---------------------------------------------------------------------------
# Phase 2 — run (the spawned subprocess; parallel + writes results)
# ---------------------------------------------------------------------------
def run_eval(eval_id: str, max_workers: int | None = None) -> None:
    # ---- gather tasks (read, then release the connection during the pool) ----
    conn = get_db()
    batch = conn.execute("SELECT * FROM run_oos_evals WHERE id = ?", (eval_id,)).fetchone()
    if not batch:
        conn.close()
        raise ValueError(f"OOS eval '{eval_id}' not found")
    recompute_is = bool(batch["recompute_is"])
    rows = conn.execute(
        "SELECT * FROM experiment_oos_results WHERE eval_id = ? AND status = 'pending'",
        (eval_id,),
    ).fetchall()

    # IS Sharpe baseline per experiment (for deltas + rank-corr) — from the
    # experiments table (the numbers the survivor was actually selected on).
    is_sharpe = {}
    tasks = []
    for r in rows:
        ex = conn.execute(
            """SELECT id, portfolio_id, portfolio_config, backtest_start, backtest_end,
                      sharpe_ratio_annualized
               FROM experiments WHERE id = ?""", (r["experiment_id"],)).fetchone()
        is_sharpe[r["id"]] = ex["sharpe_ratio_annualized"] if ex else None
        cfg = _load_config(conn, ex) if ex else None
        if cfg is None:
            _mark_result_error(conn, r["id"], "no portfolio config found for experiment")
            continue
        tasks.append({
            "result_id": r["id"], "config": cfg,
            "eval_start": batch["eval_start"], "eval_end": batch["eval_end"],
            "recompute_is": recompute_is,
            "is_start": ex["backtest_start"], "is_end": ex["backtest_end"],
        })
    conn.commit()
    conn.close()

    if not tasks:
        _finalize(eval_id)
        return

    import multiprocessing as mp
    workers = max_workers or min(len(tasks), max(1, (mp.cpu_count() or 2) - 1), 8)

    # ---- run backtests in parallel ----
    results = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for res in pool.map(_backtest_one, tasks):
            results.append(res)

    # ---- write results (parent serializes all SQLite writes) ----
    conn = get_db()
    for res in results:
        if "error" in res:
            _mark_result_error(conn, res["result_id"], res["error"])
            continue
        _write_result(conn, res, is_sharpe.get(res["result_id"]), recompute_is)
    conn.commit()
    conn.close()

    _finalize(eval_id)


def _mark_result_error(conn, result_id: str, msg: str) -> None:
    conn.execute(
        "UPDATE experiment_oos_results SET status='error', error=?, updated_at=? WHERE id=?",
        (msg[:2000], _now(), result_id),
    )


def _write_result(conn, res: dict, is_sharpe_ann, recompute_is: bool) -> None:
    m = res["oos_metrics"]
    # If IS was recomputed, that overrides the stored baseline for the delta.
    if recompute_is and res.get("is_metrics"):
        is_sharpe_ann = res["is_metrics"].get("sharpe_ratio_annualized")

    oos_sharpe_ann = _finite(m.get("sharpe_ratio_annualized"))
    d_sharpe = None
    retention = None
    if oos_sharpe_ann is not None and is_sharpe_ann is not None:
        d_sharpe = round(oos_sharpe_ann - is_sharpe_ann, 4)
        if is_sharpe_ann != 0:
            retention = round(oos_sharpe_ann / is_sharpe_ann, 4)

    cols = {f"oos_{k}": _finite(m.get(k)) for k in CANON_METRICS}
    cols.update({
        "status": "done", "error": None,
        "d_sharpe_ann": d_sharpe, "sharpe_retention": retention,
        "oos_metrics_json": json.dumps(_clean(m)),
        "is_metrics_json": json.dumps(_clean(res["is_metrics"])) if res.get("is_metrics") else None,
        "raw_result_json": json.dumps(_clean(res["raw"])),
        "updated_at": _now(),
    })
    sets = ", ".join(f"{c}=?" for c in cols)
    conn.execute(f"UPDATE experiment_oos_results SET {sets} WHERE id=?",
                 (*cols.values(), res["result_id"]))


def _finalize(eval_id: str) -> None:
    """Recompute counts + cohort diagnostics and flip status to done."""
    conn = get_db()
    rows = conn.execute(
        """SELECT r.status, r.oos_sharpe_ratio_annualized AS oos_sharpe,
                  e.sharpe_ratio_annualized AS is_sharpe, r.is_metrics_json
           FROM experiment_oos_results r
           JOIN experiments e ON e.id = r.experiment_id
           WHERE r.eval_id = ?""", (eval_id,)).fetchall()
    done = [r for r in rows if r["status"] == "done"]
    n_done = len(done)
    n_error = sum(1 for r in rows if r["status"] == "error")

    oos_sharpes = [r["oos_sharpe"] for r in done if r["oos_sharpe"] is not None]
    # IS Sharpe for the rank-corr: recomputed value if present, else stored.
    is_vals, oos_vals = [], []
    for r in done:
        is_s = r["is_sharpe"]
        if r["is_metrics_json"]:
            try:
                is_s = json.loads(r["is_metrics_json"]).get("sharpe_ratio_annualized", is_s)
            except (json.JSONDecodeError, TypeError):
                pass
        if is_s is not None and r["oos_sharpe"] is not None:
            is_vals.append(is_s)
            oos_vals.append(r["oos_sharpe"])

    mean = round(sum(oos_sharpes) / len(oos_sharpes), 4) if oos_sharpes else None
    std = None
    if len(oos_sharpes) >= 2:
        std = round((sum((x - mean) ** 2 for x in oos_sharpes) / (len(oos_sharpes) - 1)) ** 0.5, 4)

    conn.execute(
        """UPDATE run_oos_evals
           SET status='done', n_done=?, n_error=?,
               is_oos_rank_corr=?, oos_sharpe_mean=?, oos_sharpe_std=?, updated_at=?
           WHERE id=?""",
        (n_done, n_error, _spearman(is_vals, oos_vals), mean, std, _now(), eval_id),
    )
    conn.commit()
    conn.close()


def _mark_batch_error(eval_id: str, msg: str) -> None:
    conn = get_db()
    conn.execute("UPDATE run_oos_evals SET status='error', error=?, updated_at=? WHERE id=?",
                 (msg[:2000], _now(), eval_id))
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# CLI — spawned by the API: `python3 oos_eval.py run <eval_id>`
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "run":
        eid = sys.argv[2]
        try:
            run_eval(eid)
        except Exception as exc:  # noqa: BLE001
            import traceback
            _mark_batch_error(eid, f"{exc}\n{traceback.format_exc()}")
            raise
    else:
        print("usage: oos_eval.py run <eval_id>", file=sys.stderr)
        sys.exit(2)
