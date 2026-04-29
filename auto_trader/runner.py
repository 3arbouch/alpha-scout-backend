#!/usr/bin/env python3
"""
Auto-Trader Runner — autonomous portfolio research loop.

Runs N experiments:
  1. Agent researches market data (via Bash + data-query skill)
  2. Agent outputs thesis + portfolio config
  3. Runner backtests the portfolio
  4. Runner scores: metric improved + conditions met? → KEEP or DISCARD
  5. Runner logs the experiment
  6. Repeat with history context

Usage:
    python3 auto_trader/runner.py --max-experiments 10 --metric sharpe_ratio \
        --condition "alpha_ann_pct > 0" --condition "annualized_volatility_pct < 15"
"""

import os
import sys
import json
import time
import asyncio
import argparse
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from auto_trader.schema import (
    log_experiment, get_experiment_history, get_best_experiment,
    get_run_summary, get_db, get_recent_lessons,
)
from auto_trader.tools import create_auto_trader_tools
from auto_trader.events import emit as emit_event


def generate_run_id() -> str:
    raw = f"run:{datetime.now(timezone.utc).isoformat()}"
    return hashlib.md5(raw.encode()).hexdigest()[:10]


def _load_schemas() -> str:
    """Load strategy and portfolio schemas from the engines.

    Schemas are dumped compactly (separators=',:') because the SDK launches
    the bundled Claude binary as a subprocess and passes the system prompt
    as a CLI argument. Linux's MAX_ARG_STRLEN is 128 KiB per single argument;
    the verbose pretty-printed schemas alone exceed that on configs with
    discriminated unions of ~10+ types. Compact JSON cuts the schema bytes
    roughly in half without changing the agent's ability to parse it.
    """
    import json as _json
    try:
        from backtest_engine import get_config_schema as strategy_schema
        from portfolio_engine import get_config_schema as portfolio_schema
        strat = _json.dumps(strategy_schema(), separators=(",", ":"))
        port = _json.dumps(portfolio_schema(), separators=(",", ":"))

        thesis_schema = _json.dumps({
            "thesis": {
                "thesis": {"type": "string", "description": "A clear 2-3 sentence investment thesis"},
                "assumptions": {"type": "array", "items": "string", "description": "List of assumptions that must hold for this thesis to work"},
            },
            "portfolio": "(see Portfolio Config Schema below)",
        })

        return (
            "### Thesis Output Schema\n\n"
            f"```json\n{thesis_schema}\n```\n\n"
            "### Strategy Config Schema (authoritative — only use fields/values listed here)\n\n"
            f"```json\n{strat}\n```\n\n"
            "### Portfolio Config Schema\n\n"
            f"```json\n{port}\n```"
        )
    except Exception as e:
        return f"## Schema Error\nFailed to load schemas: {e}"


def _load_factor_catalog() -> str:
    """Build the factor catalog block from server.factors registry + a recent
    cross-section from features_daily.

    For each registered feature: name, category, unit, definition. For
    materialized features, also p10/p50/p90 across all symbols on the most
    recent trading date with broad coverage — gives the agent a 'what's a
    normal value today' anchor before it picks a threshold.
    """
    try:
        import sqlite3
        import statistics
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from server.factors import all_features
        from db_config import MARKET_DB_PATH  # type: ignore

        feats = all_features()
        if not feats:
            return ""

        # Pull a recent cross-section per feature. We use the most recent date
        # in features_daily with > 100 non-null values for that feature.
        rows: list[str] = []
        rows.append(
            "| name | category | unit | definition | "
            "p10 | p50 | p90 | n |"
        )
        rows.append(
            "|---|---|---|---|---|---|---|---|"
        )
        with sqlite3.connect(str(MARKET_DB_PATH)) as conn:
            existing_cols = {
                r[1] for r in conn.execute("PRAGMA table_info(features_daily)").fetchall()
            }
            for f in feats:
                p10 = p50 = p90 = ""
                n_str = ""
                if f.materialization == "precomputed":
                    if f.name not in existing_cols:
                        # Registered but not yet backfilled into features_daily.
                        p10 = p50 = p90 = "_pending backfill_"
                        n_str = "0"
                    else:
                        cur = conn.cursor()
                        latest = cur.execute(
                            f"SELECT date FROM features_daily "
                            f"WHERE {f.name} IS NOT NULL "
                            f"GROUP BY date HAVING COUNT(*) > 100 "
                            f"ORDER BY date DESC LIMIT 1"
                        ).fetchone()
                        if latest:
                            vals = [r[0] for r in cur.execute(
                                f"SELECT {f.name} FROM features_daily "
                                f"WHERE date = ? AND {f.name} IS NOT NULL",
                                (latest[0],),
                            ).fetchall()]
                            if vals:
                                vs = sorted(vals)
                                n = len(vs)
                                p10 = f"{vs[int(n*0.1)]:.2f}"
                                p50 = f"{statistics.median(vs):.2f}"
                                p90 = f"{vs[int(n*0.9)]:.2f}"
                                n_str = str(n)
                else:
                    p10 = p50 = p90 = "_on-the-fly_"
                    n_str = "—"
                rows.append(
                    f"| `{f.name}` | {f.category} | {f.unit} | "
                    f"{f.description} | {p10} | {p50} | {p90} | {n_str} |"
                )

        return (
            "### Factor Catalog (server/factors registry)\n\n"
            "Every named factor that `feature_threshold` and `feature_percentile` "
            "can reference. Materialized factors are stored in the `features_daily` "
            "table and queryable via `data-query`. Cross-section stats (p10/p50/p90, "
            "n) are from the most recent trading date with broad coverage — use "
            "them to pick reasonable thresholds.\n\n"
            + "\n".join(rows)
        )
    except Exception as e:
        return f"### Factor Catalog\n_(load failed: {e})_"


_custom_prompt = None
# Explicit allowlist for this run. None means the API/CLI didn't supply one,
# in which case we fall back to the full current catalog (CLI convenience).
_allowed_tools: list[str] | None = None


def _resolve_allowed_mcp_tools() -> list[str]:
    """Map the per-agent allowlist to the SDK tool ids, dropping any unknowns.

    None  -> no allowlist supplied; fall back to the full current catalog.
             (API runs always supply one; this only triggers for ad-hoc CLI use.)
    list  -> explicit subset; unknown names are skipped with a warning so a
             tool that's been deleted from the codebase doesn't crash a backtest.
    """
    from auto_trader.tools import ALL_TOOLS, TOOL_NAMES, mcp_tool_id
    if _allowed_tools is None:
        names = [t.name for t in ALL_TOOLS]
    else:
        names = []
        for n in _allowed_tools:
            if n in TOOL_NAMES:
                names.append(n)
            else:
                print(f"  [warn] agent allowlist references unknown tool '{n}' — skipping")
    return [mcp_tool_id(n) for n in names]


def _resolve_model_api_id(model_id: str) -> str:
    """Map a short model id ('opus', 'opus-4-7') to its full Anthropic API id.

    If the input is already a full API id (contains 'claude-'), returns as-is.
    Unknown short ids also pass through unchanged — the SDK will either resolve
    a slug alias or error with a clear message.
    """
    if not model_id or "claude-" in model_id:
        return model_id
    # Keep in sync with AVAILABLE_MODELS in auto_trader/api.py
    mapping = {
        "haiku": "claude-haiku-4-5-20251001",
        "sonnet": "claude-sonnet-4-6",
        "opus": "claude-opus-4-6",
        "opus-4-7": "claude-opus-4-7",
    }
    return mapping.get(model_id, model_id)

def load_program(agent_prompt: str | None = None) -> str:
    """Build the full system prompt: agent prompt + system mechanics + schemas.

    The agent prompt owns identity and research style; system.md owns engine
    mechanics (validation, output format, rules); schemas land last so they're
    freshest in context when the agent emits its JSON output.

    Tool definitions are NOT injected here — the SDK transmits them automatically
    via the API tools channel (filtered by ClaudeAgentOptions.allowed_tools).

    Resolution order for the agent prompt:
    1. explicit `agent_prompt` arg (used by the API for per-agent preview)
    2. module-level `_custom_prompt` (set via CLI --prompt-file)
    3. fallback to program.md on disk
    """
    system_path = Path(__file__).parent / "system.md"
    system_instructions = system_path.read_text()

    if agent_prompt is None:
        agent_prompt = _custom_prompt or (Path(__file__).parent / "program.md").read_text()

    schemas = _load_schemas()
    catalog = _load_factor_catalog()

    parts = [agent_prompt, system_instructions, catalog, schemas]
    return "\n\n---\n\n".join(p for p in parts if p)


def parse_thesis(agent_output: str) -> dict | None:
    """Extract the thesis JSON from the agent's output.

    Scans for a JSON object containing both 'thesis' and 'portfolio' keys.
    """
    # Find all JSON-like blocks in the output
    depth = 0
    start = None
    for i, ch in enumerate(agent_output):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start is not None:
                candidate = agent_output[start:i+1]
                try:
                    parsed = json.loads(candidate)
                    if isinstance(parsed, dict) and "thesis" in parsed and "portfolio" in parsed:
                        return parsed
                except json.JSONDecodeError:
                    pass
                start = None
    return None


def _get_trade_summary(exp_id: str) -> dict | None:
    """Pull compact trade aggregates for an experiment.

    Returns None if no trades (backtest generated no executions, or trade
    persist failed for this experiment).
    """
    from auto_trader.schema import get_db
    conn = get_db()
    # Aggregate stats — only SELL rows carry pnl
    agg = conn.execute(
        """SELECT
               COUNT(*) AS total_events,
               SUM(CASE WHEN action='BUY' THEN 1 ELSE 0 END) AS buys,
               SUM(CASE WHEN action='SELL' THEN 1 ELSE 0 END) AS sells,
               SUM(CASE WHEN action='SELL' AND pnl > 0 THEN 1 ELSE 0 END) AS winners,
               SUM(CASE WHEN action='SELL' AND pnl <= 0 THEN 1 ELSE 0 END) AS losers,
               AVG(CASE WHEN action='SELL' AND pnl > 0 THEN pnl_pct END) AS avg_win_pct,
               AVG(CASE WHEN action='SELL' AND pnl <= 0 THEN pnl_pct END) AS avg_loss_pct,
               AVG(CASE WHEN action='SELL' THEN days_held END) AS avg_days_held
           FROM trades WHERE source_type='experiment' AND source_id = ?""",
        (exp_id,),
    ).fetchone()
    if not agg or (agg["total_events"] or 0) == 0:
        conn.close()
        return None

    # Exit-reason histogram
    reasons = conn.execute(
        """SELECT reason, COUNT(*) AS n FROM trades
           WHERE source_type='experiment' AND source_id=? AND action='SELL' AND reason IS NOT NULL
           GROUP BY reason ORDER BY n DESC""",
        (exp_id,),
    ).fetchall()

    # Top 3 winners and worst 3 losers by pnl_pct
    winners_rows = conn.execute(
        """SELECT symbol, pnl_pct, days_held FROM trades
           WHERE source_type='experiment' AND source_id=? AND action='SELL' AND pnl > 0
           ORDER BY pnl_pct DESC LIMIT 3""",
        (exp_id,),
    ).fetchall()
    losers_rows = conn.execute(
        """SELECT symbol, pnl_pct, days_held FROM trades
           WHERE source_type='experiment' AND source_id=? AND action='SELL' AND pnl <= 0
           ORDER BY pnl_pct ASC LIMIT 3""",
        (exp_id,),
    ).fetchall()
    conn.close()

    sells = agg["sells"] or 0
    winners = agg["winners"] or 0
    hit_rate = (winners / sells * 100) if sells else 0.0

    return {
        "sells": sells,
        "winners": winners,
        "losers": agg["losers"] or 0,
        "hit_rate_pct": round(hit_rate, 1),
        "avg_win_pct": round(agg["avg_win_pct"], 2) if agg["avg_win_pct"] is not None else None,
        "avg_loss_pct": round(agg["avg_loss_pct"], 2) if agg["avg_loss_pct"] is not None else None,
        "avg_days_held": round(agg["avg_days_held"], 1) if agg["avg_days_held"] is not None else None,
        "exit_reasons": [(r["reason"], r["n"]) for r in reasons],
        "top_winners": [(r["symbol"], r["pnl_pct"], r["days_held"]) for r in winners_rows],
        "worst_losers": [(r["symbol"], r["pnl_pct"], r["days_held"]) for r in losers_rows],
    }


def build_recent_lessons_context(run_id: str, limit: int = 3) -> str:
    """Format the last N experiments' lessons for inclusion in the user prompt.

    This is intentionally narrow (default last 3) — different from
    build_history_context, which deliberately excludes lessons across the
    aggregated history to avoid anchoring the agent on its own past
    interpretation. Surfacing only the *most recent* lessons strikes a
    balance: the agent gets continuity from its immediate prior reflections
    without compounding bias across the full run.

    Returns "" if no lessons are recorded yet (e.g. experiment 1).
    """
    rows = get_recent_lessons(run_id, limit=limit)
    if not rows:
        return ""
    lines = [f"## Lessons from prior experiments (most recent first)\n"]
    for r in rows:
        # Header refers to the experiment the lessons reflect on, not the
        # current iteration. The agent writes these at the end of experiment N
        # after seeing N's results, so they describe what was learned through
        # iteration N — labeling them "Lessons after Experiment N" reads
        # coherently when the agent is on iteration N+1.
        lines.append(f"### Lessons after Experiment {r['iteration']}\n")
        lines.append(r["lessons"].strip())
        lines.append("")  # blank line between blocks
    return "\n".join(lines)


def build_history_context(run_id: str, target_metric: str, limit: int = 20) -> str:
    """Build full history of past experiments for the agent to learn from.

    Deliberately excludes the `lessons` field. Lessons are the agent's own
    interpretation of prior experiments and are persisted for UI display only;
    surfacing them here would bias subsequent iterations by anchoring the
    agent on its own past self-interpretation. `get_experiment_history()`
    intentionally does not SELECT the column so the data isn't available
    at this layer. Do not change either without reading the design rationale.

    NOTE: build_recent_lessons_context() above does surface the LAST N
    lessons separately — that's a deliberate scoped exception, not a
    contradiction. See its docstring.
    """
    history = get_experiment_history(run_id, limit=limit)
    if not history:
        return "No previous experiments. This is your first experiment."

    lines = [f"## Past Experiments ({len(history)} most recent)\n"]
    for exp in reversed(history):  # oldest first
        status = "KEEP" if exp["decision"] == "keep" else "DISCARD"
        lines.append(f"### Experiment {exp['iteration']} [id: {exp['id']}] — {status}")

        # Full thesis
        lines.append(f"**Thesis:** {exp.get('thesis', 'N/A')}")

        # Full assumptions
        assumptions = exp.get("assumptions")
        if assumptions:
            if isinstance(assumptions, str):
                try:
                    assumptions = json.loads(assumptions)
                except json.JSONDecodeError:
                    assumptions = [assumptions]
            if isinstance(assumptions, list) and assumptions:
                lines.append("**Assumptions:**")
                for a in assumptions:
                    lines.append(f"- {a}")

        # Full metrics
        metrics = []
        if exp.get("sharpe_ratio") is not None:
            metrics.append(f"Sharpe={exp['sharpe_ratio']:.2f}")
        if exp.get("alpha_ann_pct") is not None:
            metrics.append(f"Alpha={exp['alpha_ann_pct']:.1f}%")
        if exp.get("annualized_volatility_pct") is not None:
            metrics.append(f"Vol={exp['annualized_volatility_pct']:.1f}%")
        if exp.get("total_return_pct") is not None:
            metrics.append(f"Return={exp['total_return_pct']:.1f}%")
        if exp.get("max_drawdown_pct") is not None:
            metrics.append(f"MaxDD={exp['max_drawdown_pct']:.1f}%")
        lines.append(f"**Metrics:** {', '.join(metrics)}")

        # Full portfolio config
        portfolio_config = exp.get("portfolio_config")
        if portfolio_config:
            if isinstance(portfolio_config, str):
                try:
                    portfolio_config = json.loads(portfolio_config)
                except json.JSONDecodeError:
                    portfolio_config = None
            if isinstance(portfolio_config, dict):
                lines.append(f"**Portfolio:** {portfolio_config.get('name', 'N/A')}")
                for sleeve in portfolio_config.get("sleeves", []):
                    label = sleeve.get("label", "?")
                    weight = sleeve.get("weight", 0)
                    sc = sleeve.get("strategy_config", {})
                    universe = sc.get("universe", {})
                    entry = sc.get("entry", {})
                    conditions_list = entry.get("conditions", [])
                    cond_types = [c.get("type", "?") for c in conditions_list]
                    lines.append(f"- {label} ({weight*100:.0f}%): universe={universe.get('type','?')} "
                                 f"sector={universe.get('sector','all')} "
                                 f"entry=[{', '.join(cond_types)}]")

        # Compact trade summary pulled from persisted trades
        try:
            summary = _get_trade_summary(exp["id"])
        except Exception:
            summary = None
        if summary and summary["sells"] > 0:
            win_str = f"{summary['avg_win_pct']:+.1f}%" if summary["avg_win_pct"] is not None else "n/a"
            loss_str = f"{summary['avg_loss_pct']:+.1f}%" if summary["avg_loss_pct"] is not None else "n/a"
            days_str = f"{summary['avg_days_held']:.0f}d" if summary["avg_days_held"] is not None else "n/a"
            lines.append(
                f"**Trades:** {summary['sells']} closed "
                f"({summary['winners']}W/{summary['losers']}L, hit={summary['hit_rate_pct']:.0f}%, "
                f"avg_win={win_str}, avg_loss={loss_str}, avg_hold={days_str})"
            )
            if summary["exit_reasons"]:
                reasons_str = ", ".join(f"{r}:{n}" for r, n in summary["exit_reasons"])
                lines.append(f"**Exit reasons:** {reasons_str}")
            if summary["top_winners"]:
                top_str = ", ".join(f"{s} {p:+.1f}%/{d}d" for s, p, d in summary["top_winners"])
                lines.append(f"**Top winners:** {top_str}")
            if summary["worst_losers"]:
                bot_str = ", ".join(f"{s} {p:+.1f}%/{d}d" for s, p, d in summary["worst_losers"])
                lines.append(f"**Worst losers:** {bot_str}")

        lines.append("")

    higher = METRIC_DIRECTION.get(target_metric, True)
    best = get_best_experiment(run_id, higher_is_better=higher)
    if best:
        lines.append(f"**Current best:** Experiment {best['iteration']} "
                      f"(target={best.get('target_value', 'N/A')})")

    return "\n".join(lines)


# Metric direction: True = higher is better, False = lower is better
METRIC_DIRECTION = {
    "sharpe_ratio": True,
    "alpha_ann_pct": True,
    "annualized_volatility_pct": False,
    "max_drawdown_pct": True,  # less negative = better, so higher is better
}

VALID_METRICS = list(METRIC_DIRECTION.keys())


def is_improvement(metric: str, new_value: float, best_value: float) -> bool:
    """Check if new_value is an improvement over best_value for the given metric."""
    higher_is_better = METRIC_DIRECTION.get(metric, True)
    if higher_is_better:
        return new_value > best_value
    else:
        return new_value < best_value


def parse_conditions(condition_strs: list[str]) -> list[dict]:
    """Parse condition strings like 'alpha_ann_pct > 0' into dicts."""
    conditions = []
    for s in condition_strs:
        parts = s.strip().split()
        if len(parts) != 3:
            raise ValueError(f"Invalid condition: '{s}'. Format: 'metric operator value'")
        metric, operator, value = parts
        if operator not in (">", ">=", "<", "<=", "==", "!="):
            raise ValueError(f"Invalid operator: '{operator}'")
        conditions.append({
            "metric": metric,
            "operator": operator,
            "value": float(value),
        })
    return conditions


def check_conditions(metrics: dict, conditions: list[dict]) -> tuple[bool, list[dict]]:
    """Check if all conditions are met. Returns (all_met, detail)."""
    detail = []
    for cond in conditions:
        actual = metrics.get(cond["metric"])
        if actual is None:
            detail.append({**cond, "actual": None, "met": False})
            continue

        op = cond["operator"]
        val = cond["value"]
        met = (
            (op == ">" and actual > val) or
            (op == ">=" and actual >= val) or
            (op == "<" and actual < val) or
            (op == "<=" and actual <= val) or
            (op == "==" and actual == val) or
            (op == "!=" and actual != val)
        )
        detail.append({**cond, "actual": round(actual, 4), "met": met})

    all_met = all(d["met"] for d in detail)
    return all_met, detail


def normalize_config(portfolio_config: dict) -> dict:
    """Fix common config issues from LLM output before passing to engine."""
    config = dict(portfolio_config)

    for sleeve in config.get("sleeves", []):
        sc = sleeve.get("strategy_config", {})

        # Fix exit_conditions: engine expects a flat list, LLM sometimes wraps in {"conditions": [...]}
        ec = sc.get("exit_conditions")
        if isinstance(ec, dict) and "conditions" in ec:
            sc["exit_conditions"] = ec["conditions"]

        # Ensure regime_gate is a list of strings (not dicts)
        rg = sleeve.get("regime_gate", ["*"])
        if isinstance(rg, list) and rg and isinstance(rg[0], dict):
            # Agent put inline conditions instead of regime IDs — disable gating
            sleeve["regime_gate"] = ["*"]

    return config


def save_portfolio(portfolio_config: dict) -> str | None:
    """Save a portfolio config to the portfolios table (idempotent).
    Returns portfolio_id, or None if save fails.

    Uses deterministic hash so identical configs dedupe to the same ID.
    """
    try:
        from portfolio_engine import compute_portfolio_id
        pid = compute_portfolio_id(portfolio_config)
        conn = get_db()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO portfolios (portfolio_id, name, config, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (pid, portfolio_config.get("name", "Unnamed"), json.dumps(portfolio_config), now, now),
        )
        conn.commit()
        conn.close()
        return pid
    except Exception as e:
        print(f"  [warn] Failed to save portfolio: {e}")
        return None


def run_backtest(portfolio_config: dict, start: str, end: str, capital: float,
                 sector: str | None = None) -> dict | None:
    """Run a portfolio backtest.

    Returns:
        {"metrics": {...}, "sleeve_trades": [{"label": str, "trades": [...]}, ...]}
        or None on failure.

    sleeve_trades mirrors the engine's per-sleeve grouping — each sleeve's full
    BUY+SELL event log in chronological order. Portfolio-level trades = union of
    all sleeve lists.
    """
    try:
        from portfolio_engine import run_portfolio_backtest
        from backtest_engine import compute_benchmark, SECTOR_ETF_MAP

        config = normalize_config(portfolio_config)
        config["backtest"] = {
            "start": start,
            "end": end,
            "initial_capital": capital,
        }

        # Don't force-close at backtest_end. Forced liquidation corrupts the
        # signal — it replaces the strategy's exit rules with "sell because the
        # calendar ran out" at whatever price the last day happens to print.
        # Final NAV already marks-to-market via record_nav, so total_return_pct
        # and Sharpe are correct. Win rate / profit factor now compute on real
        # signal-driven exits only; open positions are reported separately.
        result = run_portfolio_backtest(config, force_close_at_end=False)
        metrics = result.get("metrics", {})

        # The portfolio engine already computed one benchmark (SPY or sector ETF).
        # We need to ensure we have BOTH market and sector alphas.
        nav_history = result.get("combined_nav_history", [])
        if nav_history:
            trading_dates = [p["date"] for p in nav_history]
            final_nav = nav_history[-1]["nav"]
            ann_return = metrics.get("annualized_return_pct", 0)

            # Market benchmark (SPY) — always compute
            market_bench = compute_benchmark(trading_dates, capital, sector=None)
            if market_bench:
                market_ann = market_bench["metrics"]["annualized_return_pct"]
                metrics["alpha_vs_market_pct"] = round(ann_return - market_ann, 2)
                metrics["market_benchmark_return_pct"] = market_bench["metrics"]["total_return_pct"]
                metrics["market_benchmark_ann_return_pct"] = market_bench["metrics"]["annualized_return_pct"]

            # Sector benchmark — compute if sector is set
            if sector and sector in SECTOR_ETF_MAP:
                sector_bench = compute_benchmark(trading_dates, capital, sector=sector)
                if sector_bench:
                    sector_ann = sector_bench["metrics"]["annualized_return_pct"]
                    metrics["alpha_vs_sector_pct"] = round(ann_return - sector_ann, 2)
                    metrics["sector_benchmark_return_pct"] = sector_bench["metrics"]["total_return_pct"]
                    metrics["sector_benchmark_ann_return_pct"] = sector_bench["metrics"]["annualized_return_pct"]

        # Build per-sleeve trade groups with labels attached
        sleeve_results = result.get("sleeve_results", [])
        per_sleeve = result.get("per_sleeve", [])
        sleeve_trades = [
            {
                "label": per_sleeve[i].get("label") if i < len(per_sleeve) else f"sleeve_{i}",
                "trades": sr.get("trades", []),
            }
            for i, sr in enumerate(sleeve_results)
        ]

        return {"metrics": metrics, "sleeve_trades": sleeve_trades}
    except Exception as e:
        print(f"  Backtest failed: {e}")
        import traceback
        traceback.print_exc()
        return None


async def run_agent_iteration(
    run_id: str,
    iteration: int,
    target_metric: str,
    conditions: list[dict],
    best_value: float | None,
    backtest_start: str,
    backtest_end: str,
    initial_capital: float,
    model: str,
    sector: str | None = None,
    alpha_benchmark: str = "market",
) -> dict:
    """Run a single agent iteration. Returns experiment result."""
    from claude_agent_sdk import query, ClaudeAgentOptions

    t0 = time.time()

    # Build the prompt
    program = load_program()
    history = build_history_context(run_id, target_metric)
    recent_lessons = build_recent_lessons_context(run_id, limit=3)

    conditions_desc = ", ".join(
        f"{c['metric']} {c['operator']} {c['value']}" for c in conditions
    )

    sector_desc = f"\n**Sector:** {sector} — All data queries are restricted to {sector} stocks only. Alpha is measured against the {sector} sector ETF." if sector else ""
    benchmark_desc = f"sector ETF" if alpha_benchmark == "sector" else "S&P 500 (SPY)"

    prompt = f"""You are on experiment {iteration} of an autonomous research loop.

**Objective (portfolio level):** Design a portfolio that {"maximizes" if METRIC_DIRECTION.get(target_metric, True) else "minimizes"} `{target_metric}`.
**Conditions (portfolio level):** {conditions_desc if conditions else 'None'}
**Backtest period:** {backtest_start} to {backtest_end}
**Capital:** ${initial_capital:,.0f}
**Alpha benchmark:** {benchmark_desc}{sector_desc}
{"**Current best " + target_metric + ":** " + f"{best_value:.4f}" if best_value is not None else "**No best yet — this is the first experiment.**"}

**Knowledge cutoff: {backtest_end}**
You are researching as of {backtest_end}. You do not know what happens after this date.
- All market data queries are automatically limited to data on or before {backtest_end}
- Do NOT use your training knowledge about market events, prices, or outcomes after {backtest_end}
- Form your thesis based only on data you query and patterns you observe within the allowed period
- Pretend today is {backtest_end}

Use the `query_market_data` tool for all data queries. Use `validate_portfolio` to check your config before outputting.

{recent_lessons}
{history}"""

    # Run the agent with skill discovery + query_market_data + validate_portfolio
    emit_event(run_id, "experiment_started", {"experiment_number": iteration})
    agent_output = []
    tool_calls = 0
    session_id = None
    auto_trader_tools = create_auto_trader_tools(
        stop_date=backtest_end, sector=sector, start_date=backtest_start,
        run_id=run_id,
        # Enforce the agent's allowlist at the MCP server boundary so forbidden
        # tools don't appear in the model's tool catalog at all.
        allowed_tool_names=_allowed_tools,
    )
    # Resolve short id (e.g. "opus-4-7") to the full API id so the SDK
    # dispatches to the exact model regardless of its slug aliasing.
    resolved_model = _resolve_model_api_id(model)

    agent_opts = dict(
        system_prompt=program,
        cwd=str(PROJECT_ROOT / "auto_trader"),
        model=resolved_model,
        setting_sources=["project"],
        allowed_tools=["Skill", "Read"] + _resolve_allowed_mcp_tools(),
        mcp_servers={"auto_trader": auto_trader_tools},
        permission_mode="acceptEdits",
        max_turns=50,
    )
    # Opus 4.7 rejects the legacy thinking.type='enabled' shape the CLI
    # defaults to. Force adaptive thinking for 4.7 only; leave other
    # models on their defaults so we don't alter working behavior.
    #
    # Note: Opus 4.7's thinking blocks return with empty `thinking` text
    # and a signature only (display defaults to "omitted" per-model).
    # Anthropic's docs say passing display="summarized" unseals the text,
    # but claude-agent-sdk 0.1.61's subprocess transport only reads
    # `type` from this dict (see subprocess_cli.py:305-312) and the
    # bundled CLI binary has no --display flag. Until the SDK plumbs
    # this through, the value is not actually configurable here.
    if resolved_model == "claude-opus-4-7":
        agent_opts["thinking"] = {"type": "adaptive"}

    try:
        async for message in query(
            prompt=prompt,
            options=ClaudeAgentOptions(**agent_opts),
        ):
            msg_type = type(message).__name__
            # Capture session_id from any message that has it
            if hasattr(message, "session_id") and message.session_id:
                session_id = message.session_id

            if hasattr(message, "result") and message.result:
                agent_output.append(message.result)
                print(f"  [agent] Result received ({len(message.result)} chars)")
                emit_event(run_id, "agent_result", {
                    "experiment_number": iteration,
                    "chars": len(message.result),
                })
            elif msg_type == "AssistantMessage":
                content = getattr(message, "content", [])
                for block in (content if isinstance(content, list) else [content]):
                    block_type = type(block).__name__
                    if block_type == "TextBlock":
                        agent_output.append(block.text)
                        # Emit short reasoning snippets
                        text = block.text.strip()
                        if text and len(text) > 10:
                            emit_event(run_id, "agent_thinking", {
                                "experiment_number": iteration,
                                "text": text[:300],
                            })
                    elif block_type == "ThinkingBlock":
                        # Extended-thinking content (Opus 4.6 explicit budget /
                        # Opus 4.7 adaptive). Surface it so the live activity
                        # reflects actual reasoning, not just narrative text.
                        text = (getattr(block, "thinking", "") or "").strip()
                        if text and len(text) > 10:
                            emit_event(run_id, "agent_thinking", {
                                "experiment_number": iteration,
                                "text": text[:300],
                            })
                    elif "ToolUse" in block_type:
                        tool_calls += 1
                        tool_name = getattr(block, "name", "?")
                        tool_input = getattr(block, "input", {})
                        print(f"  [agent] Tool call #{tool_calls}: {tool_name}")
                        emit_event(run_id, "tool_call", {
                            "experiment_number": iteration,
                            "tool": tool_name,
                            "input": tool_input if isinstance(tool_input, dict) else {},
                            "call_number": tool_calls,
                        })
    except Exception as e:
        duration = time.time() - t0
        exp_id = log_experiment(
            run_id=run_id, iteration=iteration,
            thesis="", assumptions=[], portfolio_config={},
            metrics={}, target_metric=target_metric, target_value=None,
            conditions=conditions, conditions_met=False,
            decision="discard", best_value_so_far=best_value or 0,
            backtest_start=backtest_start, backtest_end=backtest_end,
            initial_capital=initial_capital, model=model,
            session_id=session_id, duration_seconds=duration, error=str(e),
        )
        return {"decision": "discard", "error": str(e), "id": exp_id}

    duration_agent = time.time() - t0
    full_output = "\n".join(agent_output)

    # Parse thesis
    thesis_data = parse_thesis(full_output)
    if not thesis_data:
        exp_id = log_experiment(
            run_id=run_id, iteration=iteration,
            thesis=full_output[:500], assumptions=[], portfolio_config={},
            metrics={}, target_metric=target_metric, target_value=None,
            conditions=conditions, conditions_met=False,
            decision="discard", best_value_so_far=best_value or 0,
            backtest_start=backtest_start, backtest_end=backtest_end,
            initial_capital=initial_capital, model=model,
            session_id=session_id, duration_seconds=duration_agent,
            error="Failed to parse thesis JSON from agent output",
        )
        return {"decision": "discard", "error": "parse_failed", "id": exp_id}

    # Extract thesis (handles both flat and nested formats)
    thesis_obj = thesis_data.get("thesis", {})
    if isinstance(thesis_obj, dict):
        thesis = thesis_obj.get("thesis", "")
        assumptions = thesis_obj.get("assumptions", [])
    else:
        thesis = str(thesis_obj)
        assumptions = thesis_data.get("assumptions", [])
    portfolio_config = thesis_data.get("portfolio", {})
    # Agent's reflection on prior experiments. Stored for UI display only —
    # deliberately NOT surfaced in build_history_context to keep next
    # iterations unbiased by prior self-interpretation.
    lessons = thesis_data.get("lessons")
    if isinstance(lessons, str):
        lessons = lessons.strip() or None
    elif lessons is not None:
        # Agent emitted a non-string; coerce to string rather than dropping.
        lessons = json.dumps(lessons, default=str)

    print(f"  Thesis: {thesis[:100]}...")
    print(f"  Sleeves: {len(portfolio_config.get('sleeves', []))}")
    emit_event(run_id, "thesis_generated", {
        "experiment_number": iteration,
        "thesis": thesis[:300],
        "sleeves": len(portfolio_config.get("sleeves", [])),
    })

    # Run backtest
    print(f"  Running backtest ({backtest_start} to {backtest_end})...")
    emit_event(run_id, "backtest_started", {"experiment_number": iteration})
    bt_result = run_backtest(portfolio_config, backtest_start, backtest_end, initial_capital, sector=sector)

    if bt_result is None:
        metrics = None
    else:
        metrics = bt_result["metrics"]
        sleeve_trades = bt_result["sleeve_trades"]

    if metrics is None:
        exp_id = log_experiment(
            run_id=run_id, iteration=iteration,
            thesis=thesis, assumptions=assumptions, portfolio_config=portfolio_config,
            metrics={}, target_metric=target_metric, target_value=None,
            conditions=conditions, conditions_met=False,
            decision="discard", best_value_so_far=best_value or 0,
            backtest_start=backtest_start, backtest_end=backtest_end,
            initial_capital=initial_capital, model=model,
            session_id=session_id, duration_seconds=time.time() - t0, error="Backtest failed",
            lessons=lessons,
        )
        return {"decision": "discard", "error": "backtest_failed", "id": exp_id}

    # Map alpha_ann_pct to the right benchmark based on run config
    if alpha_benchmark == "sector" and "alpha_vs_sector_pct" in metrics:
        metrics["alpha_ann_pct"] = metrics["alpha_vs_sector_pct"]
    elif "alpha_vs_market_pct" in metrics:
        metrics["alpha_ann_pct"] = metrics["alpha_vs_market_pct"]

    # Score
    target_value = metrics.get(target_metric)
    conditions_met, conditions_detail = check_conditions(metrics, conditions)

    improved = (
        target_value is not None
        and (best_value is None or is_improvement(target_metric, target_value, best_value))
        and conditions_met
    )
    decision = "keep" if improved else "discard"

    duration_total = time.time() - t0

    # Save portfolio to portfolios table (idempotent) — links experiment → portfolio
    portfolio_id = save_portfolio(portfolio_config)

    exp_id = log_experiment(
        run_id=run_id, iteration=iteration,
        thesis=thesis, assumptions=assumptions, portfolio_config=portfolio_config,
        metrics=metrics, target_metric=target_metric, target_value=target_value,
        conditions=conditions, conditions_met=conditions_met,
        decision=decision, best_value_so_far=best_value or 0,
        backtest_start=backtest_start, backtest_end=backtest_end,
        initial_capital=initial_capital, model=model,
        session_id=session_id, duration_seconds=duration_total,
        portfolio_id=portfolio_id,
        lessons=lessons,
    )

    # Persist trades per sleeve under source_type='experiment'.
    # Failure here is enrichment loss only — the experiment row is already saved.
    try:
        from deploy_engine import persist_trades
        total = 0
        for sleeve in sleeve_trades:
            if sleeve["trades"]:
                total += persist_trades("experiment", exp_id, sleeve["trades"],
                                        sleeve_label=sleeve["label"])
        if total:
            print(f"  💾 {total} trade(s) persisted for {exp_id}")
    except Exception as e:
        print(f"  ⚠ Trade persist failed for {exp_id}: {e}")

    print(f"  {target_metric}: {target_value:.4f}" if target_value else f"  {target_metric}: N/A")
    print(f"  Conditions met: {conditions_met}")
    for d in conditions_detail:
        status = "PASS" if d["met"] else "FAIL"
        print(f"    {d['metric']} {d['operator']} {d['value']}: actual={d.get('actual', 'N/A')} [{status}]")
    print(f"  Decision: {decision.upper()}")
    if improved:
        print(f"  NEW BEST: {target_value:.4f}")

    emit_event(run_id, "experiment_completed", {
        "experiment_number": iteration,
        "experiment_id": exp_id,
        "decision": decision,
        "target_metric": target_metric,
        "target_value": target_value,
        "conditions_met": conditions_met,
        "sharpe_ratio": metrics.get("sharpe_ratio"),
        "alpha_ann_pct": metrics.get("alpha_ann_pct"),
        "annualized_volatility_pct": metrics.get("annualized_volatility_pct"),
        "max_drawdown_pct": metrics.get("max_drawdown_pct"),
        "total_return_pct": metrics.get("total_return_pct"),
        "duration_seconds": round(duration_total, 1),
    })

    return {
        "decision": decision,
        "target_value": target_value,
        "metrics": metrics,
        "id": exp_id,
    }


def _update_run_status(run_id: str, status: str, **kwargs):
    """Update run status in DB (called by runner during execution)."""
    conn = get_db()
    now = datetime.now(timezone.utc).isoformat()
    sets = ["status = ?", "updated_at = ?"]
    params = [status, now]
    for k, v in kwargs.items():
        sets.append(f"{k} = ?")
        params.append(v)
    params.append(run_id)
    conn.execute(f"UPDATE auto_trader_runs SET {', '.join(sets)} WHERE id = ?", params)
    conn.commit()
    conn.close()


async def main():
    parser = argparse.ArgumentParser(description="Auto-Trader: autonomous portfolio research")
    parser.add_argument("--run-id", type=str, default=None, help="Run ID (from API)")
    parser.add_argument("--max-experiments", type=int, default=10, help="Maximum number of experiments")
    parser.add_argument("--metric", type=str, default="sharpe_ratio", help="Metric to optimize")
    parser.add_argument("--condition", action="append", default=[], help="Conditions (e.g. 'alpha_ann_pct > 0')")
    parser.add_argument("--start", type=str, default="2015-01-01", help="Backtest start date")
    parser.add_argument("--end", type=str, default="2024-12-31", help="Backtest end date")
    parser.add_argument("--capital", type=float, default=1_000_000, help="Initial capital")
    parser.add_argument("--model", type=str, default="sonnet", help="Claude model (sonnet, opus, haiku)")
    parser.add_argument("--history", type=int, default=10, help="Past experiments to show agent")
    parser.add_argument("--prompt-file", type=str, default=None, help="Path to prompt file (from API)")
    parser.add_argument("--allowed-tools", type=str, default=None,
                        help="JSON array of MCP tool names this agent may call. "
                             "Omit (or pass empty) for 'all current tools'. '[]' disables all MCP tools.")
    parser.add_argument("--starting-portfolio", type=str, default=None, help="Path to starting portfolio config JSON")
    parser.add_argument("--sector", type=str, default=None, help="Restrict data queries to this sector")
    parser.add_argument("--alpha-benchmark", type=str, default="market", help="Benchmark: sector or market")
    args = parser.parse_args()

    if args.metric not in VALID_METRICS:
        print(f"Invalid metric: '{args.metric}'. Valid: {VALID_METRICS}")
        return

    conditions = parse_conditions(args.condition)
    run_id = args.run_id or generate_run_id()
    direction = "maximize" if METRIC_DIRECTION[args.metric] else "minimize"

    # Load prompt from file if provided (API flow), else from program.md
    if args.prompt_file and Path(args.prompt_file).exists():
        global _custom_prompt
        _custom_prompt = Path(args.prompt_file).read_text()

    # Per-agent MCP tool allowlist. None = all current tools.
    if args.allowed_tools:
        global _allowed_tools
        try:
            parsed = json.loads(args.allowed_tools)
        except json.JSONDecodeError as e:
            print(f"Invalid --allowed-tools JSON: {e}")
            return
        if not isinstance(parsed, list) or not all(isinstance(n, str) for n in parsed):
            print(f"--allowed-tools must be a JSON array of strings; got {parsed!r}")
            return
        _allowed_tools = parsed

    # Stop flag path
    stop_flag = PROJECT_ROOT / "auto_trader" / f".stop_{run_id}"

    print("=" * 70)
    print("AUTO-TRADER")
    print("=" * 70)
    print(f"Run ID:      {run_id}")
    print(f"Max experiments: {args.max_experiments}")
    print(f"Direction:   {direction}")
    print(f"Optimize:    {args.metric}")
    print(f"Conditions:  {args.condition or 'none'}")
    print(f"Backtest:    {args.start} to {args.end}")
    print(f"Capital:     ${args.capital:,.0f}")
    print(f"Model:       {args.model}")
    print("=" * 70)

    best_value = None
    start_from = 1

    # Check for existing experiments (resume from stopped run)
    existing = get_experiment_history(run_id, limit=1000)
    if existing:
        start_from = max(e["iteration"] for e in existing) + 1
        higher = METRIC_DIRECTION.get(args.metric, True)
        best_exp = get_best_experiment(run_id, higher_is_better=higher)
        if best_exp and best_exp.get("target_value") is not None:
            best_value = best_exp["target_value"]
        print(f"Resuming: {len(existing)} experiments already completed, starting from #{start_from}")
        if best_value is not None:
            print(f"Current best {args.metric}: {best_value}")
    else:
        # Iteration 0: backtest starting portfolio if provided (only on fresh runs)
        if args.starting_portfolio and Path(args.starting_portfolio).exists():
            print(f"\n{'=' * 70}")
            print("EXPERIMENT 0 (Starting Portfolio)")
            print(f"{'=' * 70}")

            sp_config = json.loads(Path(args.starting_portfolio).read_text())
            sp_name = sp_config.get("name", "Starting Portfolio")
            sleeves = sp_config.get("sleeves", sp_config.get("strategies", []))
            print(f"  Portfolio: {sp_name} ({len(sleeves)} sleeves)")
            print(f"  Running backtest ({args.start} to {args.end})...")

            bt_result = run_backtest(sp_config, args.start, args.end, args.capital)

            if bt_result:
                metrics = bt_result["metrics"]
                sp_sleeve_trades = bt_result["sleeve_trades"]
                target_value = metrics.get(args.metric)
                conditions_met_flag, cond_detail = check_conditions(metrics, conditions)

                decision = "keep" if conditions_met_flag and target_value is not None else "discard"

                exp_id = log_experiment(
                    run_id=run_id, iteration=0,
                    thesis=f"User-provided starting portfolio: {sp_name}",
                    assumptions=["Starting point for optimization"],
                    portfolio_config=sp_config,
                    metrics=metrics, target_metric=args.metric,
                    target_value=target_value, conditions=conditions,
                    conditions_met=conditions_met_flag, decision=decision,
                    best_value_so_far=0,
                    backtest_start=args.start, backtest_end=args.end,
                    initial_capital=args.capital, model="none",
                )

                # Persist trades for the starting portfolio experiment
                try:
                    from deploy_engine import persist_trades
                    for sleeve in sp_sleeve_trades:
                        if sleeve["trades"]:
                            persist_trades("experiment", exp_id, sleeve["trades"],
                                           sleeve_label=sleeve["label"])
                except Exception as e:
                    print(f"  ⚠ Trade persist failed for {exp_id}: {e}")

                if decision == "keep":
                    best_value = target_value
                    _update_run_status(run_id, "running",
                                       best_metric_value=best_value)

                print(f"  {args.metric}: {target_value}")
                for d in cond_detail:
                    status = "PASS" if d["met"] else "FAIL"
                    print(f"    {d['metric']} {d['operator']} {d['value']}: actual={d.get('actual')} [{status}]")
                print(f"  Decision: {decision.upper()}")
                if decision == "keep":
                    print(f"  BASELINE: {target_value}")
            else:
                print("  Starting portfolio backtest failed — continuing from scratch")

            # Clean up temp file
            Path(args.starting_portfolio).unlink(missing_ok=True)

    for i in range(start_from, args.max_experiments + 1):
        # Check stop flag
        if stop_flag.exists():
            print(f"\nStop flag detected. Stopping after iteration {i-1}.")
            stop_flag.unlink(missing_ok=True)
            _update_run_status(run_id, "stopped")
            break

        print(f"\n{'=' * 70}")
        print(f"EXPERIMENT {i}/{args.max_experiments}")
        print(f"{'=' * 70}")

        # Update progress in DB
        _update_run_status(run_id, "running", current_iteration=i)

        result = await run_agent_iteration(
            run_id=run_id,
            iteration=i,
            target_metric=args.metric,
            conditions=conditions,
            best_value=best_value,
            backtest_start=args.start,
            backtest_end=args.end,
            initial_capital=args.capital,
            model=args.model,
            sector=args.sector,
            alpha_benchmark=args.alpha_benchmark,
        )

        if result["decision"] == "keep" and result.get("target_value") is not None:
            best_value = result["target_value"]
            _update_run_status(run_id, "running",
                               best_metric_value=best_value,
                               best_experiment_id=result.get("id"))

        print(f"\n  Duration: {result.get('duration_seconds', 0):.0f}s")

    # Final summary
    summary = get_run_summary(run_id)
    best = get_best_experiment(run_id, higher_is_better=METRIC_DIRECTION.get(args.metric, True))

    print("\n" + "=" * 70)
    print("RUN COMPLETE")
    print("=" * 70)
    print(f"Run ID:       {run_id}")
    print(f"Experiments:   {summary['total_experiments']}")
    print(f"Keeps:        {summary['keeps']}")
    print(f"Discards:     {summary['discards']}")
    print(f"Errors:       {summary['errors']}")
    print(f"Best {args.metric}: {summary['best_value']}")

    # Mark run as completed (reached max_experiments)
    _update_run_status(run_id, "completed")

    if best:
        print(f"\nBest thesis: {best.get('thesis', 'N/A')}")
        config = json.loads(best["portfolio_config"]) if best.get("portfolio_config") else {}
        print(f"Config: {json.dumps(config, indent=2)[:500]}")


if __name__ == "__main__":
    asyncio.run(main())
