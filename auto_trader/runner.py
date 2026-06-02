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

from auto_trader.schema import log_experiment, get_experiment_history, get_best_experiment, get_run_summary, get_db
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
# When False, analyst notes are kept out of the agent's history context.
# Memos are still generated; the agent just doesn't see them.
_include_analyst_notes: bool = True


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
        "opus-4-8": "claude-opus-4-8",
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


def build_history_context(run_id: str, target_metric: str, limit: int = 20,
                          aggregator: str = "overall",
                          include_analyst_notes: bool = True) -> str:
    """Build full history of past experiments for the agent to learn from.

    Lessons convention: the `lessons` field stored on experiment row N is the
    agent's reflection at the START of iteration N, i.e. a post-mortem of
    experiments 1..N-1 (most directly about N-1). When rendering, we therefore
    attach lessons[N] under the display block for experiment N-1 — that is the
    experiment the lesson is actually about.

    To avoid anchoring the agent on stale self-interpretation, only the 3 most
    recently written lessons are surfaced. Older lessons stay in the DB for UI
    display but are stripped from the prompt.
    """
    history = get_experiment_history(run_id, limit=limit)
    if not history:
        return "No previous experiments. This is your first experiment."

    history_asc = list(reversed(history))  # oldest first

    # Pair each experiment with the lesson the AGENT wrote about it on the
    # next iteration. lessons_for_exp[exp_iter] = (writing_iter, text).
    lessons_for_exp: dict[int, tuple[int, str]] = {}
    for i in range(len(history_asc) - 1):
        next_exp = history_asc[i + 1]
        lesson_text = next_exp.get("lessons")
        if not lesson_text or not isinstance(lesson_text, str):
            continue
        lesson_text = lesson_text.strip()
        if not lesson_text:
            continue
        lessons_for_exp[history_asc[i]["iteration"]] = (
            next_exp["iteration"], lesson_text,
        )

    # Keep only the 3 most-recently-written lessons (by writing_iter).
    recent_lessons = dict(
        sorted(
            lessons_for_exp.items(),
            key=lambda kv: kv[1][0],
            reverse=True,
        )[:3]
    )

    lines = [f"## Past Experiments ({len(history)} most recent)\n"]
    reflection_note = (
        "- **Your reflection** — a free-text lesson you wrote at the start of "
        "the next iteration. Your own narrative, not independently verified. "
        "Only the 3 most-recently-written are shown (older self-reflections "
        "are dropped to avoid anchoring on stale interpretations).\n"
    )
    if include_analyst_notes:
        lines.append(
            "Each experiment below carries two memory streams — treat them as "
            "different kinds of evidence:\n"
            + reflection_note +
            "- **Analyst observations** — forward-looking claims extracted by an "
            "independent post-trade analyst that reviewed the actual trade "
            "ledger, realized P&L, NAV, and factor attribution after each "
            "experiment. Third-party, data-grounded. The analyst sees what "
            "actually happened in the books, not what your thesis predicted.\n"
        )
    else:
        lines.append(
            "Each experiment below carries a memory stream:\n" + reflection_note
        )
    for exp in history_asc:
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
        lines.append(f"**Metrics (training-period):** {', '.join(metrics)}")

        # Walk-forward eval block — render distribution stats so the agent
        # sees per-period stability, not just one aggregate number.
        eval_json = exp.get("eval_metrics_json")
        if eval_json:
            try:
                ev = json.loads(eval_json) if isinstance(eval_json, str) else eval_json
            except json.JSONDecodeError:
                ev = None
            if isinstance(ev, dict):
                agg = ev.get("aggregated", {}) or {}
                spec = ev.get("spec", {}) or {}
                windows = ev.get("windows", []) or []
                n = len(windows)
                # One concise spread-stats line. Skip metrics with no data.
                parts = []
                for name, label in (
                    ("sharpe_ratio",          "Sharpe"),
                    ("alpha_ann_pct",         "Alpha"),
                    ("max_drawdown_pct",      "MaxDD"),
                    ("annualized_volatility_pct", "Vol"),
                ):
                    b = agg.get(name)
                    if not b:
                        continue
                    fmt = "{:+.1f}%" if name.endswith("_pct") else "{:.2f}"
                    mn = fmt.format(b["min"]); md = fmt.format(b["median"]); mx = fmt.format(b["max"])
                    parts.append(f"{label} min/med/max={mn}/{md}/{mx}")
                if parts:
                    w_label = spec.get("window", "?")
                    o_label = spec.get("overlap", "?")
                    agg_name = exp.get("target_aggregator") or "overall"
                    target_str = ""
                    if agg_name != "overall" and exp.get("target_value") is not None:
                        target_str = f" | target={exp['target_aggregator']}({exp['target_metric']})={exp['target_value']:.4f}"
                    lines.append(
                        f"**Eval ({n} windows, {w_label}/{o_label}):** "
                        + " | ".join(parts) + target_str
                    )

                # Per-window detail — labels match the get_experiment_trades/
                # get_experiment_stats `window=` filter so the agent can drill
                # into any individual window via existing tools.
                if windows:
                    lines.append("**Per-window:**")
                    for w in windows:
                        m = w.get("metrics", {}) or {}
                        bits = []
                        if m.get("sharpe_ratio") is not None:
                            bits.append(f"Sharpe={m['sharpe_ratio']:.2f}")
                        if m.get("alpha_ann_pct") is not None:
                            bits.append(f"Alpha={m['alpha_ann_pct']:+.1f}%")
                        if m.get("max_drawdown_pct") is not None:
                            bits.append(f"MaxDD={m['max_drawdown_pct']:+.1f}%")
                        if m.get("annualized_volatility_pct") is not None:
                            bits.append(f"Vol={m['annualized_volatility_pct']:.1f}%")
                        if bits:
                            lines.append(f"- {w.get('label','?')}: {', '.join(bits)}")

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

        # Lesson reflecting on THIS experiment (written at the start of the
        # NEXT iteration). Only surfaced for the 3 most-recently-written
        # lessons; earlier ones are dropped to avoid anchoring on stale
        # self-interpretation.
        lesson_pair = recent_lessons.get(exp["iteration"])
        if lesson_pair is not None:
            writing_iter, lesson_text = lesson_pair
            lines.append(
                f"**Your reflection (written at start of iter {writing_iter}):**"
            )
            lines.append(lesson_text)

        # Analyst observations for this specific experiment.
        if include_analyst_notes:
            try:
                from auto_trader.analyst import render_memo_items_for_experiment
                analyst_block = render_memo_items_for_experiment(exp["id"])
                if analyst_block:
                    lines.append(analyst_block)
            except Exception:
                pass

        lines.append("")

    higher = aggregator_higher_is_better(aggregator, target_metric)
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


def is_improvement(metric: str, new_value: float, best_value: float,
                   aggregator: str = "overall") -> bool:
    """Check if new_value is an improvement over best_value.

    Direction is metric × aggregator:
      - aggregator='overall'/'mean'/'median'/'min'/'max'/'p10'/'p25' →
        preserve the metric's direction (e.g., higher Sharpe is better).
      - aggregator='stdev'/'iqr'/'range' → lower is better (consistency).
      - aggregator='snr' → higher is better (more signal per unit noise).
    """
    higher_is_better = aggregator_higher_is_better(aggregator, metric)
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


def _run_one_backtest(portfolio_config: dict, start: str, end: str, capital: float,
                      sector: str | None = None) -> dict | None:
    """Run a single backtest over one [start, end] window.

    Returns: {"metrics": {...}, "sleeve_trades": [{"label", "trades"}, ...]}
             or None on failure.

    This is the unit of work — `run_backtest` calls this once for the training
    period and N more times for each eval sub-window (when configured).

    Engine dispatch: default is v2 (the unified-position-book engine that all
    live deployments run). Set `engine_version: "v1"` on the portfolio config
    to opt back into the legacy engine. Keeping the agent on the same engine
    as deployments prevents optimize-vs-deploy metric drift.
    """
    try:
        if portfolio_config.get("engine_version") == "v1":
            from portfolio_engine import run_portfolio_backtest
        else:
            from portfolio_engine_v2 import run_portfolio_backtest
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

        # Surface which engine actually executed — observability + audit.
        # v2 tags its result with engine_version="v2"; v1 doesn't tag (legacy).
        engine_version = result.get("engine_version") or (
            "v1" if portfolio_config.get("engine_version") == "v1" else "v2"
        )

        return {
            "metrics": metrics,
            "sleeve_trades": sleeve_trades,
            "engine_version": engine_version,
        }
    except Exception as e:
        print(f"  Backtest failed: {e}")
        import traceback
        traceback.print_exc()
        return None


# ---------------------------------------------------------------------------
# Walk-forward window generation + aggregation
# ---------------------------------------------------------------------------


def _generate_eval_windows(eval_block) -> list[tuple[str, str, str]]:
    """Generate [(start_iso, end_iso, label), ...] from an EvalBlock.

    First window starts at eval.start. Step = window - overlap. A final
    partial window (would extend past eval.end) is DROPPED. Result may be
    empty if window > eval span.
    """
    from datetime import date

    spec = eval_block.spec
    window_delta = spec.window_delta()
    step_delta = spec.step_delta()

    cur_start = date.fromisoformat(eval_block.start)
    end_cap = date.fromisoformat(eval_block.end)

    windows: list[tuple[str, str, str]] = []
    # Safety: bound the loop to prevent any pathological infinite loop on a
    # zero-step relativedelta (validator already disallows overlap >= window).
    for _ in range(1024):
        cur_end = cur_start + window_delta
        if cur_end > end_cap:
            break
        label = f"{cur_start.isoformat()}_{cur_end.isoformat()}"
        windows.append((cur_start.isoformat(), cur_end.isoformat(), label))
        next_start = cur_start + step_delta
        if next_start <= cur_start:  # safety; should be unreachable
            break
        cur_start = next_start
    return windows


# Metric names whose distribution we aggregate across eval windows.
_AGG_METRIC_NAMES = (
    "sharpe_ratio",
    "sortino_ratio",
    "calmar_ratio",
    "alpha_ann_pct",
    "alpha_vs_market_pct",
    "alpha_vs_sector_pct",
    "annualized_return_pct",
    "annualized_volatility_pct",
    "total_return_pct",
    "max_drawdown_pct",
    "win_rate_pct",
    "profit_factor",
)


def _quantile(sorted_xs: list[float], q: float) -> float | None:
    """Linear-interp quantile (type-7, R default)."""
    if not sorted_xs:
        return None
    n = len(sorted_xs)
    if n == 1:
        return sorted_xs[0]
    pos = (n - 1) * q
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return sorted_xs[lo] + frac * (sorted_xs[hi] - sorted_xs[lo])


# Floor for stdev when computing snr (mean/std). At small N, std can be tiny
# and produce explosive ratios that dominate optimization. 1e-6 is below any
# meaningful financial dispersion; preserves sign and reasonable magnitude.
_SNR_STD_FLOOR = 1e-6


def _sample_stdev(xs: list[float]) -> float | None:
    """Sample (n-1) standard deviation. Returns None for n < 2."""
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    return (sum((x - m) ** 2 for x in xs) / (n - 1)) ** 0.5


def _aggregate_window_metrics(windows: list[dict]) -> dict:
    """Reduce per-window metric scalars to a fixed set of summary stats.

    Per metric, returns: {mean, median, min, max, p10, p25, stdev, iqr, range,
    snr, count}. Some are None when the sample is too small:
      - stdev / snr / iqr / range are None when count < 2.
      - snr clamps stdev to _SNR_STD_FLOOR to avoid explosions; sign-preserving.

    Skips metrics not present in any window. Skips windows where the metric is
    None. Returns an empty dict for any metric that no window reports.
    """
    out: dict[str, dict] = {}
    for name in _AGG_METRIC_NAMES:
        vals = [w["metrics"].get(name) for w in windows if w.get("metrics")]
        vals = [v for v in vals if v is not None]
        if not vals:
            continue
        srt = sorted(vals)
        n = len(vals)
        mean = sum(vals) / n
        std = _sample_stdev(vals)
        p25 = _quantile(srt, 0.25)
        p75 = _quantile(srt, 0.75)
        out[name] = {
            "mean":   mean,
            "median": _quantile(srt, 0.5),
            "min":    srt[0],
            "max":    srt[-1],
            "p10":    _quantile(srt, 0.10),
            "p25":    p25,
            "stdev":  std,
            "iqr":    (p75 - p25) if (n >= 2 and p25 is not None and p75 is not None) else None,
            "range":  (srt[-1] - srt[0]) if n >= 2 else None,
            "snr":    (mean / max(std, _SNR_STD_FLOOR)) if std is not None else None,
            "count":  n,
        }
    return out


def run_backtest(
    portfolio_config: dict,
    start: str | None = None,
    end: str | None = None,
    capital: float | None = None,
    sector: str | None = None,
    config: "BacktestConfig | None" = None,  # noqa: F821 (forward-ref string)
) -> dict | None:
    """Run a backtest (single training period, optionally + walk-forward eval).

    Two calling conventions:

      1. Legacy flat args:
         run_backtest(portfolio_config, start, end, capital, sector=sector)
         → single training-period backtest, returns
           {"metrics": {...}, "sleeve_trades": [...]}.

      2. BacktestConfig:
         run_backtest(portfolio_config, config=bt_cfg)
         → training-period backtest + N eval-window backtests if cfg.eval set.
           Returns
             {
               "metrics": {...},           # training-period
               "sleeve_trades": [...],     # training-period
               "eval": {
                 "windows":    [{"label","start","end","metrics","sleeve_trades"}, ...],
                 "aggregated": {<metric>: {mean, median, min, max, p25, count}, ...},
                 "spec":       {"window": "...", "overlap": "..."},
               }   # absent when cfg.eval is None
             }

    Eval windows each run a fresh-capital backtest at cfg.initial_capital; no
    state rolls between windows. Partial trailing window (extends past
    eval.end) is dropped. Failed eval windows are logged and skipped.
    """
    # Resolve the call style.
    if config is None:
        from server.models.backtest import BacktestConfig
        if start is None or end is None or capital is None:
            raise ValueError(
                "run_backtest requires either `config=BacktestConfig(...)` "
                "or all of (start, end, capital)"
            )
        config = BacktestConfig.from_legacy_args(start, end, capital, sector=sector)
    # When both styles are provided, BacktestConfig wins (sector arg ignored).

    # 1. Training-period backtest (mandatory).
    training = _run_one_backtest(
        portfolio_config,
        config.training_start, config.training_end,
        config.initial_capital, config.sector,
    )
    if training is None:
        return None

    # 2. If no eval block, done — preserve today's shape exactly.
    if config.eval is None:
        return training

    # 3. Walk-forward eval windows.
    window_specs = _generate_eval_windows(config.eval)
    if not window_specs:
        # Eval configured but produced zero windows (window > span). Surface
        # warning, return training with empty eval block.
        print(f"  [warn] eval block produced 0 windows (window {config.eval.spec.window} > span)")
        training["eval"] = {
            "windows": [],
            "aggregated": {},
            "spec": {"window": config.eval.spec.window, "overlap": config.eval.spec.overlap},
        }
        return training

    print(f"  Running {len(window_specs)} eval window(s)...")
    eval_windows: list[dict] = []
    for w_start, w_end, w_label in window_specs:
        res = _run_one_backtest(
            portfolio_config, w_start, w_end,
            config.initial_capital, config.sector,
        )
        if res is None:
            print(f"  [warn] eval window {w_label} failed; skipping")
            continue
        eval_windows.append({
            "label":  w_label,
            "start":  w_start,
            "end":    w_end,
            "metrics": res["metrics"],
            "sleeve_trades": res["sleeve_trades"],
        })

    aggregated = _aggregate_window_metrics(eval_windows)

    training["eval"] = {
        "windows": eval_windows,
        "aggregated": aggregated,
        "spec": {"window": config.eval.spec.window, "overlap": config.eval.spec.overlap},
    }
    return training


def _resolve_target_value(
    training_metrics: dict, eval_aggregated: dict, target_metric_name: str, aggregator: str,
) -> float | None:
    """Resolve the single scalar the agent climbs.

    aggregator='overall' → reads `target_metric_name` from training_metrics.
    Any other aggregator → reads from
        eval_aggregated[target_metric_name][aggregator].
    Returns None if the metric or aggregator is missing/undefined (e.g. stdev
    with only one window, or 'p10' on a metric no window reports).

    Direction note: dispersion aggregators (stdev, iqr, range) are typically
    MINIMIZED (consistency target), not maximized. Whether maximizing or
    minimizing applies is handled by AGGREGATOR_DIRECTION below.
    """
    if aggregator == "overall":
        v = training_metrics.get(target_metric_name)
        return v if v is not None else None
    bucket = (eval_aggregated or {}).get(target_metric_name)
    if not bucket:
        return None
    return bucket.get(aggregator)


# Whether the agent should maximize or minimize an aggregator (independent of
# the underlying metric). Most aggregators preserve the metric's direction —
# higher Sharpe is better, higher median Sharpe is better. Dispersion ones are
# usually MINIMIZED (lower stdev = more consistent). 'snr' is maximized
# (higher mean/std = more consistent edge).
AGGREGATOR_DIRECTION = {
    "overall": "preserve",   # follow underlying METRIC_DIRECTION
    "mean":    "preserve",
    "median":  "preserve",
    "min":     "preserve",
    "max":     "preserve",
    "p10":     "preserve",
    "p25":     "preserve",
    "stdev":   "minimize",   # consistency target
    "iqr":     "minimize",
    "range":   "minimize",
    "snr":     "maximize",   # high mean / low std = consistent edge
}


def aggregator_higher_is_better(aggregator: str, metric_name: str) -> bool:
    """Whether the agent should maximize the resolved scalar for this combo."""
    direction = AGGREGATOR_DIRECTION.get(aggregator, "preserve")
    if direction == "preserve":
        return METRIC_DIRECTION.get(metric_name, True)
    if direction == "maximize":
        return True
    return False  # "minimize"


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
    eval_block: dict | None = None,
    target_aggregator: str = "overall",
) -> dict:
    """Run a single agent iteration. Returns experiment result.

    `eval_block` (optional) and `target_aggregator` enable walk-forward eval:
        eval_block = {
            "start": "YYYY-MM-DD",
            "end":   "YYYY-MM-DD",
            "spec":  {"window": "Ny|Nm|Nd", "overlap": "..."},
        }
        target_aggregator ∈ {"overall","mean","median","min","max","p25"}

    When `eval_block` is None, behavior is identical to legacy single-period
    backtest. When set, the backtest runs N+1 simulations (training + each
    eval sub-window) and the agent's "best so far" climbs the aggregator.
    """
    from claude_agent_sdk import query, ClaudeAgentOptions
    from backtest_engine import clear_precompute_cache

    # Reset the per-iteration precompute_condition memoization cache. The
    # cache speeds up repeated rank_signals/evaluate_signal calls within a
    # single iteration but must be flushed at iteration boundaries to bound
    # memory and prevent any cross-iteration drift if market data updates.
    prior_cache_stats = clear_precompute_cache()
    if prior_cache_stats.get("entries", 0) > 0:
        h = prior_cache_stats["hits"]
        m = prior_cache_stats["misses"]
        total = h + m
        hit_rate = (h / total * 100) if total > 0 else 0
        print(f"  [precompute cache] prior iter: {h} hits / {m} misses "
              f"({hit_rate:.0f}% hit rate, {prior_cache_stats['entries']} entries)")

    t0 = time.time()

    # Build the prompt
    program = load_program()
    history = build_history_context(run_id, target_metric, aggregator=target_aggregator,
                                    include_analyst_notes=_include_analyst_notes)

    conditions_desc = ", ".join(
        f"{c['metric']} {c['operator']} {c['value']}" for c in conditions
    )

    sector_desc = f"\n**Sector:** {sector} — All data queries are restricted to {sector} stocks only. Alpha is measured against the {sector} sector ETF." if sector else ""
    benchmark_desc = f"sector ETF" if alpha_benchmark == "sector" else "S&P 500 (SPY)"

    # Frame the objective so the agent knows whether it's climbing the
    # training-period scalar or the eval-window aggregator. When eval is set
    # but aggregator='overall', the training scalar is still the target and
    # the eval block is purely informational context.
    direction = "maximizes" if aggregator_higher_is_better(target_aggregator, target_metric) else "minimizes"
    if eval_block and target_aggregator != "overall":
        spec = eval_block.get("spec", {})
        objective_line = (
            f"**Objective:** Design a portfolio that {direction} the "
            f"**{target_aggregator} of `{target_metric}` across walk-forward eval windows** "
            f"({spec.get('window','?')} window, {spec.get('overlap','?')} overlap, "
            f"{eval_block.get('start','?')} → {eval_block.get('end','?')})."
        )
        period_line = (
            f"**Training period (informational; the eval aggregator is the target):** "
            f"{backtest_start} to {backtest_end}\n"
            f"**Eval period:** {eval_block.get('start','?')} to {eval_block.get('end','?')} "
            f"({spec.get('window','?')} windows, {spec.get('overlap','?')} overlap)"
        )
    elif eval_block:
        # eval set but aggregator='overall' — eval is supporting evidence only.
        spec = eval_block.get("spec", {})
        objective_line = (
            f"**Objective (portfolio level):** Design a portfolio that {direction} `{target_metric}` "
            f"over the training period. Eval windows are reported as supporting evidence."
        )
        period_line = (
            f"**Training period (target):** {backtest_start} to {backtest_end}\n"
            f"**Eval period (informational):** {eval_block.get('start','?')} to {eval_block.get('end','?')} "
            f"({spec.get('window','?')} windows, {spec.get('overlap','?')} overlap)"
        )
    else:
        objective_line = (
            f"**Objective (portfolio level):** Design a portfolio that {direction} `{target_metric}`."
        )
        period_line = f"**Backtest period:** {backtest_start} to {backtest_end}"

    best_label = f"{target_aggregator} {target_metric}" if target_aggregator != "overall" else target_metric
    best_line = (
        f"**Current best {best_label}:** {best_value:.4f}"
        if best_value is not None
        else "**No best yet — this is the first experiment.**"
    )

    prompt = f"""You are on experiment {iteration} of an autonomous research loop.

{objective_line}
**Conditions (portfolio level):** {conditions_desc if conditions else 'None'}
{period_line}
**Capital:** ${initial_capital:,.0f}
**Alpha benchmark:** {benchmark_desc}{sector_desc}
{best_line}

**Knowledge cutoff: {backtest_end}**
You are researching as of {backtest_end}. You do not know what happens after this date.
- All market data queries are automatically limited to data on or before {backtest_end}
- Do NOT use your training knowledge about market events, prices, or outcomes after {backtest_end}
- Form your thesis based only on data you query and patterns you observe within the allowed period
- Pretend today is {backtest_end}

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
    # Opus 4.7+ reject the legacy thinking.type='enabled' shape the CLI
    # defaults to. Force adaptive thinking for 4.7/4.8 only; leave other
    # models on their defaults so we don't alter working behavior.
    #
    # Note: Opus 4.7's thinking blocks return with empty `thinking` text
    # and a signature only (display defaults to "omitted" per-model).
    # Anthropic's docs say passing display="summarized" unseals the text,
    # but claude-agent-sdk 0.1.61's subprocess transport only reads
    # `type` from this dict (see subprocess_cli.py:305-312) and the
    # bundled CLI binary has no --display flag. Until the SDK plumbs
    # this through, the value is not actually configurable here.
    if resolved_model in ("claude-opus-4-7", "claude-opus-4-8"):
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

    # Run backtest — single training period if no eval_block, else N+1 backtests.
    print(f"  Running backtest ({backtest_start} to {backtest_end})...")
    emit_event(run_id, "backtest_started", {"experiment_number": iteration})

    # Build BacktestConfig (with optional eval) and run.
    from server.models.backtest import BacktestConfig, EvalBlock, WindowSpec
    bt_cfg_kwargs = dict(
        training_start=backtest_start, training_end=backtest_end,
        initial_capital=initial_capital, sector=sector,
        benchmark="sector" if alpha_benchmark == "sector" else "market",
    )
    if eval_block:
        bt_cfg_kwargs["eval"] = EvalBlock(
            start=eval_block["start"], end=eval_block["end"],
            spec=WindowSpec(**eval_block.get("spec", {})),
        )
    bt_cfg = BacktestConfig(**bt_cfg_kwargs)
    bt_result = run_backtest(portfolio_config, config=bt_cfg)

    if bt_result is None:
        metrics = None
        eval_data = None
    else:
        metrics = bt_result["metrics"]
        sleeve_trades = bt_result["sleeve_trades"]
        eval_data = bt_result.get("eval")  # None or {"windows", "aggregated", "spec"}

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
    # Mirror the same alpha mapping inside each eval window so per-window
    # alpha_ann_pct is consistent with the agent's chosen benchmark.
    if eval_data:
        for w in eval_data.get("windows", []):
            wm = w.get("metrics", {})
            if alpha_benchmark == "sector" and "alpha_vs_sector_pct" in wm:
                wm["alpha_ann_pct"] = wm["alpha_vs_sector_pct"]
            elif "alpha_vs_market_pct" in wm:
                wm["alpha_ann_pct"] = wm["alpha_vs_market_pct"]
        # Rebuild aggregated dict to reflect the alpha remap above.
        eval_data["aggregated"] = _aggregate_window_metrics(eval_data["windows"])

    # Score — resolve the single scalar the agent climbs.
    eval_aggregated = (eval_data or {}).get("aggregated", {})
    target_value = _resolve_target_value(metrics, eval_aggregated, target_metric, target_aggregator)
    conditions_met, conditions_detail = check_conditions(metrics, conditions)

    improved = (
        target_value is not None
        and (best_value is None or is_improvement(target_metric, target_value, best_value,
                                                  aggregator=target_aggregator))
        and conditions_met
    )
    decision = "keep" if improved else "discard"

    duration_total = time.time() - t0

    # Save portfolio to portfolios table (idempotent) — links experiment → portfolio
    portfolio_id = save_portfolio(portfolio_config)

    # Persist the eval block as JSON. Strip per-window sleeve_trades from the
    # JSON blob to keep the row size bounded — trades are persisted separately
    # in the trades table with window_label, queryable via get_experiment_trades.
    eval_metrics_json = None
    if eval_data is not None:
        eval_compact = {
            "spec": eval_data.get("spec"),
            "aggregated": eval_data.get("aggregated"),
            "windows": [
                {"label": w["label"], "start": w["start"], "end": w["end"], "metrics": w["metrics"]}
                for w in eval_data.get("windows", [])
            ],
        }
        eval_metrics_json = json.dumps(eval_compact)

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
        eval_metrics_json=eval_metrics_json,
        target_aggregator=target_aggregator,
    )

    # Persist trades per sleeve under source_type='experiment'.
    # Training-period trades have window_label=NULL; eval-window trades carry
    # their window label so get_experiment_trades can filter by window.
    # Failure here is enrichment loss only — the experiment row is already saved.
    try:
        from deploy_engine import persist_trades
        total = 0
        # Training-period trades (window_label=NULL).
        for sleeve in sleeve_trades:
            if sleeve["trades"]:
                total += persist_trades("experiment", exp_id, sleeve["trades"],
                                        sleeve_label=sleeve["label"])
        # Eval-window trades (window_label = window's label).
        if eval_data is not None:
            for w in eval_data.get("windows", []):
                w_label = w["label"]
                for sleeve in w.get("sleeve_trades", []):
                    if sleeve["trades"]:
                        total += persist_trades(
                            "experiment", exp_id, sleeve["trades"],
                            sleeve_label=sleeve["label"], window_label=w_label,
                        )
        if total:
            print(f"  💾 {total} trade(s) persisted for {exp_id}")
    except Exception as e:
        print(f"  ⚠ Trade persist failed for {exp_id}: {e}")

    # Auto-run the post-trade analyst. Failure is enrichment loss only — the
    # experiment row + trades are already saved.
    try:
        from auto_trader.analyst import analyst_pass
        analyst_result = await analyst_pass(exp_id)
        if analyst_result.get("error"):
            print(f"  ⚠ analyst_pass returned error for {exp_id}: {analyst_result['error']}")
        else:
            print(f"  📝 analyst: {analyst_result['n_items']} items, "
                  f"{analyst_result['memo_chars']} chars, "
                  f"{analyst_result['duration_seconds']}s")
    except Exception as e:
        print(f"  ⚠ analyst_pass failed for {exp_id}: {e}")

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
    parser.add_argument("--eval-file", type=str, default=None,
                        help="Path to JSON file with walk-forward eval block: "
                             "{'start':..., 'end':..., 'spec':{'window':'2y','overlap':'1y'}}")
    parser.add_argument("--target-aggregator", type=str, default="overall",
                        choices=("overall", "mean", "median", "min", "max",
                                  "p10", "p25", "stdev", "iqr", "range", "snr"),
                        help="How to reduce per-window metrics to the agent's optimization scalar. "
                             "Non-'overall' requires --eval-file. "
                             "Dispersion (stdev/iqr/range) is MINIMIZED; snr (mean/stdev) is maximized.")
    parser.add_argument("--no-analyst-notes", action="store_true",
                        help="Don't surface analyst notes in the agent's history context. "
                             "Memos are still generated; the agent just doesn't see them.")
    args = parser.parse_args()

    # Load eval block from sidecar file.
    eval_block = None
    if args.eval_file:
        eval_path = Path(args.eval_file)
        if not eval_path.exists():
            print(f"--eval-file not found: {args.eval_file}")
            return
        try:
            eval_block = json.loads(eval_path.read_text())
        except json.JSONDecodeError as e:
            print(f"Invalid --eval-file JSON: {e}")
            return
    if args.target_aggregator != "overall" and eval_block is None:
        print(f"--target-aggregator={args.target_aggregator!r} requires --eval-file to be set")
        return

    if args.metric not in VALID_METRICS:
        print(f"Invalid metric: '{args.metric}'. Valid: {VALID_METRICS}")
        return

    conditions = parse_conditions(args.condition)
    run_id = args.run_id or generate_run_id()
    direction = "maximize" if aggregator_higher_is_better(args.target_aggregator, args.metric) else "minimize"

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

    if args.no_analyst_notes:
        global _include_analyst_notes
        _include_analyst_notes = False

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
        higher = aggregator_higher_is_better(args.target_aggregator, args.metric)
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

                # Auto-run the post-trade analyst on the starting portfolio.
                try:
                    from auto_trader.analyst import analyst_pass
                    analyst_result = await analyst_pass(exp_id)
                    if analyst_result.get("error"):
                        print(f"  ⚠ analyst_pass returned error for {exp_id}: {analyst_result['error']}")
                    else:
                        print(f"  📝 analyst: {analyst_result['n_items']} items, "
                              f"{analyst_result['memo_chars']} chars, "
                              f"{analyst_result['duration_seconds']}s")
                except Exception as e:
                    print(f"  ⚠ analyst_pass failed for {exp_id}: {e}")

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
            eval_block=eval_block,
            target_aggregator=args.target_aggregator,
        )

        if result["decision"] == "keep" and result.get("target_value") is not None:
            best_value = result["target_value"]
            _update_run_status(run_id, "running",
                               best_metric_value=best_value,
                               best_experiment_id=result.get("id"))

        print(f"\n  Duration: {result.get('duration_seconds', 0):.0f}s")

    # Final summary
    summary = get_run_summary(run_id)
    best = get_best_experiment(run_id, higher_is_better=aggregator_higher_is_better(args.target_aggregator, args.metric))

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
