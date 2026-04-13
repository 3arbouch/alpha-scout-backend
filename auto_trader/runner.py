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
    """Load strategy and portfolio schemas from the engines."""
    import json as _json
    try:
        from backtest_engine import get_config_schema as strategy_schema
        from portfolio_engine import get_config_schema as portfolio_schema
        strat = _json.dumps(strategy_schema(), indent=2)
        port = _json.dumps(portfolio_schema(), indent=2)

        thesis_schema = _json.dumps({
            "thesis": {
                "thesis": {"type": "string", "description": "A clear 2-3 sentence investment thesis"},
                "assumptions": {"type": "array", "items": "string", "description": "List of assumptions that must hold for this thesis to work"},
            },
            "portfolio": "(see Portfolio Config Schema below)",
        }, indent=2)

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


_custom_prompt = None

def load_program() -> str:
    """Build the full system prompt: system.md + agent prompt + schemas."""
    # Fixed system instructions
    system_path = Path(__file__).parent / "system.md"
    system_instructions = system_path.read_text()

    # Agent prompt (custom or default)
    if _custom_prompt:
        agent_prompt = _custom_prompt
    else:
        agent_prompt = (Path(__file__).parent / "program.md").read_text()

    # Dynamic schemas
    schemas = _load_schemas()

    return system_instructions + "\n\n---\n\n" + agent_prompt + "\n\n---\n\n" + schemas


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


def build_history_context(run_id: str, target_metric: str, limit: int = 20) -> str:
    """Build full history of past experiments for the agent to learn from."""
    history = get_experiment_history(run_id, limit=limit)
    if not history:
        return "No previous experiments. This is your first experiment."

    lines = [f"## Past Experiments ({len(history)} most recent)\n"]
    for exp in reversed(history):  # oldest first
        status = "KEEP" if exp["decision"] == "keep" else "DISCARD"
        lines.append(f"### Experiment {exp['iteration']} — {status}")

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


def run_backtest(portfolio_config: dict, start: str, end: str, capital: float,
                 sector: str | None = None) -> dict | None:
    """Run a portfolio backtest. Returns metrics dict with both market and sector alpha."""
    try:
        from portfolio_engine import run_portfolio_backtest
        from backtest_engine import compute_benchmark, SECTOR_ETF_MAP

        config = normalize_config(portfolio_config)
        config["backtest"] = {
            "start": start,
            "end": end,
            "initial_capital": capital,
        }

        result = run_portfolio_backtest(config, force_close_at_end=True)
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

            # Sector benchmark — compute if sector is set
            if sector and sector in SECTOR_ETF_MAP:
                sector_bench = compute_benchmark(trading_dates, capital, sector=sector)
                if sector_bench:
                    sector_ann = sector_bench["metrics"]["annualized_return_pct"]
                    metrics["alpha_vs_sector_pct"] = round(ann_return - sector_ann, 2)
                    metrics["sector_benchmark_return_pct"] = sector_bench["metrics"]["total_return_pct"]

        return metrics
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

    conditions_desc = ", ".join(
        f"{c['metric']} {c['operator']} {c['value']}" for c in conditions
    )

    sector_desc = f"\n**Sector:** {sector} — All data queries are restricted to {sector} stocks only. Alpha is measured against the {sector} sector ETF." if sector else ""
    benchmark_desc = f"sector ETF" if alpha_benchmark == "sector" else "S&P 500 (SPY)"

    prompt = f"""You are on experiment {iteration} of an autonomous research loop.

**Objective:** Design a portfolio that {"maximizes" if METRIC_DIRECTION.get(target_metric, True) else "minimizes"} `{target_metric}`.
**Conditions:** {conditions_desc if conditions else 'None'}
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

{history}

---

Research the market data, form a thesis, and output your portfolio config.
Remember: query the data first, don't guess. Explore before you commit."""

    # Run the agent with skill discovery + query_market_data + validate_portfolio
    emit_event(run_id, "experiment_started", {"experiment_number": iteration})
    agent_output = []
    tool_calls = 0
    session_id = None
    auto_trader_tools = create_auto_trader_tools(stop_date=backtest_end, sector=sector)
    try:
        async for message in query(
            prompt=prompt,
            options=ClaudeAgentOptions(
                system_prompt=program,
                cwd=str(PROJECT_ROOT / "auto_trader"),
                model=model,
                setting_sources=["project"],
                allowed_tools=[
                    "Skill", "Read",
                    "mcp__auto_trader__query_market_data",
                    "mcp__auto_trader__validate_portfolio",
                ],
                mcp_servers={"auto_trader": auto_trader_tools},
                permission_mode="acceptEdits",
                max_turns=50,
            ),
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
    metrics = run_backtest(portfolio_config, backtest_start, backtest_end, initial_capital, sector=sector)

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

    exp_id = log_experiment(
        run_id=run_id, iteration=iteration,
        thesis=thesis, assumptions=assumptions, portfolio_config=portfolio_config,
        metrics=metrics, target_metric=target_metric, target_value=target_value,
        conditions=conditions, conditions_met=conditions_met,
        decision=decision, best_value_so_far=best_value or 0,
        backtest_start=backtest_start, backtest_end=backtest_end,
        initial_capital=initial_capital, model=model,
        session_id=session_id, duration_seconds=duration_total,
    )

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

            metrics = run_backtest(sp_config, args.start, args.end, args.capital)

            if metrics:
                target_value = metrics.get(args.metric)
                conditions_met_flag, cond_detail = check_conditions(metrics, conditions)

                decision = "keep" if conditions_met_flag and target_value is not None else "discard"

                log_experiment(
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
