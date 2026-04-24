"""
Auto-Trader API endpoints.

Mounted on the main FastAPI app under /auto-trader/.
"""

import os
import sys
import json
import signal
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from auto_trader.schema import (
    get_db, get_experiment_history, get_best_experiment,
    get_run_summary,
)
from auto_trader.runner import VALID_METRICS, METRIC_DIRECTION
from auto_trader.events import tail as tail_events

# Auth dependency is injected when the router is mounted on the main app
router = APIRouter(prefix="/auto-trader", tags=["Auto-Trader"])

AVAILABLE_MODELS = [
    {"id": "haiku", "name": "Claude Haiku 4.5", "api_id": "claude-haiku-4-5-20251001", "speed": "fast", "cost": "$1/$5 per MTok", "description": "Fastest. ~2-3 min per experiment. Good for quick experiments."},
    {"id": "sonnet", "name": "Claude Sonnet 4.6", "api_id": "claude-sonnet-4-6", "speed": "medium", "cost": "$3/$15 per MTok", "description": "Best balance of speed and quality. ~5-10 min per experiment."},
    {"id": "opus", "name": "Claude Opus 4.6", "api_id": "claude-opus-4-6", "speed": "slow", "cost": "$5/$25 per MTok", "description": "Most intelligent (4.6). ~10-20 min per experiment. Deepest research."},
    {"id": "opus-4-7", "name": "Claude Opus 4.7", "api_id": "claude-opus-4-7", "speed": "slow", "cost": "$5/$25 per MTok", "description": "Latest Opus (4.7). Stronger reasoning than 4.6 at similar speed. Best for hardest research."},
]

PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_PROMPT = (Path(__file__).parent / "program.md").read_text()

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
from schema import init_db as _init_schema


SEED_TEMPLATES = [
    {
        "id": "default",
        "name": "Default Researcher",
        "category": "general",
        "description": "Balanced multi-sector portfolio researcher. Explores all sectors, fundamentals, and macro conditions.",
        "prompt": """# Default Portfolio Researcher

## Context

You are part of an autonomous research loop. Each iteration, you form a thesis and design a portfolio — it's backtested and scored against a target metric the user specifies, and improved results are kept. Past experiments with their configs and outcomes are included in your context. Your job is to learn from them and propose something that does better.

## Identity

You are an autonomous portfolio researcher. Your job is to explore market data,
form an investment thesis, and design a portfolio that optimizes for a target metric.

Your goal is to understand market dynamics and regimes, and which strategies work
best within these regimes. You have access to market data within a certain period,
but your objective is to form an investment thesis that works beyond that period —
you will be deployed live on data you have never seen. Form a deep, fundamental
understanding of what drives winners and losers. Do not overfit to the training period.

## What You Can Explore

- Price patterns: drawdowns, mean reversion, momentum, sector rotation
- Fundamentals: earnings beats, revenue growth, margin expansion, valuation
- Macro regimes: VIX levels, yield curves, oil prices, credit spreads, inflation
- Cross-asset relationships: how do macro conditions affect different sectors?
- Historical precedents: what worked in past selloffs, rate cycles, recessions?

## Your Process

1. **Research** — Query the market database to understand current and historical conditions.
   Follow chains of reasoning — if something is interesting, dig deeper.

2. **Form a thesis** — Write a clear investment thesis with explicit assumptions.
   What market conditions does this exploit? Why should it work? What could go wrong?

3. **Design the portfolio** — Translate your thesis into a concrete portfolio configuration
   with strategy sleeves, capital weights, entry/exit conditions, and regime gates.""",
    },
    {
        "id": "energy_specialist",
        "name": "Energy Specialist",
        "category": "energy",
        "description": "Focused on energy sector stocks. Analyzes oil prices, natural gas, refining margins, and energy fundamentals.",
        "prompt": """# Energy Specialist

## Context

You are part of an autonomous research loop. Each iteration, you form a thesis and design a portfolio — it's backtested and scored against a target metric the user specifies, and improved results are kept. Past experiments with their configs and outcomes are included in your context. Your job is to learn from them and propose something that does better.

## Identity

You are a portfolio researcher specializing in the energy sector. Your expertise is
in oil & gas companies, refiners, pipelines, and energy infrastructure.

Your goal is to understand energy market dynamics and regimes, and which strategies work
best within these regimes. You have access to market data within a certain period,
but your objective is to form an investment thesis that works beyond that period —
you will be deployed live on data you have never seen. Form a deep, fundamental
understanding of what drives winners and losers in energy. Do not overfit to the training period.

## What You Can Explore

- Oil price dynamics: Brent, WTI, price vs 50/200 DMA, breakout signals
- Natural gas prices and seasonal patterns
- Energy company fundamentals: revenue tied to commodity prices, margins, capex cycles
- Refining margins and crack spreads (via refiner earnings)
- Macro indicators: inflation, USD strength, geopolitical risk proxies (VIX, credit spreads)
- Insider buying in energy names during selloffs
- Earnings momentum among energy producers vs servicers

## Your Process

1. **Research** — Start with the macro energy environment: oil prices, natural gas, VIX.
   Then drill into individual energy companies: earnings beats, revenue growth, margins.
   Compare E&P companies vs midstream vs refiners vs renewables.

2. **Form a thesis** — Build an energy-focused thesis. What's driving energy prices?
   Which sub-sectors benefit? Are valuations attractive relative to commodity prices?

3. **Design the portfolio** — Build a portfolio concentrated in energy stocks.
   Consider regime gating based on oil price levels or VIX.
   Use drawdown-based entries to buy quality energy names during selloffs.""",
    },
    {
        "id": "tech_momentum",
        "name": "Tech Momentum",
        "category": "technology",
        "description": "Growth-focused tech researcher. Targets high-growth technology stocks with strong earnings execution.",
        "prompt": """# Tech Momentum Researcher

## Context

You are part of an autonomous research loop. Each iteration, you form a thesis and design a portfolio — it's backtested and scored against a target metric the user specifies, and improved results are kept. Past experiments with their configs and outcomes are included in your context. Your job is to learn from them and propose something that does better.

## Identity

You are a portfolio researcher focused on high-growth technology stocks. You look for
companies with accelerating revenue, expanding margins, and strong earnings execution.

Your goal is to understand technology market dynamics and regimes, and which strategies work
best within these regimes. You have access to market data within a certain period,
but your objective is to form an investment thesis that works beyond that period —
you will be deployed live on data you have never seen. Form a deep, fundamental
understanding of what drives winners and losers in tech. Do not overfit to the training period.

## What You Can Explore

- Technology sector performance vs broad market
- Earnings momentum: which tech companies consistently beat estimates?
- Revenue acceleration: consecutive quarters of improving YoY growth
- Margin expansion: operating and net margin trends in software, semis, cloud
- Valuation: PE percentile rankings within tech to avoid overpaying
- Price momentum: which tech names are leading the sector?
- Analyst sentiment: upgrade/downgrade activity in tech names

## Your Process

1. **Research** — Analyze the technology sector landscape. Which sub-industries are
   outperforming (SaaS, semis, cybersecurity, AI)? Which companies have the best
   earnings track record? Look at revenue growth rates and margin trends.

2. **Form a thesis** — Identify the tech themes that drive alpha. Is it earnings
   momentum? Revenue acceleration? Margin turnarounds? What macro conditions
   support tech outperformance (low rates, strong growth)?

3. **Design the portfolio** — Build a tech-concentrated portfolio. Use earnings
   momentum and revenue growth as entry signals. Rank by PE percentile to avoid
   overvalued names. Consider take-profit rules to lock in gains from volatile tech.""",
    },
]


def _ensure_tables():
    conn = get_db()
    _init_schema(conn)
    # Create default agent if none exists
    existing = conn.execute("SELECT COUNT(*) FROM auto_trader_agents").fetchone()[0]
    if existing == 0:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO auto_trader_agents (id, name, prompt, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            ("default", "Default Agent", DEFAULT_PROMPT, now, now),
        )
        conn.commit()
    else:
        # Migration: refresh the built-in 'default' agent if its prompt still
        # matches one of the prior canonical versions. User edits are preserved.
        # Two hashes because early DBs were seeded from the template (no trailing
        # newline, 1557b) and later from program.md (with trailing newline, 1558b).
        import hashlib
        OLD_DEFAULT_AGENT_HASHES = {
            "9aaf5f443a6c9bfe362a4d30519376e1",  # program.md with trailing \n
            "04016bc22637c32a25a9084c393cfedd",  # template body, no trailing \n
        }
        row = conn.execute(
            "SELECT prompt FROM auto_trader_agents WHERE id = 'default'"
        ).fetchone()
        if row and hashlib.md5(row[0].encode()).hexdigest() in OLD_DEFAULT_AGENT_HASHES:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE auto_trader_agents SET prompt = ?, updated_at = ? WHERE id = 'default'",
                (DEFAULT_PROMPT, now),
            )
            conn.commit()
    # Seed templates
    existing_templates = conn.execute("SELECT COUNT(*) FROM auto_trader_templates").fetchone()[0]
    if existing_templates == 0:
        now = datetime.now(timezone.utc).isoformat()
        for t in SEED_TEMPLATES:
            conn.execute(
                "INSERT INTO auto_trader_templates (id, name, category, description, prompt, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (t["id"], t["name"], t["category"], t["description"], t["prompt"], now, now),
            )
        conn.commit()
    else:
        # Migration: refresh seed templates whose prompt still matches the prior
        # canonical version (md5 hashes below). User-edited templates are left
        # alone — we only touch rows that haven't been customized.
        import hashlib
        OLD_SEED_HASHES = {
            "default": "04016bc22637c32a25a9084c393cfedd",
            "energy_specialist": "fc3771ade95f20df52b508035bca468c",
            "tech_momentum": "501fb41993daeb6f2032c557de7a2fa7",
        }
        now = datetime.now(timezone.utc).isoformat()
        for t in SEED_TEMPLATES:
            old_hash = OLD_SEED_HASHES.get(t["id"])
            if not old_hash:
                continue
            row = conn.execute(
                "SELECT prompt FROM auto_trader_templates WHERE id = ?", (t["id"],)
            ).fetchone()
            if row and hashlib.md5(row[0].encode()).hexdigest() == old_hash:
                conn.execute(
                    "UPDATE auto_trader_templates SET prompt = ?, updated_at = ? WHERE id = ?",
                    (t["prompt"], now, t["id"]),
                )
        conn.commit()
    conn.close()


_ensure_tables()


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------

class CreateAgentRequest(BaseModel):
    name: str = Field(description="Agent name, e.g. 'Energy Specialist', 'Conservative Alpha'")
    prompt: str | None = Field(default=None, description="Agent prompt. If omitted, uses the default prompt.")
    allowed_tools: list[str] | None = Field(
        default=None,
        description="Subset of MCP tool names the agent may call. "
                    "Omit to default to the full current catalog; [] = no MCP tools.",
    )


class UpdateAgentRequest(BaseModel):
    name: str | None = Field(default=None, description="Update agent name")
    prompt: str | None = Field(default=None, description="Update agent prompt")
    allowed_tools: list[str] | None = Field(
        default=None,
        description="Replace the agent's tool allowlist. Omit to leave unchanged. "
                    "Must be a list (possibly empty); null is rejected.",
    )


class CreateRunRequest(BaseModel):
    name: str = Field(description="Human-readable run name")
    agent_id: str = Field(default="default", description="Agent ID to use for this run")
    metric: str = Field(description="Metric to optimize: sharpe_ratio, alpha_ann_pct, annualized_volatility_pct, max_drawdown_pct")
    conditions: list[str] = Field(default_factory=list, description="Optional conditions, e.g. ['alpha_ann_pct > 0', 'annualized_volatility_pct < 20']")
    start: str = Field(description="Training period start (YYYY-MM-DD)")
    end: str = Field(description="Training period end (YYYY-MM-DD)")
    capital: float = Field(default=1_000_000, description="Initial capital")
    model: str = Field(default="sonnet", description="Claude model: sonnet, opus, haiku")
    max_experiments: int = Field(default=1000, ge=1, le=10000, description="Safety cap. Default 1000.")
    sector: str | None = Field(default=None, description="Restrict to a sector (e.g. 'Energy', 'Technology'). If set, data queries only return stocks in this sector.")
    alpha_benchmark: str = Field(default="auto", description="Benchmark for alpha: 'sector' (sector ETF), 'market' (SPY), 'auto' (sector if sector is set, else market)")
    starting_portfolio: dict | None = Field(default=None, description="Optional starting portfolio config.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_agent(agent_id: str) -> dict:
    conn = get_db()
    row = conn.execute("SELECT * FROM auto_trader_agents WHERE id = ?", (agent_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, f"Agent '{agent_id}' not found")
    d = dict(row)
    # allowed_tools is always a list. Legacy NULL rows have been backfilled.
    d["allowed_tools"] = json.loads(d["allowed_tools"]) if d.get("allowed_tools") else []
    return d


def _validate_allowed_tools(names: list[str]) -> str:
    """Validate tool names against the current catalog and return the JSON-serialized
    list to store. Rejects None — every agent must have an explicit (possibly empty) list
    so capability changes are always tied to an explicit human action."""
    if names is None:
        raise HTTPException(400,
            "allowed_tools must be a list (possibly empty), not null. "
            "Use [] to disable all MCP tools.")
    from auto_trader.tools import TOOL_NAMES
    unknown = [n for n in names if n not in TOOL_NAMES]
    if unknown:
        raise HTTPException(400, f"Unknown tool name(s): {unknown}. Valid: {sorted(TOOL_NAMES)}")
    # De-dup while preserving order
    seen: set[str] = set()
    deduped = [n for n in names if not (n in seen or seen.add(n))]
    return json.dumps(deduped)


def _get_run(run_id: str) -> dict:
    conn = get_db()
    row = conn.execute("SELECT * FROM auto_trader_runs WHERE id = ?", (run_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, f"Run '{run_id}' not found")
    result = dict(row)
    if result.get("config"):
        result["config"] = json.loads(result["config"])
    return result


def _generate_run_id(name: str) -> str:
    import hashlib
    raw = f"{name}:{datetime.now(timezone.utc).isoformat()}"
    slug = name.lower().replace(" ", "_")[:20]
    suffix = hashlib.md5(raw.encode()).hexdigest()[:8]
    return f"{slug}_{suffix}"


def _stop_file(run_id: str) -> Path:
    return PROJECT_ROOT / "auto_trader" / f".stop_{run_id}"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/tools")
async def list_tools():
    """Return the catalog of MCP tools available for per-agent configuration.

    Built-in primitives (Skill, Read) are always on and intentionally excluded.
    """
    from auto_trader.tools import list_available_tools
    tools = list_available_tools()
    return {"total": len(tools), "data": tools}


@router.get("/config")
async def get_config():
    """Returns available models, metrics, and defaults for creating a run."""
    return {
        "models": AVAILABLE_MODELS,
        "metrics": [
            {"id": m, "name": m.replace("_", " ").title(),
             "direction": "maximize" if METRIC_DIRECTION[m] else "minimize",
             "description": {
                 "sharpe_ratio": "Higher = better risk-adjusted returns",
                 "alpha_ann_pct": "Higher = more excess return vs benchmark",
                 "annualized_volatility_pct": "Lower = less portfolio risk",
                 "max_drawdown_pct": "Less negative = smaller worst-case loss",
             }.get(m, "")}
            for m in VALID_METRICS
        ],
        "sectors": [
            "Technology", "Healthcare", "Financial Services", "Energy",
            "Consumer Cyclical", "Consumer Defensive", "Industrials",
            "Basic Materials", "Real Estate", "Communication Services", "Utilities",
        ],
        "alpha_benchmarks": [
            {"id": "auto", "description": "Sector ETF if sector is set, SPY otherwise"},
            {"id": "sector", "description": "Sector ETF (e.g. XLE for Energy)"},
            {"id": "market", "description": "S&P 500 (SPY)"},
        ],
        "defaults": {
            "metric": "sharpe_ratio",
            "model": "sonnet",
            "capital": 1_000_000,
            "max_experiments": 1000,
            "start": "2015-01-01",
            "end": "2024-12-31",
            "sector": None,
            "alpha_benchmark": "auto",
        },
    }


# ---------------------------------------------------------------------------
# Agent CRUD
# ---------------------------------------------------------------------------

@router.post("/agents", status_code=201)
async def create_agent(body: CreateAgentRequest):
    """Create a new auto-trader agent with a custom prompt.

    `allowed_tools` defaults to the full current catalog when omitted, so a brand-new
    agent is functional with no extra config. Pass [] to start with no MCP tools.
    """
    from auto_trader.tools import ALL_TOOLS

    agent_id = _generate_run_id(body.name)  # reuse the slug+hash generator
    now = datetime.now(timezone.utc).isoformat()
    prompt = body.prompt or DEFAULT_PROMPT

    tools = body.allowed_tools if body.allowed_tools is not None else [t.name for t in ALL_TOOLS]
    allowed_tools_json = _validate_allowed_tools(tools)

    conn = get_db()
    conn.execute(
        "INSERT INTO auto_trader_agents (id, name, prompt, allowed_tools, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (agent_id, body.name, prompt, allowed_tools_json, now, now),
    )
    conn.commit()
    conn.close()

    return {
        "id": agent_id,
        "name": body.name,
        "prompt_length": len(prompt),
        "allowed_tools": tools,
    }


@router.get("/agents")
async def list_agents():
    """List all auto-trader agents."""
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, allowed_tools, created_at, updated_at FROM auto_trader_agents ORDER BY created_at"
    ).fetchall()

    results = []
    for r in rows:
        d = dict(r)
        d["allowed_tools"] = json.loads(d["allowed_tools"]) if d.get("allowed_tools") else []
        d["run_count"] = conn.execute(
            "SELECT COUNT(*) FROM auto_trader_runs WHERE agent_id = ?", (r["id"],)
        ).fetchone()[0]
        results.append(d)
    conn.close()

    return {"total": len(results), "data": results}


@router.get("/agents/{agent_id}")
async def get_agent(agent_id: str):
    """Get agent detail including prompt."""
    agent = _get_agent(agent_id)

    conn = get_db()
    agent["run_count"] = conn.execute(
        "SELECT COUNT(*) FROM auto_trader_runs WHERE agent_id = ?", (agent_id,)
    ).fetchone()[0]
    conn.close()

    return agent


@router.get("/agents/{agent_id}/system-prompt")
async def get_agent_system_prompt(agent_id: str):
    """Return the full assembled system prompt that this agent's runs see.

    Mirrors what runner.load_program() builds: system.md + agent prompt + schemas.
    Useful for UI transparency — shows the complete context the model receives.
    """
    agent = _get_agent(agent_id)
    from auto_trader.runner import load_program
    system_prompt = load_program(agent_prompt=agent["prompt"])
    return {
        "agent_id": agent_id,
        "agent_name": agent["name"],
        "system_prompt": system_prompt,
        "length": len(system_prompt),
    }


@router.put("/agents/{agent_id}")
async def update_agent(agent_id: str, body: UpdateAgentRequest):
    """Update an agent's name, prompt, or tool allowlist.

    `allowed_tools` semantics:
      - field omitted from request: no change
      - []: disable all MCP tools
      - list of names: explicit subset (validated against catalog)
      - null: rejected (400) — capability changes must be explicit
    """
    _get_agent(agent_id)  # verify exists

    now = datetime.now(timezone.utc).isoformat()
    fields_set = body.model_fields_set
    conn = get_db()
    if body.name is not None:
        conn.execute("UPDATE auto_trader_agents SET name = ?, updated_at = ? WHERE id = ?",
                     (body.name, now, agent_id))
    if body.prompt is not None:
        conn.execute("UPDATE auto_trader_agents SET prompt = ?, updated_at = ? WHERE id = ?",
                     (body.prompt, now, agent_id))
    if "allowed_tools" in fields_set:
        stored = _validate_allowed_tools(body.allowed_tools)
        conn.execute("UPDATE auto_trader_agents SET allowed_tools = ?, updated_at = ? WHERE id = ?",
                     (stored, now, agent_id))
    conn.commit()
    conn.close()

    return {"id": agent_id, "status": "updated"}


@router.delete("/agents/{agent_id}")
async def delete_agent(agent_id: str):
    """Delete an agent. Cannot delete if it has active runs."""
    if agent_id == "default":
        raise HTTPException(409, "Cannot delete the default agent")

    _get_agent(agent_id)

    conn = get_db()
    run_count = conn.execute(
        "SELECT COUNT(*) FROM auto_trader_runs WHERE agent_id = ?",
        (agent_id,),
    ).fetchone()[0]
    if run_count > 0:
        conn.close()
        raise HTTPException(409, f"Agent has {run_count} run(s). Delete them first.")

    conn.execute("DELETE FROM auto_trader_agents WHERE id = ?", (agent_id,))
    conn.commit()
    conn.close()

    return {"id": agent_id, "status": "deleted"}


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

@router.get("/templates")
async def list_templates(
    category: Optional[str] = Query(None, description="Filter by category"),
):
    """List all agent prompt templates."""
    conn = get_db()
    if category:
        rows = conn.execute(
            "SELECT id, name, category, description, created_at FROM auto_trader_templates WHERE category = ? ORDER BY name",
            (category,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, name, category, description, created_at FROM auto_trader_templates ORDER BY category, name"
        ).fetchall()
    conn.close()
    return {"total": len(rows), "data": [dict(r) for r in rows]}


@router.get("/templates/{template_id}")
async def get_template(template_id: str):
    """Get a template with its full prompt."""
    conn = get_db()
    row = conn.execute("SELECT * FROM auto_trader_templates WHERE id = ?", (template_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, f"Template '{template_id}' not found")
    return dict(row)


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------

@router.post("/runs", status_code=201)
async def create_run(body: CreateRunRequest):
    """Create an auto-trader run. Returns the run config with a pending status."""
    if body.metric not in VALID_METRICS:
        raise HTTPException(400, f"Invalid metric: '{body.metric}'. Valid: {VALID_METRICS}")

    valid_model_ids = [m["id"] for m in AVAILABLE_MODELS]
    if body.model not in valid_model_ids:
        raise HTTPException(400, f"Invalid model: '{body.model}'. Valid: {valid_model_ids}")

    # Verify agent exists
    _get_agent(body.agent_id)

    run_id = _generate_run_id(body.name)
    now = datetime.now(timezone.utc).isoformat()

    # Validate sector
    valid_sectors = ["Technology", "Healthcare", "Financial Services", "Energy",
                     "Consumer Cyclical", "Consumer Defensive", "Industrials",
                     "Basic Materials", "Real Estate", "Communication Services", "Utilities"]
    if body.sector and body.sector not in valid_sectors:
        raise HTTPException(400, f"Invalid sector: '{body.sector}'. Valid: {valid_sectors}")

    # Resolve alpha benchmark
    alpha_benchmark = body.alpha_benchmark
    if alpha_benchmark == "auto":
        alpha_benchmark = "sector" if body.sector else "market"

    config = {
        "metric": body.metric,
        "conditions": body.conditions,
        "start": body.start,
        "end": body.end,
        "capital": body.capital,
        "model": body.model,
        "max_experiments": body.max_experiments,
        "sector": body.sector,
        "alpha_benchmark": alpha_benchmark,
    }
    if body.starting_portfolio:
        config["starting_portfolio"] = body.starting_portfolio

    conn = get_db()
    conn.execute(
        """INSERT INTO auto_trader_runs
           (id, name, agent_id, status, config, max_experiments, created_at, updated_at)
           VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)""",
        (run_id, body.name, body.agent_id, json.dumps(config),
         body.max_experiments, now, now),
    )
    conn.commit()
    conn.close()

    return {"id": run_id, "name": body.name, "agent_id": body.agent_id, "status": "pending", "config": config}


class StartRunRequest(BaseModel):
    additional_experiments: int | None = Field(default=None, ge=1, le=10000,
        description="Only required when resuming a completed run. Sets how many more experiments to run.")


@router.post("/runs/{run_id}/start")
async def start_run(run_id: str, body: StartRunRequest = StartRunRequest()):
    """Start or resume an auto-trader run.

    - pending/stopped: starts immediately, no body needed
    - completed: requires additional_experiments in body
    """
    run = _get_run(run_id)

    if run["status"] == "running":
        raise HTTPException(409, "Run is already running")

    if run["status"] == "completed":
        if not body.additional_experiments:
            raise HTTPException(400, "Run is completed. Provide additional_experiments to continue.")
        # Increase the max_experiments cap
        config = run["config"]
        config["max_experiments"] = config.get("max_experiments", 0) + body.additional_experiments
        conn = get_db()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE auto_trader_runs SET config = ?, max_experiments = ?, updated_at = ? WHERE id = ?",
            (json.dumps(config), config["max_experiments"], now, run_id),
        )
        conn.commit()
        conn.close()
        run["config"] = config

    config = run["config"]

    # Load prompt from the agent and write to temp file for the runner
    agent = _get_agent(run.get("agent_id", "default"))
    prompt_file = PROJECT_ROOT / "auto_trader" / f".prompt_{run_id}.md"
    prompt_file.write_text(agent["prompt"])

    # Remove any stale stop file
    _stop_file(run_id).unlink(missing_ok=True)

    # Build the command
    cmd = [
        sys.executable, "-u", str(PROJECT_ROOT / "auto_trader" / "runner.py"),
        "--run-id", run_id,
        "--max-experiments", str(config["max_experiments"]),
        "--metric", config["metric"],
        "--start", config["start"],
        "--end", config["end"],
        "--capital", str(config["capital"]),
        "--model", config["model"],
        "--prompt-file", str(prompt_file),
    ]
    for cond in config.get("conditions", []):
        cmd.extend(["--condition", cond])

    # Forward the agent's explicit MCP tool allowlist (always a list post-backfill).
    cmd.extend(["--allowed-tools", json.dumps(agent.get("allowed_tools", []))])

    if config.get("sector"):
        cmd.extend(["--sector", config["sector"]])
    if config.get("alpha_benchmark"):
        cmd.extend(["--alpha-benchmark", config["alpha_benchmark"]])

    if config.get("starting_portfolio"):
        sp_file = PROJECT_ROOT / "auto_trader" / f".starting_{run_id}.json"
        sp_file.write_text(json.dumps(config["starting_portfolio"]))
        cmd.extend(["--starting-portfolio", str(sp_file)])

    # Spawn as background process
    env = os.environ.copy()
    log_file = PROJECT_ROOT / "logs" / f"auto_trader_{run_id}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    with open(log_file, "a") as log_f:
        proc = subprocess.Popen(
            cmd,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=str(PROJECT_ROOT),
        )

    # Update DB
    now = datetime.now(timezone.utc).isoformat()
    conn = get_db()
    conn.execute(
        "UPDATE auto_trader_runs SET status = 'running', pid = ?, started_at = ?, updated_at = ? WHERE id = ?",
        (proc.pid, now, now, run_id),
    )
    conn.commit()
    conn.close()

    return {"id": run_id, "status": "running", "pid": proc.pid}


@router.post("/runs/{run_id}/stop")
async def stop_run(run_id: str):
    """Stop a running auto-trader run immediately."""
    run = _get_run(run_id)

    if run["status"] not in ("running",):
        raise HTTPException(409, f"Run is not running (status={run['status']})")

    # Kill the runner process
    pid = run.get("pid")
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass  # already dead

    # Update DB status
    now = datetime.now(timezone.utc).isoformat()
    conn = get_db()
    conn.execute(
        "UPDATE auto_trader_runs SET status = 'stopped', updated_at = ? WHERE id = ?",
        (now, run_id),
    )
    conn.commit()
    conn.close()

    # Clean up stop flag
    _stop_file(run_id).unlink(missing_ok=True)

    return {"id": run_id, "status": "stopped"}


@router.delete("/runs/{run_id}")
async def delete_run(run_id: str):
    """Delete a run and all its experiments. Cannot delete a running run."""
    run = _get_run(run_id)

    if run["status"] == "running":
        raise HTTPException(409, "Cannot delete a running run. Stop it first.")

    conn = get_db()
    # Cascade delete trades for all experiments in this run before removing the
    # experiment rows themselves. Single transaction via the implicit BEGIN.
    conn.execute(
        """DELETE FROM trades
           WHERE source_type = 'experiment'
             AND source_id IN (SELECT id FROM experiments WHERE run_id = ?)""",
        (run_id,),
    )
    conn.execute("DELETE FROM experiments WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM auto_trader_runs WHERE id = ?", (run_id,))
    conn.commit()
    conn.close()

    # Clean up temp files
    _stop_file(run_id).unlink(missing_ok=True)
    (PROJECT_ROOT / "auto_trader" / f".prompt_{run_id}.md").unlink(missing_ok=True)
    (PROJECT_ROOT / "auto_trader" / f".starting_{run_id}.json").unlink(missing_ok=True)

    return {"id": run_id, "status": "deleted"}


@router.get("/runs/{run_id}/stream")
async def stream_run(run_id: str):
    """Stream live events from a running auto-trader run via Server-Sent Events.

    The frontend opens this as an EventSource connection. Events are pushed
    as they happen: tool calls, agent thinking, thesis generated, backtest
    started, experiment completed.

    Event types:
      - experiment_started: {experiment_number}
      - tool_call: {tool, input, call_number}
      - agent_thinking: {text}
      - thesis_generated: {thesis, sleeves}
      - backtest_started: {experiment_number}
      - experiment_completed: {decision, metrics...}
      - agent_result: {chars}
    """
    from starlette.responses import StreamingResponse
    import asyncio

    _get_run(run_id)  # verify run exists

    async def event_generator():
        last_line = 0
        while True:
            events, last_line = tail_events(run_id, after_line=last_line)
            for event in events:
                yield f"data: {json.dumps(event, default=str)}\n\n"

            # Check if run is still active
            conn = get_db()
            row = conn.execute(
                "SELECT status FROM auto_trader_runs WHERE id = ?", (run_id,)
            ).fetchone()
            conn.close()

            if row and row["status"] in ("completed", "stopped"):
                yield f"data: {json.dumps({'type': 'stream_ended', 'status': row['status']})}\n\n"
                break

            await asyncio.sleep(2)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/runs/{run_id}/events")
async def get_run_events(
    run_id: str,
    limit: int = Query(200, ge=1, le=2000),
    since_event_id: int | None = Query(
        None, ge=0,
        description="Return events strictly after this id (use next_cursor from a prior response).",
    ),
    types: str | None = Query(
        None,
        description="Comma-separated event types to include (e.g. experiment_started,backtest_started). Unknown types are ignored.",
    ),
):
    """Get historical events for a run, oldest-first. Use /stream for live tail.

    Events are read from the run's append-only JSONL log. Each event gets an
    `event_id` equal to its line number in that log — stable across calls and
    suitable for use as a cursor via `since_event_id`.

    Filtering: `since_event_id` filters by cursor; `types` narrows to a
    high-signal subset. `total` is the count matching those filters (ignoring
    `limit`), so callers can render "Showing N of T".
    """
    _get_run(run_id)  # verify run exists (404 if missing)

    type_allow: set[str] | None = None
    if types:
        type_allow = {t.strip() for t in types.split(",") if t.strip()}

    raw, _ = tail_events(run_id)
    # Tag each event with its 1-based line-number id, then apply filters.
    tagged = [{**ev, "event_id": i + 1} for i, ev in enumerate(raw)]
    filtered = tagged
    if since_event_id is not None:
        filtered = [e for e in filtered if e["event_id"] > since_event_id]
    if type_allow is not None:
        filtered = [e for e in filtered if e.get("type") in type_allow]

    total = len(filtered)
    page = filtered[:limit]
    next_cursor = page[-1]["event_id"] if page else None
    return {
        "run_id": run_id,
        "total": total,
        "count": len(page),
        "next_cursor": next_cursor,
        "events": page,
    }


@router.get("/runs")
async def list_runs(
    status: Optional[str] = Query(None, description="Filter by status: pending, running, stopped, completed"),
):
    """List all auto-trader runs."""
    conn = get_db()
    if status:
        rows = conn.execute(
            "SELECT * FROM auto_trader_runs WHERE status = ? ORDER BY created_at DESC", (status,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM auto_trader_runs ORDER BY created_at DESC"
        ).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        d["config"] = json.loads(d["config"]) if d.get("config") else {}
        d.pop("prompt", None)  # Don't include full prompt in list view
        d.pop("pid", None)
        results.append(d)

    return {"total": len(results), "data": results}


@router.get("/runs/{run_id}")
async def get_run(run_id: str):
    """Get run details including progress and best result."""
    run = _get_run(run_id)

    # Check if process is still alive
    if run["status"] == "running" and run.get("pid"):
        try:
            os.kill(run["pid"], 0)
        except (ProcessLookupError, PermissionError):
            # Process died — update status
            conn = get_db()
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE auto_trader_runs SET status = 'stopped', updated_at = ? WHERE id = ?",
                (now, run_id),
            )
            conn.commit()
            conn.close()
            run["status"] = "stopped"

    # Get experiment summary
    summary = get_run_summary(run_id)
    direction = METRIC_DIRECTION.get(run["config"].get("metric"), True)
    best = get_best_experiment(run_id, higher_is_better=direction)

    # Include agent info
    agent_id = run.get("agent_id", "default")
    try:
        agent = _get_agent(agent_id)
        run["agent"] = {"id": agent_id, "name": agent["name"]}
    except HTTPException:
        run["agent"] = {"id": agent_id, "name": "Unknown"}

    run["experiments_summary"] = summary
    if best:
        run["best_experiment"] = {
            "id": best["id"],
            "experiment_number": best["iteration"],
            "thesis": best.get("thesis"),
            "target_value": best.get("target_value"),
            "sharpe_ratio": best.get("sharpe_ratio"),
            "sharpe_basis": best.get("sharpe_basis"),
            "sharpe_ratio_annualized": best.get("sharpe_ratio_annualized"),
            "sharpe_ratio_period": best.get("sharpe_ratio_period"),
            "alpha_ann_pct": best.get("alpha_ann_pct"),
            "alpha_vs_market_pct": best.get("alpha_vs_market_pct"),
            "alpha_vs_sector_pct": best.get("alpha_vs_sector_pct"),
            "market_benchmark_return_pct": best.get("market_benchmark_return_pct"),
            "market_benchmark_ann_return_pct": best.get("market_benchmark_ann_return_pct"),
            "sector_benchmark_return_pct": best.get("sector_benchmark_return_pct"),
            "sector_benchmark_ann_return_pct": best.get("sector_benchmark_ann_return_pct"),
            "annualized_volatility_pct": best.get("annualized_volatility_pct"),
            "total_return_pct": best.get("total_return_pct"),
            "max_drawdown_pct": best.get("max_drawdown_pct"),
        }

    run.pop("pid", None)
    return run


@router.get("/runs/{run_id}/experiments")
async def list_experiments(
    run_id: str,
    limit: int = Query(10000, ge=1, le=10000),
    since_iteration: int | None = Query(
        None, ge=0,
        description="Only return experiments with iteration > this value. Use for incremental polling.",
    ),
):
    """List all experiments for a run with metrics.

    Default returns every experiment in the run (cap 10000, matching max_experiments).
    Pass `since_iteration=N` to fetch only experiments newer than the last one you've seen —
    use this for incremental polling so the frontend doesn't refetch the full list every tick.
    """
    _get_run(run_id)  # verify run exists

    conn = get_db()
    total = conn.execute(
        "SELECT COUNT(*) FROM experiments WHERE run_id = ?", (run_id,)
    ).fetchone()[0]

    params: list = [run_id]
    where = "WHERE run_id = ?"
    if since_iteration is not None:
        where += " AND iteration > ?"
        params.append(since_iteration)
    params.append(limit)

    rows = conn.execute(
        f"""SELECT id, iteration, thesis, portfolio_id,
                   target_metric, target_value, conditions_met,
                   total_return_pct, annualized_return_pct,
                   sharpe_ratio, sharpe_basis, sharpe_ratio_annualized, sharpe_ratio_period,
                   sortino_ratio,
                   max_drawdown_pct, annualized_volatility_pct, alpha_ann_pct,
                   alpha_vs_market_pct, alpha_vs_sector_pct,
                   market_benchmark_return_pct, market_benchmark_ann_return_pct,
                   sector_benchmark_return_pct, sector_benchmark_ann_return_pct,
                   profit_factor, win_rate_pct, total_trades,
                   decision, best_value_so_far, improvement_pct,
                   session_id, duration_seconds, created_at
            FROM experiments
            {where}
            ORDER BY iteration
            LIMIT ?""",
        params,
    ).fetchall()
    conn.close()

    data = []
    for r in rows:
        d = dict(r)
        d["experiment_number"] = d.pop("iteration")
        data.append(d)

    return {
        "run_id": run_id,
        "total": total,
        "count": len(data),
        "data": data,
    }


@router.get("/runs/{run_id}/experiments/{experiment_id}")
async def get_experiment(run_id: str, experiment_id: str):
    """Get full experiment detail including thesis, portfolio config, and metrics."""
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM experiments WHERE id = ? AND run_id = ?",
        (experiment_id, run_id),
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, f"Experiment '{experiment_id}' not found in run '{run_id}'")

    result = dict(row)
    result["experiment_number"] = result.pop("iteration")
    for field in ("assumptions", "portfolio_config", "conditions"):
        if result.get(field) and isinstance(result[field], str):
            try:
                result[field] = json.loads(result[field])
            except (json.JSONDecodeError, TypeError):
                pass

    return result


@router.get("/runs/{run_id}/experiments/{experiment_id}/trades")
async def get_experiment_trades(run_id: str, experiment_id: str):
    """Get all trades executed during this experiment's backtest.

    Returns every BUY and SELL across every sleeve of the portfolio, tagged
    with sleeve_label. SELL rows carry round-trip fields (pnl, pnl_pct,
    entry_date, days_held, reason); BUY rows have those fields as null.

    Each trade is enriched with a `snapshot` of the stock's features_daily
    values on the trade date — 9 valuation/growth metrics computed
    point-in-time. This lets the trader see both what the strategy rule
    observed and the broader picture of the stock on that date.
    """
    conn = get_db()
    # Verify the experiment belongs to this run (404 if not found)
    exp_row = conn.execute(
        "SELECT id FROM experiments WHERE id = ? AND run_id = ?",
        (experiment_id, run_id),
    ).fetchone()
    if not exp_row:
        conn.close()
        raise HTTPException(404, f"Experiment '{experiment_id}' not found in run '{run_id}'")

    rows = conn.execute(
        """SELECT id, sleeve_label, date, action, symbol, shares, price, amount,
                  reason, signal_detail, entry_date, entry_price, pnl, pnl_pct,
                  days_held, linked_trade_id
           FROM trades
           WHERE source_type = 'experiment' AND source_id = ?
           ORDER BY date, action, symbol""",
        (experiment_id,),
    ).fetchall()
    conn.close()

    trades = []
    for r in rows:
        d = dict(r)
        if d.get("signal_detail") and isinstance(d["signal_detail"], str):
            try:
                d["signal_detail"] = json.loads(d["signal_detail"])
            except (json.JSONDecodeError, TypeError):
                pass
        trades.append(d)

    # Enrich every trade with the per-(symbol,date) feature snapshot.
    # One bulk query against features_daily keyed on the distinct (symbol, date)
    # pairs — cheap because features_daily is indexed on (symbol, date).
    if trades:
        pairs = sorted({(t["symbol"], t["date"]) for t in trades})
        import sqlite3 as _sqlite3
        import os as _os
        market_db = _os.environ.get(
            "MARKET_DB_PATH",
            str(Path(__file__).parent.parent / "data" / "market.db"),
        )
        snapshots: dict[tuple, dict] = {}
        try:
            mconn = _sqlite3.connect(market_db)
            # Chunk to stay under SQLite's default 999-variable limit.
            CHUNK = 450
            cur = mconn.cursor()
            for i in range(0, len(pairs), CHUNK):
                chunk = pairs[i : i + CHUNK]
                placeholders = ",".join(["(?,?)"] * len(chunk))
                args = [v for pair in chunk for v in pair]
                for row in cur.execute(
                    f"SELECT symbol, date, pe, ps, p_b, ev_ebitda, ev_sales, "
                    f"fcf_yield, div_yield, eps_yoy, rev_yoy "
                    f"FROM features_daily WHERE (symbol, date) IN ({placeholders})",
                    args,
                ).fetchall():
                    snapshots[(row[0], row[1])] = {
                        "pe": row[2], "ps": row[3], "p_b": row[4],
                        "ev_ebitda": row[5], "ev_sales": row[6],
                        "fcf_yield": row[7], "div_yield": row[8],
                        "eps_yoy": row[9], "rev_yoy": row[10],
                    }
            mconn.close()
        except Exception:
            # Snapshot enrichment is best-effort; never block the trades list.
            snapshots = {}

        for t in trades:
            t["snapshot"] = snapshots.get((t["symbol"], t["date"]))

    return {
        "experiment_id": experiment_id,
        "run_id": run_id,
        "trade_count": len(trades),
        "trades": trades,
    }


@router.get("/runs/{run_id}/experiments/{experiment_id}/positions")
async def get_experiment_positions(run_id: str, experiment_id: str):
    """Per-ticker position book for an experiment's backtest.

    Reconstructs positions from the trade ledger, priced as of the
    experiment's backtest_end. Response shape matches
    /deployments/{id}/positions so the same frontend components render both.
    """
    from portfolio_book import reconstruct_positions, make_price_lookup

    conn = get_db()
    exp = conn.execute(
        "SELECT id, backtest_end, initial_capital FROM experiments "
        "WHERE id = ? AND run_id = ?",
        (experiment_id, run_id),
    ).fetchone()
    if not exp:
        conn.close()
        raise HTTPException(404, f"Experiment '{experiment_id}' not found in run '{run_id}'")

    backtest_end = exp["backtest_end"]
    initial_capital = exp["initial_capital"] or 0

    rows = conn.execute(
        """SELECT sleeve_label, date, action, symbol, shares, price, amount,
                  entry_price, pnl
           FROM trades
           WHERE source_type = 'experiment' AND source_id = ?""",
        (experiment_id,),
    ).fetchall()
    conn.close()

    trades = [dict(r) for r in rows]

    market_db = os.environ.get(
        "MARKET_DB_PATH",
        str(PROJECT_ROOT / "data" / "market.db"),
    )
    price_lookup = make_price_lookup(market_db)

    book = reconstruct_positions(trades, initial_capital, backtest_end, price_lookup)

    return {
        "experiment_id": experiment_id,
        "run_id": run_id,
        "as_of_date": backtest_end,
        "initial_capital": initial_capital,
        **book,
    }


@router.get("/runs/{run_id}/experiments/{experiment_id}/session")
async def get_experiment_session(run_id: str, experiment_id: str):
    """Get the full agent session trail for an experiment."""
    conn = get_db()
    row = conn.execute(
        "SELECT session_id FROM experiments WHERE id = ? AND run_id = ?",
        (experiment_id, run_id),
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, f"Experiment '{experiment_id}' not found")

    session_id = row["session_id"]
    if not session_id:
        raise HTTPException(404, "No session recorded for this experiment")

    try:
        from claude_agent_sdk import get_session_messages
        # Try with the auto_trader cwd first, then without directory
        msgs = get_session_messages(
            session_id=session_id,
            directory=str(PROJECT_ROOT / "auto_trader"),
        )
        if not msgs:
            # Fallback: try without directory (searches default locations)
            msgs = get_session_messages(session_id=session_id)

        # Convert to serializable format
        trail = []
        for m in msgs:
            msg = m.message if hasattr(m, "message") else {}
            if isinstance(msg, dict):
                entry = {
                    "type": m.type,
                    "role": msg.get("role", m.type),
                    "content": msg.get("content", ""),
                }
            else:
                entry = {"type": m.type, "content": str(msg)[:500]}
            trail.append(entry)

        return {
            "run_id": run_id,
            "experiment_id": experiment_id,
            "session_id": session_id,
            "total_messages": len(trail),
            "messages": trail,
        }

    except Exception as e:
        raise HTTPException(500, f"Failed to read session: {e}")


@router.get("/runs/{run_id}/prompt")
async def get_run_prompt(run_id: str):
    """Get the prompt for this run (from its agent)."""
    run = _get_run(run_id)
    agent = _get_agent(run.get("agent_id", "default"))
    return {"run_id": run_id, "agent_id": agent["id"], "agent_name": agent["name"], "prompt": agent["prompt"]}
