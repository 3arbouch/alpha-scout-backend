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
    {"id": "opus", "name": "Claude Opus 4.6", "api_id": "claude-opus-4-6", "speed": "slow", "cost": "$5/$25 per MTok", "description": "Most intelligent. ~10-20 min per experiment. Deepest research."},
]

PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_PROMPT = (Path(__file__).parent / "program.md").read_text()

# ---------------------------------------------------------------------------
# Runs table schema
# ---------------------------------------------------------------------------

RUNS_SCHEMA = """
CREATE TABLE IF NOT EXISTS auto_trader_runs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    config TEXT NOT NULL,
    prompt TEXT NOT NULL,
    current_iteration INTEGER DEFAULT 0,
    max_experiments INTEGER NOT NULL,
    best_metric_value REAL,
    best_experiment_id TEXT,
    pid INTEGER,
    error TEXT,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_atr_status ON auto_trader_runs(status);
"""


def _ensure_runs_table():
    conn = get_db()
    conn.executescript(RUNS_SCHEMA)
    conn.close()


_ensure_runs_table()


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------

class CreateRunRequest(BaseModel):
    name: str = Field(description="Human-readable run name")
    metric: str = Field(description="Metric to optimize: sharpe_ratio, alpha_ann_pct, annualized_volatility_pct, max_drawdown_pct")
    conditions: list[str] = Field(default_factory=list, description="Optional conditions, e.g. ['alpha_ann_pct > 0', 'annualized_volatility_pct < 20']")
    start: str = Field(description="Training period start (YYYY-MM-DD)")
    end: str = Field(description="Training period end (YYYY-MM-DD)")
    capital: float = Field(default=1_000_000, description="Initial capital")
    model: str = Field(default="sonnet", description="Claude model: sonnet, opus, haiku")
    max_experiments: int = Field(default=1000, ge=1, le=10000, description="Safety cap. Run stops automatically when reached. Default 1000.")
    starting_portfolio: dict | None = Field(default=None, description="Optional starting portfolio config. Backtested as iteration 0.")


class UpdatePromptRequest(BaseModel):
    prompt: str = Field(description="The user-editable agent prompt")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
        "defaults": {
            "metric": "sharpe_ratio",
            "model": "sonnet",
            "capital": 1_000_000,
            "max_experiments": 1000,
            "start": "2015-01-01",
            "end": "2024-12-31",
        },
    }


@router.post("/runs", status_code=201)
async def create_run(body: CreateRunRequest):
    """Create an auto-trader run. Returns the run config with a pending status."""
    if body.metric not in VALID_METRICS:
        raise HTTPException(400, f"Invalid metric: '{body.metric}'. Valid: {VALID_METRICS}")

    valid_model_ids = [m["id"] for m in AVAILABLE_MODELS]
    if body.model not in valid_model_ids:
        raise HTTPException(400, f"Invalid model: '{body.model}'. Valid: {valid_model_ids}")

    run_id = _generate_run_id(body.name)
    now = datetime.now(timezone.utc).isoformat()

    config = {
        "metric": body.metric,
        "conditions": body.conditions,
        "start": body.start,
        "end": body.end,
        "capital": body.capital,
        "model": body.model,
        "max_experiments": body.max_experiments,
    }
    if body.starting_portfolio:
        config["starting_portfolio"] = body.starting_portfolio

    conn = get_db()
    conn.execute(
        """INSERT INTO auto_trader_runs
           (id, name, status, config, prompt, max_experiments, created_at, updated_at)
           VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)""",
        (run_id, body.name, json.dumps(config), DEFAULT_PROMPT,
         body.max_experiments, now, now),
    )
    conn.commit()
    conn.close()

    return {"id": run_id, "name": body.name, "status": "pending", "config": config}


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

    # Write the prompt to a temp file for the runner to read
    prompt_file = PROJECT_ROOT / "auto_trader" / f".prompt_{run_id}.md"
    prompt_file.write_text(run["prompt"])

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
async def get_run_events(run_id: str):
    """Get all events for a run (historical). Use /stream for live events."""
    _get_run(run_id)  # verify run exists
    events, _ = tail_events(run_id)
    return {"run_id": run_id, "total": len(events), "events": events}


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

    run["experiments_summary"] = summary
    if best:
        run["best_experiment"] = {
            "id": best["id"],
            "experiment_number": best["iteration"],
            "thesis": best.get("thesis"),
            "target_value": best.get("target_value"),
            "sharpe_ratio": best.get("sharpe_ratio"),
            "alpha_ann_pct": best.get("alpha_ann_pct"),
            "annualized_volatility_pct": best.get("annualized_volatility_pct"),
            "total_return_pct": best.get("total_return_pct"),
            "max_drawdown_pct": best.get("max_drawdown_pct"),
        }

    run.pop("pid", None)
    return run


@router.get("/runs/{run_id}/experiments")
async def list_experiments(
    run_id: str,
    limit: int = Query(50, ge=1, le=200),
):
    """List all experiments for a run with metrics."""
    _get_run(run_id)  # verify run exists

    conn = get_db()
    rows = conn.execute(
        """SELECT id, iteration, thesis, target_metric, target_value, conditions_met,
                  total_return_pct, annualized_return_pct, sharpe_ratio, sortino_ratio,
                  max_drawdown_pct, annualized_volatility_pct, alpha_ann_pct,
                  profit_factor, win_rate_pct, total_trades,
                  decision, best_value_so_far, improvement_pct,
                  session_id, duration_seconds, created_at
           FROM experiments
           WHERE run_id = ?
           ORDER BY iteration
           LIMIT ?""",
        (run_id, limit),
    ).fetchall()
    conn.close()

    data = []
    for r in rows:
        d = dict(r)
        d["experiment_number"] = d.pop("iteration")
        data.append(d)

    return {
        "run_id": run_id,
        "total": len(data),
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
    """Get the user-editable agent prompt for a run."""
    run = _get_run(run_id)
    return {"run_id": run_id, "prompt": run["prompt"]}


@router.put("/runs/{run_id}/prompt")
async def update_run_prompt(run_id: str, body: UpdatePromptRequest):
    """Update the agent prompt for a run. Only works on pending runs."""
    run = _get_run(run_id)

    if run["status"] not in ("pending", "stopped", "completed"):
        raise HTTPException(409, f"Cannot edit prompt while run is {run['status']}")

    now = datetime.now(timezone.utc).isoformat()
    conn = get_db()
    conn.execute(
        "UPDATE auto_trader_runs SET prompt = ?, updated_at = ? WHERE id = ?",
        (body.prompt, now, run_id),
    )
    conn.commit()
    conn.close()

    return {"run_id": run_id, "status": "updated"}
