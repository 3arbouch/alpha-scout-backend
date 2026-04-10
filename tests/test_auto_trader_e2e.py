#!/usr/bin/env python3
"""
Auto-Trader end-to-end tests.

Tests the full lifecycle: config, create, start, poll, stop, experiments, sessions, prompts.
Does NOT run actual agent iterations (too slow) — tests the API layer and DB persistence.
For agent integration tests, use runner.py directly.

Run:
    cd /path/to/alpha-scout-backend-dev
    python3 tests/test_auto_trader_e2e.py
"""

import os
import sys
import json
import time
import sqlite3
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

API = os.environ.get("API_URL", "http://localhost:8091")
KEY = os.environ.get("ALPHASCOUT_API_KEY", "")

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


def api(method, path, body=None, expected=200):
    url = f"{API}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    if KEY:
        headers["X-API-Key"] = KEY
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        resp = urllib.request.urlopen(req)
        return resp.getcode(), json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode() if e.fp else ""
        try:
            return e.code, json.loads(body_text)
        except json.JSONDecodeError:
            return e.code, {"detail": body_text}


# =========================================================================
print("=" * 70)
print("AUTO-TRADER E2E TESTS")
print("=" * 70)

# =========================================================================
print("\nTEST 1: GET /auto-trader/config")
print("=" * 70)

code, data = api("GET", "/auto-trader/config")
check("Config returns 200", code == 200)
check("Has models", len(data.get("models", [])) >= 3)
check("Has metrics", len(data.get("metrics", [])) >= 3)
check("Has defaults", "defaults" in data)
check("Models have id, name, speed, cost", all(
    all(k in m for k in ("id", "name", "speed", "cost")) for m in data["models"]
))
check("Metrics have id, direction", all(
    all(k in m for k in ("id", "direction")) for m in data["metrics"]
))
check("Sharpe direction is maximize",
      any(m["id"] == "sharpe_ratio" and m["direction"] == "maximize" for m in data["metrics"]))
check("Volatility direction is minimize",
      any(m["id"] == "annualized_volatility_pct" and m["direction"] == "minimize" for m in data["metrics"]))
check("Drawdown direction is maximize (less negative = better)",
      any(m["id"] == "max_drawdown_pct" and m["direction"] == "maximize" for m in data["metrics"]))

# =========================================================================
print("\nTEST 2: Validation — invalid inputs")
print("=" * 70)

code, _ = api("POST", "/auto-trader/runs", {})
check("Empty body returns 422", code == 422)

code, _ = api("POST", "/auto-trader/runs", {
    "name": "test", "metric": "invalid_metric",
    "start": "2015-01-01", "end": "2020-01-01"
})
check("Invalid metric returns 400", code == 400)

code, _ = api("POST", "/auto-trader/runs", {
    "name": "test", "metric": "sharpe_ratio", "model": "gpt-4",
    "start": "2015-01-01", "end": "2020-01-01"
})
check("Invalid model returns 400", code == 400)

# =========================================================================
print("\nTEST 3: Create a run")
print("=" * 70)

code, run = api("POST", "/auto-trader/runs", {
    "name": "E2E Test Run",
    "metric": "sharpe_ratio",
    "conditions": ["alpha_ann_pct > 0", "annualized_volatility_pct < 20"],
    "start": "2015-01-01",
    "end": "2020-01-01",
    "capital": 500000,
    "model": "haiku",
    "max_experiments": 5,
})
check("Create returns 201", code == 201)
check("Has id", "id" in run)
check("Status is pending", run.get("status") == "pending")
check("Config has metric", run.get("config", {}).get("metric") == "sharpe_ratio")
check("Config has conditions", len(run.get("config", {}).get("conditions", [])) == 2)
check("Config has capital", run.get("config", {}).get("capital") == 500000)

RUN_ID = run.get("id", "")

# =========================================================================
print("\nTEST 4: Get run detail (pending)")
print("=" * 70)

code, detail = api("GET", f"/auto-trader/runs/{RUN_ID}")
check("Get run returns 200", code == 200)
check("Status is pending", detail.get("status") == "pending")
check("Has prompt", len(detail.get("prompt", "")) > 100)
check("Has config", "config" in detail)
check("Has experiments_summary", "experiments_summary" in detail)
check("Total experiments is 0", detail.get("experiments_summary", {}).get("total_experiments") == 0)

# =========================================================================
print("\nTEST 5: List runs")
print("=" * 70)

code, runs = api("GET", "/auto-trader/runs")
check("List returns 200", code == 200)
check("Has total", "total" in runs)
check("Has data array", isinstance(runs.get("data"), list))
check("Our run is in the list", any(r["id"] == RUN_ID for r in runs.get("data", [])))
check("List does not include prompt", all("prompt" not in r for r in runs.get("data", [])))

# Filter by status
code, filtered = api("GET", "/auto-trader/runs?status=pending")
check("Status filter works", all(r["status"] == "pending" for r in filtered.get("data", [])))

# =========================================================================
print("\nTEST 6: Get and update prompt")
print("=" * 70)

code, prompt_data = api("GET", f"/auto-trader/runs/{RUN_ID}/prompt")
check("Get prompt returns 200", code == 200)
check("Has prompt text", len(prompt_data.get("prompt", "")) > 100)
check("Has run_id", prompt_data.get("run_id") == RUN_ID)

original_prompt = prompt_data["prompt"]

code, update_result = api("PUT", f"/auto-trader/runs/{RUN_ID}/prompt", {
    "prompt": "# Custom Prompt\n\nFocus on energy sector only."
})
check("Update prompt returns 200", code == 200)
check("Update status is updated", update_result.get("status") == "updated")

code, updated = api("GET", f"/auto-trader/runs/{RUN_ID}/prompt")
check("Prompt was updated", "energy sector" in updated.get("prompt", ""))

# Restore original
api("PUT", f"/auto-trader/runs/{RUN_ID}/prompt", {"prompt": original_prompt})

# =========================================================================
print("\nTEST 7: Create run with starting portfolio")
print("=" * 70)

code, sp_run = api("POST", "/auto-trader/runs", {
    "name": "Starting Portfolio Test",
    "metric": "sharpe_ratio",
    "start": "2015-01-01",
    "end": "2020-01-01",
    "model": "haiku",
    "max_experiments": 1,
    "starting_portfolio": {
        "name": "Simple Tech",
        "sleeves": [{
            "label": "Tech",
            "weight": 1.0,
            "regime_gate": ["*"],
            "strategy_config": {
                "name": "Tech Buy Hold",
                "universe": {"type": "sector", "sector": "Technology"},
                "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                "sizing": {"type": "equal_weight", "max_positions": 10, "initial_allocation": 1000000},
                "backtest": {"start": "2015-01-01", "end": "2020-01-01", "entry_price": "next_close", "slippage_bps": 10}
            }
        }],
        "regime_filter": False,
        "capital_when_gated_off": "to_cash"
    }
})
check("Create with starting portfolio returns 201", code == 201)
check("Config has starting_portfolio", "starting_portfolio" in sp_run.get("config", {}))

SP_RUN_ID = sp_run.get("id", "")

# =========================================================================
print("\nTEST 8: Start and stop lifecycle")
print("=" * 70)

# Can't stop a pending run
code, _ = api("POST", f"/auto-trader/runs/{RUN_ID}/stop")
check("Can't stop pending run (409)", code == 409)

# Start the run
code, start_result = api("POST", f"/auto-trader/runs/{RUN_ID}/start")
check("Start returns 200", code == 200)
check("Status is running", start_result.get("status") == "running")
check("Has pid", "pid" in start_result)

# Can't start again
code, _ = api("POST", f"/auto-trader/runs/{RUN_ID}/start")
check("Can't start running run (409)", code == 409)

# Check status
time.sleep(2)
code, detail = api("GET", f"/auto-trader/runs/{RUN_ID}")
check("Status is running", detail.get("status") == "running")

# Stop it
code, stop_result = api("POST", f"/auto-trader/runs/{RUN_ID}/stop")
check("Stop returns 200", code == 200)
check("Status is stopping", stop_result.get("status") == "stopping")

# Wait for process to exit
time.sleep(10)

# =========================================================================
print("\nTEST 9: Experiments list (empty or with data)")
print("=" * 70)

code, exps = api("GET", f"/auto-trader/runs/{RUN_ID}/experiments")
check("Experiments returns 200", code == 200)
check("Has run_id", exps.get("run_id") == RUN_ID)
check("Has total", "total" in exps)
check("Has data array", isinstance(exps.get("data"), list))

if exps["total"] > 0:
    exp = exps["data"][0]
    check("Experiment has id", "id" in exp)
    check("Experiment has experiment_number", "experiment_number" in exp)
    check("No iteration field", "iteration" not in exp)
    check("Has decision", exp.get("decision") in ("keep", "discard"))
    check("Has sharpe_ratio", "sharpe_ratio" in exp)
    check("Has session_id", "session_id" in exp)

    # Test experiment detail
    EXP_ID = exp["id"]
    code, detail = api("GET", f"/auto-trader/runs/{RUN_ID}/experiments/{EXP_ID}")
    check("Experiment detail returns 200", code == 200)
    check("Detail has thesis", "thesis" in detail)
    check("Detail has portfolio_config", "portfolio_config" in detail)
    check("Detail has experiment_number", "experiment_number" in detail)
    check("Detail has no iteration field", "iteration" not in detail)
    check("portfolio_config is dict", isinstance(detail.get("portfolio_config"), dict))
    check("assumptions is list", isinstance(detail.get("assumptions"), list))

    # Test session
    if exp.get("session_id"):
        code, session = api("GET", f"/auto-trader/runs/{RUN_ID}/experiments/{EXP_ID}/session")
        check("Session returns 200", code == 200)
        check("Session has experiment_id", session.get("experiment_id") == EXP_ID)
        check("Session has messages", isinstance(session.get("messages"), list))
        check("Session has total_messages", session.get("total_messages", 0) > 0)
else:
    print("  (no experiments yet — run was stopped before completing)")

# =========================================================================
print("\nTEST 10: 404 handling")
print("=" * 70)

code, _ = api("GET", "/auto-trader/runs/nonexistent")
check("Nonexistent run returns 404", code == 404)

code, _ = api("GET", f"/auto-trader/runs/{RUN_ID}/experiments/nonexistent")
check("Nonexistent experiment returns 404", code == 404)

code, _ = api("GET", f"/auto-trader/runs/{RUN_ID}/experiments/nonexistent/session")
check("Nonexistent session returns 404", code == 404)

# =========================================================================
print("\nTEST 11: Auth")
print("=" * 70)

# Test without API key
url = f"{API}/auto-trader/runs"
req = urllib.request.Request(url)
try:
    resp = urllib.request.urlopen(req)
    no_auth_code = resp.getcode()
except urllib.error.HTTPError as e:
    no_auth_code = e.code
check("No API key returns 401", no_auth_code == 401)

# =========================================================================
print("\nTEST 12: Prompt update restrictions")
print("=" * 70)

# Can't edit prompt on a running/completed run — use a completed one
completed_runs = [r for r in runs.get("data", []) if r["status"] in ("completed", "running")]
if completed_runs:
    cr_id = completed_runs[0]["id"]
    code, _ = api("PUT", f"/auto-trader/runs/{cr_id}/prompt", {"prompt": "test"})
    check(f"Can't edit prompt on {completed_runs[0]['status']} run (409)", code == 409)
else:
    print("  (no completed/running runs to test)")

# =========================================================================
print("\nTEST 13: DB persistence")
print("=" * 70)

from auto_trader.schema import get_db

conn = get_db()

# Check runs table
run_row = conn.execute("SELECT * FROM auto_trader_runs WHERE id = ?", (RUN_ID,)).fetchone()
check("Run exists in DB", run_row is not None)
if run_row:
    check("DB name matches", run_row["name"] == "E2E Test Run")
    check("DB config is valid JSON", json.loads(run_row["config"]) is not None)
    check("DB prompt is stored", len(run_row["prompt"]) > 100)
    check("DB max_experiments matches", run_row["max_experiments"] == 5)

# Check experiments table structure
cols = {r[1] for r in conn.execute("PRAGMA table_info(experiments)").fetchall()}
check("experiments has session_id column", "session_id" in cols)
check("experiments has iteration column (internal)", "iteration" in cols)

conn.close()

# =========================================================================
print("\nTEST 14: Metric direction logic")
print("=" * 70)

from auto_trader.runner import METRIC_DIRECTION, VALID_METRICS, is_improvement

check("sharpe_ratio is maximize", METRIC_DIRECTION["sharpe_ratio"] == True)
check("alpha_ann_pct is maximize", METRIC_DIRECTION["alpha_ann_pct"] == True)
check("volatility is minimize", METRIC_DIRECTION["annualized_volatility_pct"] == False)
check("drawdown is maximize (less negative = better)", METRIC_DIRECTION["max_drawdown_pct"] == True)

check("Sharpe 1.5 > 1.0 is improvement", is_improvement("sharpe_ratio", 1.5, 1.0))
check("Sharpe 0.5 > 1.0 is not improvement", not is_improvement("sharpe_ratio", 0.5, 1.0))
check("Vol 8% < 12% is improvement", is_improvement("annualized_volatility_pct", 8.0, 12.0))
check("Vol 15% < 12% is not improvement", not is_improvement("annualized_volatility_pct", 15.0, 12.0))
check("DD -5% < -10% is improvement (less negative)", is_improvement("max_drawdown_pct", -5.0, -10.0))

# =========================================================================
print("\nTEST 15: Conditions parsing")
print("=" * 70)

from auto_trader.runner import parse_conditions, check_conditions

conditions = parse_conditions(["alpha_ann_pct > 0", "annualized_volatility_pct < 20"])
check("Parsed 2 conditions", len(conditions) == 2)
check("First condition metric", conditions[0]["metric"] == "alpha_ann_pct")
check("First condition operator", conditions[0]["operator"] == ">")
check("First condition value", conditions[0]["value"] == 0.0)

metrics = {"alpha_ann_pct": 5.0, "annualized_volatility_pct": 15.0}
met, detail = check_conditions(metrics, conditions)
check("Both conditions met", met)
check("All detail entries have met=True", all(d["met"] for d in detail))

metrics_fail = {"alpha_ann_pct": -2.0, "annualized_volatility_pct": 15.0}
met_fail, detail_fail = check_conditions(metrics_fail, conditions)
check("Negative alpha fails", not met_fail)

# =========================================================================
print("\nTEST 16: Validate portfolio tool")
print("=" * 70)

from auto_trader.tools import validate_portfolio

valid_config = {
    "name": "Test",
    "sleeves": [{
        "label": "S1", "weight": 1.0, "regime_gate": ["*"],
        "strategy_config": {
            "name": "Test Strat",
            "universe": {"type": "sector", "sector": "Technology"},
            "entry": {"conditions": [{"type": "always"}], "logic": "all"},
            "sizing": {"type": "equal_weight", "max_positions": 5, "initial_allocation": 100000},
            "backtest": {"start": "2020-01-01", "end": "2024-12-31", "entry_price": "next_close", "slippage_bps": 10}
        }
    }]
}
result = validate_portfolio(valid_config)
check("Valid config passes", result["valid"] == True)

bad_weights = {**valid_config, "sleeves": [{**valid_config["sleeves"][0], "weight": 0.5}]}
result = validate_portfolio(bad_weights)
check("Bad weights rejected", result["valid"] == False)
check("Error mentions weights", "weight" in result.get("error", "").lower())

no_sleeves = {"name": "Test"}
result = validate_portfolio(no_sleeves)
check("No sleeves rejected", result["valid"] == False)

# =========================================================================
print("\nTEST 17: Date filtering")
print("=" * 70)

from auto_trader.tools import create_auto_trader_tools, execute_query

create_auto_trader_tools(stop_date="2019-12-31")
r = execute_query("SELECT MAX(date) as latest FROM prices")
check("Date filter applied", r["rows"][0]["latest"] <= "2019-12-31")

r = execute_query("SELECT COUNT(*) as n FROM universe_profiles")
check("Non-date tables unfiltered", r["rows"][0]["n"] > 500)

r = execute_query("DELETE FROM prices")
check("Mutation blocked", "error" in r)

create_auto_trader_tools(stop_date=None)  # reset

# =========================================================================
# Cleanup — stop any running processes
# =========================================================================
for rid in [RUN_ID, SP_RUN_ID]:
    try:
        api("POST", f"/auto-trader/runs/{rid}/stop")
    except:
        pass

# =========================================================================
print("\n" + "=" * 70)
print(f"RESULTS: {PASS}/{PASS + FAIL} passed, {FAIL} failed")
print("=" * 70)

if FAIL > 0:
    print(f"{FAIL} TESTS FAILED ❌")
    sys.exit(1)
else:
    print("ALL TESTS PASSED ✅")
