#!/usr/bin/env python3
"""
Unit test for build_history_context's eval-block rendering.

Inserts synthetic experiment rows (one with eval, one without) into a
temporary app DB, then calls build_history_context and asserts the rendered
prompt contains the right substrings.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_history_render_unit.py
"""
import json
import os
import sys
import tempfile

# Use a temp DB for this test so we don't touch app_dev.db.
TMP_DB = tempfile.NamedTemporaryFile(suffix="_history_test.db", delete=False)
TMP_DB.close()
os.environ["APP_DB_PATH"] = TMP_DB.name

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "auto_trader"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

# Force reload of auto_trader.schema to pick up the new APP_DB_PATH env.
import importlib
import auto_trader.schema as _schema  # noqa: E402
importlib.reload(_schema)

from auto_trader.schema import get_db, log_experiment  # noqa: E402
from runner import build_history_context  # noqa: E402

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


# Ensure tables exist.
get_db().close()


# ----- Insert experiment 1: no eval (legacy shape) -----
log_experiment(
    run_id="histtest",
    iteration=1,
    thesis="Old-school experiment with no eval block.",
    assumptions=["assumption A"],
    portfolio_config={"name": "Legacy", "sleeves": []},
    metrics={
        "total_return_pct": 12.5, "annualized_return_pct": 8.0,
        "sharpe_ratio": 1.10, "max_drawdown_pct": -15.0,
        "annualized_volatility_pct": 18.0, "alpha_ann_pct": 2.5,
    },
    target_metric="sharpe_ratio",
    target_value=1.10,
    conditions=[],
    conditions_met=True,
    decision="keep",
    best_value_so_far=0,
    backtest_start="2015-01-01", backtest_end="2025-01-01",
    initial_capital=100_000,
    eval_metrics_json=None,
    target_aggregator="overall",
)

# ----- Insert experiment 2: WITH eval block, aggregator=median -----
eval_block_2 = {
    "spec": {"window": "2y", "overlap": "1y"},
    "aggregated": {
        "sharpe_ratio":              {"mean": 1.05, "median": 1.15, "min": 0.30, "max": 1.90, "p25": 0.85, "count": 5},
        "alpha_ann_pct":             {"mean": 4.5,  "median": 6.2,  "min": -3.5, "max": 12.0, "p25": 1.0,  "count": 5},
        "max_drawdown_pct":          {"mean": -22.0,"median": -18.5,"min": -38.0,"max": -8.0, "p25": -30.0,"count": 5},
        "annualized_volatility_pct": {"mean": 17.0, "median": 16.5, "min": 13.0, "max": 24.0, "p25": 14.5, "count": 5},
    },
    "windows": [
        # Renderer uses len(windows) for the count, so populate all 5.
        {"label": f"win{i}", "start": "2015-01-01", "end": "2017-01-01",
         "metrics": {"sharpe_ratio": 1.15}}
        for i in range(5)
    ],
}
log_experiment(
    run_id="histtest",
    iteration=2,
    thesis="New experiment with walk-forward eval, climbing median Sharpe.",
    assumptions=["assumption B"],
    portfolio_config={"name": "WalkForward", "sleeves": []},
    metrics={
        "total_return_pct": 50.0, "annualized_return_pct": 8.5,
        "sharpe_ratio": 1.20, "max_drawdown_pct": -17.0,
        "annualized_volatility_pct": 16.0, "alpha_ann_pct": 3.0,
    },
    target_metric="sharpe_ratio",
    target_value=1.15,   # the median, per aggregator
    conditions=[],
    conditions_met=True,
    decision="keep",
    best_value_so_far=1.10,
    backtest_start="2015-01-01", backtest_end="2025-01-01",
    initial_capital=100_000,
    eval_metrics_json=json.dumps(eval_block_2),
    target_aggregator="median",
)

# ----- Render -----
rendered = build_history_context("histtest", "sharpe_ratio")
print("\n--- RENDERED ---")
print(rendered)
print("--- END ---\n")

# ----- Assertions -----
print("Assertions:")
check("rendered both experiments",
      "Experiment 1" in rendered and "Experiment 2" in rendered)
check("legacy metrics line uses 'training-period' qualifier",
      "**Metrics (training-period):**" in rendered)
check("legacy experiment has NO Eval line",
      rendered.count("**Eval (") == 1,  # only exp 2 should produce it
      f"got {rendered.count('**Eval (')} Eval lines")

# Eval line content checks.
check("eval line shows window count",          "5 windows, 2y/1y" in rendered)
check("eval line includes Sharpe min/med/max", "Sharpe min/med/max=0.30/1.15/1.90" in rendered)
check("eval line includes Alpha min/med/max",  "Alpha min/med/max=-3.5%/+6.2%/+12.0%" in rendered)
check("eval line includes MaxDD min/med/max",  "MaxDD min/med/max=-38.0%/-18.5%/-8.0%" in rendered)
check("eval line includes target=median(...)", "target=median(sharpe_ratio)=1.1500" in rendered)

# Cleanup tmp DB.
os.unlink(TMP_DB.name)

print(f"\n{'=' * 50}\n{PASS} passed, {FAIL} failed\n{'=' * 50}")
sys.exit(0 if FAIL == 0 else 1)
