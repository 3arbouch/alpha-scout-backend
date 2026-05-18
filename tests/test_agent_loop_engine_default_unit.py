#!/usr/bin/env python3
"""
Verifies the agent loop uses engine v2 by default.

Today's behavior: `runner._run_one_backtest` imports `run_portfolio_backtest`
from `portfolio_engine_v2` UNLESS the portfolio config sets
`engine_version: "v1"` (explicit opt-out).

This is the operationally-load-bearing thing — the metrics the agent climbs
must come from the same engine deployments run, otherwise we get
optimize-vs-deploy drift.

Run: python3 tests/test_agent_loop_engine_default_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "auto_trader"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

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


# Build a portfolio config without engine_version, capture which module
# `_run_one_backtest` actually imports from. We monkey-patch the two
# candidate functions to identify themselves.
import auto_trader.runner as runner_mod  # noqa: E402
import portfolio_engine as v1_mod  # noqa: E402
import portfolio_engine_v2 as v2_mod  # noqa: E402


CALLED_ENGINE = {"value": None}


def fake_v1(cfg, force_close_at_end=True):
    CALLED_ENGINE["value"] = "v1"
    return {"metrics": {}, "sleeve_results": [], "combined_nav_history": []}


def fake_v2(cfg, force_close_at_end=True):
    CALLED_ENGINE["value"] = "v2"
    return {"metrics": {}, "sleeve_results": [], "combined_nav_history": []}


_orig_v1 = v1_mod.run_portfolio_backtest
_orig_v2 = v2_mod.run_portfolio_backtest


def call_runner(engine_version=None):
    """Invoke _run_one_backtest with optional engine_version on the config.

    The runner does `from portfolio_engine[_v2] import run_portfolio_backtest`
    inside the function — so we patch on the modules so the import sees
    our fakes. Then we look at which fake fired.
    """
    v1_mod.run_portfolio_backtest = fake_v1
    v2_mod.run_portfolio_backtest = fake_v2
    CALLED_ENGINE["value"] = None
    cfg = {
        "name": "engine-default-probe",
        "sleeves": [{"label": "S", "weight": 1.0, "regime_gate": ["*"],
                     "strategy_config": {
                         "name": "s",
                         "universe": {"type": "symbols", "symbols": ["AAPL"]},
                         "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                         "stop_loss": {"type": "drawdown_from_entry", "value": -25, "cooldown_days": 30},
                         "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 1},
                         "rebalancing": {"frequency": "none", "rules": {}},
                         "sizing": {"type": "equal_weight", "max_positions": 1, "initial_allocation": 1000},
                     }}],
        "regime_filter": False,
        "capital_when_gated_off": "to_cash",
    }
    if engine_version is not None:
        cfg["engine_version"] = engine_version
    try:
        runner_mod._run_one_backtest(cfg, "2024-01-01", "2024-03-31", 1000, None)
    finally:
        v1_mod.run_portfolio_backtest = _orig_v1
        v2_mod.run_portfolio_backtest = _orig_v2
    return CALLED_ENGINE["value"]


print("\nAgent loop engine dispatch:")
check("no engine_version → v2 (new default)",
      call_runner(engine_version=None) == "v2")
check("engine_version='v2' → v2",
      call_runner(engine_version="v2") == "v2")
check("engine_version='v1' → v1 (explicit opt-out)",
      call_runner(engine_version="v1") == "v1")
check("engine_version='bogus' → v2 (anything not 'v1' → default)",
      call_runner(engine_version="bogus") == "v2")


print(f"\n{'=' * 50}\n{PASS} passed, {FAIL} failed\n{'=' * 50}")
sys.exit(0 if FAIL == 0 else 1)
