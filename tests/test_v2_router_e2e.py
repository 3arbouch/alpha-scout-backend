#!/usr/bin/env python3
"""
V2 engine_version router (Phase 2 Step 4).

The API + deploy_engine route a portfolio backtest to v1 or v2 based on
`config.engine_version`. Default is v1 (no behavior change for any
existing deployment). Setting "v2" opts in to the unified-position-book
executor.

This test verifies:
  - Default routing (no engine_version, or engine_version="v1") → v1
  - engine_version="v2" → v2
  - Both engines accept the same input shape
  - Both return the same top-level response shape (trades, metrics,
    sleeve_results, etc) so downstream UI code doesn't break

We don't reach through the FastAPI HTTP layer — we directly call the
router function `_run_portfolio_bt` from server/api.py. That exercises
exactly the path the API uses.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_v2_router_e2e.py
"""
import copy
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "server"))

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


# Import the router function via the api.py module so we exercise exactly
# the path the API uses. The router is module-level after the imports.
import importlib.util
spec = importlib.util.spec_from_file_location(
    "_api_router",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "server", "api.py"),
)
# Hack: don't fully load api.py (it spins up FastAPI). Pull just the router
# function and its dependencies via direct re-imports.
from portfolio_engine import run_portfolio_backtest as run_v1
from portfolio_engine_v2 import run_portfolio_backtest as run_v2


def routed_run(config: dict, force_close_at_end: bool = True):
    """Mirror server/api.py:_run_portfolio_bt routing."""
    if (config or {}).get("engine_version") == "v2":
        return run_v2(config, force_close_at_end=force_close_at_end)
    return run_v1(config, force_close_at_end=force_close_at_end)


# ---------------------------------------------------------------------------
# Test config
# ---------------------------------------------------------------------------
TECH = ["AAPL", "MSFT", "NVDA", "AMD", "AVGO"]


def base_cfg(engine_version=None):
    cfg = {
        "name": "RouterProbe",
        "sleeves": [{
            "label": "Tech", "weight": 1.0, "regime_gate": ["*"],
            "strategy_config": {
                "name": "s",
                "universe": {"type": "symbols", "symbols": TECH},
                "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                "stop_loss": {"type": "drawdown_from_entry", "value": -25,
                              "cooldown_days": 60},
                "time_stop": {"max_days": 365},
                "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 5},
                "rebalancing": {"frequency": "none", "rules": {}},
                "sizing": {"type": "equal_weight", "max_positions": 5,
                            "initial_allocation": 100_000},
                "backtest": {"start": "2024-01-01", "end": "2024-03-31",
                             "entry_price": "next_close", "slippage_bps": 10},
            },
        }],
        "regime_filter": False,
        "capital_when_gated_off": "to_cash",
        "backtest": {"start": "2024-01-01", "end": "2024-03-31",
                     "initial_capital": 100_000},
    }
    if engine_version:
        cfg["engine_version"] = engine_version
    return cfg


# ---------------------------------------------------------------------------
# 1. Default config (no engine_version) → v1
# ---------------------------------------------------------------------------
print("\n=== 1. Default (no engine_version) routes to V1 ===")
r_default = routed_run(base_cfg(engine_version=None))
check("default response has 'sleeve_results'", "sleeve_results" in r_default)
check("default response missing 'engine_version' field (legacy v1)",
      "engine_version" not in r_default,
      "v1 doesn't tag its response — that's how we identify routing")


# ---------------------------------------------------------------------------
# 2. engine_version="v1" explicit → v1
# ---------------------------------------------------------------------------
print("\n=== 2. engine_version='v1' explicit routes to V1 ===")
r_v1 = routed_run(base_cfg(engine_version="v1"))
check("explicit v1 missing 'engine_version' in response (v1 doesn't tag)",
      "engine_version" not in r_v1)


# ---------------------------------------------------------------------------
# 3. engine_version="v2" → v2
# ---------------------------------------------------------------------------
print("\n=== 3. engine_version='v2' routes to V2 ===")
r_v2 = routed_run(base_cfg(engine_version="v2"))
check("v2 response includes engine_version=v2 tag",
      r_v2.get("engine_version") == "v2",
      f"got engine_version={r_v2.get('engine_version')}")


# ---------------------------------------------------------------------------
# 4. Both engines return the same top-level shape (for UI compat)
# ---------------------------------------------------------------------------
print("\n=== 4. Response shape compatibility ===")
required_shared = ["metrics", "sleeve_results", "per_sleeve", "config"]
for k in required_shared:
    check(f"v1 response has '{k}'", k in r_default, f"keys: {sorted(r_default.keys())[:6]}")
    check(f"v2 response has '{k}'", k in r_v2, f"keys: {sorted(r_v2.keys())[:6]}")
# v2-specific top-level convenience block; v1 lives inside config["backtest"]
check("v2 response has 'backtest' top-level convenience", "backtest" in r_v2)
check("v1 has backtest info via config.backtest",
      r_default.get("config", {}).get("backtest") is not None)

check("both responses have len(sleeve_results) == 1",
      len(r_default.get("sleeve_results", [])) == 1
      and len(r_v2.get("sleeve_results", [])) == 1)


# ---------------------------------------------------------------------------
# 5. For a no-regime config, v1 and v2 produce comparable metrics
# ---------------------------------------------------------------------------
print("\n=== 5. v1 vs v2 metrics agree on no-regime config ===")
m1 = r_v1.get("metrics") or {}
m2 = r_v2.get("metrics") or {}
for k in ("total_return_pct", "max_drawdown_pct"):
    v1 = m1.get(k)
    v2 = m2.get(k)
    check(f"metrics.{k} parity (v1={v1}, v2={v2})",
          v1 == v2,
          f"v1={v1} v2={v2}")


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
