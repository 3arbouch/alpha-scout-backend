#!/usr/bin/env python3
"""
V2 is the only engine.

The legacy v1 engine has been decommissioned. The API + deploy_engine +
agent loop always run the v2 unified-position-book engine; the
`config.engine_version` field is retained for backward compatibility but is
ignored (any value — including "v1" — runs on v2).

This test verifies:
  - Default config (no engine_version) → v2
  - engine_version="v1" → v2 (field ignored, not an opt-out anymore)
  - engine_version="v2" → v2
  - Setting engine_version doesn't perturb results
  - v2's response shape (trades, metrics, sleeve_results, etc)

We don't reach through the FastAPI HTTP layer — we mirror the now-trivial
`_run_portfolio_bt` from server/api.py, which always calls v2.

(Numerical v1-vs-v2 parity is covered separately by test_v1_v2_parity_e2e.py,
which calls both executors directly.)

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
# Hack: don't fully load api.py (it spins up FastAPI). Pull just the engine
# function via direct re-import.
from portfolio_engine_v2 import run_portfolio_backtest as run_v2


def routed_run(config: dict, force_close_at_end: bool = True):
    """Mirror server/api.py:_run_portfolio_bt — v2 is the only engine."""
    return run_v2(config, force_close_at_end=force_close_at_end)


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
# 1. Default config (no engine_version) → V2 (new default)
# ---------------------------------------------------------------------------
print("\n=== 1. Default (no engine_version) routes to V2 ===")
r_default = routed_run(base_cfg(engine_version=None))
check("default response has 'sleeve_results'", "sleeve_results" in r_default)
check("default response includes engine_version=v2 tag (default is v2)",
      r_default.get("engine_version") == "v2",
      f"got engine_version={r_default.get('engine_version')}")


# ---------------------------------------------------------------------------
# 2. engine_version="v1" → V2 (field ignored, not an opt-out anymore)
# ---------------------------------------------------------------------------
print("\n=== 2. engine_version='v1' is ignored, routes to V2 ===")
r_v1 = routed_run(base_cfg(engine_version="v1"))
check("explicit v1 still runs v2 (tagged engine_version=v2)",
      r_v1.get("engine_version") == "v2",
      f"got engine_version={r_v1.get('engine_version')}")


# ---------------------------------------------------------------------------
# 3. engine_version="v2" explicit → V2 (matches default)
# ---------------------------------------------------------------------------
print("\n=== 3. engine_version='v2' explicit routes to V2 ===")
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
    check(f"default(v2) response has '{k}'", k in r_default,
          f"keys: {sorted(r_default.keys())[:6]}")
    check(f"v1-cfg(→v2) response has '{k}'", k in r_v1,
          f"keys: {sorted(r_v1.keys())[:6]}")
check("default(v2) response has 'backtest' top-level convenience",
      "backtest" in r_default)

check("both responses have len(sleeve_results) == 1",
      len(r_default.get("sleeve_results", [])) == 1
      and len(r_v1.get("sleeve_results", [])) == 1)


# ---------------------------------------------------------------------------
# 5. Setting engine_version doesn't perturb results — all run on v2
# ---------------------------------------------------------------------------
print("\n=== 5. engine_version field is inert (v1/v2/default all agree) ===")
m1 = r_v1.get("metrics") or {}
m2 = r_v2.get("metrics") or {}
for k in ("total_return_pct", "max_drawdown_pct"):
    a = m1.get(k)
    b = m2.get(k)
    check(f"metrics.{k} identical regardless of engine_version (v1-cfg={a}, v2-cfg={b})",
          a == b,
          f"v1-cfg={a} v2-cfg={b}")


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
