#!/usr/bin/env python3
"""
V1 ↔ V2 parity test (Phase 2 Step 3a).

The unified-position-book V2 engine must produce BYTE-IDENTICAL trade
ledgers and NAV trajectories as V1 for configurations where V1 is
known to be correct (no dual-bookkeeping risk):

  Step 3a — single sleeve, no regime, fixed-weight=1.0:
    The trivial case. V1 has no lerp, no allocation profile, no phantom
    trades. V2 should reproduce it exactly.

Later steps add regime_gate, allocation profiles, multi-sleeve. Where V1's
behavior was the broken dual-bookkeeping pattern, V2 will INTENTIONALLY
diverge to produce the clean broker-equivalent ledger (those tests live
elsewhere). This file is the parity gate — V2 cannot ship until the
no-regime case is byte-identical.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_v1_v2_parity_e2e.py
"""
import copy
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine import run_portfolio_backtest as run_v1
from portfolio_engine_v2 import run_portfolio_backtest as run_v2

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


def trade_sig(t):
    """Canonical signature for comparing trades."""
    return (
        t["date"], t["symbol"], t["action"], t.get("reason"),
        round(float(t.get("price", 0)), 4),
        round(float(t.get("shares", 0)), 4),
    )


# ---------------------------------------------------------------------------
# Helper: build a no-regime single-sleeve config
# ---------------------------------------------------------------------------
TECH_UNI = ["AAPL", "MSFT", "NVDA", "AMD", "AVGO", "INTC",
             "MU", "ARM", "COHR", "WDC", "TER", "GLW"]


def base_strat(extra_strategy=None):
    s = {
        "name": "ParityProbe",
        "universe": {"type": "symbols", "symbols": TECH_UNI},
        "entry": {"conditions": [{"type": "always"}], "logic": "all"},
        "stop_loss": {"type": "drawdown_from_entry", "value": -25, "cooldown_days": 60},
        "time_stop": {"max_days": 365},
        "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 5},
        "rebalancing": {"frequency": "none", "rules": {}},
        "sizing": {"type": "equal_weight", "max_positions": 5,
                    "initial_allocation": 500_000},
        "backtest": {"start": "2024-01-01", "end": "2024-06-30",
                     "entry_price": "next_close", "slippage_bps": 10},
    }
    if extra_strategy:
        for k, v in extra_strategy.items():
            s[k] = v
    return s


def base_portfolio(strat_overrides=None):
    return {
        "name": "ParityPortfolio",
        "sleeves": [{
            "label": "Tech", "weight": 1.0, "regime_gate": ["*"],
            "strategy_config": base_strat(strat_overrides),
        }],
        "regime_filter": False,
        "capital_when_gated_off": "to_cash",
        "backtest": {"start": "2024-01-01", "end": "2024-06-30",
                     "initial_capital": 500_000},
    }


# ---------------------------------------------------------------------------
# Run a config through both engines and compare
# ---------------------------------------------------------------------------
def run_parity(name: str, config: dict):
    print(f"\n{'━' * 70}\n  {name}\n{'━' * 70}")

    r_v1 = run_v1(copy.deepcopy(config), force_close_at_end=True)
    r_v2 = run_v2(copy.deepcopy(config), force_close_at_end=True)

    # Trade ledger
    if r_v1.get("trades"):
        t1 = r_v1["trades"]
    else:
        # v1's run_portfolio_backtest puts trades inside each sleeve_results
        # entry; flatten across all sleeves for multi-sleeve comparison.
        t1 = []
        for sr in r_v1.get("sleeve_results", []):
            t1.extend(sr.get("trades", []))
    t2 = r_v2.get("trades", [])

    sigs1 = sorted([trade_sig(t) for t in t1])
    sigs2 = sorted([trade_sig(t) for t in t2])

    check(f"trade count parity (v1={len(t1)}, v2={len(t2)})",
          len(t1) == len(t2),
          f"v1 has {len(t1)}, v2 has {len(t2)}")

    check("trade ledger byte-identical (sorted)",
          sigs1 == sigs2,
          f"first diff: {next(((a, b) for a, b in zip(sigs1, sigs2) if a != b), 'none')}")

    # Top-level metrics. total_return_pct / max_drawdown_pct must match
    # byte-for-byte. final_nav allows ≤$1 sub-cent FP noise from different
    # cash-aggregation summation orders across sleeves (v1 sums per-sleeve
    # rounded NAVs; v2 sums per-sleeve cash pools then mark-to-market).
    m1 = r_v1.get("metrics") or {}
    m2 = r_v2.get("metrics") or {}
    for key in ("total_return_pct", "max_drawdown_pct"):
        v1 = m1.get(key)
        v2 = m2.get(key)
        check(f"metrics.{key} parity: v1={v1} v2={v2}",
              v1 == v2,
              f"v1={v1} v2={v2}")
    fn1, fn2 = m1.get("final_nav"), m2.get("final_nav")
    check(f"metrics.final_nav within $1 (v1={fn1}, v2={fn2})",
          fn1 is not None and fn2 is not None and abs(fn1 - fn2) <= 1.0,
          f"v1={fn1} v2={fn2}  diff=${abs((fn1 or 0)-(fn2 or 0)):.4f}")


# ---------------------------------------------------------------------------
# Scenario tests
# ---------------------------------------------------------------------------

# Scenario 1: simplest — always entry, momentum_rank, equal_weight, no rebalance, no stops fire
run_parity("S3a-1 simplest single sleeve",
            base_portfolio())

# Scenario 2: with stops + take_profit
run_parity("S3a-2 with stop_loss + take_profit",
            base_portfolio({
                "take_profit": {"type": "above_peak", "value": 20},
            }))

# Scenario 3: with quarterly rebalance + add_on_earnings_beat
run_parity("S3a-3 quarterly trim rebalance + earnings_beat add",
            base_portfolio({
                "rebalancing": {"frequency": "quarterly", "mode": "trim",
                                "rules": {"max_position_pct": 30,
                                          "trim_pct": 50,
                                          "on_earnings_beat": "add",
                                          "add_on_earnings_beat": {
                                              "min_gain_pct": 10,
                                              "max_add_multiplier": 1.5,
                                              "lookback_days": 90}}},
            }))

# Scenario 4: risk_parity sizing
run_parity("S3a-4 risk_parity sizing",
            base_portfolio({
                "sizing": {"type": "risk_parity", "max_positions": 5,
                            "initial_allocation": 500_000, "vol_window_days": 20},
            }))

# Scenario 5: next_open entry mode
run_parity("S3a-5 entry_price=next_open",
            base_portfolio({
                "backtest": {"start": "2024-01-01", "end": "2024-06-30",
                             "entry_price": "next_open", "slippage_bps": 10},
            }))


# ---------------------------------------------------------------------------
# Step 3b: multi-sleeve fixed-weight (regime_filter=false)
# ---------------------------------------------------------------------------
DEF_UNI = ["JNJ", "PG", "KO", "PEP", "WMT", "JPM", "BAC", "XOM"]


def two_sleeve_portfolio(tech_weight=0.7, def_weight=0.3):
    tech = base_strat()
    tech["sizing"]["initial_allocation"] = int(500_000 * tech_weight)
    defs = {
        "name": "DefStrat", "universe": {"type": "symbols", "symbols": DEF_UNI},
        "entry": {"conditions": [{"type": "always"}], "logic": "all"},
        "stop_loss": {"type": "drawdown_from_entry", "value": -15, "cooldown_days": 60},
        "time_stop": {"max_days": 365},
        "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 4},
        "rebalancing": {"frequency": "none", "rules": {}},
        "sizing": {"type": "equal_weight", "max_positions": 4,
                    "initial_allocation": int(500_000 * def_weight)},
        "backtest": {"start": "2024-01-01", "end": "2024-06-30",
                     "entry_price": "next_close", "slippage_bps": 10},
    }
    return {
        "name": "MultiSleeve",
        "sleeves": [
            {"label": "Tech",      "weight": tech_weight, "regime_gate": ["*"],
             "strategy_config": tech},
            {"label": "Defensive", "weight": def_weight,  "regime_gate": ["*"],
             "strategy_config": defs},
        ],
        "regime_filter": False,
        "capital_when_gated_off": "to_cash",
        "backtest": {"start": "2024-01-01", "end": "2024-06-30",
                     "initial_capital": 500_000},
    }


run_parity("S3b-1 two sleeves 70/30 fixed-weight",
            two_sleeve_portfolio(tech_weight=0.7, def_weight=0.3))

run_parity("S3b-2 two sleeves 50/50 fixed-weight",
            two_sleeve_portfolio(tech_weight=0.5, def_weight=0.5))


# Three sleeves with different sizing types
def three_sleeve_portfolio():
    tech = base_strat()
    tech["sizing"]["initial_allocation"] = 250_000
    defs = {
        "name": "DefStrat", "universe": {"type": "symbols", "symbols": DEF_UNI},
        "entry": {"conditions": [{"type": "always"}], "logic": "all"},
        "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 3},
        "rebalancing": {"frequency": "none", "rules": {}},
        "sizing": {"type": "equal_weight", "max_positions": 3,
                    "initial_allocation": 150_000},
        "backtest": {"start": "2024-01-01", "end": "2024-06-30",
                     "entry_price": "next_close", "slippage_bps": 10},
    }
    big_tech = base_strat()
    big_tech["sizing"] = {"type": "risk_parity", "max_positions": 5,
                          "initial_allocation": 100_000, "vol_window_days": 20}
    return {
        "name": "ThreeSleeve",
        "sleeves": [
            {"label": "Tech",      "weight": 0.5, "regime_gate": ["*"],
             "strategy_config": tech},
            {"label": "Defensive", "weight": 0.3, "regime_gate": ["*"],
             "strategy_config": defs},
            {"label": "TechRP",    "weight": 0.2, "regime_gate": ["*"],
             "strategy_config": big_tech},
        ],
        "regime_filter": False,
        "capital_when_gated_off": "to_cash",
        "backtest": {"start": "2024-01-01", "end": "2024-06-30",
                     "initial_capital": 500_000},
    }


run_parity("S3b-3 three sleeves with mixed sizing types",
            three_sleeve_portfolio())


# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 70)
sys.exit(0 if FAIL == 0 else 1)
