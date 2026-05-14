#!/usr/bin/env python3
"""
V2 allocation_profile invariants (Phase 2 Step 3d).

V2 INTENTIONALLY diverges from V1 here. V1 had the dual-bookkeeping bug
(patched, never fully fixed) where each sleeve simulates standalone and
portfolio-level lerp trades layer on top, producing phantom entries and
share-count mismatches.

V2 produces a clean broker-equivalent ledger:
  - On the day allocation_profile drops a sleeve to weight=0, liquidate
    all sleeve-tagged positions cleanly with reason=rebalance_to_<profile>.
  - On gated-off days, the sleeve emits NO trades (no entries, no exits,
    no rebalance adjustments).
  - On the day the sleeve returns to weight > 0, the normal signal
    pipeline rebuilds positions based on today's signals — NOT a
    proportional rebuy of pre-gate holdings.

This file tests those invariants on the v44-style config (TechMomentum
sleeve with stress_spx → 0 / default → 0.9). Acceptance criteria are
about the TRADE LEDGER, not v1 parity.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_v2_allocation_profile_e2e.py
"""
import copy
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine_v2 import run_portfolio_backtest as run_v2
from regime import evaluate_regime_series

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


# ---------------------------------------------------------------------------
# The v44-style config: TechMomentum sleeve, stress_spx zeroes it out
# ---------------------------------------------------------------------------
TECH = ["AAPL", "MSFT", "NVDA", "AMD", "AVGO", "INTC",
         "MU", "ARM", "COHR", "WDC", "TER", "GLW"]

STRESS = {
    "conditions": [{"series": "spx_vs_200dma_pct", "operator": "<", "value": 0}],
    "logic": "all",
    "entry_persistence_days": 3,
    "exit_persistence_days": 7,
}


def v44_style_config(start="2022-01-01", end="2022-12-31"):
    return {
        "name": "V44Style",
        "sleeves": [{
            "label": "TechMomentum", "weight": 1.0, "regime_gate": [],
            "strategy_config": {
                "name": "TechM", "universe": {"type": "symbols", "symbols": TECH},
                "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                "stop_loss": {"type": "drawdown_from_entry", "value": -25, "cooldown_days": 60},
                "time_stop": {"max_days": 365},
                "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 5},
                "rebalancing": {"frequency": "none", "rules": {}},
                "sizing": {"type": "equal_weight", "max_positions": 5,
                            "initial_allocation": 500_000},
                "backtest": {"start": start, "end": end,
                             "entry_price": "next_close", "slippage_bps": 10},
            }
        }],
        "regime_filter": True,
        "regime_definitions": {"stress_spx": STRESS},
        "allocation_profiles": {
            "stress_spx_profile": {"trigger": ["stress_spx"],
                                    "weights": {"TechMomentum": 0, "Cash": 1}},
            "default": {"trigger": [],
                         "weights": {"TechMomentum": 0.9, "Cash": 0.1}},
        },
        "profile_priority": ["stress_spx_profile", "default"],
        "transition_days_to_defensive": 1,
        "transition_days_to_offensive": 1,
        "rebalance_threshold": 0.05,
        "capital_when_gated_off": "to_cash",
        "backtest": {"start": start, "end": end,
                     "initial_capital": 500_000},
    }


# ---------------------------------------------------------------------------
print("\n=== V2 v44-style allocation_profile run ===")
result = run_v2(copy.deepcopy(v44_style_config()))
trades = result["trades"]
print(f"\n  trades emitted: {len(trades)}")


# ---------------------------------------------------------------------------
# 1. Compute stress/default day sets directly from regime series
# ---------------------------------------------------------------------------
print("\n=== 1. Identify stress days ===")
regime_series = evaluate_regime_series(
    "2022-01-01", "2022-12-31",
    [{"name": "stress_spx", **STRESS}],
)
stress_dates = {d for d, active in regime_series.items() if "stress_spx" in active}
default_dates = {d for d in regime_series if d not in stress_dates}
print(f"  stress_spx days: {len(stress_dates)} | default days: {len(default_dates)}")
check("config has both regimes firing during 2022",
      len(stress_dates) > 10 and len(default_dates) > 10)


# ---------------------------------------------------------------------------
# 2. NO phantom entry/rebalance BUYs on stress days
# ---------------------------------------------------------------------------
print("\n=== 2. No phantom sleeve activity on stress days (v44 bug) ===")
phantom = [
    t for t in trades
    if t["action"] == "BUY"
    and t["date"] in stress_dates
    and not str(t.get("reason", "")).startswith("rebalance_to_")
]
check("zero sleeve-level BUYs on stress days (signal-driven entries suppressed)",
      len(phantom) == 0,
      f"got {len(phantom)} phantom BUYs: "
      f"{[(t['date'], t['symbol'], t['reason']) for t in phantom[:5]]}")

phantom_exits = [
    t for t in trades
    if t["action"] == "SELL"
    and t["date"] in stress_dates
    and t.get("reason") in ("stop_loss", "take_profit", "time_stop")
]
check("zero phantom stop/TP/time_stop SELLs on stress days",
      len(phantom_exits) == 0,
      f"got {len(phantom_exits)} phantom exits")


# ---------------------------------------------------------------------------
# 3. Liquidation trades on first stress day
# ---------------------------------------------------------------------------
print("\n=== 3. Liquidation on alloc_profile flip ===")
liquidations = [
    t for t in trades
    if t["action"] == "SELL"
    and str(t.get("reason", "")).startswith("rebalance_to_")
]
check("at least 1 rebalance_to_* SELL emitted (allocation flip)",
      len(liquidations) >= 1, f"got {len(liquidations)}")
# At least one liquidation should fire on the FIRST stress day after a default period
first_stress = min(stress_dates) if stress_dates else None
liq_on_first_stress = [
    t for t in liquidations if t["date"] == first_stress
]
check(f"liquidation fires on first stress day {first_stress}",
      len(liq_on_first_stress) >= 1,
      f"got {len(liq_on_first_stress)} (first stress = {first_stress})")


# ---------------------------------------------------------------------------
# 4. Cumulative shares per symbol never goes negative
# ---------------------------------------------------------------------------
print("\n=== 4. Trade ledger consistency: cum_shares >= 0 ===")
cum = defaultdict(float)
violations = []
for t in sorted(trades, key=lambda x: (x["date"], 0 if x["action"] == "BUY" else 1)):
    sym = t["symbol"]
    s = float(t["shares"])
    if t["action"] == "BUY":
        cum[sym] += s
    else:
        if cum[sym] - s < -1e-3:
            violations.append((t["date"], sym, t["action"], s, cum[sym]))
        cum[sym] -= s
check("no SELL ever exceeds cumulative held shares (no negative cum_shares)",
      not violations,
      f"first violations: {violations[:3]}")


# ---------------------------------------------------------------------------
# 5. Re-entry after gate end uses normal signal pipeline
# ---------------------------------------------------------------------------
print("\n=== 5. Re-entry after stress → default uses signal pipeline ===")
# After a stress block ends, the next BUYs should have reason="entry" (NOT
# rebalance_to_default) — proving the sleeve's normal signal pipeline drives
# re-entry, not a proportional rebuy.
import sqlite3
m = sqlite3.connect("/home/mohamed/alpha-scout-backend/data/market.db")
trading_dates = [r[0] for r in m.execute(
    "SELECT DISTINCT date FROM prices WHERE date BETWEEN '2022-01-01' AND '2022-12-31' ORDER BY date"
).fetchall()]
m.close()

# Find the first date that is default AFTER a stress block
post_stress = None
for i, d in enumerate(trading_dates):
    if d in default_dates and i > 0 and trading_dates[i-1] in stress_dates:
        post_stress = d
        break

if post_stress is None:
    check("found at least one stress→default transition",
          False, "no transition found in 2022 window")
else:
    print(f"  first post-stress default day: {post_stress}")
    # On that day or shortly after, sleeve signals fire → normal entry BUYs
    # Look at the BUYs in the 30 days after post_stress
    post_buys = [
        t for t in trades
        if t["action"] == "BUY" and t["date"] >= post_stress
        and t.get("reason") == "entry"
    ]
    check(f"after stress→default transition, sleeve emits entry BUYs from signal pipeline",
          len(post_buys) > 0,
          f"got {len(post_buys)} entry BUYs after {post_stress}")


# ---------------------------------------------------------------------------
# 6. NAV reconciles
# ---------------------------------------------------------------------------
print("\n=== 6. Combined NAV reconciles ===")
metrics = result["metrics"]
final = metrics.get("final_nav")
implied = 500_000 * (1 + metrics.get("total_return_pct", 0) / 100)
# Tolerance allows for compute_metrics' 2dp rounding of total_return_pct:
# 0.005% of $500k = $25 fluctuation is expected even when math is correct.
check(f"final_nav matches total_return_pct (got {final}, expected ~{implied:.2f})",
      final is not None and abs(final - implied) < 50.0)


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
