#!/usr/bin/env python3
"""
Allocation-profile gating regression (v44-style bug).

Before the fix: when a sleeve had regime_gate=[] and allocation_profiles
zeroed it out for some regime, the sleeve's own simulation continued to run
unconstrained — emitting entry / rebalance trades during regimes when the
portfolio-level lerp held the sleeve at 0% allocation. The combined NAV was
correct (because the lerp ignored the sleeve), but the trade log surfaced
phantom trades that confused users.

After the fix: sleeve_gate_dates is the intersection of:
  - regime_gate dates (or all-on if ["*"] / [] / regime disabled)
  - allocation-profile non-zero-weight dates

So when the active allocation profile assigns the sleeve weight=0, the sleeve
is gated off — no entries, no rebalances, only exits can fire.

Test approach: build a portfolio that mirrors v44's shape (single sleeve,
regime_gate=[], allocation_profile with stress regime zeroing it out), run
on a window that includes the stress regime, and assert:
  - During the stress period, NO entry / rebalance trades fire on the sleeve.
  - Portfolio-level rebalance_to_* trades still fire (those are correct).
  - Combined NAV during stress is approximately constant (all cash).
  - When the stress regime exits, sleeve resumes normal activity.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_allocation_profile_gating_e2e.py
"""
import copy
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine import run_portfolio_backtest

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


# ---------------------------------------------------------------------------
# Portfolio config: mirrors v44 shape — single sleeve with regime_gate=[]
# (which historically meant "always active") plus an allocation_profile that
# zeroes it out on stress_spx regime. Pre-fix this produced phantom entries
# during stress periods.
# ---------------------------------------------------------------------------
STRATEGY = {
    "name": "TechMomentum",
    "universe": {"type": "symbols",
                  "symbols": ["AAPL", "MSFT", "NVDA", "AMD", "AVGO", "INTC",
                              "MU", "GLW", "ARM", "COHR", "WDC", "TER"]},
    "entry": {
        "conditions": [{"type": "always"}],
        "logic": "all",
    },
    "stop_loss": {"type": "drawdown_from_entry", "value": -25, "cooldown_days": 60},
    "time_stop": {"max_days": 365},
    "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 5},
    "rebalancing": {"frequency": "quarterly", "mode": "trim",
                     "rules": {"max_position_pct": 25, "trim_pct": 50}},
    "sizing": {"type": "equal_weight", "max_positions": 5, "initial_allocation": 1000000},
    "backtest": {"start": "2022-01-01", "end": "2022-12-31",
                 "entry_price": "next_close", "slippage_bps": 10},
}

PORTFOLIO = {
    "name": "v44-style gating regression",
    "sleeves": [{
        "label": "TechMomentum",
        "weight": 1,
        "regime_gate": [],   # ← the v44 pattern: empty means "no regime_gate"
        "strategy_config": STRATEGY,
    }],
    "regime_filter": True,
    "regime_definitions": {
        # SPX < 200dma — a regime that fires in 2024 around mid-year corrections.
        "stress_spx": {
            "conditions": [
                {"series": "spx_vs_200dma_pct", "operator": "<", "value": 0}
            ],
            "logic": "all",
            "entry_persistence_days": 3,
            "exit_persistence_days": 7,
        },
    },
    "capital_when_gated_off": "to_cash",
    "allocation_profiles": {
        "stress_spx_profile": {
            "trigger": ["stress_spx"],
            "weights": {"TechMomentum": 0, "Cash": 1},   # ← sleeve zero'd out
        },
        "default": {
            "trigger": [],
            "weights": {"TechMomentum": 0.9, "Cash": 0.1},
        },
    },
    "profile_priority": ["stress_spx_profile", "default"],
    "transition_days_to_defensive": 1,
    "transition_days_to_offensive": 3,
    "rebalance_threshold": 0.05,
    "backtest": {"start": "2022-01-01", "end": "2022-12-31",
                 "initial_capital": 1000000},
}


print("\n=== Running portfolio backtest with v44-style config ===")
result = run_portfolio_backtest(copy.deepcopy(PORTFOLIO), force_close_at_end=True)

# Pull the sleeve's combined trade ledger (sleeve trades + portfolio-level
# rebalance_to_* trades appended).
sleeve_result = result["sleeve_results"][0]
trades = sleeve_result["trades"]


# ---------------------------------------------------------------------------
# 1. Identify the stress_spx-active dates from the regime/profile history
# ---------------------------------------------------------------------------
print("\n=== 1. Identify gated-off date ranges ===")

# Recompute the regime series directly so we know which days the
# stress_spx_profile would have been the active target. allocation_profile_history
# only records transitions, not per-day state.
from regime import evaluate_regime_series
regime_series = evaluate_regime_series(
    PORTFOLIO["backtest"]["start"], PORTFOLIO["backtest"]["end"],
    [{"name": "stress_spx",
      **PORTFOLIO["regime_definitions"]["stress_spx"]}],
)
stress_dates = {d for d, active in regime_series.items() if "stress_spx" in active}
default_dates = {d for d in regime_series if d not in stress_dates}

print(f"  stress_spx-active days (TechMomentum weight=0): {len(stress_dates)}")
print(f"  default-active days     (TechMomentum weight>0): {len(default_dates)}")

# Require BOTH stress and default to exist in 2024 for the test to be meaningful.
check("stress_spx_profile activated for at least 5 days in window",
      len(stress_dates) >= 5,
      f"got {len(stress_dates)} — backtest window may be too short or stress regime didn't fire")
check("default profile activated for at least 5 days in window",
      len(default_dates) >= 5, f"got {len(default_dates)}")


# ---------------------------------------------------------------------------
# 2. No sleeve-level entry trades on gated-off (stress) dates
# ---------------------------------------------------------------------------
print("\n=== 2. No phantom sleeve entries during stress regime ===")

# Sleeve-level entry trades have reason == "entry". Portfolio-level
# rebalance_to_* trades are emitted on profile-flip days and are EXPECTED
# (those are the actual capital-movement trades).
sleeve_entries_in_stress = [
    t for t in trades
    if t["action"] == "BUY"
    and t.get("reason") == "entry"
    and t["date"] in stress_dates
]
check("zero sleeve-level entry trades on stress_spx days (the v44 bug)",
      len(sleeve_entries_in_stress) == 0,
      f"got {len(sleeve_entries_in_stress)} phantom entries: "
      f"{[(t['date'], t['symbol'], t['shares']) for t in sleeve_entries_in_stress[:5]]}")

# Same for rebalance add-on entries that previously fired during gated-off
# quarters. Those are also tagged with "entry" by the engine (open_position
# always uses reason="entry"). Caught by the same assertion above.


# ---------------------------------------------------------------------------
# 3. Portfolio-level rebalance_to_* trades DO fire on profile flips
# ---------------------------------------------------------------------------
print("\n=== 3. Portfolio-level lerp trades still fire ===")

reb_trades = [t for t in trades if str(t.get("reason", "")).startswith("rebalance_to_")]
check("at least 1 rebalance_to_* trade fired (portfolio profile flip)",
      len(reb_trades) >= 1, f"got {len(reb_trades)}")

# When regime enters stress: should see rebalance_to_stress_spx_profile
to_stress = [t for t in reb_trades if "stress" in str(t.get("reason", ""))]
to_default = [t for t in reb_trades if "default" in str(t.get("reason", ""))]
check("entered stress_spx via rebalance_to_stress_spx_profile",
      len(to_stress) >= 1, f"got {len(to_stress)}")


# ---------------------------------------------------------------------------
# 4. Sleeve has 0 NAV contribution on pure stress days (no transition)
# ---------------------------------------------------------------------------
print("\n=== 4. Sleeve display_nav = 0 on stress days ===")

# combined_nav_history entries include per-sleeve dicts with display NAV.
# On a stress day with stress_spx_profile active and no transition in
# progress, the sleeve's weight should be 0, so its display_nav contribution
# is 0 (or near-0 from end-of-transition rounding).
sleeve_navs_on_stress = []
for entry in result.get("combined_nav_history", []):
    if entry["date"] not in stress_dates:
        continue
    sleeve_data = entry.get("sleeves") or []
    for s in sleeve_data:
        if s.get("label") == "TechMomentum":
            sleeve_navs_on_stress.append((entry["date"], s.get("nav", 0), s.get("weight", 0)))

# We expect MOST stress days to have weight=0. Allow some tail (transitions
# overlap with the start of the stress period).
zero_weight_days = sum(1 for _, _, w in sleeve_navs_on_stress if w == 0)
total_stress_with_data = len(sleeve_navs_on_stress)
if total_stress_with_data > 0:
    pct_at_zero = zero_weight_days / total_stress_with_data * 100
    check(f"sleeve weight = 0 on >= 90% of stress days ({pct_at_zero:.1f}% zero)",
          pct_at_zero >= 90,
          f"only {pct_at_zero:.1f}% — too many transition / leak days")
else:
    check("sleeve nav recorded for stress days",
          False, "no per-sleeve nav data found in combined_nav_history")


# ---------------------------------------------------------------------------
# 5. Sleeve resumes normal activity after regime exit
# ---------------------------------------------------------------------------
print("\n=== 5. Sleeve resumes after regime exits ===")

# Sleeve-level entries on DEFAULT dates should be > 0 — proves gating only
# fires when it should, not always.
sleeve_entries_in_default = [
    t for t in trades
    if t["action"] == "BUY"
    and t.get("reason") == "entry"
    and t["date"] in default_dates
]
check("sleeve does enter normally on default-profile days",
      len(sleeve_entries_in_default) > 0,
      f"got {len(sleeve_entries_in_default)} — gate may be over-restrictive")


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
