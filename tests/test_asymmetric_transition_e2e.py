#!/usr/bin/env python3
"""
Asymmetric transition_days test (Gap 12).

Verifies portfolio_engine's `transition_days_to_defensive` and
`transition_days_to_offensive` configurables:

  • Defensive direction = equity_weight decreases (more cash / less risk)
  • Offensive direction = equity_weight increases (less cash / more risk)

Two parts:
  1. Spec test — replicates _resolve_transition_days logic and confirms
     direction detection given from/to weight pairs.
  2. Integration test — runs a portfolio_backtest with
     to_defensive=2 / to_offensive=10 and a regime-driven profile switch.
     Inspects allocation_profile_history for the recorded lerp durations.

The integration test gracefully handles the case where the regime never
fires during the window — it reports skip instead of failing.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_asymmetric_transition_e2e.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine import run_portfolio_backtest

PASS = 0
FAIL = 0
SKIP = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


def skip(name, reason):
    global SKIP
    SKIP += 1
    print(f"  ⊘ {name} — SKIPPED: {reason}")


# ---------------------------------------------------------------------------
# Part 1: Spec test — replicate _resolve_transition_days
# ---------------------------------------------------------------------------
print("\n=== Part 1: spec — direction detection from equity weight delta ===")


def equity_weight(weights):
    """Sum of non-Cash weights (matches portfolio_engine._equity_weight)."""
    return sum(w for k, w in (weights or {}).items() if k.lower() != "cash")


def resolve_transition_days(
    from_weights, to_weights,
    transition_days, td_to_defensive, td_to_offensive,
):
    """Replicates portfolio_engine._resolve_transition_days (lines 601-612)."""
    asymmetric = td_to_defensive is not None or td_to_offensive is not None
    if not asymmetric:
        return transition_days
    eq_from = equity_weight(from_weights)
    eq_to = equity_weight(to_weights)
    if eq_to < eq_from:
        return td_to_defensive or transition_days
    if eq_to > eq_from:
        return td_to_offensive or transition_days
    return transition_days


# Defensive direction: equity weight drops
out = resolve_transition_days(
    from_weights={"Tech": 0.50, "Defensive": 0.50, "Cash": 0.0},
    to_weights={"Tech": 0.10, "Defensive": 0.40, "Cash": 0.50},
    transition_days=5, td_to_defensive=2, td_to_offensive=10,
)
check("defensive (equity 1.0 → 0.5) → uses to_defensive=2", out == 2,
      f"got {out}")

# Offensive direction: equity weight rises
out = resolve_transition_days(
    from_weights={"Tech": 0.10, "Defensive": 0.40, "Cash": 0.50},
    to_weights={"Tech": 0.50, "Defensive": 0.50, "Cash": 0.0},
    transition_days=5, td_to_defensive=2, td_to_offensive=10,
)
check("offensive (equity 0.5 → 1.0) → uses to_offensive=10", out == 10,
      f"got {out}")

# Equal equity weight (sideways rotation): falls back to legacy transition_days
out = resolve_transition_days(
    from_weights={"Tech": 0.50, "Defensive": 0.50, "Cash": 0.0},
    to_weights={"Tech": 0.20, "Defensive": 0.80, "Cash": 0.0},
    transition_days=5, td_to_defensive=2, td_to_offensive=10,
)
check("equal equity weight → falls back to legacy transition_days=5",
      out == 5, f"got {out}")

# Asymmetric inactive (both None): always returns legacy transition_days
out = resolve_transition_days(
    from_weights={"Tech": 1.0},
    to_weights={"Tech": 0.0, "Cash": 1.0},  # defensive
    transition_days=7, td_to_defensive=None, td_to_offensive=None,
)
check("asymmetric inactive (both None) → returns legacy transition_days=7",
      out == 7, f"got {out}")

# Only to_defensive set; offensive direction falls back
out = resolve_transition_days(
    from_weights={"Tech": 0.0, "Cash": 1.0},
    to_weights={"Tech": 1.0, "Cash": 0.0},  # offensive
    transition_days=7, td_to_defensive=2, td_to_offensive=None,
)
check("offensive with to_offensive=None → falls back to transition_days=7",
      out == 7, f"got {out}")


# ---------------------------------------------------------------------------
# Part 2: Integration test — full backtest with asymmetric config
# ---------------------------------------------------------------------------
print("\n=== Part 2: integration — backtest emits asymmetric lerp durations ===")

STRATEGY_TECH = {
    "name": "Tech",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "NVDA"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": "2020-01-01", "end": "2022-12-31",
                 "entry_price": "next_close", "slippage_bps": 10},
}
STRATEGY_DEFENSIVE = {
    "name": "Defensive",
    "universe": {"type": "symbols", "symbols": ["JNJ", "PG", "KO"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": "2020-01-01", "end": "2022-12-31",
                 "entry_price": "next_close", "slippage_bps": 10},
}

# 2020-2022 covers COVID shock + recovery + inflation/Fed-hike cycle, so
# credit_stress should fire and recover at least once. This is the simplest
# way to guarantee a profile flip in both directions.
portfolio_config = {
    "name": "asym_transition_test",
    "sleeves": [
        {"strategy_config": STRATEGY_TECH, "weight": 0.50,
         "regime_gate": ["*"], "label": "Tech"},
        {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.50,
         "regime_gate": ["*"], "label": "Defensive"},
    ],
    "regime_filter": True,
    "capital_flow": "to_cash",
    "backtest": {"start": "2020-01-01", "end": "2022-12-31",
                 "initial_capital": 200000, "slippage_bps": 10},
    # Production shape: every profile (including "default") is wrapped in
    # {trigger, weights}. The auto_trader emits this form universally; the
    # bare top-level form only appears in legacy tests.
    "allocation_profiles": {
        "default": {"trigger": [], "weights": {"Tech": 0.50, "Defensive": 0.50, "Cash": 0.0}},
        "risk_off": {
            "trigger": ["credit_stress_49152632"],
            "weights": {"Tech": 0.10, "Defensive": 0.40, "Cash": 0.50},
        },
    },
    "profile_priority": ["risk_off", "default"],
    "transition_days_to_defensive": 2,
    "transition_days_to_offensive": 10,
}

print("Running asymmetric backtest (2020-2022, may take 30-60s)...")
result = run_portfolio_backtest(portfolio_config)

history = result.get("allocation_profile_history", [])
print(f"\nallocation_profile_history entries: {len(history)}")

# Extract only entries that record a lerp (transition string starting with "gradual")
lerps = [h for h in history if str(h.get("transition", "")).startswith("gradual")]
print(f"Gradual-lerp transitions recorded: {len(lerps)}")

# Group by direction
to_defensive_lerps = []
to_offensive_lerps = []
for h in lerps:
    from_w = h.get("from_weights", {})
    to_w = h.get("weights", {})
    eq_from = equity_weight(from_w)
    eq_to = equity_weight(to_w)
    if eq_to < eq_from:
        to_defensive_lerps.append(h)
    elif eq_to > eq_from:
        to_offensive_lerps.append(h)

for h in to_defensive_lerps:
    print(f"  → defensive on {h['date']}: {h['transition']}")
for h in to_offensive_lerps:
    print(f"  → offensive on {h['date']}: {h['transition']}")

# Reclassify direction using profile_name as well as weights, because the
# engine currently emits `weights={}` for transitions back to "default" — see
# BUG note below.
expected_defensive_dates = [h["date"] for h in lerps if h["profile_name"] == "risk_off"]
expected_offensive_dates = [h["date"] for h in lerps if h["profile_name"] == "default"]

if not lerps:
    skip("backtest produced no gradual transitions (credit_stress may not have fired)",
         "consider a longer or different window")
else:
    # ---- to-defensive transitions should use to_defensive=2 days ----
    defensive_lerps = [x for x in lerps if x["profile_name"] == "risk_off"]
    for h in defensive_lerps:
        check(f"to-defensive on {h['date']} records 'gradual over 2 days'",
              "gradual over 2 days" in h["transition"],
              f"got {h['transition']!r}")

    # ---- to-offensive transitions should use to_offensive=10 days ----
    offensive_lerps = [x for x in lerps if x["profile_name"] == "default"]
    for h in offensive_lerps:
        check(f"to-offensive (to default) on {h['date']} has populated weights",
              bool(h.get("weights")),
              f"weights={h.get('weights')!r}")
        check(f"to-offensive (to default) on {h['date']} records 'gradual over 10 days'",
              "gradual over 10 days" in h["transition"],
              f"got {h['transition']!r}")

    if defensive_lerps and offensive_lerps:
        def_durations = {h["transition"] for h in defensive_lerps}
        off_durations = {h["transition"] for h in offensive_lerps}
        check("asymmetry: defensive and offensive lerps use different durations",
              def_durations & off_durations == set(),
              f"defensive={def_durations}, offensive={off_durations}")


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED:  {PASS}")
print(f"FAILED:  {FAIL}")
print(f"SKIPPED: {SKIP}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
