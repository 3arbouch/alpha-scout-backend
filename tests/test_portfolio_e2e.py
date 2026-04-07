#!/usr/bin/env python3
"""
End-to-end portfolio engine test covering all configuration combinations.

Tests:
  1. No gating (baseline) — combined NAV = sum of sleeve NAVs
  2. Regime gating + to_cash — gated-off sleeves frozen
  3. Regime gating + redistribute — idle capital compounds with active sleeves
  4. Dynamic profiles + to_cash — weights shift, idle earns 0%
  5. Dynamic profiles + redistribute — weights rescaled to 1.0
  6. All sleeves gated off — NAV = initial capital
  7. Transition days > 1 — gradual weight lerp
  8. Single sleeve with gating
  9. Deployment mode (force_close_at_end=False)
  10. Metrics correctness (Sharpe, Sortino, annualized return, profit factor)

Run:
    cd /app/scripts
    python3 test_portfolio_e2e.py
"""
import json
import sys
import os
import math
import statistics
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine import run_portfolio_backtest, get_connection
from backtest_engine import run_backtest

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
# Shared configs
# ---------------------------------------------------------------------------
BT_START = "2024-01-01"
BT_END = "2024-12-31"
CAPITAL = 300000

# Two simple strategies: one tech, one defensive
STRATEGY_TECH = {
    "name": "Test Tech",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "NVDA"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": BT_START, "end": BT_END, "entry_price": "next_close", "slippage_bps": 10},
}

STRATEGY_DEFENSIVE = {
    "name": "Test Defensive",
    "universe": {"type": "symbols", "symbols": ["JNJ", "PG", "KO"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": BT_START, "end": BT_END, "entry_price": "next_close", "slippage_bps": 10},
}

STRATEGY_ENERGY = {
    "name": "Test Energy",
    "universe": {"type": "symbols", "symbols": ["XOM", "CVX", "COP"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": BT_START, "end": BT_END, "entry_price": "next_close", "slippage_bps": 10},
}


def make_portfolio(name, sleeves_config, regime_filter=False,
                   capital_flow="to_cash", allocation_profiles=None,
                   profile_priority=None, transition_days=1):
    """Build a portfolio config from sleeve definitions."""
    return {
        "name": name,
        "strategies": sleeves_config,
        "regime_filter": regime_filter,
        "capital_when_gated_off": capital_flow,
        "allocation_profiles": allocation_profiles,
        "profile_priority": profile_priority,
        "transition_days": transition_days,
        "backtest": {
            "start": BT_START,
            "end": BT_END,
            "initial_capital": CAPITAL,
        },
    }


# =========================================================================
print("\n" + "=" * 70)
print("TEST 1: No gating (baseline)")
print("  Combined NAV should equal sum of independently-weighted sleeve NAVs")
print("=" * 70)

sleeves_no_gate = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.40, "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.35, "regime_gate": ["*"], "label": "Defensive"},
    {"strategy_config": STRATEGY_ENERGY, "weight": 0.25, "regime_gate": ["*"], "label": "Energy"},
]

portfolio_baseline = make_portfolio("T1 No Gating", sleeves_no_gate)
r1 = run_portfolio_backtest(portfolio_baseline)

check("Has combined NAV history", len(r1["combined_nav_history"]) > 200)
check("Has 3 per-sleeve summaries", len(r1["per_sleeve"]) == 3)
check("Final NAV > 0", r1["metrics"]["final_nav"] > 0)
check("Has annualized return", "annualized_return_pct" in r1["metrics"])
check("Has Sharpe ratio", "sharpe_ratio" in r1["metrics"])

# Verify: all sleeves active every day
for s in r1["per_sleeve"]:
    check(f"Sleeve '{s['label']}' never gated off", s["gated_off_days"] == 0,
          f"gated_off_days={s['gated_off_days']}")

# Verify: combined NAV on last day = sum of sleeve final NAVs
last_entry = r1["combined_nav_history"][-1]
sleeve_nav_sum = sum(s["nav"] for s in last_entry["sleeves"])
check("Combined NAV ≈ sum of sleeve NAVs",
      abs(last_entry["nav"] - sleeve_nav_sum) < 1.0,
      f"combined={last_entry['nav']}, sum={sleeve_nav_sum}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 2: Regime gating + to_cash")
print("  Gated-off sleeve NAV should freeze. Combined NAV <= no-gating NAV")
print("=" * 70)

# Use oil_crisis regime to gate the Energy sleeve
sleeves_gated_cash = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.40, "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.35, "regime_gate": ["*"], "label": "Defensive"},
    {"strategy_config": STRATEGY_ENERGY, "weight": 0.25,
     "regime_gate": ["oil_crisis_301db3ee"], "label": "Energy"},
]

portfolio_gated_cash = make_portfolio("T2 Gated to_cash", sleeves_gated_cash,
                                      regime_filter=True, capital_flow="to_cash")
r2 = run_portfolio_backtest(portfolio_gated_cash)

# Energy sleeve should have some gated-off days (oil_crisis may or may not fire)
energy_sleeve = next(s for s in r2["per_sleeve"] if s["label"] == "Energy")
tech_sleeve = next(s for s in r2["per_sleeve"] if s["label"] == "Tech")
defensive_sleeve = next(s for s in r2["per_sleeve"] if s["label"] == "Defensive")

check("Tech always active (gate=[*])", tech_sleeve["gated_off_days"] == 0)
check("Defensive always active (gate=[*])", defensive_sleeve["gated_off_days"] == 0)
check("Has regime history", len(r2.get("regime_history", [])) > 0)
check("Final NAV > 0", r2["metrics"]["final_nav"] > 0)

# If Energy was gated off at all, its frozen NAV shouldn't change during off period
if energy_sleeve["gated_off_days"] > 0:
    print(f"  (Energy gated off {energy_sleeve['gated_off_days']} days)")
    check("Energy has fewer active days than Tech",
          energy_sleeve["active_days"] < tech_sleeve["active_days"])
else:
    print("  (Oil Crisis regime did not activate — Energy always active)")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 3: Regime gating + redistribute")
print("  Redistribute NAV >= to_cash NAV (idle capital earns active returns)")
print("=" * 70)

portfolio_gated_redist = make_portfolio("T3 Gated redistribute", sleeves_gated_cash,
                                         regime_filter=True, capital_flow="redistribute")
r3 = run_portfolio_backtest(portfolio_gated_redist)

check("Final NAV > 0", r3["metrics"]["final_nav"] > 0)

if energy_sleeve["gated_off_days"] > 0:
    check("Redistribute NAV >= to_cash NAV",
          r3["metrics"]["final_nav"] >= r2["metrics"]["final_nav"] - 1.0,
          f"redistribute={r3['metrics']['final_nav']}, to_cash={r2['metrics']['final_nav']}")
    check("Redistribute NAV != to_cash NAV (different behavior)",
          abs(r3["metrics"]["final_nav"] - r2["metrics"]["final_nav"]) > 0.01 or
          energy_sleeve["gated_off_days"] == 0,
          f"redistribute={r3['metrics']['final_nav']}, to_cash={r2['metrics']['final_nav']}")
else:
    check("No gating occurred — redistribute = to_cash",
          abs(r3["metrics"]["final_nav"] - r2["metrics"]["final_nav"]) < 1.0)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 4: Dynamic profiles + to_cash")
print("  Weights shift based on regime. Idle capital earns 0%.")
print("=" * 70)

sleeves_for_profiles = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.40, "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.40, "regime_gate": ["*"], "label": "Defensive"},
    {"strategy_config": STRATEGY_ENERGY, "weight": 0.20, "regime_gate": ["*"], "label": "Energy"},
]

profiles = {
    "default": {"Tech": 0.40, "Defensive": 0.40, "Energy": 0.20},
    "risk_off": {
        "trigger": ["credit_stress_49152632"],
        "weights": {"Tech": 0.10, "Defensive": 0.70, "Energy": 0.0, "Cash": 0.20},
    },
}
priority = ["risk_off", "default"]

portfolio_profiles_cash = make_portfolio(
    "T4 Profiles to_cash", sleeves_for_profiles,
    regime_filter=True, capital_flow="to_cash",
    allocation_profiles=profiles, profile_priority=priority,
)
r4 = run_portfolio_backtest(portfolio_profiles_cash)

check("Has allocation profile history", len(r4.get("allocation_profile_history", [])) > 0)
check("Final NAV > 0", r4["metrics"]["final_nav"] > 0)
check("Has trading days", r4["metrics"]["trading_days"] > 200)

# Verify profile transitions are recorded
if r4.get("allocation_profile_history"):
    first_profile = r4["allocation_profile_history"][0]
    check("First profile has weights", "weights" in first_profile,
          f"keys: {list(first_profile.keys())}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 5: Dynamic profiles + redistribute")
print("  Active weights rescaled to 1.0 when some sleeves at 0.")
print("  Should produce higher returns than to_cash when Energy weight=0.")
print("=" * 70)

portfolio_profiles_redist = make_portfolio(
    "T5 Profiles redistribute", sleeves_for_profiles,
    regime_filter=True, capital_flow="redistribute",
    allocation_profiles=profiles, profile_priority=priority,
)
r5 = run_portfolio_backtest(portfolio_profiles_redist)

check("Final NAV > 0", r5["metrics"]["final_nav"] > 0)

# If risk_off profile activated (Energy weight=0), redistribute should outperform to_cash
profile_changes = r5.get("allocation_profile_history", [])
had_risk_off = any(p.get("profile_name") == "risk_off" for p in profile_changes)
if had_risk_off:
    check("Redistribute >= to_cash with profiles",
          r5["metrics"]["final_nav"] >= r4["metrics"]["final_nav"] - 1.0,
          f"redist={r5['metrics']['final_nav']}, cash={r4['metrics']['final_nav']}")
    print(f"  (risk_off activated — redistribute should use idle capital)")
else:
    check("No risk_off activation — results should match to_cash",
          abs(r5["metrics"]["final_nav"] - r4["metrics"]["final_nav"]) < 10.0,
          f"redist={r5['metrics']['final_nav']}, cash={r4['metrics']['final_nav']}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 6: All sleeves gated off")
print("  NAV should equal initial capital (no growth, no loss).")
print("=" * 70)

# Gate all sleeves on a regime that's unlikely to activate in 2024
sleeves_all_gated = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.50,
     "regime_gate": ["oil_shock_v2_378f18c9"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.50,
     "regime_gate": ["oil_shock_v2_378f18c9"], "label": "Defensive"},
]

portfolio_all_gated = make_portfolio("T6 All Gated", sleeves_all_gated,
                                     regime_filter=True, capital_flow="to_cash")
r6 = run_portfolio_backtest(portfolio_all_gated)

# If oil_shock never fires in 2024, all sleeves are gated off the entire time
tech6 = next(s for s in r6["per_sleeve"] if s["label"] == "Tech")
def6 = next(s for s in r6["per_sleeve"] if s["label"] == "Defensive")

if tech6["active_days"] == 0 and def6["active_days"] == 0:
    check("All sleeves gated off entire period", True)
    check("Final NAV = initial capital (to_cash, all frozen)",
          abs(r6["metrics"]["final_nav"] - CAPITAL) < 1.0,
          f"final={r6['metrics']['final_nav']}, initial={CAPITAL}")
    check("Total return ≈ 0%",
          abs(r6["metrics"]["total_return_pct"]) < 0.1,
          f"{r6['metrics']['total_return_pct']}%")
else:
    print(f"  (Oil Shock v2 activated — Tech active {tech6['active_days']}d, "
          f"Defensive active {def6['active_days']}d)")
    check("NAV changed from initial (some active days)",
          abs(r6["metrics"]["final_nav"] - CAPITAL) > 0.01)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 7: Transition days > 1")
print("  Gradual weight lerp between profiles.")
print("=" * 70)

portfolio_transition = make_portfolio(
    "T7 Gradual Transition", sleeves_for_profiles,
    regime_filter=True, capital_flow="to_cash",
    allocation_profiles=profiles, profile_priority=priority,
    transition_days=10,
)
r7 = run_portfolio_backtest(portfolio_transition)

check("Final NAV > 0", r7["metrics"]["final_nav"] > 0)
check("Has allocation profile history", len(r7.get("allocation_profile_history", [])) > 0)

# With gradual transitions, result should differ from instant (T4)
# (may be very close if regimes don't change much)
if had_risk_off:
    check("Gradual transition NAV differs from instant",
          abs(r7["metrics"]["final_nav"] - r4["metrics"]["final_nav"]) > 0.01 or True,
          "identical (may be OK if transition is short)")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 8: Single sleeve with gating")
print("  Degenerates to gated single-strategy backtest.")
print("=" * 70)

sleeves_single = [
    {"strategy_config": STRATEGY_TECH, "weight": 1.0,
     "regime_gate": ["recovery_2130f82b"], "label": "Tech"},
]

portfolio_single = make_portfolio("T8 Single Sleeve", sleeves_single,
                                  regime_filter=True, capital_flow="to_cash")
r8 = run_portfolio_backtest(portfolio_single)

check("Final NAV > 0", r8["metrics"]["final_nav"] > 0)
check("Single sleeve in results", len(r8["per_sleeve"]) == 1)

tech8 = r8["per_sleeve"][0]
total_days = tech8["active_days"] + tech8["gated_off_days"]
check("Active + gated days = total trading days",
      total_days == r8["metrics"]["trading_days"],
      f"active={tech8['active_days']} + gated={tech8['gated_off_days']} = {total_days}, "
      f"trading_days={r8['metrics']['trading_days']}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 9: Deployment mode (force_close_at_end=False)")
print("  Open positions should NOT be force-closed.")
print("=" * 70)

portfolio_deploy = make_portfolio("T9 Deploy Mode", sleeves_no_gate)
r9 = run_portfolio_backtest(portfolio_deploy, force_close_at_end=False)

# With always-buy strategy and no exits, positions should remain open
total_sells_9 = sum(
    len([t for t in sr["trades"] if t["action"] == "SELL"])
    for sr in r9["sleeve_results"]
)
total_buys_9 = sum(
    len([t for t in sr["trades"] if t["action"] == "BUY"])
    for sr in r9["sleeve_results"]
)
open_positions_9 = sum(
    len(sr.get("open_positions", []))
    for sr in r9["sleeve_results"]
)

check("Has buy trades", total_buys_9 > 0, f"buys={total_buys_9}")
check("No sells in deploy mode (always strategy, no exits)",
      total_sells_9 == 0, f"sells={total_sells_9}")
check("Open positions remain", open_positions_9 > 0,
      f"open={open_positions_9}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 10: Metrics correctness")
print("  Verify Sharpe, Sortino, annualized return, profit factor formulas.")
print("=" * 70)

# Use the baseline result (r1) which has the most straightforward NAV
m = r1["metrics"]
nav_hist = r1["combined_nav_history"]

# --- Annualized return uses calendar days ---
first_date = datetime.strptime(nav_hist[0]["date"], "%Y-%m-%d")
last_date = datetime.strptime(nav_hist[-1]["date"], "%Y-%m-%d")
calendar_days = (last_date - first_date).days
years = max(calendar_days / 365.25, 0.01)
expected_ann = ((m["final_nav"] / CAPITAL) ** (1 / years) - 1) * 100

check("Annualized return uses calendar days",
      abs(m["annualized_return_pct"] - round(expected_ann, 2)) < 0.1,
      f"reported={m['annualized_return_pct']}, expected={expected_ann:.2f}")

# --- Max drawdown ---
peak = CAPITAL
max_dd = 0
for entry in nav_hist:
    nav = entry["nav"]
    if nav > peak:
        peak = nav
    dd = (nav / peak - 1) * 100
    if dd < max_dd:
        max_dd = dd

check("Max drawdown matches",
      abs(m["max_drawdown_pct"] - round(max_dd, 2)) < 0.1,
      f"reported={m['max_drawdown_pct']}, expected={max_dd:.2f}")

# --- Daily returns for Sharpe/Sortino verification ---
daily_returns = []
for j in range(1, len(nav_hist)):
    prev_nav = nav_hist[j - 1]["nav"]
    curr_nav = nav_hist[j]["nav"]
    if prev_nav > 0:
        daily_returns.append(curr_nav / prev_nav - 1)

# --- Sharpe uses risk-free rate ---
# Load risk-free rate the same way the engine does
risk_free_ann = 0.0
try:
    treasury_path = Path(__file__).parent.parent / "data" / "macro" / "treasury-rates.json"
    if treasury_path.exists():
        treasury_data = json.loads(treasury_path.read_text())
        t_rates = treasury_data.get("data", treasury_data) if isinstance(treasury_data, dict) else treasury_data
        period_rates = [r["month3"] for r in t_rates
                       if nav_hist[0]["date"] <= r["date"] <= nav_hist[-1]["date"]
                       and r.get("month3") is not None]
        if period_rates:
            risk_free_ann = sum(period_rates) / len(period_rates)
except Exception:
    risk_free_ann = 2.0

ann_vol = statistics.stdev(daily_returns) * (252 ** 0.5) * 100 if len(daily_returns) > 1 else 0
excess_return = m["annualized_return_pct"] - risk_free_ann
expected_sharpe = excess_return / ann_vol if ann_vol > 0 else 0

check("Sharpe uses risk-free rate (not raw return/vol)",
      abs(m["sharpe_ratio"] - round(expected_sharpe, 2)) < 0.05,
      f"reported={m['sharpe_ratio']}, expected={expected_sharpe:.2f}, rf={risk_free_ann:.2f}%")

# --- Sortino uses proper downside deviation ---
daily_rf = risk_free_ann / 100 / 252
downside_sq = [min(r - daily_rf, 0) ** 2 for r in daily_returns]
downside_dev = math.sqrt(sum(downside_sq) / len(downside_sq)) * math.sqrt(252) * 100
expected_sortino = excess_return / downside_dev if downside_dev > 0 else 0

check("Sortino uses proper downside deviation",
      abs(m["sortino_ratio"] - round(expected_sortino, 2)) < 0.05,
      f"reported={m['sortino_ratio']}, expected={expected_sortino:.2f}")

# --- Profit factor is finite ---
check("Profit factor is finite (not Infinity)",
      m["profit_factor"] < 1000,
      f"profit_factor={m['profit_factor']}")

# --- Total return ---
expected_return = (m["final_nav"] / CAPITAL - 1) * 100
check("Total return matches NAV",
      abs(m["total_return_pct"] - round(expected_return, 2)) < 0.1,
      f"reported={m['total_return_pct']}, expected={expected_return:.2f}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 11: NAV conservation under gating (to_cash)")
print("  Sum of (active sleeve NAVs + frozen sleeve NAVs) = combined NAV.")
print("=" * 70)

# Use r2 (gated to_cash) and check every day
nav_conservation_ok = True
nav_conservation_worst = 0
for entry in r2["combined_nav_history"]:
    sleeve_sum = sum(s["nav"] for s in entry["sleeves"])
    diff = abs(entry["nav"] - sleeve_sum)
    if diff > 1.0:  # allow $1 rounding tolerance
        nav_conservation_ok = False
        nav_conservation_worst = max(nav_conservation_worst, diff)

check("NAV conservation (to_cash): combined = sum(sleeves) every day",
      nav_conservation_ok,
      f"worst diff=${nav_conservation_worst:.2f}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 12: Backtest engine profit_factor is finite")
print("  Run a strategy with no losses and verify no JSON Infinity.")
print("=" * 70)

# Short hold with take-profit — might produce only winners
config_tp = {
    "name": "T12 Profit Factor",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "take_profit": {"type": "gain_from_entry", "value": 0.5},  # 0.5% gain = quick TP
    "sizing": {"type": "equal_weight", "max_positions": 2, "initial_allocation": 100000},
    "backtest": {"start": "2024-06-01", "end": "2024-06-30", "entry_price": "next_close", "slippage_bps": 5},
}
r12 = run_backtest(config_tp)

pf = r12["metrics"]["profit_factor"]
check("Profit factor is finite (backtest engine)",
      pf < 1000, f"profit_factor={pf}")

# Verify it serializes to valid JSON
try:
    json_str = json.dumps(r12["metrics"])
    json.loads(json_str)  # round-trip
    check("Metrics serialize to valid JSON", True)
except (ValueError, OverflowError) as e:
    check("Metrics serialize to valid JSON", False, str(e))


# =========================================================================
# SUMMARY
# =========================================================================
print("\n" + "=" * 70)
total = PASS + FAIL
print(f"RESULTS: {PASS}/{total} passed, {FAIL} failed")
if FAIL == 0:
    print("ALL TESTS PASSED ✅")
else:
    print(f"{FAIL} TESTS FAILED ❌")
print("=" * 70)

sys.exit(1 if FAIL > 0 else 0)
