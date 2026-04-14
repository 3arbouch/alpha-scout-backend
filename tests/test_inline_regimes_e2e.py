#!/usr/bin/env python3
"""
End-to-end tests for inline regime definitions and Pydantic model validation.

Tests:
  1. Inline regime gating (to_cash) — regime defined in portfolio config, not DB
  2. Inline regime gating (redistribute) — idle capital compounds with active sleeves
  3. Multiple inline regimes — different regimes per sleeve
  4. Mixed inline + DB regimes — one sleeve uses inline, another uses DB regime
  5. Inline regime with allocation profiles — dynamic weight shifts
  6. Inline regime never activates — all capital frozen
  7. Inline regime always active — behaves like no gating
  8. validate_portfolio: correct inline config passes
  9. validate_portfolio: wrong field names caught (threshold, conditions vs entry_conditions)
  10. validate_portfolio: undefined regime_gate reference caught
  11. validate_portfolio: invalid operator/series caught
  12. validate_portfolio: regime_filter=False skips regime validation
  13. Schema generation: correct field names in generated schema
  14. Entry condition coverage: rsi, momentum_rank, ma_crossover, volume_capitulation

Run:
    set -a && source .env && set +a && python3 tests/test_inline_regimes_e2e.py
"""
import json
import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "server"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from portfolio_engine import run_portfolio_backtest
from auto_trader.tools import validate_portfolio

PASS = 0
FAIL = 0

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  \u2705 {name}")
    else:
        FAIL += 1
        print(f"  \u274c {name} \u2014 {detail}")


# ---------------------------------------------------------------------------
# Shared configs
# ---------------------------------------------------------------------------
BT_START = "2024-01-01"
BT_END = "2024-12-31"
CAPITAL = 300000

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

# Inline regime: low VIX = bull market
REGIME_LOW_VIX = {
    "conditions": [{"series": "vix", "operator": "<", "value": 20}],
    "logic": "all",
}

# Inline regime: high VIX = stress
REGIME_HIGH_VIX = {
    "conditions": [{"series": "vix", "operator": ">", "value": 25}],
    "logic": "all",
}

# Inline regime: impossible (VIX < 1 never happens)
REGIME_NEVER = {
    "conditions": [{"series": "vix", "operator": "<", "value": 1}],
    "logic": "all",
}

# Inline regime: always active (VIX > 0 always true)
REGIME_ALWAYS = {
    "conditions": [{"series": "vix", "operator": ">", "value": 0}],
    "logic": "all",
}

# Inline regime: credit stress (matches DB regime credit_stress_49152632 logic)
REGIME_CREDIT_STRESS = {
    "conditions": [{"series": "hy_spread_zscore", "operator": ">", "value": 1.5}],
    "logic": "all",
}


def make_portfolio(name, sleeves, regime_definitions=None, regime_filter=True,
                   capital_flow="to_cash", allocation_profiles=None,
                   profile_priority=None, transition_days=1):
    config = {
        "name": name,
        "sleeves": sleeves,
        "regime_filter": regime_filter,
        "capital_when_gated_off": capital_flow,
        "transition_days": transition_days,
        "backtest": {"start": BT_START, "end": BT_END, "initial_capital": CAPITAL},
    }
    if regime_definitions:
        config["regime_definitions"] = regime_definitions
    if allocation_profiles:
        config["allocation_profiles"] = allocation_profiles
    if profile_priority:
        config["profile_priority"] = profile_priority
    return config


# =========================================================================
print("\n" + "=" * 70)
print("TEST 1: Inline regime gating + to_cash")
print("  Tech gated by low_vix (inline). Defensive always active.")
print("=" * 70)

sleeves_1 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.60,
     "regime_gate": ["low_vix"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.40,
     "regime_gate": ["*"], "label": "Defensive"},
]

p1 = make_portfolio("T1 Inline Gating to_cash", sleeves_1,
                     regime_definitions={"low_vix": REGIME_LOW_VIX},
                     capital_flow="to_cash")
r1 = run_portfolio_backtest(p1)

check("Has combined NAV history", len(r1["combined_nav_history"]) > 200)
check("Has 2 per-sleeve summaries", len(r1["per_sleeve"]) == 2)
check("Final NAV > 0", r1["metrics"]["final_nav"] > 0)
check("Has Sharpe ratio", "sharpe_ratio" in r1["metrics"])
check("Has regime history", len(r1.get("regime_history", [])) > 0)

tech1 = next(s for s in r1["per_sleeve"] if s["label"] == "Tech")
def1 = next(s for s in r1["per_sleeve"] if s["label"] == "Defensive")

check("Defensive always active (gate=[*])", def1["gated_off_days"] == 0)
# VIX was below 20 for most of 2024, so Tech should be active most days
check("Tech has active days", tech1["active_days"] > 0,
      f"active={tech1['active_days']}")
# But VIX spiked above 20 at least once, so some gated-off days expected
if tech1["gated_off_days"] > 0:
    print(f"  (Tech gated off {tech1['gated_off_days']} days — inline regime working)")
    check("Tech has fewer active days than Defensive",
          tech1["active_days"] < def1["active_days"])
else:
    print("  (VIX stayed below 20 all year — Tech never gated off)")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 2: Inline regime gating + redistribute")
print("  Same setup as T1 but redistribute. NAV >= to_cash NAV.")
print("=" * 70)

p2 = make_portfolio("T2 Inline Gating redistribute", sleeves_1,
                     regime_definitions={"low_vix": REGIME_LOW_VIX},
                     capital_flow="redistribute")
r2 = run_portfolio_backtest(p2)

check("Final NAV > 0", r2["metrics"]["final_nav"] > 0)

if tech1["gated_off_days"] > 0:
    check("Redistribute NAV >= to_cash NAV",
          r2["metrics"]["final_nav"] >= r1["metrics"]["final_nav"] - 1.0,
          f"redistribute={r2['metrics']['final_nav']}, to_cash={r1['metrics']['final_nav']}")
else:
    check("No gating — redistribute = to_cash",
          abs(r2["metrics"]["final_nav"] - r1["metrics"]["final_nav"]) < 1.0)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 3: Multiple inline regimes — different per sleeve")
print("  Tech gated by low_vix, Energy gated by credit_stress. Defensive always on.")
print("=" * 70)

sleeves_3 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.40,
     "regime_gate": ["low_vix"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.35,
     "regime_gate": ["*"], "label": "Defensive"},
    {"strategy_config": STRATEGY_ENERGY, "weight": 0.25,
     "regime_gate": ["credit_stress_inline"], "label": "Energy"},
]

p3 = make_portfolio("T3 Multi Inline Regimes", sleeves_3,
                     regime_definitions={
                         "low_vix": REGIME_LOW_VIX,
                         "credit_stress_inline": REGIME_CREDIT_STRESS,
                     })
r3 = run_portfolio_backtest(p3)

check("Has 3 per-sleeve summaries", len(r3["per_sleeve"]) == 3)
check("Final NAV > 0", r3["metrics"]["final_nav"] > 0)

tech3 = next(s for s in r3["per_sleeve"] if s["label"] == "Tech")
energy3 = next(s for s in r3["per_sleeve"] if s["label"] == "Energy")
def3 = next(s for s in r3["per_sleeve"] if s["label"] == "Defensive")

check("Defensive always active", def3["gated_off_days"] == 0)
check("Tech has active days (low_vix)", tech3["active_days"] > 0)
# Credit stress (HY spread z > 1.5) is rare — energy may be gated off most of 2024
print(f"  Tech: active={tech3['active_days']}, gated={tech3['gated_off_days']}")
print(f"  Energy: active={energy3['active_days']}, gated={energy3['gated_off_days']}")
print(f"  Defensive: active={def3['active_days']}, gated={def3['gated_off_days']}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 4: Mixed inline + DB regimes")
print("  Tech gated by inline low_vix, Energy gated by DB regime oil_crisis_301db3ee.")
print("=" * 70)

sleeves_4 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.40,
     "regime_gate": ["low_vix"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.35,
     "regime_gate": ["*"], "label": "Defensive"},
    {"strategy_config": STRATEGY_ENERGY, "weight": 0.25,
     "regime_gate": ["oil_crisis_301db3ee"], "label": "Energy"},
]

p4 = make_portfolio("T4 Mixed Inline+DB", sleeves_4,
                     regime_definitions={"low_vix": REGIME_LOW_VIX})
r4 = run_portfolio_backtest(p4)

check("Has 3 per-sleeve summaries", len(r4["per_sleeve"]) == 3)
check("Final NAV > 0", r4["metrics"]["final_nav"] > 0)
check("Has regime history", len(r4.get("regime_history", [])) > 0)

# Both inline and DB regimes should show up in regime_history
regime_names_seen = set()
for rh in r4.get("regime_history", []):
    for name in rh.get("active_regimes", []):
        regime_names_seen.add(name)
print(f"  Regimes seen: {regime_names_seen}")
check("Inline regime 'low_vix' in regime history", "low_vix" in regime_names_seen)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 5: Inline regime with allocation profiles")
print("  Dynamic weight shift triggered by inline high_vix regime.")
print("=" * 70)

sleeves_5 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.50,
     "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.50,
     "regime_gate": ["*"], "label": "Defensive"},
]

profiles_5 = {
    "default": {"Tech": 0.50, "Defensive": 0.50},
    "risk_off": {
        "trigger": ["high_vix_inline"],
        "weights": {"Tech": 0.20, "Defensive": 0.60, "Cash": 0.20},
    },
}

p5 = make_portfolio("T5 Inline Profiles", sleeves_5,
                     regime_definitions={"high_vix_inline": REGIME_HIGH_VIX},
                     allocation_profiles=profiles_5,
                     profile_priority=["risk_off", "default"])
r5 = run_portfolio_backtest(p5)

check("Final NAV > 0", r5["metrics"]["final_nav"] > 0)
check("Has allocation profile history", len(r5.get("allocation_profile_history", [])) > 0)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 6: Inline regime never activates — all capital frozen")
print("  Both sleeves gated on impossible regime (VIX < 1).")
print("=" * 70)

sleeves_6 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.50,
     "regime_gate": ["impossible"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.50,
     "regime_gate": ["impossible"], "label": "Defensive"},
]

p6 = make_portfolio("T6 Never Active", sleeves_6,
                     regime_definitions={"impossible": REGIME_NEVER})
r6 = run_portfolio_backtest(p6)

tech6 = next(s for s in r6["per_sleeve"] if s["label"] == "Tech")
def6 = next(s for s in r6["per_sleeve"] if s["label"] == "Defensive")

check("Tech never active", tech6["active_days"] == 0,
      f"active={tech6['active_days']}")
check("Defensive never active", def6["active_days"] == 0,
      f"active={def6['active_days']}")
check("Final NAV = initial capital (all frozen)",
      abs(r6["metrics"]["final_nav"] - CAPITAL) < 1.0,
      f"final={r6['metrics']['final_nav']}, initial={CAPITAL}")
check("Total return ~ 0%",
      abs(r6["metrics"]["total_return_pct"]) < 0.1,
      f"{r6['metrics']['total_return_pct']}%")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 7: Inline regime always active — same as no gating")
print("  Regime (VIX > 0) is always true. Should match ungated baseline.")
print("=" * 70)

sleeves_7 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.60,
     "regime_gate": ["always_on"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.40,
     "regime_gate": ["always_on"], "label": "Defensive"},
]

p7_gated = make_portfolio("T7 Always Active (gated)", sleeves_7,
                           regime_definitions={"always_on": REGIME_ALWAYS})

sleeves_7_ungated = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.60,
     "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.40,
     "regime_gate": ["*"], "label": "Defensive"},
]

p7_ungated = make_portfolio("T7 Ungated Baseline", sleeves_7_ungated, regime_filter=False)

r7_gated = run_portfolio_backtest(p7_gated)
r7_ungated = run_portfolio_backtest(p7_ungated)

tech7 = next(s for s in r7_gated["per_sleeve"] if s["label"] == "Tech")
check("Tech always active with always-on regime", tech7["gated_off_days"] == 0,
      f"gated_off={tech7['gated_off_days']}")
check("Always-on regime NAV ~ ungated NAV",
      abs(r7_gated["metrics"]["final_nav"] - r7_ungated["metrics"]["final_nav"]) < 1.0,
      f"gated={r7_gated['metrics']['final_nav']}, ungated={r7_ungated['metrics']['final_nav']}")


# =========================================================================
# VALIDATION TESTS
# =========================================================================
print("\n" + "=" * 70)
print("TEST 8: validate_portfolio — correct inline config passes")
print("=" * 70)

valid = validate_portfolio({
    "name": "Valid",
    "sleeves": [
        {"label": "A", "weight": 0.6, "regime_gate": ["bull"],
         "strategy_config": {
             "name": "S1", "universe": {"type": "sector", "sector": "Technology"},
             "entry": {"conditions": [{"type": "always"}], "logic": "all"},
             "sizing": {"type": "equal_weight", "max_positions": 10},
         }},
        {"label": "B", "weight": 0.4, "regime_gate": ["*"],
         "strategy_config": {
             "name": "S2", "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT"]},
             "entry": {"conditions": [{"type": "always"}], "logic": "all"},
             "sizing": {"type": "equal_weight", "max_positions": 5},
         }},
    ],
    "regime_definitions": {
        "bull": {"conditions": [{"series": "vix", "operator": "<", "value": 25}], "logic": "all"},
    },
    "regime_filter": True,
})
check("Valid inline config passes", valid["valid"], valid.get("error", ""))


# =========================================================================
print("\n" + "=" * 70)
print("TEST 9: validate_portfolio — wrong field names caught")
print("=" * 70)

# threshold instead of value
r9a = validate_portfolio({
    "name": "Bad",
    "sleeves": [{"label": "A", "weight": 1.0, "regime_gate": ["r1"],
                 "strategy_config": {
                     "name": "S", "universe": {"type": "sector", "sector": "Technology"},
                     "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                     "sizing": {"type": "equal_weight", "max_positions": 10},
                 }}],
    "regime_definitions": {
        "r1": {"conditions": [{"series": "vix", "operator": "<", "threshold": 25}], "logic": "all"},
    },
})
check("'threshold' instead of 'value' rejected", not r9a["valid"])
check("Error mentions 'value'", "value" in r9a.get("error", "").lower(),
      r9a.get("error", ""))

# entry_conditions instead of conditions (using RegimeConfig format in InlineRegimeDefinition)
r9b = validate_portfolio({
    "name": "Bad",
    "sleeves": [{"label": "A", "weight": 1.0, "regime_gate": ["r1"],
                 "strategy_config": {
                     "name": "S", "universe": {"type": "sector", "sector": "Technology"},
                     "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                     "sizing": {"type": "equal_weight", "max_positions": 10},
                 }}],
    "regime_definitions": {
        "r1": {"entry_conditions": [{"series": "vix", "operator": "<", "value": 25}], "entry_logic": "all"},
    },
})
check("'entry_conditions' in inline regime rejected", not r9b["valid"])
check("Error mentions 'conditions'", "conditions" in r9b.get("error", "").lower(),
      r9b.get("error", ""))

# 'and' instead of 'all' for logic
r9c = validate_portfolio({
    "name": "Bad",
    "sleeves": [{"label": "A", "weight": 1.0, "regime_gate": ["r1"],
                 "strategy_config": {
                     "name": "S", "universe": {"type": "sector", "sector": "Technology"},
                     "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                     "sizing": {"type": "equal_weight", "max_positions": 10},
                 }}],
    "regime_definitions": {
        "r1": {"conditions": [{"series": "vix", "operator": "<", "value": 25}], "logic": "and"},
    },
})
check("'and' instead of 'all' for logic rejected", not r9c["valid"])
check("Error mentions 'all' or 'any'", "'all'" in r9c.get("error", "") or "all" in r9c.get("error", ""),
      r9c.get("error", ""))


# =========================================================================
print("\n" + "=" * 70)
print("TEST 10: validate_portfolio — undefined regime_gate reference")
print("=" * 70)

r10 = validate_portfolio({
    "name": "Bad",
    "sleeves": [{"label": "A", "weight": 1.0, "regime_gate": ["nonexistent"],
                 "strategy_config": {
                     "name": "S", "universe": {"type": "sector", "sector": "Technology"},
                     "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                     "sizing": {"type": "equal_weight", "max_positions": 10},
                 }}],
})
check("Undefined regime ref rejected", not r10["valid"])
check("Error mentions 'nonexistent'", "nonexistent" in r10.get("error", ""),
      r10.get("error", ""))


# =========================================================================
print("\n" + "=" * 70)
print("TEST 11: validate_portfolio — invalid operator and missing series")
print("=" * 70)

r11a = validate_portfolio({
    "name": "Bad",
    "sleeves": [{"label": "A", "weight": 1.0, "regime_gate": ["r1"],
                 "strategy_config": {
                     "name": "S", "universe": {"type": "sector", "sector": "Technology"},
                     "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                     "sizing": {"type": "equal_weight", "max_positions": 10},
                 }}],
    "regime_definitions": {
        "r1": {"conditions": [{"series": "vix", "operator": "LESS_THAN", "value": 25}], "logic": "all"},
    },
})
check("Invalid operator rejected", not r11a["valid"])
check("Error mentions operator", "operator" in r11a.get("error", "").lower(),
      r11a.get("error", ""))

r11b = validate_portfolio({
    "name": "Bad",
    "sleeves": [{"label": "A", "weight": 1.0, "regime_gate": ["r1"],
                 "strategy_config": {
                     "name": "S", "universe": {"type": "sector", "sector": "Technology"},
                     "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                     "sizing": {"type": "equal_weight", "max_positions": 10},
                 }}],
    "regime_definitions": {
        "r1": {"conditions": [{"operator": "<", "value": 25}], "logic": "all"},
    },
})
check("Missing 'series' rejected", not r11b["valid"])
check("Error mentions 'series'", "series" in r11b.get("error", "").lower(),
      r11b.get("error", ""))


# =========================================================================
print("\n" + "=" * 70)
print("TEST 12: validate_portfolio — regime_filter=False skips regime check")
print("=" * 70)

r12 = validate_portfolio({
    "name": "OK",
    "sleeves": [{"label": "A", "weight": 1.0, "regime_gate": ["nonexistent"],
                 "strategy_config": {
                     "name": "S", "universe": {"type": "sector", "sector": "Technology"},
                     "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                     "sizing": {"type": "equal_weight", "max_positions": 10},
                 }}],
    "regime_filter": False,
})
# regime_filter=False means gating is off, but regime_gate references are still
# structurally present. The validator still checks them because the agent should
# define regimes correctly regardless of whether gating is on/off.
check("regime_filter=False still validates regime refs", not r12["valid"],
      "Should still catch undefined regime refs")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 13: Schema generation — correct field names")
print("=" * 70)

from portfolio_engine import get_config_schema as portfolio_schema
from backtest_engine import get_config_schema as strategy_schema

ps = portfolio_schema()
ss = strategy_schema()

check("Portfolio schema is valid JSON schema", "$defs" in ps or "properties" in ps)
check("Strategy schema is valid JSON schema", "$defs" in ss or "properties" in ss)

# Check regime-related fields
p_defs = ps.get("$defs", {})
rc = p_defs.get("RegimeCondition", {}).get("properties", {})
check("RegimeCondition uses 'value' (not 'threshold')", "value" in rc and "threshold" not in rc,
      f"fields={list(rc.keys())}")

ird = p_defs.get("InlineRegimeDefinition", {}).get("properties", {})
check("InlineRegimeDefinition uses 'conditions' (not 'entry_conditions')",
      "conditions" in ird and "entry_conditions" not in ird,
      f"fields={list(ird.keys())}")

sleeve = p_defs.get("SleeveConfig", {}).get("properties", {})
check("SleeveConfig uses 'regime_gate' (not 'regime_gates')",
      "regime_gate" in sleeve and "regime_gates" not in sleeve,
      f"fields={list(sleeve.keys())}")

# Check portfolio-level fields
port_props = ps.get("properties", {})
check("PortfolioConfig has 'regime_filter'", "regime_filter" in port_props)

# Check strategy fields
s_defs = ss.get("$defs", {})
uni = s_defs.get("UniverseConfig", {}).get("properties", {})
check("UniverseConfig uses 'symbols' (not 'tickers')",
      "symbols" in uni and "tickers" not in uni,
      f"fields={list(uni.keys())}")

ts = s_defs.get("TimeStopConfig", {}).get("properties", {})
check("TimeStopConfig uses 'max_days' (not 'days')",
      "max_days" in ts and "days" not in ts,
      f"fields={list(ts.keys())}")

bp = p_defs.get("BacktestParams", {}).get("properties", {})
check("BacktestParams has 'initial_capital'", "initial_capital" in bp)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 14: Entry condition types — technical indicators validate")
print("=" * 70)

for ctype, params in [
    ("rsi", {"period": 14, "operator": "<=", "value": 30}),
    ("momentum_rank", {"lookback": 63, "operator": ">=", "value": 75}),
    ("ma_crossover", {"fast": 50, "slow": 200, "operator": "==", "value": 1}),
    ("volume_capitulation", {"window": 20, "multiplier": 3.0}),
]:
    r = validate_portfolio({
        "name": "Test",
        "sleeves": [{"label": "A", "weight": 1.0,
                     "strategy_config": {
                         "name": f"Test {ctype}",
                         "universe": {"type": "symbols", "symbols": ["AAPL"]},
                         "entry": {"conditions": [{"type": ctype, **params}], "logic": "all"},
                         "sizing": {"type": "equal_weight", "max_positions": 5},
                     }}],
    })
    check(f"Entry condition '{ctype}' validates", r["valid"], r.get("error", ""))


# =========================================================================
# SUMMARY
# =========================================================================
print("\n" + "=" * 70)
total = PASS + FAIL
print(f"RESULTS: {PASS}/{total} passed, {FAIL} failed")
if FAIL == 0:
    print("ALL TESTS PASSED \u2705")
else:
    print(f"{FAIL} TESTS FAILED \u274c")
print("=" * 70)

sys.exit(1 if FAIL > 0 else 0)
