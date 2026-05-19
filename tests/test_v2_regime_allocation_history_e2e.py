#!/usr/bin/env python3
"""
V2 regime_history / allocation_history exposure + reconciliation invariants.

Frontend needs three series to render the dynamic-allocation chart:

  1. regime_history       — dense, one row per trading day: what was active
  2. allocation_history   — dense, one row per trading day: what the engine planned
  3. (realized weights are derived client-side from nav_history)

This test asserts:

  • Both series exist, are length-equal to trading_dates, and align 1:1 by date.
  • target_weights always sums to 1.0 (Cash explicit; no forward-fill needed).
  • Sparse `allocation_profile_history` is correctly derived from the dense series.
  • `regime_definitions` is annotated with `label` and `series_label`.
  • Trade ledger reconciles with the planned allocation (the audit invariants).

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend-dev/data/market_dev.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    PYTHONPATH=../scripts python3 test_v2_regime_allocation_history_e2e.py
"""
import copy
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine_v2 import run_portfolio_backtest as run_v2

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} — {detail}")


# ---------------------------------------------------------------------------
# Fixture: dual-sleeve regime-filtered portfolio, 2022 (a year with real
# macro_defensive triggers in the historical macro DB).
# ---------------------------------------------------------------------------
TECH = ["AAPL", "MSFT", "NVDA", "AMD", "AVGO", "INTC", "MU"]
DEFENSIVE = ["KO", "PEP", "WMT", "JNJ", "PG", "VZ", "MO"]

MACRO_DEFENSIVE = {
    "conditions": [
        {"series": "spx_vs_200dma_pct", "operator": "<", "value": -8},
        {"series": "hy_spread_zscore",  "operator": ">", "value": 2.0},
    ],
    "logic": "any",
    "entry_persistence_days": 3,
    "exit_persistence_days": 5,
}


def dual_sleeve_config(start="2022-01-01", end="2022-12-31"):
    return {
        "name": "DualSleeveRegimeTest",
        "sleeves": [
            {
                "label": "Core", "weight": 0.6, "regime_gate": [],
                "strategy_config": {
                    "name": "Core", "universe": {"type": "symbols", "symbols": TECH},
                    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                    "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 5},
                    "rebalancing": {"frequency": "none", "rules": {}},
                    "sizing": {"type": "equal_weight", "max_positions": 5,
                                "initial_allocation": 600_000},
                    "stop_loss": {"type": "drawdown_from_entry", "value": -25,
                                   "cooldown_days": 60},
                    "backtest": {"start": start, "end": end,
                                  "entry_price": "next_close", "slippage_bps": 10},
                },
            },
            {
                "label": "Defensive", "weight": 0.4, "regime_gate": [],
                "strategy_config": {
                    "name": "Def", "universe": {"type": "symbols", "symbols": DEFENSIVE},
                    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                    "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 5},
                    "rebalancing": {"frequency": "none", "rules": {}},
                    "sizing": {"type": "equal_weight", "max_positions": 5,
                                "initial_allocation": 400_000},
                    "stop_loss": {"type": "drawdown_from_entry", "value": -25,
                                   "cooldown_days": 60},
                    "backtest": {"start": start, "end": end,
                                  "entry_price": "next_close", "slippage_bps": 10},
                },
            },
        ],
        "regime_filter": True,
        "regime_definitions": {"macro_defensive": MACRO_DEFENSIVE},
        "allocation_profiles": {
            "default":       {"trigger": [],
                              "weights": {"Core": 0.6, "Defensive": 0.4}},
            "macro_defense": {"trigger": ["macro_defensive"],
                              "weights": {"Core": 0.0, "Defensive": 0.4, "Cash": 0.6}},
        },
        "profile_priority": ["macro_defense", "default"],
        "capital_when_gated_off": "to_cash",
        "backtest": {"start": start, "end": end, "initial_capital": 1_000_000},
    }


# ---------------------------------------------------------------------------
print("\n=== Running v2 backtest with regime + allocation_profiles ===")
result = run_v2(copy.deepcopy(dual_sleeve_config()))

nav_history = result["nav_history"]
regime_history = result.get("regime_history", [])
allocation_history = result.get("allocation_history", [])
allocation_profile_history = result.get("allocation_profile_history", [])
config_out = result.get("config", {})
trades = result["trades"]

print(f"  trading days:               {len(nav_history)}")
print(f"  regime_history rows:        {len(regime_history)}")
print(f"  allocation_history rows:    {len(allocation_history)}")
print(f"  allocation_profile_history: {len(allocation_profile_history)}")
print(f"  trades:                     {len(trades)}")


# ---------------------------------------------------------------------------
print("\n=== 1. Series exist + are length-equal to trading dates ===")
trading_dates = [p["date"] for p in nav_history]
check("regime_history is non-empty", len(regime_history) > 0)
check("allocation_history is non-empty", len(allocation_history) > 0)
check(
    "regime_history length == trading days",
    len(regime_history) == len(trading_dates),
    f"{len(regime_history)} vs {len(trading_dates)}",
)
check(
    "allocation_history length == trading days",
    len(allocation_history) == len(trading_dates),
    f"{len(allocation_history)} vs {len(trading_dates)}",
)


# ---------------------------------------------------------------------------
print("\n=== 2. Date alignment (1:1 zip with nav_history) ===")
mis_r = [
    i for i, (a, b) in enumerate(zip(regime_history, nav_history))
    if a["date"] != b["date"]
]
mis_a = [
    i for i, (a, b) in enumerate(zip(allocation_history, nav_history))
    if a["date"] != b["date"]
]
check("regime_history.date == nav_history.date on every row", len(mis_r) == 0,
      f"mismatch at index {mis_r[:3]}")
check("allocation_history.date == nav_history.date on every row", len(mis_a) == 0,
      f"mismatch at index {mis_a[:3]}")


# ---------------------------------------------------------------------------
print("\n=== 3. target_weights normalization (Cash explicit, sums to 1.0) ===")
bad_sum = []
missing_cash = []
for row in allocation_history:
    tw = row["target_weights"]
    if "Cash" not in tw:
        missing_cash.append(row["date"])
    s = sum(tw.values())
    if abs(s - 1.0) > 1e-6:
        bad_sum.append((row["date"], s))
check("Cash key present on every row", not missing_cash,
      f"missing on {missing_cash[:3]}")
check("target_weights sums to 1.0 on every row", not bad_sum,
      f"bad rows: {bad_sum[:3]}")


# ---------------------------------------------------------------------------
print("\n=== 4. Sleeves are present on every row (even when weight=0) ===")
sleeve_labels = {"Core", "Defensive"}
missing_sleeve = []
for row in allocation_history:
    for lbl in sleeve_labels:
        if lbl not in row["target_weights"]:
            missing_sleeve.append((row["date"], lbl))
check("every sleeve label appears on every row", not missing_sleeve,
      f"missing: {missing_sleeve[:3]}")


# ---------------------------------------------------------------------------
print("\n=== 5. profile_name is set + matches allocation_profiles keys ===")
profile_keys = set(dual_sleeve_config()["allocation_profiles"].keys())
unknown_profiles = [
    row["profile_name"] for row in allocation_history
    if row["profile_name"] not in profile_keys
]
check("profile_name on every row is a known profile", not unknown_profiles,
      f"unknown: {unknown_profiles[:3]}")


# ---------------------------------------------------------------------------
print("\n=== 6. Sparse allocation_profile_history is correctly derived ===")
# Walk dense series and confirm sparse has exactly the transition points.
transitions = []
prev = None
for row in allocation_history:
    if row["profile_name"] != prev:
        transitions.append(row["date"])
        prev = row["profile_name"]
check(
    "sparse length == number of profile transitions in dense series",
    len(allocation_profile_history) == len(transitions),
    f"{len(allocation_profile_history)} vs {len(transitions)}",
)
check(
    "sparse dates == transition dates from dense",
    [e["date"] for e in allocation_profile_history] == transitions,
)
# from_weights on every entry except the initial one
non_initial = allocation_profile_history[1:]
have_from = sum(1 for e in non_initial if "from_weights" in e)
check("sparse entries (after the initial) carry from_weights",
      have_from == len(non_initial),
      f"{have_from} of {len(non_initial)}")


# ---------------------------------------------------------------------------
print("\n=== 7. regime_definitions annotated with display labels ===")
rd = config_out.get("regime_definitions", {})
md = rd.get("macro_defensive", {})
check("macro_defensive has `label`", "label" in md,
      f"keys: {sorted(md.keys())}")
check(
    "macro_defensive.label is human-readable",
    md.get("label") == "Macro Defensive",
    f"got: {md.get('label')!r}",
)
# Every condition has series_label
conds = md.get("conditions", []) or []
unlabeled = [c for c in conds if "series_label" not in c]
check("every condition has `series_label`", not unlabeled,
      f"unlabeled: {unlabeled[:2]}")
# Check the actual rendered labels
labels_by_series = {c["series"]: c.get("series_label") for c in conds}
check(
    "hy_spread_zscore → HY Spread Z-Score",
    labels_by_series.get("hy_spread_zscore") == "HY Spread Z-Score",
    f"got: {labels_by_series.get('hy_spread_zscore')!r}",
)
check(
    "spx_vs_200dma_pct → SPX vs 200-DMA (%)",
    labels_by_series.get("spx_vs_200dma_pct") == "SPX vs 200-DMA (%)",
    f"got: {labels_by_series.get('spx_vs_200dma_pct')!r}",
)


# ---------------------------------------------------------------------------
print("\n=== 8. Trade ledger reconciles with planned allocation ===")
# Invariant A (flow conservation): for each sleeve, sum of trade amounts ±
# matches the change in (open positions cost basis + sleeve cash).
# We test the simpler day-1 case: initial_cash[sleeve] = sum(buys[sleeve, day 1]).
# (Full multi-day conservation is covered by audit suite.)
day_one = trading_dates[0] if trading_dates else None
# Find first day with trades — entry day
buy_amounts_by_sleeve = defaultdict(float)
first_buy_date = None
for t in trades:
    if t["action"] == "BUY":
        if first_buy_date is None:
            first_buy_date = t["date"]
        if t["date"] == first_buy_date:
            buy_amounts_by_sleeve[t["sleeve_label"]] += t["amount"]

# Find allocation_history row for first_buy_date
ah_row = next((r for r in allocation_history if r["date"] == first_buy_date), None)
nav_row = next((r for r in nav_history if r["date"] == first_buy_date), None)
check(
    "allocation_history has a row for the entry day",
    ah_row is not None,
    f"first_buy_date={first_buy_date}",
)
check(
    "nav_history has a row for the entry day",
    nav_row is not None,
    f"first_buy_date={first_buy_date}",
)

if ah_row and nav_row:
    nav_at_entry = nav_row["nav"]
    for lbl in sleeve_labels:
        target_dollars = ah_row["target_weights"].get(lbl, 0.0) * nav_at_entry
        filled = buy_amounts_by_sleeve.get(lbl, 0.0)
        # Drag budget: 10 bps slippage on filled + 1 share at most expensive price.
        # We use an empirical drag ceiling: 2% of target (well above realistic
        # whole-share + slippage drag for this fixture; the v2 audit suite has
        # the tighter analytical bound).
        drag_budget = max(target_dollars * 0.02, 1000.0)
        delta = abs(target_dollars - filled)
        check(
            f"Invariant B — sleeve '{lbl}': |target − filled| ≤ drag budget",
            delta <= drag_budget,
            f"target=${target_dollars:,.0f} filled=${filled:,.0f} "
            f"delta=${delta:,.0f} budget=${drag_budget:,.0f}",
        )


# ---------------------------------------------------------------------------
print("\n=== 9. Realized + cash + leftover = NAV on every day ===")
# Invariant: nav = sum(per_sleeve_positions_value) + cash for every day.
# This is what makes the frontend's realized-weights derivation trustworthy.
bad_days = []
for p in nav_history:
    psv = p.get("per_sleeve_positions_value", {}) or {}
    realized = sum(psv.values()) + p.get("cash", 0.0)
    if abs(realized - p["nav"]) > 0.5:  # 50¢ tolerance for float rounding
        bad_days.append((p["date"], realized, p["nav"]))
check(
    "sum(per_sleeve_positions_value) + cash == nav on every day",
    not bad_days,
    f"bad days: {bad_days[:3]}",
)


# ---------------------------------------------------------------------------
print(f"\n{'='*60}\nPASSED: {PASS}\nFAILED: {FAIL}\n{'='*60}")
sys.exit(0 if FAIL == 0 else 1)
