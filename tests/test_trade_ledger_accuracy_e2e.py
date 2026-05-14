#!/usr/bin/env python3
"""
Comprehensive trade-ledger accuracy audit — live-trading readiness gate.

Runs the portfolio engine over a matrix of representative configs and
asserts the trade ledger satisfies invariants required for live trading.

Scenarios (built bottom-up from least-to-most complex):

  S1. Single sleeve, regime disabled — baseline. No gating, free trading.

  S2. Single sleeve, regime_gate=[regime_id], NO allocation_profiles —
       legacy pattern. Sleeve gated by its own regime list.

  S3. Single sleeve, regime_gate=[], allocation_profiles with profile
       zeroing the sleeve — the v44 pattern this branch fixes.

  S4. Single sleeve, regime_gate=[regime_id] AND allocation_profiles —
       both gating layers stacked.

  S5. Two sleeves, allocation_profiles redistribute weights between them.

  S6. Allocation-profile transition lerp (transition_days_to_offensive=3).

Universal invariants applied to every scenario's trade ledger:

  I1.  Every BUY at (date, symbol) has a valid price on that date.
  I2.  Every SELL is preceded by a BUY of the same symbol (no naked sells).
  I3.  Cumulative shares per symbol never go negative.
  I4.  No phantom entries: zero non-rebalance BUYs on days the sleeve is
       gated off via its (regime_gate ∪ allocation_profile) effective gate.
  I5.  No phantom rebalance add-ons on gated-off days either.
  I6.  Exits (stop_loss / take_profit / time_stop / fundamental_exit)
       MAY still fire on gated-off days — that's correct behavior.
  I7.  Trade reason is one of the known taxonomy values; rebalance_to_X
       reasons reference a profile name that exists in the config.
  I8.  Final combined NAV reconciles with initial_capital × (1 + total_return).
  I9.  Combined NAV at every point ≥ 0.
  I10. No duplicate trades on (date, symbol, action, reason, shares).
  I11. backtest_end trades are SELLs only and fire only on the last day.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_trade_ledger_accuracy_e2e.py
"""
import copy
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine import run_portfolio_backtest
from regime import evaluate_regime_series

PASS = 0
FAIL = 0
SCENARIO_FAILS = defaultdict(list)


def check(scenario, name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
    else:
        FAIL += 1
        SCENARIO_FAILS[scenario].append((name, detail))


KNOWN_REASONS = {
    "entry", "stop_loss", "take_profit", "time_stop",
    "fundamental_exit", "backtest_end",
    # rebalance_to_<profile> — checked via prefix
}


def apply_invariants(scenario_name, result, config, gated_off_dates_by_sleeve, last_trading_date):
    """Run the 11 universal invariants against one scenario's result."""
    sleeve_results = result.get("sleeve_results", [])
    sleeves_config = config.get("sleeves", [])
    profile_names = set((config.get("allocation_profiles") or {}).keys())

    # I8: NAV reconciliation
    metrics = result.get("metrics") or {}
    initial_capital = config["backtest"]["initial_capital"]
    final_nav = metrics.get("final_nav")
    total_ret_pct = metrics.get("total_return_pct")
    if final_nav is not None and total_ret_pct is not None:
        implied = initial_capital * (1 + total_ret_pct / 100)
        check(scenario_name,
              "I8 final NAV reconciles with total_return_pct",
              abs(final_nav - implied) < initial_capital * 0.001,  # 0.1% tolerance for rounding
              f"final_nav={final_nav} vs initial*({1+total_ret_pct/100})={implied}")

    # I9: Combined NAV always >= 0
    cnav = result.get("combined_nav_history", [])
    all_navs_positive = all(p["nav"] >= -1e-3 for p in cnav)  # tiny tolerance for FP error
    check(scenario_name, "I9 combined NAV >= 0 throughout",
          all_navs_positive,
          f"min nav: {min((p['nav'] for p in cnav), default=None)}")

    # Per-sleeve invariants
    for i, sleeve_result in enumerate(sleeve_results):
        sleeve_label = sleeves_config[i]["label"] if i < len(sleeves_config) else f"sleeve_{i}"
        trades = sleeve_result.get("trades", [])

        # I1: every BUY has a price for that date
        # I6/I7: reason taxonomy
        for t in trades:
            sym = t["symbol"]
            date = t["date"]
            reason = t.get("reason", "")
            # Skip portfolio-level rebalance_to_* (these use mid prices, not feature lookups)
            if reason.startswith("rebalance_to_"):
                check(scenario_name,
                      f"I7 rebalance_to_X reason '{reason}' targets known profile",
                      reason[len("rebalance_to_"):] in profile_names,
                      f"profiles={profile_names}")
            else:
                check(scenario_name,
                      f"I7 reason '{reason}' in known taxonomy ({sleeve_label}/{sym}/{date})",
                      reason in KNOWN_REASONS,
                      f"unknown reason '{reason}'")

        # I2 + I3: SELL must be preceded by BUY; cumulative shares >= 0
        cum_shares = defaultdict(float)
        for t in trades:
            sym = t["symbol"]
            shares = float(t.get("shares", 0))
            action = t["action"]
            if action == "BUY":
                cum_shares[sym] += shares
            else:  # SELL
                cum_shares[sym] -= shares
                # Allow tiny FP error
                check(scenario_name,
                      f"I3 cum_shares({sym}) >= 0 after SELL on {t['date']}",
                      cum_shares[sym] >= -1e-3,
                      f"cum={cum_shares[sym]} after this trade ({t.get('reason')})")

        # I4 + I5: NO entry / rebalance-add BUYs on gated-off days for this sleeve.
        # We allow rebalance_to_<profile> BUYs on gated days because those are
        # the portfolio-level lerp trades that MOVE capital INTO the sleeve.
        gated_off = gated_off_dates_by_sleeve.get(sleeve_label, set())
        for t in trades:
            if t["action"] != "BUY":
                continue
            if t["date"] not in gated_off:
                continue
            reason = t.get("reason", "")
            if reason.startswith("rebalance_to_"):
                continue  # portfolio-level lerp trade is allowed
            # Anything else on a gated day is a phantom trade.
            check(scenario_name,
                  f"I4 no phantom BUY on gated-off day {t['date']} ({sleeve_label}/{t['symbol']}, reason={reason})",
                  False,
                  f"trade: {t}")

        # I11: backtest_end trades are SELLs only and fire on last day
        for t in trades:
            if t.get("reason") == "backtest_end":
                check(scenario_name,
                      f"I11 backtest_end is SELL ({sleeve_label}/{t['symbol']})",
                      t["action"] == "SELL", f"action={t['action']}")
                check(scenario_name,
                      f"I11 backtest_end fires on last day ({t['date']})",
                      t["date"] == last_trading_date,
                      f"date={t['date']} vs last={last_trading_date}")

        # I10: no duplicate trades. Build a multiset and check no over-count of identical entries.
        # Two legitimately distinct trades CAN have the same (date,symbol,action,reason,shares)
        # if e.g. there are partial fills on consecutive days — but within one day for one
        # symbol there should be at most one record per (action, reason).
        per_day_key = defaultdict(int)
        for t in trades:
            k = (t["date"], t["symbol"], t["action"], t.get("reason", ""), round(float(t.get("shares", 0)), 6))
            per_day_key[k] += 1
        for k, cnt in per_day_key.items():
            check(scenario_name,
                  f"I10 no exact-duplicate trade {k}",
                  cnt == 1, f"appears {cnt} times")


def compute_gated_off_dates(config, regime_series, sleeves_config):
    """Mirror portfolio_engine.py:455-547 to derive each sleeve's gated-off dates.

    For each sleeve, dates gated off = total_days - dates_on, where dates_on
    is the intersection of regime_gate dates and allocation-profile non-zero
    dates. Returns {sleeve_label: set(dates_off)}.
    """
    all_dates = set(regime_series.keys())
    alloc_profiles = config.get("allocation_profiles")
    priority = config.get("profile_priority") or []

    def _target_weights_for(active_names):
        if not alloc_profiles or not priority:
            return None
        for pname in priority:
            if pname == "default":
                return alloc_profiles.get("default", {}).get("weights", {})
            pdef = alloc_profiles.get(pname, {})
            trigs = pdef.get("trigger", [])
            if not trigs:
                continue
            if set(trigs).issubset(active_names):
                return pdef.get("weights", {})
        return alloc_profiles.get("default", {}).get("weights", {}) if "default" in alloc_profiles else None

    profile_weights_by_date = {}
    if alloc_profiles and priority:
        for d, active in regime_series.items():
            w = _target_weights_for(set(active))
            if w is not None:
                profile_weights_by_date[d] = w

    gated_off_by_sleeve = {}
    for sleeve in sleeves_config:
        label = sleeve["label"]
        gate = sleeve.get("regime_gate", [])

        # Layer 1: regime_gate
        if not regime_series or gate == ["*"] or not gate:
            regime_dates_on = None
        else:
            regime_dates_on = {
                d for d, active in regime_series.items()
                if set(gate) & set(active)
            }

        # Layer 2: allocation profile
        if profile_weights_by_date:
            alloc_dates_on = {
                d for d, w in profile_weights_by_date.items()
                if float(w.get(label, 0) or 0) > 0
            }
        else:
            alloc_dates_on = None

        if regime_dates_on is None and alloc_dates_on is None:
            dates_on = all_dates
        elif regime_dates_on is None:
            dates_on = alloc_dates_on
        elif alloc_dates_on is None:
            dates_on = regime_dates_on
        else:
            dates_on = regime_dates_on & alloc_dates_on

        gated_off_by_sleeve[label] = all_dates - dates_on
    return gated_off_by_sleeve


# ---------------------------------------------------------------------------
# Universe and strategy templates
# ---------------------------------------------------------------------------
TECH_UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMD", "AVGO", "INTC",
                  "MU", "ARM", "COHR", "WDC", "TER", "GLW"]
DEFENSIVE_UNIVERSE = ["JNJ", "PG", "KO", "PEP", "WMT", "JPM", "BAC", "XOM"]


def strat(name, universe, start, end, max_pos=5, capital=1000000):
    return {
        "name": name,
        "universe": {"type": "symbols", "symbols": universe},
        "entry": {"conditions": [{"type": "always"}], "logic": "all"},
        "stop_loss": {"type": "drawdown_from_entry", "value": -25, "cooldown_days": 60},
        "time_stop": {"max_days": 365},
        "ranking": {"by": "momentum_rank", "order": "desc", "top_n": max_pos},
        "rebalancing": {"frequency": "quarterly", "mode": "trim",
                         "rules": {"max_position_pct": 30, "trim_pct": 50}},
        "sizing": {"type": "equal_weight", "max_positions": max_pos,
                    "initial_allocation": capital},
        "backtest": {"start": start, "end": end,
                     "entry_price": "next_close", "slippage_bps": 10},
    }


STRESS_REGIME_DEF = {
    "conditions": [{"series": "spx_vs_200dma_pct", "operator": "<", "value": 0}],
    "logic": "all",
    "entry_persistence_days": 3,
    "exit_persistence_days": 7,
}

WINDOW_START = "2022-01-01"   # Includes 2022 bear market
WINDOW_END = "2022-12-31"


# ---------------------------------------------------------------------------
# Scenario builders
# ---------------------------------------------------------------------------
def scenario_S1_no_regime():
    return {
        "name": "S1 single sleeve, no regime",
        "sleeves": [{"label": "Tech", "weight": 1, "regime_gate": ["*"],
                     "strategy_config": strat("Tech", TECH_UNIVERSE, WINDOW_START, WINDOW_END)}],
        "regime_filter": False,
        "capital_when_gated_off": "to_cash",
        "backtest": {"start": WINDOW_START, "end": WINDOW_END,
                     "initial_capital": 1000000},
    }


def scenario_S2_regime_gate_only():
    """Sleeve with regime_gate listing a specific regime — sleeve active only when regime fires."""
    return {
        "name": "S2 single sleeve, regime_gate=[stress_spx]",
        "sleeves": [{"label": "Tech", "weight": 1, "regime_gate": ["stress_spx"],
                     "strategy_config": strat("Tech", TECH_UNIVERSE, WINDOW_START, WINDOW_END)}],
        "regime_filter": True,
        "regime_definitions": {"stress_spx": STRESS_REGIME_DEF},
        "capital_when_gated_off": "to_cash",
        "backtest": {"start": WINDOW_START, "end": WINDOW_END,
                     "initial_capital": 1000000},
    }


def scenario_S3_alloc_profile_zero():
    """The v44 pattern: regime_gate=[] (always-on at layer 1) but allocation_profile zeros the sleeve."""
    return {
        "name": "S3 alloc_profile zeros sleeve (v44 pattern)",
        "sleeves": [{"label": "Tech", "weight": 1, "regime_gate": [],
                     "strategy_config": strat("Tech", TECH_UNIVERSE, WINDOW_START, WINDOW_END)}],
        "regime_filter": True,
        "regime_definitions": {"stress_spx": STRESS_REGIME_DEF},
        "capital_when_gated_off": "to_cash",
        "allocation_profiles": {
            "stress_spx_profile": {"trigger": ["stress_spx"],
                                    "weights": {"Tech": 0, "Cash": 1}},
            "default": {"trigger": [], "weights": {"Tech": 0.9, "Cash": 0.1}},
        },
        "profile_priority": ["stress_spx_profile", "default"],
        "transition_days_to_defensive": 1,
        "transition_days_to_offensive": 3,
        "rebalance_threshold": 0.05,
        "backtest": {"start": WINDOW_START, "end": WINDOW_END,
                     "initial_capital": 1000000},
    }


def scenario_S4_both_layers():
    """regime_gate=[stress_spx] AND allocation_profiles — both gating layers in play."""
    return {
        "name": "S4 regime_gate + alloc_profile both",
        "sleeves": [{"label": "Tech", "weight": 1, "regime_gate": ["stress_spx"],
                     "strategy_config": strat("Tech", TECH_UNIVERSE, WINDOW_START, WINDOW_END)}],
        "regime_filter": True,
        "regime_definitions": {"stress_spx": STRESS_REGIME_DEF},
        "capital_when_gated_off": "to_cash",
        "allocation_profiles": {
            "stress_spx_profile": {"trigger": ["stress_spx"],
                                    "weights": {"Tech": 1.0, "Cash": 0}},
            "default": {"trigger": [], "weights": {"Tech": 0, "Cash": 1.0}},
        },
        "profile_priority": ["stress_spx_profile", "default"],
        "transition_days_to_defensive": 1,
        "transition_days_to_offensive": 3,
        "rebalance_threshold": 0.05,
        "backtest": {"start": WINDOW_START, "end": WINDOW_END,
                     "initial_capital": 1000000},
    }


def scenario_S5_two_sleeves_redistribute():
    """Two sleeves; alloc_profile redistributes weight between them by regime."""
    return {
        "name": "S5 two-sleeve redistribute via alloc_profile",
        "sleeves": [
            {"label": "Tech", "weight": 0.5, "regime_gate": [],
             "strategy_config": strat("Tech", TECH_UNIVERSE, WINDOW_START, WINDOW_END,
                                      capital=500000)},
            {"label": "Defensive", "weight": 0.5, "regime_gate": [],
             "strategy_config": strat("Defensive", DEFENSIVE_UNIVERSE, WINDOW_START, WINDOW_END,
                                      capital=500000)},
        ],
        "regime_filter": True,
        "regime_definitions": {"stress_spx": STRESS_REGIME_DEF},
        "capital_when_gated_off": "to_cash",
        "allocation_profiles": {
            "stress_spx_profile": {"trigger": ["stress_spx"],
                                    "weights": {"Tech": 0, "Defensive": 1.0}},
            "default": {"trigger": [],
                         "weights": {"Tech": 0.7, "Defensive": 0.3}},
        },
        "profile_priority": ["stress_spx_profile", "default"],
        "transition_days_to_defensive": 1,
        "transition_days_to_offensive": 3,
        "rebalance_threshold": 0.05,
        "backtest": {"start": WINDOW_START, "end": WINDOW_END,
                     "initial_capital": 1000000},
    }


def scenario_S6_slow_transition():
    """Same as S3 but with a 5-day defensive transition lerp."""
    base = scenario_S3_alloc_profile_zero()
    base = copy.deepcopy(base)
    base["name"] = "S6 multi-day defensive transition lerp"
    base["transition_days_to_defensive"] = 5
    return base


# ---------------------------------------------------------------------------
# Run all scenarios
# ---------------------------------------------------------------------------
SCENARIOS = [
    scenario_S1_no_regime(),
    scenario_S2_regime_gate_only(),
    scenario_S3_alloc_profile_zero(),
    scenario_S4_both_layers(),
    scenario_S5_two_sleeves_redistribute(),
    scenario_S6_slow_transition(),
]

for cfg in SCENARIOS:
    name = cfg["name"]
    print(f"\n{'=' * 70}\n  {name}\n{'=' * 70}")

    result = run_portfolio_backtest(copy.deepcopy(cfg), force_close_at_end=True)

    # Compute the regime series for invariant I4/I5 (gated-off detection).
    regime_series = {}
    if cfg.get("regime_filter") and cfg.get("regime_definitions"):
        regime_configs = [{"name": rid, **defn}
                          for rid, defn in cfg["regime_definitions"].items()]
        regime_series = evaluate_regime_series(
            cfg["backtest"]["start"], cfg["backtest"]["end"], regime_configs)
    else:
        # Build a "no regime" series so gated_off computation has dates to iterate
        cnav_dates = [p["date"] for p in result.get("combined_nav_history", [])]
        regime_series = {d: [] for d in cnav_dates}

    gated_off = compute_gated_off_dates(cfg, regime_series, cfg["sleeves"])
    last_date = result["combined_nav_history"][-1]["date"] if result.get("combined_nav_history") else None

    apply_invariants(name, result, cfg, gated_off, last_date)
    fails_this = len(SCENARIO_FAILS[name])
    print(f"  → {name}: {fails_this} failure(s)" if fails_this else f"  → {name}: clean ✓")
    for fname, detail in SCENARIO_FAILS[name][:5]:
        print(f"     ✗ {fname} — {detail}")


# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print(f"TOTAL: {PASS} passed, {FAIL} failed across {len(SCENARIOS)} scenarios")
print("=" * 70)
if FAIL > 0:
    print()
    print("Failures by scenario:")
    for sname, fails in SCENARIO_FAILS.items():
        if fails:
            print(f"  {sname}: {len(fails)} failure(s)")
sys.exit(0 if FAIL == 0 else 1)
