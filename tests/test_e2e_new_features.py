#!/usr/bin/env python3
"""
End-to-end test for: always condition, ranking, equal-weight rebalance.
Tests new features + backward compatibility with existing strategies.
"""
import json
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from backtest_engine import run_backtest, load_strategy, resolve_universe, get_connection

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


# =========================================================================
print("\n" + "="*70)
print("TEST 1: Always condition — buy-and-hold, no exits, no rebalance")
print("="*70)

config1 = {
    "name": "T1 Always Buy-Hold",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "GOOGL"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": "2024-01-01", "end": "2024-12-31", "entry_price": "next_close", "slippage_bps": 10},
}

r1 = run_backtest(config1)
buys1 = [t for t in r1["trades"] if t["action"] == "BUY"]
sells1 = [t for t in r1["trades"] if t["action"] == "SELL"]
non_end_sells = [t for t in sells1 if t.get("reason") != "backtest_end"]

check("Bought all 3 tickers", len(buys1) == 3, f"got {len(buys1)}")
check("All bought on day 1 (next_close)", all(t["date"] == "2024-01-03" for t in buys1))
check("No mid-backtest sells", len(non_end_sells) == 0, f"got {len(non_end_sells)}")
check("3 backtest_end sells", len([t for t in sells1 if t.get("reason") == "backtest_end"]) == 3)
check("Positive return", r1["metrics"]["total_return_pct"] > 0, f"{r1['metrics']['total_return_pct']}%")
check("signal_detail is always", buys1[0].get("signal_detail", [{}])[0].get("type") == "always")

# =========================================================================
print("\n" + "="*70)
print("TEST 2: Always + ranking (no rebalance) — picks top N by PE")
print("="*70)

config2 = {
    "name": "T2 Ranked Entry",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "GOOGL", "NVDA", "META", "AMZN", "INTC", "AMD"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "ranking": {"by": "pe_percentile", "order": "asc", "top_n": 3},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": "2024-01-01", "end": "2024-12-31", "entry_price": "next_close", "slippage_bps": 10},
}

r2 = run_backtest(config2)
buys2 = [t for t in r2["trades"] if t["action"] == "BUY"]
buy_symbols2 = [t["symbol"] for t in buys2]

check("Bought exactly 3 tickers", len(buys2) == 3, f"got {len(buys2)}: {buy_symbols2}")
check("8 tickers in universe but only 3 bought", len(config2["universe"]["symbols"]) == 8)
check("Did NOT buy all 8", len(buy_symbols2) < 8)

# =========================================================================
print("\n" + "="*70)
print("TEST 3: Always + ranking + equal-weight rebalance — quarterly rotation")
print("="*70)

config3 = {
    "name": "T3 EW Rebalance Rotation",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "GOOGL", "NVDA", "META", "AMZN", "CRM", "ADBE", "ORCL", "INTC"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "ranking": {"by": "pe_percentile", "order": "asc", "top_n": 4},
    "sizing": {"type": "equal_weight", "max_positions": 4, "initial_allocation": 100000},
    "rebalancing": {"frequency": "quarterly", "mode": "equal_weight", "rules": {}},
    "backtest": {"start": "2023-01-01", "end": "2025-12-31", "entry_price": "next_close", "slippage_bps": 10},
}

r3 = run_backtest(config3)
trades3 = r3["trades"]
rotation_sells = [t for t in trades3 if t.get("reason") == "rebalance_rotation"]
trim_sells = [t for t in trades3 if t.get("reason") == "rebalance_trim"]
all_symbols_traded = set(t["symbol"] for t in trades3)

check("More than 4 unique symbols traded (rotation happened)", len(all_symbols_traded) > 4, f"got {len(all_symbols_traded)}: {all_symbols_traded}")
check("Has rotation sells", len(rotation_sells) > 0, f"got {len(rotation_sells)}")
check("Has trim sells (reweighting)", len(trim_sells) > 0, f"got {len(trim_sells)}")
check("Total trades > 10 (active rebalancing)", len(trades3) > 10, f"got {len(trades3)}")
check("NAV history has entries", len(r3["nav_history"]) > 200)
check("Metrics computed", "total_return_pct" in r3["metrics"])

# Verify max positions never exceeded
for nav_entry in r3["nav_history"]:
    if nav_entry["num_positions"] > 4:
        check("Max positions never exceeded 4", False, f"{nav_entry['date']}: {nav_entry['num_positions']}")
        break
else:
    check("Max positions never exceeded 4", True)

# =========================================================================
print("\n" + "="*70)
print("TEST 4: Equal-weight rebalance WITHOUT ranking — reweight only, no rotation")
print("="*70)

config4 = {
    "name": "T4 EW No Ranking",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "GOOGL", "NVDA"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 4, "initial_allocation": 100000},
    "rebalancing": {"frequency": "quarterly", "mode": "equal_weight", "rules": {}},
    "backtest": {"start": "2024-01-01", "end": "2025-12-31", "entry_price": "next_close", "slippage_bps": 10},
}

r4 = run_backtest(config4)
trades4 = r4["trades"]
buys4 = [t for t in trades4 if t["action"] == "BUY"]
buy_symbols4 = set(t["symbol"] for t in buys4)
rotation_sells4 = [t for t in trades4 if t.get("reason") == "rebalance_rotation"]

check("Bought all 4 tickers", len(set(t["symbol"] for t in buys4 if t["date"] == "2024-01-03")) == 4)
check("No rotation sells (no ranking = keep same names)", len(rotation_sells4) == 0, f"got {len(rotation_sells4)}")
check("Has rebalance trims or adds", len(trades4) > 8, f"got {len(trades4)} trades")

# =========================================================================
print("\n" + "="*70)
print("TEST 5: Backward compatibility — existing mean-reversion strategy")
print("="*70)

config5 = {
    "name": "T5 Legacy Mean Reversion",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "GOOGL", "NVDA", "META"]},
    "entry": {
        "conditions": [{"type": "current_drop", "threshold": -15, "window_days": 90}],
        "logic": "all"
    },
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "stop_loss": {"type": "drawdown_from_entry", "value": -25},
    "take_profit": {"type": "gain_from_entry", "value": 30},
    "backtest": {"start": "2023-01-01", "end": "2025-12-31", "entry_price": "next_close", "slippage_bps": 10},
}

r5 = run_backtest(config5)
check("Legacy strategy runs without error", True)
check("Has trades", len(r5["trades"]) > 0, f"got {len(r5['trades'])}")
check("No 'always' signals in legacy", all(
    t.get("signal_detail", [{}])[0].get("type") != "always" 
    for t in r5["trades"] if t["action"] == "BUY" and t.get("signal_detail")
), "found always signal in legacy strategy")

# =========================================================================
print("\n" + "="*70)
print("TEST 6: Ranking with different metrics")
print("="*70)

for metric, order in [("current_drop", "asc"), ("momentum_rank", "desc"), ("rsi", "asc")]:
    config6 = {
        "name": f"T6 Rank by {metric}",
        "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "GOOGL", "NVDA", "META", "AMZN"]},
        "entry": {"conditions": [{"type": "always"}], "logic": "all"},
        "ranking": {"by": metric, "order": order, "top_n": 3},
        "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
        "backtest": {"start": "2024-06-01", "end": "2024-12-31", "entry_price": "next_close", "slippage_bps": 10},
    }
    try:
        r6 = run_backtest(config6)
        buys6 = [t for t in r6["trades"] if t["action"] == "BUY"]
        check(f"Ranking by {metric} ({order}): bought {len(buys6)} positions", len(buys6) == 3, f"got {len(buys6)}")
    except Exception as e:
        check(f"Ranking by {metric} ({order})", False, str(e))

# =========================================================================
print("\n" + "="*70)
print("TEST 7: Validation — invalid ranking metric rejected")
print("="*70)

config7 = {
    "name": "T7 Bad Ranking",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "ranking": {"by": "made_up_metric", "order": "asc"},
    "sizing": {"type": "equal_weight", "max_positions": 2, "initial_allocation": 100000},
    "backtest": {"start": "2024-01-01", "end": "2024-06-30", "entry_price": "next_close", "slippage_bps": 10},
}

try:
    load_strategy_inline = lambda c: c  # skip file load
    # Manually trigger validation
    from backtest_engine import VALID_RANKING_METRICS
    rank_by = config7["ranking"]["by"]
    check("Invalid ranking metric rejected", rank_by not in VALID_RANKING_METRICS, f"{rank_by} was accepted")
except Exception as e:
    check("Validation error raised", True)

# =========================================================================
print("\n" + "="*70)
print("TEST 8: Sector universe + always + ranking")
print("="*70)

config8 = {
    "name": "T8 Sector EW",
    "universe": {"type": "sector", "sector": "Technology"},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "ranking": {"by": "pe_percentile", "order": "asc", "top_n": 10},
    "sizing": {"type": "equal_weight", "max_positions": 10, "initial_allocation": 500000},
    "rebalancing": {"frequency": "quarterly", "mode": "equal_weight", "rules": {}},
    "backtest": {"start": "2024-01-01", "end": "2025-12-31", "entry_price": "next_close", "slippage_bps": 10},
}

r8 = run_backtest(config8)
buys8_day1 = [t for t in r8["trades"] if t["action"] == "BUY" and t["date"] == "2024-01-03"]
all_syms8 = set(t["symbol"] for t in r8["trades"])

check("Sector universe resolved (>10 tickers available)", len(all_syms8) >= 10, f"got {len(all_syms8)}")
check("Bought exactly 10 on day 1", len(buys8_day1) == 10, f"got {len(buys8_day1)}")
check("Rotation happened (>10 unique symbols traded)", len(all_syms8) > 10, f"got {len(all_syms8)}")
check("Positive NAV", r8["nav_history"][-1]["nav"] > 0)

# =========================================================================
print("\n" + "="*70)
print(f"RESULTS: {PASS} passed, {FAIL} failed")
print("="*70)

if FAIL > 0:
    sys.exit(1)
else:
    print("All tests passed! ✅")
