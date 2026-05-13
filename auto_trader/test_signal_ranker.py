#!/usr/bin/env python3
"""
End-to-end tests for signal evaluator and ranker.

Tests:
1. evaluate_signal — single signal with known behavior
2. evaluate_signal — multiple signal types
3. rank_signals — forward selection math
4. rank_signals — verify intersection logic
5. Edge cases — empty results, single signal ranking
"""

import sys
import os
from pathlib import Path

# Add paths
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent))

DB_PATH = os.environ.get("MARKET_DB_PATH",
    str(Path(__file__).parent.parent.parent / "alpha-scout-backend" / "data" / "market.db"))

from signal_ranker import evaluate_signal, rank_signals

PASS = 0
FAIL = 0

def test(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} — {detail}")


# =========================================================================
# Test 1: evaluate_signal — momentum_rank
# =========================================================================
print("\n" + "=" * 70)
print("TEST 1: evaluate_signal — momentum_rank >= 80 (top 20%), 6m horizon")
print("=" * 70)

result = evaluate_signal(
    signal_config={"type": "momentum_rank", "lookback": 63, "operator": ">=", "value": 80},
    target_horizon="6m",
    db_path=DB_PATH,
    start="2016-01-01",
    end="2020-12-31",
    sector="Technology",
)

test("No error", "error" not in result, result.get("error", ""))
test("Has trigger_count", result.get("trigger_count", 0) > 0,
     f"trigger_count={result.get('trigger_count')}")
test("Reasonable trigger count (>100 for tech sector over 5y)",
     result.get("trigger_count", 0) > 100,
     f"trigger_count={result.get('trigger_count')}")
test("Win rate between 0 and 1", 0 <= result.get("win_rate", -1) <= 1,
     f"win_rate={result.get('win_rate')}")
test("Sharpe is a number", isinstance(result.get("sharpe"), (int, float)),
     f"sharpe={result.get('sharpe')}")
test("avg_return is reasonable (-1 to 2)", -1 < result.get("avg_return", 99) < 2,
     f"avg_return={result.get('avg_return')}")
test("Has return_percentiles", len(result.get("return_percentiles", {})) == 5,
     f"keys={list(result.get('return_percentiles', {}).keys())}")
test("Has yearly_breakdown", len(result.get("yearly_breakdown", [])) >= 4,
     f"years={len(result.get('yearly_breakdown', []))}")
test("Has top_stocks", len(result.get("top_stocks", [])) > 0,
     f"count={len(result.get('top_stocks', []))}")
test("Has bottom_stocks", len(result.get("bottom_stocks", [])) > 0,
     f"count={len(result.get('bottom_stocks', []))}")
test("Has unique_stocks", result.get("unique_stocks", 0) > 0,
     f"unique_stocks={result.get('unique_stocks')}")
test("p50 close to median_return",
     abs(result.get("return_percentiles", {}).get("p50", 99) - result.get("median_return", 0)) < 0.001,
     f"p50={result.get('return_percentiles', {}).get('p50')}, median={result.get('median_return')}")

print(f"\n  Summary: {result.get('trigger_count')} triggers across {result.get('unique_stocks')} stocks, "
      f"win_rate={result.get('win_rate')}, avg_return={result.get('avg_return')}, "
      f"sharpe={result.get('sharpe')}")
print(f"  Percentiles: {result.get('return_percentiles')}")
print(f"  Yearly: {[(y['year'], y['sharpe']) for y in result.get('yearly_breakdown', [])]}")
print(f"  Top 3: {[(s['symbol'], s['avg_return']) for s in result.get('top_stocks', [])[:3]]}")
print(f"  Bottom 3: {[(s['symbol'], s['avg_return']) for s in result.get('bottom_stocks', [])[:3]]}")


# =========================================================================
# Test 2: evaluate_signal — earnings_momentum
# =========================================================================
print("\n" + "=" * 70)
print("TEST 2: evaluate_signal — earnings_momentum (3 beats in 4 quarters), 6m")
print("=" * 70)

result2 = evaluate_signal(
    signal_config={"type": "earnings_momentum", "lookback_quarters": 4, "min_beats": 3},
    target_horizon="6m",
    db_path=DB_PATH,
    start="2016-01-01",
    end="2020-12-31",
    sector="Technology",
)

test("No error", "error" not in result2, result2.get("error", ""))
test("Has triggers", result2.get("trigger_count", 0) > 0,
     f"trigger_count={result2.get('trigger_count')}")
test("Win rate between 0 and 1", 0 <= result2.get("win_rate", -1) <= 1,
     f"win_rate={result2.get('win_rate')}")

print(f"\n  Summary: {result2.get('trigger_count')} triggers, "
      f"win_rate={result2.get('win_rate')}, avg_return={result2.get('avg_return')}, "
      f"sharpe={result2.get('sharpe')}")


# =========================================================================
# Test 3: evaluate_signal — pe_percentile
# NOTE: PE data only available from 2024+, so use 2024-2025 period
# =========================================================================
print("\n" + "=" * 70)
print("TEST 3: evaluate_signal — pe_percentile (cheapest 20%), 3m, 2024-2025")
print("=" * 70)

result3 = evaluate_signal(
    signal_config={"type": "pe_percentile", "max_percentile": 20},
    target_horizon="3m",
    db_path=DB_PATH,
    start="2024-04-01",
    end="2025-06-30",
    sector="Technology",
)

test("No error", "error" not in result3, result3.get("error", ""))
test("Has triggers", result3.get("trigger_count", 0) > 0,
     f"trigger_count={result3.get('trigger_count')} (PE data only from 2024+)")

print(f"\n  Summary: {result3.get('trigger_count')} triggers, "
      f"win_rate={result3.get('win_rate')}, avg_return={result3.get('avg_return')}, "
      f"sharpe={result3.get('sharpe')}")


# =========================================================================
# Test 4: evaluate_signal — rsi
# =========================================================================
print("\n" + "=" * 70)
print("TEST 4: evaluate_signal — RSI <= 30 (oversold), 3m")
print("=" * 70)

result4 = evaluate_signal(
    signal_config={"type": "rsi", "period": 14, "operator": "<=", "value": 30},
    target_horizon="3m",
    db_path=DB_PATH,
    start="2016-01-01",
    end="2020-12-31",
    sector="Technology",
)

test("No error", "error" not in result4, result4.get("error", ""))
test("Has triggers", result4.get("trigger_count", 0) > 0,
     f"trigger_count={result4.get('trigger_count')}")

print(f"\n  Summary: {result4.get('trigger_count')} triggers, "
      f"win_rate={result4.get('win_rate')}, avg_return={result4.get('avg_return')}, "
      f"sharpe={result4.get('sharpe')}")


# =========================================================================
# Test 5: evaluate_signal — current_drop
# =========================================================================
print("\n" + "=" * 70)
print("TEST 5: evaluate_signal — current_drop >= -15% from 90d high, 6m")
print("=" * 70)

result5 = evaluate_signal(
    signal_config={"type": "current_drop", "threshold": -15, "window_days": 90},
    target_horizon="6m",
    db_path=DB_PATH,
    start="2016-01-01",
    end="2020-12-31",
    sector="Technology",
)

test("No error", "error" not in result5, result5.get("error", ""))
test("Has triggers", result5.get("trigger_count", 0) > 0,
     f"trigger_count={result5.get('trigger_count')}")

print(f"\n  Summary: {result5.get('trigger_count')} triggers, "
      f"win_rate={result5.get('win_rate')}, avg_return={result5.get('avg_return')}, "
      f"sharpe={result5.get('sharpe')}")


# =========================================================================
# Test 6: rank_signals — forward selection with 4 candidates
# =========================================================================
print("\n" + "=" * 70)
print("TEST 6: rank_signals — 4 candidates, forward selection")
print("=" * 70)

rank_result = rank_signals(
    candidate_signals=[
        {"type": "momentum_rank", "lookback": 63, "operator": ">=", "value": 80},
        {"type": "earnings_momentum", "lookback_quarters": 4, "min_beats": 3},
        {"type": "current_drop", "threshold": -15, "window_days": 90},
        {"type": "rsi", "period": 14, "operator": "<=", "value": 30},
    ],
    target_horizon="6m",
    db_path=DB_PATH,
    start="2016-01-01",
    end="2020-12-31",
    sector="Technology",
)

test("No error", "error" not in rank_result, rank_result.get("error", ""))
test("Has 4 individual_signals", len(rank_result.get("individual_signals", [])) == 4,
     f"count={len(rank_result.get('individual_signals', []))}")
test("Has forward_selection", len(rank_result.get("forward_selection", [])) > 0,
     f"count={len(rank_result.get('forward_selection', []))}")

# Check forward selection math
steps = rank_result.get("forward_selection", [])
if len(steps) >= 2:
    # First step should have no delta (or None)
    test("First step delta is None", steps[0].get("delta") is None,
         f"delta={steps[0].get('delta')}")
    # Sharpe should increase or stay stable for "kept" steps
    kept_steps = [s for s in steps if s.get("verdict") == "kept"]
    for i in range(1, len(kept_steps)):
        test(f"Step {kept_steps[i]['step']} Sharpe >= previous",
             kept_steps[i]["combined_sharpe"] >= kept_steps[i-1]["combined_sharpe"],
             f"{kept_steps[i]['combined_sharpe']} < {kept_steps[i-1]['combined_sharpe']}")
    # If a signal was dropped, it should have negative delta
    dropped = [s for s in steps if s.get("verdict") == "dropped"]
    for d in dropped:
        test(f"Dropped signal has non-positive delta",
             d.get("delta", 0) <= 0,
             f"delta={d.get('delta')}")
else:
    print("  (Not enough steps to test forward selection math)")

# Check that individual stats are consistent
for sig_result in rank_result.get("individual_signals", []):
    sig_type = sig_result.get("signal", {}).get("type", "?")
    test(f"Individual '{sig_type}' has valid win_rate",
         0 <= sig_result.get("win_rate", -1) <= 1,
         f"win_rate={sig_result.get('win_rate')}")

print("\n  Forward selection steps:")
for step in steps:
    sig_type = step.get("added_signal", {}).get("type", "?")
    print(f"    Step {step['step']}: +{sig_type} → "
          f"Sharpe={step['combined_sharpe']}, "
          f"triggers={step.get('trigger_count')}, "
          f"win_rate={step.get('win_rate')}, "
          f"delta={step.get('delta')}, "
          f"verdict={step.get('verdict')}")

# =========================================================================
# Test 7: rank_signals — single signal (should still work)
# =========================================================================
print("\n" + "=" * 70)
print("TEST 7: rank_signals — single signal (edge case)")
print("=" * 70)

rank_single = rank_signals(
    candidate_signals=[
        {"type": "momentum_rank", "lookback": 63, "operator": ">=", "value": 80},
    ],
    target_horizon="6m",
    db_path=DB_PATH,
    start="2016-01-01",
    end="2020-12-31",
    sector="Technology",
)

test("No error", "error" not in rank_single, rank_single.get("error", ""))
test("1 individual signal", len(rank_single.get("individual_signals", [])) == 1)
test("1 forward selection step", len(rank_single.get("forward_selection", [])) == 1)


# =========================================================================
# Test 8: evaluate_signal — no matches (very tight threshold)
# =========================================================================
print("\n" + "=" * 70)
print("TEST 8: evaluate_signal — very tight threshold (expect few/no triggers)")
print("=" * 70)

result_tight = evaluate_signal(
    signal_config={"type": "momentum_rank", "lookback": 63, "operator": ">=", "value": 99.9},
    target_horizon="6m",
    db_path=DB_PATH,
    start="2016-01-01",
    end="2020-12-31",
    sector="Technology",
)

test("No error", "error" not in result_tight, result_tight.get("error", ""))
# With threshold 99.9, only ~1 stock/day qualifies. Over 1257 trading days
# that's ~1200 triggers — much fewer than the 20437 at threshold 80.
test("Fewer triggers than loose threshold",
     result_tight.get("trigger_count", 999999) < result.get("trigger_count", 0),
     f"tight={result_tight.get('trigger_count')}, loose={result.get('trigger_count')}")


# =========================================================================
# Test 9: rank_signals — verify intersection shrinks trigger count
# =========================================================================
print("\n" + "=" * 70)
print("TEST 9: Verify intersection logic — combined triggers < individual")
print("=" * 70)

if len(rank_result.get("individual_signals", [])) >= 2 and len(steps) >= 2:
    # The first individual signal's trigger count should be >= the combined count at step 2
    first_individual = rank_result["individual_signals"][0]
    # Find the individual result matching the first selected signal
    first_selected_type = steps[0].get("added_signal", {}).get("type")
    first_selected_individual = None
    for ind in rank_result["individual_signals"]:
        if ind.get("signal", {}).get("type") == first_selected_type:
            first_selected_individual = ind
            break

    if first_selected_individual and len(steps) >= 2 and steps[1].get("verdict") == "kept":
        solo_triggers = first_selected_individual.get("trigger_count", 0)
        combo_triggers = steps[1].get("trigger_count", 0)
        test("Combined triggers <= solo triggers (intersection shrinks)",
             combo_triggers <= solo_triggers,
             f"solo={solo_triggers}, combo={combo_triggers}")
    else:
        print("  (Skipped — not enough kept steps to compare)")
else:
    print("  (Skipped — not enough data)")


# =========================================================================
# Test 10: Cross-sector — Energy (different sector, ensure no crash)
# =========================================================================
print("\n" + "=" * 70)
print("TEST 10: Cross-sector — Energy, momentum_rank, 6m")
print("=" * 70)

result_energy = evaluate_signal(
    signal_config={"type": "momentum_rank", "lookback": 63, "operator": ">=", "value": 80},
    target_horizon="6m",
    db_path=DB_PATH,
    start="2016-01-01",
    end="2020-12-31",
    sector="Energy",
)

test("No error", "error" not in result_energy, result_energy.get("error", ""))
test("Has triggers", result_energy.get("trigger_count", 0) > 0,
     f"trigger_count={result_energy.get('trigger_count')}")
# Energy should have fewer triggers than Tech (smaller sector)
test("Fewer triggers than tech (smaller sector)",
     result_energy.get("trigger_count", 0) < result.get("trigger_count", 0),
     f"energy={result_energy.get('trigger_count')}, tech={result.get('trigger_count')}")

print(f"\n  Energy: {result_energy.get('trigger_count')} triggers, "
      f"win_rate={result_energy.get('win_rate')}, sharpe={result_energy.get('sharpe')}")


# =========================================================================
# Summary
# =========================================================================
print("\n" + "=" * 70)
total = PASS + FAIL
print(f"RESULTS: {PASS}/{total} passed, {FAIL} failed")
if FAIL == 0:
    print("ALL TESTS PASSED")
else:
    print(f"FAILURES: {FAIL}")
print("=" * 70)
