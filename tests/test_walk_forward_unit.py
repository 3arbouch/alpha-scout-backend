#!/usr/bin/env python3
"""
Unit tests for the walk-forward window generator and metric aggregator.

Validates pure functions only — no actual backtests are run here. The
end-to-end behavior (training + N eval backtests) is exercised by
test_walk_forward_smoke.py.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_walk_forward_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "auto_trader"))

from server.models.backtest import EvalBlock, WindowSpec  # noqa: E402
from runner import (  # noqa: E402
    _aggregate_window_metrics,
    _generate_eval_windows,
    _quantile,
    _resolve_target_value,
)

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


# ---------- _generate_eval_windows ----------
print("\n_generate_eval_windows:")

# 2y window, 1y overlap, 2015–2025 → 9 windows, all 2y long, step 1y.
w1 = _generate_eval_windows(
    EvalBlock(start="2015-01-01", end="2025-01-01",
              spec=WindowSpec(window="2y", overlap="1y"))
)
check("2y/1y over 10y → 9 windows", len(w1) == 9, f"got {len(w1)}")
check("first window starts at eval.start", w1[0][0] == "2015-01-01")
check("first window ends 2 years later",   w1[0][1] == "2017-01-01")
check("last window ends on or before end_cap", w1[-1][1] <= "2025-01-01")
check("second window starts 1y after first",  w1[1][0] == "2016-01-01")

# 2y window, 0d overlap (contiguous) → 5 windows for 2015-2025.
w2 = _generate_eval_windows(
    EvalBlock(start="2015-01-01", end="2025-01-01",
              spec=WindowSpec(window="2y", overlap="0d"))
)
check("2y/0d over 10y → 5 windows", len(w2) == 5, f"got {len(w2)}")
check("contiguous: window 2 starts where window 1 ends",
      w2[1][0] == w2[0][1])

# 1y/6m over 3y → 5 windows: [2023-01, 2024-01], [2023-07, 2024-07],
# [2024-01, 2025-01], [2024-07, 2025-07], [2025-01, 2026-01] — but end_cap is
# 2025-12-31, so the [2025-01..2026-01] window is dropped (cur_end > end_cap).
# Result: 4 windows.
w3 = _generate_eval_windows(
    EvalBlock(start="2023-01-01", end="2025-12-31",
              spec=WindowSpec(window="1y", overlap="6m"))
)
check("1y/6m over ~3y → 4 windows (partial dropped)", len(w3) == 4, f"got {len(w3)}")
check("partial-tail dropped: last window end <= end_cap",
      w3[-1][1] <= "2025-12-31")

# Window > span → zero windows.
w4 = _generate_eval_windows(
    EvalBlock(start="2023-01-01", end="2023-06-30",
              spec=WindowSpec(window="1y", overlap="0d"))
)
check("window > span → 0 windows", len(w4) == 0)

# Labels are unique and well-formed.
labels = [w[2] for w in w1]
check("labels unique", len(set(labels)) == len(labels))
check("label format start_end",
      all("_" in lbl and lbl.count("-") == 4 for lbl in labels))


# ---------- _quantile ----------
print("\n_quantile (type-7, R default):")
check("p50 odd",  _quantile([1, 2, 3], 0.5) == 2)
check("p50 even", _quantile([1, 2, 3, 4], 0.5) == 2.5)
check("p25",      _quantile([1, 2, 3, 4], 0.25) == 1.75)
check("p75",      _quantile([1, 2, 3, 4], 0.75) == 3.25)
check("singleton",   _quantile([5.5], 0.5) == 5.5)
check("empty → None", _quantile([], 0.5) is None)


# ---------- _aggregate_window_metrics ----------
print("\n_aggregate_window_metrics:")

# 5 windows, hand-crafted metrics for sharpe and alpha.
windows = [
    {"metrics": {"sharpe_ratio": 0.5, "alpha_ann_pct": -2.0}},
    {"metrics": {"sharpe_ratio": 1.0, "alpha_ann_pct":  5.0}},
    {"metrics": {"sharpe_ratio": 1.2, "alpha_ann_pct":  3.0}},
    {"metrics": {"sharpe_ratio": 1.8, "alpha_ann_pct": 10.0}},
    {"metrics": {"sharpe_ratio": 2.0, "alpha_ann_pct": 15.0}},
]
agg = _aggregate_window_metrics(windows)
check("sharpe bucket exists",  "sharpe_ratio" in agg)
check("sharpe count = 5",      agg["sharpe_ratio"]["count"] == 5)
check("sharpe min = 0.5",      agg["sharpe_ratio"]["min"] == 0.5)
check("sharpe max = 2.0",      agg["sharpe_ratio"]["max"] == 2.0)
check("sharpe median = 1.2",   agg["sharpe_ratio"]["median"] == 1.2)
check("sharpe mean = 1.30",    abs(agg["sharpe_ratio"]["mean"] - 1.3) < 1e-9)
# alpha sorted = [-2, 3, 5, 10, 15], p25 pos = (5-1)*0.25 = 1.0 → sorted[1] = 3.0
check("alpha p25 = 3.0",       agg["alpha_ann_pct"]["p25"] == 3.0,
      f"got {agg['alpha_ann_pct']['p25']}")

# Missing values: one window's sharpe is None, skipped from sharpe stats only.
windows_with_holes = [
    {"metrics": {"sharpe_ratio": 1.0, "alpha_ann_pct":  5.0}},
    {"metrics": {"sharpe_ratio": None, "alpha_ann_pct": 10.0}},
    {"metrics": {"sharpe_ratio": 2.0, "alpha_ann_pct": None}},
]
agg_h = _aggregate_window_metrics(windows_with_holes)
check("sharpe count drops to 2 (skipped None)",
      agg_h["sharpe_ratio"]["count"] == 2)
check("alpha count drops to 2 (skipped None)",
      agg_h["alpha_ann_pct"]["count"] == 2)

# Empty windows list → empty dict.
check("empty windows → empty agg", _aggregate_window_metrics([]) == {})

# Metric absent from all windows → not in result.
no_sharpe = [{"metrics": {"alpha_ann_pct": 1.0}}, {"metrics": {"alpha_ann_pct": 2.0}}]
agg_ns = _aggregate_window_metrics(no_sharpe)
check("metric absent → key missing", "sharpe_ratio" not in agg_ns)


# ---------- _resolve_target_value ----------
print("\n_resolve_target_value:")

training_m = {"sharpe_ratio": 1.5, "alpha_ann_pct": 8.0, "max_drawdown_pct": -22.0}
agg = {
    "sharpe_ratio": {"mean": 1.0, "median": 1.2, "min": 0.5, "max": 2.0, "p25": 0.9, "count": 5},
    "alpha_ann_pct": {"mean": 5.0, "median": 4.0, "min": -2.0, "max": 15.0, "p25": 1.0, "count": 5},
}

check("overall reads training",
      _resolve_target_value(training_m, agg, "sharpe_ratio", "overall") == 1.5)
check("median reads eval agg",
      _resolve_target_value(training_m, agg, "sharpe_ratio", "median") == 1.2)
check("min reads eval agg",
      _resolve_target_value(training_m, agg, "sharpe_ratio", "min") == 0.5)
check("p25 reads eval agg",
      _resolve_target_value(training_m, agg, "alpha_ann_pct", "p25") == 1.0)
check("missing eval metric → None",
      _resolve_target_value(training_m, agg, "max_drawdown_pct", "median") is None)
check("missing training metric → None",
      _resolve_target_value({}, agg, "sharpe_ratio", "overall") is None)
check("no eval agg + overall still works",
      _resolve_target_value(training_m, {}, "sharpe_ratio", "overall") == 1.5)
check("no eval agg + median → None",
      _resolve_target_value(training_m, {}, "sharpe_ratio", "median") is None)


print(f"\n{'=' * 50}\n{PASS} passed, {FAIL} failed\n{'=' * 50}")
sys.exit(0 if FAIL == 0 else 1)
