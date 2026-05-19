#!/usr/bin/env python3
"""
Unit tests for server/models/research_run.py — TargetMetric + ResearchRunConfig.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_research_run_unit.py
"""
import os
import sys

from pydantic import ValidationError

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from server.models.backtest import BacktestConfig, EvalBlock, WindowSpec  # noqa: E402
from server.models.research_run import (  # noqa: E402
    ResearchRunConfig,
    TargetMetric,
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


def expect_raises(name, fn):
    try:
        fn()
    except (ValidationError, ValueError):
        check(name, True)
        return
    except Exception as e:
        check(name, False, f"got {type(e).__name__}: {e}")
        return
    check(name, False, "expected ValidationError/ValueError, got nothing")


bt_no_eval = BacktestConfig(
    training_start="2015-01-01", training_end="2025-01-01", initial_capital=100_000,
)
bt_with_eval = BacktestConfig(
    training_start="2015-01-01", training_end="2022-12-31", initial_capital=100_000,
    eval=EvalBlock(start="2023-01-01", end="2025-12-31",
                   spec=WindowSpec(window="1y", overlap="6m")),
)

# ---------- TargetMetric ----------
print("\nTargetMetric:")
tm_overall = TargetMetric(name="sharpe_ratio")  # aggregator defaults to 'overall'
check("default aggregator = overall", tm_overall.aggregator == "overall")

tm_median = TargetMetric(name="sharpe_ratio", aggregator="median")
check("explicit median accepted", tm_median.aggregator == "median")

expect_raises("invalid aggregator",
              lambda: TargetMetric(name="sharpe_ratio", aggregator="bogus"))

# ---------- ResearchRunConfig ----------
print("\nResearchRunConfig:")
cfg_default = ResearchRunConfig(
    backtest=bt_no_eval,
    target_metric=TargetMetric(name="sharpe_ratio"),  # 'overall'
)
check("default valid (no eval, overall aggregator)",
      cfg_default.target_metric.aggregator == "overall"
      and cfg_default.backtest.eval is None)

cfg_eval_overall = ResearchRunConfig(
    backtest=bt_with_eval,
    target_metric=TargetMetric(name="alpha_ann_pct"),  # overall + eval set is OK
)
check("eval set + overall aggregator valid",
      cfg_eval_overall.target_metric.aggregator == "overall"
      and cfg_eval_overall.backtest.eval is not None)

cfg_eval_median = ResearchRunConfig(
    backtest=bt_with_eval,
    target_metric=TargetMetric(name="sharpe_ratio", aggregator="median"),
)
check("eval set + median aggregator valid",
      cfg_eval_median.target_metric.aggregator == "median")

# CRITICAL: aggregator != overall must require eval.
expect_raises("median without eval",
              lambda: ResearchRunConfig(
                  backtest=bt_no_eval,
                  target_metric=TargetMetric(name="sharpe_ratio", aggregator="median"),
              ))
expect_raises("min without eval",
              lambda: ResearchRunConfig(
                  backtest=bt_no_eval,
                  target_metric=TargetMetric(name="sharpe_ratio", aggregator="min"),
              ))
expect_raises("p25 without eval",
              lambda: ResearchRunConfig(
                  backtest=bt_no_eval,
                  target_metric=TargetMetric(name="alpha_ann_pct", aggregator="p25"),
              ))

# New aggregators (p10, stdev, iqr, range, snr) all require eval set.
for new_agg in ("p10", "stdev", "iqr", "range", "snr"):
    # Valid when eval is set
    cfg = ResearchRunConfig(
        backtest=bt_with_eval,
        target_metric=TargetMetric(name="sharpe_ratio", aggregator=new_agg),
    )
    check(f"{new_agg} aggregator with eval valid",
          cfg.target_metric.aggregator == new_agg)

    # Rejected without eval
    expect_raises(f"{new_agg} without eval",
                  lambda agg=new_agg: ResearchRunConfig(
                      backtest=bt_no_eval,
                      target_metric=TargetMetric(name="sharpe_ratio", aggregator=agg),
                  ))

# Conditions default
check("conditions default to empty", cfg_default.conditions == [])

# Field defaults sane
check("max_iterations default", cfg_default.max_iterations == 50)
check("model default", cfg_default.model == "opus-4-7")

expect_raises("zero max_iterations",
              lambda: ResearchRunConfig(
                  backtest=bt_no_eval,
                  target_metric=TargetMetric(name="sharpe_ratio"),
                  max_iterations=0,
              ))

print(f"\n{'=' * 50}\n{PASS} passed, {FAIL} failed\n{'=' * 50}")
sys.exit(0 if FAIL == 0 else 1)
