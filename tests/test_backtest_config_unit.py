#!/usr/bin/env python3
"""
Unit tests for server/models/backtest.py INPUT models:
WindowSpec, EvalBlock, BacktestConfig.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_backtest_config_unit.py
"""
import os
import sys

from dateutil.relativedelta import relativedelta
from pydantic import ValidationError

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from server.models.backtest import (  # noqa: E402
    BacktestConfig,
    EvalBlock,
    WindowSpec,
    _parse_duration,
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


# ---------- _parse_duration ----------
print("\n_parse_duration:")
check("2y",   _parse_duration("2y")   == (2, 0, 0))
check("12m",  _parse_duration("12m")  == (0, 12, 0))
check("180d", _parse_duration("180d") == (0, 0, 180))
check("space tolerance", _parse_duration("  3Y ") == (3, 0, 0))

expect_raises("reject 'two years'",       lambda: _parse_duration("two years"))
expect_raises("reject empty",             lambda: _parse_duration(""))
expect_raises("reject negative",          lambda: _parse_duration("-1y"))
expect_raises("reject bare number",       lambda: _parse_duration("5"))
expect_raises("reject mixed units",       lambda: _parse_duration("1y6m"))

# ---------- WindowSpec ----------
print("\nWindowSpec:")
w1 = WindowSpec(window="2y", overlap="1y")
check("2y/1y window_delta", w1.window_delta() == relativedelta(years=2))
check("2y/1y step_delta",   w1.step_delta()   == relativedelta(years=1))

w2 = WindowSpec(window="2y", overlap="0d")
check("2y/0d step = 2y", w2.step_delta() == relativedelta(years=2))

w3 = WindowSpec(window="12m", overlap="6m")
check("12m/6m step = 6m", w3.step_delta() == relativedelta(months=6))

expect_raises("overlap == window",    lambda: WindowSpec(window="2y", overlap="2y"))
expect_raises("overlap > window",     lambda: WindowSpec(window="1y", overlap="2y"))
expect_raises("overlap > window (m)", lambda: WindowSpec(window="6m", overlap="12m"))
expect_raises("window=0",             lambda: WindowSpec(window="0d", overlap="0d"))
expect_raises("bad window format",    lambda: WindowSpec(window="2 years", overlap="1y"))

# ---------- EvalBlock ----------
print("\nEvalBlock:")
e1 = EvalBlock(start="2023-01-01", end="2025-01-01", spec=WindowSpec(window="1y", overlap="6m"))
check("valid eval block", e1.start == "2023-01-01" and e1.end == "2025-01-01")

expect_raises("eval start >= end",   lambda: EvalBlock(start="2025-01-01", end="2025-01-01", spec=WindowSpec(window="1y", overlap="0d")))
expect_raises("eval start > end",    lambda: EvalBlock(start="2025-02-01", end="2025-01-01", spec=WindowSpec(window="1y", overlap="0d")))
expect_raises("bad date format",     lambda: EvalBlock(start="2025/01/01", end="2025-12-31", spec=WindowSpec(window="1y", overlap="0d")))

# ---------- BacktestConfig ----------
print("\nBacktestConfig:")
c1 = BacktestConfig(
    training_start="2015-01-01",
    training_end="2025-01-01",
    initial_capital=100000,
)
check("minimal config (no eval)",
      c1.eval is None and c1.benchmark == "market" and c1.sector is None)

c2 = BacktestConfig(
    training_start="2015-01-01",
    training_end="2022-12-31",
    initial_capital=50000,
    sector="Technology",
    benchmark="sector",
    eval=EvalBlock(
        start="2023-01-01", end="2025-12-31",
        spec=WindowSpec(window="1y", overlap="6m"),
    ),
)
check("full config with eval",
      c2.eval is not None and c2.eval.spec.window == "1y")

# from_legacy_args back-compat
c3 = BacktestConfig.from_legacy_args(
    start="2015-01-01", end="2025-01-01", capital=10000, sector=None,
)
check("from_legacy_args back-compat",
      c3.eval is None and c3.training_start == "2015-01-01" and c3.initial_capital == 10000)

expect_raises("training_start >= training_end",
              lambda: BacktestConfig(training_start="2025-01-01", training_end="2015-01-01", initial_capital=100))
expect_raises("training_start == training_end",
              lambda: BacktestConfig(training_start="2020-01-01", training_end="2020-01-01", initial_capital=100))
expect_raises("benchmark=sector without sector",
              lambda: BacktestConfig(training_start="2015-01-01", training_end="2025-01-01", initial_capital=100, benchmark="sector"))
expect_raises("zero capital",
              lambda: BacktestConfig(training_start="2015-01-01", training_end="2025-01-01", initial_capital=0))
expect_raises("negative capital",
              lambda: BacktestConfig(training_start="2015-01-01", training_end="2025-01-01", initial_capital=-100))
expect_raises("bad training_start format",
              lambda: BacktestConfig(training_start="01/01/2015", training_end="2025-01-01", initial_capital=100))

# Eval may be inside training (common), outside training (disjoint), or partly overlapping
inside  = BacktestConfig(training_start="2015-01-01", training_end="2025-01-01", initial_capital=100,
                          eval=EvalBlock(start="2018-01-01", end="2022-01-01", spec=WindowSpec(window="2y", overlap="1y")))
disjoint = BacktestConfig(training_start="2015-01-01", training_end="2022-12-31", initial_capital=100,
                          eval=EvalBlock(start="2023-01-01", end="2025-12-31", spec=WindowSpec(window="1y", overlap="0d")))
overlap_partial = BacktestConfig(training_start="2015-01-01", training_end="2023-06-30", initial_capital=100,
                          eval=EvalBlock(start="2022-01-01", end="2025-12-31", spec=WindowSpec(window="1y", overlap="0d")))
check("eval inside training",          inside.eval is not None)
check("eval disjoint from training",   disjoint.eval is not None)
check("eval partially overlapping",    overlap_partial.eval is not None)

# ---------- summary ----------
print(f"\n{'=' * 50}\n{PASS} passed, {FAIL} failed\n{'=' * 50}")
sys.exit(0 if FAIL == 0 else 1)
