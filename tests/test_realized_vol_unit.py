#!/usr/bin/env python3
"""
Unit test: realized_vol factor math (precomputed), hand-computed, no DB.

Verifies: daily simple returns → sample stdev (ddof=1) × √252 × 100, over a
trailing window ending at the as-of date, point-in-time (insufficient history
→ None).

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_realized_vol_unit.py
"""
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "server"))

from factors.context import ComputeContext
from factors.library.risk import _realized_vol

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


def ctx_with(prices, date, close):
    return ComputeContext(symbol="T", date=date, close=close,
                          income_slice=[], balance_asof=None, cashflow_slice=[],
                          prices_history=prices)


# Closes engineered so the 3 trailing returns are exactly [+0.10, -0.10, +0.10].
prices = [("d0", 100.0), ("d1", 110.0), ("d2", 99.0), ("d3", 108.9)]
ctx = ctx_with(prices, "d3", 108.9)

# Hand math: returns [0.1, -0.1, 0.1]; mean=1/30; sample var = 0.02666667/2;
# sd = sqrt(0.01333333) = 0.1154701; ×√252×100 = 183.3286...
rets = [0.10, -0.10, 0.10]
mean = sum(rets) / 3
var = sum((r - mean) ** 2 for r in rets) / 2          # ddof=1
expected = math.sqrt(var) * math.sqrt(252) * 100.0

print("=== realized_vol math (window=3) ===")
got = _realized_vol(ctx, 3)
check(f"matches hand-computed ({expected:.4f})", got is not None and abs(got - expected) < 1e-6,
      f"got {got}")
check("value is ~183.303 (extreme by design)", got is not None and abs(got - 183.303) < 0.01, str(got))

print("\n=== point-in-time / insufficient history ===")
check("window longer than history → None", _realized_vol(ctx, 10) is None)
check("date not in price series → None",
      _realized_vol(ctx_with(prices, "d99", 1.0), 3) is None)

print("\n=== constant prices → zero vol ===")
flat = [("d0", 50.0), ("d1", 50.0), ("d2", 50.0), ("d3", 50.0)]
check("flat series → 0.0", _realized_vol(ctx_with(flat, "d3", 50.0), 3) == 0.0)

print("\n=== a realistic case sanity (≈ plausible annualized %) ===")
# ~1% daily moves alternating → annualized vol should land in a sane range.
import itertools
closes = [100.0]
for i in range(60):
    closes.append(closes[-1] * (1.01 if i % 2 == 0 else 0.99))
pr = [(f"x{i}", c) for i, c in enumerate(closes)]
rv = _realized_vol(ctx_with(pr, f"x{len(closes)-1}", closes[-1]), 60)
check("60d realized vol in a sane 5-60% band", rv is not None and 5 < rv < 60, f"got {rv}")

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
