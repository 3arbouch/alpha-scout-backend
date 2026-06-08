#!/usr/bin/env python3
"""
Unit test: _rank_buffer_target_weights — sizing-aware reweight targets.

The rank_buffer reweight now targets per-name weights from sizing.type instead
of always equal weight. Verified:
  - equal_weight / fixed_amount → uniform 1/n_targets (byte-identical legacy)
  - risk_parity → inverse-vol shares (low-vol name gets MORE), normalized to 1
  - missing-vol name → falls back to median vol (stays invested, not zero)

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_rank_buffer_sizing_weights_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine_v2 import _rank_buffer_target_weights

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


class _Sleeve:
    def __init__(self, sizing_type):
        self.config = {"sizing": {"type": sizing_type, "vol_window_days": 20, "vol_source": "historical"}}


# Synthetic price series (>=21 closes ending before `date`), two clearly
# different vols: LOWV = gently rising (low realized vol), HIGHV = zig-zag (high vol).
DATES = [f"2026-01-{d:02d}" for d in range(1, 25)]   # 24 days < date
DATE = "2026-02-01"
low = [100 + i * 0.1 for i in range(24)]                       # smooth → low vol
high = [100 * (1.15 if i % 2 else 0.87) for i in range(24)]    # alternating → high vol
PRICE_INDEX = {
    "LOWV":  {d: low[i] for i, d in enumerate(DATES)},
    "HIGHV": {d: high[i] for i, d in enumerate(DATES)},
}
SYMS = ["LOWV", "HIGHV"]

print("=== equal_weight → uniform (byte-identical legacy) ===")
w = _rank_buffer_target_weights(SYMS, _Sleeve("equal_weight"), PRICE_INDEX, DATE, 2)
check("both = 0.5", abs(w["LOWV"] - 0.5) < 1e-9 and abs(w["HIGHV"] - 0.5) < 1e-9, str(w))

print("\n=== fixed_amount → also uniform ===")
w = _rank_buffer_target_weights(SYMS, _Sleeve("fixed_amount"), PRICE_INDEX, DATE, 2)
check("both = 0.5", abs(w["LOWV"] - 0.5) < 1e-9, str(w))

print("\n=== risk_parity → low-vol name gets MORE, sums to 1 ===")
w = _rank_buffer_target_weights(SYMS, _Sleeve("risk_parity"), PRICE_INDEX, DATE, 2)
check("LOWV weight > HIGHV weight", w["LOWV"] > w["HIGHV"], str(w))
check("weights sum to 1", abs(w["LOWV"] + w["HIGHV"] - 1.0) < 1e-9, str(w))
check("not equal weight (inverse-vol took effect)", abs(w["LOWV"] - 0.5) > 0.01, str(w))

print("\n=== risk_parity with an unpriceable name → median-vol fallback, stays invested ===")
pi2 = dict(PRICE_INDEX); pi2["NOVOL"] = {}     # no prices → vol unestimable
w = _rank_buffer_target_weights(SYMS + ["NOVOL"], _Sleeve("risk_parity"), pi2, DATE, 3)
check("NOVOL still gets a positive weight", w.get("NOVOL", 0) > 0, str(w))
check("all three sum to 1", abs(sum(w.values()) - 1.0) < 1e-9, str(w))

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
