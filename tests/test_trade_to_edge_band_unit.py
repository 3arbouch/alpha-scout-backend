#!/usr/bin/env python3
"""
Unit test: trade-to-edge no-trade band (_band_dest_amount).

The rank_buffer reweight sizes a surviving position to the destination this
helper returns. The behaviour under test:

  - within the band (|weight - target| <= band)        → None (no trade)
  - overweight breach                                  → trim to UPPER edge
                                                          (target + band), NOT
                                                          all the way to target
  - underweight breach                                 → add to LOWER edge
                                                          (target - band)
  - band == 0                                          → full correction to target
  - lower edge clamped at 0 weight

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_trade_to_edge_band_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine_v2 import _band_dest_amount

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


def approx(a, b, tol=1e-6):
    return a is not None and b is not None and abs(a - b) < tol


NAV = 1_000_000.0
TARGET = 0.04          # 25 equal-weight names
BAND = 0.01            # ±1pp band → edges at 3% and 5%

print("=== within the band → no trade ===")
check("exactly at target → None", _band_dest_amount(0.040 * NAV, NAV, TARGET, BAND) is None)
check("inside upper edge (4.5%) → None", _band_dest_amount(0.045 * NAV, NAV, TARGET, BAND) is None)
check("inside lower edge (3.5%) → None", _band_dest_amount(0.035 * NAV, NAV, TARGET, BAND) is None)
# Exactly on the edge is a float knife's-edge: either None or a zero-size move
# to the edge (dest == current mv). Both mean "no meaningful trade."
_edge = _band_dest_amount(0.050 * NAV, NAV, TARGET, BAND)
check("on the edge (5.0%) → no meaningful trade", _edge is None or approx(_edge, 0.050 * NAV))

print("\n=== overweight breach → trim to UPPER edge, not to target ===")
dest = _band_dest_amount(0.080 * NAV, NAV, TARGET, BAND)   # 8% position
check("8% → destination is 5% (upper edge)", approx(dest, 0.05 * NAV), f"dest={dest}")
check("destination is NOT target (4%)", not approx(dest, 0.04 * NAV))
check("trade-to-edge trims less than trade-to-target",
      (0.080 * NAV - dest) < (0.080 * NAV - 0.04 * NAV))

print("\n=== underweight breach → add to LOWER edge ===")
dest = _band_dest_amount(0.010 * NAV, NAV, TARGET, BAND)   # 1% position
check("1% → destination is 3% (lower edge)", approx(dest, 0.03 * NAV), f"dest={dest}")

print("\n=== band == 0 → full correction to target ===")
check("8% with band 0 → target (4%)", approx(_band_dest_amount(0.08 * NAV, NAV, TARGET, 0.0), 0.04 * NAV))
check("1% with band 0 → target (4%)", approx(_band_dest_amount(0.01 * NAV, NAV, TARGET, 0.0), 0.04 * NAV))

print("\n=== edge cases ===")
# band wider than target: lower edge would be negative → clamp at 0; but such a
# position can't breach the lower band anyway (|w-target| <= band for any w<=target).
check("non-positive NAV → None", _band_dest_amount(1000.0, 0.0, TARGET, BAND) is None)
# wide band, big overweight still trims to upper edge
check("wide band (3pp) overweight 12% → upper edge 7%",
      approx(_band_dest_amount(0.12 * NAV, NAV, TARGET, 0.03), 0.07 * NAV))

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
