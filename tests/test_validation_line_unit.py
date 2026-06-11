#!/usr/bin/env python3
"""
Unit test: _validation_line — the passive-context validation tag (no DB).

Verifies the trader's history context surfaces a memo item's out-of-sample
validation status: unvalidated candidate (warn), validated (act), validated
only-in-regime (conditional), failed (reject), and no-status (omit).

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_validation_line_unit.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from auto_trader.analyst import _validation_line

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


print("=== no validation_status → omitted ===")
check("None status → None", _validation_line({"claim": "x"}) is None)
check("empty status → None", _validation_line({"validation_status": ""}) is None)

print("\n=== candidate → warn, untested ===")
line = _validation_line({"validation_status": "candidate"})
check("mentions UNVALIDATED", line and "UNVALIDATED" in line, str(line))
check("warns it's a hypothesis", line and "hypothesis" in line.lower(), str(line))

print("\n=== validated → act, carries OOS confidence ===")
line = _validation_line({"validation_status": "validated", "validated_confidence": "high"})
check("says held out-of-sample", line and "out-of-sample" in line, str(line))
check("shows OOS confidence", line and "high" in line, str(line))

print("\n=== validated_conditional → surfaces the regime summary ===")
# regime_conditions is the validator's human-readable string (NOT JSON).
line = _validation_line({
    "validation_status": "validated_conditional",
    "validated_confidence": "medium",
    "regime_conditions": "holds in risk_off (+12.3% ann, t=2.1)",
})
check("says conditional", line and "CONDITIONALLY" in line, str(line))
check("carries regime summary", line and "risk_off" in line, str(line))
check("scopes to regime holding", line and "regime currently holds" in line, str(line))

# missing regime_conditions → still conditional, no bracket noise
line = _validation_line({"validation_status": "validated_conditional"})
check("no regimes → still conditional, no empty brackets",
      line and "CONDITIONALLY" in line and "[]" not in line, str(line))

print("\n=== rejected / unknown → reject ===")
line = _validation_line({
    "validation_status": "rejected",
    "regime_conditions": "no regime shows a meaningful effect",
})
check("says did NOT hold", line and "did NOT hold" in line, str(line))
check("says do not rely", line and "Do not rely" in line, str(line))

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
