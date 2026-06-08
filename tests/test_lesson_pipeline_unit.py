#!/usr/bin/env python3
"""
Unit test: lesson_pipeline.operationalize_claim (Phase 2, the platitude filter).
Deterministic, no DB.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_lesson_pipeline_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "auto_trader"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from lesson_pipeline import operationalize_claim

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


KNOWN = {"ret_12_1m", "ev_ebitda", "roe", "div_yield"}
GOOD = {"test_spec": {"primary_factor": "ret_12_1m", "conditioning_factor": "ev_ebitda",
                      "horizon_days": 63, "hypothesis": "cheap_beats_expensive"}}

print("=== accepts a well-formed interaction spec ===")
spec = operationalize_claim(GOOD, known_factors=KNOWN)
check("returns a normalized spec", spec is not None)
check("defaults primary_bucket to top_quintile", spec and spec["primary_bucket"] == "top_quintile")

print("\n=== rejects un-speccable / malformed claims (the platitude filter) ===")
check("no test_spec → None", operationalize_claim({"claim": "avoid hype"}) is None)
check("primary == conditioning → None",
      operationalize_claim({"test_spec": {**GOOD["test_spec"], "conditioning_factor": "ret_12_1m"}}) is None)
check("non-positive horizon → None",
      operationalize_claim({"test_spec": {**GOOD["test_spec"], "horizon_days": 0}}) is None)
check("unknown hypothesis → None",
      operationalize_claim({"test_spec": {**GOOD["test_spec"], "hypothesis": "vibes"}}) is None)
check("unknown factor (gated) → None",
      operationalize_claim({"test_spec": {**GOOD["test_spec"], "conditioning_factor": "made_up"}},
                           known_factors=KNOWN) is None)
check("missing required field → None",
      operationalize_claim({"test_spec": {"primary_factor": "ret_12_1m", "horizon_days": 63}}) is None)

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
