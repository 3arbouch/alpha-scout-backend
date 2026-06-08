#!/usr/bin/env python3
"""
Unit test: weekly rebalance cadence in is_rebalance_date.

Verifies the new "weekly" frequency (>=7 days since last rebalance) without
disturbing the existing quarterly/monthly/none semantics.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_weekly_rebalance_cadence_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from backtest_engine import is_rebalance_date

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


LAST = "2026-01-01"

print("=== weekly fires at >=7 days since last rebalance ===")
check("6 days → no rebalance", is_rebalance_date("2026-01-07", LAST, "weekly") is False)
check("7 days → rebalance", is_rebalance_date("2026-01-08", LAST, "weekly") is True)
check("14 days → rebalance", is_rebalance_date("2026-01-15", LAST, "weekly") is True)
check("no last_rebal → never (needs an initial entry first)",
      is_rebalance_date("2026-06-01", "", "weekly") is False)

print("\n=== existing frequencies unchanged (no regression) ===")
check("quarterly: 89d no", is_rebalance_date("2026-03-31", LAST, "quarterly") is False)
check("quarterly: 90d yes", is_rebalance_date("2026-04-01", LAST, "quarterly") is True)
check("monthly: 29d no", is_rebalance_date("2026-01-30", LAST, "monthly") is False)
check("monthly: 30d yes", is_rebalance_date("2026-01-31", LAST, "monthly") is True)
check("none → never", is_rebalance_date("2027-01-01", LAST, "none") is False)
check("weekly cadence is tighter than monthly at day 7",
      is_rebalance_date("2026-01-08", LAST, "weekly") is True
      and is_rebalance_date("2026-01-08", LAST, "monthly") is False)

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
