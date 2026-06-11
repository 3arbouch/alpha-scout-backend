#!/usr/bin/env python3
"""
Unit test: FeatureName (entry-condition allow-list) is derived from the factor
registry — no drift. FeatureName feeds feature_threshold / feature_percentile
conditions; it used to be a hand-kept Literal that fell out of sync with
server/factors (e.g. gross_profitability was usable in composite_score ranking
but rejected as an entry filter). This guards that they stay identical.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_featurename_registry_unit.py
"""
import os
import sys
from typing import get_args

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from server.models.strategy import FeatureName
from server.factors import all_features

PASS = FAIL = 0


def check(label, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {label}")
    else:
        FAIL += 1
        print(f"  FAIL  {label}  {extra}")


feature_names = set(get_args(FeatureName))
registry_names = {f.name for f in all_features()}

print("=== FeatureName is derived from the registry (no drift) ===")
check("FeatureName == registry feature set",
      feature_names == registry_names,
      f"only in FeatureName: {sorted(feature_names - registry_names)}; "
      f"only in registry: {sorted(registry_names - feature_names)}")

check("FeatureName is non-empty", len(feature_names) > 0, str(len(feature_names)))

print("\n=== newly added factors flow through automatically ===")
for name in ("gross_profitability", "realized_vol_60", "realized_vol_252"):
    check(f"{name} usable as entry-condition filter",
          name in feature_names, f"present={name in registry_names} in registry")

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
