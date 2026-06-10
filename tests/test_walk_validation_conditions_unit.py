#!/usr/bin/env python3
"""
Unit test: walk-validation conditions — `<metric>.<aggregator>` constraints over
the per-window eval aggregates (e.g. `alpha_ann_pct.min > 3` = alpha above 3% in
EVERY walk-forward window). No DB.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_walk_validation_conditions_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from auto_trader.runner import (
    parse_conditions, check_conditions, conditions_namespace, _aggregate_window_metrics,
)

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


def windows(alphas, sharpes=None):
    sharpes = sharpes or [1.0] * len(alphas)
    return [{"metrics": {"alpha_ann_pct": a, "sharpe_ratio": s}} for a, s in zip(alphas, sharpes)]


OVERALL = {"alpha_ann_pct": 4.25, "sharpe_ratio": 1.0}

print("=== parse_conditions handles dotted metric names ===")
c = parse_conditions(["alpha_ann_pct.min > 3"])[0]
check("parses 'alpha_ann_pct.min > 3'", c == {"metric": "alpha_ann_pct.min", "operator": ">", "value": 3.0}, str(c))

print("\n=== conditions_namespace flattens overall + per-window aggregates ===")
agg = _aggregate_window_metrics(windows([5.0, 4.0, 2.0, 6.0]))
ns = conditions_namespace(OVERALL, agg)
check("bare name = overall value", ns["alpha_ann_pct"] == 4.25, str(ns.get("alpha_ann_pct")))
check("alpha_ann_pct.min present = worst window", abs(ns["alpha_ann_pct.min"] - 2.0) < 1e-9, str(ns.get("alpha_ann_pct.min")))
check("alpha_ann_pct.max = best window", abs(ns["alpha_ann_pct.max"] - 6.0) < 1e-9, str(ns.get("alpha_ann_pct.max")))
check("alpha_ann_pct.mean = 4.25", abs(ns["alpha_ann_pct.mean"] - 4.25) < 1e-9, str(ns.get("alpha_ann_pct.mean")))

print("\n=== 'alpha_ann_pct.min > 3' = consistency across ALL windows ===")
# one window at 2% → worst window fails the 3% floor
met, _ = check_conditions(conditions_namespace(OVERALL, _aggregate_window_metrics(windows([5.0, 4.0, 2.0, 6.0]))),
                          parse_conditions(["alpha_ann_pct.min > 3"]))
check("min=2 → NOT met (one weak window)", met is False)
# every window ≥ 3.5 → passes
met, _ = check_conditions(conditions_namespace(OVERALL, _aggregate_window_metrics(windows([5.0, 4.0, 3.5, 6.0]))),
                          parse_conditions(["alpha_ann_pct.min > 3"]))
check("all windows > 3 → met", met is True)

print("\n=== composition: optimize-agnostic gate of median Sharpe + min alpha ===")
agg2 = _aggregate_window_metrics(windows([4.0, 3.5, 5.0, 6.0], sharpes=[1.3, 1.1, 1.4, 1.2]))
met, detail = check_conditions(conditions_namespace(OVERALL, agg2),
                               parse_conditions(["alpha_ann_pct.min > 3", "sharpe_ratio.median > 1.1"]))
check("min alpha 3.5>3 AND median sharpe>1.1 → met", met is True, str([(d['metric'], d['actual'], d['met']) for d in detail]))

print("\n=== backward compatibility: bare names still = overall period ===")
met, _ = check_conditions(conditions_namespace(OVERALL, agg), parse_conditions(["alpha_ann_pct > 0"]))
check("bare 'alpha_ann_pct > 0' → met (overall 4.25)", met is True)
met, _ = check_conditions(conditions_namespace(OVERALL, agg), parse_conditions(["sharpe_ratio > 1.1"]))
check("bare 'sharpe_ratio > 1.1' → NOT met (overall 1.0)", met is False)

print("\n=== no eval block → per-window condition is unmet (not silently passed) ===")
met, detail = check_conditions(conditions_namespace(OVERALL, {}), parse_conditions(["alpha_ann_pct.min > 3"]))
check("no aggregates → '.min' absent → NOT met", met is False and detail[0]["actual"] is None, str(detail))

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
