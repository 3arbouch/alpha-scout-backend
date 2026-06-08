#!/usr/bin/env python3
"""
Unit test: lesson_validator pure core (double-sort spread, per-regime
aggregation, verdict derivation). No DB — deterministic.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_lesson_validator_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "auto_trader"))

from lesson_validator import _double_sort_spread, _aggregate, derive_verdict

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


def approx(a, b, tol=1e-6):
    return a is not None and abs(a - b) < tol


print("=== _double_sort_spread: cheap beats expensive within top-momentum ===")
# 50 names; momentum = index → top quintile = s40..s49. Among those, conditioning
# 1..10 (s40 cheapest). m = 10//5 = 2 → cheap={s40,s41}, expensive={s48,s49}.
xs = []
top_cond = {40: 1, 41: 2, 42: 3, 43: 4, 44: 5, 45: 6, 46: 7, 47: 8, 48: 9, 49: 10}
for i in range(50):
    cond = top_cond.get(i, 100)          # non-top names irrelevant
    xs.append((f"s{i}", float(i), float(cond)))
fwd = {"s40": 0.10, "s41": 0.10, "s48": -0.05, "s49": -0.05}
spread = _double_sort_spread(xs, lambda s: fwd.get(s, 0.0))
check("cheap−expensive spread = +0.15", approx(spread, 0.15), f"got {spread}")

print("\n=== _double_sort_spread: too-thin cross-section → None ===")
check("n<25 returns None", _double_sort_spread(xs[:10], lambda s: 0.0) is None)

print("\n=== _aggregate: per-regime grouping + annualization ===")
per_date = {"2020-01-01": 0.02, "2020-02-01": 0.04, "2020-03-01": -0.06}
labels = {"2020-01-01": ["calm_uptrend"], "2020-02-01": ["calm_uptrend"], "2020-03-01": ["risk_off"]}
agg = _aggregate(per_date, labels, horizon_days=63)   # ann = 252/63 = 4
check("calm_uptrend n=2", agg["calm_uptrend"]["n"] == 2)
check("calm_uptrend mean_ann = +12.0%", approx(agg["calm_uptrend"]["mean_ann_pct"], 12.0), str(agg["calm_uptrend"]))
check("risk_off mean_ann = -24.0%", approx(agg["risk_off"]["mean_ann_pct"], -24.0), str(agg["risk_off"]))
check("overall mean_ann ≈ 0", approx(agg["__overall__"]["mean_ann_pct"], 0.0), str(agg["__overall__"]))
check("risk_off hit_rate = 0", approx(agg["risk_off"]["hit_rate"], 0.0))

print("\n=== derive_verdict: conditional (holds one regime, reverses another) ===")
agg2 = {
    "risk_off":     {"n": 10, "mean_ann_pct": 6.4, "t_stat": 2.3, "hit_rate": 0.7},
    "calm_uptrend": {"n": 10, "mean_ann_pct": -9.1, "t_stat": -2.8, "hit_rate": 0.3},
    "__overall__":  {"n": 20, "mean_ann_pct": 0.4, "t_stat": 0.4, "hit_rate": 0.52},
}
v = derive_verdict(agg2)
check("status = validated_conditional", v["status"] == "validated_conditional", v["status"])
check("conditions mention holds in risk_off", "holds in risk_off" in v["regime_conditions"], v["regime_conditions"])
check("conditions mention REVERSES in calm_uptrend", "REVERSES in calm_uptrend" in v["regime_conditions"], v["regime_conditions"])

print("\n=== derive_verdict: nothing significant → rejected ===")
agg3 = {
    "risk_off":    {"n": 10, "mean_ann_pct": 0.5, "t_stat": 0.3, "hit_rate": 0.5},
    "__overall__": {"n": 10, "mean_ann_pct": 0.4, "t_stat": 0.2, "hit_rate": 0.5},
}
check("status = rejected", derive_verdict(agg3)["status"] == "rejected")

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
