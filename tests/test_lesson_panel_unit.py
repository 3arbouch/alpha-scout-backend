#!/usr/bin/env python3
"""
Unit test: panel verdict taxonomy + per-window summarizers (no DB, deterministic).

Covers derive_verdict_panel's four shapes — unconditional (regime-independent),
regime_reversing (sign flips by regime), validated_conditional (concentrated),
rejected — and the IS/OOS window summarizers.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_lesson_panel_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "auto_trader"))

from lesson_validator import derive_verdict_panel, _summarize_windows, _oos_persistence

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


def R(n, ann, t):
    return {"n": n, "mean_ann_pct": ann, "t_stat": t, "hit_rate": 0.5}


print("=== unconditional: strong pooled, all regimes agree, none reverses ===")
agg = {
    "risk_off":     R(20, 8.0, 2.0),
    "calm_uptrend": R(20, 7.5, 1.9),
    "neutral":      R(20, 8.2, 2.1),
    "__overall__":  R(60, 7.9, 3.2),
}
v = derive_verdict_panel(agg)
check("status = unconditional", v["status"] == "unconditional", v["status"])
check("confidence high", v["validated_confidence"] == "high", str(v))
check("says regime adds no conditioning", "no conditioning" in v["regime_conditions"], v["regime_conditions"])

print("\n=== regime_reversing: holds in one, reverses in another ===")
agg = {
    "risk_off":     R(15, 9.0, 2.2),
    "calm_uptrend": R(15, -8.0, -2.1),
    "__overall__":  R(30, 0.4, 0.3),
}
v = derive_verdict_panel(agg)
check("status = regime_reversing", v["status"] == "regime_reversing", v["status"])
check("mentions holds + REVERSES",
      "holds in risk_off" in v["regime_conditions"] and "REVERSES in calm_uptrend" in v["regime_conditions"],
      v["regime_conditions"])

print("\n=== validated_conditional: concentrated (holds one, flat elsewhere) ===")
agg = {
    "risk_off":     R(15, 9.0, 2.2),     # holds
    "calm_uptrend": R(15, 0.3, 0.2),     # flat → contradicts homogeneity
    "__overall__":  R(30, 4.5, 1.7),     # pooled significant
}
v = derive_verdict_panel(agg)
check("status = validated_conditional (not unconditional)",
      v["status"] == "validated_conditional", v["status"])

print("\n=== rejected: nothing significant ===")
agg = {
    "risk_off":    R(15, 0.5, 0.3),
    "__overall__": R(15, 0.4, 0.2),
}
check("status = rejected", derive_verdict_panel(agg)["status"] == "rejected")

print("\n=== thin regime (n<8) can't flip the verdict ===")
agg = {
    "risk_off":     R(20, 8.0, 2.0),     # real, big
    "calm_uptrend": R(5, -30.0, -3.0),   # huge t but n<8 → ignored
    "neutral":      R(20, 7.8, 2.0),
    "__overall__":  R(45, 7.5, 3.0),
}
v = derive_verdict_panel(agg)
check("thin reversing regime ignored → still unconditional", v["status"] == "unconditional", v["status"])

print("\n=== per-window summarizers (IS/OOS) ===")
windows = [
    {"label": "w1", "is_oos": False, "mean_ann_pct": 8.0, "t_stat": 2.1},
    {"label": "w2", "is_oos": False, "mean_ann_pct": 6.4, "t_stat": 1.7},
    {"label": "w3", "is_oos": True,  "mean_ann_pct": 9.2, "t_stat": 2.0},
    {"label": "w4", "is_oos": True,  "mean_ann_pct": -3.1, "t_stat": -1.4},
]
s = _summarize_windows(windows)
check("summary tags IS and OOS", "IS:+8.0%(t2.1)" in s and "OOS:+9.2%(t2.0)" in s, s)
check("OOS persistence = 1/2 hold", _oos_persistence(windows) == "1/2 OOS windows hold",
      _oos_persistence(windows))
check("no-OOS → in-sample-only note",
      "in-sample only" in _oos_persistence([{"is_oos": False, "t_stat": 2.0, "mean_ann_pct": 8.0}]))

print("\n=== verdict carries window fields when per_window passed ===")
v = derive_verdict_panel({"risk_off": R(20, 8, 2.0), "neutral": R(20, 8, 2.0),
                          "__overall__": R(40, 8, 3.0)}, windows)
check("windows_summary present", "windows_summary" in v and v["windows_summary"])
check("oos_persistence present", v.get("oos_persistence") == "1/2 OOS windows hold", str(v.get("oos_persistence")))

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
