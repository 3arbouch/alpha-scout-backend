#!/usr/bin/env python3
"""
Unit test (Gap 6): _compute_composite_score end-to-end math.

We monkey-patch _load_feature_series to return synthetic factor values
for a known symbol set, then verify the engine's composite scores against
hand-computed expected values.

Verified behaviors:
  1. Bucket-weight normalization (raw weights → Σ=1)
  2. Standardization branches (z and rank)
  3. Sign flip ("-" inverts the factor's contribution)
  4. Per-bucket mean over (sign · z), then cross-bucket weighted sum
  5. NaN handling — missing factor → bucket-mean over what's present;
     entire bucket missing → contributes 0;
     all factors missing → symbol absent from output
  6. Ranking ordering matches expected (top-N selection works)

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_composite_score_unit.py
"""
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import backtest_engine as be

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
    if a is None or b is None:
        return False
    return abs(a - b) < tol


# ---------------------------------------------------------------------------
# Fixture: synthetic factor values per (factor, symbol)
# ---------------------------------------------------------------------------
#
# _load_feature_series returns: {symbol: [(date_str, value), ...]}
# The composite-score path bisects to find the most recent point <= `date`.
# We return a single point dated 2024-01-01 for every symbol.

FACTOR_VALUES = {
    # symmetric values → mean=0, std=sqrt((1+9+1+9)/4) = sqrt(5) ≈ 2.236
    "rev_yoy":       {"A": -3.0, "B": -1.0, "C": 1.0, "D": 3.0},
    # different scale, different ordering
    "earnings_yoy":  {"A": -1.0, "B": 1.0, "C": -1.0, "D": 1.0},   # mean=0, std=1
    "op_margin":     {"A": 0.30, "B": 0.20, "C": 0.10, "D": 0.40}, # mean=0.25, std≈0.1118
    "debt_ratio":    {"A": 0.10, "B": 0.30, "C": 0.20, "D": 0.50}, # lower is better → sign=-
}


def fake_load_feature_series(fname, symbols, start, end, conn, price_index=None):
    """Drop-in replacement for _load_feature_series. Returns one data point
    per symbol on 2024-01-01."""
    return {
        s: [("2024-01-01", FACTOR_VALUES.get(fname, {}).get(s))]
        for s in symbols
        if FACTOR_VALUES.get(fname, {}).get(s) is not None
    }


# Install monkey patch
be._load_feature_series = fake_load_feature_series

import numpy as np


def hand_z(vals: list[float]) -> list[float]:
    """Pure-Python z-score (ddof=0), same as backtest_engine.py:986-987."""
    a = np.array(vals, dtype=np.float64)
    mu = a.mean()
    sd = a.std(ddof=0)
    if sd == 0:
        return [0.0] * len(vals)
    return ((a - mu) / sd).tolist()


# ---------------------------------------------------------------------------
# 1. Single bucket, single factor, sign='+', standardization='z'
# ---------------------------------------------------------------------------
print("\n=== 1. Single bucket, single factor ===")

cfg_1f = {
    "standardization": "z",
    "buckets": {
        "growth": {"weight": 1.0, "factors": [{"name": "rev_yoy", "sign": "+"}]},
    },
}
symbols = ["A", "B", "C", "D"]
out = be._compute_composite_score(symbols, conn=None, date="2024-01-01",
                                   price_index=None, composite_config=cfg_1f)

# rev_yoy = [-3, -1, 1, 3] for [A,B,C,D] → z = [-1.342, -0.447, 0.447, 1.342]
expected_z = hand_z([-3.0, -1.0, 1.0, 3.0])
for sym, z in zip(symbols, expected_z):
    check(f"symbol {sym}: score == z(rev_yoy)={z:.4f}", approx(out[sym], z))


# ---------------------------------------------------------------------------
# 2. Sign flip: same factor with sign='-' produces negated scores
# ---------------------------------------------------------------------------
print("\n=== 2. Sign='-' flips the contribution ===")
cfg_flip = {
    "standardization": "z",
    "buckets": {
        "growth": {"weight": 1.0, "factors": [{"name": "rev_yoy", "sign": "-"}]},
    },
}
out = be._compute_composite_score(symbols, None, "2024-01-01", None, cfg_flip)
for sym, z in zip(symbols, expected_z):
    check(f"symbol {sym}: sign='-' negates → -z={-z:.4f}", approx(out[sym], -z))


# ---------------------------------------------------------------------------
# 3. Two factors in one bucket → bucket score is the mean of (sign·z)
# ---------------------------------------------------------------------------
print("\n=== 3. Two factors in one bucket → bucket mean ===")
cfg_2f = {
    "standardization": "z",
    "buckets": {
        "growth": {
            "weight": 1.0,
            "factors": [
                {"name": "rev_yoy", "sign": "+"},
                {"name": "earnings_yoy", "sign": "+"},
            ],
        },
    },
}
out = be._compute_composite_score(symbols, None, "2024-01-01", None, cfg_2f)

z_rev = dict(zip(symbols, hand_z([-3.0, -1.0, 1.0, 3.0])))
z_eps = dict(zip(symbols, hand_z([-1.0, 1.0, -1.0, 1.0])))
for sym in symbols:
    expected = (z_rev[sym] + z_eps[sym]) / 2.0
    check(f"{sym}: bucket score = mean(z_rev, z_eps) = {expected:.4f}",
          approx(out[sym], expected))


# ---------------------------------------------------------------------------
# 4. Two buckets, raw weights normalize to sum 1
# ---------------------------------------------------------------------------
print("\n=== 4. Two buckets: raw weights (3, 2) → normalized (0.6, 0.4) ===")
cfg_2b = {
    "standardization": "z",
    "buckets": {
        "growth": {"weight": 3.0, "factors": [{"name": "rev_yoy", "sign": "+"}]},
        "quality": {"weight": 2.0, "factors": [{"name": "op_margin", "sign": "+"}]},
    },
}
out = be._compute_composite_score(symbols, None, "2024-01-01", None, cfg_2b)

z_op = dict(zip(symbols, hand_z([0.30, 0.20, 0.10, 0.40])))
for sym in symbols:
    expected = 0.6 * z_rev[sym] + 0.4 * z_op[sym]
    check(f"{sym}: 0.6·z_rev + 0.4·z_op = {expected:.4f}",
          approx(out[sym], expected))


# ---------------------------------------------------------------------------
# 5. Sign flip inside multi-bucket composite
# ---------------------------------------------------------------------------
print("\n=== 5. Multi-bucket with sign flip on debt_ratio ===")
cfg_full = {
    "standardization": "z",
    "buckets": {
        "growth": {
            "weight": 0.6,
            "factors": [{"name": "rev_yoy", "sign": "+"}],
        },
        "quality": {
            "weight": 0.4,
            "factors": [
                {"name": "op_margin", "sign": "+"},
                {"name": "debt_ratio", "sign": "-"},  # lower is better
            ],
        },
    },
}
out = be._compute_composite_score(symbols, None, "2024-01-01", None, cfg_full)

z_debt = dict(zip(symbols, hand_z([0.10, 0.30, 0.20, 0.50])))
for sym in symbols:
    bucket_growth = z_rev[sym]
    bucket_quality = (z_op[sym] + (-z_debt[sym])) / 2.0
    expected = 0.6 * bucket_growth + 0.4 * bucket_quality
    check(f"{sym}: full composite = {expected:.4f}",
          approx(out[sym], expected))


# ---------------------------------------------------------------------------
# 6. Symbol missing one factor in a bucket → bucket-mean over what's present
# ---------------------------------------------------------------------------
print("\n=== 6. Symbol missing one factor in a bucket ===")
# Drop symbol "B" from earnings_yoy → bucket_growth for B should use z_rev only
saved = FACTOR_VALUES["earnings_yoy"].copy()
del FACTOR_VALUES["earnings_yoy"]["B"]

# Re-standardize earnings_yoy over the 3 remaining symbols
remaining_syms = ["A", "C", "D"]
remaining_vals = [FACTOR_VALUES["earnings_yoy"][s] for s in remaining_syms]
z_eps_partial = dict(zip(remaining_syms, hand_z(remaining_vals)))

out = be._compute_composite_score(symbols, None, "2024-01-01", None, cfg_2f)

# B's bucket score should be z_rev[B] only (no earnings_yoy available)
check(f"B (missing earnings_yoy): bucket = z_rev[B] = {z_rev['B']:.4f}",
      approx(out["B"], z_rev["B"]))
# A's bucket score: mean(z_rev[A], z_eps_partial[A])
exp_A = (z_rev["A"] + z_eps_partial["A"]) / 2.0
check(f"A (has both, z_eps re-standardized over {{A,C,D}}): bucket = {exp_A:.4f}",
      approx(out["A"], exp_A))
FACTOR_VALUES["earnings_yoy"] = saved  # restore


# ---------------------------------------------------------------------------
# 7. Entire bucket missing for a symbol → contributes 0 (other buckets still count)
# ---------------------------------------------------------------------------
print("\n=== 7. Entire bucket missing for symbol → contributes 0 ===")
# Drop B from both growth factors
saved_rev = FACTOR_VALUES["rev_yoy"].copy()
saved_eps = FACTOR_VALUES["earnings_yoy"].copy()
del FACTOR_VALUES["rev_yoy"]["B"]
del FACTOR_VALUES["earnings_yoy"]["B"]

cfg_two_buckets_full = {
    "standardization": "z",
    "buckets": {
        "growth": {
            "weight": 0.6,
            "factors": [
                {"name": "rev_yoy", "sign": "+"},
                {"name": "earnings_yoy", "sign": "+"},
            ],
        },
        "quality": {
            "weight": 0.4,
            "factors": [{"name": "op_margin", "sign": "+"}],
        },
    },
}
out = be._compute_composite_score(symbols, None, "2024-01-01", None, cfg_two_buckets_full)

# B has no growth factors but op_margin is present → composite = 0.4 * z_op[B]
# (growth bucket contributes 0, but the cross-bucket weighted sum still
# uses growth's weight=0.6 — meaning growth's missing bucket is treated as 0
# contribution, NOT as "re-normalize remaining bucket weights")
# Re-standardize op_margin over symbols present {A, B, C, D} (still all present)
z_op_full = dict(zip(symbols, hand_z([0.30, 0.20, 0.10, 0.40])))
expected_B = 0.4 * z_op_full["B"]  # growth bucket contributes 0
check(f"B (no growth factors, op_margin present): score = 0.4·z_op[B] = {expected_B:.4f}",
      approx(out["B"], expected_B))

# Re-standardize rev_yoy and earnings_yoy over remaining {A, C, D}
z_rev_3 = dict(zip(["A", "C", "D"], hand_z([-3.0, 1.0, 3.0])))
z_eps_3 = dict(zip(["A", "C", "D"], hand_z([-1.0, -1.0, 1.0])))
bucket_growth_A = (z_rev_3["A"] + z_eps_3["A"]) / 2.0
expected_A = 0.6 * bucket_growth_A + 0.4 * z_op_full["A"]
check(f"A (both factors present, re-standardized over {{A,C,D}}): {expected_A:.4f}",
      approx(out["A"], expected_A))

FACTOR_VALUES["rev_yoy"] = saved_rev
FACTOR_VALUES["earnings_yoy"] = saved_eps


# ---------------------------------------------------------------------------
# 8. Symbol missing every factor in every bucket → not in output
# ---------------------------------------------------------------------------
print("\n=== 8. Symbol missing all factors → excluded from output ===")
saved_all = {k: v.copy() for k, v in FACTOR_VALUES.items()}
for fname in FACTOR_VALUES:
    FACTOR_VALUES[fname].pop("B", None)

out = be._compute_composite_score(symbols, None, "2024-01-01", None, cfg_full)
check("B (all factors missing) → not in scores", "B" not in out,
      f"unexpected: B in output = {out.get('B')!r}")
check("A still scored", "A" in out)

for k, v in saved_all.items():
    FACTOR_VALUES[k] = v


# ---------------------------------------------------------------------------
# 9. Standardization = 'rank' produces a different ordering than 'z'
# ---------------------------------------------------------------------------
print("\n=== 9. Standardization='rank' returns scores too ===")
cfg_rank = {
    "standardization": "rank",
    "buckets": {
        "growth": {"weight": 1.0, "factors": [{"name": "rev_yoy", "sign": "+"}]},
    },
}
out_rank = be._compute_composite_score(symbols, None, "2024-01-01", None, cfg_rank)

# rank-based should still preserve ordering: D > C > B > A for rev_yoy ascending
ranked = sorted(out_rank.items(), key=lambda kv: kv[1])
ordered_syms = [s for s, _ in ranked]
check("rank standardization preserves the rev_yoy ordering (A<B<C<D)",
      ordered_syms == ["A", "B", "C", "D"],
      f"got {ordered_syms}")
check("rank scores have mean ≈ 0 (centered)",
      approx(sum(out_rank.values()) / len(out_rank), 0.0, tol=1e-9))


# ---------------------------------------------------------------------------
# 10. Empty buckets / no factors → empty output
# ---------------------------------------------------------------------------
print("\n=== 10. Defensive edge cases ===")
out = be._compute_composite_score(symbols, None, "2024-01-01", None,
                                   {"standardization": "z", "buckets": {}})
check("empty buckets → empty output", out == {})

cfg_no_factors = {
    "standardization": "z",
    "buckets": {"growth": {"weight": 1.0, "factors": []}},
}
out = be._compute_composite_score(symbols, None, "2024-01-01", None, cfg_no_factors)
check("bucket with empty factors → empty output", out == {})


print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
