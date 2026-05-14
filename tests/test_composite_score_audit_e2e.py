#!/usr/bin/env python3
"""
End-to-end composite_score audit — wiring from rank_candidates to actual picks.

The existing unit test (test_composite_score_unit.py, 30 asserts) verifies
the math kernel in isolation. This test ensures composite_score is
correctly wired through the engine's daily ranking → entry-fill path, with
deterministic factor inputs so we can predict the exact top-N picks.

What this verifies (on top of the unit test's 30 math assertions):

  E1  rank_candidates routes composite_score to _compute_composite_score.
  E2  Top-N selection respects `top_n` cap from ranking config.
  E3  `order: desc` selects the HIGHEST composite scores.
  E4  `order: asc` selects the LOWEST composite scores.
  E5  Sign convention: factor with sign="-" → low raw value → high score
      → top of the ranking when order=desc. End-to-end test on the engine.
  E6  Determinism: same config, same data → byte-identical trade ledger
      across two engine runs.
  E7  Tie handling: two symbols with identical factor values → deterministic
      ordering (not random) and both can be picked if within top_n.
  E8  z vs rank parity: standardization differences don't change ordering
      when factor values are strictly monotone (no ties).
  E9  Cross-sectional standardization: z is computed across the CANDIDATE
      set (post entry-filter), not the universe. Filtering changes z, and
      thus may change the ranking, in expected ways.
  E10 Sanity on real data: top-ranked names from a real backtest moment
      satisfy the composite criteria (e.g., high momentum names rank high
      under sign="+" momentum factor).

Approach:
  Monkey-patch _load_feature_series with synthetic per-symbol values for a
  tiny known universe. Run the engine with composite_score ranking and
  verify the trades emitted are exactly the top-N predicted by the math.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_composite_score_audit_e2e.py
"""
import copy
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import backtest_engine as be
from backtest_engine import run_backtest, _compute_composite_score

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
# Universe: 6 large-cap names, real prices from market.db (so the engine
# can do its normal price-based work). Factor values come from a monkey-patch.
# ---------------------------------------------------------------------------
UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMD", "AVGO", "INTC"]


def install_factor_patch(values_by_factor):
    """Monkey-patch _load_feature_series to return synthetic per-symbol values
    for any factor mentioned in values_by_factor, falling back to the real
    implementation for everything else.

    values_by_factor[factor][symbol] = scalar (single point dated 2024-01-01)
    """
    orig = be._load_feature_series

    def patched(fname, symbols, start, end, conn, price_index=None):
        if fname in values_by_factor:
            return {
                s: [("2024-01-01", v)]
                for s, v in values_by_factor[fname].items()
                if s in symbols
            }
        return orig(fname, symbols, start, end, conn, price_index=price_index)

    be._load_feature_series = patched
    return orig


def restore_factor_patch(orig_fn):
    be._load_feature_series = orig_fn


# ---------------------------------------------------------------------------
# E1-E5: end-to-end ranking determines actual entries
# ---------------------------------------------------------------------------
def base_strategy(extra=None, ranking_order="desc", top_n=3):
    s = {
        "name": "CompositeAudit",
        "universe": {"type": "symbols", "symbols": UNIVERSE},
        "entry": {"conditions": [{"type": "always"}], "logic": "all"},
        "ranking": {"by": "composite_score", "order": ranking_order, "top_n": top_n},
        "composite_score": {
            "standardization": "z",
            "buckets": {
                "momentum": {"weight": 1.0,
                             "factors": [{"name": "ret_12_1m", "sign": "+"}]},
            },
        },
        "rebalancing": {"frequency": "none", "rules": {}},
        "sizing": {"type": "equal_weight", "max_positions": top_n,
                    "initial_allocation": 600000},
        "backtest": {"start": "2024-01-02", "end": "2024-01-15",
                     "entry_price": "next_close", "slippage_bps": 0},
    }
    if extra:
        for k, v in extra.items():
            if isinstance(v, dict) and k in s and isinstance(s[k], dict):
                s[k] = {**s[k], **v}
            else:
                s[k] = v
    return s


# E3: order="desc" picks highest scores. Assign ret_12_1m so AAPL & MSFT &
# NVDA are the top three (high values), AMD/AVGO/INTC the bottom three.
print("\n=== E3. order='desc' picks highest composite scores ===")

orig = install_factor_patch({
    "ret_12_1m": {"AAPL": 90, "MSFT": 80, "NVDA": 70,
                   "AMD": 30, "AVGO": 20, "INTC": 10},
})
try:
    result = run_backtest(base_strategy(top_n=3, ranking_order="desc"))
    bought = [t["symbol"] for t in result["trades"] if t["action"] == "BUY"]
    top3 = set(bought[:3])
    check("desc: top-3 BUYs are AAPL/MSFT/NVDA (highest momentum)",
          top3 == {"AAPL", "MSFT", "NVDA"},
          f"got {sorted(top3)}")
    check("only 3 names bought (top_n cap)",
          len(set(bought)) == 3,
          f"unique BUYs: {sorted(set(bought))}")
finally:
    restore_factor_patch(orig)


# E4: order="asc" picks LOWEST composite scores. Same data, reversed pick.
print("\n=== E4. order='asc' picks lowest composite scores ===")

orig = install_factor_patch({
    "ret_12_1m": {"AAPL": 90, "MSFT": 80, "NVDA": 70,
                   "AMD": 30, "AVGO": 20, "INTC": 10},
})
try:
    result = run_backtest(base_strategy(top_n=3, ranking_order="asc"))
    bought = set(t["symbol"] for t in result["trades"] if t["action"] == "BUY")
    check("asc: top-3 BUYs are AMD/AVGO/INTC (lowest momentum)",
          bought == {"AMD", "AVGO", "INTC"},
          f"got {sorted(bought)}")
finally:
    restore_factor_patch(orig)


# E5: sign="-" flips the meaning. With pe values where lower is better,
# the engine should pick the LOWEST pe stocks.
print("\n=== E5. Sign='-' end-to-end: lowest pe → top picks ===")

strat = base_strategy(top_n=3, ranking_order="desc")
strat["composite_score"] = {
    "standardization": "z",
    "buckets": {
        "value": {"weight": 1.0, "factors": [{"name": "pe", "sign": "-"}]},
    },
}
orig = install_factor_patch({
    "pe": {"AAPL": 50, "MSFT": 40, "NVDA": 30,
            "AMD": 20, "AVGO": 15, "INTC": 10},  # low pe = "cheap"
})
try:
    result = run_backtest(strat)
    bought = set(t["symbol"] for t in result["trades"] if t["action"] == "BUY")
    check("sign='-' picks lowest-pe names (INTC/AVGO/AMD)",
          bought == {"INTC", "AVGO", "AMD"},
          f"got {sorted(bought)}")
finally:
    restore_factor_patch(orig)


# ---------------------------------------------------------------------------
# E6: Determinism
# ---------------------------------------------------------------------------
print("\n=== E6. Determinism — same inputs produce byte-identical trades ===")

orig = install_factor_patch({
    "ret_12_1m": {"AAPL": 90, "MSFT": 80, "NVDA": 70,
                   "AMD": 30, "AVGO": 20, "INTC": 10},
})
try:
    r1 = run_backtest(base_strategy())
    r2 = run_backtest(base_strategy())
    trades_1 = [(t["date"], t["symbol"], t["action"], t.get("price"), t.get("shares"))
                for t in r1["trades"]]
    trades_2 = [(t["date"], t["symbol"], t["action"], t.get("price"), t.get("shares"))
                for t in r2["trades"]]
    check("two identical runs produce identical trade ledgers",
          trades_1 == trades_2,
          f"first diff at index "
          f"{next((i for i,(a,b) in enumerate(zip(trades_1,trades_2)) if a!=b), 'none')}")
finally:
    restore_factor_patch(orig)


# ---------------------------------------------------------------------------
# E7: Tie handling
# ---------------------------------------------------------------------------
print("\n=== E7. Ties: identical factor values yield stable ordering ===")

# Tie between AMD, AVGO, INTC at the SAME momentum value (40).
# Top_n=3 means all three should be picked. Ordering among ties is internal
# to numpy.argsort (stable), so the result is deterministic.
orig = install_factor_patch({
    "ret_12_1m": {"AAPL": 90, "MSFT": 80, "NVDA": 70,
                   "AMD": 40, "AVGO": 40, "INTC": 40},
})
try:
    result = run_backtest(base_strategy(top_n=3, ranking_order="asc"))
    bought = set(t["symbol"] for t in result["trades"] if t["action"] == "BUY")
    check("asc with 3 tied lowest values: all 3 tied picked",
          bought == {"AMD", "AVGO", "INTC"},
          f"got {sorted(bought)}")
finally:
    restore_factor_patch(orig)


# ---------------------------------------------------------------------------
# E8: z vs rank parity on monotone data (no ties)
# ---------------------------------------------------------------------------
print("\n=== E8. z and rank give same ORDERING on strictly monotone data ===")

# Use monotonically increasing values — z and rank both produce monotone
# orderings, so the top-N picks must be identical.
orig = install_factor_patch({
    "ret_12_1m": {"AAPL": 100, "MSFT": 80, "NVDA": 60,
                   "AMD": 40, "AVGO": 20, "INTC": 0},
})
try:
    strat_z = base_strategy(top_n=3, ranking_order="desc")
    strat_z["composite_score"]["standardization"] = "z"
    out_z = run_backtest(strat_z)
    bought_z = set(t["symbol"] for t in out_z["trades"] if t["action"] == "BUY")

    strat_r = base_strategy(top_n=3, ranking_order="desc")
    strat_r["composite_score"]["standardization"] = "rank"
    out_r = run_backtest(strat_r)
    bought_r = set(t["symbol"] for t in out_r["trades"] if t["action"] == "BUY")

    check("z and rank pick the same top-3 on monotone data",
          bought_z == bought_r,
          f"z picked {sorted(bought_z)}, rank picked {sorted(bought_r)}")
finally:
    restore_factor_patch(orig)


# ---------------------------------------------------------------------------
# E9: Cross-sectional standardization across CANDIDATES, not universe
# ---------------------------------------------------------------------------
print("\n=== E9. Standardization is computed across the candidate subset ===")

# Build values where candidates {A, B, C} have z-scored uniformly within
# their subset. Verify the engine's _compute_composite_score over the
# subset matches a hand-rolled z over just those values.
orig = install_factor_patch({
    "ret_12_1m": {"AAPL": 10, "MSFT": 20, "NVDA": 30,
                   "AMD": 40, "AVGO": 50, "INTC": 60},
})
try:
    # Pass only 3 candidates → z is over those 3, not the 6-symbol universe.
    cfg = {
        "standardization": "z",
        "buckets": {"momentum": {"weight": 1.0,
                                 "factors": [{"name": "ret_12_1m", "sign": "+"}]}},
    }
    scores_subset = _compute_composite_score(
        ["AAPL", "MSFT", "NVDA"], conn=None, date="2024-01-15",
        price_index=None, composite_config=cfg,
    )
    # Hand-rolled z over [10, 20, 30]: mean=20, std=sqrt(200/3)≈8.165
    import math
    mu = 20.0
    sd = math.sqrt(((10-20)**2 + (20-20)**2 + (30-20)**2) / 3.0)
    z_a = (10 - mu) / sd
    z_m = (20 - mu) / sd
    z_n = (30 - mu) / sd
    check("z standardization for AAPL over candidates {AAPL,MSFT,NVDA}",
          approx(scores_subset["AAPL"], z_a))
    check("z standardization for MSFT over candidates {AAPL,MSFT,NVDA}",
          approx(scores_subset["MSFT"], z_m))
    check("z standardization for NVDA over candidates {AAPL,MSFT,NVDA}",
          approx(scores_subset["NVDA"], z_n))

    # Now compute over the FULL 6-symbol set. The same symbol's z should
    # be different because the standardization base changed.
    scores_full = _compute_composite_score(
        list(UNIVERSE), None, "2024-01-15", None, cfg,
    )
    check("AAPL's z over 6-symbol set differs from over 3-symbol subset",
          not approx(scores_full["AAPL"], scores_subset["AAPL"]),
          f"subset={scores_subset['AAPL']:.4f} full={scores_full['AAPL']:.4f} — "
          f"if equal, cross-sectional logic is broken")
finally:
    restore_factor_patch(orig)


# ---------------------------------------------------------------------------
# E10: Real-data sanity check — value factor on real market data
# Verifies the engine's composite_score produces sane numbers and that the
# top-picked symbol has the expected raw factor value.
# ---------------------------------------------------------------------------
print("\n=== E10. Real-data sanity: value factor (sign='-' on pe) ===")

date = "2024-06-14"
cfg_real = {
    "standardization": "z",
    "buckets": {"value": {"weight": 1.0,
                           "factors": [{"name": "pe", "sign": "-"}]}},
}
m = sqlite3.connect("/home/mohamed/alpha-scout-backend/data/market.db")
scores = _compute_composite_score(list(UNIVERSE), conn=m, date=date,
                                   price_index=None, composite_config=cfg_real)

if scores:
    # Sign='-' on pe means LOWER pe → HIGHER composite score → top pick.
    top_sym = max(scores, key=scores.get)
    bot_sym = min(scores, key=scores.get)
    top_pe = m.execute(
        "SELECT pe FROM features_daily WHERE symbol=? AND date <= ? "
        "AND pe IS NOT NULL ORDER BY date DESC LIMIT 1", (top_sym, date)
    ).fetchone()
    bot_pe = m.execute(
        "SELECT pe FROM features_daily WHERE symbol=? AND date <= ? "
        "AND pe IS NOT NULL ORDER BY date DESC LIMIT 1", (bot_sym, date)
    ).fetchone()
    m.close()

    check(f"composite top pick {top_sym} has LOWER raw pe than "
          f"composite bottom pick {bot_sym} (sign='-' inverts)",
          top_pe and bot_pe and top_pe[0] is not None and bot_pe[0] is not None
          and top_pe[0] < bot_pe[0],
          f"{top_sym}.pe={top_pe} vs {bot_sym}.pe={bot_pe}")
    check("composite scores are sane numbers (|z| <= 5 for top)",
          scores[top_sym] is not None and -5 <= scores[top_sym] <= 5,
          f"top z-score={scores[top_sym]}")
    check("composite mean across the universe ≈ 0 (z standardization)",
          approx(sum(scores.values()) / len(scores), 0.0, tol=1e-6),
          f"got mean={sum(scores.values())/len(scores):.6f}")
else:
    m.close()
    check("real-data composite returned non-empty scores",
          False, "got empty scores dict — pe data missing for these symbols")


# ---------------------------------------------------------------------------
# E11: Single candidate — should not crash, returns an empty or one-element
# scores dict depending on standardization semantics (z over a single value
# has stdev=0 → 0; rank centers it to 0).
# ---------------------------------------------------------------------------
print("\n=== E11. Single candidate — graceful degenerate handling ===")

orig = install_factor_patch({"ret_12_1m": {"AAPL": 50}})
try:
    cfg = {
        "standardization": "z",
        "buckets": {"momentum": {"weight": 1.0,
                                 "factors": [{"name": "ret_12_1m", "sign": "+"}]}},
    }
    out_single = _compute_composite_score(
        ["AAPL"], None, "2024-01-15", None, cfg
    )
    # With < 2 candidates, the standardization branch skips this factor,
    # so the bucket has no factors with values, no contribution → AAPL is
    # not in the output (no factor seen).
    check("single-candidate set: AAPL gracefully excluded from scores "
          "(< 2 needed for cross-sectional standardization)",
          "AAPL" not in out_single,
          f"got {out_single}")
finally:
    restore_factor_patch(orig)


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
