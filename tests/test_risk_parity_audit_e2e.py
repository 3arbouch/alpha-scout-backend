#!/usr/bin/env python3
"""
risk_parity sizing — end-to-end audit.

The math kernel is covered by test_engine_kernels_unit.py (with a hand-copied
formula replica) and compute_realized_vol is covered by test_stop_pricing_unit.py.
This file fills the gap: prove the engine WIRES risk_parity correctly using
real-data prices, and that observed position dollar amounts match the
inverse-vol math exactly.

What this verifies:

  R1   Vol is computed from prices STRICTLY BEFORE the entry-fill date
       (no same-day lookahead in the vol estimate).
  R2   Inverse-vol weights sum to 1 across the batch.
  R3   Low-vol names get LARGER dollar allocations than high-vol names.
  R4   Each filled position's notional ≈ pool × weight (within slippage drag).
  R5   Sum of filled positions' notionals ≈ pool = n_batch / max_positions × NAV.
  R6   compute_realized_vol matches numpy.std(log_returns, ddof=1) for the
       'historical' source.
  R7   Symbols with insufficient history fall back to equal-weight slot.
  R8   Increasing vol_window_days widens the window used for sigma.
  R9   Vol uses LOG returns, not arithmetic — verified against numpy ground truth.
  R10  End-to-end run completes; trade ledger satisfies the standard invariants
       (cum shares ≥ 0, finite numbers, max_positions cap respected).

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_risk_parity_audit_e2e.py
"""
import copy
import math
import os
import sqlite3
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import numpy as np

from backtest_engine import run_backtest
from stop_pricing import compute_realized_vol

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


def approx(a, b, tol=1e-6):
    return a is not None and b is not None and abs(a - b) < tol


# ---------------------------------------------------------------------------
# Universe: 15 names — mix of large-cap stable + smaller tech + recent IPOs.
# Enough vol-dispersion that inverse-vol allocations differ visibly.
# ---------------------------------------------------------------------------
UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",  # mega-cap, mid vol
    "JNJ", "PG", "KO", "WMT", "PEP",            # defensive, low vol
    "NVDA", "AMD", "TSLA",                       # high-vol semis/tech
    "COIN", "ARM",                                # recent IPOs, higher vol
]


def strat(max_pos=15, capital=1_500_000, window_days=20):
    return {
        "name": "RiskParityProbe",
        "universe": {"type": "symbols", "symbols": UNIVERSE},
        "entry": {"conditions": [{"type": "always"}], "logic": "all"},
        "ranking": {"by": "momentum_rank", "order": "desc", "top_n": max_pos},
        "rebalancing": {"frequency": "none", "rules": {}},
        "sizing": {
            "type": "risk_parity",
            "max_positions": max_pos,
            "initial_allocation": capital,
            "vol_window_days": window_days,
        },
        "backtest": {
            "start": "2024-06-03", "end": "2024-06-14",
            "entry_price": "next_close", "slippage_bps": 10,
        },
    }


# ---------------------------------------------------------------------------
# R6 + R9: compute_realized_vol matches numpy
# ---------------------------------------------------------------------------
print("\n=== R6, R9. compute_realized_vol — numpy parity (historical source) ===")

rng = np.random.default_rng(7)
prices = [100.0]
for _ in range(50):
    r = rng.normal(0, 0.02)
    prices.append(prices[-1] * (1 + r))

# Historical: sample stdev (ddof=1) of log returns over the last `window_days`.
window = 20
tail = prices[-(window + 1):]
log_rets = [math.log(tail[i] / tail[i - 1]) for i in range(1, len(tail))]
expected_sigma = float(np.std(log_rets, ddof=1))
got = compute_realized_vol(prices, window, "historical")
check(f"sigma matches numpy.std(log_rets, ddof=1): got={got:.6f}, expected={expected_sigma:.6f}",
      approx(got, expected_sigma, tol=1e-9))

# Insufficient history
check("vol_window > closes-1 → None",
      compute_realized_vol([100.0, 101.0], 20, "historical") is None)
check("non-positive prices → None",
      compute_realized_vol([100.0] * 10 + [-1.0] * 11, 20, "historical") is None)


# ---------------------------------------------------------------------------
# R10: end-to-end engine run with real-data prices
# ---------------------------------------------------------------------------
print("\n=== R10. Engine runs cleanly with risk_parity, max_positions=15 ===")

result = run_backtest(strat(max_pos=15, capital=1_500_000))
trades = result["trades"]
buys = [t for t in trades if t["action"] == "BUY"]
check("at least 1 BUY emitted", len(buys) >= 1, f"got {len(buys)}")
check(f"≤ max_positions BUYs on the first fill day "
      f"(actual fill day = day after start = 2024-06-04)",
      len([t for t in buys if t["date"] == "2024-06-04"]) <= 15)
check("no negative shares / non-finite fields",
      all(t["shares"] > 0 and math.isfinite(float(t["price"])) and
          math.isfinite(float(t.get("amount", 0))) for t in buys))


# ---------------------------------------------------------------------------
# R1, R2, R3, R4, R5: position dollar amounts reproduce the inverse-vol math
# ---------------------------------------------------------------------------
print("\n=== R1-R5. Position notional == pool × inverse-vol weight ===")

# Pull the first day's BUYs (the batch sized by risk-parity)
first_day = min(t["date"] for t in buys)
batch = [t for t in buys if t["date"] == first_day]
n_batch = len(batch)
print(f"  first fill day: {first_day}  ({n_batch} positions in batch)")

# Compute expected sigma per symbol from REAL prices STRICTLY before first_day.
# This is what the engine does at scripts/backtest_engine.py:2358-2363.
m = sqlite3.connect("/home/mohamed/alpha-scout-backend/data/market.db")
expected_sigmas = {}
for t in batch:
    sym = t["symbol"]
    rows = m.execute(
        "SELECT date, close FROM prices WHERE symbol=? AND date < ? "
        "AND close IS NOT NULL ORDER BY date DESC LIMIT 21",
        (sym, first_day),
    ).fetchall()
    if len(rows) < 21:
        expected_sigmas[sym] = None
        continue
    closes = [r[1] for r in reversed(rows)]
    expected_sigmas[sym] = compute_realized_vol(closes, 20, "historical")
m.close()

n_with_vol = sum(1 for v in expected_sigmas.values() if v is not None and v > 0)
check(f"all {len(batch)} batch symbols had ≥21 prior closes for vol estimation "
      f"({n_with_vol} usable sigmas)",
      n_with_vol == len(batch),
      f"missing vol: {[s for s, v in expected_sigmas.items() if not v]}")

# Hand-compute inverse-vol weights for the batch
inv = {s: 1.0 / v for s, v in expected_sigmas.items() if v and v > 0}
total_inv = sum(inv.values())
weights = {s: i / total_inv for s, i in inv.items()}
check("R2 inverse-vol weights sum to 1",
      approx(sum(weights.values()), 1.0, tol=1e-9))

# R3: lowest sigma should have the largest weight
syms_by_vol = sorted(expected_sigmas, key=expected_sigmas.get)
lowest_vol = syms_by_vol[0]
highest_vol = syms_by_vol[-1]
check(f"R3 lowest-vol {lowest_vol} (σ={expected_sigmas[lowest_vol]:.4f}) has the LARGEST weight",
      weights[lowest_vol] == max(weights.values()))
check(f"R3 highest-vol {highest_vol} (σ={expected_sigmas[highest_vol]:.4f}) has the SMALLEST weight",
      weights[highest_vol] == min(weights.values()))

# R4 + R5: notional per trade == pool × weight, total ≈ pool.
# pool = n_batch / max_positions × NAV_at_batch_start.
#   The engine recomputes NAV inside the per-symbol loop AFTER each fill, so
#   the pool shrinks slightly each iteration due to slippage drag. The first
#   trade in the batch uses the pre-batch NAV.
initial_cap = 1_500_000
max_pos = 15
pool_pre = (n_batch / max_pos) * initial_cap   # NAV starts at initial_cap

# Compute observed notional per trade (shares × price)
observed_notional = {}
for t in batch:
    sym = t["symbol"]
    observed_notional[sym] = float(t["shares"]) * float(t["price"])

# Aggregate total deployed ≈ pool (small slippage / cash-buffer drag allowed)
total_deployed = sum(observed_notional.values())
expected_total = pool_pre
deploy_pct_err = abs(total_deployed - expected_total) / expected_total * 100
check(f"R5 total deployed {total_deployed:,.0f} ≈ pool {expected_total:,.0f}  "
      f"(within 2% drag, observed {deploy_pct_err:.2f}%)",
      deploy_pct_err < 2.0,
      f"too much drift")

# R4: per-symbol notional ≈ pool × weight (allow ~2% tolerance for the
# NAV-shrinks-inside-loop drag and the 10bps slippage on each fill).
for sym in observed_notional:
    expected_amt = pool_pre * weights[sym]
    err_pct = abs(observed_notional[sym] - expected_amt) / expected_amt * 100
    check(f"R4 {sym}: observed ${observed_notional[sym]:,.0f} vs expected ${expected_amt:,.0f}  "
          f"({err_pct:.2f}% diff)",
          err_pct < 2.0,
          f"too far from pool × weight; observed_notional may not respect inverse-vol")


# ---------------------------------------------------------------------------
# R1: vol uses prices STRICTLY BEFORE the entry-fill date
# ---------------------------------------------------------------------------
print("\n=== R1. Vol estimator excludes the fill day itself ===")

# Strategy: pick the symbol with the highest sigma. Now build a synthetic
# series where adding the fill day's close would WILDLY change the sigma.
# If the engine included that day, position size would be very different.
#
# We can't easily inject into the live engine here, so we just assert the
# code path uses `date < first_day` from the source:
import inspect
import backtest_engine as be
src = inspect.getsource(be.run_backtest)
check("source guard: vol-estimation slice uses `d < date` (no same-day prices)",
      "for d in pm if d < date" in src,
      "engine may have changed; verify lookahead safety manually")
check("source guard: vol uses `vol_window + 1` closes (one extra for log returns)",
      "vol_window + 1" in src)


# ---------------------------------------------------------------------------
# R7: Insufficient history → equal-weight fallback
# ---------------------------------------------------------------------------
print("\n=== R7. Insufficient-history symbols fall back to equal-weight slot ===")

# ARM IPO'd 2023-09-15. With a backtest START on 2023-09-15 itself and
# vol_window=20, ARM has 0 closes before start → no vol → falls back.
SHORT = ["ARM", "AAPL", "MSFT"]  # 2 with full history, 1 brand new
short_strat = {
    "name": "Fallback",
    "universe": {"type": "symbols", "symbols": SHORT},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 3},
    "rebalancing": {"frequency": "none", "rules": {}},
    "sizing": {"type": "risk_parity", "max_positions": 3,
                "initial_allocation": 300_000, "vol_window_days": 20},
    "backtest": {"start": "2023-09-15", "end": "2023-09-25",
                 "entry_price": "next_close", "slippage_bps": 10},
}
sr = run_backtest(short_strat)
buys_sr = [t for t in sr["trades"] if t["action"] == "BUY"]
arm_buys = [t for t in buys_sr if t["symbol"] == "ARM"]
if arm_buys:
    # equal-weight slot = initial_cash / max_positions = 100_000
    arm_amt = float(arm_buys[0]["shares"]) * float(arm_buys[0]["price"])
    check(f"ARM (no vol history) sized at equal-weight slot ≈ $100,000 "
          f"(observed ${arm_amt:,.0f})",
          abs(arm_amt - 100_000) / 100_000 < 0.05,
          f"got {arm_amt}, expected fallback ~100k")
else:
    check("ARM should still get an entry even without vol history "
          "(price-based fallback kicks in)",
          False, "no ARM buy at all")


# ---------------------------------------------------------------------------
# R8: vol_window_days configurable
# ---------------------------------------------------------------------------
print("\n=== R8. vol_window_days changes the sigma estimate ===")

# Two configs same universe, different windows. Sigma differs → weights
# differ → position sizes differ.
r_w20 = run_backtest(strat(max_pos=15, window_days=20))
r_w60 = run_backtest(strat(max_pos=15, window_days=60))

# Compare share counts for AAPL on the first day — should differ
def first_buy(r, sym):
    for t in r["trades"]:
        if t["action"] == "BUY" and t["symbol"] == sym:
            return t
    return None

a20 = first_buy(r_w20, "AAPL")
a60 = first_buy(r_w60, "AAPL")
if a20 and a60:
    check(f"AAPL shares differ between window=20 ({a20['shares']:.2f}) and "
          f"window=60 ({a60['shares']:.2f})",
          a20["shares"] != a60["shares"],
          "windows produced identical sigmas — vol_window_days may be ignored")
else:
    check("both runs produced AAPL BUY", False, "missing BUY")


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
