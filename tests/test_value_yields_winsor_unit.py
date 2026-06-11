#!/usr/bin/env python3
"""
Unit tests: (a) yield-form valuation factors (E/P, B/P, S/P, EBITDA/EV, S/EV)
and their key robustness property (negative/zero numerator → finite yield, not
explosion); (b) winsorized z-standardization (an outlier no longer collapses
the cross-section).

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_value_yields_winsor_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "server"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import numpy as np
from factors.context import ComputeContext
from factors.library.valuation import (
    _earnings_yield, _book_to_price, _sales_to_price, _ebitda_to_ev, _sales_to_ev,
)
from backtest_engine import _standardize_vals

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


def approx(a, b, tol=1e-6):
    return a is not None and abs(a - b) < tol


def inc(rev=0.0, ni=0.0, ebitda=0.0, shares=None):
    # (period_end, revenue, net_income, ebitda, eps_diluted, shares_diluted, gross_profit, operating_income, available_from)
    return ("d", rev, ni, ebitda, 0, shares, 0, 0, "d")


def ctx(close, shares, rev_q, ni_q, ebitda_q, equity, net_debt):
    income = [inc(rev=rev_q, ni=ni_q, ebitda=ebitda_q, shares=shares) for _ in range(4)]
    balance = ("d", equity, net_debt, 0, 0, "d")   # (pe, equity, net_debt, total_debt, total_assets, avail)
    return ComputeContext(symbol="T", date="d", close=close, income_slice=income,
                          balance_asof=balance, cashflow_slice=[])


print("=== yield-form valuation: hand-computed ===")
# close=10, shares=100 → mcap=1000; ttm: rev=100, ni=100, ebitda=200; equity=500; net_debt=200 → EV=1200
c = ctx(close=10.0, shares=100.0, rev_q=25.0, ni_q=25.0, ebitda_q=50.0, equity=500.0, net_debt=200.0)
check("earnings_yield = 10% (100/1000)", approx(_earnings_yield(c), 10.0), str(_earnings_yield(c)))
check("book_to_price = 50% (500/1000)", approx(_book_to_price(c), 50.0), str(_book_to_price(c)))
check("sales_to_price = 10% (100/1000)", approx(_sales_to_price(c), 10.0), str(_sales_to_price(c)))
check("ebitda_to_ev = 16.667% (200/1200)", approx(_ebitda_to_ev(c), 200/1200*100), str(_ebitda_to_ev(c)))
check("sales_to_ev = 8.333% (100/1200)", approx(_sales_to_ev(c), 100/1200*100), str(_sales_to_ev(c)))

print("\n=== robustness: numerator ≤ 0 → finite yield, NOT explosion (the whole point) ===")
cneg = ctx(close=10.0, shares=100.0, rev_q=25.0, ni_q=-25.0, ebitda_q=-10.0, equity=-200.0, net_debt=200.0)
check("negative earnings → -10% (not None/explosion)", approx(_earnings_yield(cneg), -10.0), str(_earnings_yield(cneg)))
check("negative book → -20% (not None)", approx(_book_to_price(cneg), -20.0), str(_book_to_price(cneg)))
czero = ctx(close=10.0, shares=100.0, rev_q=25.0, ni_q=0.0, ebitda_q=50.0, equity=500.0, net_debt=200.0)
check("zero earnings → 0.0 (benign)", approx(_earnings_yield(czero), 0.0), str(_earnings_yield(czero)))
# EV ≤ 0 (huge net cash) → None for EV-based yields
cevneg = ctx(close=10.0, shares=100.0, rev_q=25.0, ni_q=25.0, ebitda_q=50.0, equity=500.0, net_debt=-2000.0)
check("EV ≤ 0 → ebitda_to_ev None", _ebitda_to_ev(cevneg) is None, str(_ebitda_to_ev(cevneg)))

print("\n=== winsorized z-standardization: an outlier no longer collapses the cross-section ===")
clean = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], dtype=float)
withbad = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1e15], dtype=float)
z_clean = _standardize_vals(clean, "z")
z_bad = _standardize_vals(withbad, "z")
# clean data: winsorize must NOT distort it (band covers all) → equals plain z-score
plain = (clean - clean.mean()) / clean.std(ddof=0)
check("clean data unchanged by winsorize", np.allclose(z_clean, plain, atol=1e-9),
      f"max diff {np.max(np.abs(z_clean-plain)):.2e}")
# with outlier: the 10 normal names retain real spread (NOT all collapsed to ~0)
spread_normals = float(np.std(z_bad[:10]))
check("normals keep spread despite 1e15 outlier", spread_normals > 0.5, f"std={spread_normals:.3f}")
# and every output is finite + bounded (outlier clipped, not ±1e15-driven)
check("all outputs finite & bounded (|z|<6)", np.all(np.isfinite(z_bad)) and np.max(np.abs(z_bad)) < 6.0,
      f"max|z|={np.max(np.abs(z_bad)):.2f}")
# sanity: WITHOUT winsorization a 1e15 outlier would crush normals to ~0 — confirm we beat that
naive_mu, naive_sd = withbad.mean(), withbad.std(ddof=0)
naive_normals_spread = float(np.std((withbad[:10] - naive_mu) / naive_sd))
check("winsorized spread >> naive spread", spread_normals > 100 * naive_normals_spread,
      f"winsor={spread_normals:.3f} naive={naive_normals_spread:.2e}")

print("\n=== rank path still robust + untouched (deployed strategies use rank) ===")
r_bad = _standardize_vals(withbad, "rank")
check("rank handles outlier (finite, bounded)", np.all(np.isfinite(r_bad)) and np.max(np.abs(r_bad)) < 3.0,
      f"max={np.max(np.abs(r_bad)):.2f}")

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
