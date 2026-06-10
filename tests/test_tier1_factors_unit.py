#!/usr/bin/env python3
"""
Unit tests: Tier-1 factor math (gross_profitability, accruals, net_issuance,
asset_growth), hand-computed, no DB. Mirrors the realized_vol test pattern.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_tier1_factors_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "server"))

import statistics

from factors.context import ComputeContext
from factors.library.profitability import _gross_profitability, _accruals
from factors.library.investment import _net_issuance, _asset_growth
from factors.library.surprise import _sue, _earnings_surprise

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


def approx(a, b, tol=1e-9):
    return a is not None and abs(a - b) < tol


# Row builders match the exact context tuple layouts.
def inc(date, gp=None, ni=None, shares=None, avail=None):
    # (period_end, revenue, net_income, ebitda, eps_diluted, shares_diluted,
    #  gross_profit, operating_income, available_from)
    return (date, 0, ni, 0, 0, shares, gp, 0, avail or date)


def bal(date, ta=None, avail=None):
    # (period_end, total_equity, net_debt, total_debt, total_assets, available_from)
    return (date, 0, 0, 0, ta, avail or date)


def cf(date, ocf=None, avail=None):
    # (period_end, free_cash_flow, dividends_paid, operating_cf, available_from)
    return (date, 0, 0, ocf, avail or date)


def ctx(income=None, balance_slice=None, cashflow=None):
    bsl = balance_slice or []
    return ComputeContext(
        symbol="T", date="dN", close=100.0,
        income_slice=income or [], balance_asof=(bsl[-1] if bsl else None),
        cashflow_slice=cashflow or [], balance_slice=bsl,
    )


print("=== gross_profitability = TTM gross_profit / total_assets ===")
# 4 quarters gp=25 → TTM=100; total_assets=400 → 0.25
c = ctx(income=[inc(f"q{i}", gp=25.0) for i in range(4)],
        balance_slice=[bal("b", ta=400.0)])
check("0.25 (100/400)", approx(_gross_profitability(c), 0.25), str(_gross_profitability(c)))
# <4 quarters → TTM None → None
check("3 quarters → None", _gross_profitability(
    ctx(income=[inc(f"q{i}", gp=25.0) for i in range(3)], balance_slice=[bal("b", ta=400.0)])) is None)
# zero assets → None
check("total_assets=0 → None", _gross_profitability(
    ctx(income=[inc(f"q{i}", gp=25.0) for i in range(4)], balance_slice=[bal("b", ta=0.0)])) is None)

print("\n=== accruals = (TTM net_income − TTM operating_cf) / total_assets ===")
# TTM ni = 4×10 = 40; TTM ocf = 4×30 = 120; assets=400 → (40-120)/400 = -0.2
c = ctx(income=[inc(f"q{i}", ni=10.0) for i in range(4)],
        cashflow=[cf(f"q{i}", ocf=30.0) for i in range(4)],
        balance_slice=[bal("b", ta=400.0)])
check("-0.2 ((40-120)/400)", approx(_accruals(c), -0.2), str(_accruals(c)))
# high accruals (ni>ocf) → positive
c2 = ctx(income=[inc(f"q{i}", ni=50.0) for i in range(4)],
         cashflow=[cf(f"q{i}", ocf=10.0) for i in range(4)],
         balance_slice=[bal("b", ta=400.0)])
check("+0.4 ((200-40)/400)", approx(_accruals(c2), 0.4), str(_accruals(c2)))
# missing operating_cf history → None
check("no cashflow → None", _accruals(
    ctx(income=[inc(f"q{i}", ni=10.0) for i in range(4)], balance_slice=[bal("b", ta=400.0)])) is None)

print("\n=== net_issuance = YoY % change in diluted shares ===")
# 5 income rows: index0 (year-ago) shares=100, index4 (latest) shares=110 → +10%
inc5 = [inc("q0", shares=100.0), inc("q1", shares=105.0), inc("q2", shares=108.0),
        inc("q3", shares=109.0), inc("q4", shares=110.0)]
check("+10% dilution", approx(_net_issuance(ctx(income=inc5)), 10.0), str(_net_issuance(ctx(income=inc5))))
# buyback: latest 90 vs year-ago 100 → -10%
incbb = [inc("q0", shares=100.0)] + [inc(f"q{i}", shares=95.0) for i in range(1, 4)] + [inc("q4", shares=90.0)]
check("-10% buyback", approx(_net_issuance(ctx(income=incbb)), -10.0), str(_net_issuance(ctx(income=incbb))))
# <5 quarters (no prior_year_q) → None
check("4 quarters → None (no year-ago)", _net_issuance(
    ctx(income=[inc(f"q{i}", shares=100.0) for i in range(4)])) is None)

print("\n=== asset_growth = YoY % change in total_assets ===")
# 5 balance rows: index0 (year-ago) ta=400, index4 (latest) ta=440 → +10%
bal5 = [bal("b0", ta=400.0), bal("b1", ta=410.0), bal("b2", ta=420.0),
        bal("b3", ta=430.0), bal("b4", ta=440.0)]
check("+10% asset growth", approx(_asset_growth(ctx(balance_slice=bal5)), 10.0),
      str(_asset_growth(ctx(balance_slice=bal5))))
# shrinking assets: 440 → 396 = -10%
balsh = [bal("b0", ta=440.0)] + [bal(f"b{i}", ta=420.0) for i in range(1, 4)] + [bal("b4", ta=396.0)]
check("-10% asset shrink", approx(_asset_growth(ctx(balance_slice=balsh)), -10.0),
      str(_asset_growth(ctx(balance_slice=balsh))))
# <5 balance rows → None
check("4 balance rows → None", _asset_growth(ctx(balance_slice=bal5[:4])) is None)

print("\n=== sue = latest surprise / sample stdev of trailing surprises ===")
# surprises [1,2,3,4,5]; sample stdev = sqrt(2.5)=1.5811; SUE = 5/1.5811 = 3.1623
sur = [1.0, 2.0, 3.0, 4.0, 5.0]
exp_sue = sur[-1] / statistics.stdev(sur)
check(f"matches hand-computed ({exp_sue:.4f})", approx(_sue(sur), exp_sue), str(_sue(sur)))
# negative surprise (miss) → negative SUE
check("miss → negative", _sue([2.0, 1.0, 0.0, -1.0, -3.0]) < 0)
# <4 surprises → None
check("3 surprises → None", _sue([1.0, 2.0, 3.0]) is None)
# zero dispersion (identical surprises) → None (avoid div-by-zero)
check("flat surprises → None", _sue([2.0, 2.0, 2.0, 2.0, 2.0]) is None)


def ectx(history, date="d5"):
    return ComputeContext(symbol="T", date=date, close=100.0, income_slice=[],
                          balance_asof=None, cashflow_slice=[], earnings_history=history)


print("\n=== sue via context: PIT filters to announced-on-or-before date ===")
# surprises 1..5 on d1..d5; estimate fixed so surprise = actual - est.
# rows: (date, eps_actual, eps_estimated); surprise = actual-est
hist = [("d1", 11.0, 10.0), ("d2", 12.0, 10.0), ("d3", 13.0, 10.0),
        ("d4", 14.0, 10.0), ("d5", 15.0, 10.0)]   # surprises 1,2,3,4,5
got = _earnings_surprise(ectx(hist, date="d5"))
check(f"as-of d5 SUE = {exp_sue:.4f}", approx(got, exp_sue), str(got))
# as-of d4: only surprises 1,2,3,4 visible (d5 not yet announced) — no lookahead
sur4 = [1.0, 2.0, 3.0, 4.0]
check("as-of d4 excludes future d5", approx(_earnings_surprise(ectx(hist, date="d4")),
                                            sur4[-1] / statistics.stdev(sur4)), "")
# row with missing actual is skipped
hist_gap = hist[:4] + [("d5", None, 10.0)]
check("missing eps_actual skipped → uses d1..d4", approx(_earnings_surprise(ectx(hist_gap, "d5")),
                                                         sur4[-1] / statistics.stdev(sur4)), "")

print("\n=== formula recompute (independent) on arbitrary numbers ===")
gp_q, ta = [11.0, 13.0, 17.0, 19.0], 250.0   # TTM = 60
exp_gp = sum(gp_q) / ta
got_gp = _gross_profitability(ctx(income=[inc(f"q{i}", gp=g) for i, g in enumerate(gp_q)],
                                  balance_slice=[bal("b", ta=ta)]))
check(f"gross_prof matches recompute ({exp_gp:.6f})", approx(got_gp, exp_gp), str(got_gp))

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
