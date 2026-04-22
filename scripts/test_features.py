"""
Unit tests for features.py — TTM point-in-time helpers and feature derivation.

Run: python3 -m scripts.test_features
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import features as F  # noqa: E402


def _row(date, *vals):
    """Build a tuple (date, *vals) — mirrors the shape features.py returns from _load_symbol_bundles."""
    return (date, *vals)


def test_compute_ttm_sums_last_four():
    # income-shape rows: (date, revenue, net_income, ebitda, eps_diluted, shares_diluted)
    rows = [
        _row("2024-03-31", 100, 10, 15, 0.5, 1000),
        _row("2024-06-30", 110, 12, 17, 0.6, 1000),
        _row("2024-09-30", 120, 13, 18, 0.65, 1000),
        _row("2024-12-31", 130, 15, 20, 0.75, 1000),
    ]
    assert F.compute_ttm(rows, F.I_REV) == 460
    assert F.compute_ttm(rows, F.I_NI) == 50


def test_compute_ttm_takes_last_four_only_when_more():
    rows = [
        _row("2023-03-31", 80, 7, 12, 0.4, 1000),    # should be excluded
        _row("2024-03-31", 100, 10, 15, 0.5, 1000),
        _row("2024-06-30", 110, 12, 17, 0.6, 1000),
        _row("2024-09-30", 120, 13, 18, 0.65, 1000),
        _row("2024-12-31", 130, 15, 20, 0.75, 1000),
    ]
    # Sums the last 4 entries regardless of overall length
    assert F.compute_ttm(rows, F.I_REV) == 460


def test_compute_ttm_none_if_fewer_than_four():
    rows = [
        _row("2024-06-30", 110, 12, 17, 0.6, 1000),
        _row("2024-09-30", 120, 13, 18, 0.65, 1000),
        _row("2024-12-31", 130, 15, 20, 0.75, 1000),
    ]
    assert F.compute_ttm(rows, F.I_REV) is None


def test_compute_ttm_none_on_null_in_window():
    rows = [
        _row("2024-03-31", 100, 10, 15, 0.5, 1000),
        _row("2024-06-30", 110, None, 17, 0.6, 1000),   # NULL net_income
        _row("2024-09-30", 120, 13, 18, 0.65, 1000),
        _row("2024-12-31", 130, 15, 20, 0.75, 1000),
    ]
    # Revenue TTM still computes (no null in revenue)
    assert F.compute_ttm(rows, F.I_REV) == 460
    # Net income TTM bails because one of the last 4 is None
    assert F.compute_ttm(rows, F.I_NI) is None


def test_yoy_pct():
    curr = _row("2024-12-31", 130, 15, 20, 0.75, 1000)
    prior = _row("2023-12-31", 100, 10, 15, 0.5, 1000)
    assert abs(F.yoy_pct(curr, prior, F.I_REV) - 30.0) < 1e-9
    assert abs(F.yoy_pct(curr, prior, F.I_EPS_D) - 50.0) < 1e-9


def test_yoy_pct_none_when_prior_missing():
    curr = _row("2024-12-31", 130, 15, 20, 0.75, 1000)
    assert F.yoy_pct(curr, None, F.I_REV) is None


def test_yoy_pct_zero_denominator_returns_none():
    curr = _row("2024-12-31", 130, 15, 20, 0.75, 1000)
    prior = _row("2023-12-31", 0, 10, 15, 0.5, 1000)   # revenue == 0
    assert F.yoy_pct(curr, prior, F.I_REV) is None


def test_yoy_pct_handles_negative_prior():
    # Turnaround case: prior quarter net income was negative. YoY uses abs(prior).
    curr = _row("2024-12-31", 130, 10, 20, 0.75, 1000)
    prior = _row("2023-12-31", 100, -5, 15, 0.5, 1000)
    # (10 - (-5)) / |-5| * 100 = 300%
    assert abs(F.yoy_pct(curr, prior, F.I_NI) - 300.0) < 1e-9


def test_as_of_picks_latest_on_or_before():
    rows = [
        _row("2024-03-31", 1),
        _row("2024-06-30", 2),
        _row("2024-09-30", 3),
    ]
    assert F._as_of(rows, "2024-05-15")[1] == 1    # between Q1 and Q2, picks Q1
    assert F._as_of(rows, "2024-06-30")[1] == 2    # exact date picks that row
    assert F._as_of(rows, "2024-12-01")[1] == 3    # after last, picks last
    assert F._as_of(rows, "2023-01-01") is None    # before all returns None
    assert F._as_of([], "2024-01-01") is None


def test_as_of_slice_returns_prefix():
    rows = [
        _row("2024-03-31", 1),
        _row("2024-06-30", 2),
        _row("2024-09-30", 3),
    ]
    assert len(F._as_of_slice(rows, "2024-06-30")) == 2
    assert len(F._as_of_slice(rows, "2024-06-29")) == 1
    assert F._as_of_slice(rows, "2023-01-01") == []


def test_same_quarter_prior_year_index_math():
    # Five quarters in ascending order; latest is at index 4; prior year is at index 0
    rows = [_row(f"2023-{m:02d}-01", i) for i, m in enumerate([3, 6, 9, 12])]
    rows.append(_row("2024-03-01", 999))
    prior = F._same_quarter_prior_year(rows, len(rows) - 1)
    assert prior is not None
    assert prior[0] == "2023-03-01"


def test_same_quarter_prior_year_none_when_insufficient():
    rows = [_row("2024-03-01", 100), _row("2024-06-01", 110), _row("2024-09-01", 120)]
    assert F._same_quarter_prior_year(rows, 2) is None   # only 3 quarters, can't reach prior year


def test_compute_features_for_day_integration():
    """End-to-end: build the 9 features for a synthetic symbol."""
    # 5 quarters so TTM + YoY both work
    income = [
        _row("2023-03-31", 100, 10, 15, 0.5, 1000),
        _row("2023-06-30", 105, 11, 16, 0.55, 1000),
        _row("2023-09-30", 110, 12, 17, 0.60, 1000),
        _row("2023-12-31", 120, 13, 18, 0.65, 1000),
        _row("2024-03-31", 130, 15, 20, 0.75, 1000),    # TTM window ends here
    ]
    balance = [
        _row("2023-12-31", 500, 200),   # total_equity, net_debt
        _row("2024-03-31", 520, 190),
    ]
    cashflow = [
        _row("2023-03-31", 8, -2),
        _row("2023-06-30", 9, -2),
        _row("2023-09-30", 10, -2),
        _row("2023-12-31", 11, -2),
        _row("2024-03-31", 12, -3),
    ]

    feats = F.compute_features_for_day("2024-04-15", close=50.0,
                                        income=income, balance=balance, cashflow=cashflow)
    assert feats is not None

    # market_cap = 50 * 1000 = 50_000
    # TTM (latest 4 quarters): rev = 105+110+120+130 = 465; ni = 51; ebitda = 71; fcf = 42
    # pe = 50_000 / 51
    assert abs(feats["pe"] - 50_000 / 51) < 1e-6
    assert abs(feats["ps"] - 50_000 / 465) < 1e-6
    # p_b uses latest-as-of balance: 2024-03-31 total_equity = 520
    assert abs(feats["p_b"] - 50_000 / 520) < 1e-6
    # ev = mcap + net_debt(190) = 50190
    assert abs(feats["ev_ebitda"] - 50_190 / 71) < 1e-6
    assert abs(feats["ev_sales"] - 50_190 / 465) < 1e-6
    # fcf_yield = 42 / 50000 * 100 = 0.084
    assert abs(feats["fcf_yield"] - 0.084) < 1e-9
    # div_yield: dividends_paid TTM = -2 + -2 + -2 + -3 = -9, abs = 9 → 9/50000*100 = 0.018
    assert abs(feats["div_yield"] - 0.018) < 1e-9
    # yoy: latest Q (2024-03-31) vs 4 back (2023-03-31):
    #   eps_yoy = (0.75 - 0.5)/0.5 * 100 = 50
    #   rev_yoy = (130 - 100)/100 * 100 = 30
    assert abs(feats["eps_yoy"] - 50.0) < 1e-6
    assert abs(feats["rev_yoy"] - 30.0) < 1e-6


def test_compute_features_for_day_returns_none_when_no_fundamentals_as_of():
    """Before any filing, features are undefined."""
    income = [_row("2024-03-31", 100, 10, 15, 0.5, 1000)]
    feats = F.compute_features_for_day("2022-01-01", close=50.0,
                                        income=income, balance=[], cashflow=[])
    assert feats is None


def test_compute_features_negative_earnings_yields_null_pe():
    """If TTM net_income is negative or zero, PE is None (cheap names with losses shouldn't rank)."""
    income = [
        _row("2023-03-31", 100, -5, 5, -0.1, 1000),
        _row("2023-06-30", 105, -5, 6, -0.1, 1000),
        _row("2023-09-30", 110, -5, 6, -0.1, 1000),
        _row("2023-12-31", 120, -5, 7, -0.1, 1000),
    ]
    feats = F.compute_features_for_day("2024-04-15", close=50.0,
                                        income=income, balance=[], cashflow=[])
    assert feats is not None
    assert feats["pe"] is None
    # PS still computed (revenue > 0)
    assert feats["ps"] is not None


def run():
    tests = [v for k, v in globals().items() if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS {t.__name__}")
        except Exception as e:
            failed += 1
            print(f"  FAIL {t.__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(run())
