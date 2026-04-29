"""ComputeContext — the per-(symbol, date) input bundle passed to feature.compute().

The daily update job loads a symbol's raw history once, then iterates trading
days. For each day it builds a ComputeContext that exposes lazily-computed
TTM aggregates and point-in-time slices. Every registered feature reads from
this context so the slice work happens once per (symbol, date), not once per
feature.

The semantics here MUST match scripts/features.py exactly — that's what the
parity test verifies.
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property


# Column indices inside the income tuple loaded as
# (date, revenue, net_income, ebitda, eps_diluted, shares_diluted,
#  gross_profit, operating_income)
I_DATE, I_REV, I_NI, I_EBITDA, I_EPS_D, I_SHARES = 0, 1, 2, 3, 4, 5
I_GROSS_PROFIT, I_OP_INCOME = 6, 7
# Balance: (date, total_equity, net_debt, total_debt)
B_EQUITY, B_NET_DEBT, B_TOTAL_DEBT = 1, 2, 3
# Cashflow: (date, free_cash_flow, dividends_paid)
C_FCF, C_DIV = 1, 2


def _ttm(quarters: list[tuple], col_idx: int) -> float | None:
    """Sum the last 4 quarter values at col_idx. None if <4 quarters or any NULL."""
    if len(quarters) < 4:
        return None
    total = 0.0
    for q in quarters[-4:]:
        v = q[col_idx]
        if v is None:
            return None
        total += v
    return total


def _yoy_pct(latest: tuple, prior: tuple | None, col_idx: int) -> float | None:
    if prior is None:
        return None
    curr = latest[col_idx]
    prev = prior[col_idx]
    if curr is None or prev is None or prev == 0:
        return None
    return (curr - prev) / abs(prev) * 100.0


def _as_of(rows: list[tuple], target_date: str) -> tuple | None:
    if not rows:
        return None
    dates = [r[0] for r in rows]
    idx = bisect_right(dates, target_date) - 1
    return rows[idx] if idx >= 0 else None


def _as_of_slice(rows: list[tuple], target_date: str) -> list[tuple]:
    if not rows:
        return []
    dates = [r[0] for r in rows]
    idx = bisect_right(dates, target_date)
    return rows[:idx]


@dataclass
class ComputeContext:
    """Inputs available to every feature compute() for one (symbol, date)."""
    symbol: str
    date: str
    close: float
    income_slice: list[tuple]      # ascending, all rows with date <= self.date
    balance_asof: tuple | None     # last balance row with date <= self.date
    cashflow_slice: list[tuple]    # ascending, all rows with date <= self.date
    earnings_dates: list[str] = field(default_factory=list)   # ascending, all symbol earnings (past + scheduled)
    grades_slice: list[tuple] = field(default_factory=list)   # ascending (date, action), all rows with date <= self.date

    @cached_property
    def latest_q(self) -> tuple | None:
        return self.income_slice[-1] if self.income_slice else None

    @cached_property
    def prior_year_q(self) -> tuple | None:
        """Income row 4 quarters before the latest, or None."""
        if not self.income_slice:
            return None
        latest_idx = len(self.income_slice) - 1
        prior_idx = latest_idx - 4
        return self.income_slice[prior_idx] if prior_idx >= 0 else None

    @cached_property
    def shares(self) -> float | None:
        if not self.latest_q:
            return None
        s = self.latest_q[I_SHARES]
        return s if s and s > 0 else None

    @cached_property
    def market_cap(self) -> float | None:
        return self.close * self.shares if self.shares else None

    @cached_property
    def ttm_revenue(self) -> float | None:
        return _ttm(self.income_slice, I_REV)

    @cached_property
    def ttm_net_income(self) -> float | None:
        return _ttm(self.income_slice, I_NI)

    @cached_property
    def ttm_ebitda(self) -> float | None:
        return _ttm(self.income_slice, I_EBITDA)

    @cached_property
    def total_equity(self) -> float | None:
        return self.balance_asof[B_EQUITY] if self.balance_asof else None

    @cached_property
    def net_debt(self) -> float | None:
        return self.balance_asof[B_NET_DEBT] if self.balance_asof else None

    @cached_property
    def enterprise_value(self) -> float | None:
        if self.market_cap is None or self.net_debt is None:
            return None
        return self.market_cap + self.net_debt

    @cached_property
    def ttm_fcf(self) -> float | None:
        return _ttm(self.cashflow_slice, C_FCF) if self.cashflow_slice else None

    @cached_property
    def ttm_dividends(self) -> float | None:
        return _ttm(self.cashflow_slice, C_DIV) if self.cashflow_slice else None

    @cached_property
    def ttm_gross_profit(self) -> float | None:
        return _ttm(self.income_slice, I_GROSS_PROFIT)

    @cached_property
    def ttm_op_income(self) -> float | None:
        return _ttm(self.income_slice, I_OP_INCOME)

    @cached_property
    def total_debt(self) -> float | None:
        return self.balance_asof[B_TOTAL_DEBT] if self.balance_asof else None

    # ---- Prior-quarter primitives for YoY-accel features --------------------
    @cached_property
    def prior_q(self) -> tuple | None:
        """The quarter immediately before the latest, or None."""
        return self.income_slice[-2] if len(self.income_slice) >= 2 else None

    @cached_property
    def prior_q_year_ago(self) -> tuple | None:
        """Same fiscal quarter one year before the prior quarter (5 back from latest)."""
        return self.income_slice[-6] if len(self.income_slice) >= 6 else None

    # ---- Earnings-calendar primitives ---------------------------------------
    @cached_property
    def next_earnings_date(self) -> str | None:
        """Earliest scheduled earnings date strictly greater than self.date."""
        idx = bisect_right(self.earnings_dates, self.date)
        return self.earnings_dates[idx] if idx < len(self.earnings_dates) else None

    @cached_property
    def last_earnings_date(self) -> str | None:
        """Latest earnings date <= self.date."""
        idx = bisect_right(self.earnings_dates, self.date) - 1
        return self.earnings_dates[idx] if idx >= 0 else None

    @cached_property
    def days_to_next_earnings(self) -> int | None:
        """Calendar days from self.date to next earnings. None if no future earnings."""
        if self.next_earnings_date is None:
            return None
        d0 = datetime.strptime(self.date, "%Y-%m-%d")
        d1 = datetime.strptime(self.next_earnings_date, "%Y-%m-%d")
        return (d1 - d0).days

    @cached_property
    def days_since_last_earnings(self) -> int | None:
        """Calendar days from last earnings to self.date. None if no past earnings."""
        if self.last_earnings_date is None:
            return None
        d0 = datetime.strptime(self.last_earnings_date, "%Y-%m-%d")
        d1 = datetime.strptime(self.date, "%Y-%m-%d")
        return (d1 - d0).days

    # ---- Analyst-grade window helpers --------------------------------------
    def grades_in_window(self, days: int) -> list[tuple]:
        """Grade rows with date in [self.date - days + 1, self.date]."""
        from datetime import timedelta
        cutoff = (datetime.strptime(self.date, "%Y-%m-%d") - timedelta(days=days - 1)).strftime("%Y-%m-%d")
        # grades_slice is ascending; bisect for the window start.
        dates = [g[0] for g in self.grades_slice]
        from bisect import bisect_left
        lo = bisect_left(dates, cutoff)
        return self.grades_slice[lo:]


def build_context(
    symbol: str,
    date: str,
    close: float,
    income: list[tuple],
    balance: list[tuple],
    cashflow: list[tuple],
    earnings_dates: list[str] | None = None,
    grades: list[tuple] | None = None,
) -> ComputeContext | None:
    """Slice raw symbol bundles to the as-of view for `date` and wrap them.

    Returns None when the row should be skipped entirely:
      - no income rows as-of this date (no fundamentals yet), OR
      - the latest as-of income row has no usable shares_diluted.
    Both are conditions under which every materialized feature would be None,
    so we skip the row to match scripts/features.py:compute_features_for_day.

    earnings_dates and grades are optional — features that don't need them
    work whether or not they're provided. The daily update job loads them
    once per symbol and threads them through.
    """
    income_slice = _as_of_slice(income, date)
    if not income_slice:
        return None
    shares = income_slice[-1][I_SHARES]
    if not shares or shares <= 0:
        return None
    return ComputeContext(
        symbol=symbol,
        date=date,
        close=close,
        income_slice=income_slice,
        balance_asof=_as_of(balance, date),
        cashflow_slice=_as_of_slice(cashflow, date),
        earnings_dates=list(earnings_dates) if earnings_dates else [],
        grades_slice=_as_of_slice(grades, date) if grades else [],
    )
