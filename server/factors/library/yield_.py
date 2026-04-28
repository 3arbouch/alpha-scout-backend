"""Yield factors: cashflow-to-price and dividend-to-price."""
from __future__ import annotations

from ..context import ComputeContext
from ..registry import register_feature


def _fcf_yield(ctx: ComputeContext) -> float | None:
    if ctx.ttm_fcf is None or ctx.market_cap is None or ctx.market_cap <= 0:
        return None
    return ctx.ttm_fcf / ctx.market_cap * 100.0


def _div_yield(ctx: ComputeContext) -> float | None:
    if ctx.ttm_dividends is None or ctx.market_cap is None or ctx.market_cap <= 0:
        return None
    # dividends_paid is stored as a negative number in cashflow; yield reported positive.
    return abs(ctx.ttm_dividends) / ctx.market_cap * 100.0


register_feature(
    name="fcf_yield", compute=_fcf_yield,
    deps=("prices.close", "cashflow.free_cash_flow", "income.shares_diluted"),
    materialization="precomputed", category="yield", unit="percent",
    description="TTM free_cash_flow / market_cap × 100. Percent.",
)
register_feature(
    name="div_yield", compute=_div_yield,
    deps=("prices.close", "cashflow.dividends_paid", "income.shares_diluted"),
    materialization="precomputed", category="yield", unit="percent",
    description="TTM abs(dividends_paid) / market_cap × 100.",
)
