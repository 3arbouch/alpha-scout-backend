"""Valuation factors. All price-dependent ratios → daily resolution."""
from __future__ import annotations

from ..context import ComputeContext, I_EPS_D, I_REV
from ..registry import register_feature


def _pe(ctx: ComputeContext) -> float | None:
    if ctx.market_cap is None or not ctx.ttm_net_income or ctx.ttm_net_income <= 0:
        return None
    return ctx.market_cap / ctx.ttm_net_income


def _ps(ctx: ComputeContext) -> float | None:
    if ctx.market_cap is None or not ctx.ttm_revenue or ctx.ttm_revenue <= 0:
        return None
    return ctx.market_cap / ctx.ttm_revenue


def _p_b(ctx: ComputeContext) -> float | None:
    if ctx.market_cap is None or not ctx.total_equity or ctx.total_equity <= 0:
        return None
    return ctx.market_cap / ctx.total_equity


def _ev_ebitda(ctx: ComputeContext) -> float | None:
    if ctx.enterprise_value is None or not ctx.ttm_ebitda or ctx.ttm_ebitda <= 0:
        return None
    return ctx.enterprise_value / ctx.ttm_ebitda


def _ev_sales(ctx: ComputeContext) -> float | None:
    if ctx.enterprise_value is None or not ctx.ttm_revenue or ctx.ttm_revenue <= 0:
        return None
    return ctx.enterprise_value / ctx.ttm_revenue


register_feature(
    name="pe", compute=_pe,
    deps=("prices.close", "income.net_income", "income.shares_diluted"),
    materialization="precomputed", category="value", unit="ratio",
    description="market_cap / TTM net_income. None if TTM net_income ≤ 0.",
)
register_feature(
    name="ps", compute=_ps,
    deps=("prices.close", "income.revenue", "income.shares_diluted"),
    materialization="precomputed", category="value", unit="ratio",
    description="market_cap / TTM revenue. None if TTM revenue ≤ 0.",
)
register_feature(
    name="p_b", compute=_p_b,
    deps=("prices.close", "balance.total_equity", "income.shares_diluted"),
    materialization="precomputed", category="value", unit="ratio",
    description="market_cap / total_equity (latest balance as-of). None if equity ≤ 0.",
)
register_feature(
    name="ev_ebitda", compute=_ev_ebitda,
    deps=("prices.close", "income.shares_diluted", "balance.net_debt", "income.ebitda"),
    materialization="precomputed", category="value", unit="ratio",
    description="(market_cap + net_debt) / TTM ebitda. None if ebitda ≤ 0 or net_debt missing.",
)
register_feature(
    name="ev_sales", compute=_ev_sales,
    deps=("prices.close", "income.shares_diluted", "balance.net_debt", "income.revenue"),
    materialization="precomputed", category="value", unit="ratio",
    description="(market_cap + net_debt) / TTM revenue. None if revenue ≤ 0 or net_debt missing.",
)
