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


# ---------------------------------------------------------------------------
# Yield-form valuation (fundamental / price). The robust direction: the
# numerator (earnings, book, sales, ebitda) may be zero or negative — that is
# benign here (the yield just goes to/through zero) — while the denominator
# (market_cap / enterprise_value) cannot vanish for a real large-cap. So these
# do NOT explode the way the price/fundamental multiples above do. Higher =
# cheaper. Reported in percent.
# ---------------------------------------------------------------------------
def _earnings_yield(ctx: ComputeContext) -> float | None:
    if ctx.market_cap is None or ctx.market_cap <= 0 or ctx.ttm_net_income is None:
        return None
    return ctx.ttm_net_income / ctx.market_cap * 100.0


def _book_to_price(ctx: ComputeContext) -> float | None:
    if ctx.market_cap is None or ctx.market_cap <= 0 or ctx.total_equity is None:
        return None
    return ctx.total_equity / ctx.market_cap * 100.0


def _sales_to_price(ctx: ComputeContext) -> float | None:
    if ctx.market_cap is None or ctx.market_cap <= 0 or ctx.ttm_revenue is None:
        return None
    return ctx.ttm_revenue / ctx.market_cap * 100.0


def _ebitda_to_ev(ctx: ComputeContext) -> float | None:
    ev = ctx.enterprise_value
    if ev is None or ev <= 0 or ctx.ttm_ebitda is None:
        return None
    return ctx.ttm_ebitda / ev * 100.0


def _sales_to_ev(ctx: ComputeContext) -> float | None:
    ev = ctx.enterprise_value
    if ev is None or ev <= 0 or ctx.ttm_revenue is None:
        return None
    return ctx.ttm_revenue / ev * 100.0


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
register_feature(
    name="earnings_yield", compute=_earnings_yield,
    deps=("prices.close", "income.net_income", "income.shares_diluted"),
    materialization="precomputed", category="value", unit="percent",
    description="TTM net_income / market_cap × 100 (E/P). Higher = cheaper. Negative/zero earnings handled (no explosion).",
)
register_feature(
    name="book_to_price", compute=_book_to_price,
    deps=("prices.close", "balance.total_equity", "income.shares_diluted"),
    materialization="precomputed", category="value", unit="percent",
    description="total_equity / market_cap × 100 (B/P). Higher = cheaper.",
)
register_feature(
    name="sales_to_price", compute=_sales_to_price,
    deps=("prices.close", "income.revenue", "income.shares_diluted"),
    materialization="precomputed", category="value", unit="percent",
    description="TTM revenue / market_cap × 100 (S/P). Higher = cheaper.",
)
register_feature(
    name="ebitda_to_ev", compute=_ebitda_to_ev,
    deps=("prices.close", "income.shares_diluted", "balance.net_debt", "income.ebitda"),
    materialization="precomputed", category="value", unit="percent",
    description="TTM ebitda / enterprise_value × 100 (EBITDA/EV). Higher = cheaper. None if EV ≤ 0.",
)
register_feature(
    name="sales_to_ev", compute=_sales_to_ev,
    deps=("prices.close", "income.shares_diluted", "balance.net_debt", "income.revenue"),
    materialization="precomputed", category="value", unit="percent",
    description="TTM revenue / enterprise_value × 100 (S/EV). Higher = cheaper. None if EV ≤ 0.",
)
