"""Balance-sheet-derived quality ratios.

ROE, ROIC, debt-to-equity. interest_coverage is intentionally omitted —
the income table doesn't carry interest_expense in the current schema.

ROIC uses ttm_op_income / (total_equity + total_debt) as a serviceable
proxy: the textbook is NOPAT / invested_capital, but NOPAT requires a tax
rate we don't ingest cleanly. Document the proxy in the description so the
agent knows what it is.
"""
from __future__ import annotations

from ..context import ComputeContext
from ..registry import register_feature


def _roe(ctx: ComputeContext) -> float | None:
    if (ctx.ttm_net_income is None or not ctx.total_equity or ctx.total_equity <= 0):
        return None
    return ctx.ttm_net_income / ctx.total_equity * 100.0


def _roic(ctx: ComputeContext) -> float | None:
    if ctx.ttm_op_income is None:
        return None
    eq = ctx.total_equity if (ctx.total_equity and ctx.total_equity > 0) else 0
    debt = ctx.total_debt if (ctx.total_debt and ctx.total_debt > 0) else 0
    invested = eq + debt
    if invested <= 0:
        return None
    return ctx.ttm_op_income / invested * 100.0


def _debt_to_equity(ctx: ComputeContext) -> float | None:
    if (ctx.total_debt is None or not ctx.total_equity or ctx.total_equity <= 0):
        return None
    return ctx.total_debt / ctx.total_equity


register_feature(
    name="roe", compute=_roe,
    deps=("income.net_income", "balance.total_equity"),
    materialization="precomputed", category="quality", unit="percent",
    description="TTM net_income / total_equity × 100.",
)
register_feature(
    name="roic", compute=_roic,
    deps=("income.operating_income", "balance.total_equity", "balance.total_debt"),
    materialization="precomputed", category="quality", unit="percent",
    description="TTM operating_income / (total_equity + total_debt) × 100. "
                "Proxy for ROIC — true NOPAT / invested-capital requires a tax "
                "rate not ingested in the current schema.",
)
register_feature(
    name="debt_to_equity", compute=_debt_to_equity,
    deps=("balance.total_debt", "balance.total_equity"),
    materialization="precomputed", category="quality", unit="ratio",
    description="total_debt / total_equity. Higher = more leverage.",
)
