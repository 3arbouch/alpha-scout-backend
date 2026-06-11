"""Profitability / earnings-quality factors.

gross_profitability — Novy-Marx (2013): TTM gross profit scaled by total assets.
  The most robust standalone quality measure; complements value. Higher = better.

accruals — Sloan (1996), cash-flow form: (TTM net income − TTM operating cash
  flow) / total assets. High accruals signal low earnings quality and predict
  LOWER future returns, so LOWER (more negative) is better.

Both are TTM-over-assets ratios, point-in-time via the context's as-of slices.
"""
from __future__ import annotations

from ..context import ComputeContext
from ..registry import register_feature


# Economic sanity bounds: real GP/assets ~ [0, 3], accruals/assets ~ [-1, 1].
# Values far outside these come from degenerate/stub denominators (e.g. a
# total_assets data glitch) — return None so a single bad row can't blow up a
# z-standardized cross-section. Bounds are deliberately generous.
def _gross_profitability(ctx: ComputeContext) -> float | None:
    gp = ctx.ttm_gross_profit
    ta = ctx.total_assets
    if gp is None or not ta or ta <= 0:
        return None
    v = gp / ta
    return v if -5.0 < v < 10.0 else None


def _accruals(ctx: ComputeContext) -> float | None:
    ni = ctx.ttm_net_income
    ocf = ctx.ttm_operating_cf
    ta = ctx.total_assets
    if ni is None or ocf is None or not ta or ta <= 0:
        return None
    v = (ni - ocf) / ta
    return v if -5.0 < v < 5.0 else None


register_feature(
    name="gross_profitability", compute=_gross_profitability,
    deps=("income.gross_profit", "balance.total_assets"),
    materialization="precomputed", category="quality", unit="ratio",
    description="TTM gross_profit / total_assets (Novy-Marx). Higher = more gross profit per unit of assets.",
)
register_feature(
    name="accruals", compute=_accruals,
    deps=("income.net_income", "cashflow.operating_cf", "balance.total_assets"),
    materialization="precomputed", category="quality", unit="ratio",
    description="(TTM net_income − TTM operating_cf) / total_assets (Sloan). Lower = higher earnings quality.",
)
