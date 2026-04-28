"""Growth factors: YoY change vs the same fiscal quarter one year earlier."""
from __future__ import annotations

from ..context import ComputeContext, I_EPS_D, I_REV, _yoy_pct
from ..registry import register_feature


def _eps_yoy(ctx: ComputeContext) -> float | None:
    if not ctx.latest_q:
        return None
    return _yoy_pct(ctx.latest_q, ctx.prior_year_q, I_EPS_D)


def _rev_yoy(ctx: ComputeContext) -> float | None:
    if not ctx.latest_q:
        return None
    return _yoy_pct(ctx.latest_q, ctx.prior_year_q, I_REV)


register_feature(
    name="eps_yoy", compute=_eps_yoy,
    deps=("income.eps_diluted",),
    materialization="precomputed", category="growth", unit="percent",
    description="(latest-Q eps_diluted − same-Q prior year) / |prior| × 100. Percent.",
)
register_feature(
    name="rev_yoy", compute=_rev_yoy,
    deps=("income.revenue",),
    materialization="precomputed", category="growth", unit="percent",
    description="(latest-Q revenue − same-Q prior year) / prior × 100. Percent.",
)
