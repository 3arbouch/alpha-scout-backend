"""Investment-style factors: corporate asset growth and share issuance.

asset_growth — YoY % change in total assets (Cooper-Gulen-Schill 2008). High
  asset growth predicts LOWER future returns, so LOWER is better.

net_issuance — YoY % change in diluted shares outstanding. Net issuers
  underperform and net repurchasers outperform, so LOWER (negative = buybacks)
  is better.

Both compare the latest as-of value to the same fiscal quarter one year earlier.
"""
from __future__ import annotations

from ..context import ComputeContext, I_SHARES, B_TOTAL_ASSETS
from ..registry import register_feature


# YoY % changes explode when the year-ago denominator is near zero (mergers,
# IPOs, stub rows). A change below -100% is impossible and a change of many
# hundreds of percent is a corporate-action artifact, not an organic signal —
# return None outside generous bounds so one such row can't dominate a
# z-standardized cross-section.
def _net_issuance(ctx: ComputeContext) -> float | None:
    if not ctx.latest_q or not ctx.prior_year_q:
        return None
    now = ctx.latest_q[I_SHARES]
    prior = ctx.prior_year_q[I_SHARES]
    if not now or not prior or prior <= 0:
        return None
    v = (now / prior - 1.0) * 100.0
    return v if -100.0 <= v <= 500.0 else None


def _asset_growth(ctx: ComputeContext) -> float | None:
    if not ctx.balance_asof or not ctx.prior_year_balance:
        return None
    now = ctx.balance_asof[B_TOTAL_ASSETS]
    prior = ctx.prior_year_balance[B_TOTAL_ASSETS]
    if now is None or not prior or prior <= 0:
        return None
    v = (now / prior - 1.0) * 100.0
    return v if -100.0 <= v <= 1000.0 else None


register_feature(
    name="net_issuance", compute=_net_issuance,
    deps=("income.shares_diluted",),
    materialization="precomputed", category="investment", unit="percent",
    description="YoY % change in diluted shares (latest Q vs same Q prior year). "
                "Positive = dilution; lower/negative (buybacks) = better.",
)
register_feature(
    name="asset_growth", compute=_asset_growth,
    deps=("balance.total_assets",),
    materialization="precomputed", category="investment", unit="percent",
    description="YoY % change in total_assets (latest balance vs ~4 quarters prior). "
                "High growth predicts lower returns; lower = better.",
)
