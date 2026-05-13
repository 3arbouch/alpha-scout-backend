"""Margin levels and growth/margin acceleration features.

Margin levels are TTM ratios reported as percent. Growth-acceleration features
report change in percentage-points (pp) — the *delta* of YoY growth between
the latest and prior quarter, so positive values mean growth is accelerating.
"""
from __future__ import annotations

from ..context import ComputeContext, I_REV, I_NI, I_OP_INCOME, I_EPS_D, _yoy_pct
from ..registry import register_feature


# ---------------------------------------------------------------------------
# Margin levels
# ---------------------------------------------------------------------------
def _net_margin(ctx: ComputeContext) -> float | None:
    if (ctx.ttm_net_income is None or not ctx.ttm_revenue or ctx.ttm_revenue <= 0):
        return None
    return ctx.ttm_net_income / ctx.ttm_revenue * 100.0


def _op_margin(ctx: ComputeContext) -> float | None:
    if (ctx.ttm_op_income is None or not ctx.ttm_revenue or ctx.ttm_revenue <= 0):
        return None
    return ctx.ttm_op_income / ctx.ttm_revenue * 100.0


def _gross_margin(ctx: ComputeContext) -> float | None:
    if (ctx.ttm_gross_profit is None or not ctx.ttm_revenue or ctx.ttm_revenue <= 0):
        return None
    return ctx.ttm_gross_profit / ctx.ttm_revenue * 100.0


# ---------------------------------------------------------------------------
# Margin YoY deltas — change in margin level vs same quarter one year ago.
# Reported in percentage-points (pp).
# ---------------------------------------------------------------------------
def _quarter_margin(q: tuple, num_idx: int) -> float | None:
    rev = q[I_REV]
    num = q[num_idx]
    if num is None or rev is None or rev == 0:
        return None
    return num / rev * 100.0


def _net_margin_yoy_delta(ctx: ComputeContext) -> float | None:
    if not ctx.latest_q or not ctx.prior_year_q:
        return None
    a = _quarter_margin(ctx.latest_q, I_NI)
    b = _quarter_margin(ctx.prior_year_q, I_NI)
    return None if a is None or b is None else a - b


def _op_margin_yoy_delta(ctx: ComputeContext) -> float | None:
    if not ctx.latest_q or not ctx.prior_year_q:
        return None
    a = _quarter_margin(ctx.latest_q, I_OP_INCOME)
    b = _quarter_margin(ctx.prior_year_q, I_OP_INCOME)
    return None if a is None or b is None else a - b


# ---------------------------------------------------------------------------
# Growth acceleration — YoY change in YoY growth, in pp.
# ---------------------------------------------------------------------------
def _rev_yoy_accel(ctx: ComputeContext) -> float | None:
    if not (ctx.latest_q and ctx.prior_year_q and ctx.prior_q and ctx.prior_q_year_ago):
        return None
    this_yoy = _yoy_pct(ctx.latest_q, ctx.prior_year_q, I_REV)
    last_yoy = _yoy_pct(ctx.prior_q, ctx.prior_q_year_ago, I_REV)
    return None if this_yoy is None or last_yoy is None else this_yoy - last_yoy


def _eps_yoy_accel(ctx: ComputeContext) -> float | None:
    if not (ctx.latest_q and ctx.prior_year_q and ctx.prior_q and ctx.prior_q_year_ago):
        return None
    this_yoy = _yoy_pct(ctx.latest_q, ctx.prior_year_q, I_EPS_D)
    last_yoy = _yoy_pct(ctx.prior_q, ctx.prior_q_year_ago, I_EPS_D)
    return None if this_yoy is None or last_yoy is None else this_yoy - last_yoy


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_feature(
    name="net_margin", compute=_net_margin,
    deps=("income.net_income", "income.revenue"),
    materialization="precomputed", category="quality", unit="percent",
    description="TTM net_income / TTM revenue × 100.",
)
register_feature(
    name="op_margin", compute=_op_margin,
    deps=("income.operating_income", "income.revenue"),
    materialization="precomputed", category="quality", unit="percent",
    description="TTM operating_income / TTM revenue × 100.",
)
register_feature(
    name="gross_margin", compute=_gross_margin,
    deps=("income.gross_profit", "income.revenue"),
    materialization="precomputed", category="quality", unit="percent",
    description="TTM gross_profit / TTM revenue × 100.",
)
register_feature(
    name="net_margin_yoy_delta", compute=_net_margin_yoy_delta,
    deps=("income.net_income", "income.revenue"),
    materialization="precomputed", category="growth", unit="pp",
    description="net_margin (latest Q) − net_margin (same Q prior year), in percentage points.",
)
register_feature(
    name="op_margin_yoy_delta", compute=_op_margin_yoy_delta,
    deps=("income.operating_income", "income.revenue"),
    materialization="precomputed", category="growth", unit="pp",
    description="op_margin (latest Q) − op_margin (same Q prior year), in percentage points.",
)
register_feature(
    name="rev_yoy_accel", compute=_rev_yoy_accel,
    deps=("income.revenue",),
    materialization="precomputed", category="growth", unit="pp",
    description="rev_yoy (latest Q) − rev_yoy (prior Q), in pp. Positive = accelerating growth.",
)
register_feature(
    name="eps_yoy_accel", compute=_eps_yoy_accel,
    deps=("income.eps_diluted",),
    materialization="precomputed", category="growth", unit="pp",
    description="eps_yoy (latest Q) − eps_yoy (prior Q), in pp. Positive = accelerating growth.",
)
