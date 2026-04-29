"""Momentum return factors. Precomputed to features_daily.

ret_n at date T = (close_T / close_{T-N_trading_days} − 1) × 100. Uses each
symbol's own price history (carried by ComputeContext.prices_history) and
trading-day lookback indices, not calendar days, to match the standard
momentum-factor convention.

ret_12_1m is the textbook momentum factor — 12-month return excluding the
most recent month, designed to skip the well-documented short-term reversal
effect. Computed as (close[T-21d] / close[T-252d] − 1) × 100.

Promoted from on_the_fly to precomputed because the cross-sectional rank
in feature_percentile over the full universe was the dominant cost in
backtests using these factors. Precomputed values let the engine read from
features_daily via a single SELECT instead of streaming-recomputing per
symbol on every backtest.
"""
from __future__ import annotations

from ..context import ComputeContext
from ..registry import register_feature


def _ret_1m(ctx: ComputeContext) -> float | None:
    return ctx.trailing_return(lookback=21)


def _ret_3m(ctx: ComputeContext) -> float | None:
    return ctx.trailing_return(lookback=63)


def _ret_6m(ctx: ComputeContext) -> float | None:
    return ctx.trailing_return(lookback=126)


def _ret_12m(ctx: ComputeContext) -> float | None:
    return ctx.trailing_return(lookback=252)


def _ret_12_1m(ctx: ComputeContext) -> float | None:
    """12-month return excluding the most recent month: close[T-21d] / close[T-252d] − 1."""
    return ctx.trailing_return(lookback=231, skip=21)


for name, fn, lookback_label in [
    ("ret_1m",  _ret_1m,  "21 trading days"),
    ("ret_3m",  _ret_3m,  "63 trading days"),
    ("ret_6m",  _ret_6m,  "126 trading days"),
    ("ret_12m", _ret_12m, "252 trading days"),
]:
    register_feature(
        name=name, compute=fn,
        deps=("prices.close",),
        materialization="precomputed", category="momentum", unit="percent",
        description=f"close return over {lookback_label}, in percent.",
    )

register_feature(
    name="ret_12_1m", compute=_ret_12_1m,
    deps=("prices.close",),
    materialization="precomputed", category="momentum", unit="percent",
    description="12-month return excluding the most recent month "
                "(close[T-21d] / close[T-252d] - 1) × 100. Textbook momentum factor.",
)
