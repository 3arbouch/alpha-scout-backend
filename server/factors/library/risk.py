"""Risk factors — realized volatility (the low-volatility anomaly). Precomputed.

realized_vol_N at date T = the sample standard deviation of the trailing N daily
simple returns (ending at T), annualized (×√252) and expressed in percent:

    r_t           = close_t / close_{t-1} − 1
    realized_vol  = stdev_sample(r_{T-N+1..T}) × √252 × 100

Lower = calmer. Use with sign='-' in a composite: low-volatility / low-beta
stocks have historically delivered better risk-adjusted returns than high-vol
ones (the low-volatility / betting-against-beta anomaly), and the factor is
negatively correlated with momentum — a genuine diversifier for a Q+M book.

Precomputed (like ret_*), not on-the-fly: it's a cross-sectional ranking factor,
so the agent should be able to query it via the data-query skill and
analyze_factor_library needs real cross-sectional stats on it. Point-in-time —
the trailing window ends at T and uses only returns realized by T's close.
"""
from __future__ import annotations

import math
import statistics

from ..context import ComputeContext
from ..registry import register_feature

_ANNUALIZE = math.sqrt(252)


def _realized_vol(ctx: ComputeContext, window: int) -> float | None:
    rets = ctx.trailing_daily_returns(window)
    if not rets or len(rets) < 2:
        return None
    sd = statistics.stdev(rets)            # sample stdev (ddof=1)
    return sd * _ANNUALIZE * 100.0         # annualized, in percent


def _realized_vol_60(ctx: ComputeContext) -> float | None:
    return _realized_vol(ctx, 60)


def _realized_vol_252(ctx: ComputeContext) -> float | None:
    return _realized_vol(ctx, 252)


for _name, _fn, _lbl in [
    ("realized_vol_60",  _realized_vol_60,  "60 trading days"),
    ("realized_vol_252", _realized_vol_252, "252 trading days"),
]:
    register_feature(
        name=_name, compute=_fn,
        deps=("prices.close",),
        materialization="precomputed", category="risk", unit="percent",
        description=(
            f"Annualized realized volatility over {_lbl} — sample stdev of "
            "trailing daily returns × √252 × 100. Lower = calmer; use sign='-' "
            "(low-volatility anomaly; diversifies momentum)."
        ),
    )
