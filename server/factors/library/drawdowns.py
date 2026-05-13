"""Drawdown features. On-the-fly: streaming pass over closes.

drawdown at date T = (close_T - peak_window_T) / peak_window_T × 100, where
peak_window_T is the maximum close in the lookback window ending at T.
Negative values mean below the peak; -10 = down 10% from peak.

drawdown_alltime walks all history; drawdown_252d uses ~52 weeks; drawdown_60d
is a short-term local-peak measure.
"""
from __future__ import annotations

from ..registry import register_feature


def _drawdown_series_window(prices: list[tuple[str, float]], window_days: int | None) -> dict[str, float]:
    """{date: drawdown_pct_from_peak_in_window}.

    `window_days=None` means peak from the start of history (all-time).
    Streaming for all-time (running max), O(N×W) for windowed — still fast at
    typical N≈3000, W≤252.
    """
    out: dict[str, float] = {}
    if window_days is None:
        running_max = 0.0
        for d, c in prices:
            if c > running_max:
                running_max = c
            if running_max > 0:
                out[d] = (c - running_max) / running_max * 100.0
        return out
    closes = [c for _, c in prices]
    for i, (d, c) in enumerate(prices):
        start = max(0, i - window_days + 1)
        peak = max(closes[start:i + 1])
        if peak > 0:
            out[d] = (c - peak) / peak * 100.0
    return out


def _dd_60d(symbol, prices):     return _drawdown_series_window(prices, 60)
def _dd_252d(symbol, prices):    return _drawdown_series_window(prices, 252)
def _dd_alltime(symbol, prices): return _drawdown_series_window(prices, None)


register_feature(
    name="drawdown_60d", compute_series=_dd_60d,
    deps=("prices.close",),
    materialization="on_the_fly", category="momentum", unit="percent",
    description="(close − max(close, last 60 trading days)) / peak × 100. "
                "Short-term local-peak drawdown.",
)
register_feature(
    name="drawdown_252d", compute_series=_dd_252d,
    deps=("prices.close",),
    materialization="on_the_fly", category="momentum", unit="percent",
    description="(close − max(close, last 252 trading days)) / peak × 100. "
                "52-week-high drawdown.",
)
register_feature(
    name="drawdown_alltime", compute_series=_dd_alltime,
    deps=("prices.close",),
    materialization="on_the_fly", category="momentum", unit="percent",
    description="(close − all-time-high since 2015) / peak × 100. "
                "Full-history drawdown.",
)
