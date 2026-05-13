"""Momentum / oscillator features.

These are on-the-fly: not stored in features_daily, computed once per symbol
per backtest from the symbol's price history. Streaming algorithms (Wilder
smoothing) keep this O(n) per symbol regardless of how many query dates are
later requested.
"""
from __future__ import annotations

from ..registry import register_feature


def _wilder_rsi_series(closes: list[tuple[str, float]], period: int) -> dict[str, float]:
    """Wilder RSI over (date, close) pairs in ascending order.

    Returns {date: rsi_value} where rsi is in [0, 100]. Empty dict if there
    isn't enough history (need period + 1 closes for the first value).

    Semantics match scripts/signals.py:compute_rsi exactly so a
    feature_threshold(feature="rsi_14") fires on the same days as the
    legacy `rsi` entry-condition type. Values rounded to 2 decimals to match.
    """
    if len(closes) < period + 1:
        return {}
    # Per-bar changes.
    changes: list[tuple[str, float]] = []
    for i in range(1, len(closes)):
        changes.append((closes[i][0], closes[i][1] - closes[i - 1][1]))

    # Seed with SMA of first `period` gains/losses.
    gains = [max(c, 0) for _, c in changes[:period]]
    losses = [max(-c, 0) for _, c in changes[:period]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    out: dict[str, float] = {}
    seed_date = changes[period - 1][0]
    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
    out[seed_date] = round(rsi, 2)

    # Wilder smoothing for the rest.
    for i in range(period, len(changes)):
        date, change = changes[i]
        gain = max(change, 0)
        loss = max(-change, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        out[date] = round(rsi, 2)

    return out


def _rsi_14_series(symbol: str, prices: list[tuple[str, float]]) -> dict[str, float]:
    return _wilder_rsi_series(prices, 14)


register_feature(
    name="rsi_14", compute_series=_rsi_14_series,
    deps=("prices.close",),
    materialization="on_the_fly", category="momentum", unit="ratio_0_100",
    description="Wilder 14-bar RSI. <30 oversold, >70 overbought.",
)
