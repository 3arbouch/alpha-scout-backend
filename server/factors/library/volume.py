"""Volume-based features. On-the-fly.

These need volume in addition to closes. The on-the-fly compute_series
signature is (symbol, prices=[(date, close)...]) — the prices list doesn't
carry volume. As a transitional pattern, these features open their own
read-only connection to market.db and self-fetch volume per symbol.

This is fine at current scale (one extra read per symbol per backtest); when
enough features need richer per-bar inputs, we'll move to a shared
OnTheFlyContext that carries (date, ohlcv) tuples instead. Until then the
overhead is contained to this module.
"""
from __future__ import annotations

import os
import sqlite3
import statistics

from ..registry import register_feature

_DEFAULT_DB = "/home/mohamed/alpha-scout-backend/data/market.db"


def _load_volumes(symbol: str) -> dict[str, float]:
    db_path = os.environ.get("MARKET_DB_PATH", _DEFAULT_DB)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT date, volume FROM prices WHERE symbol=? "
            "AND volume IS NOT NULL ORDER BY date",
            (symbol,),
        ).fetchall()
    return {d: float(v) for d, v in rows}


def _vol_z_20_series(symbol, prices: list[tuple[str, float]]) -> dict[str, float]:
    """Today's volume z-score over the trailing 20-day mean/stdev.

    z = (vol_today − mean_20) / stdev_20. Positive = above-average volume,
    negative = below. None when the trailing window is constant (stdev=0).
    """
    vols_by_date = _load_volumes(symbol)
    if not vols_by_date:
        return {}
    dates = [d for d, _ in prices if d in vols_by_date]
    series = [vols_by_date[d] for d in dates]
    out: dict[str, float] = {}
    for i in range(20, len(series)):
        window = series[i - 20:i]
        mu = sum(window) / 20
        try:
            sd = statistics.stdev(window)
        except statistics.StatisticsError:
            continue
        if sd <= 0:
            continue
        out[dates[i]] = (series[i] - mu) / sd
    return out


def _dollar_vol_20_series(symbol, prices: list[tuple[str, float]]) -> dict[str, float]:
    """20-day mean of close × volume. Reported in raw dollars."""
    vols_by_date = _load_volumes(symbol)
    if not vols_by_date:
        return {}
    closes_by_date = {d: c for d, c in prices}
    dates = [d for d, _ in prices if d in vols_by_date]
    series = [closes_by_date[d] * vols_by_date[d] for d in dates]
    out: dict[str, float] = {}
    for i in range(20, len(series)):
        out[dates[i]] = sum(series[i - 20:i]) / 20
    return out


register_feature(
    name="vol_z_20", compute_series=_vol_z_20_series,
    deps=("prices.volume",),
    materialization="on_the_fly", category="volume", unit="ratio",
    description="(volume_today − mean_20) / stdev_20. Standardized volume "
                "anomaly. >2 ≈ unusually high day, <-2 ≈ unusually quiet.",
)
register_feature(
    name="dollar_vol_20", compute_series=_dollar_vol_20_series,
    deps=("prices.close", "prices.volume"),
    materialization="on_the_fly", category="volume", unit="absolute_dollars",
    description="Trailing 20-day mean of close × volume. Liquidity proxy in $.",
)
