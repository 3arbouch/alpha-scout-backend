"""Momentum return factors. On-the-fly: cheap streaming pass over closes.

ret_n at date T = (close_T / close_{T-N}) - 1, in percent. N is in trading
days (21 ≈ 1 month, 63 ≈ 3 months, 126 ≈ 6 months, 252 ≈ 12 months).

ret_12_1m is the textbook momentum factor — 12-month return excluding the
most recent month, designed to skip the well-documented short-term reversal
effect.
"""
from __future__ import annotations

from ..registry import register_feature


def _simple_return_series(prices: list[tuple[str, float]], lookback: int) -> dict[str, float]:
    """{date: pct_return_over_lookback_trading_days}. Skip rows with insufficient history."""
    out: dict[str, float] = {}
    for i in range(lookback, len(prices)):
        prev = prices[i - lookback][1]
        cur = prices[i][1]
        if prev and prev > 0:
            out[prices[i][0]] = (cur / prev - 1) * 100.0
    return out


def _ret_1m_series(symbol, prices):  return _simple_return_series(prices, 21)
def _ret_3m_series(symbol, prices):  return _simple_return_series(prices, 63)
def _ret_6m_series(symbol, prices):  return _simple_return_series(prices, 126)
def _ret_12m_series(symbol, prices): return _simple_return_series(prices, 252)


def _ret_12_1m_series(symbol, prices: list[tuple[str, float]]) -> dict[str, float]:
    """12-month return excluding the last month: close[T-21] / close[T-252] - 1."""
    out: dict[str, float] = {}
    for i in range(252, len(prices)):
        c_skip = prices[i - 21][1]
        c_lookback = prices[i - 252][1]
        if c_lookback and c_lookback > 0:
            out[prices[i][0]] = (c_skip / c_lookback - 1) * 100.0
    return out


for name, fn, lookback_label in [
    ("ret_1m",  _ret_1m_series,  "21 trading days"),
    ("ret_3m",  _ret_3m_series,  "63 trading days"),
    ("ret_6m",  _ret_6m_series,  "126 trading days"),
    ("ret_12m", _ret_12m_series, "252 trading days"),
]:
    register_feature(
        name=name, compute_series=fn,
        deps=("prices.close",),
        materialization="on_the_fly", category="momentum", unit="percent",
        description=f"close return over {lookback_label}, in percent.",
    )

register_feature(
    name="ret_12_1m", compute_series=_ret_12_1m_series,
    deps=("prices.close",),
    materialization="on_the_fly", category="momentum", unit="percent",
    description="12-month return excluding the most recent month "
                "(close[T-21d] / close[T-252d] - 1) × 100. Textbook momentum factor.",
)
