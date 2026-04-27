"""
Volatility-adaptive stop & take-profit price computation.

Used by the backtest engine when StopLossConfig / TakeProfitConfig.type is
`atr_multiple` or `realized_vol_multiple`. Stops are computed once at entry
and frozen on the Position. See server/models/strategy.py for the configs
and scripts/backtest_engine.py for the call sites.

All functions are pure: caller supplies the OHLC tail, we return numbers.
Returning None signals "insufficient history — caller decides what to do"
(today: skip the trade).
"""
from __future__ import annotations

import math
import statistics
from typing import Callable, Iterable

# RiskMetrics-standard daily decay. Hardcoded so the config stays narrow;
# revisit if a sleeve needs a different decay.
EWMA_LAMBDA = 0.94


def compute_atr(bars: list[tuple[float, float, float]], window_days: int) -> float | None:
    """Average True Range over `window_days` bars.

    `bars` is a list of (high, low, close) tuples in ascending date order,
    ending at (and including) the bar BEFORE the entry bar — no lookahead.
    Need `window_days + 1` bars to compute `window_days` true ranges (TR
    references the prior close).

    Returns ATR in absolute price units, or None if insufficient history.
    """
    if window_days < 1 or len(bars) < window_days + 1:
        return None
    trs: list[float] = []
    for i in range(len(bars) - window_days, len(bars)):
        high, low, _ = bars[i]
        prev_close = bars[i - 1][2]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    return sum(trs) / window_days


def compute_realized_vol(closes: list[float], window_days: int, source: str) -> float | None:
    """Daily volatility (fractional, e.g. 0.018 = 1.8%/day) over `window_days`.

    `closes` is ascending close prices ending at the bar BEFORE the entry bar.
    Need `window_days + 1` closes to compute `window_days` log returns.

    `source`:
      - "historical": sample stdev (n-1) of log returns over the window.
      - "ewma":       RiskMetrics EWMA with lambda=0.94, seeded by the
                      historical variance over the same window, then rolled
                      forward across that window's returns.

    Returns daily sigma, or None if insufficient history / non-positive prices.
    """
    if window_days < 1 or len(closes) < window_days + 1:
        return None
    # Need strictly positive prices to take logs.
    tail = closes[-(window_days + 1):]
    if any(p <= 0 for p in tail):
        return None
    rets = [math.log(tail[i] / tail[i - 1]) for i in range(1, len(tail))]
    if source == "historical":
        if len(rets) < 2:
            return None
        return statistics.stdev(rets)
    if source == "ewma":
        # Seed variance with the same window's plain variance, then roll EWMA
        # across the window so the result reflects within-window weighting
        # rather than the seed alone.
        if len(rets) < 2:
            return None
        var = statistics.pvariance(rets)
        for r in rets:
            var = EWMA_LAMBDA * var + (1.0 - EWMA_LAMBDA) * (r * r)
        return math.sqrt(var)
    return None


def compute_stop_pricing(
    strategy_config: dict,
    symbol: str,
    entry_date: str,
    entry_price: float,
    ohlc_fetcher: Callable[[str, str, int], list[tuple[float, float, float]]] | None,
) -> dict:
    """Compute frozen stop_price / take_profit_price for the new modes.

    Returns a dict with keys:
      - stop_price: float | None       — frozen stop, or None if mode is legacy / unset.
      - take_profit_price: float | None
      - stop_metadata: dict | None     — mode/k/window/sigma_or_atr_at_entry, for signal_detail.
      - tp_metadata: dict | None
      - abort: bool                    — True if a new mode was requested but history is
                                         insufficient. Caller skips the entry.

    Legacy modes (drawdown_from_entry, gain_from_entry, above_peak) and unset
    configs return all-None and abort=False — the engine uses its existing
    dynamic checks.
    """
    out = {
        "stop_price": None,
        "take_profit_price": None,
        "stop_metadata": None,
        "tp_metadata": None,
        "abort": False,
    }

    sl = strategy_config.get("stop_loss") or {}
    tp = strategy_config.get("take_profit") or {}
    sl_type = sl.get("type")
    tp_type = tp.get("type")

    needs_ohlc = sl_type in ("atr_multiple", "realized_vol_multiple") or \
                 tp_type in ("atr_multiple", "realized_vol_multiple")

    if not needs_ohlc:
        return out

    # Determine the longest window we need so we fetch once.
    windows: list[int] = []
    for cfg in (sl, tp):
        if cfg.get("type") in ("atr_multiple", "realized_vol_multiple"):
            w = cfg.get("window_days")
            if isinstance(w, int) and w > 0:
                windows.append(w)
    if not windows:
        # New mode requested but no valid window — treat as abort so we don't
        # silently fall back. The Pydantic validator should normally prevent this.
        out["abort"] = True
        return out
    max_window = max(windows)

    if ohlc_fetcher is None:
        out["abort"] = True
        return out

    bars = ohlc_fetcher(symbol, entry_date, max_window + 1)
    if not bars or len(bars) < max_window + 1:
        out["abort"] = True
        return out
    closes = [c for _, _, c in bars]

    def _price_for(cfg: dict, side: str) -> tuple[float | None, dict | None]:
        """side = 'stop' (price below entry) or 'tp' (price above entry)."""
        mode = cfg.get("type")
        k = cfg.get("k")
        window = cfg.get("window_days")
        if mode == "atr_multiple":
            atr = compute_atr(bars, window)
            if atr is None or atr <= 0:
                return None, None
            offset = k * atr
            price = entry_price - offset if side == "stop" else entry_price + offset
            return price, {
                "mode": mode, "k": k, "window_days": window,
                "atr_at_entry": round(atr, 6), "frozen_price": round(price, 6),
            }
        if mode == "realized_vol_multiple":
            sigma_source = cfg.get("sigma_source", "historical")
            sigma = compute_realized_vol(closes, window, sigma_source)
            if sigma is None or sigma <= 0:
                return None, None
            move = k * sigma  # daily fractional move; flat-percent interpretation
            price = entry_price * (1 - move) if side == "stop" else entry_price * (1 + move)
            return price, {
                "mode": mode, "k": k, "window_days": window,
                "sigma_source": sigma_source, "sigma_at_entry": round(sigma, 8),
                "frozen_price": round(price, 6),
            }
        return None, None

    if sl_type in ("atr_multiple", "realized_vol_multiple"):
        price, meta = _price_for(sl, "stop")
        if price is None:
            out["abort"] = True
            return out
        out["stop_price"] = price
        out["stop_metadata"] = meta

    if tp_type in ("atr_multiple", "realized_vol_multiple"):
        price, meta = _price_for(tp, "tp")
        if price is None:
            out["abort"] = True
            return out
        out["take_profit_price"] = price
        out["tp_metadata"] = meta

    return out


def make_sqlite_ohlc_fetcher(conn) -> Callable[[str, str, int], list[tuple[float, float, float]]]:
    """Return a closure that fetches the last `n_bars` (high, low, close) tuples
    for `symbol` strictly BEFORE `entry_date` (no lookahead), in ascending order.

    The closure is safe to call repeatedly during the simulation loop.
    """
    def _fetch(symbol: str, entry_date: str, n_bars: int) -> list[tuple[float, float, float]]:
        cur = conn.cursor()
        # Strictly before entry_date — the entry bar's OHLC is "today" and would be lookahead.
        rows = cur.execute(
            "SELECT high, low, close FROM prices "
            "WHERE symbol = ? AND date < ? "
            "ORDER BY date DESC LIMIT ?",
            (symbol, entry_date, n_bars),
        ).fetchall()
        # Reverse to ascending.
        return list(reversed([(float(h), float(l), float(c)) for h, l, c in rows]))
    return _fetch
