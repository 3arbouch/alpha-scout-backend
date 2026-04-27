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


def _summarize(role: str, type_: str, params: dict, evidence: dict) -> str:
    """BE-rendered human-readable label for a stop / take-profit record.

    The FE renders this as-is when it doesn't have a custom layout for `type_`.
    Adding a future signal type means writing a `_summarize` branch — the FE
    keeps working without changes.
    """
    label = "Stop" if role == "stop" else "TP"
    if type_ == "drawdown_from_entry":
        return f"{label} @ {params.get('value')}%"
    if type_ == "gain_from_entry":
        sign = "+" if (params.get("value") or 0) >= 0 else ""
        return f"{label} @ {sign}{params.get('value')}%"
    if type_ == "above_peak":
        return f"{label} @ +{params.get('value')}% above peak"
    if type_ == "atr_multiple":
        fp = evidence.get("frozen_price")
        fp_s = f" = ${fp:.2f}" if isinstance(fp, (int, float)) else ""
        return f"{label} @ {params.get('k')}× ATR({params.get('window_days')}d){fp_s}"
    if type_ == "realized_vol_multiple":
        fp = evidence.get("frozen_price")
        fp_s = f" = ${fp:.2f}" if isinstance(fp, (int, float)) else ""
        src = params.get("sigma_source", "historical")
        return f"{label} @ {params.get('k')}σ ({params.get('window_days')}d, {src}){fp_s}"
    return f"{label} ({type_})"


def _make_record(role: str, cfg: dict, params_keys: list[str], evidence: dict) -> dict:
    """Build the unified {type, params, evidence, summary} record for one role.

    `params_keys` is the subset of `cfg` keys that belong in `params` (the input
    config) — keeps `type` and any fields that don't make sense to echo out.
    """
    type_ = cfg.get("type")
    params = {k: cfg[k] for k in params_keys if k in cfg}
    return {
        "type": type_,
        "params": params,
        "evidence": evidence,
        "summary": _summarize(role, type_, params, evidence),
    }


# Per-type fields that go into `params`. New types extend these mappings.
_STOP_PARAMS_BY_TYPE = {
    "drawdown_from_entry": ["value", "cooldown_days"],
    "atr_multiple": ["k", "window_days", "cooldown_days"],
    "realized_vol_multiple": ["k", "window_days", "sigma_source", "cooldown_days"],
}
_TP_PARAMS_BY_TYPE = {
    "gain_from_entry": ["value"],
    "above_peak": ["value"],
    "atr_multiple": ["k", "window_days"],
    "realized_vol_multiple": ["k", "window_days", "sigma_source"],
}


def compute_stop_pricing(
    strategy_config: dict,
    symbol: str,
    entry_date: str,
    entry_price: float,
    ohlc_fetcher: Callable[[str, str, int], list[tuple[float, float, float]]] | None,
) -> dict:
    """Compute frozen exit prices and unified-shape signal_detail records.

    Returns a dict with keys:
      - stop_price: float | None        — frozen stop for vol-adaptive modes,
                                          None for legacy (engine uses dynamic check).
      - take_profit_price: float | None — same.
      - stop_record: dict | None        — {type, params, evidence, summary} for
                                          ANY configured stop mode (legacy or new).
                                          None when no stop is configured.
      - tp_record: dict | None          — same for take_profit.
      - abort: bool                     — True if a new mode was requested but history
                                          is insufficient. Caller skips the entry.
    """
    out = {
        "stop_price": None,
        "take_profit_price": None,
        "stop_record": None,
        "tp_record": None,
        "abort": False,
    }

    sl = strategy_config.get("stop_loss") or {}
    tp = strategy_config.get("take_profit") or {}
    sl_type = sl.get("type")
    tp_type = tp.get("type")

    # Legacy modes need no OHLC. Build their records up front.
    if sl_type in ("drawdown_from_entry",):
        out["stop_record"] = _make_record(
            "stop", sl, _STOP_PARAMS_BY_TYPE.get(sl_type, []), evidence={},
        )
    if tp_type in ("gain_from_entry", "above_peak"):
        out["tp_record"] = _make_record(
            "tp", tp, _TP_PARAMS_BY_TYPE.get(tp_type, []), evidence={},
        )

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

    def _price_and_evidence(cfg: dict, side: str) -> tuple[float | None, dict | None]:
        """Compute frozen exit price and the evidence dict. side = 'stop' | 'tp'."""
        type_ = cfg.get("type")
        k = cfg.get("k")
        window = cfg.get("window_days")
        if type_ == "atr_multiple":
            atr = compute_atr(bars, window)
            if atr is None or atr <= 0:
                return None, None
            offset = k * atr
            price = entry_price - offset if side == "stop" else entry_price + offset
            return price, {"atr": round(atr, 6), "frozen_price": round(price, 6)}
        if type_ == "realized_vol_multiple":
            sigma_source = cfg.get("sigma_source", "historical")
            sigma = compute_realized_vol(closes, window, sigma_source)
            if sigma is None or sigma <= 0:
                return None, None
            move = k * sigma  # daily fractional; flat-percent interpretation
            price = entry_price * (1 - move) if side == "stop" else entry_price * (1 + move)
            return price, {"sigma": round(sigma, 8), "frozen_price": round(price, 6)}
        return None, None

    if sl_type in ("atr_multiple", "realized_vol_multiple"):
        price, evidence = _price_and_evidence(sl, "stop")
        if price is None:
            out["abort"] = True
            return out
        out["stop_price"] = price
        out["stop_record"] = _make_record(
            "stop", sl, _STOP_PARAMS_BY_TYPE.get(sl_type, []), evidence,
        )

    if tp_type in ("atr_multiple", "realized_vol_multiple"):
        price, evidence = _price_and_evidence(tp, "tp")
        if price is None:
            out["abort"] = True
            return out
        out["take_profit_price"] = price
        out["tp_record"] = _make_record(
            "tp", tp, _TP_PARAMS_BY_TYPE.get(tp_type, []), evidence,
        )

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
