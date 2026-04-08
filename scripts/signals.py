#!/usr/bin/env python3
"""
AlphaScout Signals Library
===========================
Event detection functions on price time series.

Functions take price data from SQLite and return timestamps where conditions are met.

Usage as library:
    from signals import get_prices, find_selloffs, find_recovery, scan_universe

Usage as CLI (for testing):
    python3 signals.py selloffs NKE --drop -20
    python3 signals.py selloffs NKE --drop -20 --peak-window 52w
    python3 signals.py recovery NKE --drop -20 --recovery "+10% from bottom"
    python3 signals.py scan --sector "Technology" --drop -20 --recovery "+10% from bottom" --start 2024-01-01 --end 2025-12-31
"""

import os
import sqlite3
import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
from db_config import MARKET_DB_PATH as DB_PATH
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))

def get_connection():
    return sqlite3.connect(str(DB_PATH))

# ---------------------------------------------------------------------------
# get_prices: pull time series from SQLite
# ---------------------------------------------------------------------------
def get_prices(symbol: str, start: str = None, end: str = None, conn=None) -> list[tuple]:
    """
    Get price series for a ticker.

    Args:
        symbol: Ticker symbol
        start: Start date (YYYY-MM-DD), default earliest available
        end: End date (YYYY-MM-DD), default latest available
        conn: SQLite connection (creates one if not provided)

    Returns:
        List of (date, close) tuples, sorted chronologically
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    cur = conn.cursor()
    query = "SELECT date, close FROM prices WHERE symbol = ?"
    params = [symbol]

    if start:
        query += " AND date >= ?"
        params.append(start)
    if end:
        query += " AND date <= ?"
        params.append(end)

    query += " ORDER BY date ASC"
    cur.execute(query, params)
    results = cur.fetchall()

    if own_conn:
        conn.close()
    return results

# ---------------------------------------------------------------------------
# running_peak: compute rolling peak and drawdown
# ---------------------------------------------------------------------------
def running_peak(prices: list[tuple], window: str = "all_time") -> list[dict]:
    """
    For each date, compute the peak within a window and the drawdown from it.

    Args:
        prices: List of (date, close) tuples, chronological
        window: Peak reference window
            - "all_time": highest close ever up to this date
            - "52w": highest close in last 252 trading days
            - "ytd": highest close since Jan 1 of current year
            - "YYYY-MM-DD": highest close since this date

    Returns:
        List of dicts: {date, close, peak, peak_date, drawdown_pct}
    """
    if not prices:
        return []

    results = []

    if window == "all_time":
        peak = 0.0
        peak_date = None
        for date, close in prices:
            if close >= peak:
                peak = close
                peak_date = date
            drawdown = ((close - peak) / peak) * 100 if peak > 0 else 0
            results.append({
                "date": date,
                "close": close,
                "peak": peak,
                "peak_date": peak_date,
                "drawdown_pct": round(drawdown, 2),
            })

    elif window == "52w":
        lookback = 252  # trading days in a year
        for i, (date, close) in enumerate(prices):
            start_idx = max(0, i - lookback)
            window_prices = prices[start_idx:i + 1]
            peak = max(window_prices, key=lambda x: x[1])
            drawdown = ((close - peak[1]) / peak[1]) * 100 if peak[1] > 0 else 0
            results.append({
                "date": date,
                "close": close,
                "peak": peak[1],
                "peak_date": peak[0],
                "drawdown_pct": round(drawdown, 2),
            })

    elif window == "ytd":
        peak = 0.0
        peak_date = None
        current_year = None
        for date, close in prices:
            year = date[:4]
            if year != current_year:
                # New year — reset peak
                current_year = year
                peak = close
                peak_date = date
            if close >= peak:
                peak = close
                peak_date = date
            drawdown = ((close - peak) / peak) * 100 if peak > 0 else 0
            results.append({
                "date": date,
                "close": close,
                "peak": peak,
                "peak_date": peak_date,
                "drawdown_pct": round(drawdown, 2),
            })

    else:
        # Custom start date — peak since that date
        peak = 0.0
        peak_date = None
        for date, close in prices:
            if date < window:
                continue
            if close >= peak:
                peak = close
                peak_date = date
            drawdown = ((close - peak) / peak) * 100 if peak > 0 else 0
            results.append({
                "date": date,
                "close": close,
                "peak": peak,
                "peak_date": peak_date,
                "drawdown_pct": round(drawdown, 2),
            })

    return results

# ---------------------------------------------------------------------------
# find_selloffs: detect selloff events
# ---------------------------------------------------------------------------
def find_selloffs(prices: list[tuple], drop_threshold: float = -20.0,
                  peak_window: str = "all_time") -> list[dict]:
    """
    Find all selloff events in a price series.

    A selloff starts when drawdown from peak crosses drop_threshold.
    It ends when price recovers back to the peak (new cycle begins).

    Args:
        prices: List of (date, close) tuples, chronological
        drop_threshold: Negative percentage (e.g., -20 for 20% drop)
        peak_window: How to define the peak ("all_time", "52w", "ytd", or date)

    Returns:
        List of selloff dicts: {
            symbol, peak_price, peak_date, trough_price, trough_date,
            drawdown_pct, trigger_date, trigger_price, current_price,
            current_date, status
        }
    """
    series = running_peak(prices, window=peak_window)
    if not series:
        return []

    selloffs = []
    in_selloff = False
    current = None

    for point in series:
        if not in_selloff:
            if point["drawdown_pct"] <= drop_threshold:
                # Selloff triggered
                in_selloff = True
                current = {
                    "peak_price": point["peak"],
                    "peak_date": point["peak_date"],
                    "trigger_date": point["date"],
                    "trigger_price": point["close"],
                    "trough_price": point["close"],
                    "trough_date": point["date"],
                }
        else:
            # Track trough
            if point["close"] < current["trough_price"]:
                current["trough_price"] = point["close"]
                current["trough_date"] = point["date"]

            # Check if recovered to peak
            if point["close"] >= current["peak_price"]:
                current["drawdown_pct"] = round(
                    ((current["trough_price"] - current["peak_price"]) / current["peak_price"]) * 100, 2)
                current["current_price"] = point["close"]
                current["current_date"] = point["date"]
                current["status"] = "recovered"
                selloffs.append(current)
                current = None
                in_selloff = False

    # Still in selloff at end of data
    if in_selloff and current:
        last = series[-1]
        current["drawdown_pct"] = round(
            ((current["trough_price"] - current["peak_price"]) / current["peak_price"]) * 100, 2)
        current["current_price"] = last["close"]
        current["current_date"] = last["date"]
        if last["close"] > current["trough_price"]:
            current["status"] = "recovering"
        else:
            current["status"] = "active"
        selloffs.append(current)

    return selloffs

# ---------------------------------------------------------------------------
# find_recovery: detect recovery from a selloff
# ---------------------------------------------------------------------------
def find_recovery(prices: list[tuple], trough_date: str, trough_price: float,
                  peak_price: float, condition: str,
                  recovery_within_days: int = None) -> dict | None:
    """
    Given a selloff trough, find when a recovery condition was met.

    Args:
        prices: List of (date, close) tuples, chronological
        trough_date: Date of the selloff trough
        trough_price: Price at the trough
        peak_price: Price at the peak before selloff
        condition: Recovery condition string:
            - "peak" → price >= peak_price
            - "+10% from bottom" → price >= trough_price * 1.10
            - "+5% from selloff" → price >= peak_price * 0.95 (only 5% below peak)
            - "+10% above peak" → price >= peak_price * 1.10
        recovery_within_days: Max calendar days from trough to recovery (None = unlimited)

    Returns:
        {recovery_date, recovery_price, days_from_trough} or None
    """
    # Parse condition
    target = _parse_recovery_target(condition, trough_price, peak_price)
    if target is None:
        return None

    trough_dt = datetime.strptime(trough_date, "%Y-%m-%d")
    deadline_dt = trough_dt + timedelta(days=recovery_within_days) if recovery_within_days else None

    # Scan from trough forward
    past_trough = False
    for date, close in prices:
        if date < trough_date:
            continue
        if date == trough_date:
            past_trough = True
            continue
        if not past_trough:
            continue

        cur_dt = datetime.strptime(date, "%Y-%m-%d")
        # Past deadline — no recovery within window
        if deadline_dt and cur_dt > deadline_dt:
            return None

        if close >= target:
            days = (cur_dt - trough_dt).days
            return {
                "recovery_date": date,
                "recovery_price": close,
                "target_price": round(target, 2),
                "days_from_trough": days,
            }
    return None


def _parse_recovery_target(condition: str, trough_price: float, peak_price: float) -> float | None:
    """Parse a recovery condition string into a target price."""
    condition = condition.strip().lower()

    if condition == "peak":
        return peak_price

    # "+X% from bottom" or "+X% from trough"
    if "from bottom" in condition or "from trough" in condition:
        pct = _extract_pct(condition)
        if pct is not None:
            return trough_price * (1 + pct / 100)

    # "+X% from selloff" or "+X% from peak" (meaning X% below peak)
    if "from selloff" in condition or "from peak" in condition:
        pct = _extract_pct(condition)
        if pct is not None:
            return peak_price * (1 + pct / 100)

    # "+X% above peak"
    if "above peak" in condition:
        pct = _extract_pct(condition)
        if pct is not None:
            return peak_price * (1 + pct / 100)

    # Just a percentage — assume from bottom
    pct = _extract_pct(condition)
    if pct is not None:
        return trough_price * (1 + pct / 100)

    return None


def _extract_pct(s: str) -> float | None:
    """Extract a percentage number from a string like '+10% from bottom'."""
    import re
    match = re.search(r'[+-]?\d+\.?\d*', s)
    if match:
        return float(match.group())
    return None


def _parse_duration_days(s: str) -> int | None:
    """Parse a duration string like '1y', '6m', '90d', '3mo' into calendar days."""
    import re
    s = s.strip().lower()
    m = re.match(r'^(\d+)\s*(y|yr|year|years|m|mo|month|months|d|day|days|w|wk|week|weeks)$', s)
    if not m:
        return None
    val = int(m.group(1))
    unit = m.group(2)
    if unit in ('y', 'yr', 'year', 'years'):
        return val * 365
    elif unit in ('m', 'mo', 'month', 'months'):
        return val * 30
    elif unit in ('w', 'wk', 'week', 'weeks'):
        return val * 7
    elif unit in ('d', 'day', 'days'):
        return val
    return None

# ---------------------------------------------------------------------------
# find_period_drops: detect drops over a rolling N-day window
# ---------------------------------------------------------------------------
def find_period_drops(prices: list[tuple], period_days: int = 5,
                      threshold: float = -10.0) -> list[dict]:
    """
    Find all dates where the intra-window peak-to-trough drawdown exceeds threshold.

    For each sliding window of `period_days` trading days, finds the worst
    peak-to-trough drawdown (peak must precede trough). Returns one record per
    window end-date where the condition is met.

    This is the RAW output — every date the trigger is active. Use
    `summarize_period_drops()` to aggregate into distinct selloff events.

    Args:
        prices: List of (date, close) tuples, chronological
        period_days: Window size in trading days
        threshold: Drop threshold as negative percentage (e.g., -20)

    Returns:
        List of dicts: {signal_date, close, peak_date, peak_price, trough_date,
                        trough_price, drawdown_pct, period_days}
        Sorted chronologically by signal_date.
    """
    if len(prices) <= period_days:
        return []

    events = []

    for i in range(period_days, len(prices) + 1):
        window = prices[i - period_days:i]
        signal_date = window[-1][0]
        signal_close = window[-1][1]

        best_drawdown = 0.0
        best_trough_price = None
        best_trough_date = None
        event_peak_price = None
        event_peak_date = None

        running_peak_price = window[0][1]
        running_peak_date = window[0][0]

        for date, close in window:
            if close >= running_peak_price:
                running_peak_price = close
                running_peak_date = date

            if running_peak_price > 0:
                dd = ((close - running_peak_price) / running_peak_price) * 100
                if dd < best_drawdown:
                    best_drawdown = dd
                    best_trough_price = close
                    best_trough_date = date
                    event_peak_price = running_peak_price
                    event_peak_date = running_peak_date

        if best_drawdown <= threshold and best_trough_date is not None:
            events.append({
                "signal_date": signal_date,
                "close": signal_close,
                "peak_date": event_peak_date,
                "peak_price": event_peak_price,
                "trough_date": best_trough_date,
                "trough_price": best_trough_price,
                "drawdown_pct": round(best_drawdown, 2),
                "period_days": period_days,
            })

    return events


# ---------------------------------------------------------------------------
# find_current_drops: detect when current price is X% below rolling window high
# ---------------------------------------------------------------------------
def find_current_drops(prices: list[tuple], period_days: int = 63,
                       threshold: float = -15.0) -> list[dict]:
    """
    Find all dates where the current close is X% or more below the highest
    close in the preceding `period_days` trading days.

    Unlike find_period_drops (which finds historical intra-window drawdowns),
    this only fires when the stock is *currently* depressed relative to its
    recent peak. The signal turns off as soon as the stock recovers.

    Args:
        prices: List of (date, close) tuples, chronological
        period_days: Lookback window in trading days
        threshold: Drop threshold as negative percentage (e.g., -15 means
                   current price must be ≥15% below the window high)

    Returns:
        List of dicts: {signal_date, close, peak_date, peak_price,
                        drawdown_pct, period_days}
        Sorted chronologically by signal_date.
    """
    if len(prices) <= period_days:
        return []

    events = []

    for i in range(period_days, len(prices)):
        window = prices[i - period_days:i]  # lookback window (excludes current day)
        current_date = prices[i][0]
        current_close = prices[i][1]

        # Find highest close in the lookback window
        peak_price = window[0][1]
        peak_date = window[0][0]
        for date, close in window:
            if close > peak_price:
                peak_price = close
                peak_date = date

        if peak_price <= 0:
            continue

        drawdown = ((current_close - peak_price) / peak_price) * 100

        if drawdown <= threshold:
            events.append({
                "signal_date": current_date,
                "close": current_close,
                "peak_date": peak_date,
                "peak_price": peak_price,
                "drawdown_pct": round(drawdown, 2),
                "period_days": period_days,
            })

    return events


def summarize_period_drops(raw_events: list[dict], prices: list[tuple],
                           period_days: int) -> list[dict]:
    """
    Aggregate raw period-drop signals into distinct selloff events.

    Clusters raw signals whose trough dates fall within `period_days` trading
    days of each other, then keeps the worst drawdown per cluster.

    Args:
        raw_events: Output from find_period_drops()
        prices: Original price series (for date-index mapping)
        period_days: Window size (used for clustering distance)

    Returns:
        List of dicts: {peak_date, peak_price, trough_date, trough_price,
                        drawdown_pct, period_days, first_signal, last_signal,
                        signal_count}
        Sorted by drawdown (worst first).
    """
    if not raw_events:
        return []

    # Build date index
    date_to_idx = {d: i for i, (d, _) in enumerate(prices)}

    # Sort by trough_date then drawdown
    sorted_events = sorted(raw_events, key=lambda x: (x["trough_date"], x["drawdown_pct"]))

    clusters = []
    current_cluster = [sorted_events[0]]

    for ev in sorted_events[1:]:
        prev_trough = current_cluster[0]["trough_date"]
        cur_trough = ev["trough_date"]

        idx_prev = date_to_idx.get(prev_trough, 0)
        idx_cur = date_to_idx.get(cur_trough, 0)

        if idx_cur - idx_prev <= period_days:
            current_cluster.append(ev)
        else:
            clusters.append(current_cluster)
            current_cluster = [ev]

    clusters.append(current_cluster)

    # For each cluster, pick worst drawdown and compute signal metadata
    events = []
    for cluster in clusters:
        best = min(cluster, key=lambda x: x["drawdown_pct"])
        signal_dates = sorted(set(e["signal_date"] for e in cluster))
        events.append({
            "peak_date": best["peak_date"],
            "peak_price": best["peak_price"],
            "trough_date": best["trough_date"],
            "trough_price": best["trough_price"],
            "drawdown_pct": best["drawdown_pct"],
            "period_days": best["period_days"],
            "first_signal": signal_dates[0],
            "last_signal": signal_dates[-1],
            "signal_count": len(signal_dates),
        })

    events.sort(key=lambda x: x["drawdown_pct"])
    return events

# ---------------------------------------------------------------------------
# find_period_drops_with_recovery: period drops + forward recovery scan
# ---------------------------------------------------------------------------
def find_period_drops_with_recovery(prices: list[tuple], period_days: int = 63,
                                     threshold: float = -15.0,
                                     recovery_condition: str = "+10% from bottom",
                                     recovery_within_days: int = None) -> list[dict]:
    """
    Find summarized period drops and check if each recovered within a time window.

    Uses summarize_period_drops() to get distinct events, then checks recovery
    for each.

    Args:
        prices: List of (date, close) tuples, chronological
        period_days: Rolling window in trading days
        threshold: Drop threshold (e.g., -15)
        recovery_condition: Recovery condition string
        recovery_within_days: Max calendar days for recovery (None = unlimited)

    Returns:
        List of dicts: summarized period drop info + recovery info
    """
    raw = find_period_drops(prices, period_days=period_days, threshold=threshold)
    drops = summarize_period_drops(raw, prices, period_days)
    results = []

    for drop in drops:
        recovery = find_recovery(
            prices,
            trough_date=drop["trough_date"],
            trough_price=drop["trough_price"],
            peak_price=drop["peak_price"],
            condition=recovery_condition,
            recovery_within_days=recovery_within_days,
        )

        result = {**drop}
        if recovery:
            result["recovery"] = recovery
            result["recovery_met"] = True
        else:
            result["recovery"] = None
            result["recovery_met"] = False

        results.append(result)

    return results


# ---------------------------------------------------------------------------
# find_daily_drops: detect single-day drops
# ---------------------------------------------------------------------------
def find_daily_drops(prices: list[tuple], threshold: float = -5.0) -> list[dict]:
    """
    Find days where the stock dropped X% in a single session.

    Args:
        prices: List of (date, close) tuples, chronological
        threshold: Drop threshold as negative percentage (e.g., -5)

    Returns:
        List of dicts: {date, close, prev_date, prev_close, change_pct}
    """
    if len(prices) < 2:
        return []

    events = []
    for i in range(1, len(prices)):
        prev_date, prev_close = prices[i - 1]
        cur_date, cur_close = prices[i]

        if prev_close <= 0:
            continue

        change_pct = ((cur_close - prev_close) / prev_close) * 100

        if change_pct <= threshold:
            events.append({
                "date": cur_date,
                "close": cur_close,
                "prev_date": prev_date,
                "prev_close": prev_close,
                "change_pct": round(change_pct, 2),
            })

    return events

# ---------------------------------------------------------------------------
# scan_universe: run selloff + recovery across multiple tickers
# ---------------------------------------------------------------------------
def scan_universe(symbols: list[str] = None, sector: str = None,
                  drop_threshold: float = -20.0, peak_window: str = "all_time",
                  recovery_condition: str = None, recovery_within_days: int = None,
                  start: str = None, end: str = None,
                  conn=None) -> list[dict]:
    """
    Scan multiple tickers for selloffs and optional recovery.

    Args:
        symbols: List of tickers (if None, uses sector filter or all)
        sector: Filter by sector (e.g., "Technology")
        drop_threshold: Selloff threshold (e.g., -20)
        peak_window: Peak reference ("all_time", "52w", "ytd", date)
        recovery_condition: Optional recovery filter (e.g., "+10% from bottom")
        start: Date range start
        end: Date range end
        conn: SQLite connection

    Returns:
        List of result dicts with selloff + recovery info
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    # Resolve symbols
    if symbols is None:
        symbols = _get_symbols(sector=sector, conn=conn)

    results = []
    for symbol in symbols:
        prices = get_prices(symbol, start=start, end=end, conn=conn)
        if len(prices) < 10:
            continue

        selloffs = find_selloffs(prices, drop_threshold=drop_threshold, peak_window=peak_window)

        for s in selloffs:
            # Filter by date range
            if start and s["trough_date"] < start:
                continue
            if end and s["peak_date"] > end:
                continue

            result = {"symbol": symbol, **s}

            # Check recovery if requested
            if recovery_condition:
                recovery = find_recovery(
                    prices, s["trough_date"], s["trough_price"],
                    s["peak_price"], recovery_condition,
                    recovery_within_days=recovery_within_days,
                )
                if recovery:
                    result["recovery"] = recovery
                else:
                    result["recovery"] = None
                    # If recovery is required, skip this result
                    if recovery_condition:
                        # Still include but mark as not recovered
                        result["recovery_met"] = False

            results.append(result)

    if own_conn:
        conn.close()

    # Sort by drawdown (worst first)
    results.sort(key=lambda x: x.get("drawdown_pct", 0))
    return results

# ---------------------------------------------------------------------------
# scan_period_drops: find period drops across multiple tickers
# ---------------------------------------------------------------------------
def scan_period_drops(symbols: list[str] = None, sector: str = None,
                      period_days: int = 5, threshold: float = -10.0,
                      start: str = None, end: str = None, conn=None) -> list[dict]:
    """
    Scan multiple tickers for drops over a rolling N-day window.

    Args:
        symbols: List of tickers (if None, uses sector filter or all)
        sector: Filter by sector
        period_days: Rolling window in trading days
        threshold: Drop threshold (e.g., -15)
        start: Date range start
        end: Date range end
        conn: SQLite connection

    Returns:
        List of dicts with symbol + period drop info, sorted by worst drop
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    if symbols is None:
        symbols = _get_symbols(sector=sector, conn=conn)

    results = []
    for symbol in symbols:
        prices = get_prices(symbol, start=start, end=end, conn=conn)
        if len(prices) <= period_days:
            continue

        raw = find_period_drops(prices, period_days=period_days, threshold=threshold)
        events = summarize_period_drops(raw, prices, period_days)
        for e in events:
            if start and e["trough_date"] < start:
                continue
            if end and e["peak_date"] > end:
                continue
            results.append({"symbol": symbol, **e})

    if own_conn:
        conn.close()

    results.sort(key=lambda x: x.get("drawdown_pct", 0))
    return results

# ---------------------------------------------------------------------------
# scan_period_drops_with_recovery: period drops + recovery across universe
# ---------------------------------------------------------------------------
def scan_period_drops_with_recovery(symbols: list[str] = None, sector: str = None,
                                     period_days: int = 63, threshold: float = -15.0,
                                     recovery_condition: str = "+10% from bottom",
                                     recovery_within_days: int = None,
                                     start: str = None, end: str = None,
                                     recovered_only: bool = False,
                                     conn=None) -> list[dict]:
    """
    Scan universe for period drops with optional recovery filtering.

    Args:
        symbols: List of tickers (if None, uses sector filter or all)
        sector: Filter by sector
        period_days: Rolling window in trading days
        threshold: Drop threshold (e.g., -15)
        recovery_condition: Recovery condition string
        recovery_within_days: Max calendar days for recovery (None = unlimited)
        start: Date range start
        end: Date range end
        recovered_only: If True, only return events where recovery was met
        conn: SQLite connection

    Returns:
        List of dicts with period drop + recovery info
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    if symbols is None:
        symbols = _get_symbols(sector=sector, conn=conn)

    results = []
    for symbol in symbols:
        prices = get_prices(symbol, start=start, end=end, conn=conn)
        if len(prices) <= period_days:
            continue

        # Fetch wider price range for recovery scanning (need data after the drop end_date)
        if end:
            extended_prices = get_prices(symbol, start=start, conn=conn)
        else:
            extended_prices = prices

        raw = find_period_drops(prices, period_days=period_days, threshold=threshold)
        drops = summarize_period_drops(raw, prices, period_days)

        for drop in drops:
            if start and drop["trough_date"] < start:
                continue
            if end and drop["peak_date"] > end:
                continue

            recovery = find_recovery(
                extended_prices,
                trough_date=drop["trough_date"],
                trough_price=drop["trough_price"],
                peak_price=drop["peak_price"],
                condition=recovery_condition,
                recovery_within_days=recovery_within_days,
            )

            result = {"symbol": symbol, **drop}
            if recovery:
                result["recovery"] = recovery
                result["recovery_met"] = True
            else:
                result["recovery"] = None
                result["recovery_met"] = False

            if recovered_only and not result["recovery_met"]:
                continue
            results.append(result)

    if own_conn:
        conn.close()

    results.sort(key=lambda x: x.get("drawdown_pct", 0))
    return results


# ---------------------------------------------------------------------------
# scan_daily_drops: find single-day drops across multiple tickers
# ---------------------------------------------------------------------------
def scan_daily_drops(symbols: list[str] = None, sector: str = None,
                     threshold: float = -5.0, start: str = None, end: str = None,
                     conn=None) -> list[dict]:
    """
    Scan multiple tickers for single-day drops.

    Args:
        symbols: List of tickers (if None, uses sector filter or all)
        sector: Filter by sector
        threshold: Drop threshold (e.g., -5)
        start: Date range start
        end: Date range end
        conn: SQLite connection

    Returns:
        List of dicts with symbol + daily drop info, sorted by worst drop
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    if symbols is None:
        symbols = _get_symbols(sector=sector, conn=conn)

    # Fetch one extra day before start so we can compute change for the first date
    fetch_start = start
    if start:
        cur = conn.cursor()
        cur.execute("SELECT MAX(date) FROM prices WHERE date < ? LIMIT 1", (start,))
        row = cur.fetchone()
        if row and row[0]:
            fetch_start = row[0]

    results = []
    for symbol in symbols:
        prices = get_prices(symbol, start=fetch_start, end=end, conn=conn)
        if len(prices) < 2:
            continue

        events = find_daily_drops(prices, threshold=threshold)
        for e in events:
            if start and e["date"] < start:
                continue
            if end and e["date"] > end:
                continue
            results.append({"symbol": symbol, **e})

    if own_conn:
        conn.close()

    results.sort(key=lambda x: x.get("change_pct", 0))
    return results


def _get_symbols(sector: str = None, conn=None) -> list[str]:
    """Get ticker list, optionally filtered by sector."""
    # Try universe profiles
    if sector:
        profile_dir = DATA_DIR / "universe" / "profiles"
        if profile_dir.exists():
            symbols = []
            for f in profile_dir.glob("*.json"):
                try:
                    content = json.loads(f.read_text())
                    data = content.get("data", [])
                    if isinstance(data, list) and data:
                        profile = data[0]
                    elif isinstance(data, dict):
                        profile = data
                    else:
                        continue
                    if profile.get("sector", "").lower() == sector.lower():
                        symbols.append(f.stem)
                except (json.JSONDecodeError, KeyError):
                    continue
            return sorted(symbols)

    # Default: all tickers in prices table
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT symbol FROM prices ORDER BY symbol")
    return [row[0] for row in cur.fetchall()]

# ---------------------------------------------------------------------------
# CLI for testing
# ---------------------------------------------------------------------------
def _print_selloffs(results, show_recovery=False):
    """Pretty print selloff results."""
    if not results:
        print("No selloffs found matching criteria.")
        return

    print(f"\n  Found {len(results)} selloff(s):\n")
    print(f"{'Symbol':<8} {'Peak':>8} {'Peak Date':<12} {'Trough':>8} {'Trough Date':<12} {'Drawdown':>9} {'Status':<12}", end="")
    if show_recovery:
        print(f" {'Recovery Date':<14} {'Rec Price':>10} {'Days':>6}", end="")
    print()
    print("-" * (80 if not show_recovery else 115))

    for r in results:
        print(f"{r['symbol']:<8} ${r['peak_price']:>7.2f} {r['peak_date']:<12} "
              f"${r['trough_price']:>7.2f} {r['trough_date']:<12} "
              f"{r['drawdown_pct']:>8.1f}% {r['status']:<12}", end="")
        if show_recovery and r.get("recovery"):
            rec = r["recovery"]
            print(f" {rec['recovery_date']:<14} ${rec['recovery_price']:>9.2f} {rec['days_from_trough']:>5}d", end="")
        elif show_recovery:
            print(f" {'—':<14} {'—':>10} {'—':>6}", end="")
        print()


# ---------------------------------------------------------------------------
# Fundamental Signal Detectors
# ---------------------------------------------------------------------------

def _load_quarterly_income(symbol: str) -> list[dict]:
    """Load quarterly income statements sorted chronologically."""
    path = DATA_DIR / "fundamentals" / "income" / f"{symbol}.json"
    if not path.exists():
        return []
    with open(path) as f:
        data = json.load(f).get("data", [])
    qtrs = [q for q in data if q.get("period", "").startswith("Q")]
    qtrs.sort(key=lambda x: x["date"])
    return qtrs


def _quarter_key(q: dict) -> str:
    """Return 'YYYY-QN' key for matching same quarter across years."""
    return f"{q['date'][:4]}-{q['period']}"


def _find_prior_year_quarter(qtrs: list[dict], target: dict) -> dict | None:
    """Find the same fiscal quarter one year prior."""
    target_period = target.get("period")
    target_year = int(target["date"][:4])
    for q in qtrs:
        if q.get("period") == target_period and int(q["date"][:4]) == target_year - 1:
            return q
    return None


def _compute_yoy_revenue_growth(qtrs: list[dict]) -> list[dict]:
    """
    For each quarter, compute YoY revenue growth %.
    Returns list of {date, filingDate, revenue, yoy_growth, net_margin, op_margin} sorted by date.
    """
    results = []
    for q in qtrs:
        rev = q.get("revenue", 0)
        if not rev or rev <= 0:
            continue
        prior = _find_prior_year_quarter(qtrs, q)
        if not prior or not prior.get("revenue") or prior["revenue"] <= 0:
            continue
        yoy = (rev / prior["revenue"] - 1) * 100
        ni = q.get("netIncome", 0) or 0
        oi = q.get("operatingIncome", 0) or 0
        prior_ni = prior.get("netIncome", 0) or 0
        prior_oi = prior.get("operatingIncome", 0) or 0
        results.append({
            "date": q["date"],  # fiscal quarter end
            "filingDate": q.get("filingDate", q["date"]),  # when market knows
            "period": q.get("period"),
            "revenue": rev,
            "prior_revenue": prior["revenue"],
            "yoy_growth": yoy,
            "net_margin": ni / rev * 100 if rev else 0,
            "prior_net_margin": prior_ni / prior["revenue"] * 100 if prior["revenue"] else 0,
            "op_margin": oi / rev * 100 if rev else 0,
            "prior_op_margin": prior_oi / prior["revenue"] * 100 if prior["revenue"] else 0,
            "net_income": ni,
            "operating_income": oi,
        })
    results.sort(key=lambda x: x["date"])
    return results


def find_revenue_breakouts(symbol: str, threshold: float = 50.0,
                           start: str = None, end: str = None) -> list[dict]:
    """
    Find quarters where YoY revenue growth >= threshold%.

    Args:
        symbol: Ticker
        threshold: Min YoY revenue growth % (default 50)
        start/end: Date range filter (on filingDate)

    Returns:
        List of {signal_date, date, yoy_growth, revenue, ...}
    """
    qtrs = _load_quarterly_income(symbol)
    growth_data = _compute_yoy_revenue_growth(qtrs)
    results = []
    for g in growth_data:
        sig_date = g["filingDate"]
        if start and sig_date < start:
            continue
        if end and sig_date > end:
            continue
        if g["yoy_growth"] >= threshold:
            results.append({"signal_date": sig_date, **g})
    return results


def find_revenue_acceleration(symbol: str, min_quarters: int = 2,
                              start: str = None, end: str = None) -> list[dict]:
    """
    Find periods where YoY revenue growth rate is INCREASING for N consecutive quarters.

    Signal fires on the filingDate of the Nth consecutive accelerating quarter.

    Returns:
        List of {signal_date, streak, yoy_growth, prior_yoy_growth, ...}
    """
    qtrs = _load_quarterly_income(symbol)
    growth_data = _compute_yoy_revenue_growth(qtrs)
    if len(growth_data) < 2:
        return []

    results = []
    streak = 0
    for i in range(1, len(growth_data)):
        if growth_data[i]["yoy_growth"] > growth_data[i - 1]["yoy_growth"]:
            streak += 1
        else:
            streak = 0

        if streak >= min_quarters:
            sig_date = growth_data[i]["filingDate"]
            if start and sig_date < start:
                continue
            if end and sig_date > end:
                continue
            results.append({
                "signal_date": sig_date,
                "streak": streak,
                **growth_data[i],
                "prior_yoy_growth": growth_data[i - 1]["yoy_growth"],
            })
    return results


def find_margin_expansion(symbol: str, metric: str = "net_margin",
                          min_quarters: int = 2,
                          start: str = None, end: str = None) -> list[dict]:
    """
    Find periods where margin is expanding YoY for N consecutive quarters
    AND margin is improving sequentially (QoQ).

    Args:
        metric: "net_margin" or "op_margin"
        min_quarters: Consecutive quarters required

    Returns:
        List of {signal_date, streak, margin, prior_year_margin, margin_expansion_bps, ...}
    """
    qtrs = _load_quarterly_income(symbol)
    growth_data = _compute_yoy_revenue_growth(qtrs)
    if len(growth_data) < 2:
        return []

    prior_key = f"prior_{metric}"
    results = []
    streak = 0
    for i in range(1, len(growth_data)):
        current_margin = growth_data[i][metric]
        prior_year_margin = growth_data[i][prior_key]
        prev_quarter_margin = growth_data[i - 1][metric]

        yoy_expanding = current_margin > prior_year_margin
        qoq_improving = current_margin > prev_quarter_margin

        if yoy_expanding and qoq_improving:
            streak += 1
        else:
            streak = 0

        if streak >= min_quarters:
            sig_date = growth_data[i]["filingDate"]
            if start and sig_date < start:
                continue
            if end and sig_date > end:
                continue
            expansion_bps = (current_margin - prior_year_margin) * 100
            results.append({
                "signal_date": sig_date,
                "streak": streak,
                "margin": current_margin,
                "prior_year_margin": prior_year_margin,
                "margin_expansion_bps": expansion_bps,
                "metric": metric,
                **{k: v for k, v in growth_data[i].items() if k not in ("net_margin", "op_margin", "prior_net_margin", "prior_op_margin")},
            })
    return results


def find_margin_turnaround(symbol: str, metric: str = "net_margin",
                           threshold_bps: float = 1000.0, min_quarters: int = 2,
                           start: str = None, end: str = None) -> list[dict]:
    """
    Find periods where margin expands >= threshold_bps YoY for N consecutive quarters.
    Catches efficiency turnarounds (e.g., META 2023).

    Returns:
        List of {signal_date, expansion_bps, streak, ...}
    """
    qtrs = _load_quarterly_income(symbol)
    growth_data = _compute_yoy_revenue_growth(qtrs)
    if not growth_data:
        return []

    prior_key = f"prior_{metric}"
    results = []
    streak = 0
    for i in range(len(growth_data)):
        current_margin = growth_data[i][metric]
        prior_year_margin = growth_data[i][prior_key]
        expansion_bps = (current_margin - prior_year_margin) * 100

        if expansion_bps >= threshold_bps:
            streak += 1
        else:
            streak = 0

        if streak >= min_quarters:
            sig_date = growth_data[i]["filingDate"]
            if start and sig_date < start:
                continue
            if end and sig_date > end:
                continue
            results.append({
                "signal_date": sig_date,
                "streak": streak,
                "margin": current_margin,
                "prior_year_margin": prior_year_margin,
                "expansion_bps": expansion_bps,
                "metric": metric,
                **{k: v for k, v in growth_data[i].items() if k not in ("net_margin", "op_margin", "prior_net_margin", "prior_op_margin")},
            })
    return results


def find_relative_outperformance(symbol: str, benchmark: str = "^GSPC",
                                  threshold: float = 20.0, window_days: int = 126,
                                  start: str = None, end: str = None,
                                  conn=None) -> list[dict]:
    """
    Find dates where stock's trailing return exceeds benchmark by >= threshold pp.

    Args:
        benchmark: Benchmark symbol (default SPX)
        threshold: Min outperformance in percentage points
        window_days: Trailing window in trading days (126 ≈ 6 months)

    Returns:
        List of {signal_date, stock_return, benchmark_return, spread}
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    stock_prices = get_prices(symbol, start=None, end=end, conn=conn)

    # Load benchmark from index files or DB
    bench_prices = []
    if benchmark.startswith("^"):
        # Load from index JSON
        idx_map = {"^GSPC": "GSPC", "^DJI": "DJI", "^IXIC": "IXIC"}
        idx_file = DATA_DIR / "prices" / "indices" / f"{idx_map.get(benchmark, benchmark)}.json"
        if idx_file.exists():
            with open(idx_file) as f:
                idx_data = json.load(f).get("data", [])
            bench_prices = [(b["date"], b["close"]) for b in idx_data]
            bench_prices.sort()
    else:
        bench_prices = get_prices(benchmark, start=None, end=end, conn=conn)

    if own_conn:
        conn.close()

    if not stock_prices or not bench_prices:
        return []

    # Build date-indexed lookups
    stock_idx = {d: c for d, c in stock_prices}
    bench_idx = {d: c for d, c in bench_prices}

    # Get common dates
    common_dates = sorted(set(stock_idx.keys()) & set(bench_idx.keys()))
    if len(common_dates) <= window_days:
        return []

    results = []
    for i in range(window_days, len(common_dates)):
        current_date = common_dates[i]
        lookback_date = common_dates[i - window_days]

        if start and current_date < start:
            continue
        if end and current_date > end:
            continue

        stock_ret = (stock_idx[current_date] / stock_idx[lookback_date] - 1) * 100
        bench_ret = (bench_idx[current_date] / bench_idx[lookback_date] - 1) * 100
        spread = stock_ret - bench_ret

        if spread >= threshold:
            results.append({
                "signal_date": current_date,
                "stock_return": round(stock_ret, 2),
                "benchmark_return": round(bench_ret, 2),
                "spread": round(spread, 2),
            })
    return results


def find_volume_conviction(symbol: str, short_window: int = 60,
                           long_window: int = 252, ratio_threshold: float = 0.8,
                           start: str = None, end: str = None,
                           conn=None) -> list[dict]:
    """
    Find dates where short-term avg volume < ratio_threshold * long-term avg volume
    while price is above its long-term average (rising price + falling volume = conviction).

    Returns:
        List of {signal_date, short_vol, long_vol, ratio, price, price_avg}
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    cur = conn.cursor()
    cur.execute(
        "SELECT date, close, volume FROM prices WHERE symbol = ? ORDER BY date ASC",
        [symbol],
    )
    rows = cur.fetchall()
    if own_conn:
        conn.close()

    if len(rows) < long_window + 1:
        return []

    results = []
    for i in range(long_window, len(rows)):
        date, close, volume = rows[i]
        if start and date < start:
            continue
        if end and date > end:
            continue

        short_vols = [r[2] for r in rows[i - short_window:i] if r[2] and r[2] > 0]
        long_vols = [r[2] for r in rows[i - long_window:i] if r[2] and r[2] > 0]

        if not short_vols or not long_vols:
            continue

        short_avg = sum(short_vols) / len(short_vols)
        long_avg = sum(long_vols) / len(long_vols)

        if long_avg == 0:
            continue

        ratio = short_avg / long_avg

        # Price must be above its long-term average (uptrend)
        long_prices = [r[1] for r in rows[i - long_window:i]]
        price_avg = sum(long_prices) / len(long_prices)

        if ratio <= ratio_threshold and close > price_avg:
            results.append({
                "signal_date": date,
                "short_vol": int(short_avg),
                "long_vol": int(long_avg),
                "ratio": round(ratio, 3),
                "price": close,
                "price_avg": round(price_avg, 2),
            })
    return results


# ---------------------------------------------------------------------------
# Fundamental signal helpers
# ---------------------------------------------------------------------------

def _load_quarterly_income(symbol: str) -> list[dict]:
    """Load quarterly income statements sorted chronologically."""
    path = DATA_DIR / "fundamentals" / "income" / f"{symbol}.json"
    if not path.exists():
        return []
    with open(path) as f:
        data = json.load(f).get("data", [])
    qtrs = [q for q in data if q.get("period", "").startswith("Q")]
    return sorted(qtrs, key=lambda x: x["date"])


def _quarter_key(q: dict) -> str:
    """Return 'YYYY-QN' key from a quarter record."""
    # Use filing date's year + period, or derive from date
    period = q.get("period", "")  # Q1, Q2, Q3, Q4
    year = q.get("calendarYear") or q["date"][:4]
    return f"{year}-{period}"


def _find_yoy_pair(qtrs: list[dict], idx: int) -> dict | None:
    """Find the same-period quarter from ~1 year prior."""
    target = qtrs[idx]
    target_period = target.get("period")
    target_year = int(target.get("calendarYear") or target["date"][:4])
    for q in qtrs:
        q_year = int(q.get("calendarYear") or q["date"][:4])
        if q.get("period") == target_period and q_year == target_year - 1:
            return q
    return None


def _compute_quarterly_yoy(qtrs: list[dict]) -> list[dict]:
    """
    For each quarter, compute YoY revenue growth and margin data.
    Returns list of dicts with: date, filingDate, revenue, revenue_yoy,
    net_margin, net_margin_yoy_bps, op_margin, op_margin_yoy_bps.
    """
    results = []
    for i, q in enumerate(qtrs):
        prior = _find_yoy_pair(qtrs, i)
        rev = q.get("revenue", 0) or 0
        ni = q.get("netIncome", 0) or 0
        oi = q.get("operatingIncome", 0) or 0

        net_margin = (ni / rev * 100) if rev else 0
        op_margin = (oi / rev * 100) if rev else 0

        row = {
            "date": q["date"],
            "filing_date": q.get("filingDate") or q.get("acceptedDate") or q["date"],
            "period": q.get("period"),
            "revenue": rev,
            "net_margin": round(net_margin, 2),
            "op_margin": round(op_margin, 2),
            "revenue_yoy": None,
            "net_margin_yoy_bps": None,
            "op_margin_yoy_bps": None,
        }

        if prior and (prior.get("revenue", 0) or 0) > 0:
            prev_rev = prior["revenue"]
            prev_ni = prior.get("netIncome", 0) or 0
            prev_oi = prior.get("operatingIncome", 0) or 0
            prev_net_margin = (prev_ni / prev_rev * 100) if prev_rev else 0
            prev_op_margin = (prev_oi / prev_rev * 100) if prev_rev else 0

            row["revenue_yoy"] = round((rev / prev_rev - 1) * 100, 2)
            row["net_margin_yoy_bps"] = round((net_margin - prev_net_margin) * 100, 0)
            row["op_margin_yoy_bps"] = round((op_margin - prev_op_margin) * 100, 0)

        results.append(row)
    return results


# ---------------------------------------------------------------------------
# Fundamental signals: revenue & margin based
# ---------------------------------------------------------------------------

def find_revenue_breakouts(symbol: str, threshold: float = 50.0,
                           start: str = None, end: str = None) -> list[dict]:
    """
    Signal A(a): Flag quarters where YoY revenue growth >= threshold%.

    Returns list of {signal_date, revenue_yoy, revenue, ...} dicts.
    signal_date = filingDate (when market learns about it).
    """
    qtrs = _load_quarterly_income(symbol)
    if not qtrs:
        return []
    computed = _compute_quarterly_yoy(qtrs)
    results = []
    for row in computed:
        if start and row["filing_date"] < start:
            continue
        if end and row["filing_date"] > end:
            continue
        if row["revenue_yoy"] is not None and row["revenue_yoy"] >= threshold:
            results.append({
                "signal_date": row["filing_date"],
                "quarter_end": row["date"],
                "period": row["period"],
                "revenue_yoy": row["revenue_yoy"],
                "revenue": row["revenue"],
                "net_margin": row["net_margin"],
                "op_margin": row["op_margin"],
            })
    return results


def find_revenue_acceleration(symbol: str, min_quarters: int = 2,
                              start: str = None, end: str = None) -> list[dict]:
    """
    Signal B: Flag dates where YoY revenue growth has been INCREASING
    for >= min_quarters consecutive quarters.

    Acceleration = this quarter's YoY growth > prior quarter's YoY growth.
    """
    qtrs = _load_quarterly_income(symbol)
    if not qtrs:
        return []
    computed = _compute_quarterly_yoy(qtrs)

    # Filter to rows with valid YoY
    valid = [r for r in computed if r["revenue_yoy"] is not None]

    results = []
    streak = 0
    for i in range(1, len(valid)):
        if valid[i]["revenue_yoy"] > valid[i - 1]["revenue_yoy"]:
            streak += 1
        else:
            streak = 0

        if streak >= min_quarters:
            filing = valid[i]["filing_date"]
            if start and filing < start:
                continue
            if end and filing > end:
                continue
            results.append({
                "signal_date": filing,
                "quarter_end": valid[i]["date"],
                "period": valid[i]["period"],
                "revenue_yoy": valid[i]["revenue_yoy"],
                "prev_revenue_yoy": valid[i - 1]["revenue_yoy"],
                "streak": streak,
            })
    return results


def find_margin_expansion(symbol: str, metric: str = "net_margin",
                          min_quarters: int = 2,
                          start: str = None, end: str = None) -> list[dict]:
    """
    Signal C: Flag dates where margin has expanded YoY for >= min_quarters
    consecutive quarters AND is expanding sequentially (QoQ).

    metric: 'net_margin' or 'op_margin'
    """
    qtrs = _load_quarterly_income(symbol)
    if not qtrs:
        return []
    computed = _compute_quarterly_yoy(qtrs)

    bps_key = f"{metric}_yoy_bps"
    valid = [r for r in computed if r[bps_key] is not None]

    results = []
    streak = 0
    for i in range(1, len(valid)):
        yoy_expanding = valid[i][bps_key] > 0
        seq_expanding = valid[i][metric] > valid[i - 1][metric]

        if yoy_expanding and seq_expanding:
            streak += 1
        else:
            streak = 0

        if streak >= min_quarters:
            filing = valid[i]["filing_date"]
            if start and filing < start:
                continue
            if end and filing > end:
                continue
            results.append({
                "signal_date": filing,
                "quarter_end": valid[i]["date"],
                "period": valid[i]["period"],
                "metric": metric,
                "margin": valid[i][metric],
                "margin_yoy_bps": valid[i][bps_key],
                "streak": streak,
            })
    return results


def find_margin_turnaround(symbol: str, metric: str = "net_margin",
                           threshold_bps: float = 1000,
                           min_quarters: int = 2,
                           start: str = None, end: str = None) -> list[dict]:
    """
    Signal A(b): Flag dates where margin expanded >= threshold_bps YoY
    for >= min_quarters consecutive quarters.
    Catches efficiency turnarounds (e.g. META 2023) even without revenue breakout.
    """
    qtrs = _load_quarterly_income(symbol)
    if not qtrs:
        return []
    computed = _compute_quarterly_yoy(qtrs)

    bps_key = f"{metric}_yoy_bps"
    valid = [r for r in computed if r[bps_key] is not None]

    results = []
    streak = 0
    for i in range(len(valid)):
        if valid[i][bps_key] >= threshold_bps:
            streak += 1
        else:
            streak = 0

        if streak >= min_quarters:
            filing = valid[i]["filing_date"]
            if start and filing < start:
                continue
            if end and filing > end:
                continue
            results.append({
                "signal_date": filing,
                "quarter_end": valid[i]["date"],
                "period": valid[i]["period"],
                "metric": metric,
                "margin": valid[i][metric],
                "margin_yoy_bps": valid[i][bps_key],
                "streak": streak,
            })
    return results


def find_relative_outperformance(symbol: str, benchmark_path: str = None,
                                 threshold_pp: float = 20.0,
                                 window_days: int = 126,
                                 start: str = None, end: str = None,
                                 conn=None) -> list[dict]:
    """
    Signal D: Flag dates where stock's trailing return minus benchmark
    trailing return exceeds threshold_pp (percentage points).
    """
    # Load stock prices from DB
    prices = get_prices(symbol, start=start, end=end, conn=conn)
    if len(prices) < window_days + 1:
        return []

    # Load benchmark (SPX) from JSON
    bench_path = benchmark_path or str(DATA_DIR / "prices" / "indices" / "GSPC.json")
    with open(bench_path) as f:
        bench_raw = json.load(f).get("data", [])
    bench = {b["date"]: b["close"] for b in bench_raw}

    results = []
    price_dict = {d: c for d, c in prices}
    dates = [d for d, c in prices]

    for i in range(window_days, len(dates)):
        date = dates[i]
        if start and date < start:
            continue
        if end and date > end:
            continue

        curr_price = price_dict[date]
        past_date = dates[i - window_days]
        past_price = price_dict[past_date]

        bench_curr = bench.get(date)
        bench_past = bench.get(past_date)
        if not bench_curr or not bench_past or bench_past == 0:
            continue

        stock_ret = (curr_price / past_price - 1) * 100
        bench_ret = (bench_curr / bench_past - 1) * 100
        spread = stock_ret - bench_ret

        if spread >= threshold_pp:
            results.append({
                "signal_date": date,
                "stock_return_pct": round(stock_ret, 2),
                "bench_return_pct": round(bench_ret, 2),
                "spread_pp": round(spread, 2),
                "window_days": window_days,
            })
    return results


def find_volume_conviction(symbol: str, short_window: int = 60,
                           long_window: int = 252, ratio_threshold: float = 0.8,
                           start: str = None, end: str = None,
                           conn=None) -> list[dict]:
    """
    Signal E: Flag dates where short-term avg volume < ratio_threshold * long-term avg volume
    AND price is above its long-term average (rising price + declining volume = conviction).
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    cur = conn.cursor()
    query = "SELECT date, close, volume FROM prices WHERE symbol = ?"
    params = [symbol]
    if start:
        # Pull extra history for the long window
        from datetime import datetime, timedelta
        extended_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=long_window + 50)).strftime("%Y-%m-%d")
        query += " AND date >= ?"
        params.append(extended_start)
    if end:
        query += " AND date <= ?"
        params.append(end)
    query += " ORDER BY date ASC"
    cur.execute(query, params)
    rows = cur.fetchall()
    if own_conn:
        conn.close()

    if len(rows) < long_window + 1:
        return []

    results = []
    for i in range(long_window, len(rows)):
        date, close, volume = rows[i]
        if start and date < start:
            continue
        if end and date > end:
            continue

        short_vols = [rows[j][2] for j in range(i - short_window, i) if rows[j][2]]
        long_vols = [rows[j][2] for j in range(i - long_window, i) if rows[j][2]]

        if not short_vols or not long_vols:
            continue

        short_avg = sum(short_vols) / len(short_vols)
        long_avg = sum(long_vols) / len(long_vols)

        if long_avg == 0:
            continue

        ratio = short_avg / long_avg

        # Price must be above long-term avg (rising)
        long_prices = [rows[j][1] for j in range(i - long_window, i)]
        price_avg = sum(long_prices) / len(long_prices)

        if ratio <= ratio_threshold and close > price_avg:
            results.append({
                "signal_date": date,
                "short_vol": int(short_avg),
                "long_vol": int(long_avg),
                "ratio": round(ratio, 3),
                "price": close,
                "price_avg": round(price_avg, 2),
            })
    return results


# ---------------------------------------------------------------------------
# Fundamental exit signals
# ---------------------------------------------------------------------------

def find_revenue_deceleration(symbol: str, min_quarters: int = 2,
                              require_margin_compression: bool = True,
                              margin_metric: str = "net_margin",
                              start: str = None, end: str = None) -> list[dict]:
    """
    Exit Signal: Flag dates where YoY revenue growth has DECLINED for
    >= min_quarters consecutive quarters.
    If require_margin_compression=True, at least one of those quarters must
    also show YoY margin compression.
    """
    qtrs = _load_quarterly_income(symbol)
    if not qtrs:
        return []
    computed = _compute_quarterly_yoy(qtrs)

    valid = [r for r in computed if r["revenue_yoy"] is not None]

    results = []
    streak = 0
    margin_compressed_in_streak = False
    bps_key = f"{margin_metric}_yoy_bps"

    for i in range(1, len(valid)):
        if valid[i]["revenue_yoy"] < valid[i - 1]["revenue_yoy"]:
            streak += 1
            if valid[i][bps_key] is not None and valid[i][bps_key] < 0:
                margin_compressed_in_streak = True
        else:
            streak = 0
            margin_compressed_in_streak = False

        if streak >= min_quarters:
            if require_margin_compression and not margin_compressed_in_streak:
                continue

            filing = valid[i]["filing_date"]
            if start and filing < start:
                continue
            if end and filing > end:
                continue

            results.append({
                "signal_date": filing,
                "quarter_end": valid[i]["date"],
                "period": valid[i]["period"],
                "revenue_yoy": valid[i]["revenue_yoy"],
                "prev_revenue_yoy": valid[i - 1]["revenue_yoy"],
                "margin_yoy_bps": valid[i][bps_key],
                "streak": streak,
            })
    return results


def find_margin_collapse(symbol: str, metric: str = "net_margin",
                         threshold_bps: float = -500,
                         min_quarters: int = 2,
                         start: str = None, end: str = None) -> list[dict]:
    """
    Exit Signal: Flag dates where margin contracted > threshold_bps YoY
    for >= min_quarters consecutive quarters.
    """
    qtrs = _load_quarterly_income(symbol)
    if not qtrs:
        return []
    computed = _compute_quarterly_yoy(qtrs)

    bps_key = f"{metric}_yoy_bps"
    valid = [r for r in computed if r[bps_key] is not None]

    results = []
    streak = 0
    for i in range(len(valid)):
        if valid[i][bps_key] <= threshold_bps:
            streak += 1
        else:
            streak = 0

        if streak >= min_quarters:
            filing = valid[i]["filing_date"]
            if start and filing < start:
                continue
            if end and filing > end:
                continue
            results.append({
                "signal_date": filing,
                "quarter_end": valid[i]["date"],
                "period": valid[i]["period"],
                "metric": metric,
                "margin": valid[i][metric],
                "margin_yoy_bps": valid[i][bps_key],
                "streak": streak,
            })
    return results


# ---------------------------------------------------------------------------
# Technical signals (for backtest engine conditions)
# ---------------------------------------------------------------------------

def compute_rsi(symbol: str, period: int = 14, start: str = None, end: str = None,
                conn=None, price_index: dict = None) -> dict:
    """
    Compute RSI(period) for a ticker.

    Returns:
        dict of {date: rsi_value} where rsi_value is 0-100.
    """
    if price_index and symbol in price_index:
        rows = sorted(price_index[symbol].items())  # [(date, close), ...]
    else:
        own_conn = conn is None
        if own_conn:
            conn = get_connection()

        cur = conn.cursor()
        cur.execute(
            "SELECT date, close FROM prices WHERE symbol = ? ORDER BY date ASC",
            [symbol],
        )
        rows = cur.fetchall()
        if own_conn:
            conn.close()

    if len(rows) < period + 1:
        return {}

    # Compute daily changes
    changes = []
    for i in range(1, len(rows)):
        changes.append((rows[i][0], rows[i][1] - rows[i - 1][1]))

    # Seed with SMA of first `period` gains/losses
    gains = [max(c, 0) for _, c in changes[:period]]
    losses = [max(-c, 0) for _, c in changes[:period]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    result = {}
    # First RSI value
    date = changes[period - 1][0]
    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    if (not start or date >= start) and (not end or date <= end):
        result[date] = round(rsi, 2)

    # Wilder's smoothing for remaining
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

        if (not start or date >= start) and (not end or date <= end):
            result[date] = round(rsi, 2)

    return result


def compute_momentum_rank(symbols: list[str], lookback: int = 63,
                          start: str = None, end: str = None,
                          conn=None, price_index: dict = None) -> dict:
    """
    Compute percentile momentum rank for each symbol vs the universe.
    Momentum = total return over `lookback` trading days.

    Returns:
        dict of {symbol: {date: percentile_rank}} where rank is 0-100.
        100 = best momentum in universe, 0 = worst.
    """
    if price_index:
        all_prices = {sym: dict(price_index[sym]) for sym in symbols if sym in price_index}
    else:
        own_conn = conn is None
        if own_conn:
            conn = get_connection()

        all_prices = {}
        cur = conn.cursor()
        for sym in symbols:
            cur.execute(
                "SELECT date, close FROM prices WHERE symbol = ? ORDER BY date ASC",
                [sym],
            )
            rows = cur.fetchall()
            all_prices[sym] = {r[0]: r[1] for r in rows}

        if own_conn:
            conn.close()

    # Get union of all dates, sorted
    all_dates = sorted(set(d for prices in all_prices.values() for d in prices))

    result = {sym: {} for sym in symbols}

    for i in range(lookback, len(all_dates)):
        date = all_dates[i]
        lookback_date = all_dates[i - lookback]

        if start and date < start:
            continue
        if end and date > end:
            continue

        # Compute returns for all symbols that have data at both dates
        returns = {}
        for sym in symbols:
            p_now = all_prices[sym].get(date)
            p_then = all_prices[sym].get(lookback_date)
            if p_now and p_then and p_then > 0:
                returns[sym] = (p_now - p_then) / p_then

        if len(returns) < 2:
            continue

        # Rank: percentile position
        sorted_syms = sorted(returns, key=lambda s: returns[s])
        n = len(sorted_syms)
        for rank_idx, sym in enumerate(sorted_syms):
            pct = round(rank_idx / (n - 1) * 100, 1) if n > 1 else 50.0
            result[sym][date] = pct

    return result


def compute_ma_crossover(symbol: str, fast: int = 50, slow: int = 200,
                         start: str = None, end: str = None,
                         conn=None, price_index: dict = None) -> dict:
    """
    Compute moving average crossover signal.

    Returns:
        dict of {date: signal} where:
          +1 = fast MA > slow MA (bullish / golden cross)
          -1 = fast MA < slow MA (bearish / death cross)
           0 = equal or insufficient data
    """
    if price_index and symbol in price_index:
        rows = sorted(price_index[symbol].items())  # [(date, close), ...]
    else:
        own_conn = conn is None
        if own_conn:
            conn = get_connection()

        cur = conn.cursor()
        cur.execute(
            "SELECT date, close FROM prices WHERE symbol = ? ORDER BY date ASC",
            [symbol],
        )
        rows = cur.fetchall()
        if own_conn:
            conn.close()

    if len(rows) < slow:
        return {}

    closes = [r[1] for r in rows]
    dates = [r[0] for r in rows]
    result = {}

    for i in range(slow, len(rows)):
        date = dates[i]
        if start and date < start:
            continue
        if end and date > end:
            continue

        fast_ma = sum(closes[i - fast + 1:i + 1]) / fast
        slow_ma = sum(closes[i - slow + 1:i + 1]) / slow

        if fast_ma > slow_ma:
            result[date] = 1
        elif fast_ma < slow_ma:
            result[date] = -1
        else:
            result[date] = 0

    return result


def compute_volume_capitulation(symbol: str, window: int = 20, multiplier: float = 3.0,
                                start: str = None, end: str = None,
                                conn=None) -> dict:
    """
    Detect volume capitulation: dates where volume > multiplier * average(window)
    AND price closed down on the day.

    Returns:
        dict of {date: volume_ratio} for capitulation days only.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    cur = conn.cursor()
    cur.execute(
        "SELECT date, close, volume FROM prices WHERE symbol = ? ORDER BY date ASC",
        [symbol],
    )
    rows = cur.fetchall()
    if own_conn:
        conn.close()

    if len(rows) < window + 1:
        return {}

    result = {}
    for i in range(window, len(rows)):
        date, close, volume = rows[i]
        if not volume or volume <= 0:
            continue
        if start and date < start:
            continue
        if end and date > end:
            continue

        prev_close = rows[i - 1][1]
        if close >= prev_close:
            continue  # not a down day

        avg_vol = sum(r[2] for r in rows[i - window:i] if r[2] and r[2] > 0)
        count = sum(1 for r in rows[i - window:i] if r[2] and r[2] > 0)
        if count == 0:
            continue
        avg_vol /= count

        ratio = volume / avg_vol
        if ratio >= multiplier:
            result[date] = round(ratio, 2)

    return result


def main():
    parser = argparse.ArgumentParser(description="AlphaScout Signals")
    subparsers = parser.add_subparsers(dest="command")

    # selloffs command
    sell_parser = subparsers.add_parser("selloffs", help="Find selloffs for a ticker")
    sell_parser.add_argument("ticker", type=str, help="Ticker symbol")
    sell_parser.add_argument("--drop", type=float, default=-20, help="Drop threshold (e.g., -20)")
    sell_parser.add_argument("--peak-window", type=str, default="all_time",
                            help="Peak window: all_time, 52w, ytd, or YYYY-MM-DD")
    sell_parser.add_argument("--start", type=str, help="Start date")
    sell_parser.add_argument("--end", type=str, help="End date")

    # recovery command
    rec_parser = subparsers.add_parser("recovery", help="Find selloffs with recovery")
    rec_parser.add_argument("ticker", type=str, help="Ticker symbol")
    rec_parser.add_argument("--drop", type=float, default=-20, help="Drop threshold")
    rec_parser.add_argument("--recovery", type=str, default="+10% from bottom", help="Recovery condition")
    rec_parser.add_argument("--recovery-within", type=str, help="Max time for recovery (e.g., 1y, 6m, 90d)")
    rec_parser.add_argument("--peak-window", type=str, default="all_time")
    rec_parser.add_argument("--start", type=str, help="Start date")
    rec_parser.add_argument("--end", type=str, help="End date")

    # period-drops command
    period_parser = subparsers.add_parser("period-drops", help="Find drops over N days")
    period_parser.add_argument("ticker", type=str, help="Ticker symbol")
    period_parser.add_argument("--days", type=int, default=5, help="Rolling period in trading days")
    period_parser.add_argument("--drop", type=float, default=-10, help="Drop threshold (e.g., -10)")
    period_parser.add_argument("--recovery", type=str, help="Recovery condition (e.g., '+10% from bottom')")
    period_parser.add_argument("--recovery-within", type=str, help="Max time for recovery (e.g., 1y, 6m, 90d)")
    period_parser.add_argument("--raw", action="store_true", help="Output every signal date (no aggregation)")
    period_parser.add_argument("--start", type=str, help="Start date")
    period_parser.add_argument("--end", type=str, help="End date")

    # daily-drops command
    daily_parser = subparsers.add_parser("daily-drops", help="Find single-day drops")
    daily_parser.add_argument("ticker", type=str, help="Ticker symbol")
    daily_parser.add_argument("--drop", type=float, default=-5, help="Drop threshold (e.g., -5)")
    daily_parser.add_argument("--start", type=str, help="Start date")
    daily_parser.add_argument("--end", type=str, help="End date")

    # scan command (selloffs)
    scan_parser = subparsers.add_parser("scan", help="Scan universe for selloffs")
    scan_parser.add_argument("--sector", type=str, help="Filter by sector")
    scan_parser.add_argument("--symbols", type=str, help="Comma-separated tickers")
    scan_parser.add_argument("--drop", type=float, default=-20, help="Drop threshold")
    scan_parser.add_argument("--recovery", type=str, help="Recovery condition")
    scan_parser.add_argument("--recovery-within", type=str, help="Max time for recovery (e.g., 1y, 6m, 90d)")
    scan_parser.add_argument("--peak-window", type=str, default="all_time")
    scan_parser.add_argument("--start", type=str, help="Start date")
    scan_parser.add_argument("--end", type=str, help="End date")
    scan_parser.add_argument("--limit", type=int, default=0, help="Max results (0 = unlimited)")

    # scan-period command
    scanp_parser = subparsers.add_parser("scan-period", help="Scan universe for N-day drops")
    scanp_parser.add_argument("--sector", type=str, help="Filter by sector")
    scanp_parser.add_argument("--symbols", type=str, help="Comma-separated tickers")
    scanp_parser.add_argument("--days", type=int, default=5, help="Rolling period in trading days")
    scanp_parser.add_argument("--drop", type=float, default=-10, help="Drop threshold")
    scanp_parser.add_argument("--recovery", type=str, help="Recovery condition (e.g., '+10% from bottom')")
    scanp_parser.add_argument("--recovery-within", type=str, help="Max time for recovery (e.g., 1y, 6m, 90d)")
    scanp_parser.add_argument("--recovered-only", action="store_true", help="Only show events that recovered")
    scanp_parser.add_argument("--raw", action="store_true", help="Output every signal date (no aggregation)")
    scanp_parser.add_argument("--start", type=str, help="Start date")
    scanp_parser.add_argument("--end", type=str, help="End date")
    scanp_parser.add_argument("--limit", type=int, default=0, help="Max results (0 = unlimited)")

    # scan-daily command
    scand_parser = subparsers.add_parser("scan-daily", help="Scan universe for single-day drops")
    scand_parser.add_argument("--sector", type=str, help="Filter by sector")
    scand_parser.add_argument("--symbols", type=str, help="Comma-separated tickers")
    scand_parser.add_argument("--drop", type=float, default=-5, help="Drop threshold")
    scand_parser.add_argument("--start", type=str, help="Start date")
    scand_parser.add_argument("--end", type=str, help="End date")
    scand_parser.add_argument("--limit", type=int, default=0, help="Max results (0 = unlimited)")

    args = parser.parse_args()

    if args.command == "selloffs":
        prices = get_prices(args.ticker, start=args.start, end=args.end)
        selloffs = find_selloffs(prices, drop_threshold=args.drop, peak_window=args.peak_window)
        _print_selloffs([{"symbol": args.ticker, **s} for s in selloffs])

    elif args.command == "recovery":
        within_days = _parse_duration_days(args.recovery_within) if args.recovery_within else None
        prices = get_prices(args.ticker, start=args.start, end=args.end)
        selloffs = find_selloffs(prices, drop_threshold=args.drop, peak_window=args.peak_window)
        results = []
        for s in selloffs:
            r = {"symbol": args.ticker, **s}
            recovery = find_recovery(prices, s["trough_date"], s["trough_price"],
                                    s["peak_price"], args.recovery,
                                    recovery_within_days=within_days)
            r["recovery"] = recovery
            results.append(r)
        _print_selloffs(results, show_recovery=True)

    elif args.command == "period-drops":
        within_days = _parse_duration_days(args.recovery_within) if args.recovery_within else None
        prices = get_prices(args.ticker, start=args.start, end=args.end if not args.recovery else None)
        raw = find_period_drops(prices, period_days=args.days, threshold=args.drop)

        if args.raw:
            # Raw mode: every signal date
            if not raw:
                print("No signals found.")
            else:
                print(f"\n  {len(raw)} signal date(s) where {args.drop}% drawdown detected in {args.days}-day window:\n")
                print(f"{'Symbol':<8} {'Signal Date':<12} {'Close':>8} {'Peak Date':<12} {'Peak $':>8} {'Trough Date':<12} {'Trough $':>8} {'Drawdown':>9}")
                print("-" * 100)
                for e in raw:
                    print(f"{args.ticker:<8} {e['signal_date']:<12} ${e['close']:>7.2f} "
                          f"{e['peak_date']:<12} ${e['peak_price']:>7.2f} "
                          f"{e['trough_date']:<12} ${e['trough_price']:>7.2f} "
                          f"{e['drawdown_pct']:>8.1f}%")
        elif args.recovery:
            events = summarize_period_drops(raw, prices, args.days)
            results = []
            for drop in events:
                recovery = find_recovery(prices, drop["trough_date"], drop["trough_price"],
                                        drop["peak_price"], args.recovery,
                                        recovery_within_days=within_days)
                result = {**drop, "recovery": recovery, "recovery_met": bool(recovery)}
                results.append(result)
            if not results:
                print("No period drops found.")
            else:
                print(f"\n  Found {len(results)} selloff event(s) of {args.drop}% or worse over {args.days}-day windows:\n")
                hdr = f"{'Symbol':<8} {'Peak Date':<12} {'Peak $':>8} {'Trough $':>8} {'Trough Date':<12} {'Drop':>8} {'Recovery Date':<14} {'Rec $':>8} {'Days':>6}"
                print(hdr)
                print("-" * len(hdr))
                for e in results:
                    rec = e.get("recovery")
                    rec_date = rec["recovery_date"] if rec else "—"
                    rec_price = f"${rec['recovery_price']:>7.2f}" if rec else "     —"
                    rec_days = f"{rec['days_from_trough']:>5}d" if rec else "    —"
                    print(f"{args.ticker:<8} {e['peak_date']:<12} ${e['peak_price']:>7.2f} "
                          f"${e['trough_price']:>7.2f} {e['trough_date']:<12} "
                          f"{e['drawdown_pct']:>7.1f}% {rec_date:<14} {rec_price} {rec_days}")
        else:
            events = summarize_period_drops(raw, prices, args.days)
            if not events:
                print("No period drops found.")
            else:
                print(f"\n  Found {len(events)} selloff event(s) of {args.drop}% or worse over {args.days}-day windows:\n")
                print(f"{'Symbol':<8} {'Peak Date':<12} {'Peak $':>8} {'Trough Date':<12} {'Trough $':>8} {'Drawdown':>9} {'Signals':>8}")
                print("-" * 80)
                for e in events:
                    print(f"{args.ticker:<8} {e['peak_date']:<12} ${e['peak_price']:>7.2f} "
                          f"{e['trough_date']:<12} ${e['trough_price']:>7.2f} "
                          f"{e['drawdown_pct']:>8.1f}% {e['signal_count']:>7}")

    elif args.command == "daily-drops":
        prices = get_prices(args.ticker, start=args.start, end=args.end)
        events = find_daily_drops(prices, threshold=args.drop)
        if not events:
            print("No single-day drops found.")
        else:
            print(f"\n  Found {len(events)} day(s) with {args.drop}% or worse single-day drop:\n")
            print(f"{'Symbol':<8} {'Date':<12} {'Close':>8} {'Prev Close':>10} {'Change':>8}")
            print("-" * 55)
            for e in events:
                print(f"{args.ticker:<8} {e['date']:<12} ${e['close']:>7.2f} "
                      f"${e['prev_close']:>9.2f} {e['change_pct']:>7.1f}%")

    elif args.command == "scan":
        symbols = args.symbols.split(",") if args.symbols else None
        within_days = _parse_duration_days(args.recovery_within) if args.recovery_within else None
        results = scan_universe(
            symbols=symbols, sector=args.sector,
            drop_threshold=args.drop, peak_window=args.peak_window,
            recovery_condition=args.recovery, recovery_within_days=within_days,
            start=args.start, end=args.end,
        )
        display = results if args.limit == 0 else results[:args.limit]
        _print_selloffs(display, show_recovery=bool(args.recovery))

    elif args.command == "scan-period":
        symbols = args.symbols.split(",") if args.symbols else None
        within_days = _parse_duration_days(args.recovery_within) if args.recovery_within else None
        conn = get_connection()
        resolved_symbols = symbols or _get_symbols(sector=args.sector, conn=conn)

        if args.raw:
            # Raw mode: every signal date across universe
            results = []
            for sym in resolved_symbols:
                prices = get_prices(sym, start=args.start, end=args.end, conn=conn)
                if len(prices) <= args.days:
                    continue
                raw = find_period_drops(prices, period_days=args.days, threshold=args.drop)
                for e in raw:
                    if args.start and e["signal_date"] < args.start:
                        continue
                    if args.end and e["signal_date"] > args.end:
                        continue
                    results.append({"symbol": sym, **e})
            conn.close()
            results.sort(key=lambda x: x["signal_date"])
            if not results:
                print("No signals found.")
            else:
                display = results if args.limit == 0 else results[:args.limit]
                print(f"\n  {len(results)} signal date(s) across {len(resolved_symbols)} tickers:\n")
                print(f"{'Symbol':<8} {'Signal Date':<12} {'Close':>8} {'Peak Date':<12} {'Peak $':>8} {'Trough Date':<12} {'Trough $':>8} {'Drawdown':>9}")
                print("-" * 100)
                for e in display:
                    print(f"{e['symbol']:<8} {e['signal_date']:<12} ${e['close']:>7.2f} "
                          f"{e['peak_date']:<12} ${e['peak_price']:>7.2f} "
                          f"{e['trough_date']:<12} ${e['trough_price']:>7.2f} "
                          f"{e['drawdown_pct']:>8.1f}%")
        elif args.recovery:
            conn.close()
            results = scan_period_drops_with_recovery(
                symbols=symbols, sector=args.sector,
                period_days=args.days, threshold=args.drop,
                recovery_condition=args.recovery, recovery_within_days=within_days,
                recovered_only=args.recovered_only,
                start=args.start, end=args.end,
            )
            if not results:
                print("No period drops found.")
            else:
                display = results if args.limit == 0 else results[:args.limit]
                rec_count = sum(1 for r in results if r.get("recovery_met"))
                print(f"\n  Found {len(results)} selloff event(s) of {args.drop}% or worse over {args.days}-day windows "
                      f"({rec_count} recovered):\n")
                hdr = f"{'Symbol':<8} {'Peak Date':<12} {'Peak $':>8} {'Trough $':>8} {'Trough Date':<12} {'Drop':>8} {'Recovery Date':<14} {'Rec $':>8} {'Days':>6}"
                print(hdr)
                print("-" * len(hdr))
                for e in display:
                    rec = e.get("recovery")
                    rec_date = rec["recovery_date"] if rec else "—"
                    rec_price = f"${rec['recovery_price']:>7.2f}" if rec else "     —"
                    rec_days = f"{rec['days_from_trough']:>5}d" if rec else "    —"
                    print(f"{e['symbol']:<8} {e['peak_date']:<12} ${e['peak_price']:>7.2f} "
                          f"${e['trough_price']:>7.2f} {e['trough_date']:<12} "
                          f"{e['drawdown_pct']:>7.1f}% {rec_date:<14} {rec_price} {rec_days}")
        else:
            conn.close()
            results = scan_period_drops(
                symbols=symbols, sector=args.sector,
                period_days=args.days, threshold=args.drop,
                start=args.start, end=args.end,
            )
            if not results:
                print("No period drops found.")
            else:
                display = results if args.limit == 0 else results[:args.limit]
                print(f"\n  Found {len(results)} selloff event(s) of {args.drop}% or worse over {args.days}-day windows:\n")
                print(f"{'Symbol':<8} {'Peak Date':<12} {'Peak $':>8} {'Trough Date':<12} {'Trough $':>8} {'Drawdown':>9} {'Signals':>8}")
                print("-" * 80)
                for e in display:
                    print(f"{e['symbol']:<8} {e['peak_date']:<12} ${e['peak_price']:>7.2f} "
                          f"{e['trough_date']:<12} ${e['trough_price']:>7.2f} "
                          f"{e['drawdown_pct']:>8.1f}% {e['signal_count']:>7}")

    elif args.command == "scan-daily":
        symbols = args.symbols.split(",") if args.symbols else None
        results = scan_daily_drops(
            symbols=symbols, sector=args.sector,
            threshold=args.drop,
            start=args.start, end=args.end,
        )
        if not results:
            print("No single-day drops found.")
        else:
            display = results if args.limit == 0 else results[:args.limit]
            print(f"\n  Found {len(results)} day(s) with {args.drop}% or worse drop:\n")
            print(f"{'Symbol':<8} {'Date':<12} {'Close':>8} {'Prev Close':>10} {'Change':>8}")
            print("-" * 55)
            for e in display:
                print(f"{e['symbol']:<8} {e['date']:<12} ${e['close']:>7.2f} "
                      f"${e['prev_close']:>9.2f} {e['change_pct']:>7.1f}%")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
