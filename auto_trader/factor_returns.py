"""Shared factor-return compute kernel.

Two callers share this code:

  1. `scripts/build_factor_returns_daily.py` — precomputes spreads across
     'all' + 11 GICS sectors, persisted to `factor_returns_daily` for fast
     lookup.
  2. `auto_trader.attribution` — when a portfolio's declared eligible
     universe is a custom symbol list (not a sector or 'all'), the
     attribution call computes the same Q5-Q1 spreads on-the-fly against
     that exact symbol set so z-scores and factor returns share the same
     reference frame.

The math is identical in both paths. Only the inputs differ.
"""
from __future__ import annotations

import math
import sqlite3
from typing import Iterable

import numpy as np


CANONICAL_FACTORS: list[str] = [
    "ret_12_1m", "ret_3m", "ret_1m",
    "pe", "ev_ebitda",
    "fcf_yield",
    "rev_yoy", "eps_yoy", "rev_yoy_accel",
    "roe", "gross_margin", "debt_to_equity",
    "analyst_net_upgrades_90d",
]

# Direction: "lower" factors flip sign so Q5−Q1 represents "the named bet pays."
FACTOR_DIRECTION: dict[str, str] = {
    "ret_12_1m": "higher", "ret_3m": "higher", "ret_1m": "higher",
    "pe": "lower", "ev_ebitda": "lower",
    "fcf_yield": "higher",
    "rev_yoy": "higher", "eps_yoy": "higher", "rev_yoy_accel": "higher",
    "roe": "higher", "gross_margin": "higher", "debt_to_equity": "lower",
    "analyst_net_upgrades_90d": "higher",
}

N_BUCKETS = 5
DEFAULT_MIN_SYMBOLS = 20  # adhoc + sector minimum; precomputed 'all' uses 50


def load_forward_returns(conn: sqlite3.Connection,
                          since: str | None = None,
                          until: str | None = None,
                          symbols: Iterable[str] | None = None,
                          ) -> dict[tuple[str, str], float]:
    """Return {(date, symbol): forward 1-day return in pp}.

    Each symbol's forward return uses its own next-available trading day, so
    delisted/IPO gaps are handled cleanly.
    """
    where = []
    args: list = []
    if since:
        where.append("date >= ?"); args.append(since)
    if until:
        where.append("date <= ?"); args.append(until)
    if symbols is not None:
        syms = list(symbols)
        if not syms:
            return {}
        where.append(f"symbol IN ({','.join('?' * len(syms))})")
        args.extend(syms)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(
        f"SELECT symbol, date, close FROM prices {where_sql} ORDER BY symbol, date",
        args,
    ).fetchall()
    fwd: dict[tuple[str, str], float] = {}
    if not rows:
        return fwd
    cur_sym = None
    buf: list[tuple[str, float]] = []
    for sym, d, c in rows:
        if sym != cur_sym:
            _flush_fwd(buf, cur_sym, fwd)
            cur_sym = sym
            buf = []
        if c is not None and c > 0:
            buf.append((d, float(c)))
    _flush_fwd(buf, cur_sym, fwd)
    return fwd


def _flush_fwd(buf: list[tuple[str, float]], sym: str | None,
                fwd: dict[tuple[str, str], float]) -> None:
    if not sym or len(buf) < 2:
        return
    for i in range(len(buf) - 1):
        d, c0 = buf[i]
        _, c1 = buf[i + 1]
        fwd[(d, sym)] = (c1 / c0 - 1.0) * 100.0


def load_factor_panel(conn: sqlite3.Connection,
                       since: str | None = None,
                       until: str | None = None,
                       symbols: Iterable[str] | None = None,
                       ) -> dict[str, dict[str, dict[str, float]]]:
    """Return panel[factor][date][symbol] = factor_value, for the canonical
    factor set."""
    where = []
    args: list = []
    if since:
        where.append("date >= ?"); args.append(since)
    if until:
        where.append("date <= ?"); args.append(until)
    if symbols is not None:
        syms = list(symbols)
        if not syms:
            return {f: {} for f in CANONICAL_FACTORS}
        where.append(f"symbol IN ({','.join('?' * len(syms))})")
        args.extend(syms)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    fcols = ", ".join(CANONICAL_FACTORS)
    rows = conn.execute(
        f"SELECT date, symbol, {fcols} FROM features_daily {where_sql}",
        args,
    ).fetchall()
    panel: dict[str, dict[str, dict[str, float]]] = {f: {} for f in CANONICAL_FACTORS}
    for row in rows:
        d, sym = row[0], row[1]
        for i, f in enumerate(CANONICAL_FACTORS, start=2):
            v = row[i]
            if v is None:
                continue
            try:
                vf = float(v)
            except (TypeError, ValueError):
                continue
            if not np.isfinite(vf):
                continue
            panel[f].setdefault(d, {})[sym] = vf
    return panel


def compute_spread_for_date(values_by_symbol: dict[str, float],
                             fwd_by_symbol: dict[tuple[str, str], float],
                             date: str, direction: str,
                             min_symbols: int = DEFAULT_MIN_SYMBOLS,
                             symbol_filter: set[str] | None = None,
                             ) -> tuple[float, float, float, int] | None:
    """Return (spread_pp, q1_mean, q5_mean, n_symbols) or None if too few
    symbols on `date` to form quintiles. spread_pp is sign-flipped for
    "lower" factors."""
    pairs: list[tuple[float, float]] = []
    for sym, v in values_by_symbol.items():
        if symbol_filter is not None and sym not in symbol_filter:
            continue
        ret = fwd_by_symbol.get((date, sym))
        if ret is None:
            continue
        pairs.append((v, ret))
    n = len(pairs)
    if n < min_symbols:
        return None
    arr = np.array(pairs, dtype=np.float64)
    order = np.argsort(arr[:, 0], kind="stable")
    sr = arr[order, 1]
    edges = np.linspace(0, n, N_BUCKETS + 1, dtype=int)
    q1 = sr[edges[0]:edges[1]]
    q5 = sr[edges[N_BUCKETS - 1]:edges[N_BUCKETS]]
    if q1.size == 0 or q5.size == 0:
        return None
    q1m = float(q1.mean())
    q5m = float(q5.mean())
    spread = q5m - q1m
    if direction == "lower":
        spread = -spread
    return spread, q1m, q5m, n


def compute_factor_returns_adhoc(market_conn: sqlite3.Connection,
                                  symbols: list[str],
                                  start: str, end: str,
                                  ) -> tuple[dict[str, float], dict[str, int]]:
    """Compute cumulative log factor returns for a CUSTOM universe over (start, end].

    Returns ({factor: cum_log_pp}, {factor: n_days_kept}). Drop-in for the
    precomputed-table lookup: same shape, same units. Used when the strategy's
    declared eligible universe is a custom symbol list (not a precomputed
    sector or 'all').
    """
    if not symbols:
        return {}, {}
    syms_set = set(symbols)
    fwd = load_forward_returns(market_conn, since=start, until=end, symbols=symbols)
    panel = load_factor_panel(market_conn, since=start, until=end, symbols=symbols)

    cum_log: dict[str, float] = {}
    n_days: dict[str, int] = {}
    for factor in CANONICAL_FACTORS:
        direction = FACTOR_DIRECTION[factor]
        for date, values_by_symbol in panel[factor].items():
            # only count dates strictly after `start` to match precomputed lookup
            if date <= start:
                continue
            res = compute_spread_for_date(
                values_by_symbol, fwd, date, direction,
                min_symbols=DEFAULT_MIN_SYMBOLS,
                symbol_filter=syms_set,
            )
            if res is None:
                continue
            spread_pp = res[0]
            r = spread_pp / 100.0
            if r <= -0.99999:
                continue
            cum_log[factor] = cum_log.get(factor, 0.0) + math.log1p(r) * 100.0
            n_days[factor] = n_days.get(factor, 0) + 1
    return cum_log, n_days
