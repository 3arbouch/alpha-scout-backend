"""Build / refresh `factor_returns_daily` in dev market.db.

For each (date, factor) compute the Q5−Q1 spread of forward 1-day returns
across the canonical factor set. This is the daily "factor return" series
that downstream attribution uses to translate portfolio exposures into
realized contribution in pp.

Output schema:
    factor_returns_daily (
        date         DATE,
        factor       TEXT,
        spread_pp    REAL,    -- Q5_mean − Q1_mean over forward 1d, percent.
                              -- Sign-flipped for "lower" factors so spread > 0
                              -- always means "the named bet paid."
        q1_mean_pp   REAL,    -- raw Q1 (bottom-of-factor) mean fwd return
        q5_mean_pp   REAL,    -- raw Q5 (top-of-factor)   mean fwd return
        n_symbols    INTEGER, -- symbols ranked into buckets that day
        universe_id  TEXT DEFAULT 'all',
        PRIMARY KEY (date, factor, universe_id)
    )

Rerunnable: INSERT OR REPLACE. Pass --since YYYY-MM-DD to refresh tail only.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path

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

MIN_SYMBOLS_PER_DATE = 50           # for the broad 'all' universe
MIN_SYMBOLS_PER_DATE_SECTOR = 20    # sector universes are smaller (20-90 names)
N_BUCKETS = 5

# The 11 GICS-aligned sectors we precompute against, plus 'all' for the
# multi-sector / broad-market case. Sector names match `universe_profiles.sector`.
SECTOR_UNIVERSES: list[str] = [
    "Technology", "Healthcare", "Financial Services", "Industrials",
    "Consumer Cyclical", "Consumer Defensive", "Energy", "Real Estate",
    "Communication Services", "Utilities", "Basic Materials",
]


def _resolve_db_path() -> Path:
    env = os.environ.get("MARKET_DB_PATH")
    if env:
        return Path(env)
    p = Path("/home/mohamed/alpha-scout-backend-dev/data/market_dev.db")
    if not p.exists() or p.stat().st_size == 0:
        sys.exit(f"market_dev.db missing or empty at {p}. Set MARKET_DB_PATH.")
    return p


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS factor_returns_daily (
            date         TEXT NOT NULL,
            factor       TEXT NOT NULL,
            spread_pp    REAL NOT NULL,
            q1_mean_pp   REAL,
            q5_mean_pp   REAL,
            n_symbols    INTEGER NOT NULL,
            universe_id  TEXT NOT NULL DEFAULT 'all',
            PRIMARY KEY (date, factor, universe_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_frd_factor_date "
                 "ON factor_returns_daily(factor, date)")
    conn.commit()


def _load_forward_returns(conn: sqlite3.Connection, since: str | None
                          ) -> dict[tuple[str, str], float]:
    """Return {(date, symbol): fwd_ret_pp}. Computes next-trading-day return
    per-symbol (handles delisting/IPO gaps cleanly because each symbol uses
    its own next available trading day)."""
    where = "WHERE date >= ?" if since else ""
    args = (since,) if since else ()
    rows = conn.execute(
        f"SELECT symbol, date, close FROM prices {where} "
        f"ORDER BY symbol, date",
        args,
    ).fetchall()
    fwd: dict[tuple[str, str], float] = {}
    if not rows:
        return fwd
    cur_sym = None
    buf: list[tuple[str, float]] = []  # (date, close) for current symbol
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


def _load_factor_panel(conn: sqlite3.Connection, since: str | None
                       ) -> dict[str, dict[str, dict[str, float]]]:
    """Return panel[factor][date][symbol] = factor_value."""
    where = "WHERE date >= ?" if since else ""
    args = (since,) if since else ()
    fcols = ", ".join(CANONICAL_FACTORS)
    rows = conn.execute(
        f"SELECT date, symbol, {fcols} FROM features_daily {where}",
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


def _compute_spread_for_date(values_by_symbol: dict[str, float],
                              fwd_by_symbol: dict[tuple[str, str], float],
                              date: str, direction: str,
                              min_symbols: int = MIN_SYMBOLS_PER_DATE,
                              symbol_filter: set[str] | None = None,
                              ) -> tuple[float, float, float, int] | None:
    """Return (spread_pp, q1_mean, q5_mean, n_symbols) or None if insufficient.

    `symbol_filter` restricts ranking to that subset (used for per-sector runs).
    """
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

    arr = np.array(pairs, dtype=np.float64)  # cols: factor_value, fwd_ret
    factor_vals = arr[:, 0]
    fwd_vals = arr[:, 1]

    # Equal-count quintile assignment via argsort. Ties: stable sort, then
    # split — bucket counts may differ by 1 across buckets.
    order = np.argsort(factor_vals, kind="stable")
    sorted_returns = fwd_vals[order]
    edges = np.linspace(0, n, N_BUCKETS + 1, dtype=int)
    q1 = sorted_returns[edges[0]:edges[1]]
    q5 = sorted_returns[edges[N_BUCKETS - 1]:edges[N_BUCKETS]]
    if q1.size == 0 or q5.size == 0:
        return None
    q1_mean = float(q1.mean())
    q5_mean = float(q5.mean())
    spread = q5_mean - q1_mean
    if direction == "lower":
        spread = -spread
    return spread, q1_mean, q5_mean, n


def _load_sector_symbols(conn: sqlite3.Connection) -> dict[str, set[str]]:
    """{sector: {symbols}} for the 11 sector universes."""
    out: dict[str, set[str]] = {}
    for sec in SECTOR_UNIVERSES:
        rows = conn.execute(
            "SELECT symbol FROM universe_profiles WHERE sector = ?", (sec,)
        ).fetchall()
        out[sec] = {r[0] for r in rows}
    return out


def _compute_all_spreads(panel, fwd, universe_id: str,
                         symbol_filter: set[str] | None,
                         min_symbols: int) -> list[tuple]:
    """One universe's worth of (date, factor) spread rows.

    Pass symbol_filter=None for 'all'; pass the sector's symbol set for sector runs.
    """
    out_rows: list[tuple] = []
    t0 = time.time()
    for factor in CANONICAL_FACTORS:
        direction = FACTOR_DIRECTION[factor]
        dates_for_factor = panel.get(factor, {})
        n_dates = len(dates_for_factor)
        if n_dates == 0:
            continue
        kept = 0
        for date, values_by_symbol in dates_for_factor.items():
            result = _compute_spread_for_date(
                values_by_symbol, fwd, date, direction,
                min_symbols=min_symbols, symbol_filter=symbol_filter,
            )
            if result is None:
                continue
            spread, q1_mean, q5_mean, n_symbols = result
            out_rows.append((date, factor, spread, q1_mean, q5_mean, n_symbols, universe_id))
            kept += 1
        print(f"    {factor:30s} {kept:>5d} / {n_dates:>5d} dates")
    print(f"  [{universe_id}] {len(out_rows):,} rows in {time.time() - t0:.1f}s")
    return out_rows


def _persist(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    """Rows are 7-tuples now: (date, factor, spread, q1, q5, n, universe_id)."""
    if not rows:
        return 0
    conn.executemany(
        """INSERT OR REPLACE INTO factor_returns_daily
           (date, factor, spread_pp, q1_mean_pp, q5_mean_pp, n_symbols, universe_id)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    return len(rows)


def _print_summary(conn: sqlite3.Connection) -> None:
    print("\nRow counts by universe:")
    for uid, n in conn.execute(
        "SELECT universe_id, COUNT(*) FROM factor_returns_daily GROUP BY universe_id ORDER BY universe_id"
    ):
        print(f"  {uid:<26s} {n:>7,d}")

    print("\nPer-factor ann mean (universe='Technology' vs 'all'), 2015-2025:")
    print(f"  {'factor':<26s} {'tech ann':>10s} {'all ann':>10s} {'Δ':>8s}")
    for f in CANONICAL_FACTORS:
        def ann(uid):
            row = conn.execute(
                "SELECT AVG(spread_pp), COUNT(*) FROM factor_returns_daily "
                "WHERE factor=? AND universe_id=? AND date>='2015-01-01' AND date<='2025-01-01'",
                (f, uid)).fetchone()
            if row and row[1]:
                return float(row[0]) * 252  # arithmetic ann × trading days
            return None
        ta = ann("Technology"); aa = ann("all")
        if ta is None or aa is None:
            print(f"  {f:<26s} {'—':>10s} {'—':>10s}")
            continue
        print(f"  {f:<26s} {ta:>+10.2f} {aa:>+10.2f} {ta-aa:>+8.2f}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="YYYY-MM-DD — only rebuild from this date forward")
    ap.add_argument("--universe", help="Only rebuild one universe (e.g. 'Technology' or 'all'). "
                                       "Default: all of them.")
    args = ap.parse_args()

    db_path = _resolve_db_path()
    print(f"market db: {db_path}")
    conn = sqlite3.connect(db_path)

    _ensure_schema(conn)
    print("loading forward returns...")
    fwd = _load_forward_returns(conn, args.since)
    print(f"  {len(fwd):,} (date, symbol) fwd-ret cells")
    print("loading factor panel...")
    panel = _load_factor_panel(conn, args.since)
    print(f"  factor coverage: " +
          ", ".join(f"{f}={len(panel[f])}d" for f in CANONICAL_FACTORS[:3]) + ", ...")

    print("loading sector universes...")
    sector_syms = _load_sector_symbols(conn)
    for sec, syms in sector_syms.items():
        print(f"  {sec:<26s} {len(syms):>4d} symbols")

    # Decide which universes to compute
    if args.universe:
        if args.universe == "all":
            universes = [("all", None, MIN_SYMBOLS_PER_DATE)]
        elif args.universe in sector_syms:
            universes = [(args.universe, sector_syms[args.universe], MIN_SYMBOLS_PER_DATE_SECTOR)]
        else:
            sys.exit(f"unknown universe: {args.universe}. valid: 'all', {list(sector_syms)}")
    else:
        universes = [("all", None, MIN_SYMBOLS_PER_DATE)] + [
            (sec, syms, MIN_SYMBOLS_PER_DATE_SECTOR)
            for sec, syms in sector_syms.items() if len(syms) >= MIN_SYMBOLS_PER_DATE_SECTOR
        ]

    total_inserted = 0
    for universe_id, symbol_filter, min_syms in universes:
        print(f"\n=== {universe_id} (min_symbols={min_syms}) ===")
        rows = _compute_all_spreads(panel, fwd, universe_id, symbol_filter, min_syms)
        inserted = _persist(conn, rows)
        total_inserted += inserted

    print(f"\nwrote {total_inserted:,} total rows to factor_returns_daily")
    _print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
