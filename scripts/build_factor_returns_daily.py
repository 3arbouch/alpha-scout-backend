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

MIN_SYMBOLS_PER_DATE = 50
N_BUCKETS = 5


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
                              date: str, direction: str
                              ) -> tuple[float, float, float, int] | None:
    """Return (spread_pp, q1_mean, q5_mean, n_symbols) or None if insufficient."""
    pairs: list[tuple[float, float]] = []
    for sym, v in values_by_symbol.items():
        ret = fwd_by_symbol.get((date, sym))
        if ret is None:
            continue
        pairs.append((v, ret))
    n = len(pairs)
    if n < MIN_SYMBOLS_PER_DATE:
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


def _compute_all_spreads(panel, fwd):
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
            result = _compute_spread_for_date(values_by_symbol, fwd, date, direction)
            if result is None:
                continue
            spread, q1_mean, q5_mean, n_symbols = result
            out_rows.append((date, factor, spread, q1_mean, q5_mean, n_symbols))
            kept += 1
        print(f"  {factor:30s} {kept:>5d} / {n_dates:>5d} dates")
    print(f"  computed {len(out_rows):,} rows in {time.time() - t0:.1f}s")
    return out_rows


def _persist(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn.executemany(
        """INSERT OR REPLACE INTO factor_returns_daily
           (date, factor, spread_pp, q1_mean_pp, q5_mean_pp, n_symbols, universe_id)
           VALUES (?, ?, ?, ?, ?, ?, 'all')""",
        rows,
    )
    conn.commit()
    return len(rows)


def _print_summary(conn: sqlite3.Connection) -> None:
    print("\nPer-factor summary (full history in DB):")
    print(f"  {'factor':<28s} {'n':>6s} {'first':>11s} {'last':>11s} "
          f"{'mean_pp':>9s} {'std_pp':>9s}")
    rows = conn.execute(
        """SELECT factor, COUNT(*) n, MIN(date) d0, MAX(date) d1,
                  AVG(spread_pp) mean_pp
           FROM factor_returns_daily GROUP BY factor ORDER BY factor"""
    ).fetchall()
    # std is computed via Python (SQLite has no STDEV)
    for factor, n, d0, d1, mean_pp in rows:
        spreads = [r[0] for r in conn.execute(
            "SELECT spread_pp FROM factor_returns_daily WHERE factor=?", (factor,))]
        std_pp = float(np.std(spreads, ddof=1)) if len(spreads) > 1 else float("nan")
        print(f"  {factor:<28s} {n:>6d} {d0:>11s} {d1:>11s} "
              f"{mean_pp:>9.4f} {std_pp:>9.4f}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="YYYY-MM-DD — only rebuild from this date forward")
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

    rows = _compute_all_spreads(panel, fwd)
    inserted = _persist(conn, rows)
    print(f"\nwrote {inserted:,} rows to factor_returns_daily")

    _print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
