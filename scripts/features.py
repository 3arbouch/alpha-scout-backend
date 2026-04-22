"""
features_daily — point-in-time derived valuation & growth metrics per (symbol, date).

One row per (symbol, trading day). Price-dependent ratios reflect that day's
close; TTM numerators/denominators come from the most recent filed quarter as of
the same day (bisect on income.date). This is the single source of truth the
agent queries during research AND the backtest engine reads at evaluation time.

Columns (wide schema):
  Valuation (price-dependent):
    pe          market_cap / TTM net_income
    ps          market_cap / TTM revenue
    p_b         market_cap / total_equity (latest balance as-of)
    ev_ebitda   (market_cap + net_debt) / TTM ebitda
    ev_sales    (market_cap + net_debt) / TTM revenue
    fcf_yield   TTM free_cash_flow / market_cap         (percent)
    div_yield   TTM |dividends_paid| / market_cap        (percent)
  Growth (quarterly rollup):
    eps_yoy     (latest Q eps_diluted - same-Q prior year) / |prior| * 100
    rev_yoy     (latest Q revenue - same-Q prior year) / prior * 100

CLI:
    python -m scripts.features --backfill                  # all symbols, all history
    python -m scripts.features --backfill --ticker AAPL    # single symbol
    python -m scripts.features --update                    # append today's rows
    python -m scripts.features --status                    # show coverage stats
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from bisect import bisect_right
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from db_config import MARKET_DB_PATH  # noqa: E402


FEATURE_COLUMNS = (
    "pe", "ps", "p_b", "ev_ebitda", "ev_sales",
    "fcf_yield", "div_yield", "eps_yoy", "rev_yoy",
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS features_daily (
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    pe          REAL,
    ps          REAL,
    p_b         REAL,
    ev_ebitda   REAL,
    ev_sales    REAL,
    fcf_yield   REAL,
    div_yield   REAL,
    eps_yoy     REAL,
    rev_yoy     REAL,
    PRIMARY KEY (symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_features_date ON features_daily(date);
CREATE INDEX IF NOT EXISTS idx_features_symbol ON features_daily(symbol);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


# ---------------------------------------------------------------------------
# TTM helpers — the single point-in-time chokepoint
# ---------------------------------------------------------------------------
def compute_ttm(quarters: list[tuple], col_idx: int) -> float | None:
    """Sum the `col_idx` field of the 4 most recent quarters.

    `quarters` is a list of tuples already filtered to rows with date <= as_of
    and sorted ascending by date. Returns None if fewer than 4 quarters or any
    of the last 4 has a NULL value.
    """
    if len(quarters) < 4:
        return None
    total = 0.0
    for q in quarters[-4:]:
        v = q[col_idx]
        if v is None:
            return None
        total += v
    return total


def yoy_pct(latest_q: tuple, year_ago_q: tuple | None, col_idx: int) -> float | None:
    """YoY percent change between the two quarter rows for column col_idx."""
    if year_ago_q is None:
        return None
    curr = latest_q[col_idx]
    prev = year_ago_q[col_idx]
    if curr is None or prev is None or prev == 0:
        return None
    return (curr - prev) / abs(prev) * 100.0


# ---------------------------------------------------------------------------
# Per-symbol data load
# ---------------------------------------------------------------------------
def _load_symbol_bundles(conn: sqlite3.Connection, symbol: str):
    """Return sorted-ascending lists of (date, ...) tuples for each fundamentals table."""
    cur = conn.cursor()
    # income: date, revenue, net_income, ebitda, eps_diluted, shares_diluted
    income = cur.execute(
        "SELECT date, revenue, net_income, ebitda, eps_diluted, shares_diluted "
        "FROM income WHERE symbol=? ORDER BY date ASC", (symbol,)
    ).fetchall()
    # balance: date, total_equity, net_debt
    balance = cur.execute(
        "SELECT date, total_equity, net_debt "
        "FROM balance WHERE symbol=? ORDER BY date ASC", (symbol,)
    ).fetchall()
    # cashflow: date, free_cash_flow, dividends_paid
    cashflow = cur.execute(
        "SELECT date, free_cash_flow, dividends_paid "
        "FROM cashflow WHERE symbol=? ORDER BY date ASC", (symbol,)
    ).fetchall()
    # prices: date, close
    prices = cur.execute(
        "SELECT date, close FROM prices WHERE symbol=? AND close IS NOT NULL "
        "ORDER BY date ASC", (symbol,)
    ).fetchall()
    return income, balance, cashflow, prices


# Column indices inside the income tuple (date is idx 0)
I_REV, I_NI, I_EBITDA, I_EPS_D, I_SHARES = 1, 2, 3, 4, 5
# Balance
B_EQUITY, B_NET_DEBT = 1, 2
# Cashflow
C_FCF, C_DIV = 1, 2


def _as_of(rows: list[tuple], target_date: str) -> tuple | None:
    """Last row with rows[i][0] <= target_date, or None."""
    if not rows:
        return None
    dates = [r[0] for r in rows]
    idx = bisect_right(dates, target_date) - 1
    if idx < 0:
        return None
    return rows[idx]


def _as_of_slice(rows: list[tuple], target_date: str) -> list[tuple]:
    """All rows with date <= target_date (sorted ascending)."""
    if not rows:
        return []
    dates = [r[0] for r in rows]
    idx = bisect_right(dates, target_date)
    return rows[:idx]


def _same_quarter_prior_year(income_rows: list[tuple], latest_idx: int) -> tuple | None:
    """Find the income row four entries earlier (same fiscal quarter, prior year)."""
    prior_idx = latest_idx - 4
    if prior_idx < 0:
        return None
    return income_rows[prior_idx]


# ---------------------------------------------------------------------------
# Per-day feature computation
# ---------------------------------------------------------------------------
def compute_features_for_day(
    trading_date: str,
    close: float,
    income: list[tuple],
    balance: list[tuple],
    cashflow: list[tuple],
) -> dict[str, float | None] | None:
    """Return the 9 features for one (symbol, trading_date). None if no fundamentals as-of."""
    income_slice = _as_of_slice(income, trading_date)
    if not income_slice:
        return None

    latest_q = income_slice[-1]
    shares = latest_q[I_SHARES]
    if not shares or shares <= 0:
        return None

    market_cap = close * shares

    # TTM aggregates from income
    ttm_rev = compute_ttm(income_slice, I_REV)
    ttm_ni = compute_ttm(income_slice, I_NI)
    ttm_ebitda = compute_ttm(income_slice, I_EBITDA)

    # Point-in-time balance
    bal = _as_of(balance, trading_date)
    total_equity = bal[B_EQUITY] if bal else None
    net_debt = bal[B_NET_DEBT] if bal else None

    # TTM cashflow
    cashflow_slice = _as_of_slice(cashflow, trading_date)
    ttm_fcf = compute_ttm(cashflow_slice, C_FCF) if cashflow_slice else None
    ttm_div = compute_ttm(cashflow_slice, C_DIV) if cashflow_slice else None

    # YoY growth: compare latest Q in income_slice to 4 entries earlier
    prior_q = _same_quarter_prior_year(income_slice, len(income_slice) - 1)
    eps_yoy = yoy_pct(latest_q, prior_q, I_EPS_D)
    rev_yoy = yoy_pct(latest_q, prior_q, I_REV)

    # Ratios
    pe = market_cap / ttm_ni if ttm_ni and ttm_ni > 0 else None
    ps = market_cap / ttm_rev if ttm_rev and ttm_rev > 0 else None
    p_b = market_cap / total_equity if total_equity and total_equity > 0 else None

    ev = None
    if net_debt is not None:
        ev = market_cap + net_debt
    ev_ebitda = ev / ttm_ebitda if ev is not None and ttm_ebitda and ttm_ebitda > 0 else None
    ev_sales = ev / ttm_rev if ev is not None and ttm_rev and ttm_rev > 0 else None

    fcf_yield = (ttm_fcf / market_cap * 100.0) if ttm_fcf is not None and market_cap > 0 else None
    # dividends_paid is stored as a negative number in cashflow; yield is reported positive
    div_yield = (abs(ttm_div) / market_cap * 100.0) if ttm_div is not None and market_cap > 0 else None

    return {
        "pe": pe,
        "ps": ps,
        "p_b": p_b,
        "ev_ebitda": ev_ebitda,
        "ev_sales": ev_sales,
        "fcf_yield": fcf_yield,
        "div_yield": div_yield,
        "eps_yoy": eps_yoy,
        "rev_yoy": rev_yoy,
    }


# ---------------------------------------------------------------------------
# Build per symbol
# ---------------------------------------------------------------------------
UPSERT_SQL = (
    "INSERT OR REPLACE INTO features_daily "
    "(symbol,date,pe,ps,p_b,ev_ebitda,ev_sales,fcf_yield,div_yield,eps_yoy,rev_yoy) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?)"
)


def build_symbol(conn: sqlite3.Connection, symbol: str, start_date: str | None = None) -> int:
    """Compute and upsert features for every trading day we have prices for. Returns row count."""
    income, balance, cashflow, prices = _load_symbol_bundles(conn, symbol)
    if not income or not prices:
        return 0

    rows = []
    for date, close in prices:
        if start_date and date < start_date:
            continue
        if close is None or close <= 0:
            continue
        feats = compute_features_for_day(date, close, income, balance, cashflow)
        if feats is None:
            continue
        rows.append((
            symbol, date,
            feats["pe"], feats["ps"], feats["p_b"],
            feats["ev_ebitda"], feats["ev_sales"],
            feats["fcf_yield"], feats["div_yield"],
            feats["eps_yoy"], feats["rev_yoy"],
        ))

    if rows:
        cur = conn.cursor()
        cur.executemany(UPSERT_SQL, rows)
        conn.commit()
    return len(rows)


def list_symbols(conn: sqlite3.Connection) -> list[str]:
    """Every symbol with income rows — anything else lacks fundamentals."""
    cur = conn.cursor()
    return [r[0] for r in cur.execute(
        "SELECT DISTINCT symbol FROM income ORDER BY symbol"
    ).fetchall()]


def backfill_all(conn: sqlite3.Connection, start_date: str | None = None,
                 ticker_filter: list[str] | None = None) -> tuple[int, int]:
    """Rebuild features for all (or filtered) symbols. Returns (symbols_processed, rows_written)."""
    ensure_schema(conn)
    symbols = ticker_filter or list_symbols(conn)
    total_rows = 0
    for i, sym in enumerate(symbols, 1):
        n = build_symbol(conn, sym, start_date=start_date)
        total_rows += n
        if i % 25 == 0 or i == len(symbols):
            print(f"  [{i}/{len(symbols)}] {sym}: {n} rows  (total {total_rows:,})")
    return len(symbols), total_rows


def update_recent(conn: sqlite3.Connection, lookback_days: int = 7) -> tuple[int, int]:
    """Append rows for recent trading days. Safe to run repeatedly (INSERT OR REPLACE)."""
    ensure_schema(conn)
    cur = conn.cursor()
    cutoff = cur.execute("SELECT MAX(date) FROM prices").fetchone()[0]
    if not cutoff:
        return 0, 0
    from datetime import timedelta
    start = (datetime.strptime(cutoff, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    symbols = list_symbols(conn)
    total_rows = 0
    for sym in symbols:
        total_rows += build_symbol(conn, sym, start_date=start)
    return len(symbols), total_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def show_status(conn: sqlite3.Connection) -> None:
    ensure_schema(conn)
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM features_daily").fetchone()[0]
    syms = cur.execute("SELECT COUNT(DISTINCT symbol) FROM features_daily").fetchone()[0]
    min_d, max_d = cur.execute("SELECT MIN(date), MAX(date) FROM features_daily").fetchone()
    print(f"  features_daily: {total:,} rows  {syms} tickers  ({min_d} to {max_d})")
    for col in FEATURE_COLUMNS:
        n = cur.execute(f"SELECT COUNT(*) FROM features_daily WHERE {col} IS NOT NULL").fetchone()[0]
        print(f"    {col:12s} non-null: {n:,}")


def main():
    ap = argparse.ArgumentParser(description="Derived features builder")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--backfill", action="store_true", help="Rebuild all history")
    g.add_argument("--update", action="store_true", help="Append recent rows")
    g.add_argument("--status", action="store_true", help="Show coverage stats")
    ap.add_argument("--start", type=str, default=None, help="YYYY-MM-DD floor for backfill")
    ap.add_argument("--ticker", type=str, default=None,
                    help="Single ticker or comma-separated list (backfill only)")
    args = ap.parse_args()

    conn = sqlite3.connect(str(MARKET_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    if args.status:
        show_status(conn)
    elif args.backfill:
        tickers = [t.strip().upper() for t in args.ticker.split(",")] if args.ticker else None
        t0 = time.time()
        n_syms, n_rows = backfill_all(conn, start_date=args.start, ticker_filter=tickers)
        print(f"Backfill complete: {n_syms} symbols, {n_rows:,} rows in {time.time()-t0:.1f}s")
        show_status(conn)
    elif args.update:
        t0 = time.time()
        n_syms, n_rows = update_recent(conn)
        print(f"Update complete: {n_syms} symbols, {n_rows:,} rows touched in {time.time()-t0:.1f}s")

    conn.close()


if __name__ == "__main__":
    main()
