#!/usr/bin/env python3
"""
AlphaScout Macro Data Module
==============================
Single module for all macro data: fetch from FRED, migrate legacy data,
compute derived series, backfill, and daily refresh.

All data stored in alphascout.db tables: macro_indicators, macro_derived.

Usage:
    python3 macro_data.py backfill              # Full backfill from 2015
    python3 macro_data.py daily                 # Daily refresh (last 30 days)
    python3 macro_data.py migrate               # Migrate old macro.db + fred/*.json
    python3 macro_data.py derive                # Recompute all derived series
    python3 macro_data.py status                # Show table stats
    python3 macro_data.py verify                # Verify data quality
"""

import json
import logging
import os
import sqlite3
import sys
import urllib.request
import urllib.error
import time
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("macro_data")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = Path(os.environ.get("DB_PATH", "/app/data/alphascout.db"))
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
LEGACY_MACRO_DB = DATA_DIR / "macro" / "macro.db"
LEGACY_FRED_DIR = DATA_DIR / "macro" / "fred"
FRED_KEY = os.environ.get("FRED_API_KEY", "88db2328e1ec523527f1b308b29b9de7")
BACKFILL_START = "2015-01-01"

# ---------------------------------------------------------------------------
# FRED Series Registry
# ---------------------------------------------------------------------------
FRED_SERIES = {
    # Commodities (Daily)
    "DCOILBRENTEU":     "brent",
    "DCOILWTICO":       "wti",
    "DHHNGSP":          "natgas",
    # Commodities (Weekly)
    "GASREGW":          "gas_regular",
    "GASDESW":          "gas_diesel",
    # Commodities (Monthly)
    "PCOPPUSDM":        "copper",
    "PALUMUSDM":        "aluminum",
    "PPIACO":           "ppi_commodities",
    # Volatility & Stress
    "VIXCLS":           "vix",
    "VXVCLS":           "vix_st_futures",
    "STLFSI4":          "financial_stress",
    "NFCI":             "nfci",
    "BAMLH0A0HYM2":    "hy_spread",
    "BAMLHE00EHYIEY":  "hy_yield",
    # Rates
    "DFF":              "fed_funds",
    "DGS2":             "treasury_2y",
    "DGS5":             "treasury_5y",
    "DGS10":            "treasury_10y",
    "DGS30":            "treasury_30y",
    "DFII10":           "tips_real_10y",
    "T10Y2Y":           "spread_10y2y",
    "T10Y3M":           "spread_10y3m",
    "T5YIE":            "breakeven_5y",
    "T10YIE":           "breakeven_10y",
    # Inflation
    "CPIAUCSL":         "cpi",
    "CPILFESL":         "core_cpi",
    "CPIENGSL":         "cpi_energy",
    "PCEPI":            "pce",
    "PCEPILFE":         "core_pce",
    # Activity & Sentiment
    "UMCSENT":          "consumer_sentiment",
    "INDPRO":           "industrial_prod",
    "RSAFS":            "retail_sales",
    "PAYEMS":           "nonfarm_payrolls",
    "JTSJOL":           "jolts_openings",
    "HOUST":            "housing_starts",
    "PERMIT":           "building_permits",
    "ICSA":             "initial_claims",
    "CCSA":             "continued_claims",
    "UNRATE":           "unemployment",
    # FX
    "DEXUSEU":          "eurusd",
    "DEXJPUS":          "jpyusd",
    "DEXUSUK":          "gbpusd",
    "DTWEXBGS":         "usd_broad",
    # Market
    "NASDAQCOM":        "nasdaq",
}

# Reverse mapping for lookups
SERIES_TO_FRED = {v: k for k, v in FRED_SERIES.items()}

# ---------------------------------------------------------------------------
# DB Helpers
# ---------------------------------------------------------------------------
def get_connection():
    return sqlite3.connect(str(DB_PATH))


def init_tables(conn):
    """Create macro tables if they don't exist."""
    conn.execute("""CREATE TABLE IF NOT EXISTS macro_indicators (
        date TEXT NOT NULL, series TEXT NOT NULL, value REAL, source TEXT,
        PRIMARY KEY (date, series))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_macro_ind_series ON macro_indicators(series)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_macro_ind_date ON macro_indicators(date)")
    conn.execute("""CREATE TABLE IF NOT EXISTS macro_derived (
        date TEXT NOT NULL, series TEXT NOT NULL, value REAL,
        PRIMARY KEY (date, series))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_macro_derived_series ON macro_derived(series)")
    conn.commit()


def upsert_indicators(conn, rows):
    """Insert or replace rows into macro_indicators. rows = [(date, series, value, source), ...]"""
    if not rows:
        return 0
    conn.executemany(
        "INSERT OR REPLACE INTO macro_indicators (date, series, value, source) VALUES (?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return len(rows)


def upsert_derived(conn, rows):
    """Insert or replace rows into macro_derived. rows = [(date, series, value), ...]"""
    if not rows:
        return 0
    conn.executemany(
        "INSERT OR REPLACE INTO macro_derived (date, series, value) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# FRED Fetcher
# ---------------------------------------------------------------------------
def fetch_fred(series_id, start=None, end=None):
    """
    Fetch observations from FRED API.
    Returns list of (date, value) tuples. Skips missing values ('.').
    """
    start = start or BACKFILL_START
    end = end or datetime.now().strftime("%Y-%m-%d")
    url = (
        f"https://api.stlouisfed.org/fred/series/observations?"
        f"series_id={series_id}&observation_start={start}&observation_end={end}"
        f"&file_type=json&api_key={FRED_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AlphaScout/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
        observations = data.get("observations", [])
        results = []
        for obs in observations:
            val = obs.get("value", ".")
            if val == "." or val is None:
                continue
            try:
                results.append((obs["date"], float(val)))
            except (ValueError, KeyError):
                continue
        return results
    except Exception as e:
        log.error(f"FRED fetch failed for {series_id}: {e}")
        return []


def fetch_all_fred(start=None, end=None, conn=None):
    """Fetch all FRED series and upsert into macro_indicators."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    total = 0
    errors = []
    for fred_id, series_key in FRED_SERIES.items():
        log.info(f"Fetching FRED {fred_id} → {series_key}")
        observations = fetch_fred(fred_id, start, end)
        if not observations:
            errors.append(fred_id)
            continue
        rows = [(date, series_key, value, "fred") for date, value in observations]
        n = upsert_indicators(conn, rows)
        total += n
        log.info(f"  {series_key}: {n} rows")
        time.sleep(0.15)  # FRED rate limit: ~120 req/min

    if own_conn:
        conn.close()

    log.info(f"FRED fetch complete: {total} total rows, {len(errors)} errors")
    if errors:
        log.warning(f"Failed series: {errors}")
    return total


# ---------------------------------------------------------------------------
# SPX Migration (from existing FMP index data)
# ---------------------------------------------------------------------------
def load_spx_from_index(conn, start=None):
    """Load SPX close from prices/indices/GSPC.json into macro_indicators."""
    gspc_path = DATA_DIR / "prices" / "indices" / "GSPC.json"
    if not gspc_path.exists():
        log.warning(f"GSPC.json not found at {gspc_path}")
        return 0

    with open(gspc_path) as f:
        raw = json.load(f)

    data = raw.get("data", raw.get("historical", raw))
    if isinstance(data, dict):
        data = data.get("historical", [])

    start = start or BACKFILL_START
    rows = []
    for r in data:
        date = r.get("date", "")
        close = r.get("close")
        if date >= start and close is not None:
            rows.append((date, "spx", float(close), "fmp_index"))

    n = upsert_indicators(conn, rows)
    log.info(f"SPX from GSPC.json: {n} rows")
    return n


# ---------------------------------------------------------------------------
# Legacy Migration
# ---------------------------------------------------------------------------
def migrate_legacy(conn):
    """Migrate data from old macro.db and fred/*.json into macro_indicators."""
    total = 0

    # 1. Migrate from macro.db
    if LEGACY_MACRO_DB.exists():
        log.info(f"Migrating from {LEGACY_MACRO_DB}")
        legacy = sqlite3.connect(str(LEGACY_MACRO_DB))

        # Oil prices (OHLCV → close only)
        try:
            cur = legacy.execute("SELECT date, ticker, close FROM oil_prices WHERE close IS NOT NULL")
            rows = []
            for date, ticker, close in cur.fetchall():
                series_key = ticker.lower()  # 'BRENT' → 'brent', 'WTI' → 'wti'
                if date >= BACKFILL_START:
                    rows.append((date, series_key, close, "yfinance_legacy"))
            n = upsert_indicators(conn, rows)
            total += n
            log.info(f"  oil_prices: {n} rows")
        except Exception as e:
            log.error(f"  oil_prices migration failed: {e}")

        # VIX (OHLCV → close only)
        try:
            cur = legacy.execute("SELECT date, close FROM vix WHERE close IS NOT NULL")
            rows = [(date, "vix", close, "yfinance_legacy")
                    for date, close in cur.fetchall() if date >= BACKFILL_START]
            n = upsert_indicators(conn, rows)
            total += n
            log.info(f"  vix: {n} rows")
        except Exception as e:
            log.error(f"  vix migration failed: {e}")

        # SPX (OHLCV → close only)
        try:
            cur = legacy.execute("SELECT date, close FROM spx WHERE close IS NOT NULL")
            rows = [(date, "spx", close, "yfinance_legacy")
                    for date, close in cur.fetchall() if date >= BACKFILL_START]
            n = upsert_indicators(conn, rows)
            total += n
            log.info(f"  spx: {n} rows")
        except Exception as e:
            log.error(f"  spx migration failed: {e}")

        # DXY (OHLCV → close only)
        try:
            cur = legacy.execute("SELECT date, close FROM dxy WHERE close IS NOT NULL")
            rows = [(date, "dxy", close, "yfinance_legacy")
                    for date, close in cur.fetchall() if date >= BACKFILL_START]
            n = upsert_indicators(conn, rows)
            total += n
            log.info(f"  dxy: {n} rows")
        except Exception as e:
            log.error(f"  dxy migration failed: {e}")

        # FRED series from macro.db
        try:
            fred_map = {
                "BAMLH0A0HYM2": "hy_spread",
                "T5YIFR": "breakeven_5y_fwd",  # 5Y forward, different from T5YIE
                "DGS2": "treasury_2y",
                "DGS10": "treasury_10y",
                "FEDFUNDS": "fed_funds",
            }
            cur = legacy.execute("SELECT date, series_id, value FROM fred_series WHERE value IS NOT NULL")
            rows = []
            for date, sid, value in cur.fetchall():
                key = fred_map.get(sid)
                if key and date >= BACKFILL_START:
                    rows.append((date, key, value, "fred_legacy"))
            n = upsert_indicators(conn, rows)
            total += n
            log.info(f"  fred_series: {n} rows")
        except Exception as e:
            log.error(f"  fred_series migration failed: {e}")

        legacy.close()
    else:
        log.info("No legacy macro.db found, skipping")

    # 2. Migrate from fred/*.json files
    if LEGACY_FRED_DIR.exists():
        log.info(f"Migrating from {LEGACY_FRED_DIR}")
        json_map = {
            "bbb_spread.json": "bbb_spread",
            "breakeven_inflation_10y.json": "breakeven_10y",
            "cpi.json": "cpi",
            "fed_funds_rate.json": "fed_funds",
            "gdp.json": "gdp",
            "hy_spread.json": "hy_spread",
            "initial_claims.json": "initial_claims",
            "treasury_10y.json": "treasury_10y",
            "treasury_2y.json": "treasury_2y",
            "unemployment_rate.json": "unemployment",
            "usd_index.json": "usd_broad",
            "vix.json": "vix",
            "yield_curve_10y2y.json": "spread_10y2y",
        }
        for filename, series_key in json_map.items():
            fpath = LEGACY_FRED_DIR / filename
            if not fpath.exists():
                continue
            try:
                with open(fpath) as f:
                    data = json.load(f)
                # Handle wrapper format: {series_id, name, ..., count, data: [...]}
                obs_list = data
                if isinstance(data, dict):
                    # Try common keys
                    for key in ("data", "observations"):
                        if key in data and isinstance(data[key], list):
                            obs_list = data[key]
                            break
                    else:
                        obs_list = []

                rows = []
                for obs in obs_list:
                    date = obs.get("date", "")
                    value = obs.get("value")
                    if date >= BACKFILL_START and value is not None:
                        try:
                            rows.append((date, series_key, float(value), "fred_legacy"))
                        except (ValueError, TypeError):
                            continue
                n = upsert_indicators(conn, rows)
                total += n
                log.info(f"  {filename} → {series_key}: {n} rows")
            except Exception as e:
                log.error(f"  {filename} failed: {e}")
    else:
        log.info("No legacy fred/ directory found, skipping")

    # 3. Migrate FMP macro JSONs (treasury-rates.json → individual series)
    treasury_path = DATA_DIR / "macro" / "treasury-rates.json"
    if treasury_path.exists():
        log.info(f"Migrating from {treasury_path}")
        try:
            with open(treasury_path) as f:
                raw = json.load(f)
            data = raw.get("data", raw)
            if isinstance(data, dict):
                data = []

            col_map = {
                "month1": "treasury_1m",
                "month2": "treasury_2m",
                "month3": "treasury_3m",
                "month6": "treasury_6m",
                "year1": "treasury_1y",
                "year2": "treasury_2y",
                "year3": "treasury_3y",
                "year5": "treasury_5y",
                "year7": "treasury_7y",
                "year10": "treasury_10y",
                "year20": "treasury_20y",
                "year30": "treasury_30y",
            }
            rows = []
            for r in data:
                date = r.get("date", "")
                if date < BACKFILL_START:
                    continue
                for col, series_key in col_map.items():
                    val = r.get(col)
                    if val is not None:
                        rows.append((date, series_key, float(val), "fmp_legacy"))
            n = upsert_indicators(conn, rows)
            total += n
            log.info(f"  treasury-rates.json: {n} rows")
        except Exception as e:
            log.error(f"  treasury-rates.json failed: {e}")

    log.info(f"Migration complete: {total} total rows")
    return total


# ---------------------------------------------------------------------------
# Derived Series Computation
# ---------------------------------------------------------------------------
def _get_series(conn, series, start=None):
    """Get a series as [(date, value)] sorted by date."""
    start = start or BACKFILL_START
    cur = conn.execute(
        "SELECT date, value FROM macro_indicators WHERE series = ? AND date >= ? ORDER BY date",
        (series, start),
    )
    return cur.fetchall()


def _compute_ma(conn, source_series, window, output_series, start=None):
    """Compute moving average and store in macro_derived."""
    data = _get_series(conn, source_series, start)
    if len(data) < window:
        log.warning(f"Not enough data for {output_series}: {len(data)} rows, need {window}")
        return 0

    rows = []
    values = [v for _, v in data]
    dates = [d for d, _ in data]
    for i in range(window - 1, len(values)):
        ma = sum(values[i - window + 1:i + 1]) / window
        rows.append((dates[i], output_series, ma))

    return upsert_derived(conn, rows)


def _compute_vs_ma_pct(conn, source_series, ma_series, output_series):
    """Compute (value - MA) / MA * 100 and store in macro_derived."""
    # Get raw values
    raw = dict(_get_series(conn, source_series))
    # Get MA values from derived
    cur = conn.execute(
        "SELECT date, value FROM macro_derived WHERE series = ? ORDER BY date",
        (ma_series,),
    )
    ma_data = cur.fetchall()

    rows = []
    for date, ma_val in ma_data:
        raw_val = raw.get(date)
        if raw_val is not None and ma_val is not None and ma_val != 0:
            pct = (raw_val - ma_val) / ma_val * 100
            rows.append((date, output_series, pct))

    return upsert_derived(conn, rows)


def _compute_yoy(conn, source_series, output_series):
    """Compute year-over-year % change for a monthly series."""
    data = _get_series(conn, source_series)
    if len(data) < 13:
        log.warning(f"Not enough data for YoY {output_series}: {len(data)} rows")
        return 0

    by_date = {d: v for d, v in data}
    dates = sorted(by_date.keys())

    rows = []
    for date in dates:
        # Find date ~12 months ago
        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            prev_dt = dt.replace(year=dt.year - 1)
            prev_date = prev_dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

        prev_val = by_date.get(prev_date)
        curr_val = by_date[date]
        if prev_val is not None and prev_val != 0:
            yoy = (curr_val - prev_val) / prev_val * 100
            rows.append((date, output_series, yoy))

    return upsert_derived(conn, rows)


def _compute_mom(conn, source_series, output_series):
    """Compute month-over-month % change for a monthly series."""
    data = _get_series(conn, source_series)
    if len(data) < 2:
        return 0

    rows = []
    for i in range(1, len(data)):
        prev_date, prev_val = data[i - 1]
        curr_date, curr_val = data[i]
        if prev_val is not None and prev_val != 0:
            mom = (curr_val - prev_val) / prev_val * 100
            rows.append((curr_date, output_series, mom))

    return upsert_derived(conn, rows)


def _compute_spread(conn, series_a, series_b, output_series):
    """Compute series_a - series_b and store in macro_derived."""
    a = dict(_get_series(conn, series_a))
    b = dict(_get_series(conn, series_b))

    rows = []
    for date in sorted(set(a.keys()) & set(b.keys())):
        if a[date] is not None and b[date] is not None:
            rows.append((date, output_series, a[date] - b[date]))

    return upsert_derived(conn, rows)


def _compute_ratio(conn, numerator, denominator, output_series):
    """Compute numerator / denominator and store in macro_derived."""
    a = dict(_get_series(conn, numerator))
    b = dict(_get_series(conn, denominator))

    rows = []
    for date in sorted(set(a.keys()) & set(b.keys())):
        if a[date] is not None and b[date] is not None and b[date] != 0:
            rows.append((date, output_series, a[date] / b[date]))

    return upsert_derived(conn, rows)


def _compute_zscore(conn, source_series, window, output_series):
    """Compute rolling z-score over `window` trading days."""
    data = _get_series(conn, source_series)
    if len(data) < window:
        log.warning(f"Not enough data for zscore {output_series}: {len(data)} rows, need {window}")
        return 0

    values = [v for _, v in data]
    dates = [d for d, _ in data]

    rows = []
    for i in range(window - 1, len(values)):
        window_vals = values[i - window + 1:i + 1]
        mean = sum(window_vals) / len(window_vals)
        variance = sum((v - mean) ** 2 for v in window_vals) / len(window_vals)
        std = variance ** 0.5
        if std > 0:
            z = (values[i] - mean) / std
            rows.append((dates[i], output_series, z))

    return upsert_derived(conn, rows)


def _compute_real_rate(conn, nominal_series, inflation_yoy_series, output_series):
    """Compute real rate = nominal - inflation YoY. Handles daily nominal vs monthly inflation."""
    nominal = dict(_get_series(conn, nominal_series))
    # Inflation YoY is in macro_derived
    cur = conn.execute(
        "SELECT date, value FROM macro_derived WHERE series = ? ORDER BY date",
        (inflation_yoy_series,),
    )
    inflation_data = cur.fetchall()

    # Build monthly inflation lookup (carry forward to daily)
    inflation_monthly = {}
    for date, val in inflation_data:
        # Extract YYYY-MM
        ym = date[:7]
        inflation_monthly[ym] = val

    rows = []
    for date in sorted(nominal.keys()):
        ym = date[:7]
        inf_val = inflation_monthly.get(ym)
        if inf_val is not None and nominal[date] is not None:
            rows.append((date, output_series, nominal[date] - inf_val))

    return upsert_derived(conn, rows)


def compute_all_derived(conn):
    """Compute all derived series."""
    log.info("Computing derived series...")
    total = 0

    # Moving averages
    for source, window, output in [
        ("brent", 50, "brent_50dma"),
        ("brent", 200, "brent_200dma"),
        ("spx", 50, "spx_50dma"),
        ("spx", 200, "spx_200dma"),
        ("natgas", 50, "natgas_50dma"),
    ]:
        n = _compute_ma(conn, source, window, output)
        total += n
        log.info(f"  {output}: {n} rows")

    # Percentage vs MA (depends on MAs above)
    for source, ma, output in [
        ("brent", "brent_50dma", "brent_vs_50dma_pct"),
        ("spx", "spx_200dma", "spx_vs_200dma_pct"),
    ]:
        n = _compute_vs_ma_pct(conn, source, ma, output)
        total += n
        log.info(f"  {output}: {n} rows")

    # VIX term spread
    n = _compute_spread(conn, "vix", "vix_st_futures", "vix_term_spread")
    total += n
    log.info(f"  vix_term_spread: {n} rows")

    # YoY rates
    for source, output in [
        ("cpi", "cpi_yoy"),
        ("core_cpi", "core_cpi_yoy"),
        ("core_pce", "core_pce_yoy"),
    ]:
        n = _compute_yoy(conn, source, output)
        total += n
        log.info(f"  {output}: {n} rows")

    # MoM CPI
    n = _compute_mom(conn, "cpi", "cpi_mom")
    total += n
    log.info(f"  cpi_mom: {n} rows")

    # Real fed funds (depends on core_cpi_yoy above)
    n = _compute_real_rate(conn, "fed_funds", "core_cpi_yoy", "real_fed_funds")
    total += n
    log.info(f"  real_fed_funds: {n} rows")

    # HY spread z-score (252 trading day window ≈ 1 year)
    n = _compute_zscore(conn, "hy_spread", 252, "hy_spread_zscore")
    total += n
    log.info(f"  hy_spread_zscore: {n} rows")

    log.info(f"Derived computation complete: {total} total rows")
    return total


# ---------------------------------------------------------------------------
# Status & Verification
# ---------------------------------------------------------------------------
def show_status(conn):
    """Show table statistics."""
    print("\n=== MACRO INDICATORS ===")
    cur = conn.execute(
        "SELECT series, COUNT(*), MIN(date), MAX(date) FROM macro_indicators "
        "GROUP BY series ORDER BY series"
    )
    print(f"{'Series':<25} {'Rows':>8} {'First':<12} {'Latest':<12}")
    print("-" * 60)
    total = 0
    for series, count, first, last in cur.fetchall():
        print(f"{series:<25} {count:>8} {first:<12} {last:<12}")
        total += count
    print(f"{'TOTAL':<25} {total:>8}")

    print("\n=== MACRO DERIVED ===")
    cur = conn.execute(
        "SELECT series, COUNT(*), MIN(date), MAX(date) FROM macro_derived "
        "GROUP BY series ORDER BY series"
    )
    print(f"{'Series':<25} {'Rows':>8} {'First':<12} {'Latest':<12}")
    print("-" * 60)
    total_d = 0
    for series, count, first, last in cur.fetchall():
        print(f"{series:<25} {count:>8} {first:<12} {last:<12}")
        total_d += count
    print(f"{'TOTAL':<25} {total_d:>8}")

    print(f"\nGrand total: {total + total_d} rows across both tables")


def verify_data(conn):
    """Run data quality checks."""
    issues = []

    # Check all expected FRED series exist
    expected = set(FRED_SERIES.values())
    expected.add("spx")  # From FMP index
    cur = conn.execute("SELECT DISTINCT series FROM macro_indicators")
    actual = {row[0] for row in cur.fetchall()}
    missing = expected - actual
    if missing:
        issues.append(f"Missing series in macro_indicators: {missing}")

    # Check for gaps in daily series (more than 5 business days between observations)
    daily_series = ["brent", "wti", "vix", "treasury_10y", "fed_funds"]
    for series in daily_series:
        cur = conn.execute(
            "SELECT date FROM macro_indicators WHERE series = ? AND date >= '2020-01-01' ORDER BY date",
            (series,),
        )
        dates = [row[0] for row in cur.fetchall()]
        for i in range(1, len(dates)):
            d1 = datetime.strptime(dates[i - 1], "%Y-%m-%d")
            d2 = datetime.strptime(dates[i], "%Y-%m-%d")
            gap = (d2 - d1).days
            if gap > 7:  # Allow weekends + holidays
                issues.append(f"Gap in {series}: {dates[i-1]} → {dates[i]} ({gap} days)")

    # Check derived series exist
    expected_derived = [
        "brent_50dma", "brent_200dma", "brent_vs_50dma_pct",
        "spx_50dma", "spx_200dma", "spx_vs_200dma_pct",
        "vix_term_spread", "cpi_yoy", "core_cpi_yoy", "cpi_mom",
        "core_pce_yoy", "real_fed_funds", "hy_spread_zscore", "natgas_50dma",
    ]
    cur = conn.execute("SELECT DISTINCT series FROM macro_derived")
    actual_derived = {row[0] for row in cur.fetchall()}
    missing_derived = set(expected_derived) - actual_derived
    if missing_derived:
        issues.append(f"Missing derived series: {missing_derived}")

    # Spot check: Brent should be between $10 and $200
    cur = conn.execute(
        "SELECT date, value FROM macro_indicators WHERE series = 'brent' "
        "AND (value < 10 OR value > 200) AND date >= '2015-01-01'"
    )
    outliers = cur.fetchall()
    if outliers:
        issues.append(f"Brent outliers: {outliers[:5]}")

    # Spot check: VIX should be between 5 and 90
    cur = conn.execute(
        "SELECT date, value FROM macro_indicators WHERE series = 'vix' "
        "AND (value < 5 OR value > 90) AND date >= '2015-01-01'"
    )
    outliers = cur.fetchall()
    if outliers:
        issues.append(f"VIX outliers: {outliers[:5]}")

    if issues:
        print(f"\n⚠️  {len(issues)} ISSUES FOUND:")
        for i in issues:
            print(f"  - {i}")
    else:
        print("\n✅ ALL CHECKS PASSED")

    return issues


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------
def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()
    conn = get_connection()
    init_tables(conn)

    if command == "backfill":
        log.info(f"Starting full backfill from {BACKFILL_START}")
        n = fetch_all_fred(start=BACKFILL_START, conn=conn)
        n += load_spx_from_index(conn, start=BACKFILL_START)
        log.info(f"Backfill complete: {n} rows")
        log.info("Computing derived series...")
        compute_all_derived(conn)
        show_status(conn)

    elif command == "daily":
        start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        log.info(f"Daily refresh from {start}")
        n = fetch_all_fred(start=start, conn=conn)
        n += load_spx_from_index(conn, start=start)
        log.info(f"Daily refresh: {n} rows")
        log.info("Updating derived series...")
        compute_all_derived(conn)

    elif command == "migrate":
        log.info("Migrating legacy data...")
        migrate_legacy(conn)
        load_spx_from_index(conn)
        show_status(conn)

    elif command == "derive":
        log.info("Recomputing all derived series...")
        compute_all_derived(conn)
        show_status(conn)

    elif command == "status":
        show_status(conn)

    elif command == "verify":
        show_status(conn)
        verify_data(conn)

    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()
