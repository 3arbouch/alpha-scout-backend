#!/usr/bin/env python3
"""
AlphaScout Database Builder
============================
Reads JSON data files and builds SQLite database.

Tables: prices, income, balance, cashflow, earnings, insider_trades, analyst_grades

Usage:
    python3 build_db.py                # Build all tables
    python3 build_db.py --table prices # Build one table
    python3 build_db.py --ticker NKE   # Single ticker
    python3 build_db.py --ticker NKE,AAPL  # Multiple tickers
    python3 build_db.py --status       # Show DB stats
"""

import os
import sys
import json
import sqlite3
import argparse
import logging
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
from db_config import MARKET_DB_PATH as DB_PATH
ALL_TABLES = ["prices", "income", "balance", "cashflow", "earnings", "insider_trades", "analyst_grades"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_db")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_json(filepath: Path):
    """Load a JSON file and return the 'data' field."""
    if not filepath.exists():
        return None
    try:
        content = json.loads(filepath.read_text())
        return content.get("data")
    except (json.JSONDecodeError, KeyError):
        return None


def get_tickers(ticker_filter: str = None) -> list[str]:
    """Get list of tickers from price files on disk."""
    if ticker_filter:
        return [t.strip().upper() for t in ticker_filter.split(",")]
    price_dir = DATA_DIR / "prices" / "daily"
    if not price_dir.exists():
        log.error(f"Price directory not found: {price_dir}")
        return []
    return sorted([f.stem for f in price_dir.glob("*.json")])


def build_table(conn, tickers, table_name, json_dir, row_mapper, insert_sql, is_list=True):
    """
    Generic table builder. Reads JSON files, maps rows, inserts into SQLite.

    Args:
        conn: SQLite connection
        tickers: List of ticker symbols
        table_name: Name of the table
        json_dir: Path to directory of JSON files (relative to DATA_DIR)
        row_mapper: Function that takes (ticker, data) and returns list of tuples
        insert_sql: INSERT SQL statement
        is_list: Whether data is a list of records (True) or a dict (False)
    """
    log.info(f"Building {table_name} table for {len(tickers)} tickers...")
    cur = conn.cursor()
    total_rows = 0
    skipped = 0
    t0 = time.time()

    for i, ticker in enumerate(tickers):
        filepath = DATA_DIR / json_dir / f"{ticker}.json"
        data = load_json(filepath)

        if data is None:
            skipped += 1
            continue

        # Handle both list and dict data formats
        if is_list and not isinstance(data, list):
            skipped += 1
            continue

        rows = row_mapper(ticker, data)
        if not rows:
            skipped += 1
            continue

        cur.execute(f"DELETE FROM {table_name} WHERE symbol = ?", (ticker,))
        cur.executemany(insert_sql, rows)
        total_rows += len(rows)

        if (i + 1) % 100 == 0 or (i + 1) == len(tickers):
            conn.commit()
            elapsed = time.time() - t0
            log.info(f"  {table_name}: {i+1}/{len(tickers)} tickers, {total_rows:,} rows ({elapsed:.1f}s)")

    conn.commit()
    log.info(f"  {table_name} done: {total_rows:,} rows from {len(tickers) - skipped} tickers ({skipped} skipped)")
    return total_rows


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SCHEMAS = {
    "prices": """
    CREATE TABLE IF NOT EXISTS prices (
        symbol      TEXT NOT NULL,
        date        TEXT NOT NULL,
        open        REAL,
        high        REAL,
        low         REAL,
        close       REAL,
        volume      INTEGER,
        change_pct  REAL,
        vwap        REAL,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);
    CREATE INDEX IF NOT EXISTS idx_prices_symbol ON prices(symbol);
    """,

    "income": """
    CREATE TABLE IF NOT EXISTS income (
        symbol          TEXT NOT NULL,
        date            TEXT NOT NULL,
        fiscal_year     TEXT,
        period          TEXT,
        revenue         REAL,
        gross_profit    REAL,
        operating_income REAL,
        net_income      REAL,
        ebitda          REAL,
        eps             REAL,
        eps_diluted     REAL,
        shares_diluted  REAL,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_income_symbol ON income(symbol);
    """,

    "balance": """
    CREATE TABLE IF NOT EXISTS balance (
        symbol                  TEXT NOT NULL,
        date                    TEXT NOT NULL,
        fiscal_year             TEXT,
        period                  TEXT,
        cash                    REAL,
        inventory               REAL,
        total_current_assets    REAL,
        total_assets            REAL,
        total_current_liabilities REAL,
        long_term_debt          REAL,
        total_debt              REAL,
        total_liabilities       REAL,
        total_equity            REAL,
        net_debt                REAL,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_balance_symbol ON balance(symbol);
    """,

    "cashflow": """
    CREATE TABLE IF NOT EXISTS cashflow (
        symbol          TEXT NOT NULL,
        date            TEXT NOT NULL,
        fiscal_year     TEXT,
        period          TEXT,
        operating_cf    REAL,
        capex           REAL,
        free_cash_flow  REAL,
        dividends_paid  REAL,
        stock_repurchased REAL,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_cashflow_symbol ON cashflow(symbol);
    """,

    "earnings": """
    CREATE TABLE IF NOT EXISTS earnings (
        symbol          TEXT NOT NULL,
        date            TEXT NOT NULL,
        eps_actual      REAL,
        eps_estimated   REAL,
        revenue_actual  REAL,
        revenue_estimated REAL,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_earnings_symbol ON earnings(symbol);
    """,

    "insider_trades": """
    CREATE TABLE IF NOT EXISTS insider_trades (
        symbol              TEXT NOT NULL,
        transaction_date    TEXT NOT NULL,
        reporting_name      TEXT,
        type_of_owner       TEXT,
        transaction_type    TEXT,
        shares              REAL,
        price               REAL,
        value               REAL,
        securities_owned    REAL,
        PRIMARY KEY (symbol, transaction_date, reporting_name, transaction_type)
    );
    CREATE INDEX IF NOT EXISTS idx_insider_symbol ON insider_trades(symbol);
    CREATE INDEX IF NOT EXISTS idx_insider_date ON insider_trades(transaction_date);
    CREATE INDEX IF NOT EXISTS idx_insider_type ON insider_trades(transaction_type);
    """,

    "analyst_grades": """
    CREATE TABLE IF NOT EXISTS analyst_grades (
        symbol          TEXT NOT NULL,
        date            TEXT NOT NULL,
        grading_company TEXT,
        previous_grade  TEXT,
        new_grade       TEXT,
        action          TEXT,
        PRIMARY KEY (symbol, date, grading_company)
    );
    CREATE INDEX IF NOT EXISTS idx_grades_symbol ON analyst_grades(symbol);
    CREATE INDEX IF NOT EXISTS idx_grades_date ON analyst_grades(date);
    CREATE INDEX IF NOT EXISTS idx_grades_action ON analyst_grades(action);
    """,
}


# ---------------------------------------------------------------------------
# Row mappers — extract fields from JSON into tuples for INSERT
# ---------------------------------------------------------------------------
def map_prices(ticker, data):
    return [(
        ticker, r.get("date"), r.get("open"), r.get("high"), r.get("low"),
        r.get("close"), r.get("volume"), r.get("changePercent"), r.get("vwap"),
    ) for r in data]


def map_income(ticker, data):
    return [(
        ticker, r.get("date"), r.get("fiscalYear"), r.get("period"),
        r.get("revenue"), r.get("grossProfit"), r.get("operatingIncome"),
        r.get("netIncome"), r.get("ebitda"), r.get("eps"), r.get("epsDiluted"),
        r.get("weightedAverageShsOutDil"),
    ) for r in data]


def map_balance(ticker, data):
    rows = []
    for r in data:
        cash = r.get("cashAndCashEquivalents", 0) or 0
        total_debt = (r.get("shortTermDebt", 0) or 0) + (r.get("longTermDebt", 0) or 0)
        net_debt = total_debt - cash
        rows.append((
            ticker, r.get("date"), r.get("fiscalYear"), r.get("period"),
            cash, r.get("inventory"), r.get("totalCurrentAssets"), r.get("totalAssets"),
            r.get("totalCurrentLiabilities"), r.get("longTermDebt"), total_debt,
            r.get("totalLiabilities"), r.get("totalStockholdersEquity"), net_debt,
        ))
    return rows


def map_cashflow(ticker, data):
    rows = []
    for r in data:
        operating_cf = r.get("netCashProvidedByOperatingActivities")
        capex = r.get("investmentsInPropertyPlantAndEquipment")
        # FCF = operating CF + capex (capex is negative)
        fcf = None
        if operating_cf is not None and capex is not None:
            fcf = operating_cf + capex
        rows.append((
            ticker, r.get("date"), r.get("fiscalYear"), r.get("period"),
            operating_cf, capex, fcf, r.get("commonDividendsPaid"),
            r.get("commonStockRepurchased"),
        ))
    return rows


def map_earnings(ticker, data):
    # Earnings calendar data is a list of records
    return [(
        ticker, r.get("date"), r.get("epsActual"), r.get("epsEstimated"),
        r.get("revenueActual"), r.get("revenueEstimated"),
    ) for r in data]


def map_insider_trades(ticker, data):
    rows = []
    for r in data:
        shares = r.get("securitiesTransacted", 0) or 0
        price = r.get("price", 0) or 0
        value = shares * price
        rows.append((
            ticker, r.get("transactionDate"), r.get("reportingName"),
            r.get("typeOfOwner"), r.get("transactionType"), shares, price,
            value, r.get("securitiesOwned"),
        ))
    return rows


def map_analyst_grades(ticker, data):
    return [(
        ticker, r.get("date"), r.get("gradingCompany"),
        r.get("previousGrade"), r.get("newGrade"), r.get("action"),
    ) for r in data]


# ---------------------------------------------------------------------------
# Table configs — maps table name to build parameters
# ---------------------------------------------------------------------------
TABLE_CONFIGS = {
    "prices": {
        "json_dir": "prices/daily",
        "mapper": map_prices,
        "insert": "INSERT OR REPLACE INTO prices (symbol,date,open,high,low,close,volume,change_pct,vwap) VALUES (?,?,?,?,?,?,?,?,?)",
    },
    "income": {
        "json_dir": "fundamentals/income",
        "mapper": map_income,
        "insert": "INSERT OR REPLACE INTO income (symbol,date,fiscal_year,period,revenue,gross_profit,operating_income,net_income,ebitda,eps,eps_diluted,shares_diluted) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
    },
    "balance": {
        "json_dir": "fundamentals/balance",
        "mapper": map_balance,
        "insert": "INSERT OR REPLACE INTO balance (symbol,date,fiscal_year,period,cash,inventory,total_current_assets,total_assets,total_current_liabilities,long_term_debt,total_debt,total_liabilities,total_equity,net_debt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    },
    "cashflow": {
        "json_dir": "fundamentals/cashflow",
        "mapper": map_cashflow,
        "insert": "INSERT OR REPLACE INTO cashflow (symbol,date,fiscal_year,period,operating_cf,capex,free_cash_flow,dividends_paid,stock_repurchased) VALUES (?,?,?,?,?,?,?,?,?)",
    },
    "earnings": {
        "json_dir": "earnings/calendar",
        "mapper": map_earnings,
        "insert": "INSERT OR REPLACE INTO earnings (symbol,date,eps_actual,eps_estimated,revenue_actual,revenue_estimated) VALUES (?,?,?,?,?,?)",
    },
    "insider_trades": {
        "json_dir": "catalysts/insider-trades",
        "mapper": map_insider_trades,
        "insert": "INSERT OR REPLACE INTO insider_trades (symbol,transaction_date,reporting_name,type_of_owner,transaction_type,shares,price,value,securities_owned) VALUES (?,?,?,?,?,?,?,?,?)",
    },
    "analyst_grades": {
        "json_dir": "analyst/grades",
        "mapper": map_analyst_grades,
        "insert": "INSERT OR REPLACE INTO analyst_grades (symbol,date,grading_company,previous_grade,new_grade,action) VALUES (?,?,?,?,?,?)",
    },
}


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
def show_status(conn: sqlite3.Connection):
    """Show database stats."""
    cur = conn.cursor()
    print(f"  === AlphaScout Database ===")
    print(f"  Path: {DB_PATH}")

    if DB_PATH.exists():
        print(f"  Size: {DB_PATH.stat().st_size / 1024 / 1024:.1f} MB")
    else:
        print("  Not created yet.")
        return

    print()
    for table in ALL_TABLES:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(DISTINCT symbol) FROM {table}")
            symbols = cur.fetchone()[0]

            # Get date range
            date_col = "transaction_date" if table == "insider_trades" else "date"
            cur.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}")
            min_date, max_date = cur.fetchone()

            print(f"  {table:<20} {count:>10,} rows  {symbols:>4} tickers  ({min_date} to {max_date})")
        except sqlite3.OperationalError:
            print(f"  {table:<20} not created yet")

    # DB size breakdown
    print(f"\n  Total size: {DB_PATH.stat().st_size / 1024 / 1024:.1f} MB")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="AlphaScout Database Builder")
    parser.add_argument("--table", type=str, choices=ALL_TABLES, help="Build specific table only")
    parser.add_argument("--ticker", type=str, help="Single ticker or comma-separated list")
    parser.add_argument("--status", action="store_true", help="Show database stats")
    args = parser.parse_args()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache

    if args.status:
        show_status(conn)
        conn.close()
        return

    tickers = get_tickers(args.ticker)
    if not tickers:
        log.error("No tickers found")
        sys.exit(1)

    tables_to_build = [args.table] if args.table else ALL_TABLES

    # Create schemas
    for table in tables_to_build:
        conn.executescript(SCHEMAS[table])

    t0 = time.time()
    total = 0
    for table in tables_to_build:
        cfg = TABLE_CONFIGS[table]
        rows = build_table(
            conn, tickers, table,
            json_dir=cfg["json_dir"],
            row_mapper=cfg["mapper"],
            insert_sql=cfg["insert"],
        )
        total += rows

    elapsed = time.time() - t0
    log.info(f"Build complete: {total:,} total rows in {elapsed:.1f}s")
    log.info(f"DB: {DB_PATH} ({DB_PATH.stat().st_size / 1024 / 1024:.1f} MB)")
    conn.close()


if __name__ == "__main__":
    main()
