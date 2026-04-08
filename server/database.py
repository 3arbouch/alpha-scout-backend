"""
Database connections — two databases, cleanly separated.

  market.db  — prices, fundamentals, earnings, macro, universe_profiles
               Read-only for the app. Updated by the daily pipeline.
               Shared across all environments (prod, dev).

  app.db     — strategies, portfolios, deployments, trades, alerts, regimes
               Read-write for the app. Different per environment.
               Controlled by APP_DB_PATH env var.

Usage:
    from server.database import get_market_db, get_app_db

    with get_market_db() as conn:
        prices = conn.execute("SELECT * FROM prices WHERE symbol = ?", ("AAPL",)).fetchall()

    with get_app_db() as conn:
        conn.execute("INSERT INTO strategies ...")
"""

import os
import sqlite3
from pathlib import Path
from contextlib import contextmanager

_BASE_DIR = Path(os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")))

MARKET_DB_PATH = Path(os.environ.get("MARKET_DB_PATH", str(_BASE_DIR / "market.db")))
APP_DB_PATH = Path(os.environ.get("APP_DB_PATH", str(_BASE_DIR / "app.db")))


@contextmanager
def get_market_db():
    """Read-only connection to market data (prices, fundamentals, macro)."""
    conn = sqlite3.connect(str(MARKET_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_app_db():
    """Read-write connection to application state (strategies, deployments, trades)."""
    conn = sqlite3.connect(str(APP_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()
