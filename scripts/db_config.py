"""
Database paths for scripts.

  MARKET_DB_PATH — prices, fundamentals, earnings, macro (shared, read-only)
  APP_DB_PATH    — strategies, portfolios, deployments, trades (per-environment)
"""
import os
from pathlib import Path

_BASE = Path(os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")))

MARKET_DB_PATH = Path(os.environ.get("MARKET_DB_PATH", str(_BASE / "market.db")))
APP_DB_PATH = Path(os.environ.get("APP_DB_PATH", str(_BASE / "app.db")))

# Backward compat — old scripts use DB_PATH
DB_PATH = APP_DB_PATH
