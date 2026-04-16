#!/usr/bin/env python3
"""
AlphaScout API Server
=====================
Read-only FastAPI layer over the AlphaScout SQLite database and JSON data files.

Usage:
    uvicorn api:app --host 0.0.0.0 --port 8000
    python3 api.py  # runs uvicorn directly

Environment:
    ALPHASCOUT_API_KEY  — required API key for X-API-Key header
    DB_PATH             — SQLite path (default: /app/data/alphascout.db)
    DATA_DIR            — JSON data dir (default: /app/data)
"""

import os
import sys
import json
import sqlite3
import asyncio
from pathlib import Path
from typing import Optional
from contextlib import contextmanager
from functools import partial

from fastapi import FastAPI, HTTPException, Query, Depends, Security, Header
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
import math
import re
from datetime import datetime, timezone, timedelta


async def _run_sync(func, *args, **kwargs):
    """Run a blocking function in a thread pool so it doesn't freeze the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
MARKET_DB_PATH = Path(os.environ.get("MARKET_DB_PATH", str(DATA_DIR / "market.db")))
APP_DB_PATH = Path(os.environ.get("APP_DB_PATH", str(DATA_DIR / "app.db")))
WORKSPACE = Path(os.environ.get("WORKSPACE", "/app"))
STRATEGIES_DIR = WORKSPACE / "strategies"
BACKTEST_RESULTS_DIR = WORKSPACE / "backtest" / "results"
API_KEY = os.environ.get("ALPHASCOUT_API_KEY", "")

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    # Startup: sync universe profiles and strategies from JSON files into DB
    _sync_universe_profiles()
    _sync_strategies_from_files()
    yield

app = FastAPI(
    title="AlphaScout API",
    description="Read-only financial data API — prices, fundamentals, earnings, analyst data, and more.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — open for now, tighten origins in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# JWT auth for dashboard
import jwt as _jwt
import bcrypt as _bcrypt

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24
DASHBOARD_USER = os.environ.get("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD_HASH = os.environ.get("DASHBOARD_PASSWORD_HASH", "")


async def verify_api_key(key: Optional[str] = Security(api_key_header)):
    """Accept either X-API-Key header or Bearer JWT token."""
    if not API_KEY and not DASHBOARD_PASSWORD_HASH:
        return  # No auth configured = open access
    if key and key == API_KEY:
        return  # Valid API key
    # Try JWT from Authorization header (injected by frontend)
    # FastAPI doesn't pass Authorization here, so API key is the primary path.
    # JWT is validated separately via verify_dashboard_token for /auth/* flows.
    if API_KEY and key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# ---------------------------------------------------------------------------
# Database — split into market (shared, read-only) and app (per-environment)
# ---------------------------------------------------------------------------
@contextmanager
def get_market_db():
    """Market data: prices, fundamentals, earnings, macro, universe_profiles."""
    conn = sqlite3.connect(str(MARKET_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_app_db():
    """App state: strategies, portfolios, deployments, trades, alerts, regimes."""
    conn = sqlite3.connect(str(APP_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_db():
    """Backward compat — returns app DB connection."""
    with get_app_db() as conn:
        yield conn

MACRO_DB_PATH = DATA_DIR / "macro" / "macro.db"

@contextmanager
def get_macro_db():
    """Legacy macro dashboard tables (oil_prices, vix, dxy, spx, fred_series)."""
    conn = sqlite3.connect(str(MACRO_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def query_db(sql: str, params: tuple = (), limit: int = 100, offset: int = 0):
    """Execute a read query on market DB and return list of dicts."""
    with get_market_db() as conn:
        cur = conn.cursor()
        cur.execute(sql + f" LIMIT {limit} OFFSET {offset}", params)
        rows = cur.fetchall()
        return [dict(row) for row in rows]


def query_db_count(sql: str, params: tuple = ()):
    """Get count for a query on market DB."""
    with get_market_db() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# JSON file reader
# ---------------------------------------------------------------------------
def read_json_file(path: Path) -> dict | list | None:
    """Read a JSON data file, unwrap 'data' key if present."""
    if not path.exists():
        return None
    try:
        with open(path) as f:
            d = json.load(f)
        if isinstance(d, dict) and "data" in d:
            return d["data"]
        return d
    except (json.JSONDecodeError, OSError):
        return None


def validate_symbol(symbol: str) -> str:
    """Sanitize and uppercase a ticker symbol."""
    s = symbol.strip().upper().replace("/", "").replace("..", "")
    if not s or len(s) > 10:
        raise HTTPException(status_code=400, detail="Invalid symbol")
    return s


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    """Health check — no auth required."""
    return {
        "status": "ok",
        "market_db": str(MARKET_DB_PATH),
        "market_db_exists": MARKET_DB_PATH.exists(),
        "app_db": str(APP_DB_PATH),
        "app_db_exists": APP_DB_PATH.exists(),
    }


# ---------------------------------------------------------------------------
# Dashboard Auth (JWT)
# ---------------------------------------------------------------------------
from pydantic import BaseModel as _AuthBM

class LoginRequest(_AuthBM):
    username: str
    password: str

class TokenResponse(_AuthBM):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = JWT_EXPIRY_HOURS * 3600
    user: str


@app.post("/auth/login", tags=["Auth"])
async def login(body: LoginRequest):
    """Authenticate with username/password. Returns a JWT token for dashboard access."""
    if not DASHBOARD_PASSWORD_HASH:
        raise HTTPException(500, "Dashboard auth not configured. Set DASHBOARD_PASSWORD_HASH in .env")

    if body.username != DASHBOARD_USER:
        raise HTTPException(401, "Invalid credentials")

    if not _bcrypt.checkpw(body.password.encode(), DASHBOARD_PASSWORD_HASH.encode()):
        raise HTTPException(401, "Invalid credentials")

    payload = {
        "sub": body.username,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    token = _jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    return TokenResponse(access_token=token, user=body.username)


@app.get("/auth/me", tags=["Auth"])
async def get_current_user(authorization: Optional[str] = Header(None)):
    """Validate a JWT token and return the current user."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing or invalid Authorization header")

    token = authorization[7:]
    try:
        payload = _jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return {"user": payload["sub"], "expires": payload.get("exp")}
    except _jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired")
    except _jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")


# ---------------------------------------------------------------------------
# Universe Profiles — synced from JSON files into DB for fast SQL queries
# ---------------------------------------------------------------------------

def _sync_universe_profiles():
    """Load all profile JSON files into the universe_profiles table.

    Called at startup. Can also be triggered via POST /api/universe/sync.
    Uses INSERT OR REPLACE so it's safe to re-run.
    """
    profile_dir = DATA_DIR / "universe" / "profiles"
    if not profile_dir.exists():
        return 0

    with get_market_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS universe_profiles (
                symbol       TEXT PRIMARY KEY,
                name         TEXT NOT NULL DEFAULT '',
                sector       TEXT NOT NULL DEFAULT '',
                industry     TEXT NOT NULL DEFAULT '',
                market_cap   REAL,
                exchange     TEXT NOT NULL DEFAULT '',
                country      TEXT NOT NULL DEFAULT '',
                beta         REAL,
                price        REAL,
                volume       INTEGER,
                avg_volume   INTEGER,
                is_actively_trading INTEGER DEFAULT 1,
                ipo_date     TEXT,
                is_etf       INTEGER DEFAULT 0,
                is_adr       INTEGER DEFAULT 0,
                cik          TEXT,
                description  TEXT,
                synced_at    TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_up_sector ON universe_profiles(sector)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_up_industry ON universe_profiles(industry)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_up_market_cap ON universe_profiles(market_cap)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_up_exchange ON universe_profiles(exchange)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_up_country ON universe_profiles(country)")
        # Composite index for common filter combos
        conn.execute("CREATE INDEX IF NOT EXISTS idx_up_sector_mcap ON universe_profiles(sector, market_cap)")

        now = datetime.now(timezone.utc).isoformat()
        count = 0
        for f in profile_dir.glob("*.json"):
            data = read_json_file(f)
            if not data:
                continue
            p = data[0] if isinstance(data, list) else data
            conn.execute("""
                INSERT OR REPLACE INTO universe_profiles
                    (symbol, name, sector, industry, market_cap, exchange, country,
                     beta, price, volume, avg_volume, is_actively_trading,
                     ipo_date, is_etf, is_adr, cik, description, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                p.get("symbol", f.stem),
                p.get("companyName", ""),
                p.get("sector", ""),
                p.get("industry", ""),
                p.get("marketCap"),
                p.get("exchange", ""),
                p.get("country", ""),
                p.get("beta"),
                p.get("price"),
                p.get("volume"),
                p.get("averageVolume"),
                1 if p.get("isActivelyTrading", True) else 0,
                p.get("ipoDate"),
                1 if p.get("isEtf") else 0,
                1 if p.get("isAdr") else 0,
                p.get("cik"),
                p.get("description"),
                now,
            ))
            count += 1
        conn.commit()
    return count


# ---------------------------------------------------------------------------
# Universe & Search
# ---------------------------------------------------------------------------
@app.get("/api/universe", dependencies=[Depends(verify_api_key)])
async def get_universe(
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    country: Optional[str] = None,
    exchange: Optional[str] = None,
    min_market_cap: Optional[float] = Query(None, description="Minimum market cap in USD"),
    max_market_cap: Optional[float] = Query(None, description="Maximum market cap in USD"),
    active_only: bool = Query(True, description="Only actively trading tickers"),
    sort: str = Query("symbol", description="Sort by: symbol, market_cap, name, sector"),
    order: str = Query("asc", description="Sort order: asc or desc"),
    limit: int = Query(default=600, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """List all tickers with profile data. Supports filtering by sector, industry,
    country, exchange, market cap range, and sorting."""
    allowed_sorts = {"symbol", "market_cap", "name", "sector", "industry", "exchange", "beta", "price"}
    if sort not in allowed_sorts:
        sort = "symbol"
    order_dir = "ASC" if order.lower() == "asc" else "DESC"

    conditions = []
    params: list = []
    if sector:
        conditions.append("sector = ? COLLATE NOCASE")
        params.append(sector)
    if industry:
        conditions.append("industry = ? COLLATE NOCASE")
        params.append(industry)
    if country:
        conditions.append("country = ? COLLATE NOCASE")
        params.append(country)
    if exchange:
        conditions.append("exchange = ? COLLATE NOCASE")
        params.append(exchange)
    if min_market_cap is not None:
        conditions.append("market_cap >= ?")
        params.append(min_market_cap)
    if max_market_cap is not None:
        conditions.append("market_cap <= ?")
        params.append(max_market_cap)
    if active_only:
        conditions.append("is_actively_trading = 1")

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_market_db() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM universe_profiles{where}", params
        ).fetchone()[0]
        rows = conn.execute(
            f"""SELECT symbol, name, sector, industry, market_cap, exchange, country,
                       is_actively_trading, beta, price
                FROM universe_profiles{where}
                ORDER BY {sort} {order_dir}
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()

    data = [dict(r) for r in rows]
    return {"total": total, "limit": limit, "offset": offset, "data": data}


@app.get("/api/search", dependencies=[Depends(verify_api_key)])
async def search(
    q: str = Query(..., min_length=1, max_length=50),
    limit: int = Query(default=20, le=100),
):
    """Search tickers and company names."""
    query = f"%{q}%"
    with get_market_db() as conn:
        rows = conn.execute(
            """SELECT symbol, name, sector, industry, market_cap
               FROM universe_profiles
               WHERE symbol LIKE ? COLLATE NOCASE OR name LIKE ? COLLATE NOCASE
               ORDER BY
                   CASE WHEN symbol = ? COLLATE NOCASE THEN 0
                        WHEN symbol LIKE ? COLLATE NOCASE THEN 1
                        ELSE 2 END,
                   market_cap DESC
               LIMIT ?""",
            (query, query, q.strip().upper(), f"{q}%", limit),
        ).fetchall()
    data = [dict(r) for r in rows]
    return {"total": len(data), "data": data}


@app.post("/api/universe/sync", dependencies=[Depends(verify_api_key)])
async def sync_universe():
    """Re-sync universe profiles from JSON files into the database."""
    count = _sync_universe_profiles()
    return {"status": "ok", "profiles_synced": count}


# ---------------------------------------------------------------------------
# Prices (DB)
# ---------------------------------------------------------------------------
@app.get("/api/prices/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_prices(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = Query(default=252, le=5000),
    offset: int = Query(default=0, ge=0),
):
    """Daily OHLCV prices for a ticker."""
    symbol = validate_symbol(symbol)
    conditions = ["symbol = ?"]
    params = [symbol]

    if start:
        conditions.append("date >= ?")
        params.append(start)
    if end:
        conditions.append("date <= ?")
        params.append(end)

    where = " AND ".join(conditions)
    data = query_db(
        f"SELECT * FROM prices WHERE {where} ORDER BY date DESC",
        tuple(params), limit=limit, offset=offset,
    )
    total = query_db_count(
        f"SELECT COUNT(*) FROM prices WHERE {where}", tuple(params)
    )
    return {"symbol": symbol, "total": total, "limit": limit, "offset": offset, "data": data}


# ---------------------------------------------------------------------------
# Fundamentals (DB)
# ---------------------------------------------------------------------------
@app.get("/api/income/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_income(
    symbol: str,
    period: Optional[str] = None,
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
):
    """Income statements (quarterly/annual)."""
    symbol = validate_symbol(symbol)
    conditions = ["symbol = ?"]
    params = [symbol]
    if period:
        conditions.append("period = ?")
        params.append(period)

    where = " AND ".join(conditions)
    data = query_db(
        f"SELECT * FROM income WHERE {where} ORDER BY date DESC",
        tuple(params), limit=limit, offset=offset,
    )
    return {"symbol": symbol, "total": len(data), "data": data}


@app.get("/api/balance/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_balance(
    symbol: str,
    period: Optional[str] = None,
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
):
    """Balance sheets."""
    symbol = validate_symbol(symbol)
    conditions = ["symbol = ?"]
    params = [symbol]
    if period:
        conditions.append("period = ?")
        params.append(period)

    where = " AND ".join(conditions)
    data = query_db(
        f"SELECT * FROM balance WHERE {where} ORDER BY date DESC",
        tuple(params), limit=limit, offset=offset,
    )
    return {"symbol": symbol, "total": len(data), "data": data}


@app.get("/api/cashflow/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_cashflow(
    symbol: str,
    period: Optional[str] = None,
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
):
    """Cash flow statements."""
    symbol = validate_symbol(symbol)
    conditions = ["symbol = ?"]
    params = [symbol]
    if period:
        conditions.append("period = ?")
        params.append(period)

    where = " AND ".join(conditions)
    data = query_db(
        f"SELECT * FROM cashflow WHERE {where} ORDER BY date DESC",
        tuple(params), limit=limit, offset=offset,
    )
    return {"symbol": symbol, "total": len(data), "data": data}


# ---------------------------------------------------------------------------
# Earnings (DB)
# ---------------------------------------------------------------------------
@app.get("/api/earnings/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_earnings(
    symbol: str,
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
):
    """Earnings history — EPS actual vs estimated."""
    symbol = validate_symbol(symbol)
    data = query_db(
        "SELECT * FROM earnings WHERE symbol = ? ORDER BY date DESC",
        (symbol,), limit=limit, offset=offset,
    )
    total = query_db_count("SELECT COUNT(*) FROM earnings WHERE symbol = ?", (symbol,))
    return {"symbol": symbol, "total": total, "data": data}


# ---------------------------------------------------------------------------
# Insider Trades (DB)
# ---------------------------------------------------------------------------
@app.get("/api/insider-trades/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_insider_trades(
    symbol: str,
    transaction_type: Optional[str] = None,
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
):
    """Insider transactions."""
    symbol = validate_symbol(symbol)
    conditions = ["symbol = ?"]
    params = [symbol]
    if transaction_type:
        conditions.append("transaction_type = ?")
        params.append(transaction_type)

    where = " AND ".join(conditions)
    data = query_db(
        f"SELECT * FROM insider_trades WHERE {where} ORDER BY transaction_date DESC",
        tuple(params), limit=limit, offset=offset,
    )
    total = query_db_count(
        f"SELECT COUNT(*) FROM insider_trades WHERE {where}", tuple(params)
    )
    return {"symbol": symbol, "total": total, "data": data}


# ---------------------------------------------------------------------------
# Analyst Grades (DB)
# ---------------------------------------------------------------------------
@app.get("/api/analyst-grades/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_analyst_grades(
    symbol: str,
    action: Optional[str] = None,
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
):
    """Analyst rating changes."""
    symbol = validate_symbol(symbol)
    conditions = ["symbol = ?"]
    params = [symbol]
    if action:
        conditions.append("action = ?")
        params.append(action)

    where = " AND ".join(conditions)
    data = query_db(
        f"SELECT * FROM analyst_grades WHERE {where} ORDER BY date DESC",
        tuple(params), limit=limit, offset=offset,
    )
    total = query_db_count(
        f"SELECT COUNT(*) FROM analyst_grades WHERE {where}", tuple(params)
    )
    return {"symbol": symbol, "total": total, "data": data}


# ---------------------------------------------------------------------------
# JSON-based endpoints (not in DB yet)
# ---------------------------------------------------------------------------
@app.get("/api/profile/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_profile(symbol: str):
    """Company profile — sector, industry, description, market cap."""
    symbol = validate_symbol(symbol)
    data = read_json_file(DATA_DIR / "universe" / "profiles" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Profile not found for {symbol}")
    profile = data[0] if isinstance(data, list) else data
    return {"symbol": symbol, "data": profile}


@app.get("/api/quotes/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_quote(symbol: str):
    """Latest quote snapshot — price, change, volume, 52wk range."""
    symbol = validate_symbol(symbol)
    data = read_json_file(DATA_DIR / "prices" / "quotes" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Quote not found for {symbol}")
    quote = data[0] if isinstance(data, list) else data
    return {"symbol": symbol, "data": quote}


@app.get("/api/metrics/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_metrics(
    symbol: str,
    ttm: bool = Query(default=False, description="Return TTM snapshot instead of historical"),
):
    """Key financial metrics — PE, EV/EBITDA, ROE, ROIC, FCF yield, etc."""
    symbol = validate_symbol(symbol)
    if ttm:
        data = read_json_file(DATA_DIR / "metrics" / "key-metrics-ttm" / f"{symbol}.json")
    else:
        data = read_json_file(DATA_DIR / "metrics" / "key-metrics" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Metrics not found for {symbol}")
    return {"symbol": symbol, "ttm": ttm, "data": data}


@app.get("/api/ratios/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_ratios(
    symbol: str,
    ttm: bool = Query(default=False),
):
    """Financial ratios — margins, turnover, leverage, valuation."""
    symbol = validate_symbol(symbol)
    if ttm:
        data = read_json_file(DATA_DIR / "metrics" / "ratios-ttm" / f"{symbol}.json")
    else:
        data = read_json_file(DATA_DIR / "metrics" / "ratios" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Ratios not found for {symbol}")
    return {"symbol": symbol, "ttm": ttm, "data": data}


@app.get("/api/estimates/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_estimates(symbol: str):
    """Analyst EPS/revenue estimates — annual and quarterly forward consensus."""
    symbol = validate_symbol(symbol)
    data = read_json_file(DATA_DIR / "analyst" / "estimates" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Estimates not found for {symbol}")
    return {"symbol": symbol, "data": data}


@app.get("/api/price-targets/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_price_targets(symbol: str):
    """Analyst price targets with firm names."""
    symbol = validate_symbol(symbol)
    data = read_json_file(DATA_DIR / "analyst" / "price-targets" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Price targets not found for {symbol}")
    return {"symbol": symbol, "data": data}


@app.get("/api/grades-consensus/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_grades_consensus(symbol: str):
    """Analyst consensus — buy/hold/sell counts."""
    symbol = validate_symbol(symbol)
    data = read_json_file(DATA_DIR / "analyst" / "grades-consensus" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Consensus not found for {symbol}")
    rec = data[0] if isinstance(data, list) else data
    return {"symbol": symbol, "data": rec}


@app.get("/api/dcf/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_dcf(symbol: str):
    """DCF intrinsic value estimate."""
    symbol = validate_symbol(symbol)
    data = read_json_file(DATA_DIR / "valuation" / "dcf" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"DCF not found for {symbol}")
    rec = data[0] if isinstance(data, list) else data
    return {"symbol": symbol, "data": rec}


@app.get("/api/dividends/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_dividends(symbol: str):
    """Dividend history — dates, amounts, yield, frequency."""
    symbol = validate_symbol(symbol)
    data = read_json_file(DATA_DIR / "catalysts" / "dividends" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Dividends not found for {symbol}")
    return {"symbol": symbol, "total": len(data) if isinstance(data, list) else 1, "data": data}


@app.get("/api/financial-scores/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_financial_scores(symbol: str):
    """Altman Z-Score, Piotroski F-Score."""
    symbol = validate_symbol(symbol)
    data = read_json_file(DATA_DIR / "metrics" / "financial-scores" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Scores not found for {symbol}")
    rec = data[0] if isinstance(data, list) else data
    return {"symbol": symbol, "data": rec}


@app.get("/api/owner-earnings/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_owner_earnings(symbol: str):
    """Owner earnings (Buffett-style FCF)."""
    symbol = validate_symbol(symbol)
    data = read_json_file(DATA_DIR / "metrics" / "owner-earnings" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Owner earnings not found for {symbol}")
    return {"symbol": symbol, "data": data}


@app.get("/api/enterprise-values/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_enterprise_values(symbol: str):
    """Enterprise value history — market cap, EV, net debt."""
    symbol = validate_symbol(symbol)
    data = read_json_file(DATA_DIR / "metrics" / "enterprise-values" / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"EV data not found for {symbol}")
    return {"symbol": symbol, "data": data}


@app.get("/api/growth/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_growth(
    symbol: str,
    type: str = Query(default="financial", description="financial | income | cashflow"),
):
    """Growth rates — revenue, EPS, book value, etc."""
    symbol = validate_symbol(symbol)
    type_map = {
        "financial": "fundamentals/financial-growth",
        "income": "fundamentals/income-growth",
        "cashflow": "fundamentals/cashflow-growth",
    }
    folder = type_map.get(type)
    if not folder:
        raise HTTPException(status_code=400, detail=f"Invalid growth type: {type}. Use: financial, income, cashflow")
    data = read_json_file(DATA_DIR / folder / f"{symbol}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Growth data not found for {symbol}")
    return {"symbol": symbol, "type": type, "data": data}


# ---------------------------------------------------------------------------
# Macro Tracker (SQLite-backed)
# ---------------------------------------------------------------------------
VALID_FRED_SERIES = {"BAMLH0A0HYM2", "T5YIFR", "DGS2", "DGS10", "FEDFUNDS"}


def _macro_query(table: str, columns: str, where: str = "", params: list = None,
                 limit: int = 500, offset: int = 0):
    params = params or []
    with get_macro_db() as conn:
        cur = conn.cursor()
        sql = f"SELECT {columns} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        sql += " ORDER BY date DESC"
        sql += f" LIMIT {limit} OFFSET {offset}"
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def _date_filters(from_date, to_date):
    clauses, params = [], []
    if from_date:
        clauses.append("date >= ?")
        params.append(from_date)
    if to_date:
        clauses.append("date <= ?")
        params.append(to_date)
    return " AND ".join(clauses), params


@app.get("/api/macro/dashboard", dependencies=[Depends(verify_api_key)], tags=["Macro Tracker"])
async def macro_dashboard():
    """Current regime snapshot — latest values with 1w and 1m changes."""
    result = {}
    with get_macro_db() as conn:
        cur = conn.cursor()

        def _latest_n(table, col, n, extra_where="", extra_params=None):
            ep = extra_params or []
            null_filter = f"{col} IS NOT NULL"
            w = f"WHERE {null_filter} AND {extra_where}" if extra_where else f"WHERE {null_filter}"
            cur.execute(f"SELECT date, {col} FROM {table} {w} ORDER BY date DESC LIMIT ?", ep + [n])
            return [dict(r) for r in cur.fetchall()]

        def _pct(current, old):
            if old is None or old == 0 or current is None:
                return None
            return round((current - old) / abs(old) * 100, 2)

        def _build(rows, col):
            if not rows:
                return {"value": None}
            latest = rows[0][col]
            w1 = rows[min(5, len(rows)-1)][col] if len(rows) > 1 else None
            m1 = rows[min(21, len(rows)-1)][col] if len(rows) > 1 else None
            d = {"value": latest}
            c1w = _pct(latest, w1)
            c1m = _pct(latest, m1)
            if c1w is not None:
                d["change_1w"] = c1w
            if c1m is not None:
                d["change_1m"] = c1m
            return d

        # Oil
        for label, tk in [("brent", "BRENT"), ("wti", "WTI")]:
            rows = _latest_n("oil_prices", "close", 22, "ticker = ?", [tk])
            d = _build(rows, "close")
            if "value" in d:
                d["price"] = d.pop("value")
            result[label] = d

        # DXY, VIX
        for label, table in [("dxy", "dxy"), ("vix", "vix")]:
            rows = _latest_n(table, "close", 22)
            result[label] = _build(rows, "close")

        # SPX
        rows = _latest_n("spx", "close", 22)
        result["spx"] = _build(rows, "close")

        # FRED series
        fred_map = {
            "BAMLH0A0HYM2": "hy_spreads",
            "T5YIFR": "breakeven_5y5y",
            "DGS2": "yield_2y",
            "DGS10": "yield_10y",
            "FEDFUNDS": "fed_funds",
        }
        for sid, label in fred_map.items():
            rows = _latest_n("fred_series", "value", 22, "series_id = ?", [sid])
            d = _build(rows, "value")
            if label == "fed_funds":
                d.pop("change_1w", None)
                d.pop("change_1m", None)
            result[label] = d

        as_of = None
        for table in ["oil_prices", "dxy", "vix", "spx"]:
            cur.execute(f"SELECT MAX(date) FROM {table}")
            r = cur.fetchone()[0]
            if r and (as_of is None or r > as_of):
                as_of = r

    return {"as_of": as_of, **result}


@app.get("/api/macro/oil", dependencies=[Depends(verify_api_key)], tags=["Macro Tracker"])
async def macro_oil(
    ticker: Optional[str] = Query(None, description="BZ=F (Brent) or CL=F (WTI)"),
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Oil price history — Brent and WTI."""
    clauses, params = _date_filters(from_date, to_date)
    VALID_OIL = {"BZ=F": "BRENT", "CL=F": "WTI", "BRENT": "BRENT", "WTI": "WTI"}
    if ticker:
        if ticker not in VALID_OIL:
            raise HTTPException(400, "ticker must be BRENT, WTI, BZ=F, or CL=F")
        ticker = VALID_OIL[ticker]
        clauses = " AND ".join(filter(None, [clauses, "ticker = ?"]))
        params.append(ticker)
    data = _macro_query("oil_prices", "date, ticker, open, high, low, close, volume",
                        clauses, params, limit, offset)
    return {"ticker": ticker or "ALL", "data": data, "count": len(data)}


@app.get("/api/macro/dxy", dependencies=[Depends(verify_api_key)], tags=["Macro Tracker"])
async def macro_dxy(
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Dollar index (DXY) history."""
    clauses, params = _date_filters(from_date, to_date)
    data = _macro_query("dxy", "date, open, high, low, close", clauses, params, limit, offset)
    return {"indicator": "DXY", "data": data, "count": len(data)}


@app.get("/api/macro/vix", dependencies=[Depends(verify_api_key)], tags=["Macro Tracker"])
async def macro_vix(
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """VIX history."""
    clauses, params = _date_filters(from_date, to_date)
    data = _macro_query("vix", "date, open, high, low, close", clauses, params, limit, offset)
    return {"indicator": "VIX", "data": data, "count": len(data)}


@app.get("/api/macro/spx", dependencies=[Depends(verify_api_key)], tags=["Macro Tracker"])
async def macro_spx(
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """S&P 500 history."""
    clauses, params = _date_filters(from_date, to_date)
    data = _macro_query("spx", "date, open, high, low, close, volume", clauses, params, limit, offset)
    return {"indicator": "SPX", "data": data, "count": len(data)}


@app.get("/api/macro/fred/{series_id}", dependencies=[Depends(verify_api_key)], tags=["Macro Tracker"])
async def macro_fred(
    series_id: str,
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """FRED series data."""
    if series_id not in VALID_FRED_SERIES:
        raise HTTPException(400, f"Invalid series_id. Valid: {sorted(VALID_FRED_SERIES)}")
    clauses, params = _date_filters(from_date, to_date)
    clauses = " AND ".join(filter(None, [clauses, "series_id = ?"]))
    params.append(series_id)
    data = _macro_query("fred_series", "date, value", clauses, params, limit, offset)
    return {"series_id": series_id, "data": data, "count": len(data)}


# ---------------------------------------------------------------------------
# Macro (JSON-based)
# ---------------------------------------------------------------------------
@app.get("/api/macro/{indicator}", dependencies=[Depends(verify_api_key)])
async def get_macro(indicator: str):
    """Macro indicators — cpi, gdp, treasury-rates, economic-calendar."""
    valid = ["cpi", "gdp", "treasury-rates", "economic-calendar"]
    if indicator not in valid:
        raise HTTPException(status_code=400, detail=f"Invalid indicator. Valid: {valid}")
    data = read_json_file(DATA_DIR / "macro" / f"{indicator}.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Macro data not found for {indicator}")
    return {"indicator": indicator, "data": data}


# ---------------------------------------------------------------------------
# Sector
# ---------------------------------------------------------------------------
@app.get("/api/sector/{metric}", dependencies=[Depends(verify_api_key)])
async def get_sector(metric: str):
    """Sector benchmarks — industry-pe, sector-pe, sector-performance."""
    valid = ["industry-pe", "sector-pe", "sector-performance"]
    if metric not in valid:
        raise HTTPException(status_code=400, detail=f"Invalid metric. Valid: {valid}")
    # Try {metric}.json first, then {metric}/latest.json
    data = read_json_file(DATA_DIR / "sector" / f"{metric}.json")
    if not data:
        data = read_json_file(DATA_DIR / "sector" / metric / "latest.json")
    if not data:
        raise HTTPException(status_code=404, detail=f"Sector data not found for {metric}")
    return {"metric": metric, "data": data}


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
@app.get("/api/stats", dependencies=[Depends(verify_api_key)])
async def get_stats():
    """Database and data layer statistics."""
    stats = {"market": {}, "app": {}}
    with get_market_db() as conn:
        for t in [r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
            stats["market"][t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    with get_app_db() as conn:
        for t in [r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
            stats["app"][t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]

    json_counts = {}
    for folder in ["prices/daily", "prices/quotes", "universe/profiles",
                    "fundamentals/income", "metrics/key-metrics",
                    "analyst/estimates", "catalysts/dividends",
                    "valuation/dcf", "earnings/calendar"]:
        path = DATA_DIR / folder
        if path.exists():
            json_counts[folder] = len(list(path.glob("*.json")))

    return {
        "db_tables": stats,
        "market_db_size_mb": round(MARKET_DB_PATH.stat().st_size / 1024 / 1024, 1) if MARKET_DB_PATH.exists() else 0,
        "app_db_size_mb": round(APP_DB_PATH.stat().st_size / 1024 / 1024, 1) if APP_DB_PATH.exists() else 0,
        "json_layers": json_counts,
    }


# ---------------------------------------------------------------------------
# Strategies & Backtests
# ---------------------------------------------------------------------------

def _extract_run_at(data: dict, filename: str) -> str | None:
    """Get run_at from data, or parse from filename as fallback."""
    if data.get("run_at"):
        return data["run_at"]
    # Fallback: parse YYYYMMDD_HHMMSS from end of filename
    m = re.search(r"(\d{8})_(\d{6})$", filename)
    if m:
        try:
            dt = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            pass
    return None


def _sanitize_floats(obj):
    """Replace inf/nan with None for JSON compliance."""
    if isinstance(obj, float):
        if math.isinf(obj) or math.isnan(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_floats(v) for v in obj]
    return obj


def _nulls_to_zero(obj):
    """In 'metrics' dicts, replace None with 0 so frontend .toFixed() won't crash."""
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            if k == "metrics" and isinstance(v, dict):
                result[k] = {mk: (0 if mv is None else _nulls_to_zero(mv)) for mk, mv in v.items()}
            else:
                result[k] = _nulls_to_zero(v)
        return result
    if isinstance(obj, list):
        return [_nulls_to_zero(v) for v in obj]
    return obj


# ---------------------------------------------------------------------------
# Strategy CRUD — Pydantic Models
# ---------------------------------------------------------------------------
# Domain models are the single source of truth. API DTOs compose from them,
# only adding API-specific concerns (which fields are required on create vs read).
# Backward compat (tickers→symbols, trigger/confirm→conditions, etc.) is handled
# by model_validators on the domain models themselves.
# ---------------------------------------------------------------------------
from pydantic import BaseModel as _BM, Field
from typing import Literal
from enum import Enum
import hashlib

# Import all shared types from domain models — no re-declaration
from models.strategy import (
    UniverseConfig, EntryConfig, EntryCondition,
    StopLossConfig, TakeProfitConfig, TimeStopConfig,
    RebalancingConfig, RebalancingRules, SizingConfig,
    RankingConfig, ExitCondition, BacktestParams,
)
from models.portfolio import (
    SleeveConfig, PortfolioConfig, InlineRegimeDefinition,
    AllocationProfile,
)
from models.regime import RegimeCondition, RegimeConfig


class AuthorConfig(_BM):
    """Strategy author metadata (API-only, not part of domain model)."""
    id: str = Field(default="owner", description="Author identifier.")
    name: str = Field(default="Omar", description="Author display name.")


class StrategyCreate(_BM):
    """Strategy creation request. Composes domain types — only adds API-specific fields.
    All optional fields have sensible defaults — only name, universe, and entry are required."""
    name: str = Field(min_length=1, max_length=200, description="Human-readable strategy name.")
    version: int = Field(default=1, description="Config version number.")
    author: AuthorConfig = AuthorConfig()
    universe: UniverseConfig
    entry: EntryConfig
    stop_loss: StopLossConfig | None = Field(default=None, description="Stop loss configuration. Omit or set to null to disable.")
    take_profit: TakeProfitConfig | None = Field(default=None, description="Take profit configuration. Omit or set to null to disable.")
    time_stop: TimeStopConfig | None = Field(default=None, description="Time-based exit. Omit or set to null to hold indefinitely.")
    exit_conditions: list[ExitCondition] | None = Field(default=None, description="Fundamental exit triggers (OR logic).")
    ranking: RankingConfig | None = Field(default=None, description="Rank candidates by metric before applying max_positions.")
    rebalancing: RebalancingConfig = Field(default=RebalancingConfig(), description="Periodic rebalancing settings.")
    sizing: SizingConfig = Field(default=SizingConfig(), description="Position sizing settings.")


def _normalize_config(config: dict) -> dict:
    """Normalize a stored config dict through domain model validators.

    Handles legacy field names (tickers→symbols, trigger/confirm→conditions, etc.)
    by round-tripping through the Pydantic model's model_validators.
    """
    from models.strategy import StrategyConfig
    try:
        return StrategyConfig.model_validate(config).model_dump(mode="json")
    except Exception:
        return config  # return as-is if validation fails (e.g. incomplete config)


def _compute_strategy_id(config: dict) -> str:
    """Same logic as backtest_engine.compute_strategy_id."""
    core = {k: config[k] for k in sorted(config) if k not in ("backtest", "name", "strategy_id", "version", "author")}
    return hashlib.sha256(json.dumps(core, sort_keys=True).encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Strategies table — single source of truth (replaces JSON file scan)
# ---------------------------------------------------------------------------

def _ensure_strategies_table():
    """Ensure all app tables exist."""
    sys.path.insert(0, str(WORKSPACE / "scripts"))
    from schema import init_db
    with get_db() as conn:
        init_db(conn)


def _sync_strategies_from_files():
    """DEPRECATED — DB is the single source of truth for strategies.
    Kept as a no-op so existing lifespan hook doesn't break."""
    _ensure_strategies_table()
    return 0


def _get_strategy_config(strategy_id: str) -> dict | None:
    """Look up a strategy by ID from the DB. O(1) indexed lookup."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT config FROM strategies WHERE strategy_id = ?", (strategy_id,)
        ).fetchone()
    if not row:
        return None
    return json.loads(row["config"])


def _write_strategy_file(strategy_id: str, config: dict):
    """DEPRECATED — DB is the single source of truth. No-op."""
    pass


# Create table at import time so endpoints work even without lifespan (e.g. TestClient)
_ensure_strategies_table()


# ---------------------------------------------------------------------------
# Strategy CRUD — Endpoints
# ---------------------------------------------------------------------------

@app.post("/strategies", tags=["Strategies"], status_code=201)
async def create_strategy(body: StrategyCreate, _: str = Depends(verify_api_key)):
    """Create a new strategy configuration.

    Validates all parameters, generates a deterministic strategy_id from core params,
    and saves to the database. Returns the full config with strategy_id.
    """
    config = body.model_dump(mode="json")
    strategy_id = _compute_strategy_id(config)
    config["strategy_id"] = strategy_id
    config["created_at"] = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        existing = conn.execute(
            "SELECT 1 FROM strategies WHERE strategy_id = ?", (strategy_id,)
        ).fetchone()
        if existing:
            raise HTTPException(409, f"Strategy with these parameters already exists (strategy_id: {strategy_id})")
        conn.execute(
            "INSERT INTO strategies (strategy_id, name, config, created_at) VALUES (?, ?, ?, ?)",
            (strategy_id, config["name"], json.dumps(config), config["created_at"]),
        )
        conn.commit()

    # Write JSON file for backward compat with backtest engine CLI
    _write_strategy_file(strategy_id, config)

    return config


@app.get("/strategies", tags=["Strategies"])
async def list_strategies(_: str = Depends(verify_api_key)):
    """List all saved strategy configurations."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT strategy_id, name, config, created_at, updated_at FROM strategies ORDER BY created_at"
        ).fetchall()
    results = []
    for r in rows:
        config = json.loads(r["config"])
        config = _normalize_config(config)
        results.append({
            "strategy_id": r["strategy_id"],
            "name": r["name"],
            "version": config.get("version"),
            "universe": config.get("universe", {}),
            "entry": config.get("entry", {}),
            "stop_loss": config.get("stop_loss"),
            "take_profit": config.get("take_profit"),
            "time_stop": config.get("time_stop"),
            "created_at": r["created_at"],
        })
    return results


class BacktestRunRequest(_BM):
    """Request to run a backtest on a saved strategy."""
    strategy_id: str = Field(description="Strategy ID to backtest.")
    start: str = Field(default="2015-01-01", description="Backtest start date (YYYY-MM-DD).")
    end: str = Field(default="2025-12-31", description="Backtest end date (YYYY-MM-DD).")
    initial_capital: float = Field(default=1000000, ge=1000, description="Starting capital in USD.")
    entry_price: Literal["next_close", "next_open"] = Field(default="next_close", description="Fill assumption. 'next_close': enter at closing price after signal. 'next_open': enter at next day's open.")
    slippage_bps: int = Field(default=10, ge=0, description="Simulated slippage in basis points.")


@app.post("/backtest/run", tags=["Strategies"], status_code=202)
async def run_backtest_endpoint(body: BacktestRunRequest, _: str = Depends(verify_api_key)):
    """Run a backtest on a saved strategy.

    Loads the strategy config by strategy_id, merges backtest parameters,
    runs the engine, saves results, and returns the run_id + summary metrics.
    Non-blocking — the engine runs in a thread pool so other requests are served.
    Typical runtime: 5-30 seconds depending on universe size and date range.
    """
    import traceback

    # Find strategy config from DB
    config = _get_strategy_config(body.strategy_id)
    if not config:
        raise HTTPException(404, f"Strategy '{body.strategy_id}' not found")

    # Merge backtest params into config
    config["backtest"] = {
        "start": body.start,
        "end": body.end,
        "entry_price": body.entry_price,
        "slippage_bps": body.slippage_bps,
    }
    config["sizing"]["initial_allocation"] = body.initial_capital

    # Run backtest (in thread pool — engine is CPU-bound, 5-30s)
    try:
        from backtest_engine import load_strategy, run_backtest, save_results

        # Validate through load_strategy (applies defaults, converts old format)
        # Write a temp file for load_strategy since it expects a path
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, dir=str(STRATEGIES_DIR)) as tmp:
            json.dump(config, tmp, indent=2)
            tmp_path = tmp.name

        try:
            validated_config = load_strategy(tmp_path)
            result = await _run_sync(run_backtest, validated_config)
            save_results(result, tmp_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        # Extract run_id from the saved filename
        metrics = result.get("metrics", {})
        bt = validated_config.get("backtest", {})
        name = result["strategy"].lower().replace(" ", "_")
        start_fmt = bt["start"].replace("-", "")
        end_fmt = bt["end"].replace("-", "")

        # Find the most recent result file matching this strategy
        results_dir = WORKSPACE / "backtest" / "results"
        matching = sorted(results_dir.glob(f"{name}_{start_fmt}_{end_fmt}_*.json"), reverse=True)
        matching = [f for f in matching if "_daily" not in f.name]
        run_id = matching[0].stem if matching else None

        # Persist trades to unified table
        if run_id:
            try:
                from deploy_engine import persist_trades
                all_trades = result.get("trades", [])
                if all_trades:
                    persist_trades("backtest", run_id, all_trades)
            except Exception:
                pass  # non-critical

        return _sanitize_floats({
            "run_id": run_id,
            "strategy_id": body.strategy_id,
            "strategy_name": result["strategy"],
            "status": "completed",
            "metrics": metrics,
            "total_trades": len(result.get("closed_trades", [])),
            "open_positions": len(result.get("open_positions", [])),
        })

    except ValueError as e:
        raise HTTPException(400, f"Invalid strategy config: {e}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Backtest failed: {e}")


@app.get("/backtest/runs", tags=["Strategies"])
async def list_backtest_runs(
    sort: str = Query("created_at", description="Sort field: created_at, alpha, sharpe, total_return, win_rate"),
    order: str = Query("desc", description="Sort order: asc or desc"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    universe: Optional[str] = Query(None, description="Filter by universe detail (e.g. Energy)"),
    strategy: Optional[str] = Query(None, description="Filter by strategy name (partial match)"),
    strategy_id: Optional[str] = Query(None, description="Filter by strategy_id (exact match)"),
    _: str = Depends(verify_api_key),
):
    """List all backtest runs from the index table with optional filters."""
    allowed_sorts = {"created_at", "alpha", "sharpe", "total_return", "win_rate", "ann_return", "max_drawdown", "profit_factor"}
    if sort not in allowed_sorts:
        sort = "created_at"
    order_dir = "ASC" if order.lower() == "asc" else "DESC"

    try:
        with get_db() as conn:
            where_clauses = []
            params = []
            if universe:
                where_clauses.append("universe_detail LIKE ?")
                params.append(f"%{universe}%")
            if strategy:
                where_clauses.append("strategy_name LIKE ?")
                params.append(f"%{strategy}%")
            if strategy_id:
                where_clauses.append("strategy_id = ?")
                params.append(strategy_id)

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

            # Get total count
            count_row = conn.execute(f"SELECT COUNT(*) as cnt FROM backtest_runs {where_sql}", params).fetchone()
            total = count_row["cnt"] if count_row else 0

            rows = conn.execute(
                f"""SELECT run_id, strategy_id, strategy_name, author_name, created_at,
                       universe_type, universe_detail, entry_type, entry_threshold, entry_window,
                       stop_loss, take_profit, max_positions, capital,
                       start_date, end_date,
                       total_return, ann_return, alpha, max_drawdown, max_drawdown_date,
                       sharpe, sortino, win_rate, profit_factor,
                       total_trades, wins, losses, avg_holding_days, final_nav, benchmark_return,
                       peak_utilized_capital, avg_utilized_capital, utilization_pct, return_on_utilized_capital_pct,
                       has_report, has_analysis, has_charts
                FROM backtest_runs {where_sql}
                ORDER BY {sort} {order_dir}
                LIMIT ? OFFSET ?""",
                params + [limit, offset],
            ).fetchall()

            results = []
            for r in rows:
                results.append(_sanitize_floats({
                    "run_id": r["run_id"], "strategy_id": r["strategy_id"],
                    "strategy_name": r["strategy_name"], "author": r["author_name"],
                    "created_at": r["created_at"],
                    "universe": {"type": r["universe_type"], "detail": r["universe_detail"]},
                    "entry": {"type": r["entry_type"], "threshold": r["entry_threshold"], "window_days": r["entry_window"]},
                    "stop_loss": r["stop_loss"], "take_profit": r["take_profit"],
                    "max_positions": r["max_positions"], "capital": r["capital"],
                    "period": {"start": r["start_date"], "end": r["end_date"]},
                    "metrics": {
                        "total_return": r["total_return"], "ann_return": r["ann_return"],
                        "alpha": r["alpha"], "max_drawdown": r["max_drawdown"],
                        "max_drawdown_date": r["max_drawdown_date"],
                        "sharpe": r["sharpe"], "sortino": r["sortino"],
                        "win_rate": r["win_rate"], "profit_factor": r["profit_factor"],
                        "total_trades": r["total_trades"], "wins": r["wins"], "losses": r["losses"],
                        "avg_holding_days": r["avg_holding_days"], "final_nav": r["final_nav"],
                        "benchmark_return": r["benchmark_return"],
                        "peak_utilized_capital": r["peak_utilized_capital"],
                        "avg_utilized_capital": r["avg_utilized_capital"],
                        "utilization_pct": r["utilization_pct"],
                        "return_on_utilized_capital_pct": r["return_on_utilized_capital_pct"],
                    },
                    "has_report": bool(r["has_report"]), "has_analysis": bool(r["has_analysis"]),
                    "has_charts": bool(r["has_charts"]),
                }))
            return {"total": total, "limit": limit, "offset": offset, "data": results}
    except sqlite3.OperationalError:
        # Table doesn't exist yet — fall back to file scan
        return {"total": 0, "limit": limit, "offset": offset, "data": []}


@app.get("/backtest/search", tags=["Strategies"])
async def search_backtest_runs(
    min_alpha: Optional[float] = Query(None, description="Minimum annualized alpha %"),
    min_sharpe: Optional[float] = Query(None, description="Minimum Sharpe ratio"),
    min_win_rate: Optional[float] = Query(None, description="Minimum win rate %"),
    min_return: Optional[float] = Query(None, description="Minimum total return %"),
    max_drawdown: Optional[float] = Query(None, description="Maximum drawdown % (e.g. -20)"),
    universe: Optional[str] = Query(None, description="Universe filter (e.g. Energy)"),
    sort: str = Query("alpha", description="Sort field"),
    order: str = Query("desc"),
    limit: int = Query(20, ge=1, le=100),
    _: str = Depends(verify_api_key),
):
    """Search backtest runs with performance filters."""
    allowed_sorts = {"alpha", "sharpe", "total_return", "win_rate", "ann_return", "profit_factor", "max_drawdown"}
    if sort not in allowed_sorts:
        sort = "alpha"
    order_dir = "ASC" if order.lower() == "asc" else "DESC"

    where_clauses = []
    params = []
    if min_alpha is not None:
        where_clauses.append("alpha >= ?"); params.append(min_alpha)
    if min_sharpe is not None:
        where_clauses.append("sharpe >= ?"); params.append(min_sharpe)
    if min_win_rate is not None:
        where_clauses.append("win_rate >= ?"); params.append(min_win_rate)
    if min_return is not None:
        where_clauses.append("total_return >= ?"); params.append(min_return)
    if max_drawdown is not None:
        where_clauses.append("max_drawdown >= ?"); params.append(max_drawdown)
    if universe:
        where_clauses.append("universe_detail LIKE ?"); params.append(f"%{universe}%")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    try:
        with get_db() as conn:
            rows = conn.execute(
                f"""SELECT run_id, strategy_name, universe_detail,
                       total_return, ann_return, alpha, max_drawdown,
                       sharpe, win_rate, profit_factor, total_trades, final_nav,
                       start_date, end_date, created_at
                FROM backtest_runs {where_sql}
                ORDER BY {sort} {order_dir}
                LIMIT ?""",
                params + [limit],
            ).fetchall()

            results = []
            for r in rows:
                results.append(_sanitize_floats({
                    "run_id": r["run_id"], "strategy_name": r["strategy_name"],
                    "universe": r["universe_detail"],
                    "total_return": r["total_return"], "ann_return": r["ann_return"],
                    "alpha": r["alpha"], "max_drawdown": r["max_drawdown"],
                    "sharpe": r["sharpe"], "win_rate": r["win_rate"],
                    "profit_factor": r["profit_factor"], "total_trades": r["total_trades"],
                    "final_nav": r["final_nav"],
                    "period": f"{r['start_date']} to {r['end_date']}",
                    "created_at": r["created_at"],
                }))
            return {"total": len(results), "data": results}
    except sqlite3.OperationalError:
        return {"total": 0, "data": []}


@app.get("/backtest/runs/{run_id}", tags=["Strategies"])
async def get_backtest_run(run_id: str, _: str = Depends(verify_api_key)):
    """Get full backtest results: metrics, trades, closed trades, nav history."""
    path = BACKTEST_RESULTS_DIR / f"{run_id}.json"
    if not path.exists():
        raise HTTPException(404, f"Backtest run '{run_id}' not found")
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        raise HTTPException(500, f"Error reading results: {e}")
    if not data.get("run_at"):
        data["run_at"] = _extract_run_at(data, run_id)
    return _sanitize_floats(data)


@app.get("/backtest/runs/{run_id}/daily", tags=["Strategies"])
async def get_backtest_daily(run_id: str, _: str = Depends(verify_api_key)):
    """Get daily portfolio values and benchmark for charting equity curves."""
    path = BACKTEST_RESULTS_DIR / f"{run_id}_daily.json"
    if not path.exists():
        raise HTTPException(404, f"Daily data for run '{run_id}' not found")
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        raise HTTPException(500, f"Error reading daily data: {e}")
    return data


@app.post("/backtest/runs/{run_id}/analyze", tags=["Strategies"], status_code=202)
async def analyze_backtest_run(run_id: str, _: str = Depends(verify_api_key)):
    """Generate LLM analysis for a backtest run.

    Reads the backtest results, sends them to an LLM with a report template,
    and saves the analysis as {run_id}_analysis.json and {run_id}_report.md.
    Non-blocking — uses the async Anthropic client so other requests are served
    while the LLM generates (typically 15-30 seconds).
    """
    import traceback

    # Check results exist
    results_path = BACKTEST_RESULTS_DIR / f"{run_id}.json"
    if not results_path.exists():
        raise HTTPException(404, f"Backtest run '{run_id}' not found")

    # Check if analysis already exists
    analysis_path = BACKTEST_RESULTS_DIR / f"{run_id}_analysis.json"
    report_path = BACKTEST_RESULTS_DIR / f"{run_id}_report.md"
    if analysis_path.exists() and report_path.exists():
        return {
            "status": "already_exists",
            "run_id": run_id,
            "analysis_url": f"/backtest/runs/{run_id}/analysis",
            "report_url": f"/backtest/runs/{run_id}/report",
        }

    try:
        results = json.loads(results_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        raise HTTPException(500, f"Error reading results: {e}")

    # Load the report template
    template_path = WORKSPACE / "skills" / "test-strategy" / "references" / "report-format.md"
    template = ""
    if template_path.exists():
        template = template_path.read_text()

    # Build a concise data summary for the LLM (don't send full NAV history)
    metrics = results.get("metrics", {})
    config = results.get("config", {})
    closed_trades = results.get("closed_trades", [])
    open_positions = results.get("open_positions", [])
    benchmark = results.get("benchmark", {})

    data_summary = json.dumps({
        "strategy": results.get("strategy"),
        "config": config,
        "metrics": metrics,
        "benchmark_metrics": benchmark.get("metrics") if benchmark else None,
        "closed_trades_count": len(closed_trades),
        "closed_trades_sample": closed_trades[:20],
        "open_positions": open_positions,
        "total_trades": len(results.get("trades", [])),
    }, indent=2, default=str)

    prompt = f"""You are AlphaScout, a quantitative research analyst. Analyze this backtest result and generate a report.

## Report Template
{template}

## Backtest Data
{data_summary}

Generate:
1. The full report following the template (markdown format)
2. A JSON analysis object with:
   - "executive_summary": 3-4 sentence summary for a portfolio manager
   - "observations": array of {{"category": "string", "text": "string"}} with key findings

Respond in this exact format:
---REPORT---
<the full markdown report>
---ANALYSIS_JSON---
<the JSON object>
"""

    try:
        import anthropic
        client = anthropic.AsyncAnthropic()
        response = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}],
        )
        content = response.content[0].text

        # Parse the response
        report_text = ""
        analysis_json = {}

        if "---REPORT---" in content and "---ANALYSIS_JSON---" in content:
            parts = content.split("---ANALYSIS_JSON---")
            report_text = parts[0].replace("---REPORT---", "").strip()
            try:
                json_str = parts[1].strip()
                if json_str.startswith("```"):
                    json_str = json_str.split("```")[1]
                    if json_str.startswith("json"):
                        json_str = json_str[4:]
                analysis_json = json.loads(json_str)
            except (json.JSONDecodeError, IndexError):
                analysis_json = {"executive_summary": content[:500], "observations": []}
        else:
            report_text = content
            analysis_json = {"executive_summary": content[:500], "observations": []}

        # Save analysis files
        report_path.write_text(report_text)
        analysis_path.write_text(json.dumps(analysis_json, indent=2))

        # Update the backtest_runs DB to mark analysis as available
        try:
            with get_db() as conn:
                conn.execute(
                    "UPDATE backtest_runs SET has_analysis = 1, has_report = 1 WHERE run_id = ?",
                    (run_id,),
                )
                conn.commit()
        except Exception:
            pass

        return {
            "status": "completed",
            "run_id": run_id,
            "analysis": analysis_json,
            "analysis_url": f"/backtest/runs/{run_id}/analysis",
            "report_url": f"/backtest/runs/{run_id}/report",
        }

    except ImportError:
        raise HTTPException(500, "Anthropic package not installed")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Analysis generation failed: {e}")


@app.get("/backtest/runs/{run_id}/analysis", tags=["Strategies"])
async def get_backtest_analysis(run_id: str, _: str = Depends(verify_api_key)):
    """Get LLM-generated executive summary and observations for a backtest run."""
    path = BACKTEST_RESULTS_DIR / f"{run_id}_analysis.json"
    if not path.exists():
        raise HTTPException(404, f"No analysis found for run '{run_id}'. Run the test-strategy skill to generate one.")
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        raise HTTPException(500, f"Error reading analysis: {e}")
    return data


@app.get("/backtest/runs/{run_id}/report", tags=["Strategies"])
async def get_backtest_report(run_id: str, _: str = Depends(verify_api_key)):
    """Get the LLM-generated analysis report for a backtest run (markdown)."""
    path = BACKTEST_RESULTS_DIR / f"{run_id}_report.md"
    if not path.exists():
        raise HTTPException(404, f"No analysis report found for run '{run_id}'. Run the test-strategy skill to generate one.")
    return PlainTextResponse(path.read_text(), media_type="text/markdown")


# ---------------------------------------------------------------------------
# Deployments (Live Strategy Tracking)
# ---------------------------------------------------------------------------
from pydantic import BaseModel

SCRIPTS_DIR = WORKSPACE / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


# ---------------------------------------------------------------------------
# Unified Deployment Management — /deployments/
# ---------------------------------------------------------------------------

@app.get("/deployments", tags=["Deployments (Unified)"])
async def list_deployments_unified(
    include_stopped: bool = Query(False),
    type: Optional[str] = Query(None, description="Filter by type: 'strategy' or 'portfolio'"),
    _: str = Depends(verify_api_key),
):
    """List all deployments. Optional ?type=strategy or ?type=portfolio filter."""
    from deploy_engine import list_deployments as _list
    deployments = _list(include_stopped=include_stopped, deploy_type=type)
    return {
        "total": len(deployments),
        "data": [_sanitize_floats({
            "id": d["id"],
            "type": d.get("type", "strategy"),
            "name": d.get("name", ""),
            "num_sleeves": d.get("num_sleeves", 1),
            "portfolio_id": d.get("portfolio_id"),
            "start_date": d["start_date"],
            "initial_capital": d["initial_capital"],
            "status": d["status"],
            "created_at": d["created_at"],
            "last_evaluated": d.get("last_evaluated"),
            "last_nav": d.get("last_nav"),
            "last_return_pct": d.get("last_return_pct"),
            "total_trades": d.get("total_trades", 0),
            "open_positions": d.get("open_positions", 0),
            "last_alpha_pct": d.get("last_alpha_pct"),
            "last_benchmark_return_pct": d.get("last_benchmark_return_pct"),
            "alpha_vs_market_pct": d.get("alpha_vs_market_pct"),
            "alpha_vs_sector_pct": d.get("alpha_vs_sector_pct"),
            "market_benchmark_return_pct": d.get("market_benchmark_return_pct"),
            "sector_benchmark_return_pct": d.get("sector_benchmark_return_pct"),
            "last_sharpe_ratio": d.get("last_sharpe_ratio"),
            "last_max_drawdown_pct": d.get("last_max_drawdown_pct"),
            "last_ann_volatility_pct": d.get("last_ann_volatility_pct"),
            "rolling_vol_30d_pct": d.get("rolling_vol_30d_pct"),
            "current_utilization_pct": d.get("current_utilization_pct"),
            "peak_utilized_capital": d.get("peak_utilized_capital"),
            "avg_utilized_capital": d.get("avg_utilized_capital"),
            "utilization_pct": d.get("utilization_pct"),
            "return_on_utilized_capital_pct": d.get("return_on_utilized_capital_pct"),
            "alert_mode": bool(d.get("alert_mode", 0)),
            "error": d.get("error"),
        }) for d in deployments],
    }


@app.get("/deployments/{deploy_id}", tags=["Deployments (Unified)"])
async def get_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Get full deployment details. Response includes sleeve/regime data for portfolio deployments."""
    from deploy_engine import get_deployment as _get
    d = _get(deploy_id)
    if not d:
        raise HTTPException(404, f"Deployment '{deploy_id}' not found")

    config = None
    if d.get("config_json"):
        try:
            config = json.loads(d["config_json"]) if isinstance(d["config_json"], str) else d["config_json"]
        except (json.JSONDecodeError, TypeError):
            pass

    result = _sanitize_floats({
        "id": d["id"],
        "type": d.get("type", "strategy"),
        "name": d.get("name", ""),
        "num_sleeves": d.get("num_sleeves", 1),
        "portfolio_id": d.get("portfolio_id"),
        "config": config,
        "start_date": d["start_date"],
        "initial_capital": d["initial_capital"],
        "status": d["status"],
        "created_at": d["created_at"],
        "last_evaluated": d.get("last_evaluated"),
        "last_nav": d.get("last_nav"),
        "last_return_pct": d.get("last_return_pct"),
        "total_trades": d.get("total_trades", 0),
        "open_positions": d.get("open_positions", 0),
        "last_alpha_pct": d.get("last_alpha_pct"),
        "last_benchmark_return_pct": d.get("last_benchmark_return_pct"),
        "alpha_vs_market_pct": d.get("alpha_vs_market_pct"),
        "alpha_vs_sector_pct": d.get("alpha_vs_sector_pct"),
        "market_benchmark_return_pct": d.get("market_benchmark_return_pct"),
        "sector_benchmark_return_pct": d.get("sector_benchmark_return_pct"),
        "last_sharpe_ratio": d.get("last_sharpe_ratio"),
        "last_max_drawdown_pct": d.get("last_max_drawdown_pct"),
        "last_ann_volatility_pct": d.get("last_ann_volatility_pct"),
        "rolling_vol_30d_pct": d.get("rolling_vol_30d_pct"),
        "current_utilization_pct": d.get("current_utilization_pct"),
        "peak_utilized_capital": d.get("peak_utilized_capital"),
        "avg_utilized_capital": d.get("avg_utilized_capital"),
        "utilization_pct": d.get("utilization_pct"),
        "return_on_utilized_capital_pct": d.get("return_on_utilized_capital_pct"),
        "alert_mode": bool(d.get("alert_mode", 0)),
        "error": d.get("error"),
    })

    # Rich data from results file
    if d.get("metrics"):
        result["metrics"] = _sanitize_floats(d["metrics"])
    if d.get("sleeves"):
        result["sleeves"] = _sanitize_floats(d["sleeves"])
    if d.get("nav_history"):
        result["nav_history"] = d["nav_history"]
    if d.get("benchmark"):
        result["benchmark"] = _sanitize_floats(d["benchmark"])
    if d.get("benchmark_market"):
        result["benchmark_market"] = _sanitize_floats(d["benchmark_market"])
    if d.get("benchmark_sector"):
        result["benchmark_sector"] = _sanitize_floats(d["benchmark_sector"])
    if d.get("regime_history"):
        result["regime_history"] = d["regime_history"]

    return result


@app.post("/deployments/{deploy_id}/evaluate", tags=["Deployments (Unified)"])
async def evaluate_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Manually trigger re-evaluation of a deployment."""
    from deploy_engine import evaluate_one
    result = await _run_sync(evaluate_one, deploy_id)
    if result is None:
        raise HTTPException(400, f"Could not evaluate '{deploy_id}' — check status/errors")
    metrics = result.get("metrics", {})
    return _sanitize_floats({
        "id": deploy_id,
        "status": "evaluated",
        "nav": metrics.get("final_nav"),
        "return_pct": metrics.get("total_return_pct"),
        "total_trades": metrics.get("total_trades"),
    })


def _build_position_book(sleeves: list[dict], final_nav: float) -> list[dict]:
    """Merge open positions + closed trades across sleeves into per-ticker book."""
    book: dict[str, dict] = {}

    def _ensure(sym: str) -> dict:
        if sym not in book:
            book[sym] = {
                "symbol": sym,
                "shares_held": 0, "current_price": 0,
                "market_value": 0, "cost_basis_open": 0,
                "unrealized_pnl": 0, "realized_pnl": 0,
                "realized_cost": 0,  # total capital deployed in closed round-trips
                "num_round_trips": 0, "sleeves": [],
            }
        return book[sym]

    for sleeve in sleeves or []:
        label = sleeve.get("label", "")

        for pos in sleeve.get("open_positions") or []:
            b = _ensure(pos["symbol"])
            shares = pos.get("shares", 0)
            entry = pos.get("entry_price", 0)
            cost = pos.get("cost_basis", shares * entry)
            mv = pos.get("market_value", shares * pos.get("current_price", 0))
            b["shares_held"] += shares
            b["cost_basis_open"] += cost
            b["market_value"] += mv
            b["current_price"] = pos.get("current_price", b["current_price"])
            b["unrealized_pnl"] += mv - cost
            if label and label not in b["sleeves"]:
                b["sleeves"].append(label)

        for ct in sleeve.get("closed_trades") or []:
            b = _ensure(ct["symbol"])
            b["realized_pnl"] += ct.get("pnl", 0)
            b["realized_cost"] += ct.get("shares", 0) * ct.get("entry_price", 0)
            b["num_round_trips"] += 1
            if label and label not in b["sleeves"]:
                b["sleeves"].append(label)

    positions = []
    for b in book.values():
        has_open = b["shares_held"] > 0
        has_closed = b["num_round_trips"] > 0
        b["status"] = "partial" if (has_open and has_closed) else ("open" if has_open else "closed")
        b["avg_entry"] = (b["cost_basis_open"] / b["shares_held"]) if b["shares_held"] else 0
        b["total_pnl"] = b["realized_pnl"] + b["unrealized_pnl"]
        total_cost = b["cost_basis_open"] + b["realized_cost"]
        b["total_pnl_pct"] = (b["total_pnl"] / total_cost * 100) if total_cost else 0
        b["weight_pct"] = (b["market_value"] / final_nav * 100) if final_nav and b["market_value"] else 0
        # Drop internal accounting fields
        del b["cost_basis_open"]
        del b["realized_cost"]
        positions.append(b)

    positions.sort(key=lambda p: p["total_pnl"], reverse=True)
    return positions


@app.get("/deployments/{deploy_id}/positions", tags=["Deployments (Unified)"])
async def get_deployment_positions(deploy_id: str, _: str = Depends(verify_api_key)):
    """Per-ticker position book for a deployment.

    Merges open positions (unrealized P&L) and closed trades (realized P&L)
    across all sleeves into a single per-ticker summary.
    """
    from deploy_engine import get_deployment as _get

    d = _get(deploy_id)
    if not d:
        raise HTTPException(404, f"Deployment '{deploy_id}' not found")

    initial_capital = d.get("initial_capital", 0) or 0
    portfolio_nav = (d.get("metrics") or {}).get("final_nav", 0)

    # Compute sleeve-level NAV from actual positions (the number that reconciles
    # with per-ticker P&L). Portfolio-level NAV may differ due to regime gating.
    sleeves = d.get("sleeves") or []
    sleeve_cash = 0
    sleeve_positions_value = 0
    for sl in sleeves:
        for pos in sl.get("open_positions") or []:
            sleeve_positions_value += pos.get("market_value", 0)
        # Try to get cash from last nav_history entry; fall back to 0
        nav_hist = sl.get("nav_history") or sl.get("metrics", {}).get("nav_history") or []
        if nav_hist and isinstance(nav_hist[-1], dict):
            sleeve_cash += nav_hist[-1].get("cash", 0)

    # If we couldn't get sleeve cash, derive it: sleeve_nav = initial_capital + total_pnl
    # so cash = sleeve_nav - positions_value. Use portfolio_nav as fallback.
    trading_nav = sleeve_cash + sleeve_positions_value if sleeve_cash else portfolio_nav

    positions = _build_position_book(sleeves, trading_nav)

    total_realized = sum(p["realized_pnl"] for p in positions)
    total_unrealized = sum(p["unrealized_pnl"] for p in positions)
    total_pnl = total_realized + total_unrealized
    # trading_nav derived from actual P&L — always reconciles
    trading_nav = initial_capital + total_pnl if initial_capital else trading_nav
    regime_gating_impact = portfolio_nav - trading_nav if portfolio_nav else None

    return _sanitize_floats({
        "deployment_id": deploy_id,
        "initial_capital": initial_capital,
        "trading_nav": trading_nav,
        "trading_gain": total_pnl,
        "portfolio_nav": portfolio_nav,
        "portfolio_gain": portfolio_nav - initial_capital if portfolio_nav else None,
        "regime_gating_impact": regime_gating_impact,
        "total_realized_pnl": total_realized,
        "total_unrealized_pnl": total_unrealized,
        "total_pnl": total_pnl,
        "open_count": sum(1 for p in positions if p["status"] in ("open", "partial")),
        "closed_count": sum(1 for p in positions if p["status"] == "closed"),
        "positions": positions,
    })


@app.post("/deployments/{deploy_id}/stop", tags=["Deployments (Unified)"])
async def stop_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Stop a deployment."""
    from deploy_engine import stop_deployment as _stop
    _stop(deploy_id)
    return {"id": deploy_id, "status": "stopped"}


@app.post("/deployments/{deploy_id}/pause", tags=["Deployments (Unified)"])
async def pause_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Pause a deployment (skip evaluations)."""
    from deploy_engine import pause_deployment as _pause
    _pause(deploy_id)
    return {"id": deploy_id, "status": "paused"}


@app.post("/deployments/{deploy_id}/resume", tags=["Deployments (Unified)"])
async def resume_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Resume a paused deployment."""
    from deploy_engine import resume_deployment as _resume
    _resume(deploy_id)
    return {"id": deploy_id, "status": "active"}


@app.delete("/deployments/{deploy_id}", tags=["Deployments (Unified)"])
async def delete_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Delete a stopped deployment and all related data."""
    with get_db() as conn:
        row = conn.execute("SELECT id, status FROM deployments WHERE id = ?", (deploy_id,)).fetchone()
        if not row:
            raise HTTPException(404, f"Deployment '{deploy_id}' not found")
        if row["status"] == "active":
            raise HTTPException(409, "Cannot delete an active deployment. Stop it first.")
        alert_ids = [r[0] for r in conn.execute(
            "SELECT id FROM trade_alerts WHERE deployment_id = ?", (deploy_id,)).fetchall()]
        if alert_ids:
            placeholders = ",".join("?" * len(alert_ids))
            conn.execute(f"DELETE FROM trade_executions WHERE alert_id IN ({placeholders})", alert_ids)
        conn.execute("DELETE FROM trade_alerts WHERE deployment_id = ?", (deploy_id,))
        conn.execute("DELETE FROM trades WHERE source_id = ?", (deploy_id,))
        conn.execute("DELETE FROM sleeves WHERE deployment_id = ?", (deploy_id,))
        conn.execute("DELETE FROM deployments WHERE id = ?", (deploy_id,))
        conn.commit()
    return {"deleted": deploy_id}


@app.get("/deployments/{deploy_id}/alerts", tags=["Deployments (Unified)"])
async def get_deployment_alerts_unified(
    deploy_id: str,
    date: str = Query(None), status: str = Query(None), limit: int = Query(100),
    _: str = Depends(verify_api_key),
):
    """List trade alerts for a deployment."""
    from deploy_engine import get_alerts
    alerts = get_alerts(deploy_id=deploy_id, date=date, status=status, limit=limit)
    return {"deploy_id": deploy_id, "total": len(alerts), "data": alerts}


@app.get("/deployments/{deploy_id}/alerts/summary", tags=["Deployments (Unified)"])
async def get_deployment_alerts_summary_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Get alert execution summary for a deployment."""
    from deploy_engine import get_execution_summary as _summary
    return _summary(deploy_id)


@app.post("/deployments/{deploy_id}/alerts/enable", tags=["Deployments (Unified)"])
async def enable_deployment_alerts_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Enable alert mode for a deployment."""
    from deploy_engine import set_alert_mode
    result = set_alert_mode(deploy_id, True)
    return result


@app.post("/deployments/{deploy_id}/alerts/disable", tags=["Deployments (Unified)"])
async def disable_deployment_alerts_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Disable alert mode for a deployment."""
    from deploy_engine import set_alert_mode
    result = set_alert_mode(deploy_id, False)
    return result


# ---------------------------------------------------------------------------
# Legacy Deploy Endpoints (kept for backward compat)
# ---------------------------------------------------------------------------

class DeployRequest(BaseModel):
    config_path: str | None = None  # path to strategy config (relative to workspace)
    backtest_run_id: str | None = None  # OR deploy from a previous backtest run
    strategy_id: str | None = None  # OR deploy by strategy_id (resolves to config file)
    start_date: str  # YYYY-MM-DD
    initial_capital: float = 100000
    name: str | None = None


@app.post("/strategies/deploy", tags=["Deployments"], deprecated=True)
async def deploy_strategy(body: DeployRequest, _: str = Depends(verify_api_key)):
    """Deploy a strategy for live paper-trading.

    Provide one of: strategy_id, config_path, or backtest_run_id.
    """
    from deploy_engine import deploy

    # Resolve config path
    tmp_config = None
    if body.strategy_id:
        # Look up config from DB
        strat_config = _get_strategy_config(body.strategy_id)
        if not strat_config:
            raise HTTPException(404, f"No strategy found with id '{body.strategy_id}'")
        # Write temp file for deploy engine (expects a file path)
        tmp_config = STRATEGIES_DIR / f"_deploy_tmp_{body.strategy_id}.json"
        tmp_config.write_text(json.dumps(strat_config, indent=2))
        config_path = str(tmp_config)
    elif body.backtest_run_id:
        # Extract config from backtest results
        result_path = BACKTEST_RESULTS_DIR / f"{body.backtest_run_id}.json"
        if not result_path.exists():
            raise HTTPException(404, f"Backtest run '{body.backtest_run_id}' not found")
        result_data = json.loads(result_path.read_text())
        config = result_data.get("config")
        if not config:
            raise HTTPException(400, f"No config found in backtest run '{body.backtest_run_id}'")
        # Write config to temp file
        tmp_config = STRATEGIES_DIR / f"_deploy_tmp_{body.backtest_run_id}.json"
        tmp_config.write_text(json.dumps(config, indent=2))
        config_path = str(tmp_config)
    elif body.config_path:
        config_path = str(WORKSPACE / body.config_path)
        if not Path(config_path).exists():
            raise HTTPException(404, f"Config not found: {body.config_path}")
    else:
        raise HTTPException(400, "Provide strategy_id, config_path, or backtest_run_id")

    try:
        # Read the resolved config before deploying (tmp file may be deleted after)
        resolved_config = json.loads(Path(config_path).read_text())
        result = await _run_sync(deploy, config_path, body.start_date, body.initial_capital, body.name)
        # Clean up temp file
        if tmp_config:
            tmp_config.unlink(missing_ok=True)
        result["config"] = resolved_config
        return result
    except Exception as e:
        raise HTTPException(500, f"Deployment failed: {e}")


@app.get("/strategies/deployments", tags=["Deployments"], deprecated=True)
async def list_deployments(
    include_stopped: bool = Query(False),
    type: Optional[str] = Query(None, description="Filter by type: 'strategy' or 'portfolio'"),
    _: str = Depends(verify_api_key),
):
    """List all deployments (strategies and portfolios)."""
    from deploy_engine import list_deployments as _list
    deployments = _list(include_stopped=include_stopped, deploy_type=type)
    return {
        "total": len(deployments),
        "data": [_sanitize_floats({
            "id": d["id"],
            "type": d.get("type", "strategy"),
            "name": d.get("name", ""),
            "num_sleeves": d.get("num_sleeves", 1),
            "portfolio_id": d.get("portfolio_id"),
            "start_date": d["start_date"],
            "initial_capital": d["initial_capital"],
            "status": d["status"],
            "created_at": d["created_at"],
            "last_evaluated": d.get("last_evaluated"),
            "last_nav": d.get("last_nav"),
            "last_return_pct": d.get("last_return_pct"),
            "total_trades": d.get("total_trades", 0),
            "open_positions": d.get("open_positions", 0),
            "last_alpha_pct": d.get("last_alpha_pct"),
            "last_benchmark_return_pct": d.get("last_benchmark_return_pct"),
            "alpha_vs_market_pct": d.get("alpha_vs_market_pct"),
            "alpha_vs_sector_pct": d.get("alpha_vs_sector_pct"),
            "market_benchmark_return_pct": d.get("market_benchmark_return_pct"),
            "sector_benchmark_return_pct": d.get("sector_benchmark_return_pct"),
            "last_sharpe_ratio": d.get("last_sharpe_ratio"),
            "last_max_drawdown_pct": d.get("last_max_drawdown_pct"),
            "last_ann_volatility_pct": d.get("last_ann_volatility_pct"),
            "rolling_vol_30d_pct": d.get("rolling_vol_30d_pct"),
            "current_utilization_pct": d.get("current_utilization_pct"),
            "peak_utilized_capital": d.get("peak_utilized_capital"),
            "avg_utilized_capital": d.get("avg_utilized_capital"),
            "utilization_pct": d.get("utilization_pct"),
            "return_on_utilized_capital_pct": d.get("return_on_utilized_capital_pct"),
            "alert_mode": bool(d.get("alert_mode", 0)),
            "error": d.get("error"),
        }) for d in deployments],
    }


@app.get("/strategies/deployments/{deploy_id}", tags=["Deployments"], deprecated=True)
async def get_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Get full deployment details including latest engine results.

    Returns positions, trades, NAV history, performance metrics — everything
    the backtest engine outputs, evaluated up to the most recent data refresh.
    """
    from deploy_engine import get_deployment as _get
    d = _get(deploy_id)
    if not d:
        raise HTTPException(404, f"Deployment '{deploy_id}' not found")

    # Parse stored config
    config = None
    if d.get("config_json"):
        try:
            config = json.loads(d["config_json"]) if isinstance(d["config_json"], str) else d["config_json"]
        except (json.JSONDecodeError, TypeError):
            pass

    result = _sanitize_floats({
        "id": d["id"],
        "type": d.get("type", "strategy"),
        "name": d.get("name", ""),
        "num_sleeves": d.get("num_sleeves", 1),
        "portfolio_id": d.get("portfolio_id"),
        "config": config,
        "start_date": d["start_date"],
        "initial_capital": d["initial_capital"],
        "status": d["status"],
        "created_at": d["created_at"],
        "last_evaluated": d.get("last_evaluated"),
        "last_nav": d.get("last_nav"),
        "last_return_pct": d.get("last_return_pct"),
        "total_trades": d.get("total_trades", 0),
        "open_positions": d.get("open_positions", 0),
        "last_alpha_pct": d.get("last_alpha_pct"),
        "last_benchmark_return_pct": d.get("last_benchmark_return_pct"),
        "alpha_vs_market_pct": d.get("alpha_vs_market_pct"),
        "alpha_vs_sector_pct": d.get("alpha_vs_sector_pct"),
        "market_benchmark_return_pct": d.get("market_benchmark_return_pct"),
        "sector_benchmark_return_pct": d.get("sector_benchmark_return_pct"),
        "last_sharpe_ratio": d.get("last_sharpe_ratio"),
        "last_max_drawdown_pct": d.get("last_max_drawdown_pct"),
        "last_ann_volatility_pct": d.get("last_ann_volatility_pct"),
        "rolling_vol_30d_pct": d.get("rolling_vol_30d_pct"),
        "current_utilization_pct": d.get("current_utilization_pct"),
        "peak_utilized_capital": d.get("peak_utilized_capital"),
        "avg_utilized_capital": d.get("avg_utilized_capital"),
        "utilization_pct": d.get("utilization_pct"),
        "return_on_utilized_capital_pct": d.get("return_on_utilized_capital_pct"),
        "error": d.get("error"),
    })

    # Rich data from results file (loaded by get_deployment)
    if d.get("metrics"):
        result["metrics"] = _sanitize_floats(d["metrics"])
    if d.get("sleeves"):
        result["sleeves"] = _sanitize_floats(d["sleeves"])
    if d.get("nav_history"):
        result["nav_history"] = d["nav_history"]
    if d.get("benchmark"):
        result["benchmark"] = _sanitize_floats(d["benchmark"])
    if d.get("benchmark_market"):
        result["benchmark_market"] = _sanitize_floats(d["benchmark_market"])
    if d.get("benchmark_sector"):
        result["benchmark_sector"] = _sanitize_floats(d["benchmark_sector"])
    if d.get("regime_history"):
        result["regime_history"] = d["regime_history"]

    return result


@app.get("/strategies/deployments/{deploy_id}/daily", tags=["Deployments"], deprecated=True)
async def get_deployment_daily(deploy_id: str, _: str = Depends(verify_api_key)):
    """Get daily NAV + benchmark data for charting equity curves, with utilization metrics."""
    from deploy_engine import DEPLOYMENTS_DIR as _DDIR, get_db as _get_deploy_db
    daily_path = _DDIR / deploy_id / "results_daily.json"
    if not daily_path.exists():
        raise HTTPException(404, f"No daily data for deployment '{deploy_id}'")
    try:
        data = json.loads(daily_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        raise HTTPException(500, f"Error reading daily data: {e}")

    # Look up initial_capital for utilization % and ROIC calc
    conn = _get_deploy_db()
    try:
        row = conn.execute("SELECT initial_capital FROM deployed_strategies WHERE id = ?", (deploy_id,)).fetchone()
        initial_capital = row["initial_capital"] if row else None
    finally:
        conn.close()

    # Enrich nav_history with utilization + return on invested
    if "nav_history" in data and initial_capital:
        cumulative_pnl = 0
        for point in data["nav_history"]:
            invested = point.get("positions_value", 0)
            cash = point.get("cash", 0)
            nav = point.get("nav", invested + cash)
            pnl = nav - initial_capital

            point["invested_capital"] = round(invested, 2)
            point["idle_capital"] = round(cash, 2)
            point["utilization_pct"] = round((invested / nav) * 100, 2) if nav > 0 else 0
            point["cumulative_return_pct"] = round((pnl / initial_capital) * 100, 2) if initial_capital > 0 else 0
            point["return_on_invested_pct"] = round((pnl / invested) * 100, 2) if invested > 0 else 0

    return _sanitize_floats(data)


@app.post("/strategies/deployments/{deploy_id}/evaluate", tags=["Deployments"], deprecated=True)
async def evaluate_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Manually trigger re-evaluation of a deployment (normally done by daily cron)."""
    from deploy_engine import evaluate_one
    result = await _run_sync(evaluate_one, deploy_id)
    if result is None:
        raise HTTPException(400, f"Could not evaluate '{deploy_id}' — check status/errors")
    metrics = result.get("metrics", {})
    return _sanitize_floats({
        "id": deploy_id,
        "status": "evaluated",
        "nav": metrics.get("final_nav"),
        "return_pct": metrics.get("total_return_pct"),
        "total_trades": metrics.get("total_trades"),
    })


@app.post("/strategies/deployments/{deploy_id}/stop", tags=["Deployments"], deprecated=True)
async def stop_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Stop tracking a deployment."""
    from deploy_engine import stop_deployment as _stop
    _stop(deploy_id)
    return {"id": deploy_id, "status": "stopped"}


@app.delete("/strategies/deployments/{deploy_id}", tags=["Deployments"], deprecated=True)
async def delete_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Delete a deployment and all related data (sleeves, trades, alerts)."""
    with get_db() as conn:
        row = conn.execute("SELECT id, status FROM deployments WHERE id = ?", (deploy_id,)).fetchone()
        if not row:
            raise HTTPException(404, f"Deployment '{deploy_id}' not found")
        if row["status"] == "active":
            raise HTTPException(409, "Cannot delete an active deployment. Stop it first.")
        # Cascade delete related data
        alert_ids = [r[0] for r in conn.execute(
            "SELECT id FROM trade_alerts WHERE deployment_id = ?", (deploy_id,)).fetchall()]
        if alert_ids:
            placeholders = ",".join("?" * len(alert_ids))
            conn.execute(f"DELETE FROM trade_executions WHERE alert_id IN ({placeholders})", alert_ids)
        conn.execute("DELETE FROM trade_alerts WHERE deployment_id = ?", (deploy_id,))
        conn.execute("DELETE FROM trades WHERE source_id = ?", (deploy_id,))
        conn.execute("DELETE FROM sleeves WHERE deployment_id = ?", (deploy_id,))
        conn.execute("DELETE FROM deployments WHERE id = ?", (deploy_id,))
        conn.commit()
    return {"deleted": deploy_id}


@app.post("/strategies/deployments/{deploy_id}/pause", tags=["Deployments"], deprecated=True)
async def pause_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Pause a deployment (skip daily evaluations)."""
    from deploy_engine import pause_deployment as _pause
    _pause(deploy_id)
    return {"id": deploy_id, "status": "paused"}


@app.post("/strategies/deployments/{deploy_id}/resume", tags=["Deployments"], deprecated=True)
async def resume_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Resume a paused deployment."""
    from deploy_engine import resume_deployment as _resume
    _resume(deploy_id)
    return {"id": deploy_id, "status": "active"}


# ---------------------------------------------------------------------------
# Trade Alerts
# ---------------------------------------------------------------------------

@app.post("/strategies/deployments/{deploy_id}/alerts/enable", tags=["Alerts"], deprecated=True)
async def enable_alerts(deploy_id: str, _: str = Depends(verify_api_key)):
    """Enable alert mode for a deployment. Generates daily BUY/SELL alerts."""
    from deploy_engine import set_alert_mode
    result = set_alert_mode(deploy_id, True)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@app.post("/strategies/deployments/{deploy_id}/alerts/disable", tags=["Alerts"], deprecated=True)
async def disable_alerts(deploy_id: str, _: str = Depends(verify_api_key)):
    """Disable alert mode for a deployment."""
    from deploy_engine import set_alert_mode
    result = set_alert_mode(deploy_id, False)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@app.get("/strategies/deployments/{deploy_id}/alerts", tags=["Alerts"], deprecated=True)
async def get_deployment_alerts(
    deploy_id: str,
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    status: Optional[str] = Query(None, description="Filter by execution status: pending, executed, skipped"),
    limit: int = Query(50, ge=1, le=200),
    _: str = Depends(verify_api_key),
):
    """Get trade alerts for a specific deployment."""
    from deploy_engine import get_alerts
    alerts = get_alerts(deploy_id=deploy_id, date=date, status=status, limit=limit)
    return {"total": len(alerts), "data": [_sanitize_floats(a) for a in alerts]}


@app.get("/alerts/today", tags=["Alerts"])
async def get_today_alerts(_: str = Depends(verify_api_key)):
    """Get all pending trade alerts for today across all alert-enabled deployments."""
    from deploy_engine import get_alerts
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    alerts = get_alerts(date=today, limit=200)
    # Group by deployment
    by_deploy = {}
    for a in alerts:
        did = a["deployment_id"]
        if did not in by_deploy:
            by_deploy[did] = {
                "deployment_id": did,
                "strategy_name": a.get("strategy_name", ""),
                "alerts": [],
            }
        by_deploy[did]["alerts"].append(_sanitize_floats(a))
    return {
        "date": today,
        "total_alerts": len(alerts),
        "pending": sum(1 for a in alerts if a.get("execution_status") == "pending"),
        "deployments": list(by_deploy.values()),
    }


class ExecuteAlertRequest(BaseModel):
    fill_price: float | None = None
    fill_shares: float | None = None
    broker: str = "manual"
    notes: str | None = None


@app.post("/alerts/{alert_id}/execute", tags=["Alerts"])
async def execute_alert(
    alert_id: str,
    body: ExecuteAlertRequest = ExecuteAlertRequest(),
    _: str = Depends(verify_api_key),
):
    """Mark a trade alert as executed with optional fill details."""
    from deploy_engine import execute_alert as _execute
    result = _execute(
        alert_id, fill_price=body.fill_price, fill_shares=body.fill_shares,
        broker=body.broker, notes=body.notes,
    )
    if "error" in result:
        raise HTTPException(404, result["error"])
    return _sanitize_floats(result)


class SkipAlertRequest(BaseModel):
    notes: str | None = None


@app.post("/alerts/{alert_id}/skip", tags=["Alerts"])
async def skip_alert(
    alert_id: str,
    body: SkipAlertRequest = SkipAlertRequest(),
    _: str = Depends(verify_api_key),
):
    """Mark a trade alert as skipped."""
    from deploy_engine import skip_alert as _skip
    result = _skip(alert_id, notes=body.notes)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return _sanitize_floats(result)


@app.get("/strategies/deployments/{deploy_id}/alerts/summary", tags=["Alerts"], deprecated=True)
async def get_execution_summary(deploy_id: str, _: str = Depends(verify_api_key)):
    """Get execution tracking summary: follow-through rate, avg slippage, paper vs real."""
    from deploy_engine import get_execution_summary as _summary
    return _sanitize_floats(_summary(deploy_id))


@app.get("/alerts/summary", tags=["Alerts"])
async def get_global_execution_summary(_: str = Depends(verify_api_key)):
    """Get global execution tracking summary across all deployments."""
    from deploy_engine import get_execution_summary as _summary
    return _sanitize_floats(_summary())


# ---------------------------------------------------------------------------
# Trades (unified)
# ---------------------------------------------------------------------------

@app.get("/trades", tags=["Trades"])
async def list_trades(
    source_type: Optional[str] = Query(None, description="Filter: 'backtest' or 'deployment'"),
    source_id: Optional[str] = Query(None, description="Backtest run_id or deployment ID"),
    deployment_type: Optional[str] = Query(None, description="Filter: 'strategy' or 'portfolio'"),
    sleeve_label: Optional[str] = Query(None, description="Filter by sleeve label"),
    symbol: Optional[str] = Query(None, description="Filter by ticker symbol"),
    action: Optional[str] = Query(None, description="Filter: 'BUY' or 'SELL'"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _: str = Depends(verify_api_key),
):
    """List trades with filters."""
    where = []
    params = []
    if source_type:
        where.append("t.source_type = ?")
        params.append(source_type)
    if source_id:
        where.append("t.source_id = ?")
        params.append(source_id)
    if deployment_type:
        where.append("t.deployment_type = ?")
        params.append(deployment_type)
    if sleeve_label:
        where.append("t.sleeve_label = ?")
        params.append(sleeve_label)
    if symbol:
        where.append("t.symbol = ?")
        params.append(symbol.upper())
    if action:
        where.append("t.action = ?")
        params.append(action.upper())
    if date_from:
        where.append("t.date >= ?")
        params.append(date_from)
    if date_to:
        where.append("t.date <= ?")
        params.append(date_to)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    with get_db() as conn:
        total = conn.execute(f"SELECT COUNT(*) as cnt FROM trades t {where_sql}", params).fetchone()["cnt"]
        rows = conn.execute(
            f"""SELECT t.* FROM trades t {where_sql}
                ORDER BY t.date DESC, t.symbol
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()

    data = []
    for r in rows:
        d = dict(r)
        if d.get("signal_detail") and isinstance(d["signal_detail"], str):
            try:
                d["signal_detail"] = json.loads(d["signal_detail"])
            except (json.JSONDecodeError, TypeError):
                pass
        data.append(_sanitize_floats(d))

    return {"total": total, "limit": limit, "offset": offset, "data": data}


@app.get("/trades/{trade_id}", tags=["Trades"])
async def get_trade(trade_id: str, _: str = Depends(verify_api_key)):
    """Get a single trade with full detail. Includes alert/execution info if exists."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Trade not found")
        d = dict(row)
        if d.get("signal_detail") and isinstance(d["signal_detail"], str):
            try:
                d["signal_detail"] = json.loads(d["signal_detail"])
            except (json.JSONDecodeError, TypeError):
                pass

        # Check for linked alert
        alert = conn.execute(
            """SELECT a.id as alert_id, e.status as execution_status,
                      e.fill_price, e.fill_shares, e.fill_time, e.broker, e.slippage_pct
               FROM trade_alerts a
               LEFT JOIN trade_executions e ON e.alert_id = a.id
               WHERE a.deployment_id = ? AND a.date = ? AND a.symbol = ? AND a.action = ?
               LIMIT 1""",
            (d["source_id"], d["date"], d["symbol"], d["action"]),
        ).fetchone()
        if alert:
            d["alert"] = _sanitize_floats(dict(alert))

    return _sanitize_floats(d)


@app.get("/deployments/{deploy_id}/trades", tags=["Trades"])
async def get_deployment_trades(
    deploy_id: str,
    symbol: Optional[str] = Query(None, description="Filter by ticker symbol"),
    action: Optional[str] = Query(None, description="Filter: 'BUY' or 'SELL'"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _: str = Depends(verify_api_key),
):
    """Get all trades for a deployment."""
    return await list_trades(
        source_type="deployment", source_id=deploy_id,
        deployment_type=None, sleeve_label=None,
        symbol=symbol, action=action,
        date_from=date_from, date_to=date_to,
        limit=limit, offset=offset, _=_,
    )


@app.get("/backtests/{run_id}/trades", tags=["Trades"])
async def get_backtest_trades(
    run_id: str,
    symbol: Optional[str] = Query(None, description="Filter by ticker symbol"),
    action: Optional[str] = Query(None, description="Filter: 'BUY' or 'SELL'"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _: str = Depends(verify_api_key),
):
    """Get all trades for a backtest run."""
    return await list_trades(
        source_type="backtest", source_id=run_id,
        deployment_type=None, sleeve_label=None,
        symbol=symbol, action=action,
        date_from=None, date_to=None,
        limit=limit, offset=offset, _=_,
    )


@app.get("/backtests/{run_id}/positions", tags=["Backtests"])
async def get_backtest_positions(run_id: str, _: str = Depends(verify_api_key)):
    """Per-ticker position book for a backtest run.

    Same structure as GET /deployments/{id}/positions — merges open positions
    and closed trades across all sleeves.
    """
    from portfolio_engine import _ensure_portfolio_backtest_table
    _ensure_portfolio_backtest_table()

    # Try portfolio backtest first (has sleeve_results)
    data = None
    with get_db() as conn:
        prow = conn.execute(
            "SELECT results_path FROM portfolio_backtest_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
    if prow:
        path = Path(prow["results_path"]) if prow["results_path"] else None
        if path and path.exists():
            data = json.loads(path.read_text())

    # Fallback to strategy backtest
    if not data:
        path = BACKTEST_RESULTS_DIR / f"{run_id}.json"
        if not path.exists():
            raise HTTPException(404, f"Backtest run '{run_id}' not found")
        data = json.loads(path.read_text())

    portfolio_nav = (data.get("metrics") or {}).get("final_nav", 0)
    initial_capital = (data.get("metrics") or {}).get("initial_capital") or (
        (data.get("config") or {}).get("backtest_params", {}).get("initial_capital", 0)
    )

    # Build sleeves list — portfolio backtests have sleeve_results, strategy has top-level
    sleeves = []
    for i, sr in enumerate(data.get("sleeve_results") or []):
        label = ""
        ps = data.get("per_sleeve", [])
        if i < len(ps):
            label = ps[i].get("label", "")
        sleeves.append({
            "label": label,
            "open_positions": sr.get("open_positions", []),
            "closed_trades": sr.get("closed_trades", []),
        })
    if not sleeves:
        # Strategy backtest — single sleeve at top level
        sleeves.append({
            "label": "",
            "open_positions": data.get("open_positions", []),
            "closed_trades": data.get("closed_trades", []),
        })

    positions = _build_position_book(sleeves, portfolio_nav)
    total_realized = sum(p["realized_pnl"] for p in positions)
    total_unrealized = sum(p["unrealized_pnl"] for p in positions)
    total_pnl = total_realized + total_unrealized
    trading_nav = initial_capital + total_pnl if initial_capital else portfolio_nav
    regime_gating_impact = portfolio_nav - trading_nav if portfolio_nav else None

    return _sanitize_floats({
        "run_id": run_id,
        "initial_capital": initial_capital,
        "trading_nav": trading_nav,
        "trading_gain": total_pnl,
        "portfolio_nav": portfolio_nav,
        "portfolio_gain": portfolio_nav - initial_capital if portfolio_nav else None,
        "regime_gating_impact": regime_gating_impact,
        "total_realized_pnl": total_realized,
        "total_unrealized_pnl": total_unrealized,
        "total_pnl": total_pnl,
        "open_count": sum(1 for p in positions if p["status"] in ("open", "partial")),
        "closed_count": sum(1 for p in positions if p["status"] == "closed"),
        "positions": positions,
    })


# ---------------------------------------------------------------------------
# Strategy CRUD — Detail Endpoints (after /strategies/deploy* to avoid route conflicts)
# ---------------------------------------------------------------------------

@app.get("/strategies/{strategy_id}", tags=["Strategies"])
async def get_strategy(strategy_id: str, _: str = Depends(verify_api_key)):
    """Get full strategy configuration by strategy_id."""
    config = _get_strategy_config(strategy_id)
    if not config:
        raise HTTPException(404, f"Strategy '{strategy_id}' not found")
    config = _normalize_config(config)
    return config


@app.put("/strategies/{strategy_id}", tags=["Strategies"])
async def update_strategy(strategy_id: str, body: StrategyCreate, _: str = Depends(verify_api_key)):
    """Update a strategy configuration.

    Replaces the full config. If core parameters changed, strategy_id will be
    recomputed (new params = new identity). The old row is replaced.
    """
    old_config = _get_strategy_config(strategy_id)
    if not old_config:
        raise HTTPException(404, f"Strategy '{strategy_id}' not found")

    config = body.model_dump(mode="json")
    new_id = _compute_strategy_id(config)
    config["strategy_id"] = new_id
    config["created_at"] = old_config.get("created_at")
    config["updated_at"] = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        # If ID changed, check the new ID doesn't collide
        if new_id != strategy_id:
            collision = conn.execute(
                "SELECT 1 FROM strategies WHERE strategy_id = ?", (new_id,)
            ).fetchone()
            if collision:
                raise HTTPException(409, f"A strategy with these parameters already exists (strategy_id: {new_id})")
            # Delete old row
            conn.execute("DELETE FROM strategies WHERE strategy_id = ?", (strategy_id,))

        conn.execute(
            """INSERT OR REPLACE INTO strategies (strategy_id, name, config, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (new_id, config["name"], json.dumps(config), config["created_at"], config["updated_at"]),
        )
        conn.commit()

    # Update JSON file for backward compat
    # Remove old file if ID changed
    if new_id != strategy_id:
        for f in STRATEGIES_DIR.glob(f"*_{strategy_id}.json"):
            f.unlink(missing_ok=True)
    _write_strategy_file(new_id, config)

    return config


@app.delete("/strategies/{strategy_id}", tags=["Strategies"], status_code=200)
async def delete_strategy(strategy_id: str, _: str = Depends(verify_api_key)):
    """Delete a strategy configuration.

    Does NOT affect existing backtests or deployments that used this strategy.
    """
    with get_db() as conn:
        cur = conn.execute("DELETE FROM strategies WHERE strategy_id = ?", (strategy_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, f"Strategy '{strategy_id}' not found")
        conn.commit()

    # Remove JSON file too
    for f in STRATEGIES_DIR.glob(f"*_{strategy_id}.json"):
        f.unlink(missing_ok=True)

    return {"deleted": strategy_id}


# ---------------------------------------------------------------------------
# Chat Sessions — removed (was OpenClaw gateway dependent)
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# Regimes
# ---------------------------------------------------------------------------
import hashlib as _hl

sys.path.insert(0, str(WORKSPACE / "scripts"))
from regime import evaluate_regimes as _eval_regimes, evaluate_regime_series as _eval_regime_series, get_regime_details as _get_regime_details


class RegimeCondition(_BM):
    series: str = Field(description="Macro series key (e.g. 'brent_vs_50dma_pct', 'vix', 'hy_spread_zscore')")
    operator: str = Field(description="Comparison operator: >, >=, <, <=, ==, !=")
    value: float = Field(description="Threshold value")


class RegimeCreate(_BM):
    name: str = Field(description="Human-readable regime name (e.g. 'Oil Shock')")
    # Legacy format (symmetric entry=exit)
    conditions: list[RegimeCondition] | None = Field(default=None, description="Entry conditions (legacy format)")
    logic: str | None = Field(default=None, description="Logic for conditions: 'all' or 'any' (legacy)")
    # New format (separate entry/exit + cooldown)
    entry_conditions: list[RegimeCondition] | None = Field(default=None, description="Entry conditions")
    entry_logic: str = Field(default="all", description="'all' (AND) or 'any' (OR) for entry")
    exit_conditions: list[RegimeCondition] | None = Field(default=None, description="Exit conditions (defaults to inverse of entry)")
    exit_logic: str = Field(default="any", description="'all' (AND) or 'any' (OR) for exit")
    min_hold_days: int = Field(default=0, ge=0, description="Minimum trading days before exit conditions are checked")


class RegimeUpdate(_BM):
    name: str | None = None
    conditions: list[RegimeCondition] | None = None
    logic: str | None = None
    entry_conditions: list[RegimeCondition] | None = None
    entry_logic: str | None = None
    exit_conditions: list[RegimeCondition] | None = None
    exit_logic: str | None = None
    min_hold_days: int | None = None


def _regime_id(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    short = _hl.md5(f"{slug}{datetime.now().isoformat()}".encode()).hexdigest()[:8]
    return f"{slug}_{short}"


@app.post("/regimes", tags=["Regimes"], status_code=201)
async def create_regime(body: RegimeCreate, _: str = Depends(verify_api_key)):
    """Create a new regime detector."""
    regime_id = _regime_id(body.name)
    config = {"name": body.name}

    # Support both legacy (conditions/logic) and new (entry_conditions/exit_conditions) format
    if body.entry_conditions:
        config["entry_conditions"] = [c.model_dump() for c in body.entry_conditions]
        config["entry_logic"] = body.entry_logic
        if body.exit_conditions:
            config["exit_conditions"] = [c.model_dump() for c in body.exit_conditions]
            config["exit_logic"] = body.exit_logic
        config["min_hold_days"] = body.min_hold_days
    elif body.conditions:
        # Legacy format — store as entry_conditions internally
        config["entry_conditions"] = [c.model_dump() for c in body.conditions]
        config["entry_logic"] = body.logic or "all"
    else:
        raise HTTPException(400, "Must provide either 'conditions' or 'entry_conditions'")
    with get_db() as conn:
        conn.execute(
            "INSERT INTO regimes (regime_id, name, config) VALUES (?, ?, ?)",
            (regime_id, body.name, json.dumps(config)),
        )
        conn.commit()
    return {"regime_id": regime_id, "name": body.name, "config": config}


@app.get("/regimes", tags=["Regimes"])
async def list_regimes(_: str = Depends(verify_api_key)):
    """List all saved regimes."""
    with get_db() as conn:
        cur = conn.execute("SELECT regime_id, name, config, created_at, updated_at FROM regimes ORDER BY created_at")
        rows = cur.fetchall()
    return [
        {
            "regime_id": r["regime_id"], "name": r["name"], "config": json.loads(r["config"]),
            "created_at": r["created_at"], "updated_at": r["updated_at"],
        }
        for r in rows
    ]


@app.post("/regimes/evaluate", tags=["Regimes"])
async def evaluate_multiple_regimes(
    date: str = Query(..., description="Date to evaluate (YYYY-MM-DD)"),
    regime_ids: str = Query(..., description="Comma-separated regime IDs"),
    detail: bool = Query(False, description="Include per-condition breakdown"),
    _: str = Depends(verify_api_key),
):
    """Evaluate multiple regimes for a single date."""
    ids = [r.strip() for r in regime_ids.split(",")]
    configs = []
    with get_db() as conn:
        for rid in ids:
            cur = conn.execute("SELECT config FROM regimes WHERE regime_id = ?", (rid,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, f"Regime {rid} not found")
            configs.append(json.loads(row["config"]))

    if detail:
        return _get_regime_details(date, configs)

    active = _eval_regimes(date, configs)
    return {
        "date": date,
        "active_regimes": active,
        "all_regimes": [c["name"] for c in configs],
    }


# ---------------------------------------------------------------------------
# Portfolios
# ---------------------------------------------------------------------------
sys.path.insert(0, str(WORKSPACE / "scripts"))
from portfolio_engine import run_portfolio_backtest as _run_portfolio_bt, save_portfolio_results as _save_portfolio_results, compute_portfolio_id as _compute_portfolio_id


# PortfolioCreate uses the domain PortfolioConfig directly.
# Backward compat (strategies→sleeves, capital_flow→capital_when_gated_off,
# config→strategy_config) is handled by model_validators on the domain models.
PortfolioCreate = PortfolioConfig


class PortfolioUpdate(_BM):
    name: str | None = None
    sleeves: list[SleeveConfig] | None = None
    regime_filter: bool | None = None
    capital_when_gated_off: Literal["to_cash", "redistribute"] | None = None
    regime_definitions: dict[str, InlineRegimeDefinition] | None = None
    allocation_profiles: dict[str, AllocationProfile] | None = None
    profile_priority: list[str] | None = None
    transition_days: int | None = None


class PortfolioBacktestRequest(_BM):
    portfolio_id: str = Field(description="Portfolio ID to backtest")
    start: str = Field(default="2020-01-01", description="Backtest start date")
    end: str = Field(default="2026-03-28", description="Backtest end date")
    initial_capital: float = Field(default=1000000, ge=1000, description="Starting capital")


class PortfolioBacktestParams(_BM):
    """Backtest params for the sub-resource endpoint (portfolio_id comes from URL)."""
    start: str = Field(default="2020-01-01", description="Backtest start date")
    end: str = Field(default="2026-03-28", description="Backtest end date")
    initial_capital: float = Field(default=1000000, ge=1000, description="Starting capital")


@app.post("/portfolios", tags=["Portfolios"], status_code=201)
async def create_portfolio(body: PortfolioCreate, _: str = Depends(verify_api_key)):
    """Create a new portfolio."""
    config = body.model_dump(mode="json", exclude_none=True)
    # Strip capital/backtest from inline strategy configs — capital is a deploy/backtest-time param
    for s in config.get("sleeves", []):
        sc = s.get("strategy_config")
        if sc and isinstance(sc, dict):
            sc.get("sizing", {}).pop("initial_allocation", None)
            sc.pop("backtest", None)
    portfolio_id = _compute_portfolio_id(config)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_db() as conn:
        # Check for duplicate
        cur = conn.execute("SELECT portfolio_id FROM portfolios WHERE portfolio_id = ?", (portfolio_id,))
        if cur.fetchone():
            raise HTTPException(409, f"Portfolio with these parameters already exists (portfolio_id: {portfolio_id})")

        conn.execute(
            "INSERT INTO portfolios (portfolio_id, name, config, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (portfolio_id, body.name, json.dumps(config), now, now),
        )
        conn.commit()

    return {"portfolio_id": portfolio_id, "name": body.name, "config": config}


@app.get("/portfolios", tags=["Portfolios"])
async def list_portfolios(_: str = Depends(verify_api_key)):
    """List all saved portfolios."""
    with get_db() as conn:
        cur = conn.execute("SELECT portfolio_id, name, config, created_at, updated_at FROM portfolios ORDER BY created_at")
        rows = cur.fetchall()
    return [
        {
            "portfolio_id": r["portfolio_id"], "name": r["name"], "config": json.loads(r["config"]),
            "created_at": r["created_at"], "updated_at": r["updated_at"],
        }
        for r in rows
    ]


@app.post("/portfolios/{portfolio_id}/backtest", tags=["Portfolios"], status_code=202)
async def run_portfolio_backtest_subresource(
    portfolio_id: str, body: PortfolioBacktestParams,
    _: str = Depends(verify_api_key),
):
    """Run a backtest on a saved portfolio (sub-resource pattern — portfolio_id in URL).

    Preferred over the legacy POST /portfolios/backtest endpoint.
    """
    legacy_body = PortfolioBacktestRequest(
        portfolio_id=portfolio_id,
        start=body.start, end=body.end, initial_capital=body.initial_capital,
    )
    return await run_portfolio_backtest_endpoint(legacy_body, _)


@app.post("/portfolios/backtest", tags=["Portfolios"], status_code=202, deprecated=True)
async def run_portfolio_backtest_endpoint(body: PortfolioBacktestRequest, _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use POST /portfolios/{portfolio_id}/backtest instead.

    Run a portfolio backtest. Non-blocking — runs in thread pool (typically 10-60s)."""
    import traceback

    with get_db() as conn:
        cur = conn.execute("SELECT config FROM portfolios WHERE portfolio_id = ?", (body.portfolio_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Portfolio {body.portfolio_id} not found")

    config = json.loads(row["config"])
    config["backtest"] = {
        "start": body.start,
        "end": body.end,
        "initial_capital": body.initial_capital,
    }

    try:
        result = await _run_sync(_run_portfolio_bt, config)
        # Override portfolio_id with the canonical DB ID (not the config hash)
        result["portfolio_id"] = body.portfolio_id
        saved_path = _save_portfolio_results(result)

        regime_history = result.get("regime_history", [])
        regime_transitions = sum(
            1 for i in range(1, len(regime_history))
            if regime_history[i]["active_regimes"] != regime_history[i-1]["active_regimes"]
        )

        # Build sleeves detail (matches deployment response structure)
        per_sleeve = result.get("per_sleeve", [])
        sleeve_results = result.get("sleeve_results", [])
        sleeves_detail = []
        for i, ps in enumerate(per_sleeve):
            sleeve = dict(ps)
            if i < len(sleeve_results):
                sr = sleeve_results[i]
                sleeve["metrics"] = sr.get("metrics", {})
                sleeve["open_positions"] = sr.get("open_positions", [])
                sleeve["closed_trades"] = sr.get("closed_trades", [])
                trades = sr.get("trades", [])
                sleeve["trades"] = trades
            sleeves_detail.append(sleeve)

        # Persist trades per sleeve to unified table
        try:
            from deploy_engine import persist_trades, persist_sleeves
            run_id_str = saved_path.stem
            for sleeve in sleeves_detail:
                sleeve_trades = sleeve.get("trades", [])
                if sleeve_trades:
                    persist_trades("backtest", run_id_str, sleeve_trades,
                                   deployment_type="portfolio",
                                   sleeve_label=sleeve.get("label"))
            # Persist sleeve-level data
            persist_sleeves("backtest", run_id_str, result,
                            portfolio_id=body.portfolio_id)
        except Exception:
            pass  # non-critical

        return _sanitize_floats({
            "portfolio_id": body.portfolio_id,
            "run_id": saved_path.stem,
            "status": "completed",
            "start_date": body.start,
            "end_date": body.end,
            "initial_capital": body.initial_capital,
            "metrics": result.get("metrics", {}),
            "per_sleeve": per_sleeve,
            "sleeves": sleeves_detail,
            "regime_transitions": regime_transitions,
            "nav_history": result.get("combined_nav_history", []),
            "regime_history": regime_history,
            "allocation_profile_history": result.get("allocation_profile_history", []),
            "benchmark": result.get("benchmark", {}),               # legacy: primary
            "benchmark_market": result.get("benchmark_market"),     # SPY time series
            "benchmark_sector": result.get("benchmark_sector"),     # sector ETF (or None)
        })
    except ValueError as e:
        raise HTTPException(400, f"Invalid portfolio config: {e}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Portfolio backtest failed: {e}")


@app.get("/portfolios/backtest/results", tags=["Portfolios"], deprecated=True)
async def list_portfolio_backtest_results(
    portfolio_id: Optional[str] = Query(None, description="Filter by portfolio ID"),
    limit: int = Query(50, ge=1, le=500),
    _: str = Depends(verify_api_key),
):
    """List portfolio backtest results. Optional ?portfolio_id= filter."""
    from portfolio_engine import _ensure_portfolio_backtest_table
    _ensure_portfolio_backtest_table()

    with get_db() as conn:
        if portfolio_id:
            rows = conn.execute(
                "SELECT * FROM portfolio_backtest_runs WHERE portfolio_id = ? ORDER BY created_at DESC LIMIT ?",
                (portfolio_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM portfolio_backtest_runs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()

    results = []
    for row in rows:
        r = dict(row)
        per_sleeve = json.loads(r.pop("per_sleeve_json", "[]") or "[]")
        r.pop("config_json", None)
        r.pop("results_path", None)
        metrics = {k: r[k] for k in [
            "initial_capital", "final_nav", "total_return_pct", "annualized_return_pct",
            "annualized_volatility_pct", "max_drawdown_pct", "max_drawdown_date",
            "sharpe_ratio", "sortino_ratio", "calmar_ratio", "profit_factor",
            "total_entries", "closed_trades", "wins", "losses", "win_rate_pct",
            "avg_holding_days", "utilization_pct", "trading_days",
            "benchmark_return_pct", "alpha_ann_pct",
        ] if k in r}
        results.append(_sanitize_floats({
            "run_id": r["run_id"],
            "portfolio_id": r["portfolio_id"],
            "portfolio_name": r["portfolio_name"],
            "created_at": r["created_at"],
            "start_date": r["start_date"],
            "end_date": r["end_date"],
            "regime_transitions": r.get("regime_transitions"),
            "num_sleeves": r.get("num_sleeves"),
            "metrics": metrics,
            "per_sleeve": per_sleeve,
        }))
    return results


@app.get("/portfolios/backtest/results/{run_id}", tags=["Portfolios"], deprecated=True)
async def get_portfolio_backtest_result(run_id: str, _: str = Depends(verify_api_key)):
    """Get full portfolio backtest results including trades, NAV history, and regime history."""
    from portfolio_engine import _ensure_portfolio_backtest_table
    _ensure_portfolio_backtest_table()

    with get_db() as conn:
        row = conn.execute(
            "SELECT results_path FROM portfolio_backtest_runs WHERE run_id = ?", (run_id,)
        ).fetchone()

    if not row:
        raise HTTPException(404, f"Portfolio backtest run {run_id} not found")

    path = Path(row["results_path"]) if row["results_path"] else None
    if not path or not path.exists():
        # Fallback: try legacy filename
        path = WORKSPACE / "backtest" / "portfolio_results" / f"{run_id}.json"
    if not path.exists():
        raise HTTPException(404, f"Portfolio backtest result file not found for {run_id}")

    data = json.loads(path.read_text())

    # Enrich with sleeves detail (trade-level data) if not already present
    if "sleeves" not in data:
        per_sleeve = data.get("per_sleeve", [])
        sleeve_results = data.get("sleeve_results", [])
        sleeves_detail = []
        for i, ps in enumerate(per_sleeve):
            sleeve = dict(ps)
            if i < len(sleeve_results):
                sr = sleeve_results[i]
                sleeve["metrics"] = sr.get("metrics", {})
                sleeve["open_positions"] = sr.get("open_positions", [])
                sleeve["closed_trades"] = sr.get("closed_trades", [])
                trades = sr.get("trades", [])
                sleeve["trades"] = trades
            sleeves_detail.append(sleeve)
        data["sleeves"] = sleeves_detail

    # Ensure start_date/end_date at top level
    if "start_date" not in data:
        bt = data.get("config", {}).get("backtest", {})
        data["start_date"] = bt.get("start", "")
        data["end_date"] = bt.get("end", "")
        data["initial_capital"] = bt.get("initial_capital", 0)

    return _sanitize_floats(data)


# ---------------------------------------------------------------------------
# Unified Backtests — queryable history across portfolios and strategies
# ---------------------------------------------------------------------------

@app.get("/backtests", tags=["Backtests"])
async def list_backtests_unified(
    portfolio_id: Optional[str] = Query(None, description="Filter by portfolio ID (portfolio backtests only)"),
    type: Optional[str] = Query(None, description="Filter by type: 'portfolio' or 'strategy'"),
    limit: int = Query(50, ge=1, le=500),
    _: str = Depends(verify_api_key),
):
    """List backtest runs across portfolios and strategies.

    Filters: ?portfolio_id=, ?type=portfolio|strategy. Default returns most recent first.
    """
    from portfolio_engine import _ensure_portfolio_backtest_table
    _ensure_portfolio_backtest_table()

    results = []
    with get_db() as conn:
        # Portfolio backtests
        if type != "strategy":
            if portfolio_id:
                rows = conn.execute(
                    "SELECT * FROM portfolio_backtest_runs WHERE portfolio_id = ? ORDER BY created_at DESC LIMIT ?",
                    (portfolio_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM portfolio_backtest_runs ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            for row in rows:
                r = dict(row)
                metrics = {k: r[k] for k in [
                    "initial_capital", "final_nav", "total_return_pct", "annualized_return_pct",
                    "annualized_volatility_pct", "max_drawdown_pct", "sharpe_ratio",
                    "alpha_ann_pct", "win_rate_pct", "profit_factor",
                ] if k in r and r[k] is not None}
                results.append(_sanitize_floats({
                    "run_id": r["run_id"],
                    "type": "portfolio",
                    "portfolio_id": r.get("portfolio_id"),
                    "name": r.get("portfolio_name"),
                    "created_at": r["created_at"],
                    "start_date": r["start_date"],
                    "end_date": r["end_date"],
                    "metrics": metrics,
                }))

        # Strategy backtests (only if no portfolio_id filter — strategies aren't tied to portfolio_id)
        if type != "portfolio" and not portfolio_id:
            rows = conn.execute(
                "SELECT run_id, strategy_id, strategy_name, type, name, created_at, start_date, end_date, "
                "initial_capital, final_nav, total_return, ann_return, max_drawdown, "
                "sharpe, alpha, win_rate, profit_factor "
                "FROM backtest_runs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            for row in rows:
                r = dict(row)
                if r.get("type") == "portfolio":
                    continue  # already covered above
                metrics = {
                    "initial_capital": r.get("initial_capital"),
                    "final_nav": r.get("final_nav"),
                    "total_return_pct": r.get("total_return"),
                    "annualized_return_pct": r.get("ann_return"),
                    "max_drawdown_pct": r.get("max_drawdown"),
                    "sharpe_ratio": r.get("sharpe"),
                    "alpha_ann_pct": r.get("alpha"),
                    "win_rate_pct": r.get("win_rate"),
                    "profit_factor": r.get("profit_factor"),
                }
                metrics = {k: v for k, v in metrics.items() if v is not None}
                results.append(_sanitize_floats({
                    "run_id": r["run_id"],
                    "type": "strategy",
                    "strategy_id": r.get("strategy_id"),
                    "name": r.get("strategy_name") or r.get("name"),
                    "created_at": r["created_at"],
                    "start_date": r["start_date"],
                    "end_date": r["end_date"],
                    "metrics": metrics,
                }))

    # Sort merged results by created_at descending and apply limit
    results.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return {"total": len(results[:limit]), "data": results[:limit]}


@app.get("/backtests/{run_id}", tags=["Backtests"])
async def get_backtest_unified(run_id: str, _: str = Depends(verify_api_key)):
    """Get full backtest result by run_id. Auto-detects portfolio vs strategy backtest."""
    from portfolio_engine import _ensure_portfolio_backtest_table
    _ensure_portfolio_backtest_table()

    # Try portfolio backtest first
    with get_db() as conn:
        prow = conn.execute(
            "SELECT 1 FROM portfolio_backtest_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
    if prow:
        return await get_portfolio_backtest_result(run_id, _)

    # Fallback to strategy backtest
    return await get_backtest_run(run_id, _)


# ---------------------------------------------------------------------------
# Portfolio Deployments
# ---------------------------------------------------------------------------
from deploy_engine import (
    deploy_portfolio as _deploy_portfolio,
    evaluate_portfolio_one as _eval_portfolio_one,
    list_portfolio_deployments as _list_portfolio_deploys,
    get_portfolio_deployment as _get_portfolio_deploy,
    stop_portfolio as _stop_portfolio,
    pause_portfolio as _pause_portfolio,
    resume_portfolio as _resume_portfolio,
)


class PortfolioDeployRequest(_BM):
    portfolio_id: str = Field(description="Portfolio ID to deploy")
    start_date: str = Field(description="Start date (YYYY-MM-DD)")
    initial_capital: float = Field(default=1000000, ge=1000, description="Starting capital")
    name: str | None = Field(default=None, description="Override portfolio name")


class PortfolioDeployParams(_BM):
    """Deploy params for the sub-resource endpoint (portfolio_id comes from URL)."""
    start_date: str = Field(description="Start date (YYYY-MM-DD)")
    initial_capital: float = Field(default=1000000, ge=1000, description="Starting capital")
    name: str | None = Field(default=None, description="Override portfolio name")


@app.post("/portfolios/{portfolio_id}/deploy", tags=["Portfolio Deployments"], status_code=201)
async def deploy_portfolio_subresource(
    portfolio_id: str, body: PortfolioDeployParams,
    _: str = Depends(verify_api_key),
):
    """Deploy a saved portfolio for live paper-trading (sub-resource pattern — portfolio_id in URL).

    Preferred over the legacy POST /portfolios/deploy endpoint.
    """
    legacy_body = PortfolioDeployRequest(
        portfolio_id=portfolio_id,
        start_date=body.start_date,
        initial_capital=body.initial_capital,
        name=body.name,
    )
    return await deploy_portfolio_endpoint(legacy_body, _)


@app.post("/portfolios/deploy", tags=["Portfolio Deployments"], status_code=201, deprecated=True)
async def deploy_portfolio_endpoint(body: PortfolioDeployRequest, _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use POST /portfolios/{portfolio_id}/deploy instead.

    Deploy a portfolio for live paper-trading."""
    import traceback

    with get_db() as conn:
        cur = conn.execute("SELECT config FROM portfolios WHERE portfolio_id = ?", (body.portfolio_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Portfolio {body.portfolio_id} not found")

    config = json.loads(row["config"])
    config["portfolio_id"] = body.portfolio_id

    try:
        # Pass portfolio_id so deployment records the FK for lineage
        result = await _run_sync(_deploy_portfolio, config, body.start_date, body.initial_capital, body.name, body.portfolio_id)
        return result
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Portfolio deploy failed: {e}")


@app.get("/portfolios/deployments", tags=["Portfolio Deployments"], deprecated=True)
async def list_portfolio_deployments_endpoint(
    include_stopped: bool = Query(False),
    portfolio_id: str = Query(None),
    _: str = Depends(verify_api_key),
):
    """List portfolio deployments. Optional ?portfolio_id= filter."""
    deployments = _list_portfolio_deploys(include_stopped=include_stopped, portfolio_id=portfolio_id)
    results = []
    for d in deployments:
        # Parse JSON fields
        for field in ("active_regimes", "sleeve_summary"):
            if d.get(field) and isinstance(d[field], str):
                try:
                    d[field] = json.loads(d[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        results.append(d)
    return _sanitize_floats(results)


@app.get("/portfolios/deployments/{deploy_id}", tags=["Portfolio Deployments"], deprecated=True)
async def get_portfolio_deployment_endpoint(deploy_id: str, _: str = Depends(verify_api_key)):
    """Get full portfolio deployment state."""
    d = _get_portfolio_deploy(deploy_id)
    if not d:
        raise HTTPException(404, f"Portfolio deployment {deploy_id} not found")
    return _sanitize_floats(d)


@app.post("/portfolios/deployments/{deploy_id}/evaluate", tags=["Portfolio Deployments"], deprecated=True)
async def evaluate_portfolio_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Re-evaluate a portfolio deployment (re-runs backtest to today)."""
    import traceback
    try:
        result = await _run_sync(_eval_portfolio_one, deploy_id)
        if result is None:
            raise HTTPException(404, f"Portfolio deployment {deploy_id} not found or not active")
        metrics = result.get("metrics", {})
        return _sanitize_floats({
            "deploy_id": deploy_id,
            "status": "evaluated",
            "metrics": metrics,
            "per_sleeve": result.get("per_sleeve", []),
            "active_regimes": result.get("regime_history", [{}])[-1].get("active_regimes", []) if result.get("regime_history") else [],
        })
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Evaluation failed: {e}")


@app.post("/portfolios/deployments/{deploy_id}/stop", tags=["Portfolio Deployments"], deprecated=True)
async def stop_portfolio_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Stop a portfolio deployment."""
    _stop_portfolio(deploy_id)
    return {"deploy_id": deploy_id, "status": "stopped"}


@app.delete("/portfolios/deployments/{deploy_id}", tags=["Portfolio Deployments"], deprecated=True)
async def delete_portfolio_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Delete a portfolio deployment and all related data (sleeves, trades, alerts)."""
    with get_db() as conn:
        row = conn.execute("SELECT id, status FROM deployments WHERE id = ?", (deploy_id,)).fetchone()
        if not row:
            raise HTTPException(404, f"Deployment '{deploy_id}' not found")
        if row["status"] == "active":
            raise HTTPException(409, "Cannot delete an active deployment. Stop it first.")
        # Cascade delete related data
        alert_ids = [r[0] for r in conn.execute(
            "SELECT id FROM trade_alerts WHERE deployment_id = ?", (deploy_id,)).fetchall()]
        if alert_ids:
            placeholders = ",".join("?" * len(alert_ids))
            conn.execute(f"DELETE FROM trade_executions WHERE alert_id IN ({placeholders})", alert_ids)
        conn.execute("DELETE FROM trade_alerts WHERE deployment_id = ?", (deploy_id,))
        conn.execute("DELETE FROM trades WHERE source_id = ?", (deploy_id,))
        conn.execute("DELETE FROM sleeves WHERE deployment_id = ?", (deploy_id,))
        conn.execute("DELETE FROM deployments WHERE id = ?", (deploy_id,))
        conn.commit()
    return {"deleted": deploy_id}


@app.post("/portfolios/deployments/{deploy_id}/pause", tags=["Portfolio Deployments"], deprecated=True)
async def pause_portfolio_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Pause a portfolio deployment."""
    _pause_portfolio(deploy_id)
    return {"deploy_id": deploy_id, "status": "paused"}


@app.post("/portfolios/deployments/{deploy_id}/resume", tags=["Portfolio Deployments"], deprecated=True)
async def resume_portfolio_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """Resume a portfolio deployment."""
    _resume_portfolio(deploy_id)
    return {"deploy_id": deploy_id, "status": "active"}


# ---------------------------------------------------------------------------
# Portfolio Alerts
# ---------------------------------------------------------------------------
@app.post("/portfolios/deployments/{deploy_id}/alerts/enable", tags=["Portfolio Alerts"], deprecated=True)
async def enable_portfolio_alerts(deploy_id: str, _: str = Depends(verify_api_key)):
    """Enable alert mode for a portfolio deployment. Generates daily BUY/SELL alerts from all sleeves."""
    from deploy_engine import set_portfolio_alert_mode
    result = set_portfolio_alert_mode(deploy_id, True)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@app.post("/portfolios/deployments/{deploy_id}/alerts/disable", tags=["Portfolio Alerts"], deprecated=True)
async def disable_portfolio_alerts(deploy_id: str, _: str = Depends(verify_api_key)):
    """Disable alert mode for a portfolio deployment."""
    from deploy_engine import set_portfolio_alert_mode
    result = set_portfolio_alert_mode(deploy_id, False)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@app.get("/portfolios/deployments/{deploy_id}/alerts", tags=["Portfolio Alerts"], deprecated=True)
async def get_portfolio_alerts(deploy_id: str,
                               date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
                               status: Optional[str] = Query(None, description="Filter: pending, executed, skipped"),
                               _: str = Depends(verify_api_key)):
    """Get trade alerts for a portfolio deployment."""
    from deploy_engine import get_alerts
    alerts = get_alerts(deploy_id=deploy_id, date=date, status=status)
    return {"total": len(alerts), "data": [_sanitize_floats(a) for a in alerts]}


@app.get("/portfolios/deployments/{deploy_id}/alerts/summary", tags=["Portfolio Alerts"], deprecated=True)
async def get_portfolio_execution_summary(deploy_id: str, _: str = Depends(verify_api_key)):
    """Get execution tracking summary for a portfolio deployment: follow-through rate, avg slippage, paper vs real."""
    from deploy_engine import get_execution_summary as _summary
    return _sanitize_floats(_summary(deploy_id))


@app.get("/portfolios/alerts/today", tags=["Portfolio Alerts"])
async def get_portfolio_alerts_today(_: str = Depends(verify_api_key)):
    """Get all pending trade alerts for today across all alert-enabled portfolio deployments."""
    from deploy_engine import get_alerts
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, portfolio_name FROM portfolio_deployments WHERE alert_mode = 1 AND status = 'active'"
        ).fetchall()
    all_alerts = []
    by_deploy = {}
    for row in rows:
        deploy_id, name = row["id"], row["portfolio_name"]
        alerts = get_alerts(deploy_id=deploy_id, date=today, limit=200)
        for a in alerts:
            a["portfolio_name"] = name
            all_alerts.append(a)
            if deploy_id not in by_deploy:
                by_deploy[deploy_id] = {
                    "deployment_id": deploy_id,
                    "portfolio_name": name,
                    "alerts": [],
                }
            by_deploy[deploy_id]["alerts"].append(_sanitize_floats(a))
    return {
        "date": today,
        "total_alerts": len(all_alerts),
        "pending": sum(1 for a in all_alerts if a.get("execution_status") == "pending"),
        "deployments": list(by_deploy.values()),
    }


@app.get("/portfolios/{portfolio_id}", tags=["Portfolios"])
async def get_portfolio(portfolio_id: str, _: str = Depends(verify_api_key)):
    """Get a single portfolio with lineage: source experiments and active deployments."""
    with get_db() as conn:
        cur = conn.execute(
            "SELECT portfolio_id, name, config, created_at, updated_at FROM portfolios WHERE portfolio_id = ?",
            (portfolio_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, f"Portfolio {portfolio_id} not found")

        # Linked experiments (research history)
        experiments = [dict(r) for r in conn.execute(
            """SELECT id, run_id, iteration, target_metric, target_value, decision,
                      sharpe_ratio, alpha_ann_pct, total_return_pct, max_drawdown_pct,
                      annualized_volatility_pct, created_at
               FROM experiments WHERE portfolio_id = ? ORDER BY created_at DESC""",
            (portfolio_id,),
        ).fetchall()]

        # Linked deployments (live trading)
        deployments = [dict(r) for r in conn.execute(
            """SELECT id, type, name, status, start_date, initial_capital,
                      last_nav, last_return_pct, last_sharpe_ratio, last_evaluated, created_at
               FROM deployments WHERE portfolio_id = ? ORDER BY created_at DESC""",
            (portfolio_id,),
        ).fetchall()]

    return {
        "portfolio_id": row["portfolio_id"],
        "name": row["name"],
        "config": json.loads(row["config"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "experiments": experiments,
        "deployments": deployments,
    }


@app.put("/portfolios/{portfolio_id}", tags=["Portfolios"])
async def update_portfolio(portfolio_id: str, body: PortfolioUpdate, _: str = Depends(verify_api_key)):
    """Update a portfolio."""
    with get_db() as conn:
        cur = conn.execute("SELECT config FROM portfolios WHERE portfolio_id = ?", (portfolio_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, f"Portfolio {portfolio_id} not found")

        config = json.loads(row["config"])
        if body.name is not None:
            config["name"] = body.name
        if body.strategies is not None:
            config["strategies"] = [s.model_dump(mode="json") for s in body.strategies]
            # Strip capital/backtest from inline strategy configs
            for s in config["strategies"]:
                if s.get("config") and isinstance(s["config"], dict):
                    s["config"].get("sizing", {}).pop("initial_allocation", None)
                    s["config"].pop("backtest", None)
        if body.regime_filter is not None:
            config["regime_filter"] = body.regime_filter
        if body.capital_flow is not None:
            config["capital_flow"] = body.capital_flow

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "UPDATE portfolios SET name = ?, config = ?, updated_at = ? WHERE portfolio_id = ?",
            (config.get("name", ""), json.dumps(config), now, portfolio_id),
        )
        conn.commit()
    return {"portfolio_id": portfolio_id, "config": config}


@app.delete("/portfolios/{portfolio_id}", tags=["Portfolios"])
async def delete_portfolio(portfolio_id: str, _: str = Depends(verify_api_key)):
    """Delete a portfolio."""
    with get_db() as conn:
        cur = conn.execute("DELETE FROM portfolios WHERE portfolio_id = ?", (portfolio_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, f"Portfolio {portfolio_id} not found")
        conn.commit()
    return {"deleted": portfolio_id}


# ---------------------------------------------------------------------------
# Sleeves
# ---------------------------------------------------------------------------
from deploy_engine import persist_sleeves as _persist_sleeves


@app.get("/sleeves", tags=["Sleeves"])
async def list_sleeves(
    portfolio_id: str = Query(None, description="Filter by portfolio ID"),
    deployment_id: str = Query(None, description="Filter by deployment ID"),
    source_type: str = Query(None, description="Filter: 'backtest' or 'deployment'"),
    strategy_id: str = Query(None, description="Filter by strategy ID used in sleeve"),
    limit: int = Query(50, ge=1, le=500),
    _: str = Depends(verify_api_key),
):
    """List sleeves with optional filters. Sortable by return, Sharpe, etc."""
    from deploy_engine import _get_portfolio_db
    conn = _get_portfolio_db()
    try:
        clauses = []
        params = []
        if portfolio_id:
            clauses.append("portfolio_id = ?")
            params.append(portfolio_id)
        if deployment_id:
            clauses.append("deployment_id = ?")
            params.append(deployment_id)
        if source_type:
            clauses.append("source_type = ?")
            params.append(source_type)
        if strategy_id:
            clauses.append("strategy_id = ?")
            params.append(strategy_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        rows = conn.execute(
            f"SELECT * FROM sleeves{where} ORDER BY updated_at DESC LIMIT ?", params
        ).fetchall()
    finally:
        conn.close()
    result = []
    for r in rows:
        d = dict(r)
        for field in ("regime_gate",):
            if d.get(field) and isinstance(d[field], str):
                try:
                    d[field] = json.loads(d[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        result.append(d)
    return _sanitize_floats({"total": len(result), "data": result})


@app.get("/sleeves/{sleeve_id}", tags=["Sleeves"])
async def get_sleeve(sleeve_id: str, _: str = Depends(verify_api_key)):
    """Get a single sleeve by ID."""
    from deploy_engine import _get_portfolio_db
    conn = _get_portfolio_db()
    try:
        row = conn.execute("SELECT * FROM sleeves WHERE sleeve_id = ?", (sleeve_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(404, f"Sleeve {sleeve_id} not found")
    d = dict(row)
    if d.get("regime_gate") and isinstance(d["regime_gate"], str):
        try:
            d["regime_gate"] = json.loads(d["regime_gate"])
        except (json.JSONDecodeError, TypeError):
            pass
    return _sanitize_floats(d)


@app.get("/sleeves/{sleeve_id}/trades", tags=["Sleeves"])
async def get_sleeve_trades(
    sleeve_id: str,
    action: str = Query(None, description="Filter: 'BUY' or 'SELL'"),
    limit: int = Query(200, ge=1, le=1000),
    _: str = Depends(verify_api_key),
):
    """Get all trades for a specific sleeve."""
    from deploy_engine import _get_portfolio_db, get_db
    # Look up sleeve to get source_id and label
    pconn = _get_portfolio_db()
    try:
        sleeve = pconn.execute("SELECT * FROM sleeves WHERE sleeve_id = ?", (sleeve_id,)).fetchone()
    finally:
        pconn.close()
    if not sleeve:
        raise HTTPException(404, f"Sleeve {sleeve_id} not found")

    conn = get_db()
    try:
        clauses = ["source_id = ?", "sleeve_label = ?"]
        params = [sleeve["source_id"], sleeve["label"]]
        if action:
            clauses.append("action = ?")
            params.append(action.upper())
        params.append(limit)
        where = " AND ".join(clauses)
        rows = conn.execute(
            f"SELECT * FROM trades WHERE {where} ORDER BY date DESC LIMIT ?", params
        ).fetchall()
    finally:
        conn.close()
    trades = []
    for r in rows:
        d = dict(r)
        if d.get("signal_detail") and isinstance(d["signal_detail"], str):
            try:
                d["signal_detail"] = json.loads(d["signal_detail"])
            except (json.JSONDecodeError, TypeError):
                pass
        trades.append(d)
    return _sanitize_floats({"total": len(trades), "sleeve": dict(sleeve), "data": trades})


@app.get("/portfolios/{portfolio_id}/sleeves", tags=["Sleeves"])
async def get_portfolio_sleeves(portfolio_id: str, _: str = Depends(verify_api_key)):
    """Get all sleeves for a portfolio (across backtests and deployments)."""
    from deploy_engine import _get_portfolio_db
    conn = _get_portfolio_db()
    try:
        rows = conn.execute(
            "SELECT * FROM sleeves WHERE portfolio_id = ? ORDER BY source_type, updated_at DESC",
            (portfolio_id,)
        ).fetchall()
    finally:
        conn.close()
    result = []
    for r in rows:
        d = dict(r)
        if d.get("regime_gate") and isinstance(d["regime_gate"], str):
            try:
                d["regime_gate"] = json.loads(d["regime_gate"])
            except (json.JSONDecodeError, TypeError):
                pass
        result.append(d)
    return _sanitize_floats({"total": len(result), "data": result})


@app.get("/deployments/{deploy_id}/sleeves", tags=["Sleeves"])
async def get_deployment_sleeves(deploy_id: str, _: str = Depends(verify_api_key)):
    """Get all sleeves for a specific deployment."""
    from deploy_engine import _get_portfolio_db
    conn = _get_portfolio_db()
    try:
        rows = conn.execute(
            "SELECT * FROM sleeves WHERE deployment_id = ? ORDER BY label",
            (deploy_id,)
        ).fetchall()
    finally:
        conn.close()
    result = []
    for r in rows:
        d = dict(r)
        if d.get("regime_gate") and isinstance(d["regime_gate"], str):
            try:
                d["regime_gate"] = json.loads(d["regime_gate"])
            except (json.JSONDecodeError, TypeError):
                pass
        result.append(d)
    return _sanitize_floats({"total": len(result), "data": result})


# ---------------------------------------------------------------------------
# Regime Deployments
# ---------------------------------------------------------------------------
from deploy_engine import (
    deploy_regime as _deploy_regime,
    evaluate_regime_one as _eval_regime_one,
    evaluate_all_regimes as _eval_all_regimes,
    list_regime_deployments as _list_regime_deploys,
    get_regime_deployment as _get_regime_deploy,
    stop_regime_deployment as _stop_regime_deploy,
    pause_regime_deployment as _pause_regime_deploy,
    resume_regime_deployment as _resume_regime_deploy,
    set_regime_alert_mode as _set_regime_alert_mode,
    get_regime_alerts as _get_regime_alerts,
)


class RegimeDeployRequest(_BM):
    regime_id: str = Field(description="Regime ID to deploy for live monitoring")
    name: str | None = Field(default=None, description="Override regime name")


class RegimeDeployParams(_BM):
    """Deploy params for the sub-resource endpoint (regime_id comes from URL)."""
    name: str | None = Field(default=None, description="Override regime name")


# ---------------------------------------------------------------------------
# Regime Deployments — Preferred routes (sub-resource on /regimes/{id}, instances at /regime-deployments/)
# ---------------------------------------------------------------------------

@app.post("/regimes/{regime_id}/deploy", tags=["Regime Deployments"], status_code=201)
async def deploy_regime_subresource(
    regime_id: str, body: RegimeDeployParams = RegimeDeployParams(),
    _: str = Depends(verify_api_key),
):
    """Deploy a regime for live monitoring (sub-resource pattern — regime_id in URL).

    Preferred over the legacy POST /regimes/deploy endpoint.
    """
    try:
        return _deploy_regime(regime_id, body.name)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, f"Regime deploy failed: {e}")


@app.get("/regime-deployments", tags=["Regime Deployments"])
async def list_regime_deployments_unified(
    include_stopped: bool = Query(False),
    _: str = Depends(verify_api_key),
):
    """List regime deployments. Preferred over /regimes/deployments."""
    return _sanitize_floats(_list_regime_deploys(include_stopped=include_stopped))


@app.get("/regime-deployments/{deploy_id}", tags=["Regime Deployments"])
async def get_regime_deployment_unified(
    deploy_id: str,
    include_history: bool = Query(False, description="Include daily state history"),
    _: str = Depends(verify_api_key),
):
    """Get regime deployment detail. Preferred over /regimes/deployments/{id}."""
    d = _get_regime_deploy(deploy_id, include_history=include_history)
    if not d:
        raise HTTPException(404, f"Regime deployment {deploy_id} not found")
    return _sanitize_floats(d)


@app.post("/regime-deployments/{deploy_id}/evaluate", tags=["Regime Deployments"])
async def evaluate_regime_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Force re-evaluation. Preferred over /regimes/deployments/{id}/evaluate."""
    result = _eval_regime_one(deploy_id)
    if not result:
        raise HTTPException(404, f"Regime deployment {deploy_id} not found or not active")
    return result


@app.post("/regime-deployments/{deploy_id}/stop", tags=["Regime Deployments"])
async def stop_regime_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    _stop_regime_deploy(deploy_id)
    return {"deploy_id": deploy_id, "status": "stopped"}


@app.post("/regime-deployments/{deploy_id}/pause", tags=["Regime Deployments"])
async def pause_regime_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    _pause_regime_deploy(deploy_id)
    return {"deploy_id": deploy_id, "status": "paused"}


@app.post("/regime-deployments/{deploy_id}/resume", tags=["Regime Deployments"])
async def resume_regime_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    _resume_regime_deploy(deploy_id)
    return {"deploy_id": deploy_id, "status": "active"}


@app.delete("/regime-deployments/{deploy_id}", tags=["Regime Deployments"])
async def delete_regime_deployment_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Delete a stopped regime deployment and its history/alerts."""
    with get_db() as conn:
        row = conn.execute("SELECT id, status FROM regime_deployments WHERE id = ?", (deploy_id,)).fetchone()
        if not row:
            raise HTTPException(404, f"Regime deployment '{deploy_id}' not found")
        if row["status"] == "active":
            raise HTTPException(409, "Cannot delete an active regime deployment. Stop it first.")
        conn.execute("DELETE FROM regime_alerts WHERE deployment_id = ?", (deploy_id,))
        conn.execute("DELETE FROM regime_state_history WHERE deployment_id = ?", (deploy_id,))
        conn.execute("DELETE FROM regime_deployments WHERE id = ?", (deploy_id,))
        conn.commit()
    return {"deleted": deploy_id}


@app.post("/regime-deployments/{deploy_id}/alerts/enable", tags=["Regime Alerts"])
async def enable_regime_alerts_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Enable transition alerts."""
    result = _set_regime_alert_mode(deploy_id, True)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@app.post("/regime-deployments/{deploy_id}/alerts/disable", tags=["Regime Alerts"])
async def disable_regime_alerts_unified(deploy_id: str, _: str = Depends(verify_api_key)):
    """Disable transition alerts."""
    result = _set_regime_alert_mode(deploy_id, False)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@app.get("/regime-deployments/{deploy_id}/alerts", tags=["Regime Alerts"])
async def get_regime_alerts_unified(deploy_id: str,
                                     date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
                                     _: str = Depends(verify_api_key)):
    """Get transition alerts."""
    alerts = _get_regime_alerts(deploy_id=deploy_id, date=date)
    return {"total": len(alerts), "data": [_sanitize_floats(a) for a in alerts]}


# ---------------------------------------------------------------------------
# Regime Deployments — Legacy routes (deprecated)
# ---------------------------------------------------------------------------

@app.post("/regimes/deploy", tags=["Regime Deployments"], status_code=201, deprecated=True)
async def deploy_regime_endpoint(body: RegimeDeployRequest, _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use POST /regimes/{regime_id}/deploy instead."""
    try:
        result = _deploy_regime(body.regime_id, body.name)
        return result
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, f"Regime deploy failed: {e}")


@app.get("/regimes/deployments", tags=["Regime Deployments"], deprecated=True)
async def list_regime_deployments_endpoint(
    include_stopped: bool = Query(False),
    _: str = Depends(verify_api_key),
):
    """[DEPRECATED] Use GET /regime-deployments instead."""
    deployments = _list_regime_deploys(include_stopped=include_stopped)
    return _sanitize_floats(deployments)


@app.get("/regimes/deployments/{deploy_id}", tags=["Regime Deployments"], deprecated=True)
async def get_regime_deployment_endpoint(
    deploy_id: str,
    include_history: bool = Query(False, description="Include daily state history for charts"),
    _: str = Depends(verify_api_key),
):
    """[DEPRECATED] Use GET /regime-deployments/{deploy_id} instead."""
    d = _get_regime_deploy(deploy_id, include_history=include_history)
    if not d:
        raise HTTPException(404, f"Regime deployment {deploy_id} not found")
    return _sanitize_floats(d)


@app.post("/regimes/deployments/{deploy_id}/evaluate", tags=["Regime Deployments"], deprecated=True)
async def evaluate_regime_deployment(deploy_id: str, _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use POST /regime-deployments/{deploy_id}/evaluate instead."""
    result = _eval_regime_one(deploy_id)
    if not result:
        raise HTTPException(404, f"Regime deployment {deploy_id} not found or not active")
    return result


@app.post("/regimes/deployments/{deploy_id}/stop", tags=["Regime Deployments"], deprecated=True)
async def stop_regime_deployment_endpoint(deploy_id: str, _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use POST /regime-deployments/{deploy_id}/stop instead."""
    _stop_regime_deploy(deploy_id)
    return {"deploy_id": deploy_id, "status": "stopped"}


@app.delete("/regimes/deployments/{deploy_id}", tags=["Regime Deployments"], deprecated=True)
async def delete_regime_deployment_endpoint(deploy_id: str, _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use DELETE /regime-deployments/{deploy_id} instead."""
    with get_db() as conn:
        row = conn.execute("SELECT id, status FROM regime_deployments WHERE id = ?", (deploy_id,)).fetchone()
        if not row:
            raise HTTPException(404, f"Regime deployment '{deploy_id}' not found")
        if row["status"] == "active":
            raise HTTPException(409, "Cannot delete an active regime deployment. Stop it first.")
        conn.execute("DELETE FROM regime_alerts WHERE deployment_id = ?", (deploy_id,))
        conn.execute("DELETE FROM regime_state_history WHERE deployment_id = ?", (deploy_id,))
        conn.execute("DELETE FROM regime_deployments WHERE id = ?", (deploy_id,))
        conn.commit()
    return {"deleted": deploy_id}


@app.post("/regimes/deployments/{deploy_id}/pause", tags=["Regime Deployments"], deprecated=True)
async def pause_regime_deployment_endpoint(deploy_id: str, _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use POST /regime-deployments/{deploy_id}/pause instead."""
    _pause_regime_deploy(deploy_id)
    return {"deploy_id": deploy_id, "status": "paused"}


@app.post("/regimes/deployments/{deploy_id}/resume", tags=["Regime Deployments"], deprecated=True)
async def resume_regime_deployment_endpoint(deploy_id: str, _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use POST /regime-deployments/{deploy_id}/resume instead."""
    _resume_regime_deploy(deploy_id)
    return {"deploy_id": deploy_id, "status": "active"}


@app.post("/regimes/deployments/{deploy_id}/alerts/enable", tags=["Regime Alerts"], deprecated=True)
async def enable_regime_alerts(deploy_id: str, _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use POST /regime-deployments/{deploy_id}/alerts/enable instead."""
    result = _set_regime_alert_mode(deploy_id, True)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@app.post("/regimes/deployments/{deploy_id}/alerts/disable", tags=["Regime Alerts"], deprecated=True)
async def disable_regime_alerts(deploy_id: str, _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use POST /regime-deployments/{deploy_id}/alerts/disable instead."""
    result = _set_regime_alert_mode(deploy_id, False)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@app.get("/regimes/deployments/{deploy_id}/alerts", tags=["Regime Alerts"], deprecated=True)
async def get_regime_alerts_endpoint(deploy_id: str,
                                     date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
                                     _: str = Depends(verify_api_key)):
    """[DEPRECATED] Use GET /regime-deployments/{deploy_id}/alerts instead."""
    alerts = _get_regime_alerts(deploy_id=deploy_id, date=date)
    return {"total": len(alerts), "data": [_sanitize_floats(a) for a in alerts]}


@app.get("/regimes/alerts/today", tags=["Regime Alerts"])
async def get_regime_alerts_today(_: str = Depends(verify_api_key)):
    """Get all regime transition alerts for today."""
    today = datetime.now().strftime("%Y-%m-%d")
    alerts = _get_regime_alerts(date=today)
    return {"total": len(alerts), "data": [_sanitize_floats(a) for a in alerts]}

@app.get("/regimes/{regime_id}", tags=["Regimes"])
async def get_regime(regime_id: str, _: str = Depends(verify_api_key)):
    """Get a single regime by ID."""
    with get_db() as conn:
        cur = conn.execute(
            "SELECT regime_id, name, config, created_at, updated_at FROM regimes WHERE regime_id = ?",
            (regime_id,),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Regime {regime_id} not found")
    return {
        "regime_id": row["regime_id"], "name": row["name"], "config": json.loads(row["config"]),
        "created_at": row["created_at"], "updated_at": row["updated_at"],
    }


@app.put("/regimes/{regime_id}", tags=["Regimes"])
async def update_regime(regime_id: str, body: RegimeUpdate, _: str = Depends(verify_api_key)):
    """Update an existing regime."""
    with get_db() as conn:
        cur = conn.execute("SELECT config FROM regimes WHERE regime_id = ?", (regime_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, f"Regime {regime_id} not found")

        config = json.loads(row["config"])
        if body.name is not None:
            config["name"] = body.name
        if body.conditions is not None:
            config["entry_conditions"] = [c.model_dump() for c in body.conditions]
        if body.logic is not None:
            config["entry_logic"] = body.logic
        if body.entry_conditions is not None:
            config["entry_conditions"] = [c.model_dump() for c in body.entry_conditions]
        if body.entry_logic is not None:
            config["entry_logic"] = body.entry_logic
        if body.exit_conditions is not None:
            config["exit_conditions"] = [c.model_dump() for c in body.exit_conditions]
        if body.exit_logic is not None:
            config["exit_logic"] = body.exit_logic
        if body.min_hold_days is not None:
            config["min_hold_days"] = body.min_hold_days
        # Clean up legacy fields
        config.pop("conditions", None)
        config.pop("logic", None)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "UPDATE regimes SET name = ?, config = ?, updated_at = ? WHERE regime_id = ?",
            (config["name"], json.dumps(config), now, regime_id),
        )
        conn.commit()
    return {"regime_id": regime_id, "config": config}


@app.delete("/regimes/{regime_id}", tags=["Regimes"])
async def delete_regime(regime_id: str, _: str = Depends(verify_api_key)):
    """Delete a regime."""
    with get_db() as conn:
        cur = conn.execute("DELETE FROM regimes WHERE regime_id = ?", (regime_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, f"Regime {regime_id} not found")
        conn.commit()
    return {"deleted": regime_id}


@app.get("/regimes/{regime_id}/evaluate", tags=["Regimes"])
async def evaluate_regime_endpoint(
    regime_id: str,
    date: Optional[str] = Query(None, description="Single date to evaluate (YYYY-MM-DD)"),
    start: Optional[str] = Query(None, description="Start date for series evaluation"),
    end: Optional[str] = Query(None, description="End date for series evaluation"),
    detail: bool = Query(False, description="Include per-condition breakdown"),
    _: str = Depends(verify_api_key),
):
    """Evaluate a regime — single date or date range (backtest)."""
    with get_db() as conn:
        cur = conn.execute("SELECT config FROM regimes WHERE regime_id = ?", (regime_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Regime {regime_id} not found")

    config = json.loads(row["config"])

    if date:
        if detail:
            return _get_regime_details(date, [config])
        active = _eval_regimes(date, [config])
        return {"date": date, "active": len(active) > 0, "regime": config["name"]}

    if start and end:
        series = _eval_regime_series(start, end, [config])
        sorted_dates = sorted(series.keys())
        active_dates = [d for d in sorted_dates if series[d]]

        # Compute transitions
        transitions = []
        prev_active = False
        for d in sorted_dates:
            is_active = bool(series[d])
            if is_active != prev_active:
                transitions.append({
                    "date": d,
                    "transition": "activated" if is_active else "deactivated",
                })
            prev_active = is_active

        # Compute activation periods (start/end/duration)
        periods = []
        period_start = None
        for d in sorted_dates:
            is_active = bool(series[d])
            if is_active and period_start is None:
                period_start = d
            elif not is_active and period_start is not None:
                periods.append({"start": period_start, "end": prev_date, "days": len([x for x in sorted_dates if period_start <= x <= prev_date and series[x]])})
                period_start = None
            prev_date = d
        if period_start is not None:
            periods.append({"start": period_start, "end": sorted_dates[-1], "days": len([x for x in sorted_dates if x >= period_start and series[x]])})

        result = {
            "regime": config["name"],
            "total_days": len(series),
            "active_days": len(active_dates),
            "pct_active": round(len(active_dates) / max(len(series), 1) * 100, 1),
            "first_active": active_dates[0] if active_dates else None,
            "last_active": active_dates[-1] if active_dates else None,
            "transitions": transitions,
            "activation_periods": periods,
            "series": {d: (config["name"] in names) for d, names in sorted(series.items())},
        }

        # If detail requested, include per-condition breakdown for first and last active date
        if detail and active_dates:
            result["detail_first_active"] = _get_regime_details(active_dates[0], [config])
            result["detail_last_active"] = _get_regime_details(active_dates[-1], [config])

        return _sanitize_floats(result)

    raise HTTPException(400, "Provide either 'date' or both 'start' and 'end'")



# ---------------------------------------------------------------------------
# Macro Data
# ---------------------------------------------------------------------------
@app.get("/macro/series", tags=["Macro"])
async def list_macro_series(api_key: str = Security(api_key_header)):
    """List all available macro series with metadata."""
    with get_market_db() as conn:
        cur = conn.execute(
            "SELECT series, COUNT(*) as rows, MIN(date) as first_date, MAX(date) as latest_date, "
            "MAX(source) as source FROM macro_indicators GROUP BY series ORDER BY series"
        )
        indicators = [
            {"series": r["series"], "rows": r["rows"], "first_date": r["first_date"],
             "latest_date": r["latest_date"], "source": r["source"], "table": "macro_indicators"}
            for r in cur.fetchall()
        ]
        cur = conn.execute(
            "SELECT series, COUNT(*) as rows, MIN(date) as first_date, MAX(date) as latest_date "
            "FROM macro_derived GROUP BY series ORDER BY series"
        )
        derived = [
            {"series": r["series"], "rows": r["rows"], "first_date": r["first_date"],
             "latest_date": r["latest_date"], "source": "derived", "table": "macro_derived"}
            for r in cur.fetchall()
        ]
    return {"indicators": indicators, "derived": derived}


@app.get("/macro/indicators", tags=["Macro"])
async def get_macro_indicators(
    series: str = Query(..., description="Comma-separated series keys (e.g. brent,vix,fed_funds)"),
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    api_key: str = Security(api_key_header),
):
    """Query macro indicator time series."""
    series_list = [s.strip() for s in series.split(",")]
    placeholders = ",".join("?" * len(series_list))
    query = f"SELECT date, series, value FROM macro_indicators WHERE series IN ({placeholders})"
    params = list(series_list)
    if start:
        query += " AND date >= ?"
        params.append(start)
    if end:
        query += " AND date <= ?"
        params.append(end)
    query += " ORDER BY date, series"
    with get_market_db() as conn:
        cur = conn.execute(query, params)
        rows = [{"date": r["date"], "series": r["series"], "value": r["value"]} for r in cur.fetchall()]
    return {"count": len(rows), "data": rows}


@app.get("/macro/indicators/latest", tags=["Macro"])
async def get_macro_latest(
    series: str = Query(..., description="Comma-separated series keys"),
    api_key: str = Security(api_key_header),
):
    """Get the latest value for each requested series."""
    series_list = [s.strip() for s in series.split(",")]
    results = {}
    with get_market_db() as conn:
        for s in series_list:
            cur = conn.execute(
                "SELECT date, value FROM macro_indicators WHERE series = ? ORDER BY date DESC LIMIT 1",
                (s,),
            )
            row = cur.fetchone()
            if row:
                results[s] = {"date": row["date"], "value": row["value"]}
            else:
                results[s] = None
    return results


@app.get("/macro/derived", tags=["Macro"])
async def get_macro_derived(
    series: str = Query(..., description="Comma-separated derived series keys"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    api_key: str = Security(api_key_header),
):
    """Query derived macro series."""
    series_list = [s.strip() for s in series.split(",")]
    placeholders = ",".join("?" * len(series_list))
    query = f"SELECT date, series, value FROM macro_derived WHERE series IN ({placeholders})"
    params = list(series_list)
    if start:
        query += " AND date >= ?"
        params.append(start)
    if end:
        query += " AND date <= ?"
        params.append(end)
    query += " ORDER BY date, series"
    with get_market_db() as conn:
        cur = conn.execute(query, params)
        rows = [{"date": r["date"], "series": r["series"], "value": r["value"]} for r in cur.fetchall()]
    return {"count": len(rows), "data": rows}


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Auto-Trader (mounted as sub-router)
# ---------------------------------------------------------------------------
try:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from auto_trader.api import router as auto_trader_router
    app.include_router(auto_trader_router, dependencies=[Depends(verify_api_key)])
except ImportError:
    pass  # auto_trader not available in this environment


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8090))
    uvicorn.run(app, host="0.0.0.0", port=port)
