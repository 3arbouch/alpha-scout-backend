#!/usr/bin/env python3
"""
AlphaScout Backfill — Full historical fetch + DB rebuild from scratch.
Rare manual use. Combines pipeline.py --backfill + build_db.py.

Usage:
  python3 backfill.py --dry-run
  python3 backfill.py --since 2015-01-01
  python3 backfill.py --since 2020-01-01 --ticker NKE
  python3 backfill.py --since 2015-01-01 --layer prices
  python3 backfill.py --concurrency 20
"""

import os, sys, json, time, asyncio, argparse, logging, math, sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://financialmodelingprep.com/stable"
API_KEY = os.environ.get("FMP_API_KEY", "")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
DB_PATH = Path(os.environ.get("DB_PATH", "/app/data/alphascout.db"))
RATE_LIMIT = 2800
DEFAULT_CONCURRENCY = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("backfill")

stats = {"started_at": None, "finished_at": None, "requests_made": 0, "requests_failed": 0,
         "tickers_processed": 0, "mode": "BACKFILL", "errors": [], "json_files_written": 0, "db_rows_inserted": 0}

# ---------------------------------------------------------------------------
# Mode config
# ---------------------------------------------------------------------------
class BackfillMode:
    def __init__(self, since="2015-01-01"):
        self.since_date = since
        now = datetime.now(timezone.utc)
        since_dt = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        years_back = max(1, (now - since_dt).days / 365.25)
        quarters_needed = int(math.ceil(years_back * 4)) + 4

        self.price_from = since
        self.quarterly_limit = quarters_needed
        self.annual_limit = int(math.ceil(years_back)) + 2
        self.quarterly_est_limit = quarters_needed
        self.grades_limit = max(200, quarters_needed * 3)
        self.earnings_limit = quarters_needed
        self.insider_limit = max(200, quarters_needed * 5)
        self.transcript_start_year = since_dt.year
        self.transcript_end_year = now.year
        self.cache_ttl_hours = 1

    def __str__(self):
        return f"BACKFILL (since={self.since_date})"

mode: BackfillMode = None

# ---------------------------------------------------------------------------
# Token-bucket rate limiter
# ---------------------------------------------------------------------------
class TokenBucket:
    def __init__(self, rate_per_minute):
        self.interval = 60.0 / rate_per_minute
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            wait = self._last + self.interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()

_bucket = None
_executor = None

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _sync_http_get(url, retries=3):
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "AlphaScout/1.0"})
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                if isinstance(data, dict) and "Error Message" in data:
                    return {"_error": data["Error Message"]}
                return data
        except HTTPError as e:
            if e.code == 429:
                time.sleep((attempt + 1) * 10)
            elif attempt < retries - 1:
                time.sleep((attempt + 1) * 2)
            else:
                return {"_http_error": e.code}
        except (URLError, TimeoutError, json.JSONDecodeError) as e:
            if attempt < retries - 1:
                time.sleep((attempt + 1) * 2)
            else:
                return {"_error": str(e)}
    return None

async def fmp_get(endpoint, params=None, retries=3):
    if not API_KEY:
        log.error("FMP_API_KEY not set")
        return None
    params = params or {}
    params["apikey"] = API_KEY
    url = f"{BASE_URL}/{endpoint}?{urlencode(params)}"
    await _bucket.acquire()
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_executor, _sync_http_get, url, retries)
    if result is None:
        stats["requests_failed"] += 1
        stats["errors"].append(f"Failed after {retries} retries: {endpoint}")
        return None
    if isinstance(result, dict) and ("_error" in result or "_http_error" in result):
        stats["requests_failed"] += 1
        stats["requests_made"] += 1
        return None
    stats["requests_made"] += 1
    return result

def fmp_get_sync(endpoint, params=None, retries=3):
    params = params or {}
    params["apikey"] = API_KEY
    url = f"{BASE_URL}/{endpoint}?{urlencode(params)}"
    time.sleep(60.0 / RATE_LIMIT)
    result = _sync_http_get(url, retries)
    if result is None or (isinstance(result, dict) and ("_error" in result or "_http_error" in result)):
        stats["requests_failed"] += 1
        stats["requests_made"] += 1
        return None
    stats["requests_made"] += 1
    return result

# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------
def save(data, *path_parts):
    if data is None:
        return
    if isinstance(data, list) and len(data) == 0:
        return
    filepath = DATA_DIR / Path(*path_parts)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    wrapped = {"_fetched_at": datetime.now(timezone.utc).isoformat(), "data": data}
    filepath.write_text(json.dumps(wrapped, indent=2))
    stats["json_files_written"] += 1

def load(*path_parts):
    filepath = DATA_DIR / Path(*path_parts)
    if not filepath.exists():
        return None
    try:
        content = json.loads(filepath.read_text())
        return content.get("data")
    except (json.JSONDecodeError, KeyError):
        return None

def cache_age_hours(*path_parts):
    filepath = DATA_DIR / Path(*path_parts)
    if not filepath.exists():
        return 9999.0
    try:
        content = json.loads(filepath.read_text())
        fetched = datetime.fromisoformat(content["_fetched_at"].replace("Z", "+00:00"))
        return (datetime.now(fetched.tzinfo) - fetched).total_seconds() / 3600
    except Exception:
        return 9999.0

def is_stale(*path_parts, ttl_hours=None):
    ttl = ttl_hours if ttl_hours is not None else mode.cache_ttl_hours
    return cache_age_hours(*path_parts) >= ttl

# ---------------------------------------------------------------------------
# Async batch helper
# ---------------------------------------------------------------------------
async def batch_fetch(items, fetch_fn, label="items", log_every=50):
    sem = asyncio.Semaphore(DEFAULT_CONCURRENCY * 2)
    done = 0
    total = len(items)
    async def wrapped(item):
        nonlocal done
        async with sem:
            await fetch_fn(item)
        done += 1
        if done % log_every == 0 or done == total:
            log.info(f"  {label}: {done}/{total}")
    await asyncio.gather(*(wrapped(item) for item in items))

# ---------------------------------------------------------------------------
# Layers (same as pipeline.py backfill mode)
# ---------------------------------------------------------------------------
def fetch_universe(ticker_filter=None):
    log.info("=" * 60)
    log.info("LAYER 1: Universe")
    log.info("=" * 60)
    if ticker_filter:
        log.info(f"Skipping universe refresh (single ticker mode: {ticker_filter})")
        return
    sp500 = fmp_get_sync("sp500-constituent")
    if sp500: save(sp500, "universe", "sp500.json"); log.info(f"  → {len(sp500)} S&P 500")
    nasdaq = fmp_get_sync("nasdaq-constituent")
    if nasdaq: save(nasdaq, "universe", "nasdaq.json"); log.info(f"  → {len(nasdaq)} Nasdaq")
    dowjones = fmp_get_sync("dowjones-constituent")
    if dowjones: save(dowjones, "universe", "dowjones.json"); log.info(f"  → {len(dowjones)} Dow Jones")

    tickers = {}
    for name, src in [("sp500", sp500), ("nasdaq", nasdaq), ("dowjones", dowjones)]:
        if not src: continue
        for item in src:
            sym = item.get("symbol")
            if not sym: continue
            if sym not in tickers:
                tickers[sym] = {"symbol": sym, "name": item.get("name", ""), "sector": item.get("sector", ""),
                                "subSector": item.get("subSector", ""), "indices": []}
            tickers[sym]["indices"].append(name)
    manifest = {"count": len(tickers), "updated": datetime.now(timezone.utc).isoformat(), "tickers": tickers}
    save(manifest, "_meta", "universe-manifest.json")
    log.info(f"Universe: {len(tickers)} unique tickers")

async def fetch_profiles(tickers):
    log.info("Fetching company profiles...")
    async def fetch_one(t):
        if not is_stale("universe", "profiles", f"{t}.json", ttl_hours=1): return
        data = await fmp_get("profile", {"symbol": t})
        if data: save(data, "universe", "profiles", f"{t}.json")
    await batch_fetch(tickers, fetch_one, "Profiles", log_every=50)

async def fetch_prices(tickers):
    log.info("=" * 60)
    log.info("LAYER 2: Prices")
    log.info("=" * 60)
    batch_size = 50
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    async def fetch_quote_batch(batch):
        data = await fmp_get("batch-quote", {"symbols": ",".join(batch)})
        if data:
            for q in data:
                sym = q.get("symbol")
                if sym: save(q, "prices", "quotes", f"{sym}.json")
    await batch_fetch(batches, fetch_quote_batch, "Quotes batch", log_every=2)

    log.info("Fetching daily prices...")
    async def fetch_daily(t):
        data = await fmp_get("historical-price-eod/full", {"symbol": t, "from": mode.price_from})
        if data and isinstance(data, list) and len(data) > 0:
            save(data, "prices", "daily", f"{t}.json")
    await batch_fetch(tickers, fetch_daily, "Daily prices", log_every=50)

    log.info("Fetching index prices...")
    for idx in ["^GSPC", "^DJI", "^IXIC"]:
        data = await fmp_get("historical-price-eod/full", {"symbol": idx, "from": mode.price_from})
        if data: save(data, "prices", "indices", f"{idx.replace('^','')}.json"); log.info(f"  Index {idx}: {len(data)} days")

async def fetch_fundamentals(tickers):
    log.info("=" * 60)
    log.info("LAYER 3: Fundamentals")
    log.info("=" * 60)
    for ln, ep in [("income","income-statement"),("balance","balance-sheet-statement"),("cashflow","cash-flow-statement")]:
        log.info(f"Fetching {ln}...")
        async def f(t, _ln=ln, _ep=ep):
            if not is_stale("fundamentals", _ln, f"{t}.json"): return
            data = await fmp_get(_ep, {"symbol": t, "period": "quarter", "limit": mode.quarterly_limit})
            if data: save(data, "fundamentals", _ln, f"{t}.json")
        await batch_fetch(tickers, f, ln, log_every=100)
    for ln, ep in [("income-growth","income-statement-growth"),("cashflow-growth","cash-flow-statement-growth"),("financial-growth","financial-growth")]:
        log.info(f"Fetching {ln}...")
        async def f(t, _ln=ln, _ep=ep):
            if not is_stale("fundamentals", _ln, f"{t}.json"): return
            data = await fmp_get(_ep, {"symbol": t, "period": "quarter", "limit": mode.quarterly_limit})
            if data: save(data, "fundamentals", _ln, f"{t}.json")
        await batch_fetch(tickers, f, ln, log_every=100)

async def fetch_metrics(tickers):
    log.info("=" * 60)
    log.info("LAYER 4: Metrics")
    log.info("=" * 60)
    for ln, ep in [("key-metrics","key-metrics"),("ratios","ratios"),("enterprise-values","enterprise-values")]:
        log.info(f"Fetching {ln}...")
        async def f(t, _ln=ln, _ep=ep):
            if not is_stale("metrics", _ln, f"{t}.json"): return
            data = await fmp_get(_ep, {"symbol": t, "period": "quarter", "limit": mode.quarterly_limit})
            if data: save(data, "metrics", _ln, f"{t}.json")
        await batch_fetch(tickers, f, ln, log_every=100)
    for ln, ep in [("key-metrics-ttm","key-metrics-ttm"),("ratios-ttm","ratios-ttm")]:
        log.info(f"Fetching {ln}...")
        async def f(t, _ln=ln, _ep=ep):
            if not is_stale("metrics", _ln, f"{t}.json"): return
            data = await fmp_get(_ep, {"symbol": t})
            if data: save(data, "metrics", _ln, f"{t}.json")
        await batch_fetch(tickers, f, ln, log_every=100)
    log.info("Fetching financial scores...")
    async def fs(t):
        if not is_stale("metrics", "financial-scores", f"{t}.json", ttl_hours=1): return
        data = await fmp_get("financial-scores", {"symbol": t})
        if data: save(data, "metrics", "financial-scores", f"{t}.json")
    await batch_fetch(tickers, fs, "scores", log_every=100)
    log.info("Fetching owner earnings...")
    async def fo(t):
        if not is_stale("metrics", "owner-earnings", f"{t}.json"): return
        data = await fmp_get("owner-earnings", {"symbol": t, "limit": mode.quarterly_limit})
        if data: save(data, "metrics", "owner-earnings", f"{t}.json")
    await batch_fetch(tickers, fo, "owner-earnings", log_every=100)

async def fetch_analyst(tickers):
    log.info("=" * 60)
    log.info("LAYER 5: Analyst")
    log.info("=" * 60)
    log.info("Fetching estimates...")
    async def fe(t):
        if not is_stale("analyst", "estimates", f"{t}.json"): return
        annual = await fmp_get("analyst-estimates", {"symbol": t, "period": "annual", "limit": mode.annual_limit})
        quarterly = await fmp_get("analyst-estimates", {"symbol": t, "period": "quarter", "limit": mode.quarterly_est_limit})
        if annual or quarterly: save({"annual": annual, "quarterly": quarterly}, "analyst", "estimates", f"{t}.json")
    await batch_fetch(tickers, fe, "estimates", log_every=100)
    log.info("Fetching price targets...")
    async def fp(t):
        if not is_stale("analyst", "price-targets", f"{t}.json"): return
        c = await fmp_get("price-target-consensus", {"symbol": t})
        s = await fmp_get("price-target-summary", {"symbol": t})
        if c or s: save({"consensus": c, "summary": s}, "analyst", "price-targets", f"{t}.json")
    await batch_fetch(tickers, fp, "price-targets", log_every=100)
    log.info("Fetching grades...")
    async def fg(t):
        if not is_stale("analyst", "grades", f"{t}.json"): return
        data = await fmp_get("grades", {"symbol": t, "limit": mode.grades_limit})
        if data: save(data, "analyst", "grades", f"{t}.json")
    await batch_fetch(tickers, fg, "grades", log_every=100)
    log.info("Fetching grades consensus...")
    async def fgc(t):
        if not is_stale("analyst", "grades-consensus", f"{t}.json"): return
        data = await fmp_get("grades-consensus", {"symbol": t})
        if data: save(data, "analyst", "grades-consensus", f"{t}.json")
    await batch_fetch(tickers, fgc, "grades-consensus", log_every=100)

async def fetch_earnings(tickers):
    log.info("=" * 60)
    log.info("LAYER 6: Earnings")
    log.info("=" * 60)
    async def f(t):
        if not is_stale("earnings", "calendar", f"{t}.json"): return
        data = await fmp_get("earnings", {"symbol": t, "limit": mode.earnings_limit})
        if data: save(data, "earnings", "calendar", f"{t}.json")
    await batch_fetch(tickers, f, "earnings", log_every=100)

async def fetch_transcripts(tickers):
    log.info("=" * 60)
    log.info("LAYER 6b: Transcripts")
    log.info("=" * 60)
    years = list(range(mode.transcript_start_year, mode.transcript_end_year + 1))
    jobs = []
    for t in tickers:
        for y in years:
            for q in [1,2,3,4]:
                fname = f"{t}_{y}_Q{q}.json"
                if (DATA_DIR / "earnings" / "transcripts" / fname).exists(): continue
                jobs.append((t, y, q, fname))
    log.info(f"Transcripts: {len(jobs)} to fetch")
    if not jobs: return
    fetched = 0
    async def f(job):
        nonlocal fetched
        t, y, q, fname = job
        data = await fmp_get("earning-call-transcript", {"symbol": t, "year": y, "quarter": q})
        if data and len(data) > 0: save(data, "earnings", "transcripts", fname); fetched += 1
    await batch_fetch(jobs, f, "Transcripts", log_every=200)
    log.info(f"  → Transcripts done ({fetched} fetched)")

async def fetch_catalysts(tickers):
    log.info("=" * 60)
    log.info("LAYER 7: Catalysts")
    log.info("=" * 60)
    log.info("Fetching insider trades...")
    async def fi(t):
        if not is_stale("catalysts", "insider-trades", f"{t}.json"): return
        data = await fmp_get("insider-trading/search", {"symbol": t, "limit": mode.insider_limit})
        if data: save(data, "catalysts", "insider-trades", f"{t}.json")
    await batch_fetch(tickers, fi, "insider-trades", log_every=100)
    log.info("Fetching dividends...")
    async def fd(t):
        if not is_stale("catalysts", "dividends", f"{t}.json", ttl_hours=1): return
        data = await fmp_get("dividends", {"symbol": t})
        if data: save(data, "catalysts", "dividends", f"{t}.json")
    await batch_fetch(tickers, fd, "dividends", log_every=100)
    log.info("Fetching splits...")
    async def fs(t):
        if not is_stale("catalysts", "splits", f"{t}.json", ttl_hours=1): return
        data = await fmp_get("splits", {"symbol": t})
        if data: save(data, "catalysts", "splits", f"{t}.json")
    await batch_fetch(tickers, fs, "splits", log_every=100)

async def fetch_sector():
    log.info("=" * 60)
    log.info("LAYER 8: Sector benchmarks")
    log.info("=" * 60)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data = await fmp_get("sector-pe-snapshot", {"date": today})
    if data: save(data, "sector", "sector-pe", "latest.json")
    data = await fmp_get("industry-pe-snapshot", {"date": today})
    if data: save(data, "sector", "industry-pe", "latest.json")
    data = await fmp_get("sector-performance-snapshot", {"date": today})
    if data: save(data, "sector", "sector-performance", "latest.json")

async def fetch_macro():
    log.info("=" * 60)
    log.info("LAYER 9: Macro")
    log.info("=" * 60)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    one_year_ago = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")
    data = await fmp_get("treasury-rates", {"from": "2015-01-01", "to": today})
    if data: save(data, "macro", "treasury-rates.json")
    thirty_days = (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")
    data = await fmp_get("economic-calendar", {"from": today, "to": thirty_days})
    if data: save(data, "macro", "economic-calendar.json")
    for ind in ["GDP", "CPI", "unemploymentRate"]:
        data = await fmp_get("economic-indicators", {"name": ind})
        if data: save(data, "macro", f"{ind.lower()}.json")

async def fetch_valuation(tickers):
    log.info("=" * 60)
    log.info("LAYER 10: Valuation")
    log.info("=" * 60)
    async def f(t):
        if not is_stale("valuation", "dcf", f"{t}.json"): return
        data = await fmp_get("discounted-cash-flow", {"symbol": t})
        if data: save(data, "valuation", "dcf", f"{t}.json")
    await batch_fetch(tickers, f, "dcf", log_every=100)

async def fetch_news(tickers):
    log.info("=" * 60)
    log.info("LAYER 11: News (MarketAux API)")
    log.info("=" * 60)

    marketaux_key = os.environ.get("MARKETAUX_API_KEY", "baT4qxw3sDlOqsrgPcjTrqlaoJcPmHUKVHBeNeKS")
    if not marketaux_key:
        log.warning("  MARKETAUX_API_KEY not set. Skipping news.")
        return

    from urllib.parse import quote
    from datetime import datetime, timezone, timedelta

    # Backfill: fetch last 30 days of news (MarketAux free tier limits history)
    published_after = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    batch_size = 50
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    total_articles = 0

    for idx, batch in enumerate(batches):
        symbols_csv = ",".join(batch)
        url = (
            f"https://api.marketaux.com/v1/news/all"
            f"?symbols={quote(symbols_csv)}"
            f"&filter_entities=true&language=en&limit=20"
            f"&published_after={published_after}"
            f"&api_token={marketaux_key}"
        )
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(_executor, _sync_http_get, url, 2)
            stats["requests_made"] += 1
            if result and isinstance(result, dict):
                articles = result.get("data", [])
                for art in articles:
                    normalized = {
                        "publishedDate": art.get("published_at", ""),
                        "title": art.get("title", ""),
                        "text": art.get("snippet") or art.get("description", ""),
                        "url": art.get("url", ""),
                        "site": art.get("source", ""),
                        "image": art.get("image_url", ""),
                    }
                    for entity in art.get("entities", []):
                        sym = entity.get("symbol", "")
                        if sym in batch:
                            normalized_with_sym = {**normalized, "symbol": sym, "sentiment": entity.get("sentiment_score")}
                            save([normalized_with_sym], "news", f"{sym}.json")
                            total_articles += 1
            else:
                stats["requests_failed"] += 1
        except Exception as e:
            log.warning(f"  MarketAux batch {idx+1} failed: {e}")
            stats["requests_failed"] += 1
        if (idx + 1) % 5 == 0 or (idx + 1) == len(batches):
            log.info(f"  News: {idx+1}/{len(batches)} batches, {total_articles} articles saved")
        await asyncio.sleep(1)

    log.info(f"  → News done: {total_articles} articles across {len(batches)} batches")

# ---------------------------------------------------------------------------
# Universe resolution
# ---------------------------------------------------------------------------
def get_universe_tickers(ticker_filter=None):
    if ticker_filter:
        return [t.strip().upper() for t in ticker_filter.split(",")]
    manifest = load("_meta", "universe-manifest.json")
    if manifest and "tickers" in manifest:
        return sorted(manifest["tickers"].keys())
    tickers = set()
    for f in ["sp500.json", "nasdaq.json", "dowjones.json"]:
        data = load("universe", f)
        if data:
            for item in data:
                sym = item.get("symbol")
                if sym: tickers.add(sym)
    if tickers: return sorted(tickers)
    log.info("No universe data found. Fetching fresh...")
    fetch_universe()
    manifest = load("_meta", "universe-manifest.json")
    if manifest and "tickers" in manifest:
        return sorted(manifest["tickers"].keys())
    return []

# ---------------------------------------------------------------------------
# DB rebuild (from build_db.py)
# ---------------------------------------------------------------------------
SCHEMAS = {
    "prices": """
    CREATE TABLE IF NOT EXISTS prices (
        symbol TEXT NOT NULL, date TEXT NOT NULL, open REAL, high REAL, low REAL,
        close REAL, volume INTEGER, change_pct REAL, vwap REAL,
        PRIMARY KEY (symbol, date));
    CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);
    CREATE INDEX IF NOT EXISTS idx_prices_symbol ON prices(symbol);""",
    "income": """
    CREATE TABLE IF NOT EXISTS income (
        symbol TEXT NOT NULL, date TEXT NOT NULL, fiscal_year TEXT, period TEXT,
        revenue REAL, gross_profit REAL, operating_income REAL, net_income REAL,
        ebitda REAL, eps REAL, eps_diluted REAL, shares_diluted REAL,
        PRIMARY KEY (symbol, date));
    CREATE INDEX IF NOT EXISTS idx_income_symbol ON income(symbol);""",
    "balance": """
    CREATE TABLE IF NOT EXISTS balance (
        symbol TEXT NOT NULL, date TEXT NOT NULL, fiscal_year TEXT, period TEXT,
        cash REAL, inventory REAL, total_current_assets REAL, total_assets REAL,
        total_current_liabilities REAL, long_term_debt REAL, total_debt REAL,
        total_liabilities REAL, total_equity REAL, net_debt REAL,
        PRIMARY KEY (symbol, date));
    CREATE INDEX IF NOT EXISTS idx_balance_symbol ON balance(symbol);""",
    "cashflow": """
    CREATE TABLE IF NOT EXISTS cashflow (
        symbol TEXT NOT NULL, date TEXT NOT NULL, fiscal_year TEXT, period TEXT,
        operating_cf REAL, capex REAL, free_cash_flow REAL, dividends_paid REAL,
        stock_repurchased REAL, PRIMARY KEY (symbol, date));
    CREATE INDEX IF NOT EXISTS idx_cashflow_symbol ON cashflow(symbol);""",
    "earnings": """
    CREATE TABLE IF NOT EXISTS earnings (
        symbol TEXT NOT NULL, date TEXT NOT NULL, eps_actual REAL, eps_estimated REAL,
        revenue_actual REAL, revenue_estimated REAL, PRIMARY KEY (symbol, date));
    CREATE INDEX IF NOT EXISTS idx_earnings_symbol ON earnings(symbol);""",
    "insider_trades": """
    CREATE TABLE IF NOT EXISTS insider_trades (
        symbol TEXT NOT NULL, transaction_date TEXT NOT NULL, reporting_name TEXT,
        type_of_owner TEXT, transaction_type TEXT, shares REAL, price REAL,
        value REAL, securities_owned REAL,
        PRIMARY KEY (symbol, transaction_date, reporting_name, transaction_type));
    CREATE INDEX IF NOT EXISTS idx_insider_symbol ON insider_trades(symbol);
    CREATE INDEX IF NOT EXISTS idx_insider_date ON insider_trades(transaction_date);
    CREATE INDEX IF NOT EXISTS idx_insider_type ON insider_trades(transaction_type);""",
    "analyst_grades": """
    CREATE TABLE IF NOT EXISTS analyst_grades (
        symbol TEXT NOT NULL, date TEXT NOT NULL, grading_company TEXT,
        previous_grade TEXT, new_grade TEXT, action TEXT,
        PRIMARY KEY (symbol, date, grading_company));
    CREATE INDEX IF NOT EXISTS idx_grades_symbol ON analyst_grades(symbol);
    CREATE INDEX IF NOT EXISTS idx_grades_date ON analyst_grades(date);
    CREATE INDEX IF NOT EXISTS idx_grades_action ON analyst_grades(action);""",
}

ALL_TABLES = ["prices", "income", "balance", "cashflow", "earnings", "insider_trades", "analyst_grades"]

def map_prices(t, data):
    return [(t, r.get("date"), r.get("open"), r.get("high"), r.get("low"),
             r.get("close"), r.get("volume"), r.get("changePercent"), r.get("vwap")) for r in data]

def map_income(t, data):
    return [(t, r.get("date"), r.get("fiscalYear"), r.get("period"), r.get("revenue"),
             r.get("grossProfit"), r.get("operatingIncome"), r.get("netIncome"), r.get("ebitda"),
             r.get("eps"), r.get("epsDiluted"), r.get("weightedAverageShsOutDil")) for r in data]

def map_balance(t, data):
    rows = []
    for r in data:
        cash = r.get("cashAndCashEquivalents", 0) or 0
        total_debt = (r.get("shortTermDebt", 0) or 0) + (r.get("longTermDebt", 0) or 0)
        rows.append((t, r.get("date"), r.get("fiscalYear"), r.get("period"), cash,
                      r.get("inventory"), r.get("totalCurrentAssets"), r.get("totalAssets"),
                      r.get("totalCurrentLiabilities"), r.get("longTermDebt"), total_debt,
                      r.get("totalLiabilities"), r.get("totalStockholdersEquity"), total_debt - cash))
    return rows

def map_cashflow(t, data):
    rows = []
    for r in data:
        ocf = r.get("netCashProvidedByOperatingActivities")
        capex = r.get("investmentsInPropertyPlantAndEquipment")
        fcf = (ocf + capex) if ocf is not None and capex is not None else None
        rows.append((t, r.get("date"), r.get("fiscalYear"), r.get("period"), ocf, capex, fcf,
                      r.get("commonDividendsPaid"), r.get("commonStockRepurchased")))
    return rows

def map_earnings(t, data):
    return [(t, r.get("date"), r.get("epsActual"), r.get("epsEstimated"),
             r.get("revenueActual"), r.get("revenueEstimated")) for r in data]

def map_insider_trades(t, data):
    rows = []
    for r in data:
        shares = r.get("securitiesTransacted", 0) or 0
        price = r.get("price", 0) or 0
        rows.append((t, r.get("transactionDate"), r.get("reportingName"), r.get("typeOfOwner"),
                      r.get("transactionType"), shares, price, shares * price, r.get("securitiesOwned")))
    return rows

def map_analyst_grades(t, data):
    return [(t, r.get("date"), r.get("gradingCompany"), r.get("previousGrade"),
             r.get("newGrade"), r.get("action")) for r in data]

TABLE_CONFIGS = {
    "prices": {"json_dir": "prices/daily", "mapper": map_prices,
               "insert": "INSERT OR REPLACE INTO prices (symbol,date,open,high,low,close,volume,change_pct,vwap) VALUES (?,?,?,?,?,?,?,?,?)"},
    "income": {"json_dir": "fundamentals/income", "mapper": map_income,
               "insert": "INSERT OR REPLACE INTO income (symbol,date,fiscal_year,period,revenue,gross_profit,operating_income,net_income,ebitda,eps,eps_diluted,shares_diluted) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"},
    "balance": {"json_dir": "fundamentals/balance", "mapper": map_balance,
                "insert": "INSERT OR REPLACE INTO balance (symbol,date,fiscal_year,period,cash,inventory,total_current_assets,total_assets,total_current_liabilities,long_term_debt,total_debt,total_liabilities,total_equity,net_debt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"},
    "cashflow": {"json_dir": "fundamentals/cashflow", "mapper": map_cashflow,
                 "insert": "INSERT OR REPLACE INTO cashflow (symbol,date,fiscal_year,period,operating_cf,capex,free_cash_flow,dividends_paid,stock_repurchased) VALUES (?,?,?,?,?,?,?,?,?)"},
    "earnings": {"json_dir": "earnings/calendar", "mapper": map_earnings,
                 "insert": "INSERT OR REPLACE INTO earnings (symbol,date,eps_actual,eps_estimated,revenue_actual,revenue_estimated) VALUES (?,?,?,?,?,?)"},
    "insider_trades": {"json_dir": "catalysts/insider-trades", "mapper": map_insider_trades,
                       "insert": "INSERT OR REPLACE INTO insider_trades (symbol,transaction_date,reporting_name,type_of_owner,transaction_type,shares,price,value,securities_owned) VALUES (?,?,?,?,?,?,?,?,?)"},
    "analyst_grades": {"json_dir": "analyst/grades", "mapper": map_analyst_grades,
                       "insert": "INSERT OR REPLACE INTO analyst_grades (symbol,date,grading_company,previous_grade,new_grade,action) VALUES (?,?,?,?,?,?)"},
}

def load_json(filepath):
    if not filepath.exists(): return None
    try:
        content = json.loads(filepath.read_text())
        return content.get("data")
    except (json.JSONDecodeError, KeyError):
        return None

def rebuild_db(tickers):
    log.info("=" * 60)
    log.info("REBUILDING DATABASE FROM SCRATCH")
    log.info("=" * 60)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")

    # Drop and recreate all tables
    for table in ALL_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.executescript(SCHEMAS[table])
    log.info(f"Dropped and recreated {len(ALL_TABLES)} tables")

    total_rows = 0
    for table in ALL_TABLES:
        cfg = TABLE_CONFIGS[table]
        log.info(f"Loading {table}...")
        t0 = time.time()
        table_rows = 0
        skipped = 0
        cur = conn.cursor()

        for i, ticker in enumerate(tickers):
            filepath = DATA_DIR / cfg["json_dir"] / f"{ticker}.json"
            data = load_json(filepath)
            if data is None or (isinstance(data, list) and len(data) == 0):
                skipped += 1; continue
            if not isinstance(data, list):
                skipped += 1; continue
            rows = cfg["mapper"](ticker, data)
            if not rows:
                skipped += 1; continue
            cur.executemany(cfg["insert"], rows)
            table_rows += len(rows)
            if (i + 1) % 100 == 0 or (i + 1) == len(tickers):
                conn.commit()

        conn.commit()
        elapsed = time.time() - t0
        log.info(f"  {table}: {table_rows:,} rows from {len(tickers)-skipped} tickers ({elapsed:.1f}s)")
        total_rows += table_rows

    stats["db_rows_inserted"] = total_rows
    log.info(f"DB rebuild complete: {total_rows:,} total rows")
    log.info(f"DB: {DB_PATH} ({DB_PATH.stat().st_size / 1024 / 1024:.1f} MB)")
    conn.close()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
LAYER_MAP = {"universe":"universe","prices":"prices","fundamentals":"fundamentals","metrics":"metrics",
             "analyst":"analyst","earnings":"earnings","transcripts":"transcripts","catalysts":"catalysts",
             "sector":"sector","macro":"macro","valuation":"valuation","news":"news"}

async def run_async(ticker_filter=None, layer_filter=None, dry_run=False, concurrency=DEFAULT_CONCURRENCY, since="2015-01-01"):
    global _bucket, _executor, DEFAULT_CONCURRENCY, mode

    mode = BackfillMode(since=since)
    DEFAULT_CONCURRENCY = concurrency
    _bucket = TokenBucket(RATE_LIMIT)
    _executor = ThreadPoolExecutor(max_workers=concurrency)

    stats["started_at"] = datetime.now(timezone.utc).isoformat()

    if dry_run:
        tickers = ticker_filter.upper().split(",") if ticker_filter else ["(full universe)"]
        log.info("DRY RUN — no API calls will be made")
        log.info(f"Mode: {mode}")
        log.info(f"Tickers: {', '.join(tickers[:20])}{'...' if len(tickers) > 20 else ''}")
        log.info(f"Layer: {layer_filter or 'all'}")
        log.info(f"Quarterly limit: {mode.quarterly_limit}")
        log.info("After fetching: will DROP all 7 DB tables and rebuild from JSON files")
        return

    if not API_KEY:
        log.error("FMP_API_KEY environment variable not set.")
        sys.exit(1)

    tickers = get_universe_tickers(ticker_filter)
    if not tickers:
        log.error("No tickers to process.")
        sys.exit(1)

    stats["tickers_processed"] = len(tickers)
    log.info(f"Backfill starting: {len(tickers)} tickers (concurrency={concurrency})")
    log.info(f"Mode: {mode}")

    run_all = layer_filter is None
    if run_all or layer_filter == "universe": fetch_universe(ticker_filter); await fetch_profiles(tickers)
    if run_all or layer_filter == "prices": await fetch_prices(tickers)
    if run_all or layer_filter == "fundamentals": await fetch_fundamentals(tickers)
    if run_all or layer_filter == "metrics": await fetch_metrics(tickers)
    if run_all or layer_filter == "analyst": await fetch_analyst(tickers)
    if run_all or layer_filter == "earnings": await fetch_earnings(tickers)
    if run_all or layer_filter == "transcripts": await fetch_transcripts(tickers)
    if run_all or layer_filter == "catalysts": await fetch_catalysts(tickers)
    if run_all or layer_filter == "sector": await fetch_sector()
    if run_all or layer_filter == "macro": await fetch_macro()
    if run_all or layer_filter == "valuation": await fetch_valuation(tickers)
    if run_all or layer_filter == "news": await fetch_news(tickers)

    # Rebuild DB from scratch
    rebuild_db(tickers)

    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    save(stats, "_meta", "last-run.json")
    _executor.shutdown(wait=False)

    log.info("=" * 60)
    log.info("BACKFILL COMPLETE")
    log.info(f"  Tickers:      {stats['tickers_processed']}")
    log.info(f"  API requests: {stats['requests_made']} made, {stats['requests_failed']} failed")
    log.info(f"  JSON files:   {stats['json_files_written']}")
    log.info(f"  DB rows:      {stats['db_rows_inserted']:,}")
    log.info(f"  Errors:       {len(stats['errors'])}")
    log.info("=" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AlphaScout Backfill — Full historical fetch + DB rebuild")
    parser.add_argument("--since", type=str, default="2015-01-01", help="Start date YYYY-MM-DD (default: 2015-01-01)")
    parser.add_argument("--ticker", type=str, help="Single ticker or comma-separated list")
    parser.add_argument("--layer", type=str, choices=list(LAYER_MAP.keys()), help="Single layer to fetch")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without fetching")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Max concurrent requests")
    args = parser.parse_args()
    asyncio.run(run_async(ticker_filter=args.ticker, layer_filter=args.layer, dry_run=args.dry_run,
                          concurrency=args.concurrency, since=args.since))
