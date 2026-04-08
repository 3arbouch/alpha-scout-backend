#!/usr/bin/env python3
"""
AlphaScout Daily Update — Incremental fetch + merge + DB upsert.
Designed for daily cron. Merges new data into existing JSON files (never loses history).
Updates SQLite DB incrementally via INSERT OR REPLACE.

Usage:
  python3 daily_update.py --dry-run
  python3 daily_update.py
  python3 daily_update.py --ticker NKE
  python3 daily_update.py --layer prices
  python3 daily_update.py --status
  python3 daily_update.py --concurrency 20
"""

import os, sys, json, time, asyncio, argparse, logging, sqlite3
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
log = logging.getLogger("daily_update")

stats = {"started_at": None, "finished_at": None, "requests_made": 0, "requests_failed": 0,
         "tickers_processed": 0, "mode": "DAILY_UPDATE", "errors": [],
         "json_files_updated": 0, "db_rows_upserted": 0}

# Track which tickers were modified per DB table for incremental DB update
modified_tickers = {
    "prices": set(), "income": set(), "balance": set(), "cashflow": set(),
    "earnings": set(), "insider_trades": set(), "analyst_grades": set(),
}

# ---------------------------------------------------------------------------
# Mode config (refresh TTLs)
# ---------------------------------------------------------------------------
class DailyMode:
    def __init__(self):
        now = datetime.now(timezone.utc)
        self.quarterly_limit = 8
        self.annual_limit = 3
        self.quarterly_est_limit = 8
        self.grades_limit = 20
        self.earnings_limit = 8
        self.insider_limit = 50
        self.transcript_start_year = now.year
        self.transcript_end_year = now.year
        self.cache_ttl_hours = 24
        self.profile_ttl_hours = 168
        self.scores_ttl_hours = 168
        self.dividends_ttl_hours = 168

    def __str__(self):
        return "DAILY_UPDATE (incremental merge)"

mode: DailyMode = None

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
# File I/O helpers
# ---------------------------------------------------------------------------
def load_file(*path_parts):
    """Load raw file content (full wrapper with _fetched_at)."""
    filepath = DATA_DIR / Path(*path_parts)
    if not filepath.exists():
        return None
    try:
        return json.loads(filepath.read_text())
    except (json.JSONDecodeError, KeyError):
        return None

def load(*path_parts):
    """Load data array from file."""
    content = load_file(*path_parts)
    if content is None:
        return None
    return content.get("data")

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

def save_snapshot(data, *path_parts):
    """Overwrite file — for snapshot/point-in-time data."""
    if data is None:
        return False
    if isinstance(data, list) and len(data) == 0:
        return False
    filepath = DATA_DIR / Path(*path_parts)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    wrapped = {"_fetched_at": datetime.now(timezone.utc).isoformat(), "data": data}
    filepath.write_text(json.dumps(wrapped, indent=2))
    stats["json_files_updated"] += 1
    return True

def save_merged(new_data, dedup_key_fn, *path_parts):
    """
    Merge new records into existing file by dedup key. Never loses historical data.
    dedup_key_fn: function that takes a record and returns a hashable dedup key.
    Records are sorted by date descending after merge.
    Returns True if file was actually modified.
    """
    if new_data is None:
        return False
    if isinstance(new_data, list) and len(new_data) == 0:
        return False

    filepath = DATA_DIR / Path(*path_parts)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Load existing data
    existing = []
    if filepath.exists():
        try:
            content = json.loads(filepath.read_text())
            existing = content.get("data", [])
            if not isinstance(existing, list):
                existing = []
        except (json.JSONDecodeError, KeyError):
            existing = []

    # Build map of existing by dedup key
    merged_map = {}
    for r in existing:
        key = dedup_key_fn(r)
        if key is not None:
            merged_map[key] = r

    # Add/overwrite with new data
    new_count = 0
    for r in new_data:
        key = dedup_key_fn(r)
        if key is not None:
            if key not in merged_map:
                new_count += 1
            merged_map[key] = r

    # Sort by date descending (try common date fields)
    merged = list(merged_map.values())
    def sort_key(r):
        return r.get("date") or r.get("transactionDate") or r.get("publishedDate") or ""
    merged.sort(key=sort_key, reverse=True)

    wrapped = {"_fetched_at": datetime.now(timezone.utc).isoformat(), "data": merged}
    filepath.write_text(json.dumps(wrapped, indent=2))
    stats["json_files_updated"] += 1
    return True

# ---------------------------------------------------------------------------
# Dedup key functions per layer
# ---------------------------------------------------------------------------
def dedup_date(r):
    d = r.get("date")
    return d if d else None

def dedup_grades(r):
    d, gc = r.get("date"), r.get("gradingCompany")
    return (d, gc) if d else None

def dedup_insider(r):
    d = r.get("transactionDate")
    n = r.get("reportingName", "")
    t = r.get("transactionType", "")
    return (d, n, t) if d else None

def dedup_dividends(r):
    return r.get("date")

def dedup_splits(r):
    return r.get("date")

def dedup_news(r):
    url = r.get("url")
    if url:
        return url
    return (r.get("publishedDate", ""), r.get("title", ""))

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
# Layers — daily incremental with merge
# ---------------------------------------------------------------------------
def fetch_universe(ticker_filter=None):
    log.info("=" * 60)
    log.info("LAYER 1: Universe")
    log.info("=" * 60)
    if ticker_filter:
        log.info(f"Skipping universe refresh (single ticker mode: {ticker_filter})")
        return
    sp500 = fmp_get_sync("sp500-constituent")
    if sp500: save_snapshot(sp500, "universe", "sp500.json")
    nasdaq = fmp_get_sync("nasdaq-constituent")
    if nasdaq: save_snapshot(nasdaq, "universe", "nasdaq.json")
    dowjones = fmp_get_sync("dowjones-constituent")
    if dowjones: save_snapshot(dowjones, "universe", "dowjones.json")

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
    save_snapshot(manifest, "_meta", "universe-manifest.json")
    log.info(f"Universe: {len(tickers)} unique tickers")

async def fetch_profiles(tickers):
    log.info("Fetching company profiles...")
    async def fetch_one(t):
        if not is_stale("universe", "profiles", f"{t}.json", ttl_hours=mode.profile_ttl_hours): return
        data = await fmp_get("profile", {"symbol": t})
        if data: save_snapshot(data, "universe", "profiles", f"{t}.json")
    await batch_fetch(tickers, fetch_one, "Profiles", log_every=50)

async def fetch_prices(tickers):
    log.info("=" * 60)
    log.info("LAYER 2: Prices")
    log.info("=" * 60)

    # Batch quotes (snapshot — always overwrite)
    batch_size = 50
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    async def fetch_quote_batch(batch):
        data = await fmp_get("batch-quote", {"symbols": ",".join(batch)})
        if data:
            for q in data:
                sym = q.get("symbol")
                if sym: save_snapshot(q, "prices", "quotes", f"{sym}.json")
    await batch_fetch(batches, fetch_quote_batch, "Quotes batch", log_every=2)

    # Daily OHLCV — merge (time-series)
    log.info("Fetching daily prices...")
    async def fetch_daily(t):
        existing = load("prices", "daily", f"{t}.json")
        if existing and isinstance(existing, list) and len(existing) > 0:
            try:
                from_date = existing[0].get("date", "2020-01-01")
            except (IndexError, AttributeError):
                from_date = "2020-01-01"
        else:
            from_date = "2015-01-01"

        data = await fmp_get("historical-price-eod/full", {"symbol": t, "from": from_date})
        if data and isinstance(data, list) and len(data) > 0:
            if save_merged(data, dedup_date, "prices", "daily", f"{t}.json"):
                modified_tickers["prices"].add(t)
    await batch_fetch(tickers, fetch_daily, "Daily prices", log_every=50)

    # Index prices
    log.info("Fetching index prices...")
    for idx in ["^GSPC", "^DJI", "^IXIC"]:
        safe = idx.replace("^", "")
        existing = load("prices", "indices", f"{safe}.json")
        from_date = "2015-01-01"
        if existing and isinstance(existing, list) and len(existing) > 0:
            from_date = existing[0].get("date", from_date)
        data = await fmp_get("historical-price-eod/full", {"symbol": idx, "from": from_date})
        if data: save_merged(data, dedup_date, "prices", "indices", f"{safe}.json")

    # Sector ETFs + SPY — stored in prices/daily like regular tickers so they sync to DB
    BENCHMARK_ETFS = ["SPY", "XLK", "XLF", "XLE", "XLV", "XLP", "XLY", "XLI", "XLB", "XLRE", "XLC", "XLU"]
    log.info(f"Fetching benchmark ETFs: {', '.join(BENCHMARK_ETFS)}")
    for etf in BENCHMARK_ETFS:
        existing = load("prices", "daily", f"{etf}.json")
        from_date = "2015-01-01"
        if existing and isinstance(existing, list) and len(existing) > 0:
            try:
                from_date = existing[0].get("date", from_date)
            except (IndexError, AttributeError):
                pass
        data = await fmp_get("historical-price-eod/full", {"symbol": etf, "from": from_date})
        if data and isinstance(data, list) and len(data) > 0:
            if save_merged(data, dedup_date, "prices", "daily", f"{etf}.json"):
                modified_tickers["prices"].add(etf)

async def fetch_fundamentals(tickers):
    log.info("=" * 60)
    log.info("LAYER 3: Fundamentals")
    log.info("=" * 60)

    # Time-series fundamentals — merge by date
    for ln, ep in [("income","income-statement"),("balance","balance-sheet-statement"),("cashflow","cash-flow-statement")]:
        log.info(f"Fetching {ln}...")
        db_table = ln  # maps to DB table name
        async def f(t, _ln=ln, _ep=ep, _db=db_table):
            if not is_stale("fundamentals", _ln, f"{t}.json"): return
            data = await fmp_get(_ep, {"symbol": t, "period": "quarter", "limit": mode.quarterly_limit})
            if data:
                if save_merged(data, dedup_date, "fundamentals", _ln, f"{t}.json"):
                    modified_tickers[_db].add(t)
        await batch_fetch(tickers, f, ln, log_every=100)

    # Growth — time-series, merge by date (no DB table for these)
    for ln, ep in [("income-growth","income-statement-growth"),("cashflow-growth","cash-flow-statement-growth"),("financial-growth","financial-growth")]:
        log.info(f"Fetching {ln}...")
        async def f(t, _ln=ln, _ep=ep):
            if not is_stale("fundamentals", _ln, f"{t}.json"): return
            data = await fmp_get(_ep, {"symbol": t, "period": "quarter", "limit": mode.quarterly_limit})
            if data: save_merged(data, dedup_date, "fundamentals", _ln, f"{t}.json")
        await batch_fetch(tickers, f, ln, log_every=100)

async def fetch_metrics(tickers):
    log.info("=" * 60)
    log.info("LAYER 4: Metrics")
    log.info("=" * 60)

    # Historical metrics — time-series, merge by date
    for ln, ep in [("key-metrics","key-metrics"),("ratios","ratios"),("enterprise-values","enterprise-values")]:
        log.info(f"Fetching {ln}...")
        async def f(t, _ln=ln, _ep=ep):
            if not is_stale("metrics", _ln, f"{t}.json"): return
            data = await fmp_get(_ep, {"symbol": t, "period": "quarter", "limit": mode.quarterly_limit})
            if data: save_merged(data, dedup_date, "metrics", _ln, f"{t}.json")
        await batch_fetch(tickers, f, ln, log_every=100)

    # TTM — snapshot, overwrite
    for ln, ep in [("key-metrics-ttm","key-metrics-ttm"),("ratios-ttm","ratios-ttm")]:
        log.info(f"Fetching {ln}...")
        async def f(t, _ln=ln, _ep=ep):
            if not is_stale("metrics", _ln, f"{t}.json"): return
            data = await fmp_get(_ep, {"symbol": t})
            if data: save_snapshot(data, "metrics", _ln, f"{t}.json")
        await batch_fetch(tickers, f, ln, log_every=100)

    # Financial scores — snapshot, overwrite
    log.info("Fetching financial scores...")
    async def fs(t):
        if not is_stale("metrics", "financial-scores", f"{t}.json", ttl_hours=mode.scores_ttl_hours): return
        data = await fmp_get("financial-scores", {"symbol": t})
        if data: save_snapshot(data, "metrics", "financial-scores", f"{t}.json")
    await batch_fetch(tickers, fs, "scores", log_every=100)

    # Owner earnings — time-series, merge by date
    log.info("Fetching owner earnings...")
    async def fo(t):
        if not is_stale("metrics", "owner-earnings", f"{t}.json"): return
        data = await fmp_get("owner-earnings", {"symbol": t, "limit": mode.quarterly_limit})
        if data: save_merged(data, dedup_date, "metrics", "owner-earnings", f"{t}.json")
    await batch_fetch(tickers, fo, "owner-earnings", log_every=100)

async def fetch_analyst(tickers):
    log.info("=" * 60)
    log.info("LAYER 5: Analyst")
    log.info("=" * 60)

    # Estimates — snapshot (combined annual+quarterly dict), overwrite
    log.info("Fetching estimates...")
    async def fe(t):
        if not is_stale("analyst", "estimates", f"{t}.json"): return
        annual = await fmp_get("analyst-estimates", {"symbol": t, "period": "annual", "limit": mode.annual_limit})
        quarterly = await fmp_get("analyst-estimates", {"symbol": t, "period": "quarter", "limit": mode.quarterly_est_limit})
        if annual or quarterly:
            save_snapshot({"annual": annual, "quarterly": quarterly}, "analyst", "estimates", f"{t}.json")
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            archive_path = DATA_DIR / "analyst" / "estimates-history" / t / f"{today}.json"
            if not archive_path.exists():
                save_snapshot({"annual": annual, "quarterly": quarterly}, "analyst", "estimates-history", t, f"{today}.json")
    await batch_fetch(tickers, fe, "estimates", log_every=100)

    # Price targets — snapshot, overwrite
    log.info("Fetching price targets...")
    async def fp(t):
        if not is_stale("analyst", "price-targets", f"{t}.json"): return
        c = await fmp_get("price-target-consensus", {"symbol": t})
        s = await fmp_get("price-target-summary", {"symbol": t})
        if c or s: save_snapshot({"consensus": c, "summary": s}, "analyst", "price-targets", f"{t}.json")
    await batch_fetch(tickers, fp, "price-targets", log_every=100)

    # Grades — time-series, merge by date+gradingCompany
    log.info("Fetching grades...")
    async def fg(t):
        if not is_stale("analyst", "grades", f"{t}.json"): return
        data = await fmp_get("grades", {"symbol": t, "limit": mode.grades_limit})
        if data:
            if save_merged(data, dedup_grades, "analyst", "grades", f"{t}.json"):
                modified_tickers["analyst_grades"].add(t)
    await batch_fetch(tickers, fg, "grades", log_every=100)

    # Grades consensus — snapshot, overwrite
    log.info("Fetching grades consensus...")
    async def fgc(t):
        if not is_stale("analyst", "grades-consensus", f"{t}.json"): return
        data = await fmp_get("grades-consensus", {"symbol": t})
        if data: save_snapshot(data, "analyst", "grades-consensus", f"{t}.json")
    await batch_fetch(tickers, fgc, "grades-consensus", log_every=100)

async def fetch_earnings(tickers):
    log.info("=" * 60)
    log.info("LAYER 6: Earnings")
    log.info("=" * 60)
    async def f(t):
        if not is_stale("earnings", "calendar", f"{t}.json"): return
        data = await fmp_get("earnings", {"symbol": t, "limit": mode.earnings_limit})
        if data:
            if save_merged(data, dedup_date, "earnings", "calendar", f"{t}.json"):
                modified_tickers["earnings"].add(t)
    await batch_fetch(tickers, f, "earnings", log_every=100)

async def fetch_catalysts(tickers):
    log.info("=" * 60)
    log.info("LAYER 7: Catalysts")
    log.info("=" * 60)

    log.info("Fetching insider trades...")
    async def fi(t):
        if not is_stale("catalysts", "insider-trades", f"{t}.json"): return
        data = await fmp_get("insider-trading/search", {"symbol": t, "limit": mode.insider_limit})
        if data:
            if save_merged(data, dedup_insider, "catalysts", "insider-trades", f"{t}.json"):
                modified_tickers["insider_trades"].add(t)
    await batch_fetch(tickers, fi, "insider-trades", log_every=100)

    log.info("Fetching dividends...")
    async def fd(t):
        if not is_stale("catalysts", "dividends", f"{t}.json", ttl_hours=mode.dividends_ttl_hours): return
        data = await fmp_get("dividends", {"symbol": t})
        if data: save_merged(data, dedup_dividends, "catalysts", "dividends", f"{t}.json")
    await batch_fetch(tickers, fd, "dividends", log_every=100)

    log.info("Fetching splits...")
    async def fs(t):
        if not is_stale("catalysts", "splits", f"{t}.json", ttl_hours=mode.dividends_ttl_hours): return
        data = await fmp_get("splits", {"symbol": t})
        if data: save_merged(data, dedup_splits, "catalysts", "splits", f"{t}.json")
    await batch_fetch(tickers, fs, "splits", log_every=100)

async def fetch_sector():
    log.info("=" * 60)
    log.info("LAYER 8: Sector benchmarks")
    log.info("=" * 60)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data = await fmp_get("sector-pe-snapshot", {"date": today})
    if data: save_snapshot(data, "sector", "sector-pe", "latest.json")
    data = await fmp_get("industry-pe-snapshot", {"date": today})
    if data: save_snapshot(data, "sector", "industry-pe", "latest.json")
    data = await fmp_get("sector-performance-snapshot", {"date": today})
    if data: save_snapshot(data, "sector", "sector-performance", "latest.json")

async def fetch_macro():
    log.info("=" * 60)
    log.info("LAYER 9: Macro")
    log.info("=" * 60)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    one_year_ago = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")
    # Treasury rates: merge new data into existing file to preserve history back to 2015
    data = await fmp_get("treasury-rates", {"from": one_year_ago, "to": today})
    if data:
        treasury_path = DATA_DIR / "macro" / "treasury-rates.json"
        existing = []
        if treasury_path.exists():
            try:
                raw = json.loads(treasury_path.read_text())
                existing = raw.get("data", raw) if isinstance(raw, dict) else raw
            except Exception:
                existing = []
        # Merge: existing + new, deduplicate by date, sort descending
        by_date = {r["date"]: r for r in existing}
        for r in data:
            by_date[r["date"]] = r  # new data overwrites
        merged = sorted(by_date.values(), key=lambda x: x["date"], reverse=True)
        treasury_path.parent.mkdir(parents=True, exist_ok=True)
        treasury_path.write_text(json.dumps({"_fetched_at": today, "data": merged}))
        log.info(f"Treasury rates: {len(merged)} records (merged)")
    thirty_days = (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")
    data = await fmp_get("economic-calendar", {"from": today, "to": thirty_days})
    if data: save_snapshot(data, "macro", "economic-calendar.json")
    for ind in ["GDP", "CPI", "unemploymentRate"]:
        data = await fmp_get("economic-indicators", {"name": ind})
        if data: save_snapshot(data, "macro", f"{ind.lower()}.json")

async def fetch_valuation(tickers):
    log.info("=" * 60)
    log.info("LAYER 10: Valuation")
    log.info("=" * 60)
    async def f(t):
        if not is_stale("valuation", "dcf", f"{t}.json"): return
        data = await fmp_get("discounted-cash-flow", {"symbol": t})
        if data: save_snapshot(data, "valuation", "dcf", f"{t}.json")
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

    published_after = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
    batch_size = 50  # symbols per request
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
                # Group articles by symbol and save per-ticker
                for art in articles:
                    # Normalize to match our existing news JSON format
                    normalized = {
                        "publishedDate": art.get("published_at", ""),
                        "title": art.get("title", ""),
                        "text": art.get("snippet") or art.get("description", ""),
                        "url": art.get("url", ""),
                        "site": art.get("source", ""),
                        "image": art.get("image_url", ""),
                    }
                    # Save to each matched symbol
                    for entity in art.get("entities", []):
                        sym = entity.get("symbol", "")
                        if sym in batch:
                            normalized_with_sym = {**normalized, "symbol": sym, "sentiment": entity.get("sentiment_score")}
                            save_merged([normalized_with_sym], dedup_news, "news", f"{sym}.json")
                            total_articles += 1
            else:
                stats["requests_failed"] += 1
        except Exception as e:
            log.warning(f"  MarketAux batch {idx+1} failed: {e}")
            stats["requests_failed"] += 1
        if (idx + 1) % 5 == 0 or (idx + 1) == len(batches):
            log.info(f"  News: {idx+1}/{len(batches)} batches, {total_articles} articles saved")
        # Small delay to respect rate limits
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
# DB incremental update (INSERT OR REPLACE, no DELETE)
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

def update_db_incremental():
    """Update DB incrementally — only process tickers whose JSON was modified."""
    total_modified = sum(len(v) for v in modified_tickers.values())
    if total_modified == 0:
        log.info("No JSON files modified — skipping DB update")
        return

    log.info("=" * 60)
    log.info("UPDATING DATABASE (incremental)")
    log.info("=" * 60)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")

    # Ensure schemas exist
    for table in ALL_TABLES:
        conn.executescript(SCHEMAS[table])

    total_rows = 0
    for table in ALL_TABLES:
        tickers_to_update = modified_tickers.get(table, set())
        if not tickers_to_update:
            continue

        cfg = TABLE_CONFIGS[table]
        log.info(f"  {table}: updating {len(tickers_to_update)} tickers...")
        t0 = time.time()
        table_rows = 0
        cur = conn.cursor()

        for ticker in sorted(tickers_to_update):
            filepath = DATA_DIR / cfg["json_dir"] / f"{ticker}.json"
            data = load_json(filepath)
            if data is None or not isinstance(data, list) or len(data) == 0:
                continue
            rows = cfg["mapper"](ticker, data)
            if not rows:
                continue
            # INSERT OR REPLACE — no DELETE needed
            cur.executemany(cfg["insert"], rows)
            table_rows += len(rows)

        conn.commit()
        elapsed = time.time() - t0
        log.info(f"  {table}: {table_rows:,} rows upserted ({elapsed:.1f}s)")
        total_rows += table_rows

    stats["db_rows_upserted"] = total_rows
    log.info(f"DB update complete: {total_rows:,} rows upserted")
    conn.close()

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
def show_status():
    meta_path = DATA_DIR / "_meta" / "last-run.json"
    if not meta_path.exists():
        print("No pipeline runs recorded yet.")
        return
    try:
        content = json.loads(meta_path.read_text())
        data = content.get("data", content)
        print("  === Last Run Status ===")
        print(f"  Started:   {data.get('started_at', 'unknown')}")
        print(f"  Finished:  {data.get('finished_at', 'unknown')}")
        print(f"  Mode:      {data.get('mode', 'unknown')}")
        print(f"  Requests:  {data.get('requests_made', '?')} made, {data.get('requests_failed', '?')} failed")
        print(f"  Tickers:   {data.get('tickers_processed', '?')}")
        print(f"  JSON:      {data.get('json_files_updated', '?')} files updated")
        print(f"  DB rows:   {data.get('db_rows_upserted', '?')} upserted")
        errors = data.get("errors", [])
        if errors:
            print(f"  Errors ({len(errors)}):")
            for e in errors[:10]: print(f"    - {e}")
        else:
            print("  Errors:    None")
    except Exception as e:
        print(f"Error reading status: {e}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
LAYER_MAP = {"universe":"universe","prices":"prices","fundamentals":"fundamentals","metrics":"metrics",
             "analyst":"analyst","earnings":"earnings","catalysts":"catalysts",
             "sector":"sector","macro":"macro","valuation":"valuation","news":"news"}

async def run_async(ticker_filter=None, layer_filter=None, dry_run=False, concurrency=DEFAULT_CONCURRENCY):
    global _bucket, _executor, DEFAULT_CONCURRENCY, mode

    mode = DailyMode()
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
        log.info(f"TTLs: cache={mode.cache_ttl_hours}h, profiles={mode.profile_ttl_hours}h, scores={mode.scores_ttl_hours}h")
        log.info("Time-series layers: merge (dedup + append). Snapshots: overwrite.")
        log.info("DB update: INSERT OR REPLACE on modified tickers only.")
        return

    if not API_KEY:
        log.error("FMP_API_KEY environment variable not set.")
        sys.exit(1)

    tickers = get_universe_tickers(ticker_filter)
    if not tickers:
        log.error("No tickers to process.")
        sys.exit(1)

    stats["tickers_processed"] = len(tickers)
    log.info(f"Daily update starting: {len(tickers)} tickers (concurrency={concurrency})")
    log.info(f"Mode: {mode}")

    run_all = layer_filter is None
    if run_all or layer_filter == "universe": fetch_universe(ticker_filter); await fetch_profiles(tickers)
    if run_all or layer_filter == "prices": await fetch_prices(tickers)
    if run_all or layer_filter == "fundamentals": await fetch_fundamentals(tickers)
    if run_all or layer_filter == "metrics": await fetch_metrics(tickers)
    if run_all or layer_filter == "analyst": await fetch_analyst(tickers)
    if run_all or layer_filter == "earnings": await fetch_earnings(tickers)
    if run_all or layer_filter == "catalysts": await fetch_catalysts(tickers)
    if run_all or layer_filter == "sector": await fetch_sector()
    if run_all or layer_filter == "macro": await fetch_macro()
    # Macro data refresh: FRED indicators + derived series (macro_data.py)
    if run_all or layer_filter == "macro":
        log.info("=" * 60)
        log.info("LAYER 9b: Macro — FRED + Derived (macro_data.py)")
        log.info("=" * 60)
        try:
            from macro_data import get_connection as macro_conn, init_tables, fetch_all_fred, load_spx_from_index, compute_all_derived
            from datetime import timedelta as _td
            _start = (datetime.now(timezone.utc) - _td(days=30)).strftime("%Y-%m-%d")
            _mc = macro_conn()
            init_tables(_mc)
            _n = fetch_all_fred(start=_start, conn=_mc)
            _n += load_spx_from_index(_mc, start=_start)
            compute_all_derived(_mc)
            _mc.close()
            log.info(f"FRED macro refresh: {_n} rows upserted")
        except Exception as e:
            log.error(f"FRED macro refresh failed: {e}")
    if run_all or layer_filter == "valuation": await fetch_valuation(tickers)
    if run_all or layer_filter == "news": await fetch_news(tickers)

    # DB update phase — AFTER all JSON fetching
    update_db_incremental()

    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    save_snapshot(stats, "_meta", "last-run.json")
    _executor.shutdown(wait=False)

    log.info("=" * 60)
    log.info("DAILY UPDATE COMPLETE")
    log.info(f"  Tickers:       {stats['tickers_processed']}")
    log.info(f"  API requests:  {stats['requests_made']} made, {stats['requests_failed']} failed")
    log.info(f"  JSON files:    {stats['json_files_updated']} updated")
    log.info(f"  DB rows:       {stats['db_rows_upserted']:,} upserted")
    for table, tset in modified_tickers.items():
        if tset:
            log.info(f"    {table}: {len(tset)} tickers modified")
    log.info(f"  Errors:        {len(stats['errors'])}")
    log.info("=" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AlphaScout Daily Update — Incremental fetch + merge + DB upsert")
    parser.add_argument("--ticker", type=str, help="Single ticker or comma-separated list")
    parser.add_argument("--layer", type=str, choices=list(LAYER_MAP.keys()), help="Single layer to fetch")
    parser.add_argument("--status", action="store_true", help="Show last run status")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without fetching")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Max concurrent requests")
    args = parser.parse_args()

    if args.status:
        show_status()
    else:
        asyncio.run(run_async(ticker_filter=args.ticker, layer_filter=args.layer,
                              dry_run=args.dry_run, concurrency=args.concurrency))
