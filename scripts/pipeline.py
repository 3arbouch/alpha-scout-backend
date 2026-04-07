#!/usr/bin/env python3
"""
AlphaScout Data Pipeline (Async)
=================================
Fetches and caches financial data from FMP API.
Uses asyncio + ThreadPoolExecutor with token-bucket rate limiter (700 req/min).

Two modes:
  --backfill --since 2015-01-01   Full historical pull from a date. Run once.
  --refresh                       Only fetch latest changed data. For daily cron.

Default mode is --refresh.

Usage:
  python pipeline.py --refresh                                    # Daily cron (default)
  python pipeline.py --backfill --since 2015-01-01                # Full historical backfill
  python pipeline.py --backfill --since 2015-01-01 --layer prices # Backfill one layer
  python pipeline.py --ticker NKE --refresh                       # Refresh single ticker
  python pipeline.py --ticker NKE --backfill --since 2020-01-01   # Backfill single ticker
  python pipeline.py --status                                     # Check last run
  python pipeline.py --dry-run                                    # Show plan, no fetching
  python pipeline.py --concurrency 10                             # Max concurrent requests
"""

import os
import sys
import json
import time
import asyncio
import argparse
import logging
import math
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
RATE_LIMIT = 2800  # requests per minute (buffer under 3000 plan limit)
DEFAULT_CONCURRENCY = 30

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

# ---------------------------------------------------------------------------
# Stats tracking
# ---------------------------------------------------------------------------
stats = {
    "started_at": None,
    "finished_at": None,
    "requests_made": 0,
    "requests_failed": 0,
    "tickers_processed": 0,
    "mode": None,
    "errors": [],
}

# ---------------------------------------------------------------------------
# Mode config — computed from --backfill/--refresh + --since
# ---------------------------------------------------------------------------
class PipelineMode:
    """Holds all mode-dependent parameters."""

    def __init__(self, backfill: bool = False, since: str = None):
        self.backfill = backfill
        now = datetime.now(timezone.utc)

        if backfill:
            if not since:
                since = "2015-01-01"
            self.since_date = since
            since_dt = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            years_back = max(1, (now - since_dt).days / 365.25)
            quarters_needed = int(math.ceil(years_back * 4)) + 4  # buffer

            self.price_from = since
            self.quarterly_limit = quarters_needed
            self.annual_limit = int(math.ceil(years_back)) + 2
            self.quarterly_est_limit = quarters_needed
            self.grades_limit = max(200, quarters_needed * 3)
            self.earnings_limit = quarters_needed
            self.insider_limit = max(200, quarters_needed * 5)
            self.transcript_start_year = since_dt.year
            self.cache_ttl_hours = 1  # skip files written in the last hour (resume-safe)
            self.profile_ttl_hours = 1
            self.scores_ttl_hours = 1
            self.dividends_ttl_hours = 1
        else:
            # Refresh mode — only fetch what's stale
            self.since_date = None
            self.price_from = None  # will use incremental logic
            self.quarterly_limit = 8  # last 2 years of quarters is plenty for refresh
            self.annual_limit = 3
            self.quarterly_est_limit = 8
            self.grades_limit = 20
            self.earnings_limit = 8
            self.insider_limit = 50
            self.transcript_start_year = now.year  # only current year
            self.cache_ttl_hours = 24
            self.profile_ttl_hours = 168  # 7 days
            self.scores_ttl_hours = 168
            self.dividends_ttl_hours = 168

        self.transcript_end_year = now.year

    def __str__(self):
        if self.backfill:
            return f"BACKFILL (since={self.since_date}, quarterly_limit={self.quarterly_limit})"
        return "REFRESH (daily delta)"


# Global mode — set in run()
mode: PipelineMode = None

# ---------------------------------------------------------------------------
# Token-bucket rate limiter
# ---------------------------------------------------------------------------
class TokenBucket:
    def __init__(self, rate_per_minute: int):
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


_bucket: TokenBucket | None = None
_executor: ThreadPoolExecutor | None = None

# ---------------------------------------------------------------------------
# HTTP / FMP helpers
# ---------------------------------------------------------------------------
def _sync_http_get(url: str, retries: int = 3) -> dict | list | None:
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
            else:
                if attempt < retries - 1:
                    time.sleep((attempt + 1) * 2)
                else:
                    return {"_http_error": e.code}
        except (URLError, TimeoutError, json.JSONDecodeError) as e:
            if attempt < retries - 1:
                time.sleep((attempt + 1) * 2)
            else:
                return {"_error": str(e)}
    return None


async def fmp_get(endpoint: str, params: dict = None, retries: int = 3) -> dict | list | None:
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

    if isinstance(result, dict):
        if "_error" in result:
            log.warning(f"FMP error for {endpoint}: {result['_error']}")
            stats["requests_failed"] += 1
            stats["requests_made"] += 1
            return None
        if "_http_error" in result:
            log.warning(f"HTTP {result['_http_error']} on {endpoint}")
            stats["requests_failed"] += 1
            stats["requests_made"] += 1
            return None

    stats["requests_made"] += 1
    return result


def fmp_get_sync(endpoint: str, params: dict = None, retries: int = 3) -> dict | list | None:
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
def save(data, *path_parts):
    if data is None:
        return
    if isinstance(data, list) and len(data) == 0:
        return
    filepath = DATA_DIR / Path(*path_parts)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    wrapped = {
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
    filepath.write_text(json.dumps(wrapped, indent=2))


def load(*path_parts) -> dict | list | None:
    filepath = DATA_DIR / Path(*path_parts)
    if not filepath.exists():
        return None
    try:
        content = json.loads(filepath.read_text())
        return content.get("data")
    except (json.JSONDecodeError, KeyError):
        return None


def cache_age_hours(*path_parts) -> float:
    filepath = DATA_DIR / Path(*path_parts)
    if not filepath.exists():
        return 9999.0
    try:
        content = json.loads(filepath.read_text())
        fetched = datetime.fromisoformat(content["_fetched_at"].replace("Z", "+00:00"))
        now = datetime.now(fetched.tzinfo)
        return (now - fetched).total_seconds() / 3600
    except Exception:
        return 9999.0


def is_stale(*path_parts, ttl_hours: float = None) -> bool:
    """Check if a cached file needs refreshing based on mode."""
    ttl = ttl_hours if ttl_hours is not None else mode.cache_ttl_hours
    if ttl <= 0:
        return True  # backfill: always refetch
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
# Layer 1: Universe
# ---------------------------------------------------------------------------
def fetch_universe(ticker_filter: str = None):
    log.info("=" * 60)
    log.info("LAYER 1: Universe")
    log.info("=" * 60)

    if ticker_filter:
        log.info(f"Skipping universe refresh (single ticker mode: {ticker_filter})")
        return

    log.info("Fetching S&P 500 constituents...")
    sp500 = fmp_get_sync("sp500-constituent")
    if sp500:
        save(sp500, "universe", "sp500.json")
        log.info(f"  → {len(sp500)} constituents")

    log.info("Fetching Nasdaq constituents...")
    nasdaq = fmp_get_sync("nasdaq-constituent")
    if nasdaq:
        save(nasdaq, "universe", "nasdaq.json")
        log.info(f"  → {len(nasdaq)} constituents")

    log.info("Fetching Dow Jones constituents...")
    dowjones = fmp_get_sync("dowjones-constituent")
    if dowjones:
        save(dowjones, "universe", "dowjones.json")
        log.info(f"  → {len(dowjones)} constituents")

    tickers = {}
    for source_name, source_data in [("sp500", sp500), ("nasdaq", nasdaq), ("dowjones", dowjones)]:
        if not source_data:
            continue
        for item in source_data:
            sym = item.get("symbol")
            if not sym:
                continue
            if sym not in tickers:
                tickers[sym] = {
                    "symbol": sym,
                    "name": item.get("name", ""),
                    "sector": item.get("sector", ""),
                    "subSector": item.get("subSector", ""),
                    "indices": [],
                }
            tickers[sym]["indices"].append(source_name)

    manifest = {
        "count": len(tickers),
        "updated": datetime.now(timezone.utc).isoformat(),
        "tickers": tickers,
    }
    save(manifest, "_meta", "universe-manifest.json")
    log.info(f"Universe: {len(tickers)} unique tickers across indices")


async def fetch_profiles(tickers: list[str]):
    log.info("Fetching company profiles...")

    async def fetch_one(ticker):
        if not is_stale("universe", "profiles", f"{ticker}.json", ttl_hours=mode.profile_ttl_hours):
            return
        data = await fmp_get("profile", {"symbol": ticker})
        if data:
            save(data, "universe", "profiles", f"{ticker}.json")

    await batch_fetch(tickers, fetch_one, "Profiles", log_every=50)
    log.info("  → Profiles done")

# ---------------------------------------------------------------------------
# Layer 2: Prices
# ---------------------------------------------------------------------------
async def fetch_prices(tickers: list[str]):
    log.info("=" * 60)
    log.info("LAYER 2: Prices")
    log.info("=" * 60)

    # Batch quotes (always fetch — they're cheap and always current)
    log.info("Fetching batch quotes...")
    batch_size = 50
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]

    async def fetch_quote_batch(batch):
        csv_symbols = ",".join(batch)
        data = await fmp_get("batch-quote", {"symbols": csv_symbols})
        if data:
            for quote in data:
                sym = quote.get("symbol")
                if sym:
                    save(quote, "prices", "quotes", f"{sym}.json")

    await batch_fetch(batches, fetch_quote_batch, "Quotes batch", log_every=2)

    # Daily OHLCV
    log.info("Fetching daily prices...")

    async def fetch_daily(ticker):
        existing = load("prices", "daily", f"{ticker}.json")

        if mode.backfill:
            # Backfill: fetch from since_date, merge with existing
            from_date = mode.price_from
        else:
            # Refresh: incremental from last cached date
            if existing and isinstance(existing, list) and len(existing) > 0:
                try:
                    from_date = existing[0].get("date", "2020-01-01")
                except (IndexError, AttributeError):
                    from_date = "2020-01-01"
            else:
                from_date = "2015-01-01"

        data = await fmp_get("historical-price-eod/full", {"symbol": ticker, "from": from_date})

        if data and isinstance(data, list) and len(data) > 0:
            if existing and isinstance(existing, list):
                existing_dates = {d["date"] for d in existing if "date" in d}
                new_records = [d for d in data if d.get("date") not in existing_dates]
                merged = new_records + existing
                # Sort by date descending
                merged.sort(key=lambda d: d.get("date", ""), reverse=True)
                save(merged, "prices", "daily", f"{ticker}.json")
            else:
                save(data, "prices", "daily", f"{ticker}.json")

    await batch_fetch(tickers, fetch_daily, "Daily prices", log_every=50)
    log.info("  → Daily prices done")

    # Index prices
    log.info("Fetching index prices...")
    idx_from = mode.price_from if mode.backfill else "2015-01-01"
    indices = ["^GSPC", "^DJI", "^IXIC"]
    for idx in indices:
        data = await fmp_get("historical-price-eod/full", {"symbol": idx, "from": idx_from})
        if data:
            safe_name = idx.replace("^", "")
            save(data, "prices", "indices", f"{safe_name}.json")
            log.info(f"  Index {idx}: {len(data)} days")

# ---------------------------------------------------------------------------
# Layer 3: Fundamentals
# ---------------------------------------------------------------------------
async def fetch_fundamentals(tickers: list[str]):
    log.info("=" * 60)
    log.info("LAYER 3: Fundamentals")
    log.info("=" * 60)

    endpoints = {
        "income": "income-statement",
        "balance": "balance-sheet-statement",
        "cashflow": "cash-flow-statement",
    }

    for layer_name, endpoint in endpoints.items():
        log.info(f"Fetching {layer_name} statements...")

        async def fetch_one(ticker, _ln=layer_name, _ep=endpoint):
            if not is_stale("fundamentals", _ln, f"{ticker}.json"):
                return
            data = await fmp_get(_ep, {"symbol": ticker, "period": "quarter", "limit": mode.quarterly_limit})
            if data:
                save(data, "fundamentals", _ln, f"{ticker}.json")

        await batch_fetch(tickers, fetch_one, layer_name, log_every=100)
        log.info(f"  → {layer_name} done")

    growth_endpoints = {
        "income-growth": "income-statement-growth",
        "cashflow-growth": "cash-flow-statement-growth",
        "financial-growth": "financial-growth",
    }

    for layer_name, endpoint in growth_endpoints.items():
        log.info(f"Fetching {layer_name}...")

        async def fetch_one(ticker, _ln=layer_name, _ep=endpoint):
            if not is_stale("fundamentals", _ln, f"{ticker}.json"):
                return
            data = await fmp_get(_ep, {"symbol": ticker, "period": "quarter", "limit": mode.quarterly_limit})
            if data:
                save(data, "fundamentals", _ln, f"{ticker}.json")

        await batch_fetch(tickers, fetch_one, layer_name, log_every=100)
        log.info(f"  → {layer_name} done")

# ---------------------------------------------------------------------------
# Layer 4: Metrics
# ---------------------------------------------------------------------------
async def fetch_metrics(tickers: list[str]):
    log.info("=" * 60)
    log.info("LAYER 4: Metrics")
    log.info("=" * 60)

    historical = {
        "key-metrics": ("key-metrics", {"period": "quarter", "limit": mode.quarterly_limit}),
        "ratios": ("ratios", {"period": "quarter", "limit": mode.quarterly_limit}),
        "enterprise-values": ("enterprise-values", {"period": "quarter", "limit": mode.quarterly_limit}),
    }

    for layer_name, (endpoint, extra_params) in historical.items():
        log.info(f"Fetching {layer_name}...")

        async def fetch_one(ticker, _ln=layer_name, _ep=endpoint, _extra=extra_params):
            if not is_stale("metrics", _ln, f"{ticker}.json"):
                return
            data = await fmp_get(_ep, {"symbol": ticker, **_extra})
            if data:
                save(data, "metrics", _ln, f"{ticker}.json")

        await batch_fetch(tickers, fetch_one, layer_name, log_every=100)
        log.info(f"  → {layer_name} done")

    ttm = {
        "key-metrics-ttm": "key-metrics-ttm",
        "ratios-ttm": "ratios-ttm",
    }

    for layer_name, endpoint in ttm.items():
        log.info(f"Fetching {layer_name}...")

        async def fetch_one(ticker, _ln=layer_name, _ep=endpoint):
            if not is_stale("metrics", _ln, f"{ticker}.json"):
                return
            data = await fmp_get(_ep, {"symbol": ticker})
            if data:
                save(data, "metrics", _ln, f"{ticker}.json")

        await batch_fetch(tickers, fetch_one, layer_name, log_every=100)
        log.info(f"  → {layer_name} done")

    log.info("Fetching financial scores...")

    async def fetch_score(ticker):
        if not is_stale("metrics", "financial-scores", f"{ticker}.json", ttl_hours=mode.scores_ttl_hours):
            return
        data = await fmp_get("financial-scores", {"symbol": ticker})
        if data:
            save(data, "metrics", "financial-scores", f"{ticker}.json")

    await batch_fetch(tickers, fetch_score, "scores", log_every=100)
    log.info("  → Financial scores done")

    log.info("Fetching owner earnings...")

    async def fetch_oe(ticker):
        if not is_stale("metrics", "owner-earnings", f"{ticker}.json"):
            return
        data = await fmp_get("owner-earnings", {"symbol": ticker, "limit": mode.quarterly_limit})
        if data:
            save(data, "metrics", "owner-earnings", f"{ticker}.json")

    await batch_fetch(tickers, fetch_oe, "owner-earnings", log_every=100)
    log.info("  → Owner earnings done")

# ---------------------------------------------------------------------------
# Layer 5: Analyst
# ---------------------------------------------------------------------------
async def fetch_analyst(tickers: list[str]):
    log.info("=" * 60)
    log.info("LAYER 5: Analyst")
    log.info("=" * 60)

    log.info("Fetching analyst estimates...")

    async def fetch_est(ticker):
        if not is_stale("analyst", "estimates", f"{ticker}.json"):
            return
        annual = await fmp_get("analyst-estimates", {"symbol": ticker, "period": "annual", "limit": mode.annual_limit})
        quarterly = await fmp_get("analyst-estimates", {"symbol": ticker, "period": "quarter", "limit": mode.quarterly_est_limit})
        if annual or quarterly:
            combined = {"annual": annual, "quarterly": quarterly}
            # Save current (overwritten daily — for quick lookups)
            save(combined, "analyst", "estimates", f"{ticker}.json")
            # Save dated archive (append-only — for revision tracking)
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            archive_path = DATA_DIR / "analyst" / "estimates-history" / ticker / f"{today}.json"
            if not archive_path.exists():
                save(combined, "analyst", "estimates-history", ticker, f"{today}.json")

    await batch_fetch(tickers, fetch_est, "estimates", log_every=100)
    log.info("  → Estimates done")

    log.info("Fetching price targets...")

    async def fetch_pt(ticker):
        if not is_stale("analyst", "price-targets", f"{ticker}.json"):
            return
        consensus = await fmp_get("price-target-consensus", {"symbol": ticker})
        summary = await fmp_get("price-target-summary", {"symbol": ticker})
        if consensus or summary:
            save({"consensus": consensus, "summary": summary}, "analyst", "price-targets", f"{ticker}.json")

    await batch_fetch(tickers, fetch_pt, "price-targets", log_every=100)
    log.info("  → Price targets done")

    log.info("Fetching grades...")

    async def fetch_gr(ticker):
        if not is_stale("analyst", "grades", f"{ticker}.json"):
            return
        data = await fmp_get("grades", {"symbol": ticker, "limit": mode.grades_limit})
        if data:
            save(data, "analyst", "grades", f"{ticker}.json")

    await batch_fetch(tickers, fetch_gr, "grades", log_every=100)
    log.info("  → Grades done")

    log.info("Fetching grades consensus...")

    async def fetch_gc(ticker):
        if not is_stale("analyst", "grades-consensus", f"{ticker}.json"):
            return
        data = await fmp_get("grades-consensus", {"symbol": ticker})
        if data:
            save(data, "analyst", "grades-consensus", f"{ticker}.json")

    await batch_fetch(tickers, fetch_gc, "grades-consensus", log_every=100)
    log.info("  → Grades consensus done")

# ---------------------------------------------------------------------------
# Layer 6: Earnings
# ---------------------------------------------------------------------------
async def fetch_earnings(tickers: list[str]):
    log.info("=" * 60)
    log.info("LAYER 6: Earnings")
    log.info("=" * 60)

    log.info("Fetching earnings history...")

    async def fetch_one(ticker):
        if not is_stale("earnings", "calendar", f"{ticker}.json"):
            return
        data = await fmp_get("earnings", {"symbol": ticker, "limit": mode.earnings_limit})
        if data:
            save(data, "earnings", "calendar", f"{ticker}.json")

    await batch_fetch(tickers, fetch_one, "earnings", log_every=100)
    log.info("  → Earnings history done")

# ---------------------------------------------------------------------------
# Layer 6b: Earnings Transcripts
# ---------------------------------------------------------------------------
async def fetch_transcripts(tickers: list[str]):
    log.info("=" * 60)
    log.info("LAYER 6b: Transcripts")
    log.info("=" * 60)

    years = list(range(mode.transcript_start_year, mode.transcript_end_year + 1))
    quarters = [1, 2, 3, 4]

    # Build flat list of uncached jobs
    jobs = []
    for ticker in tickers:
        for year in years:
            for quarter in quarters:
                fname = f"{ticker}_{year}_Q{quarter}.json"
                # Transcripts are immutable — skip only if file actually exists
                fpath = DATA_DIR / "earnings" / "transcripts" / fname
                if fpath.exists():
                    continue
                jobs.append((ticker, year, quarter, fname))

    total_slots = len(tickers) * len(years) * 4
    cached = total_slots - len(jobs)
    log.info(f"Transcripts: {len(jobs)} to fetch, {cached} cached ({len(tickers)} tickers × {len(years)} years)")

    if not jobs:
        log.info("  → Transcripts done (all cached)")
        return

    fetched = 0

    async def fetch_one(job):
        nonlocal fetched
        ticker, year, quarter, fname = job
        data = await fmp_get("earning-call-transcript", {
            "symbol": ticker,
            "year": year,
            "quarter": quarter,
        })
        if data and len(data) > 0:
            save(data, "earnings", "transcripts", fname)
            fetched += 1

    await batch_fetch(jobs, fetch_one, "Transcripts", log_every=200)
    log.info(f"  → Transcripts done ({fetched} fetched, {cached} cached)")

# ---------------------------------------------------------------------------
# Layer 7: Catalysts
# ---------------------------------------------------------------------------
async def fetch_catalysts(tickers: list[str]):
    log.info("=" * 60)
    log.info("LAYER 7: Catalysts")
    log.info("=" * 60)

    log.info("Fetching insider trades...")

    async def fetch_insider(ticker):
        if not is_stale("catalysts", "insider-trades", f"{ticker}.json"):
            return
        data = await fmp_get("insider-trading/search", {"symbol": ticker, "limit": mode.insider_limit})
        if data:
            save(data, "catalysts", "insider-trades", f"{ticker}.json")

    await batch_fetch(tickers, fetch_insider, "insider-trades", log_every=100)
    log.info("  → Insider trades done")

    log.info("Fetching dividends...")

    async def fetch_div(ticker):
        if not is_stale("catalysts", "dividends", f"{ticker}.json", ttl_hours=mode.dividends_ttl_hours):
            return
        data = await fmp_get("dividends", {"symbol": ticker})
        if data:
            save(data, "catalysts", "dividends", f"{ticker}.json")

    await batch_fetch(tickers, fetch_div, "dividends", log_every=100)
    log.info("  → Dividends done")

    log.info("Fetching splits...")

    async def fetch_split(ticker):
        if not is_stale("catalysts", "splits", f"{ticker}.json", ttl_hours=mode.dividends_ttl_hours):
            return
        data = await fmp_get("splits", {"symbol": ticker})
        if data:
            save(data, "catalysts", "splits", f"{ticker}.json")

    await batch_fetch(tickers, fetch_split, "splits", log_every=100)
    log.info("  → Splits done")

# ---------------------------------------------------------------------------
# Layer 8: Sector benchmarks (always current)
# ---------------------------------------------------------------------------
async def fetch_sector():
    log.info("=" * 60)
    log.info("LAYER 8: Sector benchmarks")
    log.info("=" * 60)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    data = await fmp_get("sector-pe-snapshot", {"date": today})
    if data:
        save(data, "sector", "sector-pe", "latest.json")
        log.info(f"  Sector PE: {len(data)} sectors")

    data = await fmp_get("industry-pe-snapshot", {"date": today})
    if data:
        save(data, "sector", "industry-pe", "latest.json")
        log.info(f"  Industry PE: {len(data)} industries")

    data = await fmp_get("sector-performance-snapshot", {"date": today})
    if data:
        save(data, "sector", "sector-performance", "latest.json")
        log.info(f"  Sector performance: {len(data)} sectors")

# ---------------------------------------------------------------------------
# Layer 9: Macro (always current)
# ---------------------------------------------------------------------------
async def fetch_macro():
    log.info("=" * 60)
    log.info("LAYER 9: Macro")
    log.info("=" * 60)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    one_year_ago = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")

    data = await fmp_get("treasury-rates", {"from": "2015-01-01", "to": today})
    if data:
        save(data, "macro", "treasury-rates.json")
        log.info(f"  Treasury rates: {len(data)} days")

    thirty_days = (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")
    data = await fmp_get("economic-calendar", {"from": today, "to": thirty_days})
    if data:
        save(data, "macro", "economic-calendar.json")
        log.info(f"  Economic calendar: {len(data)} events")

    for indicator in ["GDP", "CPI", "unemployment"]:
        data = await fmp_get("economic-indicators", {"name": indicator})
        if data:
            save(data, "macro", f"{indicator.lower()}.json")
            log.info(f"  {indicator}: {len(data)} data points")

# ---------------------------------------------------------------------------
# Layer 10: Valuation
# ---------------------------------------------------------------------------
async def fetch_valuation(tickers: list[str]):
    log.info("=" * 60)
    log.info("LAYER 10: Valuation")
    log.info("=" * 60)

    log.info("Fetching DCF valuations...")

    async def fetch_one(ticker):
        if not is_stale("valuation", "dcf", f"{ticker}.json"):
            return
        data = await fmp_get("discounted-cash-flow", {"symbol": ticker})
        if data:
            save(data, "valuation", "dcf", f"{ticker}.json")

    await batch_fetch(tickers, fetch_one, "dcf", log_every=100)
    log.info("  → DCF done")

# ---------------------------------------------------------------------------
# Layer 11: News
# ---------------------------------------------------------------------------
async def fetch_news(tickers: list[str]):
    log.info("=" * 60)
    log.info("LAYER 11: News")
    log.info("=" * 60)

    log.info("Fetching stock news...")

    async def fetch_one(ticker):
        if not is_stale("news", f"{ticker}.json"):
            return
        limit = 200 if mode.backfill else 50
        data = await fmp_get("stock-news", {"symbol": ticker, "limit": limit})
        if data:
            save(data, "news", f"{ticker}.json")

    await batch_fetch(tickers, fetch_one, "news", log_every=100)
    log.info("  → News done")

# ---------------------------------------------------------------------------
# Universe resolution
# ---------------------------------------------------------------------------
def get_universe_tickers(ticker_filter: str = None) -> list[str]:
    if ticker_filter:
        return [t.strip().upper() for t in ticker_filter.split(",")]

    manifest = load("_meta", "universe-manifest.json")
    if manifest and "tickers" in manifest:
        return sorted(manifest["tickers"].keys())

    tickers = set()
    for index_file in ["sp500.json", "nasdaq.json", "dowjones.json"]:
        data = load("universe", index_file)
        if data:
            for item in data:
                sym = item.get("symbol")
                if sym:
                    tickers.add(sym)

    if tickers:
        return sorted(tickers)

    log.info("No universe data found. Fetching fresh...")
    fetch_universe()
    manifest = load("_meta", "universe-manifest.json")
    if manifest and "tickers" in manifest:
        return sorted(manifest["tickers"].keys())

    log.error("Could not build universe. Check API key and connectivity.")
    return []

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

        print("  === Pipeline Status ===")
        print(f"  Last run:  {data.get('started_at', 'unknown')}")
        print(f"  Finished:  {data.get('finished_at', 'unknown')}")
        print(f"  Mode:      {data.get('mode', 'unknown')}")
        print(f"  Requests:  {data.get('requests_made', '?')} made, {data.get('requests_failed', '?')} failed")
        print(f"  Tickers:   {data.get('tickers_processed', '?')}")

        errors = data.get("errors", [])
        if errors:
            print(f"  Errors ({len(errors)}):")
            for e in errors[:10]:
                print(f"    - {e}")
            if len(errors) > 10:
                print(f"    ... and {len(errors) - 10} more")
        else:
            print("  Errors:    None")
        print()
    except Exception as e:
        print(f"Error reading status: {e}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
LAYER_MAP = {
    "universe": "universe",
    "prices": "prices",
    "fundamentals": "fundamentals",
    "metrics": "metrics",
    "analyst": "analyst",
    "earnings": "earnings",
    "transcripts": "transcripts",
    "catalysts": "catalysts",
    "sector": "sector",
    "macro": "macro",
    "valuation": "valuation",
    "news": "news",
}


async def run_async(ticker_filter: str = None, layer_filter: str = None,
                    dry_run: bool = False, concurrency: int = DEFAULT_CONCURRENCY,
                    backfill: bool = False, since: str = None):
    global _bucket, _executor, DEFAULT_CONCURRENCY, mode

    mode = PipelineMode(backfill=backfill, since=since)
    DEFAULT_CONCURRENCY = concurrency
    _bucket = TokenBucket(RATE_LIMIT)
    _executor = ThreadPoolExecutor(max_workers=concurrency)

    stats["started_at"] = datetime.now(timezone.utc).isoformat()
    stats["mode"] = str(mode)

    if dry_run:
        tickers = ticker_filter.upper().split(",") if ticker_filter else ["(full universe)"]
        log.info("DRY RUN — no API calls will be made")
        log.info(f"Mode: {mode}")
        log.info(f"Tickers: {', '.join(tickers[:20])}{'...' if len(tickers) > 20 else ''}")
        log.info(f"Layer: {layer_filter or 'all'}")
        if backfill:
            log.info(f"Quarterly limit: {mode.quarterly_limit}, Transcript years: {mode.transcript_start_year}-{mode.transcript_end_year}")
        return

    if not API_KEY:
        log.error("FMP_API_KEY environment variable not set.")
        sys.exit(1)

    tickers = get_universe_tickers(ticker_filter)
    if not tickers:
        log.error("No tickers to process.")
        sys.exit(1)

    stats["tickers_processed"] = len(tickers)
    log.info(f"Pipeline starting: {len(tickers)} tickers (concurrency={concurrency})")
    log.info(f"Mode: {mode}")
    if layer_filter:
        log.info(f"Layer filter: {layer_filter}")

    run_all = layer_filter is None

    if run_all or layer_filter == "universe":
        fetch_universe(ticker_filter)
        await fetch_profiles(tickers)

    if run_all or layer_filter == "prices":
        await fetch_prices(tickers)

    if run_all or layer_filter == "fundamentals":
        await fetch_fundamentals(tickers)

    if run_all or layer_filter == "metrics":
        await fetch_metrics(tickers)

    if run_all or layer_filter == "analyst":
        await fetch_analyst(tickers)

    if run_all or layer_filter == "earnings":
        await fetch_earnings(tickers)

    if run_all or layer_filter == "transcripts":
        await fetch_transcripts(tickers)

    if run_all or layer_filter == "catalysts":
        await fetch_catalysts(tickers)

    if run_all or layer_filter == "sector":
        await fetch_sector()

    if run_all or layer_filter == "macro":
        await fetch_macro()

    if run_all or layer_filter == "valuation":
        await fetch_valuation(tickers)

    if run_all or layer_filter == "news":
        await fetch_news(tickers)

    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    save(stats, "_meta", "last-run.json")
    _executor.shutdown(wait=False)

    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info(f"  Mode:     {mode}")
    log.info(f"  Tickers:  {stats['tickers_processed']}")
    log.info(f"  Requests: {stats['requests_made']} made, {stats['requests_failed']} failed")
    log.info(f"  Errors:   {len(stats['errors'])}")
    if stats["errors"]:
        for e in stats["errors"][:5]:
            log.info(f"    - {e}")
    log.info("=" * 60)


def run(ticker_filter=None, layer_filter=None, dry_run=False,
        concurrency=DEFAULT_CONCURRENCY, backfill=False, since=None):
    asyncio.run(run_async(ticker_filter, layer_filter, dry_run, concurrency, backfill, since))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AlphaScout Data Pipeline (Async)")
    parser.add_argument("--ticker", type=str, help="Single ticker or comma-separated list")
    parser.add_argument("--layer", type=str, choices=list(LAYER_MAP.keys()), help="Single layer to fetch")
    parser.add_argument("--status", action="store_true", help="Show last run status")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without fetching")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Max concurrent requests")

    # Mode flags
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--backfill", action="store_true", help="Full historical backfill (use with --since)")
    mode_group.add_argument("--refresh", action="store_true", default=True, help="Refresh latest data only (default)")

    parser.add_argument("--since", type=str, default=None,
                        help="Backfill start date YYYY-MM-DD (default: 2015-01-01). Only used with --backfill.")

    args = parser.parse_args()

    if args.since and not args.backfill:
        parser.error("--since requires --backfill")

    if args.status:
        show_status()
    else:
        run(
            ticker_filter=args.ticker,
            layer_filter=args.layer,
            dry_run=args.dry_run,
            concurrency=args.concurrency,
            backfill=args.backfill,
            since=args.since,
        )
