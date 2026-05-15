"""
Backfill ISIN + CUSIP for every symbol in universe_profiles.

Why this script exists separately: the bulk daily pipelines (daily_update.py,
backfill.py) already write the profile JSON to disk. _sync_universe_profiles
in server/api.py reads those JSONs into the DB at API startup. ISIN/CUSIP
are present in the JSON payload (FMP /profile returns them) but earlier
versions of the schema didn't have columns for them, so the DB rows are NULL.

This script hits FMP /profile fresh for every symbol and writes the resulting
isin/cusip directly to universe_profiles. Idempotent — INSERT OR REPLACE.

Usage:
    FMP_API_KEY=... MARKET_DB_PATH=/path/to/market.db \\
    python3 scripts/backfill_isin.py
        [--limit 999]                  # cap for testing
        [--symbols COMMA,SEP,LIST]     # specific symbols only
        [--only-missing]               # default: refresh all; set to skip rows that already have isin

Rate limit: ~250 req/min (FMP stable tier). ~700 symbols → ~3 minutes.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import MARKET_DB_PATH


_FMP_BASE = "https://financialmodelingprep.com/stable"
_RATE_SLEEP_S = 60.0 / 250   # 250 req/min


def _fmp(endpoint: str, params: dict | None = None) -> list | dict | None:
    api_key = os.environ.get("FMP_API_KEY")
    if not api_key:
        raise RuntimeError("FMP_API_KEY not set")
    params = dict(params or {})
    params["apikey"] = api_key
    url = f"{_FMP_BASE}/{endpoint}?{urlencode(params)}"
    time.sleep(_RATE_SLEEP_S)
    for attempt in range(3):
        try:
            req = Request(url, headers={"User-Agent": "alphascout-isin/1.0"})
            with urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception:
            if attempt == 2:
                return None
            time.sleep(2 ** attempt)
    return None


def ensure_columns(conn: sqlite3.Connection) -> None:
    """Add isin / cusip columns to universe_profiles if a stale schema is in play."""
    existing = {r[1] for r in conn.execute("PRAGMA table_info(universe_profiles)").fetchall()}
    for col in ("isin", "cusip"):
        if col not in existing:
            conn.execute(f"ALTER TABLE universe_profiles ADD COLUMN {col} TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_up_isin ON universe_profiles(isin)")
    conn.commit()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=99999)
    p.add_argument("--symbols", default=None)
    p.add_argument("--only-missing", action="store_true",
                    help="Skip rows that already have isin populated.")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    conn = sqlite3.connect(MARKET_DB_PATH)
    ensure_columns(conn)

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    elif args.only_missing:
        symbols = [r[0] for r in conn.execute(
            "SELECT symbol FROM universe_profiles WHERE isin IS NULL OR isin = '' ORDER BY symbol"
        ).fetchall()]
    else:
        symbols = [r[0] for r in conn.execute(
            "SELECT symbol FROM universe_profiles ORDER BY symbol"
        ).fetchall()]

    symbols = symbols[:args.limit]
    print(f"Target symbols: {len(symbols)}  (mode: "
          f"{'specific' if args.symbols else ('only-missing' if args.only_missing else 'all')})")
    if not symbols:
        print("Nothing to do.")
        return
    if args.dry_run:
        for s in symbols[:50]:
            print(f"  {s}")
        if len(symbols) > 50:
            print(f"  ... and {len(symbols)-50} more")
        return

    succ = 0
    fail: list[str] = []
    for i, sym in enumerate(symbols, 1):
        d = _fmp("profile", {"symbol": sym})
        rec = d[0] if isinstance(d, list) and d else (d if isinstance(d, dict) else None)
        if not rec:
            fail.append(sym)
            print(f"  [{i:>4d}/{len(symbols)}] ✗ {sym}: no data")
            continue
        isin = rec.get("isin")
        cusip = rec.get("cusip")
        cik = rec.get("cik")
        conn.execute(
            "UPDATE universe_profiles SET isin = ?, cusip = ?, cik = COALESCE(?, cik) "
            "WHERE symbol = ?",
            (isin, cusip, cik, sym),
        )
        if isin:
            succ += 1
            tag = "✓"
        else:
            tag = "⚠ (no isin)"
        if i % 25 == 0 or i == 1 or i == len(symbols):
            print(f"  [{i:>4d}/{len(symbols)}] {tag} {sym:8s}  isin={isin}  cusip={cusip}")
        conn.commit()

    print(f"\nSummary: {succ}/{len(symbols)} got ISIN, {len(fail)} failed entirely.")
    if fail:
        print(f"Failed: {fail[:20]}{' ...' if len(fail)>20 else ''}")


if __name__ == "__main__":
    main()
