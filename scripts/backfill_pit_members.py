"""
Backfill prices + fundamentals + profile for delisted historical index members.

Audience: the survivorship-bias fix. After ingesting the change log, we know
~144 names were S&P 500 members at some point during 2018–2023 but aren't in
our `prices` table (because they were acquired, renamed, or delisted before
we started ingesting). To run a PIT-correct backtest, we need their daily
market data, fundamentals, earnings calendar, and a profile row.

Scope: this script targets a SPECIFIC set of symbols (the missing-historical
set) rather than the whole delisted catalog (which has 2k+ names, most
irrelevant micro-caps). Default scope = ever-members of S&P 500 / NASDAQ-100
/ DJIA from 2015-01-01 to today, minus what we already have prices for.

Usage:
    FMP_API_KEY=... MARKET_DB_PATH=/path/to/market.db \\
    python3 scripts/backfill_pit_members.py
        [--start 2015-01-01]
        [--indices sp500,nasdaq,dowjones]
        [--limit 999]                  # cap for testing
        [--symbols COMMA,SEP,LIST]     # override auto-detection

It writes into the same tables daily_update.py writes: prices, income,
balance, cashflow, earnings, universe_profiles. INSERT OR REPLACE so it's
idempotent.

Rate limit: ~250 req/min (FMP stable tier). 144 symbols × 6 endpoints ≈ 864
calls → ~4 minutes.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import MARKET_DB_PATH
from universe_history import ever_members, ensure_schema as ensure_pit_schema


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
            req = Request(url, headers={"User-Agent": "alphascout-pit-backfill/1.0"})
            with urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception:
            if attempt == 2:
                return None
            time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# Insert helpers — mirror daily_update.py mappers + insert statements 1:1
# ---------------------------------------------------------------------------
def _ins_prices(conn, sym, recs):
    if not recs:
        return 0
    rows = [(sym, r.get("date"), r.get("open"), r.get("high"), r.get("low"),
             r.get("close"), r.get("volume"), r.get("changePercent"), r.get("vwap"))
            for r in recs if r.get("date")]
    conn.executemany(
        "INSERT OR REPLACE INTO prices (symbol,date,open,high,low,close,volume,change_pct,vwap) "
        "VALUES (?,?,?,?,?,?,?,?,?)", rows,
    )
    return len(rows)


def _ins_income(conn, sym, recs):
    if not recs:
        return 0
    rows = [(sym, r.get("date"), r.get("fiscalYear"), r.get("period"),
             r.get("revenue"), r.get("grossProfit"), r.get("operatingIncome"),
             r.get("netIncome"), r.get("ebitda"), r.get("eps"),
             r.get("epsDiluted"), r.get("weightedAverageShsOutDil"))
            for r in recs if r.get("date")]
    conn.executemany(
        "INSERT OR REPLACE INTO income (symbol,date,fiscal_year,period,revenue,gross_profit,"
        "operating_income,net_income,ebitda,eps,eps_diluted,shares_diluted) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows,
    )
    return len(rows)


def _ins_balance(conn, sym, recs):
    if not recs:
        return 0
    rows = []
    for r in recs:
        if not r.get("date"):
            continue
        cash = r.get("cashAndCashEquivalents", 0) or 0
        total_debt = (r.get("shortTermDebt", 0) or 0) + (r.get("longTermDebt", 0) or 0)
        rows.append((sym, r.get("date"), r.get("fiscalYear"), r.get("period"), cash,
                     r.get("inventory"), r.get("totalCurrentAssets"), r.get("totalAssets"),
                     r.get("totalCurrentLiabilities"), r.get("longTermDebt"), total_debt,
                     r.get("totalLiabilities"), r.get("totalStockholdersEquity"),
                     total_debt - cash))
    conn.executemany(
        "INSERT OR REPLACE INTO balance (symbol,date,fiscal_year,period,cash,inventory,"
        "total_current_assets,total_assets,total_current_liabilities,long_term_debt,"
        "total_debt,total_liabilities,total_equity,net_debt) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows,
    )
    return len(rows)


def _ins_cashflow(conn, sym, recs):
    if not recs:
        return 0
    rows = []
    for r in recs:
        if not r.get("date"):
            continue
        ocf = r.get("netCashProvidedByOperatingActivities")
        capex = r.get("investmentsInPropertyPlantAndEquipment")
        fcf = (ocf + capex) if ocf is not None and capex is not None else None
        rows.append((sym, r.get("date"), r.get("fiscalYear"), r.get("period"),
                     ocf, capex, fcf, r.get("commonDividendsPaid"),
                     r.get("commonStockRepurchased")))
    conn.executemany(
        "INSERT OR REPLACE INTO cashflow (symbol,date,fiscal_year,period,operating_cf,"
        "capex,free_cash_flow,dividends_paid,stock_repurchased) "
        "VALUES (?,?,?,?,?,?,?,?,?)", rows,
    )
    return len(rows)


def _ins_earnings(conn, sym, recs):
    if not recs:
        return 0
    rows = [(sym, r.get("date"), r.get("epsActual"), r.get("epsEstimated"),
             r.get("revenueActual"), r.get("revenueEstimated"))
            for r in recs if r.get("date")]
    conn.executemany(
        "INSERT OR REPLACE INTO earnings (symbol,date,eps_actual,eps_estimated,"
        "revenue_actual,revenue_estimated) VALUES (?,?,?,?,?,?)", rows,
    )
    return len(rows)


def _ins_profile(conn, sym, rec_list):
    """Single-row profile insert. rec_list is the FMP response (list of 1)."""
    if not rec_list:
        return 0
    r = rec_list[0] if isinstance(rec_list, list) else rec_list
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        "INSERT OR REPLACE INTO universe_profiles "
        "(symbol,name,sector,industry,market_cap,exchange,country,beta,price,volume,"
        "avg_volume,is_actively_trading,ipo_date,is_etf,is_adr,cik,isin,cusip,description,synced_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (sym, r.get("companyName", "") or "", r.get("sector", "") or "",
         r.get("industry", "") or "", r.get("marketCap"), r.get("exchange", "") or "",
         r.get("country", "") or "", r.get("beta"), r.get("price"),
         r.get("volume"), r.get("averageVolume"),
         int(bool(r.get("isActivelyTrading"))), r.get("ipoDate"),
         int(bool(r.get("isEtf"))), int(bool(r.get("isAdr"))),
         r.get("cik"), r.get("isin"), r.get("cusip"),
         r.get("description"), now),
    )
    return 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def determine_missing(conn: sqlite3.Connection, indices: list[str], start: str) -> list[str]:
    """Union of all-ever-members across the requested indices, in window
    [start, today], minus tickers that already have prices in the DB."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    universe: set[str] = set()
    for idx in indices:
        universe |= ever_members(conn, idx, start, today)
    if not universe:
        return []
    placeholders = ",".join("?" * len(universe))
    have = {r[0] for r in conn.execute(
        f"SELECT DISTINCT symbol FROM prices WHERE symbol IN ({placeholders})",
        tuple(universe))}
    return sorted(universe - have)


def backfill_one(conn: sqlite3.Connection, sym: str) -> dict[str, int]:
    """Pull all 6 endpoints for one ticker, insert into market.db, return per-endpoint counts."""
    counts: dict[str, int] = {}
    # Prices: historical-price-eod/full — full history (no `from` cap)
    counts["prices"] = _ins_prices(conn, sym, _fmp("historical-price-eod/full", {"symbol": sym}) or [])
    # Profile
    counts["profile"] = _ins_profile(conn, sym, _fmp("profile", {"symbol": sym}))
    # Quarterly fundamentals
    counts["income"] = _ins_income(conn, sym, _fmp("income-statement", {"symbol": sym, "period": "quarter", "limit": 200}) or [])
    counts["balance"] = _ins_balance(conn, sym, _fmp("balance-sheet-statement", {"symbol": sym, "period": "quarter", "limit": 200}) or [])
    counts["cashflow"] = _ins_cashflow(conn, sym, _fmp("cash-flow-statement", {"symbol": sym, "period": "quarter", "limit": 200}) or [])
    # Earnings calendar
    counts["earnings"] = _ins_earnings(conn, sym, _fmp("earnings", {"symbol": sym, "limit": 200}) or [])
    return counts


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2015-01-01")
    p.add_argument("--indices", default="sp500,nasdaq,dowjones")
    p.add_argument("--limit", type=int, default=99999)
    p.add_argument("--symbols", default=None)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    indices = [i.strip() for i in args.indices.split(",") if i.strip()]
    conn = sqlite3.connect(MARKET_DB_PATH)
    ensure_pit_schema(conn)

    if args.symbols:
        missing = [s.strip() for s in args.symbols.split(",") if s.strip()]
        print(f"Symbols (from --symbols): {len(missing)}")
    else:
        missing = determine_missing(conn, indices, args.start)
        print(f"Indices: {indices}  start={args.start}")
        print(f"Missing historical members to backfill: {len(missing)}")

    if not missing:
        print("Nothing to backfill.")
        return

    missing = missing[:args.limit]
    if args.dry_run:
        print("DRY RUN — not fetching. Symbols:")
        for s in missing:
            print(f"  {s}")
        return

    print(f"Backfilling {len(missing)} tickers...")
    succ = 0
    fail: list[str] = []
    for i, sym in enumerate(missing, 1):
        try:
            counts = backfill_one(conn, sym)
            conn.commit()
            total = sum(counts.values())
            if counts["prices"] > 0:
                succ += 1
                tag = "✓"
            else:
                fail.append(sym)
                tag = "⚠"
            print(f"  [{i:>3d}/{len(missing)}] {tag} {sym:8s}  "
                  f"prices={counts['prices']:>5d} profile={counts['profile']} "
                  f"income={counts['income']:>3d} bal={counts['balance']:>3d} "
                  f"cf={counts['cashflow']:>3d} earn={counts['earnings']:>3d}")
        except Exception as e:
            fail.append(sym)
            print(f"  [{i:>3d}/{len(missing)}] ✗ {sym}: {str(e)[:80]}")

    print(f"\nSummary: {succ}/{len(missing)} succeeded, {len(fail)} failed")
    if fail:
        print(f"Failed symbols: {fail}")


if __name__ == "__main__":
    main()
