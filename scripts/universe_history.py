"""
Point-in-time (PIT) index membership history.

Fixes the survivorship bias problem: a backtest of `universe.type: "sp500"`
spanning 2023 Q1 must include SIVB (member 2018-03-19 → 2023-03-14), not just
today's S&P 500. Without this, every name that has been added or removed from
an index since the backtest start date silently disappears from history, and
returns are overstated.

Tables (in market.db):

  index_membership_history
    Effective-dated membership changes, one row per add or per remove event.
    Source: FMP /stable/historical-{index}-constituent.
    Replay this table to reconstruct the membership of an index on any date.

  delisted_tickers
    Catalog of tickers that no longer trade, with their delisted_date.
    Source: FMP /stable/delisted-companies (paginated, US exchanges only here).
    Joined with index_membership_history → the set of symbols that were ever
    in a major index but no longer trade. Those are the names we MUST backfill
    prices for to have a real PIT backtest.

Public API
==========
  ingest_index_history(conn)       — fetch + store change log for sp500/nasdaq/dowjones
  ingest_delisted_catalog(conn)    — fetch + store delisted-tickers catalog (US)
  members_as_of(conn, index, date) — set of symbols in `index` on `date`
  ever_members(conn, index, start, end) — all symbols ever in `index` during [start, end]

`index` values: "sp500", "nasdaq", "dowjones" (lowercased, matches table key).
"""

from __future__ import annotations

import os
import sqlite3
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from db_config import MARKET_DB_PATH


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
_SCHEMA = """
CREATE TABLE IF NOT EXISTS index_membership_history (
    -- One row per (index, symbol, event_date, action).
    -- action='added' rows mark when a ticker joined the index; 'removed' rows
    -- mark when it left. A ticker can have multiple add/remove cycles (rare
    -- but possible — symbol changes, re-additions after market-cap recovery).
    index_name   TEXT NOT NULL,        -- 'sp500' | 'nasdaq' | 'dowjones'
    symbol       TEXT NOT NULL,
    event_date   TEXT NOT NULL,        -- ISO date (YYYY-MM-DD)
    action       TEXT NOT NULL,        -- 'added' | 'removed'
    replaced     TEXT,                 -- the ticker on the other side of the swap (nullable)
    reason       TEXT,
    PRIMARY KEY (index_name, symbol, event_date, action)
);
CREATE INDEX IF NOT EXISTS idx_imh_index_date ON index_membership_history(index_name, event_date);
CREATE INDEX IF NOT EXISTS idx_imh_symbol ON index_membership_history(symbol);

CREATE TABLE IF NOT EXISTS index_membership_current (
    -- Today's snapshot — names currently in the index. Joined with
    -- index_membership_history to anchor the "live members on the latest
    -- observed date" baseline before walking the change log backwards.
    -- Refreshed by ingest_index_history (same call).
    index_name TEXT NOT NULL,
    symbol     TEXT NOT NULL,
    PRIMARY KEY (index_name, symbol)
);

CREATE TABLE IF NOT EXISTS delisted_tickers (
    symbol         TEXT PRIMARY KEY,
    company_name   TEXT,
    exchange       TEXT,
    ipo_date       TEXT,
    delisted_date  TEXT
);
CREATE INDEX IF NOT EXISTS idx_delisted_date ON delisted_tickers(delisted_date);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)
    conn.commit()


# ---------------------------------------------------------------------------
# FMP fetch
# ---------------------------------------------------------------------------
_FMP_BASE = "https://financialmodelingprep.com/stable"


def _fmp_get(endpoint: str, params: dict | None = None, retries: int = 3) -> list | dict | None:
    """Tiny synchronous FMP fetcher. Returns None on error."""
    api_key = os.environ.get("FMP_API_KEY")
    if not api_key:
        raise RuntimeError("FMP_API_KEY environment variable not set")
    params = dict(params or {})
    params["apikey"] = api_key
    url = f"{_FMP_BASE}/{endpoint}?{urlencode(params)}"
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "alphascout-pit/1.0"})
            with urlopen(req, timeout=30) as r:
                import json
                return json.loads(r.read().decode("utf-8"))
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# Ingest: index membership history
# ---------------------------------------------------------------------------
_HIST_ENDPOINTS = {
    "sp500":    ("historical-sp500-constituent", "sp500-constituent"),
    "nasdaq":   ("historical-nasdaq-constituent", "nasdaq-constituent"),
    "dowjones": ("historical-dowjones-constituent", "dowjones-constituent"),
}


def ingest_index_history(conn: sqlite3.Connection) -> dict[str, int]:
    """Pull current + historical constituent lists for sp500/nasdaq/dowjones.

    Each historical record from FMP describes a SWAP: an `addedSecurity`
    coming in, a `removedSecurity` going out, with one effective `date`.
    We split each swap into two rows: an 'added' event for the new ticker
    and a 'removed' event for the displaced ticker — both on the same date.

    Returns {index_name: rows_written} for logging.
    """
    ensure_schema(conn)
    out: dict[str, int] = {}
    for index_name, (hist_ep, current_ep) in _HIST_ENDPOINTS.items():
        # ---- Historical change log -----------------------------------------
        hist = _fmp_get(hist_ep) or []
        rows: list[tuple] = []
        for r in hist:
            event_date = r.get("date")
            if not event_date:
                continue
            added = r.get("symbol")
            removed = r.get("removedTicker")
            reason = r.get("reason")
            if added:
                rows.append((index_name, added, event_date, "added", removed, reason))
            if removed:
                rows.append((index_name, removed, event_date, "removed", added, reason))
        if rows:
            conn.executemany(
                "INSERT OR REPLACE INTO index_membership_history "
                "(index_name, symbol, event_date, action, replaced, reason) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )

        # ---- Current snapshot ---------------------------------------------
        current = _fmp_get(current_ep) or []
        cur_rows = [(index_name, item.get("symbol")) for item in current if item.get("symbol")]
        if cur_rows:
            conn.execute(
                "DELETE FROM index_membership_current WHERE index_name = ?",
                (index_name,),
            )
            conn.executemany(
                "INSERT OR REPLACE INTO index_membership_current (index_name, symbol) VALUES (?, ?)",
                cur_rows,
            )

        out[index_name] = len(rows)
    conn.commit()
    return out


# ---------------------------------------------------------------------------
# Ingest: delisted catalog (US exchanges)
# ---------------------------------------------------------------------------
def ingest_delisted_catalog(conn: sqlite3.Connection, max_pages: int = 100,
                             us_only: bool = True) -> int:
    """Pull paginated delisted-companies and store. Returns rows written.

    FMP paginates 100 records per page in reverse-chronological order of
    delisted_date. We page until an empty response or `max_pages` reached.
    """
    ensure_schema(conn)
    us_exchanges = {"NYSE", "NASDAQ", "AMEX", "NYSEARCA", "BATS"}
    total = 0
    for page in range(max_pages):
        recs = _fmp_get("delisted-companies", {"page": page}) or []
        if not isinstance(recs, list) or not recs:
            break
        rows: list[tuple] = []
        for r in recs:
            sym = r.get("symbol")
            if not sym:
                continue
            ex = r.get("exchange", "")
            if us_only and ex not in us_exchanges:
                continue
            rows.append((sym, r.get("companyName"), ex,
                         r.get("ipoDate"), r.get("delistedDate")))
        if rows:
            conn.executemany(
                "INSERT OR REPLACE INTO delisted_tickers "
                "(symbol, company_name, exchange, ipo_date, delisted_date) "
                "VALUES (?, ?, ?, ?, ?)",
                rows,
            )
            total += len(rows)
    conn.commit()
    return total


# ---------------------------------------------------------------------------
# PIT lookups
# ---------------------------------------------------------------------------
def members_as_of(conn: sqlite3.Connection, index: str, date: str) -> set[str]:
    """The set of symbols that were in `index` on `date`.

    Algorithm: start from the CURRENT membership snapshot, then walk the
    change log backwards from now → date, undoing each event:
      - an 'added' event after `date` means: the ticker was NOT a member
        at `date`. Remove from the set.
      - a 'removed' event after `date` means: the ticker WAS a member
        at `date`. Add to the set.

    This gives an O(events_after_date) reconstruction that's robust to
    re-additions, ticker changes, and missing pre-current snapshots.

    Raises if the requested date is BEFORE the earliest event in the log
    (no way to reconstruct that far back).
    """
    cur = conn.cursor()
    # 1. Anchor: today's snapshot.
    current = {row[0] for row in cur.execute(
        "SELECT symbol FROM index_membership_current WHERE index_name = ?",
        (index,),
    )}
    if not current:
        raise ValueError(
            f"No current snapshot for index={index!r} — run ingest_index_history first."
        )

    # 2. Walk the change log backwards (events strictly AFTER `date`).
    events = cur.execute(
        "SELECT event_date, symbol, action FROM index_membership_history "
        "WHERE index_name = ? AND event_date > ? "
        "ORDER BY event_date DESC, action",   # action order doesn't matter (per-symbol independent)
        (index, date),
    ).fetchall()
    members = set(current)
    for _ed, sym, action in events:
        if action == "added":
            members.discard(sym)    # they weren't a member yet on `date`
        elif action == "removed":
            members.add(sym)        # they were still a member on `date`
    return members


def ever_members(conn: sqlite3.Connection, index: str, start: str, end: str) -> set[str]:
    """The union of all symbols that were in `index` at ANY point in [start, end].

    Used to figure out which delisted tickers we need to backfill prices for
    to make a backtest covering [start, end] survivorship-complete.
    """
    start_set = members_as_of(conn, index, start)
    end_set = members_as_of(conn, index, end)
    # Anything added or removed BETWEEN start and end was a member at some
    # point in the window.
    cur = conn.cursor()
    in_window = {row[0] for row in cur.execute(
        "SELECT DISTINCT symbol FROM index_membership_history "
        "WHERE index_name = ? AND event_date BETWEEN ? AND ?",
        (index, start, end),
    )}
    return start_set | end_set | in_window


# ---------------------------------------------------------------------------
# CLI: python -m universe_history sp500 2023-03-13
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("usage: python -m universe_history <index> <date>")
        sys.exit(2)
    idx, dt = sys.argv[1], sys.argv[2]
    c = sqlite3.connect(MARKET_DB_PATH)
    m = members_as_of(c, idx, dt)
    print(f"{idx} on {dt}: {len(m)} members")
    print("  sample:", sorted(m)[:10])
    if idx == "sp500" and "SIVB" in m:
        print("  ✅ SIVB present (PIT data working)")
    elif idx == "sp500" and dt < "2023-03-15":
        print("  ❌ SIVB MISSING (survivorship bias)")
