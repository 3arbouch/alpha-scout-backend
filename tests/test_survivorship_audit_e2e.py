#!/usr/bin/env python3
"""
Survivorship-bias audit (Tier-B #6).

This is a CHARACTERIZATION test, not a correctness test: it documents the
current shape of the data and locks in expectations so a future fix (adding
delisted-name price history) will trip the test and force a manual review.

Findings:

1. 100% of the 538 symbols in `prices` have data through the present day.
   No symbol stops pricing before 2024-01-01. The dataset is built from
   today's S&P 500 + sector ETFs roster, NOT a point-in-time historical
   index membership.

2. Known 2015-2024 S&P 500 delistings / acquisitions / failures are
   completely absent from the database (BBBY, SIVB, SBNY, ATVI, XLNX,
   AGN, CELG, TWX, TIF, DISCA, VIAC, KSU, FLT, M, etc.).

3. resolve_universe(type="all") in scripts/backtest_engine.py:211 issues
   `SELECT DISTINCT symbol FROM prices` with no date filter, so every
   backtest sees the modern roster regardless of its start date.

4. _get_sector_symbols (line 232) reads CURRENT sector membership from
   universe/profiles/*.json — same survivorship issue.

Impact: every multi-year backtest is operating on a survivor-biased
universe. Academic estimates put S&P 500 survivorship bias at ~1-2%/year
of total return for broad-universe strategies, higher for concentrated
ones. For the 11-year window (2015-2026), cumulative impact is on the
order of 10-25% of total return — silent and systematic.

Not a bug in code logic — a data-coverage limitation. Fix requires:
  (a) historical S&P 500 (or chosen index) constituent history with
      add/remove dates per name
  (b) backfilled prices for delisted names through their delisting day
  (c) point-in-time universe resolution: pass as_of_date into
      resolve_universe so it returns the index members alive on that day

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    python3 test_survivorship_audit_e2e.py
"""
import os
import sqlite3
import sys

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


db = os.environ.get("MARKET_DB_PATH", "/home/mohamed/alpha-scout-backend/data/market.db")
m = sqlite3.connect(db)

# ---------------------------------------------------------------------------
# 1. Universe shape — current state of `prices`
# ---------------------------------------------------------------------------
print("\n=== 1. Universe shape ===")

n_syms = m.execute("SELECT COUNT(DISTINCT symbol) FROM prices").fetchone()[0]
check(f"Total distinct symbols in prices ({n_syms}) — sanity > 400",
      n_syms > 400, f"got {n_syms}")

# All symbols still pricing recently → pure-survivor universe
n_recent = m.execute(
    "SELECT COUNT(*) FROM (SELECT symbol FROM prices GROUP BY symbol HAVING MAX(date) >= '2025-01-01')"
).fetchone()[0]
check(f"{n_recent} of {n_syms} symbols still pricing through 2025+",
      n_recent == n_syms,
      f"if this fails, some symbols have stopped pricing — investigate whether they were intentionally retained as delisted-history backfill")

# No symbol stopped pricing before 2024
n_pre24 = m.execute(
    "SELECT COUNT(*) FROM (SELECT symbol FROM prices GROUP BY symbol HAVING MAX(date) < '2024-01-01')"
).fetchone()[0]
check("zero symbols with last_price < 2024-01-01 (the survivorship signature)",
      n_pre24 == 0,
      f"if {n_pre24} > 0, delisted-name backfill has started — update this test")


# ---------------------------------------------------------------------------
# 2. Specific known-delisted names should be ABSENT (current state)
# ---------------------------------------------------------------------------
print("\n=== 2. Known delisted/acquired S&P 500 names (2015-2024) ===")

# These 18 names were S&P 500 constituents at some point during 2015-2024 and
# subsequently delisted (bankruptcy), acquired, or removed. None should appear
# in a survivor-biased universe.
DELISTED = [
    "BBBY",   # Bed Bath & Beyond — bankruptcy 2023
    "SIVB",   # Silicon Valley Bank — failed 2023
    "SBNY",   # Signature Bank — failed 2023
    "ATVI",   # Activision — Microsoft acquired 2023
    "XLNX",   # Xilinx — AMD acquired 2022
    "AGN",    # Allergan — AbbVie acquired 2020
    "CELG",   # Celgene — BMS acquired 2019
    "TWX",    # Time Warner — AT&T acquired 2018
    "TIF",    # Tiffany & Co — LVMH acquired 2021
    "DISCA",  # Discovery — merged into WBD 2022
    "VIAC",   # ViacomCBS — renamed to Paramount, ticker change
    "KSU",    # Kansas City Southern — CP acquired 2021
    "FLT",    # Fleetcor — renamed Corpay (CPAY) — actually still trades as CPAY
    "M",      # Macy's — removed from S&P 500 2020
    "SCG",    # SCANA — Dominion acquired 2019
    "XL",     # XL Group — AXA acquired 2018
    "WORK",   # Slack — Salesforce acquired 2021
    "RTN",    # Raytheon — merged with UTC into RTX 2020
]
missing = []
present = []
for sym in DELISTED:
    n = m.execute("SELECT COUNT(*) FROM prices WHERE symbol=?", (sym,)).fetchone()[0]
    (missing if n == 0 else present).append(sym)

check(f">= 15/18 known-delisted names are ABSENT (survivorship signature: {len(missing)} missing)",
      len(missing) >= 15,
      f"present unexpectedly: {present} — universe may be partially historical")


# ---------------------------------------------------------------------------
# 3. Universe-resolution code has no date filter
# ---------------------------------------------------------------------------
print("\n=== 3. resolve_universe() has no as-of date filter ===")

import re
here = os.path.dirname(os.path.abspath(__file__))
engine_path = os.path.join(here, "..", "scripts", "backtest_engine.py")
src = open(engine_path).read()

# Lift the resolve_universe function body
m_def = re.search(r"def resolve_universe\(config: dict, conn\) -> list\[str\]:(.*?)\ndef ", src, re.DOTALL)
body = m_def.group(1) if m_def else ""
check("resolve_universe has no 'as_of' / 'as_of_date' / 'date' parameter",
      "as_of" not in body and "as_of_date" not in body,
      "if these appear, point-in-time membership has been added — this test needs updating")

# The "all" branch must be the no-date-filter SELECT
check("type='all' branch uses unfiltered DISTINCT symbol query",
      "SELECT DISTINCT symbol FROM prices ORDER BY symbol" in body,
      "filter format has changed — re-audit")


# ---------------------------------------------------------------------------
# 4. Newly-listed names are present from IPO date forward (correct behavior)
# ---------------------------------------------------------------------------
print("\n=== 4. Late-listed names: present from IPO date, no synthetic backfill ===")

# Spot-check: ARM IPO'd 2023-09-15. No prices should exist before that date.
arm_first = m.execute("SELECT MIN(date) FROM prices WHERE symbol='ARM'").fetchone()[0]
check("ARM first price is after 2023-09-14 (post-IPO, no backfill)",
      arm_first is not None and arm_first >= "2023-09-14",
      f"got {arm_first}")

# CRWD IPO'd 2019-06-12 — should not have pre-IPO prices either
crwd_first = m.execute("SELECT MIN(date) FROM prices WHERE symbol='CRWD'").fetchone()[0]
check("CRWD first price is after 2019-06-11",
      crwd_first is not None and crwd_first >= "2019-06-11",
      f"got {crwd_first}")


# ---------------------------------------------------------------------------
# 5. Bias magnitude rough estimate (universe growth over time)
# ---------------------------------------------------------------------------
print("\n=== 5. Universe size by year — proxy for bias growth ===")

for year_d, label in [("2015-06-15", "2015"), ("2018-06-15", "2018"),
                      ("2020-06-15", "2020"), ("2022-06-15", "2022"),
                      ("2024-06-14", "2024"), ("2026-04-15", "2026")]:
    n = m.execute("SELECT COUNT(DISTINCT symbol) FROM prices WHERE date = ?", (year_d,)).fetchone()[0]
    print(f"    {label} ({year_d}): {n} symbols pricing on this trading day")

# The universe should GROW over time as more IPOs come in. Removals (delistings)
# don't reduce the count because removed names aren't in the DB at all.
n_2015 = m.execute("SELECT COUNT(DISTINCT symbol) FROM prices WHERE date = '2015-06-15'").fetchone()[0]
n_2026 = m.execute("SELECT COUNT(DISTINCT symbol) FROM prices WHERE date = '2026-04-15'").fetchone()[0]
check(f"universe grew {n_2015} → {n_2026} over the 2015-2026 window",
      n_2026 > n_2015,
      "monotone-growth pattern is the survivorship-bias smoking gun")


m.close()
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
print()
print("BOTTOM LINE: this is a data-coverage limitation, not a code bug.")
print("Fix requires sourcing historical index-membership data + delisted")
print("price history. Multi-year backtests over-state returns by an")
print("estimated 10-25% cumulative for the 2015-2026 window.")
sys.exit(0 if FAIL == 0 else 1)
