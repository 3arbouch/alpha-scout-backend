#!/usr/bin/env python3
"""
Unit tests for the PIT membership lookup (scripts/universe_history.py).

The high-bit test is the trader's check: pull the S&P 500 as of a specific
date and confirm a known-removed name is in it. We use SIVB (Silicon Valley
Bank), which was removed from the S&P 500 on 2023-03-14 after the FDIC
takeover. A survivor-biased universe excludes SIVB silently and overstates
a 2023 backtest's returns; PIT data restores it.

Other tests exercise the change-log replay (multiple add/remove cycles,
edge dates, missing-anchor failure mode) and the ever_members union helper.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    python3 test_universe_history_unit.py
"""
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from universe_history import (
    ensure_schema, members_as_of, ever_members,
)

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


# ---------------------------------------------------------------------------
# 1. SIVB on the prod DB: known-removed name surfaces correctly
# ---------------------------------------------------------------------------
print("\n=== 1. SIVB / S&P 500 PIT (the trader's canonical check) ===")
prod_db = Path(os.environ.get("MARKET_DB_PATH", "/home/mohamed/alpha-scout-backend/data/market.db"))
if prod_db.exists():
    c = sqlite3.connect(prod_db)
    # On 2023-03-10 — Friday FDIC takeover, SIVB still listed in S&P 500
    m_before = members_as_of(c, "sp500", "2023-03-10")
    check("SIVB ∈ S&P 500 on 2023-03-10 (before removal)", "SIVB" in m_before)
    # On 2023-03-13 — Monday after takeover, still member until 2023-03-14 swap
    m_pre_swap = members_as_of(c, "sp500", "2023-03-13")
    check("SIVB ∈ S&P 500 on 2023-03-13 (last day before PODD swap)",
          "SIVB" in m_pre_swap)
    # On 2023-03-14 — swap-effective date in FMP data
    m_post = members_as_of(c, "sp500", "2023-03-14")
    check("SIVB ∉ S&P 500 on 2023-03-14 (after removal)", "SIVB" not in m_post)
    # PODD (Insulet) is the replacement
    check("PODD ∈ S&P 500 on 2023-03-14 (replaced SIVB)", "PODD" in m_post)
    # Sanity: total count is ~500, not zero / not exploded
    check(f"S&P 500 count on 2023-03-10 looks right (got {len(m_before)})",
          495 <= len(m_before) <= 510)
else:
    print(f"  ⚠️  skipped (prod DB not at {prod_db})")


# ---------------------------------------------------------------------------
# 2. members_as_of replay algorithm — synthetic case with multiple cycles
# ---------------------------------------------------------------------------
print("\n=== 2. members_as_of replay (multiple add/remove cycles) ===")
with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
    tmp_db = tmp.name
try:
    cc = sqlite3.connect(tmp_db)
    ensure_schema(cc)
    # Current snapshot: {AAA, BBB, CCC}
    cc.executemany(
        "INSERT INTO index_membership_current (index_name, symbol) VALUES (?, ?)",
        [("test", "AAA"), ("test", "BBB"), ("test", "CCC")],
    )
    # History (in time order):
    #   2020-01-01: AAA added (replacing OLD1)
    #   2021-01-01: OLD1 removed → wait that's the same event. OK use separate rows.
    #   2022-06-15: DDD removed, BBB added (DDD was member before)
    #   2023-09-30: EEE removed, CCC added (EEE was member before)
    #   2024-03-01: AAA removed, then added again 2024-08-01 (re-addition test)
    #   2024-08-01: AAA added
    events = [
        # event_date, symbol, action, replaced
        ("2020-01-01", "AAA", "added",   "OLD1"),
        ("2020-01-01", "OLD1", "removed", "AAA"),
        ("2022-06-15", "BBB", "added",   "DDD"),
        ("2022-06-15", "DDD", "removed", "BBB"),
        ("2023-09-30", "CCC", "added",   "EEE"),
        ("2023-09-30", "EEE", "removed", "CCC"),
        ("2024-03-01", "AAA", "removed", "XXX"),
        ("2024-03-01", "XXX", "added",   "AAA"),
        ("2024-08-01", "AAA", "added",   "XXX"),
        ("2024-08-01", "XXX", "removed", "AAA"),
    ]
    cc.executemany(
        "INSERT INTO index_membership_history "
        "(index_name, symbol, event_date, action, replaced, reason) VALUES (?, ?, ?, ?, ?, ?)",
        [("test", s, d, a, r, None) for d, s, a, r in events],
    )
    cc.commit()

    # As of 2019-12-31 (before any event): replay strips all "added" after
    # 2019-12-31 → set becomes empty + adds back any "removed". Result:
    # current was {AAA,BBB,CCC}; undo AAA-add → remove AAA;
    #                            undo BBB-add → remove BBB;
    #                            undo CCC-add → remove CCC;
    # then add removed-after-date: OLD1, DDD, EEE.
    # AAA removed 2024-03-01 → add back; XXX added/removed pair stays gone.
    m = members_as_of(cc, "test", "2019-12-31")
    check("synthetic: pre-any-event includes OLD1, DDD, EEE; excludes ABC adds",
          m == {"OLD1", "DDD", "EEE"},
          f"got {sorted(m)}")

    # As of 2021-01-01 (post-AAA-add only): {AAA, DDD, EEE}
    m = members_as_of(cc, "test", "2021-01-01")
    check("synthetic: AAA in, DDD & EEE still in, BBB/CCC not yet in",
          m == {"AAA", "DDD", "EEE"},
          f"got {sorted(m)}")

    # As of 2023-01-01 (post BBB swap, pre CCC swap): {AAA, BBB, EEE}
    m = members_as_of(cc, "test", "2023-01-01")
    check("synthetic: AAA, BBB, EEE", m == {"AAA", "BBB", "EEE"},
          f"got {sorted(m)}")

    # As of 2024-06-01 (between AAA-removed and AAA-re-added): {XXX, BBB, CCC}
    m = members_as_of(cc, "test", "2024-06-01")
    check("synthetic: re-addition handled (AAA out, XXX in)",
          m == {"XXX", "BBB", "CCC"},
          f"got {sorted(m)}")

    # As of today (after all events): matches current
    m = members_as_of(cc, "test", "2099-12-31")
    check("synthetic: future date returns current snapshot",
          m == {"AAA", "BBB", "CCC"}, f"got {sorted(m)}")

finally:
    os.unlink(tmp_db)


# ---------------------------------------------------------------------------
# 3. ever_members: union across [start, end]
# ---------------------------------------------------------------------------
print("\n=== 3. ever_members union across window ===")
if prod_db.exists():
    c = sqlite3.connect(prod_db)
    em = ever_members(c, "sp500", "2022-01-01", "2023-12-31")
    check(f"S&P 500 ever-members 2022-2023 has 500+ names (got {len(em)})",
          len(em) >= 500)
    check("SIVB ∈ ever_members(2022-01-01..2023-12-31) [was member, then removed]",
          "SIVB" in em)
    check("PODD ∈ ever_members(2022-01-01..2023-12-31) [added 2023-03-14]",
          "PODD" in em)


# ---------------------------------------------------------------------------
# 4. Missing-anchor failure mode
# ---------------------------------------------------------------------------
print("\n=== 4. Missing-anchor failure mode ===")
with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
    tmp_db = tmp.name
try:
    cc = sqlite3.connect(tmp_db)
    ensure_schema(cc)
    # No current snapshot inserted — should raise
    try:
        members_as_of(cc, "nonexistent_idx", "2023-01-01")
        check("raises when no current snapshot", False, "no exception")
    except ValueError as e:
        check("raises ValueError when no current snapshot",
              "current snapshot" in str(e), str(e)[:60])
finally:
    os.unlink(tmp_db)


print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
