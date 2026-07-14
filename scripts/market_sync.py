#!/usr/bin/env python3
"""
Market-data replica: canonical prod market.db  ->  this env's market DB.

One-way, Write-Audit-Publish:
  1. Snapshot  — consistent copy of the live source (safe under concurrent writes).
  2. Audit     — integrity_check + never-regress (won't publish older data over newer).
  3. Preserve  — carry over any env-local tables that exist here but not in source
                 (e.g. factor_returns_daily research overlay), so a sync never
                 destroys local research artifacts.
  4. Publish   — atomic rename. Readers see old-or-new, never a half-written file.

Fail-closed: on any problem the staging file is removed and the current replica
is left untouched, and the process exits non-zero so cron surfaces it.

Usage:
    MARKET_DB_PATH=/path/market_dev.db python3 market_sync.py            # sync
    MARKET_DB_PATH=/path/market_dev.db python3 market_sync.py --dry-run  # build+audit, no swap
"""
from __future__ import annotations

import os
import sqlite3
import sys

SOURCE = os.environ.get("MARKET_SOURCE_DB", "/home/mohamed/alpha-scout-backend/data/market.db")


def _prices_max_date(db: str) -> str | None:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return conn.execute("SELECT MAX(date) FROM prices").fetchone()[0]
    finally:
        conn.close()


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    target = os.environ.get("MARKET_DB_PATH")
    if not target:
        return _fail("MARKET_DB_PATH not set")
    if not os.path.exists(SOURCE):
        return _fail(f"source market.db missing at {SOURCE}")
    staging = target + ".staging"

    # 1. Consistent, compacted snapshot of the live source.
    if os.path.exists(staging):
        os.remove(staging)
    src = sqlite3.connect(f"file:{SOURCE}?mode=ro", uri=True)
    try:
        src.execute("VACUUM INTO ?", [staging])
    finally:
        src.close()

    # 2a. Audit: structural integrity of the fresh snapshot.
    chk = sqlite3.connect(staging)
    try:
        verdict = chk.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        chk.close()
    if verdict != "ok":
        return _fail(f"integrity_check failed: {verdict}", staging)

    # 2b. Audit: never regress to older data than the current replica.
    new_max = _prices_max_date(staging)
    cur_max = _prices_max_date(target) if os.path.exists(target) else None
    if cur_max and new_max and new_max < cur_max:
        return _fail(f"would regress prices max(date) {cur_max} -> {new_max}", staging)

    # 3. Preserve env-local tables (present here, absent in source) — e.g. research overlay.
    preserved = []
    if os.path.exists(target):
        stg_ro = sqlite3.connect(f"file:{staging}?mode=ro", uri=True)
        src_tables = _tables(stg_ro)
        stg_ro.close()
        old = sqlite3.connect(f"file:{target}?mode=ro", uri=True)
        try:
            local_only = _tables(old) - src_tables
            # DDL for local-only tables + their indexes, tables first so indexes attach cleanly.
            objs = old.execute(
                "SELECT type, tbl_name, sql FROM sqlite_master "
                "WHERE type IN ('table','index') AND sql IS NOT NULL "
                "AND tbl_name IN ({}) ORDER BY (type='index')".format(
                    ",".join("?" * len(local_only))), sorted(local_only)).fetchall() if local_only else []
        finally:
            old.close()
        if local_only:
            st = sqlite3.connect(staging)
            try:
                st.execute("ATTACH ? AS old", [target])
                for typ, tbl, sql in objs:
                    st.execute(sql)
                    if typ == "table":
                        st.execute(f'INSERT INTO main."{tbl}" SELECT * FROM old."{tbl}"')
                for tbl in sorted(local_only):
                    n = st.execute(f'SELECT COUNT(*) FROM main."{tbl}"').fetchone()[0]
                    preserved.append((tbl, n))
                st.commit()
            finally:
                st.close()

    # 4. Publish (or stop here on --dry-run).
    size_mb = os.path.getsize(staging) // 1_000_000
    if dry_run:
        print(f"[dry-run] staging built: max_date={new_max}, {size_mb} MB, "
              f"preserved={preserved or 'none'}  (left at {staging}, no swap)")
        return 0
    os.replace(staging, target)
    print(f"OK: replica published -> {target} | max_date={new_max} | {size_mb} MB | "
          f"preserved={preserved or 'none'}")
    return 0


def _fail(msg: str, staging: str | None = None) -> int:
    if staging and os.path.exists(staging):
        os.remove(staging)
    print(f"MARKET SYNC: FAIL — {msg}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
