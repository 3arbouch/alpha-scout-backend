"""
Reconciliation check: assert two market DBs are in parity.

Used after dev_market_sync.sh copies prod's market.db into dev's market_dev.db,
to prove the copy landed and the two databases carry the same universe. Exits
non-zero (and prints a loud FAIL) on any drift, so a broken sync surfaces in the
cron log the same day instead of rotting silently for months.

Usage:
    python3 check_market_parity.py <reference_db> <candidate_db>
    # e.g. check_market_parity.py /path/market.db /path/market_dev.db
"""
from __future__ import annotations

import sqlite3
import sys


def _stats(db: str) -> dict:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        syms = {r[0] for r in conn.execute("SELECT DISTINCT symbol FROM prices")}
        n_rows = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        max_date = conn.execute("SELECT MAX(date) FROM prices").fetchone()[0]
    finally:
        conn.close()
    return {"symbols": syms, "n_rows": n_rows, "max_date": max_date}


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: check_market_parity.py <reference_db> <candidate_db>")
        return 2
    ref_path, cand_path = sys.argv[1], sys.argv[2]
    ref, cand = _stats(ref_path), _stats(cand_path)

    problems = []
    only_ref = ref["symbols"] - cand["symbols"]
    only_cand = cand["symbols"] - ref["symbols"]
    if only_ref:
        problems.append(f"{len(only_ref)} symbols in reference missing from candidate "
                        f"(e.g. {sorted(only_ref)[:10]})")
    if only_cand:
        problems.append(f"{len(only_cand)} symbols in candidate not in reference "
                        f"(e.g. {sorted(only_cand)[:10]})")
    if ref["max_date"] != cand["max_date"]:
        problems.append(f"max(date) differs: reference={ref['max_date']} candidate={cand['max_date']}")

    print(f"reference: {len(ref['symbols'])} symbols, {ref['n_rows']:,} rows, max_date={ref['max_date']}")
    print(f"candidate: {len(cand['symbols'])} symbols, {cand['n_rows']:,} rows, max_date={cand['max_date']}")

    if problems:
        print("MARKET PARITY CHECK: FAIL")
        for p in problems:
            print(f"  - {p}")
        return 1
    print("MARKET PARITY CHECK: PASS (symbol sets and max_date match)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
