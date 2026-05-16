"""
Bulk-update the engine_version field on existing deployments.

Reads app_dev.db (or whichever app DB is pointed at via APP_DB_PATH), takes
every deployment matching a status filter, parses its config_json, sets
engine_version to the target value, and writes it back.

Intended for dev environments. NEVER run against prod's app.db unless you
explicitly want every deployment to switch engines on next evaluation.

Usage:
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 scripts/flip_engine_version.py
        [--to v2]                # target engine version (default v2)
        [--status active]         # which deployments to touch (default 'active')
        [--name-pattern 'OMAR%']  # SQL LIKE filter on name (default all)
        [--dry-run]               # show counts without writing
        [--force]                 # apply without --dry-run sanity preview
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import APP_DB_PATH


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--to", default="v2", choices=("v1", "v2"))
    p.add_argument("--status", default="active",
                    help="Status filter (default 'active'); 'all' for everything.")
    p.add_argument("--name-pattern", default=None,
                    help="SQL LIKE filter on deployment name.")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    conn = sqlite3.connect(APP_DB_PATH)
    print(f"DB: {APP_DB_PATH}")

    where = []
    params: list = []
    if args.status != "all":
        where.append("status = ?")
        params.append(args.status)
    if args.name_pattern:
        where.append("name LIKE ?")
        params.append(args.name_pattern)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    rows = conn.execute(
        f"SELECT id, name, config_json FROM deployments{where_sql}",
        params,
    ).fetchall()
    print(f"Matched {len(rows)} deployments (status={args.status}, name_pattern={args.name_pattern})")

    # Pre-flight summary
    n_already = n_change = n_bad = 0
    for _id, _name, cj in rows:
        try:
            cfg = json.loads(cj)
            if cfg.get("engine_version") == args.to:
                n_already += 1
            else:
                n_change += 1
        except json.JSONDecodeError:
            n_bad += 1
    print(f"  already on {args.to}: {n_already}")
    print(f"  to flip:           {n_change}")
    print(f"  unparseable JSON:  {n_bad}")

    if args.dry_run:
        print("\nDRY RUN — no writes.")
        return

    if n_change == 0:
        print("\nNothing to change.")
        return

    if not args.force:
        ans = input(f"\nApply engine_version='{args.to}' to {n_change} deployments? [y/N] ").strip().lower()
        if ans != "y":
            print("Aborted.")
            return

    now = datetime.now(timezone.utc).isoformat()
    written = 0
    for _id, _name, cj in rows:
        try:
            cfg = json.loads(cj)
        except json.JSONDecodeError:
            continue
        if cfg.get("engine_version") == args.to:
            continue
        cfg["engine_version"] = args.to
        conn.execute(
            "UPDATE deployments SET config_json = ?, updated_at = ? WHERE id = ?",
            (json.dumps(cfg), now, _id),
        )
        written += 1
    conn.commit()
    print(f"\nUpdated {written} deployments to engine_version='{args.to}'.")


if __name__ == "__main__":
    main()
