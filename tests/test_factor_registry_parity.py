#!/usr/bin/env python3
"""
Parity check: server/factors registry must produce numbers identical to the
existing scripts/features.py implementation, when both compute from the same
current DB state.

We do NOT compare registry output to the stored features_daily value, because
stored rows can be stale (income/balance/cashflow get backfilled but old daily
rows aren't always recomputed). What we want to verify is that the new
implementation matches the old implementation pointwise — same inputs, same
math.

Run:
    DATA_DIR=/home/mohamed/alpha-scout-backend/data \
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \
    python3 tests/test_factor_registry_parity.py [--sample N | --all]

Default: 5,000 random rows. --all walks every row in features_daily (~1.4M).
Fails if any registered feature disagrees with the legacy compute output
beyond TOL.
"""
from __future__ import annotations

import argparse
import os
import random
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from server.factors import all_features, get  # noqa: E402
from server.factors.context import build_context  # noqa: E402

# scripts/features.py — the existing implementation, treated as the reference.
import features as legacy_features  # noqa: E402

TOL = 1e-9


def _eq_or_close(a, b) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) <= TOL


def _load_symbol_bundles(conn, symbol):
    return legacy_features._load_symbol_bundles(conn, symbol)


def _evaluate_via_registry(symbol, date, close, income, balance, cashflow):
    """Returns dict of {feature_name: value} for the materialized features."""
    ctx = build_context(symbol, date, close, income, balance, cashflow)
    if ctx is None:
        return None
    out = {}
    for fd in all_features():
        if fd.materialization != "precomputed":
            continue
        out[fd.name] = fd.compute(ctx)
    return out


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--sample", type=int, default=5000,
                   help="random sample size of rows to check (default 5000)")
    g.add_argument("--all", action="store_true",
                   help="check every row in features_daily")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    db_path = os.environ.get("MARKET_DB_PATH",
                             "/home/mohamed/alpha-scout-backend/data/market.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = None

    # Only check features that the legacy implementation knows about — that's
    # the original 9 (the parity gate was for the refactor; new features added
    # to the registry have no legacy counterpart to compare against).
    LEGACY_FEATURES = ("pe", "ps", "p_b", "ev_ebitda", "ev_sales",
                       "fcf_yield", "div_yield", "eps_yoy", "rev_yoy")
    registered = {fd.name for fd in all_features() if fd.materialization == "precomputed"}
    feature_names = [n for n in LEGACY_FEATURES if n in registered]
    cols = ",".join(feature_names)
    print(f"Parity-checked features (legacy ∩ registry): {feature_names}")

    cur = conn.cursor()
    if args.all:
        rows = cur.execute(
            f"SELECT symbol, date, {cols} FROM features_daily"
        ).fetchall()
        print(f"Walking ALL {len(rows):,} rows…")
    else:
        # Stratify by symbol for fair coverage.
        symbols = [r[0] for r in cur.execute(
            "SELECT DISTINCT symbol FROM features_daily ORDER BY symbol"
        ).fetchall()]
        random.seed(args.seed)
        per_symbol = max(1, args.sample // max(1, len(symbols)))
        rows = []
        for s in symbols:
            srows = cur.execute(
                f"SELECT symbol, date, {cols} FROM features_daily "
                "WHERE symbol = ?", (s,)
            ).fetchall()
            if len(srows) > per_symbol:
                srows = random.sample(srows, per_symbol)
            rows.extend(srows)
        print(f"Sampling {len(rows):,} rows across {len(symbols)} symbols (seed={args.seed})…")

    # Group rows by symbol so we load each bundle once.
    by_symbol: dict[str, list[tuple]] = {}
    for r in rows:
        by_symbol.setdefault(r[0], []).append(r)

    fail = 0
    skipped = 0
    checked = 0

    for i, (symbol, srows) in enumerate(by_symbol.items(), 1):
        income, balance, cashflow, prices = _load_symbol_bundles(conn, symbol)
        price_by_date = {d: c for d, c in prices}
        for row in srows:
            date = row[1]
            close = price_by_date.get(date)
            if close is None:
                skipped += 1
                continue
            # Legacy compute from current DB state.
            legacy_out = legacy_features.compute_features_for_day(
                date, close, income, balance, cashflow,
            )
            # Registry compute from current DB state.
            reg_out = _evaluate_via_registry(
                symbol, date, close, income, balance, cashflow,
            )
            if legacy_out is None and reg_out is None:
                skipped += 1
                continue
            if (legacy_out is None) != (reg_out is None):
                fail += 1
                if fail <= 20:
                    print(f"  ❌ {symbol} {date}: presence diverges "
                          f"legacy={legacy_out is not None} registry={reg_out is not None}")
                continue
            for name in feature_names:
                if not _eq_or_close(reg_out.get(name), legacy_out.get(name)):
                    fail += 1
                    if fail <= 20:
                        print(f"  ❌ {symbol} {date} {name}: "
                              f"legacy={legacy_out.get(name)}  registry={reg_out.get(name)}")
            checked += 1
        if i % 100 == 0 or i == len(by_symbol):
            print(f"  [{i}/{len(by_symbol)}] {symbol}: checked={checked:,} fail={fail} skipped={skipped}")

    print()
    print(f"=== Parity result ===")
    print(f"  rows checked: {checked:,}")
    print(f"  rows skipped: {skipped:,} (no matching close in prices)")
    print(f"  failures:     {fail}")
    sys.exit(0 if fail == 0 else 1)


if __name__ == "__main__":
    main()
