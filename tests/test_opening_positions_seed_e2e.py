#!/usr/bin/env python3
"""
V2 opening_positions (carry-in seed) — behavioural test.

`opening_positions` seeds a deployment's book with REAL pre-existing holdings
(actual broker fills: symbol, shares, entry_price, entry_date) so the
deployment's cost basis and P&L track a live-capital account. The strategy then
runs forward from that opening book EXACTLY AS DESIGNED — seeding changes only
the starting state, not the entry/exit logic.

Invariants pinned here (dev market DB, a few liquid S&P names, recent window):

  1. SEED IDENTITY: seeds show as entries on the seed date at their exact fill
     prices, and cash == initial_capital − Σ(shares × fill_price).

  2. MARK-TO-MARKET: last-day NAV == cash + Σ(shares × last_close), i.e. the
     seeded cost basis is carried and marked forward correctly.

  3. FULL BOOK → NO TOP-UP: when the seed fills max_positions, day 1 has no
     open slot, so the engine adds nothing on day 1 (held == seed count).

  4. STRATEGY RUNS AS DESIGNED: when the seed is SMALLER than max_positions,
     the engine backfills toward max_positions as usual (the seed must NOT
     suppress the normal entry logic).

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend-dev/data/market_dev.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_opening_positions_seed_e2e.py
"""
import copy
import os
import sys
import contextlib
import io

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import sqlite3
from portfolio_engine_v2 import run_portfolio_backtest as run_v2

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


START = "2026-05-18"
END = "2026-06-05"
SEEDS = [
    {"symbol": "NVDA", "shares": 23, "entry_price": 230.00, "entry_date": START},
    {"symbol": "MO",   "shares": 82, "entry_price": 73.02,  "entry_date": START},
    {"symbol": "NEM",  "shares": 33, "entry_price": 110.14, "entry_date": START},
]
COMPOSITE = {
    "standardization": "rank",
    "buckets": {
        "growth": {"weight": 1, "factors": [{"name": "rev_yoy", "sign": "+"}]},
        "quality": {"weight": 1, "factors": [{"name": "roe", "sign": "+"}]},
        "momentum": {"weight": 1, "factors": [{"name": "ret_12_1m", "sign": "+"}]},
    },
}


def base_cfg(max_positions):
    return {
        "engine_version": "v2",
        "name": "seed-test",
        "sleeves": [{
            "label": "S", "weight": 1,
            "strategy_config": {
                "name": "S", "universe": {"type": "index", "index": "sp500"},
                "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                "ranking": {"by": "composite_score", "order": "desc", "top_n": max_positions},
                "composite_score": COMPOSITE,
                "sizing": {"type": "equal_weight", "max_positions": max_positions,
                           "initial_allocation": 100000, "shares": "whole"},
                "rebalancing": {"frequency": "quarterly", "mode": "trim",
                                "rules": {"max_position_pct": 100}},
                "backtest": {"start": START, "end": END,
                             "initial_capital": 100000, "slippage_bps": 10},
            },
        }],
        "backtest": {"start": START, "end": END, "initial_capital": 100000},
    }


def run(cfg):
    with contextlib.redirect_stdout(io.StringIO()):
        r = run_v2(copy.deepcopy(cfg), force_close_at_end=False)
    return r


def held_on(r, date):
    """Symbols held at end of `date` from the nav_history snapshot."""
    for p in r.get("combined_nav_history", []):
        if p["date"] == date:
            return set((p.get("positions") or {}).keys())
    return set()


# --- Full-book seed: max_positions == len(seeds) → no day-1 top-up -----------
print("\n=== full-book seed (max_positions == 3): exact cost basis, no top-up ===")
cfg = base_cfg(max_positions=3)
cfg["opening_positions"] = SEEDS
r = run(cfg)
trades = r["trades"]; nav = r.get("combined_nav_history", [])
buys = [t for t in trades if t["action"] == "BUY"]
seed_syms = {s["symbol"] for s in SEEDS}
cost = sum(s["shares"] * s["entry_price"] for s in SEEDS)

check("seeds recorded as entries on the seed date at exact prices",
      {(t["symbol"], t["price"]) for t in buys if t["date"] == START}
      == {(s["symbol"], s["entry_price"]) for s in SEEDS},
      f"buys={[(t['symbol'], t['price']) for t in buys if t['date']==START]}")
check(f"cash == initial − cost ({100000 - cost:,.2f})",
      abs(nav[0]["cash"] - (100000 - cost)) < 1.0, f"cash={nav[0]['cash']:.2f}")
check("day-1 book == seed (full book ⇒ no top-up)",
      held_on(r, START) == seed_syms, f"held={sorted(held_on(r, START))}")

conn = sqlite3.connect(os.environ.get("MARKET_DB_PATH",
       "/home/mohamed/alpha-scout-backend-dev/data/market_dev.db"))
mtm = nav[-1]["cash"]
for s in SEEDS:
    px = conn.execute("SELECT close FROM prices WHERE symbol=? AND date<=? ORDER BY date DESC LIMIT 1",
                      (s["symbol"], nav[-1]["date"])).fetchone()[0]
    mtm += s["shares"] * px
check("last-day NAV == cash + Σ shares×last_close (cost basis carried)",
      abs(nav[-1]["nav"] - mtm) < 1.0, f"nav={nav[-1]['nav']:.2f} vs mtm={mtm:.2f}")

# --- Partial seed: max_positions=20, seed 3 → strategy backfills as designed -
print("\n=== partial seed (max_positions=20, seed 3): strategy runs as designed ===")
cfg2 = base_cfg(max_positions=20)
cfg2["opening_positions"] = SEEDS
r2 = run(cfg2)
held1 = held_on(r2, START)
last_date = r2["combined_nav_history"][-1]["date"]
held_last = held_on(r2, last_date)
check("day-1 holds the 3 seeds", seed_syms <= held1, f"held={sorted(held1)}")
# Entries are queued on the seed day and filled next_close (day 2+), so the
# backfill shows up after START — confirm the book grew toward max_positions.
check("engine backfills toward max_positions (seed did NOT suppress entries)",
      len(held_last) > len(SEEDS), f"final held only {len(held_last)} (expected >3)")

print("\n" + "=" * 60)
print(f"PASSED: {PASS}\nFAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
