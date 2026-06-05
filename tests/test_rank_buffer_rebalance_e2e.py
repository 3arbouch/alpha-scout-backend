#!/usr/bin/env python3
"""
V2 rank_buffer (hysteresis) rebalance mode — behavioural test.

rank_buffer is the low-turnover ranking rebalance: re-rank the universe, but
SELL a held name only when its rank falls past `exit_rank` and BUY a non-held
name only when its rank is within `entry_rank`, leaving names in the buffer
zone (entry_rank < rank ≤ exit_rank) untouched. There is no v1 equivalent, so
this is not a parity test — it pins the two invariants that define the mode:

  1. DEGENERATE: with entry_rank == exit_rank == top_n and band == 0,
     rank_buffer reproduces equal_weight's rotation MEMBERSHIP (same number of
     rebalance_rotation sells) and near-identical total turnover. It is not
     byte-identical by design: rank_buffer divides capital over the actionable
     book (survivors + priced entrants) rather than the nominal top_n, so it
     leaves no idle cash on an unpriceable target slot. The buffer is the only
     behavioural lever beyond that.

  2. HYSTERESIS: widening exit_rank above entry_rank strictly REDUCES
     rebalance_rotation churn (names oscillating around top_n are no longer
     sold-then-rebought), while keeping the book roughly the same size.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend-dev/data/market_dev.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_rank_buffer_rebalance_e2e.py
"""
import copy
import os
import sys
import contextlib
import io
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine_v2 import run_portfolio_backtest as run_v2

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


def sig(t):
    return (t["date"], t["symbol"], t["action"], t.get("reason"),
            round(float(t["price"]), 2), round(float(t["shares"]), 1))


COMPOSITE = {
    "standardization": "rank",
    "buckets": {
        "growth": {"weight": 1, "factors": [
            {"name": "eps_yoy", "sign": "+"}, {"name": "rev_yoy", "sign": "+"}]},
        "quality": {"weight": 1, "factors": [
            {"name": "roe", "sign": "+"}, {"name": "fcf_yield", "sign": "+"}]},
        "momentum": {"weight": 0.8, "factors": [{"name": "ret_12_1m", "sign": "+"}]},
    },
}
TOP_N = 20


def make_cfg(mode, rules):
    return {
        "engine_version": "v2",
        "name": f"rankbuf-test-{mode}",
        "sleeves": [{
            "label": "QGM", "weight": 1,
            "strategy_config": {
                "name": "QGM", "universe": {"type": "index", "index": "sp500"},
                "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                "ranking": {"by": "composite_score", "order": "desc", "top_n": TOP_N},
                "composite_score": COMPOSITE,
                "sizing": {"type": "equal_weight", "max_positions": TOP_N,
                           "initial_allocation": 10_000_000, "shares": "whole"},
                "rebalancing": {"mode": mode, "frequency": "monthly", "rules": rules},
                "backtest": {"start": "2022-06-01", "end": "2024-06-01",
                             "initial_capital": 10_000_000, "slippage_bps": 10},
            },
        }],
        "backtest": {"start": "2022-06-01", "end": "2024-06-01",
                     "initial_capital": 10_000_000},
    }


def run(cfg):
    with contextlib.redirect_stdout(io.StringIO()):
        r = run_v2(copy.deepcopy(cfg), force_close_at_end=False)
    return r.get("trades", [])


# --- Baseline: equal_weight rotation -----------------------------------------
print("\n=== running equal_weight baseline ===")
ew = run(make_cfg("equal_weight", {}))
ew_c = Counter(t.get("reason") for t in ew)
print(f"  equal_weight: {len(ew)} trades, reasons={dict(ew_c)}")

# --- Invariant 1: degenerate rank_buffer == equal_weight ---------------------
print("\n=== 1. degenerate rank_buffer (entry=exit=top_n, band=0) ≈ equal_weight ===")
deg = run(make_cfg("rank_buffer", {"entry_rank": TOP_N, "exit_rank": TOP_N,
                                    "rebalance_band_pct": 0}))
deg_c = Counter(t.get("reason") for t in deg)
print(f"  rank_buffer(degenerate): {len(deg)} trades, reasons={dict(deg_c)}")
ew_rot, deg_rot = ew_c.get("rebalance_rotation", 0), deg_c.get("rebalance_rotation", 0)
check("emits rotation trades at all (sanity)", ew_rot > 0, f"baseline rotation={ew_rot}")
check(f"rotation membership matches equal_weight (ew={ew_rot} deg={deg_rot})",
      abs(ew_rot - deg_rot) <= max(2, round(ew_rot * 0.02)),
      f"rotation differs by {abs(ew_rot - deg_rot)}")
check(f"total turnover within 1% of equal_weight (ew={len(ew)} deg={len(deg)})",
      abs(len(ew) - len(deg)) <= max(3, round(len(ew) * 0.01)),
      f"diff={abs(len(ew) - len(deg))}")

# --- Invariant 2: buffer reduces rotation churn ------------------------------
print("\n=== 2. widening exit_rank reduces rebalance_rotation churn ===")
buf = run(make_cfg("rank_buffer", {"entry_rank": TOP_N, "exit_rank": int(TOP_N * 2),
                                   "rebalance_band_pct": 0}))
buf_c = Counter(t.get("reason") for t in buf)
print(f"  rank_buffer(exit={TOP_N*2}): {len(buf)} trades, reasons={dict(buf_c)}")
ew_rot = ew_c.get("rebalance_rotation", 0)
buf_rot = buf_c.get("rebalance_rotation", 0)
check(f"rotation churn strictly lower with buffer (ew={ew_rot} → buf={buf_rot})",
      buf_rot < ew_rot, f"expected fewer rotations, got {buf_rot} vs {ew_rot}")
check("buffer still trades (not frozen)", len(buf) > 0)

# book size stays comparable (we still cap at top_n) — check final open count
def open_syms(trades):
    held = {}
    for t in sorted(trades, key=lambda x: x["date"]):
        s = t["symbol"]
        held[s] = held.get(s, 0) + (t["shares"] if t["action"] == "BUY" else -t["shares"])
    return {s for s, q in held.items() if q > 0.5}

check(f"book size stays near top_n ({len(open_syms(buf))} held, cap={TOP_N})",
      len(open_syms(buf)) <= TOP_N + 1)

print("\n" + "=" * 60)
print(f"PASSED: {PASS}\nFAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
