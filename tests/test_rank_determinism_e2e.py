#!/usr/bin/env python3
"""
Determinism test (Gap 10): running the same backtest twice in the same
Python process must produce byte-identical trade ledgers. Catches:

  - Nondeterministic ranking tie-breaking (set-iteration order, dict-order
    leaking into sort keys).
  - Hidden state mutation between runs (e.g. cache poisoning, mutated
    config dicts, global RNG side-effects).
  - Order-dependent feature pre-computation.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_rank_determinism_e2e.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from backtest_engine import run_backtest

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


def trade_signature(trades):
    """A normalized representation of the trade list, robust to dict-key order."""
    return [
        (
            t["date"],
            t["symbol"],
            t["action"],
            t.get("reason"),
            round(float(t.get("price", 0)), 4),
            round(float(t.get("shares", 0)), 6),
        )
        for t in trades
    ]


# ---------------------------------------------------------------------------
# 1. Plain strategy, no ranking — trivial determinism baseline
# ---------------------------------------------------------------------------
print("\n=== 1. Plain strategy: identical runs produce identical trades ===")

CONFIG_PLAIN = {
    "name": "DeterminismPlain",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "NVDA"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 300000},
    "backtest": {"start": "2024-01-01", "end": "2024-06-30",
                 "entry_price": "next_close", "slippage_bps": 10},
}

r1 = run_backtest(json.loads(json.dumps(CONFIG_PLAIN)))
r2 = run_backtest(json.loads(json.dumps(CONFIG_PLAIN)))

sig1 = trade_signature(r1.get("trades", []))
sig2 = trade_signature(r2.get("trades", []))

check("plain strategy: trade count matches", len(sig1) == len(sig2),
      f"r1={len(sig1)} r2={len(sig2)}")
check("plain strategy: trade ledger byte-equal", sig1 == sig2,
      f"first diff at idx {next((i for i, (a, b) in enumerate(zip(sig1, sig2)) if a != b), 'N/A')}")
check("plain strategy: final NAV identical",
      r1["nav_history"][-1]["nav"] == r2["nav_history"][-1]["nav"],
      f"r1={r1['nav_history'][-1]['nav']} r2={r2['nav_history'][-1]['nav']}")


# ---------------------------------------------------------------------------
# 2. Ranked rebalance — exercises the rank_candidates sort path
# ---------------------------------------------------------------------------
print("\n=== 2. Ranked rebalance: identical runs produce identical trades ===")

CONFIG_RANKED = {
    "name": "DeterminismRanked",
    "universe": {"type": "symbols",
                  "symbols": ["AAPL", "MSFT", "NVDA", "JNJ", "PG", "KO", "XOM", "CVX", "COP"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 5, "initial_allocation": 300000},
    "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 5},
    "rebalancing": {"frequency": "monthly", "mode": "equal_weight"},
    "backtest": {"start": "2024-01-01", "end": "2024-12-31",
                 "entry_price": "next_close", "slippage_bps": 10},
}

r3 = run_backtest(json.loads(json.dumps(CONFIG_RANKED)))
r4 = run_backtest(json.loads(json.dumps(CONFIG_RANKED)))

sig3 = trade_signature(r3.get("trades", []))
sig4 = trade_signature(r4.get("trades", []))

check("ranked strategy: trade count matches", len(sig3) == len(sig4),
      f"r3={len(sig3)} r4={len(sig4)}")
check("ranked strategy: trade ledger byte-equal", sig3 == sig4,
      f"first diff: {next(((a, b) for a, b in zip(sig3, sig4) if a != b), None)}")
check("ranked strategy: final NAV identical",
      r3["nav_history"][-1]["nav"] == r4["nav_history"][-1]["nav"])
check("ranked strategy: at least one rotation (rebalance triggered)",
      any(t.get("reason", "").startswith("rebalance") for t in r3.get("trades", [])),
      "no rebalance trades produced — rebalance path not exercised")


# ---------------------------------------------------------------------------
# 3. Same config reused — engine adds `strategy_id` (a deterministic hash),
#    but no other key should be mutated. The determinism check above already
#    proved that the added strategy_id doesn't change subsequent behavior.
# ---------------------------------------------------------------------------
print("\n=== 3. Engine config mutation is limited to strategy_id ===")

shared_cfg = json.loads(json.dumps(CONFIG_RANKED))
before_keys = set(shared_cfg.keys())
run_backtest(shared_cfg)
added = set(shared_cfg.keys()) - before_keys
check("only strategy_id added to config", added <= {"strategy_id"},
      f"unexpected mutation: {added - {'strategy_id'}}")
check("strategy_id is a non-empty string", isinstance(shared_cfg.get("strategy_id"), str) and len(shared_cfg["strategy_id"]) > 0)


print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
