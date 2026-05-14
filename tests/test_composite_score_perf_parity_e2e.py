#!/usr/bin/env python3
"""
Composite-score performance-fix parity test.

Phase 1 of the live-trading plan introduced a PRELOAD path for composite_score
factors: at the top of run_backtest, each factor's full-range series is loaded
ONCE via `_load_feature_series(fname, symbols, bt_start, bt_end, conn)`. The
result is passed through rank_candidates into _compute_composite_score, which
prefers the preloaded series over a per-day DB call.

This test ensures the preload path is BYTE-IDENTICAL to the legacy per-day
fallback path. Both paths run the same bisect-as-of logic; only the data
source differs (full-range cached vs per-day DB query). For any (sym, date)
the bisect should return the same value.

Test approach:
  1. Run a representative composite_score backtest with the preload path
     (the new default), capture the trade ledger.
  2. Monkey-patch _compute_composite_score to discard `preloaded_series`,
     forcing the legacy per-day DB lookup.
  3. Re-run the IDENTICAL backtest, capture the trade ledger.
  4. Compare trade ledgers byte-for-byte. Both NAV and trades should match.

If this passes: the preload path is a pure performance fix, not a behavior
change. The agent's composite_score strategies will produce the same trades
as before, just faster.

Universe: 40 mid/large-cap names with mixed dividend / growth / loss-maker
profiles — enough variety to surface NULL-feature edge cases.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_composite_score_perf_parity_e2e.py
"""
import copy
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import backtest_engine as be
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


# ---------------------------------------------------------------------------
# Universe + strategy
# ---------------------------------------------------------------------------
UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    "JNJ", "PG", "KO", "PEP", "WMT", "JPM", "BAC", "XOM", "CVX",
    "AMD", "AVGO", "CRM", "ADBE", "NOW",
    "PFE", "MRK", "ABBV", "UNH", "COST", "TGT",
    "BA", "CAT", "DE", "GE", "HON",
    "COIN", "ABNB", "PLTR", "CRWD", "DDOG", "NET",
    "ARM",
]

STRATEGY = {
    "name": "ParityProbe",
    "universe": {"type": "symbols", "symbols": UNIVERSE},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "stop_loss": {"type": "drawdown_from_entry", "value": -20, "cooldown_days": 60},
    "take_profit": {"type": "above_peak", "value": 25},
    "time_stop": {"max_days": 180},
    "ranking": {
        "by": "composite_score",
        "order": "desc",
        "top_n": 10,
    },
    "composite_score": {
        "standardization": "z",
        "buckets": {
            "growth": {
                "factors": [
                    {"name": "rev_yoy", "sign": "+"},
                    {"name": "op_margin_yoy_delta", "sign": "+"},
                ],
                "weight": 2,
            },
            "momentum": {
                "factors": [{"name": "ret_12_1m", "sign": "+"}],
                "weight": 1.5,
            },
            "value": {
                "factors": [
                    {"name": "pe", "sign": "-"},
                    {"name": "ev_ebitda", "sign": "-"},
                ],
                "weight": 1,
            },
        },
    },
    "rebalancing": {
        "frequency": "quarterly", "mode": "trim",
        "rules": {"max_position_pct": 15},
    },
    "sizing": {"type": "equal_weight", "max_positions": 10, "initial_allocation": 1000000},
    "backtest": {
        "start": "2023-01-01", "end": "2024-12-31",
        "entry_price": "next_close",
        "slippage_bps": 10,
    },
}


def trade_sig(t):
    """Comparable trade tuple — excludes free-form signal_detail."""
    return (
        t["date"], t["symbol"], t["action"], t.get("reason"),
        round(float(t.get("price", 0)), 4),
        round(float(t.get("shares", 0)), 6),
    )


# ---------------------------------------------------------------------------
# Run 1 — PRELOAD PATH (the new default)
# ---------------------------------------------------------------------------
print("\n=== 1. Run with PRELOAD path (new default) ===")

t0 = time.time()
result_preload = run_backtest(copy.deepcopy(STRATEGY))
elapsed_preload = time.time() - t0
trades_preload = result_preload["trades"]
nav_preload = result_preload["metrics"]["final_nav"]
print(f"  elapsed: {elapsed_preload:.1f}s   trades: {len(trades_preload)}   final_nav: ${nav_preload:,.2f}")


# ---------------------------------------------------------------------------
# Run 2 — Force per-day fallback by stripping `preloaded_series`
# ---------------------------------------------------------------------------
print("\n=== 2. Run with per-day fallback (legacy path) ===")

_orig_compute = be._compute_composite_score

def _compute_no_preload(symbols, conn, date, price_index, composite_config,
                        preloaded_series=None):
    # Drop the preloaded series so the function falls back to its per-day
    # _load_feature_series query path.
    return _orig_compute(symbols, conn, date, price_index, composite_config,
                         preloaded_series=None)

be._compute_composite_score = _compute_no_preload

t1 = time.time()
result_legacy = run_backtest(copy.deepcopy(STRATEGY))
elapsed_legacy = time.time() - t1
trades_legacy = result_legacy["trades"]
nav_legacy = result_legacy["metrics"]["final_nav"]
print(f"  elapsed: {elapsed_legacy:.1f}s   trades: {len(trades_legacy)}   final_nav: ${nav_legacy:,.2f}")

# Restore
be._compute_composite_score = _orig_compute


# ---------------------------------------------------------------------------
# 3. Parity
# ---------------------------------------------------------------------------
print("\n=== 3. Parity check ===")

speedup = elapsed_legacy / elapsed_preload if elapsed_preload > 0 else 0
print(f"  Speed: legacy={elapsed_legacy:.1f}s, preload={elapsed_preload:.1f}s   ({speedup:.1f}× faster)")

check("trade count identical",
      len(trades_preload) == len(trades_legacy),
      f"preload={len(trades_preload)} legacy={len(trades_legacy)}")

check("final_nav byte-identical",
      nav_preload == nav_legacy,
      f"preload={nav_preload} legacy={nav_legacy}")

sigs_preload = [trade_sig(t) for t in trades_preload]
sigs_legacy = [trade_sig(t) for t in trades_legacy]

trades_match = sigs_preload == sigs_legacy
check("trade ledger byte-identical (preload == legacy)",
      trades_match,
      f"first diff at index "
      f"{next((i for i,(a,b) in enumerate(zip(sigs_preload,sigs_legacy)) if a!=b), 'none')}")

if not trades_match:
    print("\n  Diff (first 10):")
    for i, (a, b) in enumerate(zip(sigs_preload, sigs_legacy)):
        if a != b:
            print(f"    [{i}] preload: {a}")
            print(f"        legacy:  {b}")
            if i > 10:
                break

# Top-level metrics
print("\n  Top-level metrics:")
for k in ("total_return_pct", "annualized_return_pct", "max_drawdown_pct",
          "sharpe_ratio", "annualized_volatility_pct"):
    p = result_preload["metrics"].get(k)
    l_ = result_legacy["metrics"].get(k)
    if p == l_:
        check(f"metrics.{k}: {p}", True)
    else:
        check(f"metrics.{k}: preload={p} vs legacy={l_}", False,
              f"differ: preload={p} legacy={l_}")


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
