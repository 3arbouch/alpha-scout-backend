#!/usr/bin/env python3
"""
V2 rebalance modes parity: equal_weight (rotation) and tied-signal fill order.

Two specific v2 regressions surfaced during the mass v1↔v2 audit:

1. **Tied-signal sort order**: when multiple symbols fire entry signals with
   identical signal values on the same day, both engines fall back to
   insertion order (stable sort). V1 walks `resolve_universe(sorted=True)`,
   so its insertion order is alphabetical. V2 was walking `signals.keys()`
   in dict order. Fixed: v2 now uses `sorted(signals.keys())`.

2. **equal_weight rebalance not implemented**: v1's `_do_equal_weight_rebalance`
   does ranking-driven rotation (sell out-of-top-N, reweight, buy new
   entrants) on rebalance dates. V2 had a stub comment saying this was
   "handled by a separate executor path" — it wasn't. Configs using
   `rebalancing.mode == "equal_weight"` (e.g. Tech Momentum Regime-Gated v3)
   lost 80%+ of their trades. Fixed: `_apply_equal_weight_rebalance` in
   portfolio_engine_v2.py mirrors v1's algorithm step-by-step.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_v2_rebalance_modes_e2e.py
"""
import copy
import json
import os
import sys
import contextlib
import io

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine import run_portfolio_backtest as run_v1
from portfolio_engine_v2 import run_portfolio_backtest as run_v2


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


def sig(t):
    return (t["date"], t["symbol"], t["action"], t.get("reason"),
            round(float(t["price"]), 2), round(float(t["shares"]), 1))


def run_both(cfg):
    with contextlib.redirect_stdout(io.StringIO()):
        r1 = run_v1(copy.deepcopy(cfg), force_close_at_end=False)
        cfg2 = copy.deepcopy(cfg); cfg2["engine_version"] = "v2"
        r2 = run_v2(cfg2, force_close_at_end=False)
    t1 = sorted([t for sr in r1.get("sleeve_results", []) for t in sr.get("trades", [])],
                key=lambda x: (x["date"], x["symbol"], x["action"]))
    t2 = sorted(r2.get("trades", []), key=lambda x: (x["date"], x["symbol"], x["action"]))
    return t1, t2


# ---------------------------------------------------------------------------
# Test 1: Tied-signal fill order — uses a real config from the audit
# (Energy Dip + Tighter Trend Gate v15) where on 2025-01-07 three symbols
# (COP, EOG, FANG) tie at signal=3 (earnings beats count). Pre-fix, v2 filled
# FANG → EOG → COP; v1 fills COP → EOG → FANG (alphabetical from
# resolve_universe). Post-fix both should match.
# ---------------------------------------------------------------------------
print("\n=== 1. Tied-signal entries fill in same order in v1 and v2 ===")
cfg_path = "../deployments/energy_dip_tighter_trend_gate_v15_3588235f/config.json"
if os.path.exists(cfg_path):
    cfg1 = json.load(open(cfg_path))
    t1, t2 = run_both(cfg1)
    s1 = set(sig(t) for t in t1)
    s2 = set(sig(t) for t in t2)
    check(f"trade counts match (v1={len(t1)} v2={len(t2)})", len(t1) == len(t2))
    check("trade signatures byte-identical",
          s1 == s2,
          f"v1_only={len(s1-s2)} v2_only={len(s2-s1)}")
else:
    print(f"  ⚠️  skipped (config not at {cfg_path})")


# ---------------------------------------------------------------------------
# Test 2: equal_weight rebalance with rotation
# Uses Tech Momentum Regime-Gated v3: quarterly equal_weight rebalance with
# composite_score ranking — generates rebalance_rotation + rebalance_trim +
# rotation-entry trades. Pre-fix, v2 missed all of these (only emitted
# entry/stop_loss trades, ~30% of the volume).
# ---------------------------------------------------------------------------
print("\n=== 2. equal_weight rebalance mode emits rotation + reweight trades ===")
cfg_path = "../deployments/tech_momentum_regime_gated_v3_1cf41c86/config.json"
if os.path.exists(cfg_path):
    cfg2_data = json.load(open(cfg_path))
    t1, t2 = run_both(cfg2_data)
    from collections import Counter
    c1 = Counter(t.get("reason") for t in t1)
    c2 = Counter(t.get("reason") for t in t2)
    check(f"v2 emits rebalance_rotation trades (v1={c1.get('rebalance_rotation', 0)}, v2={c2.get('rebalance_rotation', 0)})",
          c2.get("rebalance_rotation", 0) > 0)
    check(f"trade counts match (v1={len(t1)} v2={len(t2)})", len(t1) == len(t2))
    check("trades by reason match exactly",
          c1 == c2, f"v1={dict(c1)} v2={dict(c2)}")
    s1 = set(sig(t) for t in t1)
    s2 = set(sig(t) for t in t2)
    check("trade signatures byte-identical",
          s1 == s2,
          f"v1_only={len(s1-s2)} v2_only={len(s2-s1)}")
else:
    print(f"  ⚠️  skipped (config not at {cfg_path})")


# ---------------------------------------------------------------------------
# Test 3: vol-adaptive stops (realized_vol_multiple + atr_multiple)
# Surfaced two related bugs in v2's compute_stop_pricing wiring:
#   (a) The OHLC fetcher included the entry day's bar (`d <= end_date`),
#       leaking lookahead into the realized-vol sigma. V1's DB fetcher
#       uses strict `< entry_date`. Fixed.
#   (b) V2 passed the RAW close to compute_stop_pricing; v1 passes the
#       SLIPPAGE-ADJUSTED entry price. Fixed.
#   (c) V2's OHLC stub returned high=low=close which collapses ATR. Now
#       uses make_sqlite_ohlc_fetcher (real high/low/close from the DB).
# ---------------------------------------------------------------------------
print("\n=== 3. realized_vol_multiple stop fires byte-identical to v1 ===")
cfg_path = "../deployments/mixed_rv_stop_30pct_tp_canonical__ffbd0fb8/config.json"
if os.path.exists(cfg_path):
    cfg3 = json.load(open(cfg_path))
    t1, t2 = run_both(cfg3)
    check(f"trade counts match (v1={len(t1)} v2={len(t2)})", len(t1) == len(t2))
    s1 = set(sig(t) for t in t1); s2 = set(sig(t) for t in t2)
    check("byte-identical (no lookahead, slippage-adjusted entry, real OHLC)",
          s1 == s2, f"v1_only={len(s1-s2)} v2_only={len(s2-s1)}")
else:
    print(f"  ⚠️  skipped (config not at {cfg_path})")


print("\n=== 4. atr_multiple stop fires byte-identical to v1 ===")
cfg_path = "../deployments/vol_adaptive_stops_live_dev__1bb2e8d2/config.json"
if os.path.exists(cfg_path):
    cfg4 = json.load(open(cfg_path))
    t1, t2 = run_both(cfg4)
    check(f"trade counts match (v1={len(t1)} v2={len(t2)})", len(t1) == len(t2))
    s1 = set(sig(t) for t in t1); s2 = set(sig(t) for t in t2)
    check("byte-identical (ATR needs real high/low; close-only would collapse it)",
          s1 == s2, f"v1_only={len(s1-s2)} v2_only={len(s2-s1)}")
else:
    print(f"  ⚠️  skipped (config not at {cfg_path})")


print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
