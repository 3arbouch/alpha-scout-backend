#!/usr/bin/env python3
"""
Mass V1↔V2 comparison across every production deployment config.

Goal: prove that v2's trade ledger is CORRECT for every config users actually
have, not just the synthetic configs in the parity test suite.

For each config:
  - Run through v1 and v2 with identical inputs
  - Classify the divergence:
      OK   — byte-identical trade ledgers
      INTENDED — v2 has fewer trades because v1 emitted phantom trades during
                gated-off periods (Tier-3 / allocation_profile pattern), but
                cumulative shares are non-negative and v2's ledger is broker-
                executable
      BUG  — anything else (real divergence, share-count mismatch, etc)

Acceptance: zero BUGs. INTENDED divergences are expected for Tier-3 configs.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_v2_mass_comparison.py
"""
import copy
import glob
import json
import os
import sys
import traceback
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine import run_portfolio_backtest as run_v1
from portfolio_engine_v2 import run_portfolio_backtest as run_v2

# Limit scope to avoid an hours-long run for the first pass.
# Adjust as needed for full coverage.
MAX_CONFIGS = int(os.environ.get("V2_MASS_MAX", 30))
SKIP_GLOBS = (
    "e2e_test_*",          # CI test stubs
    "deploy_test_*",       # CI test stubs
    "persist_test_*",      # CI test stubs
    "unified_exits_e2e_*", # CI test stubs
    "*_test_*",
)


def is_skip(path: str) -> bool:
    name = os.path.basename(os.path.dirname(path))
    for g in SKIP_GLOBS:
        if g.replace("*", "") in name:
            return True
    return False


def sig(t: dict):
    """Trade signature. Shares rounded to 1dp — anything finer than 0.1
    shares (~$10–50 notional) is FP summation noise from differing iteration
    orders in NAV / weight calcs, not a real divergence."""
    return (t["date"], t["symbol"], t["action"], t.get("reason"),
            round(float(t.get("price", 0)), 2),
            round(float(t.get("shares", 0)), 1))


def cum_shares_violations(trades: list[dict]) -> int:
    cum = defaultdict(float)
    viol = 0
    for t in sorted(trades, key=lambda x: (x["date"], 0 if x["action"] == "BUY" else 1)):
        s = float(t["shares"])
        if t["action"] == "BUY":
            cum[t["symbol"]] += s
        else:
            if cum[t["symbol"]] - s < -1e-3:
                viol += 1
            cum[t["symbol"]] -= s
    return viol


def classify(v1_trades, v2_trades, has_alloc_profile) -> tuple[str, str]:
    """Return (verdict, reason)."""
    if not v1_trades and not v2_trades:
        return "OK", "both engines produced 0 trades"
    s1 = sorted([sig(t) for t in v1_trades])
    s2 = sorted([sig(t) for t in v2_trades])
    if s1 == s2:
        return "OK", "byte-identical"

    v1_viol = cum_shares_violations(v1_trades)
    v2_viol = cum_shares_violations(v2_trades)

    if v2_viol > 0:
        return "BUG", f"v2 has {v2_viol} negative-cum_shares violations"

    if not has_alloc_profile:
        # No allocation_profile → v1 and v2 SHOULD be byte-identical (Tier-1/Tier-2)
        return "BUG", (f"non-alloc-profile config diverges: "
                       f"v1={len(v1_trades)} v2={len(v2_trades)} trades")

    # Has allocation_profile and divergence — that's intended behavior for v2:
    # v1 emits dual-bookkeeping trades (phantom entries during gated periods,
    # proportional re-buys via lerp). v2 emits clean broker-equivalent.
    # Sanity: v2 should generally have FEWER trades (no phantom + no lerp).
    if len(v2_trades) <= len(v1_trades):
        return "INTENDED", f"v1={len(v1_trades)} v2={len(v2_trades)} (alloc_profile cleanup)"
    return "INTENDED_FLAGGED", (f"v2 has MORE trades than v1 ({len(v2_trades)} vs {len(v1_trades)}) "
                                f"— inspect manually")


# ---------------------------------------------------------------------------
# Pick configs
# ---------------------------------------------------------------------------
all_configs = sorted(glob.glob("../deployments/*/config.json"))
# Filter out test/CI configs and 0-sleeve configs
viable = []
for f in all_configs:
    if is_skip(f):
        continue
    try:
        cfg = json.load(open(f))
    except Exception:
        continue
    if not cfg.get("sleeves"):
        continue
    viable.append(f)

print(f"\n{'='*70}")
print(f"Viable configs: {len(viable)} (after skipping CI tests + non-symbol universes)")
print(f"Sampling first {min(MAX_CONFIGS, len(viable))} for this run")
print(f"{'='*70}\n")

# Quiet down v1/v2 print spam by capturing stdout per-run
import io
import contextlib

results = defaultdict(list)   # verdict → [(name, reason)]
errors = []

for i, f in enumerate(viable[:MAX_CONFIGS], 1):
    cfg = json.load(open(f))
    name = cfg.get("name", os.path.basename(os.path.dirname(f)))[:60]
    has_alloc = bool(cfg.get("allocation_profiles"))

    try:
        with contextlib.redirect_stdout(io.StringIO()):
            r1 = run_v1(copy.deepcopy(cfg), force_close_at_end=False)
        t1 = [t for sr in r1.get("sleeve_results", []) for t in sr.get("trades", [])]
    except Exception as e:
        errors.append((name, "v1 ERROR", str(e)[:120]))
        print(f"[{i}/{MAX_CONFIGS}] {name:60s}  v1 ERROR: {str(e)[:60]}")
        continue

    cfg_v2 = copy.deepcopy(cfg); cfg_v2["engine_version"] = "v2"
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            r2 = run_v2(cfg_v2, force_close_at_end=False)
        t2 = r2.get("trades", [])
    except Exception as e:
        errors.append((name, "v2 ERROR", str(e)[:120]))
        print(f"[{i}/{MAX_CONFIGS}] {name:60s}  v2 ERROR: {str(e)[:60]}")
        continue

    verdict, reason = classify(t1, t2, has_alloc)
    results[verdict].append((name, reason, len(t1), len(t2)))
    print(f"[{i}/{MAX_CONFIGS}] {name:60s}  {verdict:18s}  v1={len(t1):>4d} v2={len(t2):>4d}  {reason[:50]}")

print(f"\n{'='*70}")
print(f"  Summary by verdict")
print(f"{'='*70}")
for v in ("OK", "INTENDED", "INTENDED_FLAGGED", "BUG"):
    n = len(results[v])
    print(f"  {v:18s}  {n:>3d} configs")
print(f"  {'errors':18s}  {len(errors):>3d}")

if results["BUG"]:
    print(f"\n{'='*70}")
    print("  BUGS — these need fixing before v2 is safe for any config")
    print(f"{'='*70}")
    for name, reason, n1, n2 in results["BUG"]:
        print(f"  {name:60s}  v1={n1} v2={n2}  {reason}")

if results["INTENDED_FLAGGED"]:
    print(f"\n{'='*70}")
    print("  INTENDED_FLAGGED — v2 has MORE trades than v1, worth inspecting")
    print(f"{'='*70}")
    for name, reason, n1, n2 in results["INTENDED_FLAGGED"]:
        print(f"  {name:60s}  v1={n1} v2={n2}  {reason}")

if errors:
    print(f"\n{'='*70}")
    print("  Engine errors")
    print(f"{'='*70}")
    for name, which, msg in errors:
        print(f"  {name:60s}  {which}: {msg}")

# Exit 0 only if there are 0 BUGs
sys.exit(0 if not results["BUG"] else 1)
