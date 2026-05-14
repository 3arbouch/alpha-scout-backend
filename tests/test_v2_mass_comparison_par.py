#!/usr/bin/env python3
"""
Parallel version of test_v2_mass_comparison.py.

Fans out across N workers (default = ncpu). Same classification logic,
same acceptance criteria (0 BUGs required).

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    V2_MASS_WORKERS=4 python3 test_v2_mass_comparison_par.py
"""
import contextlib
import copy
import glob
import io
import json
import multiprocessing as mp
import os
import sys
from collections import defaultdict

SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts")


def _run_one(cfg_path: str):
    """Worker: run one config through v1 and v2, classify, return (path, verdict, reason, n1, n2)."""
    # Lazy import inside worker so each process picks up its own module state
    sys.path.insert(0, SCRIPTS_DIR)
    from portfolio_engine import run_portfolio_backtest as run_v1
    from portfolio_engine_v2 import run_portfolio_backtest as run_v2

    try:
        cfg = json.load(open(cfg_path))
    except Exception as e:
        return (cfg_path, "ERROR", f"config load: {str(e)[:80]}", 0, 0)

    name = cfg.get("name", os.path.basename(os.path.dirname(cfg_path)))[:60]
    has_alloc = bool(cfg.get("allocation_profiles"))

    try:
        with contextlib.redirect_stdout(io.StringIO()):
            r1 = run_v1(copy.deepcopy(cfg), force_close_at_end=False)
        t1 = [t for sr in r1.get("sleeve_results", []) for t in sr.get("trades", [])]
    except Exception as e:
        return (cfg_path, "V1_ERR", str(e)[:100], 0, 0)

    cfg_v2 = copy.deepcopy(cfg); cfg_v2["engine_version"] = "v2"
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            r2 = run_v2(cfg_v2, force_close_at_end=False)
        t2 = r2.get("trades", [])
    except Exception as e:
        return (cfg_path, "V2_ERR", str(e)[:100], 0, 0)

    verdict, reason = _classify(t1, t2, has_alloc)
    return (cfg_path, verdict, reason, len(t1), len(t2))


def _sig(t):
    return (t["date"], t["symbol"], t["action"], t.get("reason"),
            round(float(t.get("price", 0)), 2),
            round(float(t.get("shares", 0)), 1))


def _cum_violations(trades):
    cum = defaultdict(float); viol = 0
    for t in sorted(trades, key=lambda x: (x["date"], 0 if x["action"] == "BUY" else 1)):
        s = float(t["shares"])
        if t["action"] == "BUY":
            cum[t["symbol"]] += s
        else:
            if cum[t["symbol"]] - s < -1e-3:
                viol += 1
            cum[t["symbol"]] -= s
    return viol


def _classify(t1, t2, has_alloc):
    if not t1 and not t2:
        return "OK", "both 0 trades"
    s1 = sorted(_sig(t) for t in t1)
    s2 = sorted(_sig(t) for t in t2)
    if s1 == s2:
        return "OK", "byte-identical"
    if _cum_violations(t2) > 0:
        return "BUG", f"v2 cum_shares violations"
    if not has_alloc:
        return "BUG", f"non-alloc divergence v1={len(t1)} v2={len(t2)}"
    if len(t2) <= len(t1):
        return "INTENDED", f"v1={len(t1)} v2={len(t2)} (alloc cleanup)"
    return "INTENDED_FLAGGED", f"v2 has MORE trades than v1 ({len(t2)} vs {len(t1)})"


def main():
    # ---- Pick viable configs (same filter as serial version) ------------
    SKIP = ("e2e_test_", "deploy_test_", "persist_test_", "unified_exits_e2e_", "_test_")
    all_configs = sorted(glob.glob("../deployments/*/config.json"))
    viable = []
    for f in all_configs:
        name = os.path.basename(os.path.dirname(f))
        if any(s in name for s in SKIP):
            continue
        try:
            cfg = json.load(open(f))
        except Exception:
            continue
        if not cfg.get("sleeves"):
            continue
        viable.append(f)

    workers = int(os.environ.get("V2_MASS_WORKERS", min(mp.cpu_count(), 4)))
    cap = int(os.environ.get("V2_MASS_MAX", len(viable)))
    todo = viable[:cap]
    print(f"\n{'='*70}")
    print(f"Viable: {len(viable)}  Running: {len(todo)}  Workers: {workers}")
    print(f"{'='*70}\n")

    results = defaultdict(list)
    done = 0
    n = len(todo)
    with mp.Pool(processes=workers) as pool:
        for path, verdict, reason, n1, n2 in pool.imap_unordered(_run_one, todo):
            done += 1
            name = json.load(open(path)).get("name", os.path.basename(os.path.dirname(path)))[:60]
            results[verdict].append((name, reason, n1, n2))
            print(f"[{done:>3d}/{n}] {name:60s}  {verdict:18s}  v1={n1:>4d} v2={n2:>4d}  {reason[:50]}", flush=True)

    print(f"\n{'='*70}\n  Summary\n{'='*70}")
    for v in ("OK", "INTENDED", "INTENDED_FLAGGED", "BUG", "V1_ERR", "V2_ERR", "ERROR"):
        print(f"  {v:18s}  {len(results[v]):>3d}")

    if results["BUG"]:
        print(f"\n{'='*70}\n  BUGS\n{'='*70}")
        for name, reason, n1, n2 in results["BUG"]:
            print(f"  {name:60s}  v1={n1} v2={n2}  {reason}")
    if results["V2_ERR"]:
        print(f"\n{'='*70}\n  V2 errors (engine crashes — these need fixing)\n{'='*70}")
        for name, reason, _, _ in results["V2_ERR"]:
            print(f"  {name:60s}  {reason}")

    sys.exit(0 if not results["BUG"] and not results["V2_ERR"] else 1)


if __name__ == "__main__":
    main()
