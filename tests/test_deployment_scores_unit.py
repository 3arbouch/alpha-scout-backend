#!/usr/bin/env python3
"""
Integration test: daily composite-score panel (backfill + read helpers).

Picks any composite_score deployment from the dev app DB, backfills its score
panel, and asserts the invariants the visualization API relies on:
  - one row per (sleeve, date, symbol); ranks are dense 1..N within a date.
  - `selected` count per date == ranking cutoff (top_n / max_positions).
  - series() returns aligned per-symbol time series.
  - day_detail() returns per-bucket breakdown for the whole universe.

Skips cleanly if no composite deployment exists.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    WORKSPACE=/home/mohamed/alpha-scout-backend-dev \
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \
    MARKET_DB_PATH=../data/market_dev.db \
    python3 test_deployment_scores_unit.py
"""
import json
import os
import sqlite3
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, os.path.join(ROOT, "server"))
os.environ.setdefault("WORKSPACE", ROOT)
os.environ.setdefault("APP_DB_PATH", "/home/mohamed/alpha-scout-backend/data/app_dev.db")
os.environ.setdefault("MARKET_DB_PATH", os.path.join(ROOT, "data", "market_dev.db"))

PASS = FAIL = 0


def check(label, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {label}")
    else:
        FAIL += 1
        print(f"  FAIL  {label}  {extra}")


def _pick_deployment():
    c = sqlite3.connect(os.environ["APP_DB_PATH"])
    for did, cfg in c.execute("SELECT id, config_json FROM deployments"):
        if cfg and "composite_score" in cfg:
            try:
                j = json.loads(cfg)
                sc = j.get("sleeves", [{}])[0].get("strategy_config", {})
                if (sc.get("ranking") or {}).get("by") == "composite_score":
                    c.close()
                    return did
            except Exception:
                continue
    c.close()
    return None


import deployment_scores as ds

DID = _pick_deployment()
if not DID:
    print("SKIP: no composite_score deployment in app_dev.db")
    sys.exit(0)

print(f"=== deployment under test: {DID} ===")
summary = ds.compute_and_persist(DID, full=True)
print("backfill:", summary)
check("backfill wrote rows", summary["rows_written"] > 0, str(summary))

rk = ds.ranks(DID)
check("ranks() returned date-events", len(rk) > 0)

# Dense rank invariant + selected==cutoff on the latest date.
last = rk[-1]
ranks_seq = sorted(r["rank"] for r in last["rows"] if r["rank"] is not None)
check("ranks are dense 1..N on latest date",
      ranks_seq == list(range(1, len(ranks_seq) + 1)),
      f"{ranks_seq[:5]}...len={len(ranks_seq)}")
n_selected = sum(1 for r in last["rows"] if r["selected"])
# cutoff from config
_c = sqlite3.connect(os.environ["APP_DB_PATH"])
cfg = json.loads(_c.execute("SELECT config_json FROM deployments WHERE id=?", (DID,)).fetchone()[0])
_c.close()
sc = cfg["sleeves"][0]["strategy_config"]
cutoff = (sc.get("ranking") or {}).get("top_n") or (sc.get("sizing") or {}).get("max_positions")
check("selected count == ranking cutoff",
      cutoff is None or n_selected == min(cutoff, len(last["rows"])),
      f"selected={n_selected} cutoff={cutoff}")
check("selected ⊆ top of the ranking",
      all(r["selected"] == (r["rank"] is not None and cutoff and r["rank"] <= cutoff) for r in last["rows"]))

# series() shape
top_syms = [r["symbol"] for r in last["rows"][:2]]
ser = ds.series(DID, symbols=top_syms)
flat = {s: pts for bysym in ser.values() for s, pts in bysym.items()}
check("series() returns the requested symbols", set(top_syms) <= set(flat))
check("series points carry score+rank+selected+held",
      all(set(("date", "score", "rank", "selected", "held")) <= set(p) for p in next(iter(flat.values()))))

# day_detail() has per-bucket breakdown over the whole universe
dd = ds.day_detail(DID, last["date"])
sl0 = dd["sleeves"][0]
check("day_detail covers the full universe", sl0["n_candidates"] == len(last["rows"]),
      f'{sl0["n_candidates"]} vs {len(last["rows"])}')
top = sl0["candidates"][0]
check("day_detail top candidate has buckets", bool(top.get("buckets")))
check("day_detail tags selected + held", "selected" in top and "held" in top)

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
