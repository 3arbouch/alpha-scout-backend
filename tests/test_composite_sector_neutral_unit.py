#!/usr/bin/env python3
"""
Unit test: sector-neutral standardization in _compute_composite_score.

Demonstrates the core behavioral difference between whole-universe and
sector-relative standardization, on a fixture engineered so the two disagree:

  - Sector "Tech" runs hot: every Tech name has high raw factor values.
  - Sector "Health" runs cold: every Health name has low raw factor values.
  - But within each sector there is a clear best name.

Whole-universe standardization → all Tech names outrank all Health names
(it's really a sector bet). Sector-neutral standardization → the best name
in EACH sector floats up (it picks best-in-peer-group, sector level removed).

We monkey-patch _load_feature_series (synthetic factor values) and
_load_symbol_sectors (synthetic sector map), mirroring test_composite_score_unit.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_composite_sector_neutral_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import backtest_engine as be

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
# Fixture
# ---------------------------------------------------------------------------
# One factor, "growth" (sign +). Raw values chosen so:
#   Tech sector level >> Health sector level   (the "hot sector" effect)
#   within Tech:  TBEST > TMID > TLOW
#   within Health: HBEST > HMID > HLOW
SECTORS = {
    "TBEST": "Tech",   "TMID": "Tech",   "TLOW": "Tech",
    "HBEST": "Health", "HMID": "Health", "HLOW": "Health",
}
GROWTH = {
    "TBEST": 100.0, "TMID": 90.0, "TLOW": 80.0,   # all high (hot sector)
    "HBEST": 30.0,  "HMID": 20.0, "HLOW": 10.0,    # all low (cold sector)
}
SYMBOLS = list(SECTORS.keys())


def fake_load_feature_series(fname, symbols, start, end, conn, price_index=None):
    assert fname == "growth", fname
    return {s: [("2024-01-01", GROWTH[s])] for s in symbols if s in GROWTH}


def fake_load_symbol_sectors(symbols, conn):
    return {s: SECTORS[s] for s in symbols if s in SECTORS}


be._load_feature_series = fake_load_feature_series
be._load_symbol_sectors = fake_load_symbol_sectors

CFG_BASE = {
    "buckets": {"growth": {"factors": [{"name": "growth", "sign": "+"}], "weight": 1.0}},
    "standardization": "rank",
}


def score(sector_neutral):
    cfg = dict(CFG_BASE)
    if sector_neutral:
        cfg = {**CFG_BASE, "sector_neutral": True}
    return be._compute_composite_score(SYMBOLS, None, "2024-01-01", None, cfg)


def ranked(scores):
    return [s for s, _ in sorted(scores.items(), key=lambda kv: -kv[1])]


print("=== 1. Whole-universe: hot sector sweeps the top ===")
glob = score(sector_neutral=False)
order_glob = ranked(glob)
check("top 3 are all Tech (sector bet)",
      set(order_glob[:3]) == {"TBEST", "TMID", "TLOW"},
      f"order={order_glob}")
check("every Tech name outranks every Health name",
      min(glob["TBEST"], glob["TMID"], glob["TLOW"]) > max(glob["HBEST"], glob["HMID"], glob["HLOW"]),
      f"scores={glob}")

print("\n=== 2. Sector-neutral: best-in-sector floats up, sector level removed ===")
sn = score(sector_neutral=True)
order_sn = ranked(sn)
check("TBEST and HBEST are the joint top (best of each sector)",
      set(order_sn[:2]) == {"TBEST", "HBEST"},
      f"order={order_sn}")
check("TBEST and HBEST score equally (same within-sector rank)",
      abs(sn["TBEST"] - sn["HBEST"]) < 1e-9,
      f"TBEST={sn['TBEST']} HBEST={sn['HBEST']}")
check("the worst Tech name (TLOW) no longer beats the best Health name (HBEST)",
      sn["HBEST"] > sn["TLOW"],
      f"HBEST={sn['HBEST']} TLOW={sn['TLOW']}")
check("within Tech, ordering preserved (TBEST>TMID>TLOW)",
      sn["TBEST"] > sn["TMID"] > sn["TLOW"],
      f"scores={sn}")

print("\n=== 3. Singleton sector can't be standardized → omitted (no z) ===")
# Add a lone Energy name; its sector group has size 1, so it gets no growth z
# and (its only bucket empty) falls out of the scored set.
SECTORS["ELONE"] = "Energy"
GROWTH["ELONE"] = 999.0
sn2 = be._compute_composite_score(SYMBOLS + ["ELONE"], None, "2024-01-01", None,
                                  {**CFG_BASE, "sector_neutral": True})
check("lone-sector name (ELONE) excluded despite huge raw value",
      "ELONE" not in sn2,
      f"keys={list(sn2.keys())}")
del SECTORS["ELONE"]; del GROWTH["ELONE"]

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
