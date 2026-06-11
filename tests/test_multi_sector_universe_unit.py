#!/usr/bin/env python3
"""
Unit/integration test: multi-sector universe + cap-weighted blended benchmark.

Covers:
  - resolve_universe unions multiple sectors (type='sector', sectors=[...]).
  - compute_benchmark: single sector → that ETF; many → cap-weighted ETF blend.
  - _sector_cap_weights normalize to 1.
  - BacktestConfig.benchmark_sectors validation.
  - UniverseConfig.sectors / CreateRunRequest.sectors accept lists.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=../data/market_dev.db python3 test_multi_sector_universe_unit.py
"""
import os
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, os.path.join(ROOT, "server"))
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


import backtest_engine as be
from backtest_engine import (
    resolve_universe, compute_benchmark, _sector_cap_weights, get_connection,
)

conn = get_connection()
TECH, COMMS = "Technology", "Communication Services"

print("=== resolve_universe unions multiple sectors ===")
uni_tech = set(resolve_universe({"universe": {"type": "sector", "sector": TECH}}, conn))
uni_comms = set(resolve_universe({"universe": {"type": "sector", "sector": COMMS}}, conn))
uni_both = set(resolve_universe({"universe": {"type": "sector", "sectors": [TECH, COMMS]}}, conn))
check("union ⊇ tech", uni_tech <= uni_both)
check("union ⊇ comms", uni_comms <= uni_both)
check("union == tech ∪ comms", uni_both == (uni_tech | uni_comms),
      f"|both|={len(uni_both)} |t∪c|={len(uni_tech | uni_comms)}")
check("union strictly bigger than tech alone", len(uni_both) > len(uni_tech),
      f"{len(uni_both)} vs {len(uni_tech)}")

print("\n=== cap weights normalize to 1, tech > comms ===")
w = _sector_cap_weights([TECH, COMMS], conn)
check("weights sum to 1", abs(sum(w.values()) - 1.0) < 1e-9, str(w))
check("tech weight > comms weight (tech is larger)", w[TECH] > w[COMMS], str(w))

print("\n=== compute_benchmark: single sector → that ETF ===")
dates = [r[0] for r in conn.execute(
    "SELECT DISTINCT date FROM prices WHERE date BETWEEN '2022-01-01' AND '2023-01-01' ORDER BY date"
).fetchall()]
single = compute_benchmark(dates, 1_000_000, conn=conn, sector=TECH)
check("single sector resolves to XLK", single and single["symbol"] == "XLK",
      str(single and single["symbol"]))

print("\n=== compute_benchmark: many sectors → cap-weighted blend ===")
blend = compute_benchmark(dates, 1_000_000, conn=conn, sectors=[TECH, COMMS])
check("blend label is BLEND:XLK+XLC", blend and blend["symbol"] == "BLEND:XLK+XLC",
      str(blend and blend["symbol"]))
check("blend has realized total_return_pct", blend and blend["metrics"]["total_return_pct"] is not None)
# Blend return should sit between the two single-sector ETF returns (it's a convex combo).
xlk = compute_benchmark(dates, 1_000_000, conn=conn, sectors=[TECH])
xlc = compute_benchmark(dates, 1_000_000, conn=conn, sectors=[COMMS])
if blend and xlk and xlc:
    lo = min(xlk["metrics"]["total_return_pct"], xlc["metrics"]["total_return_pct"])
    hi = max(xlk["metrics"]["total_return_pct"], xlc["metrics"]["total_return_pct"])
    br = blend["metrics"]["total_return_pct"]
    check("blend return between the two ETFs", lo - 0.5 <= br <= hi + 0.5,
          f"{lo} <= {br} <= {hi}")

print("\n=== single-element sectors list == legacy single sector ===")
one = compute_benchmark(dates, 1_000_000, conn=conn, sectors=[TECH])
check("sectors=[Tech] resolves to XLK (back-compat)", one and one["symbol"] == "XLK")

conn.close()

print("\n=== model validation ===")
from models.backtest import BacktestConfig
from models.strategy import UniverseConfig

ok = BacktestConfig(training_start="2015-01-01", training_end="2020-01-01",
                    initial_capital=1e6, benchmark="sector",
                    benchmark_sectors=[TECH, COMMS])
check("BacktestConfig benchmark='sector' + benchmark_sectors valid", ok.benchmark_sectors == [TECH, COMMS])
try:
    BacktestConfig(training_start="2015-01-01", training_end="2020-01-01",
                   initial_capital=1e6, benchmark="sector")
    check("benchmark='sector' with no sectors rejected", False, "did not raise")
except Exception:
    check("benchmark='sector' with no sectors rejected", True)

u = UniverseConfig(type="sector", sectors=[TECH, COMMS])
check("UniverseConfig.sectors accepts a list", u.sectors == [TECH, COMMS])

from importlib import import_module
api = import_module("auto_trader.api")
req = api.CreateRunRequest(name="t", metric="sharpe_ratio", start="2015-01-01",
                           end="2020-01-01", sectors=[TECH, COMMS])
check("CreateRunRequest.sectors accepts a list", req.sectors == [TECH, COMMS])

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
