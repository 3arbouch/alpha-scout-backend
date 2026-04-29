#!/usr/bin/env python3
"""
End-to-end tests for the optional `smoothing` parameter on feature_threshold
and feature_percentile.

Covers:
  1. Schema accepts smoothing in valid range; rejects out-of-bounds values.
  2. `_apply_sma_to_series` math: hand-verified against known inputs.
  3. Backtest with smoothing produces fewer/different fire dates than raw.
  4. Lookahead-clean: smoothed value at T equals mean of raw[T-N+1..T] only.
  5. feature_percentile smooth-then-rank semantic verified.
  6. evaluate_signal accepts and respects smoothing.

Run:
  cd /home/mohamed/alpha-scout-backend-dev
  DATA_DIR=/home/mohamed/alpha-scout-backend/data \
  MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \
  APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \
  python3 tests/test_smoothing_e2e.py
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, os.path.join(ROOT, "auto_trader"))

from server.models.strategy import StrategyConfig, FeatureThresholdCondition, FeaturePercentileCondition
from pydantic import ValidationError

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
# 1. Schema validation
# ---------------------------------------------------------------------------
print("\n=== Schema: smoothing field on feature_threshold ===")

# Valid: smoothing=5
try:
    FeatureThresholdCondition(feature="rsi_14", operator="<=", value=30, smoothing=5)
    check("smoothing=5 accepted", True)
except ValidationError as e:
    check("smoothing=5 accepted", False, str(e)[:100])

# Valid: omit smoothing (default None)
ft = FeatureThresholdCondition(feature="rsi_14", operator="<=", value=30)
check("smoothing omitted defaults to None", ft.smoothing is None)

# Invalid: smoothing=1 (below min)
try:
    FeatureThresholdCondition(feature="rsi_14", operator="<=", value=30, smoothing=1)
    check("smoothing=1 rejected", False, "should have failed validation")
except ValidationError:
    check("smoothing=1 rejected", True)

# Invalid: smoothing=100 (above max 60)
try:
    FeatureThresholdCondition(feature="rsi_14", operator="<=", value=30, smoothing=100)
    check("smoothing=100 rejected", False, "should have failed validation")
except ValidationError:
    check("smoothing=100 rejected", True)

# Same checks on feature_percentile
print("\n=== Schema: smoothing field on feature_percentile ===")
try:
    FeaturePercentileCondition(feature="pe", max_percentile=20, smoothing=10)
    check("smoothing=10 accepted on percentile", True)
except ValidationError as e:
    check("smoothing=10 accepted on percentile", False, str(e)[:100])

try:
    FeaturePercentileCondition(feature="pe", max_percentile=20, smoothing=0)
    check("smoothing=0 rejected on percentile", False)
except ValidationError:
    check("smoothing=0 rejected on percentile", True)


# ---------------------------------------------------------------------------
# 2. _apply_sma_to_series math (hand-verified)
# ---------------------------------------------------------------------------
print("\n=== _apply_sma_to_series math ===")
from backtest_engine import _apply_sma_to_series

pts = [("2024-01-01", 10.0), ("2024-01-02", 20.0), ("2024-01-03", 30.0),
       ("2024-01-04", 40.0), ("2024-01-05", 50.0)]
sma3 = _apply_sma_to_series(pts, 3)
# SMA(3) starts at index 2 (third row): mean(10, 20, 30) = 20
# index 3: mean(20, 30, 40) = 30
# index 4: mean(30, 40, 50) = 40
expected = [("2024-01-03", 20.0), ("2024-01-04", 30.0), ("2024-01-05", 40.0)]
check("SMA-3 over 5 ascending integers", sma3 == expected, f"got {sma3}")

# Insufficient history
sma6 = _apply_sma_to_series(pts, 6)
check("insufficient history → empty list", sma6 == [])

# Window=1 (below min) returns empty
sma1 = _apply_sma_to_series(pts, 1)
check("window=1 returns empty (below min)", sma1 == [])


# ---------------------------------------------------------------------------
# 3. Backtest with smoothing produces different fires than raw
# ---------------------------------------------------------------------------
print("\n=== Backtest: smoothed RSI fires differently from raw ===")
from backtest_engine import run_backtest
from collections import Counter

BASE = {
    "name": "smoothing_test",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "NVDA", "JPM", "XOM"]},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100_000},
    "backtest": {"start": "2023-01-01", "end": "2024-06-30",
                 "entry_price": "next_close", "slippage_bps": 0},
}

cfg_raw = json.loads(json.dumps(BASE))
cfg_raw["entry"] = {"conditions": [
    {"type": "feature_threshold", "feature": "rsi_14", "operator": "<=", "value": 30}
], "logic": "all"}
cfg_smoothed = json.loads(json.dumps(BASE))
cfg_smoothed["entry"] = {"conditions": [
    {"type": "feature_threshold", "feature": "rsi_14", "operator": "<=", "value": 30, "smoothing": 5}
], "logic": "all"}

r_raw = run_backtest(cfg_raw)
r_smooth = run_backtest(cfg_smoothed)
buys_raw = {(t["symbol"], t["date"]) for t in r_raw["trades"] if t["action"] == "BUY"}
buys_smooth = {(t["symbol"], t["date"]) for t in r_smooth["trades"] if t["action"] == "BUY"}

check("raw produces buys", len(buys_raw) > 0, f"raw_buys={len(buys_raw)}")
check("smoothed produces buys", len(buys_smooth) > 0, f"smooth_buys={len(buys_smooth)}")
# They're not necessarily a strict subset (sizing may pick different fills),
# but the underlying signal sets must differ — smoothed should be more selective.
check("smoothed signal set differs from raw",
      buys_raw != buys_smooth,
      f"raw={len(buys_raw)} smoothed={len(buys_smooth)}")
print(f"  raw fires: {sorted(buys_raw)[:5]}")
print(f"  smoothed fires: {sorted(buys_smooth)[:5]}")


# ---------------------------------------------------------------------------
# 4. Lookahead-clean check: smoothed[T] == mean(raw[T-N+1..T])
# ---------------------------------------------------------------------------
print("\n=== Lookahead-clean: smoothed value uses backward window only ===")
import sqlite3
conn = sqlite3.connect("/home/mohamed/alpha-scout-backend/data/market.db")
# Pull raw RSI for AAPL via the engine's internal path so it's the same source
from backtest_engine import _load_feature_series, build_price_index

# Build a small price index for AAPL
sym_rows = conn.execute(
    "SELECT date, close FROM prices WHERE symbol='AAPL' AND date BETWEEN '2023-06-01' AND '2023-09-30' "
    "ORDER BY date ASC"
).fetchall()
price_index = {"AAPL": {d: float(c) for d, c in sym_rows}}

raw_series = _load_feature_series("rsi_14", ["AAPL"], "2023-06-01", "2023-09-30",
                                   conn, price_index=price_index, smoothing=None)
smooth_series = _load_feature_series("rsi_14", ["AAPL"], "2023-06-01", "2023-09-30",
                                      conn, price_index=price_index, smoothing=5)

raw_pts = raw_series.get("AAPL", [])
smooth_pts = smooth_series.get("AAPL", [])
check("raw series has data", len(raw_pts) > 10)
check("smoothed series has data", len(smooth_pts) > 10)
check("smoothed series is shorter than raw (drops first N-1)",
      len(smooth_pts) < len(raw_pts))

# Verify each smoothed point equals the mean of the corresponding 5 raw points
raw_dict = dict(raw_pts)
mismatches = 0
for d, smooth_v in smooth_pts[:20]:
    # Find the index of d in raw_pts
    raw_dates = [p[0] for p in raw_pts]
    if d not in raw_dates:
        continue
    idx = raw_dates.index(d)
    if idx < 4:
        continue
    expected = sum(raw_pts[i][1] for i in range(idx - 4, idx + 1)) / 5
    if abs(smooth_v - expected) > 1e-9:
        mismatches += 1
check("smoothed[T] = mean(raw[T-4..T]) for first 20 dates", mismatches == 0,
      f"{mismatches} mismatches")


# ---------------------------------------------------------------------------
# 5. feature_percentile smooth-then-rank semantic
# ---------------------------------------------------------------------------
print("\n=== feature_percentile smoothing changes ranking ===")
cfg_pct_raw = json.loads(json.dumps(BASE))
cfg_pct_raw["entry"] = {"conditions": [
    {"type": "feature_percentile", "feature": "pe", "max_percentile": 30, "scope": "universe"}
], "logic": "all"}
cfg_pct_smooth = json.loads(json.dumps(BASE))
cfg_pct_smooth["entry"] = {"conditions": [
    {"type": "feature_percentile", "feature": "pe", "max_percentile": 30, "scope": "universe", "smoothing": 20}
], "logic": "all"}

r_pct_raw = run_backtest(cfg_pct_raw)
r_pct_smooth = run_backtest(cfg_pct_smooth)
buys_pct_raw = {(t["symbol"], t["date"]) for t in r_pct_raw["trades"] if t["action"] == "BUY"}
buys_pct_smooth = {(t["symbol"], t["date"]) for t in r_pct_smooth["trades"] if t["action"] == "BUY"}

check("feature_percentile raw produces buys", len(buys_pct_raw) > 0)
check("feature_percentile smoothed produces buys", len(buys_pct_smooth) > 0)
check("smooth-then-rank produces different rankings",
      buys_pct_raw != buys_pct_smooth,
      f"raw={len(buys_pct_raw)} smoothed={len(buys_pct_smooth)}")


# ---------------------------------------------------------------------------
# 6. evaluate_signal respects smoothing
# ---------------------------------------------------------------------------
print("\n=== evaluate_signal: smoothing affects forward-return stats ===")
from signal_ranker import evaluate_signal

r_raw_es = evaluate_signal(
    signal_config={"type": "feature_threshold", "feature": "rsi_14", "operator": "<=", "value": 30},
    target_horizon="3m",
    db_path="/home/mohamed/alpha-scout-backend/data/market.db",
    start="2023-01-01", end="2024-06-30",
    universe=["AAPL", "MSFT", "NVDA", "JPM", "XOM", "GOOGL"],
)
r_smooth_es = evaluate_signal(
    signal_config={"type": "feature_threshold", "feature": "rsi_14", "operator": "<=", "value": 30, "smoothing": 5},
    target_horizon="3m",
    db_path="/home/mohamed/alpha-scout-backend/data/market.db",
    start="2023-01-01", end="2024-06-30",
    universe=["AAPL", "MSFT", "NVDA", "JPM", "XOM", "GOOGL"],
)
check("evaluate_signal raw triggered", r_raw_es.get("trigger_count", 0) > 0)
check("evaluate_signal smoothed triggered", r_smooth_es.get("trigger_count", 0) > 0)
check("smoothed evaluate_signal differs from raw",
      r_raw_es.get("trigger_count") != r_smooth_es.get("trigger_count")
      or abs(r_raw_es.get("avg_return", 0) - r_smooth_es.get("avg_return", 0)) > 1e-6,
      f"raw_trig={r_raw_es.get('trigger_count')} smooth_trig={r_smooth_es.get('trigger_count')}")
print(f"  raw:      triggers={r_raw_es.get('trigger_count')} avg_return={r_raw_es.get('avg_return'):+.4f}")
print(f"  smoothed: triggers={r_smooth_es.get('trigger_count')} avg_return={r_smooth_es.get('avg_return'):+.4f}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 60}")
print(f"Passed: {PASS}, Failed: {FAIL}")
sys.exit(0 if FAIL == 0 else 1)
