#!/usr/bin/env python3
"""
Unit test (Gap 4): entry-condition pre-computation math.

Verifies the pure-function signal detectors in scripts/signals.py used by
the engine's pre-compute path:

  • find_current_drops   — current close vs preceding-window high
  • find_daily_drops     — single-session drop
  • find_period_drops    — worst intra-window peak-to-trough drawdown

Each test builds a tiny synthetic price series with hand-picked peaks and
troughs, then compares the function output against hand-computed expected
events.

Also verifies the engine's calendar→trading day conversion used by the
config-to-precompute layer.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_signals_precompute_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from signals import find_current_drops, find_daily_drops, find_period_drops
from backtest_engine import _calendar_to_trading_days

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


def approx(a, b, tol=1e-2):
    if a is None or b is None:
        return False
    return abs(a - b) < tol


# ---------------------------------------------------------------------------
# 1. _calendar_to_trading_days
# ---------------------------------------------------------------------------
print("\n=== 1. _calendar_to_trading_days ===")
# 90 calendar days × 5/7 = 64.28 → round → 64
check("90 calendar → 64 trading", _calendar_to_trading_days(90) == 64,
      f"got {_calendar_to_trading_days(90)}")
# 7 calendar days = 1 week = 5 trading days
check("7 calendar → 5 trading", _calendar_to_trading_days(7) == 5)
# 1 calendar day floored to 1 trading day (max(1, round(5/7)) = max(1, 1) = 1)
check("1 calendar → 1 trading (floor)", _calendar_to_trading_days(1) == 1)
check("0 calendar → 1 trading (floor)", _calendar_to_trading_days(0) == 1)


# ---------------------------------------------------------------------------
# 2. find_daily_drops
# ---------------------------------------------------------------------------
print("\n=== 2. find_daily_drops ===")
prices = [
    ("2024-01-02", 100.0),
    ("2024-01-03", 99.0),    # -1.0%
    ("2024-01-04", 94.05),   # -5.0% from 99 → triggers at -5
    ("2024-01-05", 90.0),    # -4.31% from 94.05 → no trigger
    ("2024-01-08", 80.0),    # -11.1% from 90 → triggers
]
events = find_daily_drops(prices, threshold=-5)
check("daily_drop count = 2", len(events) == 2, f"got {len(events)}")
# Verify the exact dates and percent change
ev_dates = [e["date"] for e in events]
check("events on 2024-01-04 and 2024-01-08",
      ev_dates == ["2024-01-04", "2024-01-08"],
      f"got {ev_dates}")
check("2024-01-04 change_pct ≈ -5.0",
      approx(events[0]["change_pct"], -5.0))
check("2024-01-08 change_pct ≈ -11.11",
      approx(events[1]["change_pct"], -11.11))

# Boundary: exactly threshold triggers (<=)
prices_boundary = [("2024-01-02", 100.0), ("2024-01-03", 95.0)]  # exactly -5%
events = find_daily_drops(prices_boundary, threshold=-5)
check("change_pct == threshold (-5%) → triggers (<= boundary)",
      len(events) == 1, f"got {len(events)}")

prices_no_trigger = [("2024-01-02", 100.0), ("2024-01-03", 95.5)]  # -4.5%
events = find_daily_drops(prices_no_trigger, threshold=-5)
check("change_pct > threshold (-4.5% > -5%) → no trigger",
      len(events) == 0)

# Defensive: prev_close <= 0 must not blow up
events = find_daily_drops([("2024-01-02", 0.0), ("2024-01-03", 1.0)], threshold=-5)
check("prev_close=0 → skipped, no events", len(events) == 0)

# Short series
check("len(prices) < 2 → empty", find_daily_drops([("2024-01-02", 100.0)], -5) == [])


# ---------------------------------------------------------------------------
# 3. find_current_drops
# ---------------------------------------------------------------------------
print("\n=== 3. find_current_drops ===")
# Build a 6-day series: window=5 means we look at i in [5, len) → just i=5.
# window = prices[0:5], current = prices[5].
# Peak in window: max of [100, 105, 110, 108, 102] = 110
# Current = 88, dd = (88-110)/110 = -20% → triggers at -15
series = [
    ("2024-01-02", 100.0),
    ("2024-01-03", 105.0),
    ("2024-01-04", 110.0),  # peak
    ("2024-01-05", 108.0),
    ("2024-01-08", 102.0),
    ("2024-01-09", 88.0),    # current — 20% below peak
]
events = find_current_drops(series, period_days=5, threshold=-15)
check("current_drop fires on 2024-01-09", len(events) == 1)
if events:
    e = events[0]
    check("signal_date = 2024-01-09", e["signal_date"] == "2024-01-09")
    check("peak_price = 110.0", approx(e["peak_price"], 110.0))
    check("peak_date = 2024-01-04", e["peak_date"] == "2024-01-04")
    check("drawdown_pct = -20.0", approx(e["drawdown_pct"], -20.0))

# Window excludes the current bar (no lookahead): if the current bar IS the
# peak, the lookback can't see it. Construct a series where today is the high
# but the lookback window only has lower values → no drawdown vs lookback peak.
series_no_drop = [
    ("2024-01-02", 100.0),
    ("2024-01-03", 95.0),
    ("2024-01-04", 90.0),
    ("2024-01-05", 92.0),
    ("2024-01-08", 95.0),
    ("2024-01-09", 110.0),  # today is the peak — no drop
]
events = find_current_drops(series_no_drop, period_days=5, threshold=-15)
check("today=peak with falling lookback → no event",
      len(events) == 0, f"got {len(events)}")

# Boundary: exactly -15% triggers
series_boundary = [
    ("2024-01-02", 100.0), ("2024-01-03", 100.0), ("2024-01-04", 100.0),
    ("2024-01-05", 100.0), ("2024-01-08", 100.0),
    ("2024-01-09", 85.0),  # exactly -15
]
events = find_current_drops(series_boundary, period_days=5, threshold=-15)
check("dd == threshold (-15%) → triggers (<=)",
      len(events) == 1)

# Series too short
check("len(prices) <= period_days → empty",
      find_current_drops([("2024-01-02", 100.0)], period_days=5, threshold=-15) == [])


# ---------------------------------------------------------------------------
# 4. find_period_drops — worst peak-to-trough WITHIN window
# ---------------------------------------------------------------------------
print("\n=== 4. find_period_drops ===")
# Note: find_period_drops guards with `len(prices) <= period_days → []`, so
# at least period_days+1 prices are required to produce a window.
# 5-day window over 6 prices: 2 sliding windows (i=5, i=6).
# Series: peak 110 at idx 1, trough 80 at idx 3 → drawdown = (80-110)/110 = -27.27%
series = [
    ("2024-01-02", 100.0),
    ("2024-01-03", 110.0),   # peak
    ("2024-01-04", 95.0),
    ("2024-01-05", 80.0),    # trough
    ("2024-01-08", 85.0),
    ("2024-01-09", 90.0),    # extra bar so window has room
]
events = find_period_drops(series, period_days=5, threshold=-20)
check("at least one event fires", len(events) >= 1, f"got {len(events)}")
if events:
    e = events[0]
    check("peak detected at 2024-01-03 (110)", e["peak_date"] == "2024-01-03")
    check("trough detected at 2024-01-05 (80)", e["trough_date"] == "2024-01-05")
    check("drawdown ≈ -27.27%", approx(e["drawdown_pct"], -27.27))
    check("period_days carried through to event", e["period_days"] == 5)

# Note: find_period_drops uses a running peak that updates only when close
# >= running_peak. So trough following peak is what gets recorded. If the
# trough comes BEFORE any peak rise, dd is computed against the first bar.
# Series with peak at end (no trough after) → no event
series_ascending = [
    ("2024-01-02", 80.0),
    ("2024-01-03", 85.0),
    ("2024-01-04", 90.0),
    ("2024-01-05", 95.0),
    ("2024-01-08", 110.0),  # peak at end → no subsequent trough
]
events = find_period_drops(series_ascending, period_days=5, threshold=-10)
check("monotonically ascending → no drawdown event",
      len(events) == 0, f"got {len(events)}")

# Threshold too tight → no event
events = find_period_drops(series, period_days=5, threshold=-50)
check("threshold -50% with -27% drawdown → no event",
      len(events) == 0)


# ---------------------------------------------------------------------------
# 5. find_period_drops — sliding-window event count
# ---------------------------------------------------------------------------
print("\n=== 5. find_period_drops sliding window ===")
# 7-day series with one big crash mid-series, 5-day window
# Each sliding window that contains the crash should produce an event.
series = [
    ("2024-01-02", 100.0),
    ("2024-01-03", 100.0),
    ("2024-01-04", 100.0),  # peak (also peaks at 100)
    ("2024-01-05", 60.0),   # trough → -40%
    ("2024-01-08", 65.0),
    ("2024-01-09", 70.0),
    ("2024-01-10", 75.0),
]
# Sliding windows of size 5 over 7 prices: 3 windows (i=5, 6, 7).
# Window 1 (idx 0-4): peak 100, trough 60 → -40% ✓
# Window 2 (idx 1-5): peak 100, trough 60 → -40% ✓
# Window 3 (idx 2-6): peak 100, trough 60 → -40% ✓
# All three windows include both peak and trough.
events = find_period_drops(series, period_days=5, threshold=-30)
check("crash visible in 3 sliding windows → 3 events",
      len(events) == 3, f"got {len(events)}")

# When the crash slides out of the window, no more events
series_with_recovery = series + [
    ("2024-01-11", 80.0),
    ("2024-01-12", 90.0),
    ("2024-01-15", 100.0),
    ("2024-01-16", 105.0),  # full recovery
]
events = find_period_drops(series_with_recovery, period_days=5, threshold=-30)
# Once the original peak/trough drop out of the window, no more events.
# Sliding windows where peak (100 at idx 0/1/2) and trough (60 at idx 3) are
# BOTH present: i=5,6,7. After i=7, peak leaves window.
check("after crash exits the window → no more events (3 total)",
      len(events) == 3, f"got {len(events)}")


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
