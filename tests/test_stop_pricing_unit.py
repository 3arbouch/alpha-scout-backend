#!/usr/bin/env python3
"""
Unit tests for scripts/stop_pricing.py — math validated by hand.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_stop_pricing_unit.py
"""
import math
import os
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from stop_pricing import (
    compute_atr,
    compute_realized_vol,
    compute_stop_pricing,
    EWMA_LAMBDA,
)

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


def approx(a, b, tol=1e-6):
    if a is None or b is None:
        return False
    return abs(a - b) < tol


# ---------------------------------------------------------------------------
# ATR
# ---------------------------------------------------------------------------
print("\n=== ATR ===")

# Constant 2.0 spread, no gaps -> TR = 2.0 every bar -> ATR = 2.0
bars_const_spread = [(102.0, 100.0, 101.0)] * 10
check("constant 2.0 spread, no gaps", approx(compute_atr(bars_const_spread, 5), 2.0))

# Hand-computed: 6 bars, window=5
# bars (h, l, c):
#  bar0: 100, 96, 98     (used only as prior close for bar1)
#  bar1: 102, 99, 101    -> TR1 = max(3, |102-98|=4, |99-98|=1)   = 4
#  bar2: 105, 100, 104   -> TR2 = max(5, |105-101|=4, |100-101|=1)= 5
#  bar3: 103, 101, 102   -> TR3 = max(2, |103-104|=1, |101-104|=3)= 3
#  bar4: 110, 102, 109   -> TR4 = max(8, |110-102|=8, |102-102|=0)= 8
#  bar5: 108, 105, 106   -> TR5 = max(3, |108-109|=1, |105-109|=4)= 4
# ATR(5) = (4+5+3+8+4)/5 = 24/5 = 4.8
bars_hand = [
    (100, 96, 98),
    (102, 99, 101),
    (105, 100, 104),
    (103, 101, 102),
    (110, 102, 109),
    (108, 105, 106),
]
check("hand-computed 5-bar ATR", approx(compute_atr(bars_hand, 5), 4.8))

# Insufficient history
check("insufficient history returns None", compute_atr(bars_hand, 6) is None)
check("zero window returns None", compute_atr(bars_hand, 0) is None)


# ---------------------------------------------------------------------------
# Realized vol — historical
# ---------------------------------------------------------------------------
print("\n=== Realized vol (historical) ===")

# Constant prices -> 0 returns -> stdev = 0
check("constant prices -> 0 sigma",
      approx(compute_realized_vol([100.0] * 21, 20, "historical"), 0.0))

# Closes that produce known log returns. Take 5 returns alternating ±r.
# Build closes such that log(c_i / c_{i-1}) alternates between +0.01 and -0.01.
# Start at 100, multiply by exp(+0.01), exp(-0.01), ... five times.
import math as _m
seq = [100.0]
expected_rets = [0.01, -0.01, 0.01, -0.01, 0.01]
for r in expected_rets:
    seq.append(seq[-1] * _m.exp(r))
# Window=5 over these 6 closes.
# Sample stdev of [0.01, -0.01, 0.01, -0.01, 0.01]
expected = statistics.stdev(expected_rets)
got = compute_realized_vol(seq, 5, "historical")
check(f"alternating returns -> historical sigma matches statistics.stdev "
      f"(expected={expected:.8f}, got={got:.8f})",
      approx(got, expected, tol=1e-9))

# Insufficient history
check("insufficient -> None", compute_realized_vol([100.0, 101.0], 5, "historical") is None)


# ---------------------------------------------------------------------------
# Realized vol — EWMA
# ---------------------------------------------------------------------------
print("\n=== Realized vol (EWMA) ===")

# Constant prices -> 0 sigma (variance starts 0, stays 0)
check("constant prices -> 0 EWMA sigma",
      approx(compute_realized_vol([100.0] * 21, 20, "ewma"), 0.0))

# Hand-trace: returns = [+0.01, -0.01, +0.01, -0.01, +0.01], window=5.
# Seed var = pvariance of returns (population variance).
# Then for each return, var = 0.94*var + 0.06*r^2.
# Final sigma = sqrt(var).
rets = [0.01, -0.01, 0.01, -0.01, 0.01]
var = statistics.pvariance(rets)
for r in rets:
    var = EWMA_LAMBDA * var + (1.0 - EWMA_LAMBDA) * (r * r)
expected_ewma = _m.sqrt(var)
got_ewma = compute_realized_vol(seq, 5, "ewma")
check(f"hand-traced EWMA matches "
      f"(expected={expected_ewma:.8f}, got={got_ewma:.8f})",
      approx(got_ewma, expected_ewma, tol=1e-9))


# ---------------------------------------------------------------------------
# compute_stop_pricing — unified record shape + dispatch + abort
# ---------------------------------------------------------------------------
print("\n=== compute_stop_pricing dispatch (unified shape) ===")

# Legacy stop -> stop_price is None (engine uses dynamic check), but a unified
# record IS produced for the FE.
out = compute_stop_pricing(
    strategy_config={"stop_loss": {"type": "drawdown_from_entry", "value": -25,
                                    "cooldown_days": 60}},
    symbol="X", entry_date="2024-06-15", entry_price=100.0, ohlc_fetcher=None,
)
check("legacy drawdown_from_entry: stop_price is None (dynamic)",
      out["stop_price"] is None and not out["abort"])
sr = out["stop_record"]
check("legacy stop_record has unified shape (type/params/evidence/summary)",
      isinstance(sr, dict)
      and sr.get("type") == "drawdown_from_entry"
      and sr.get("params", {}).get("value") == -25
      and sr.get("evidence") == {}
      and "Stop" in sr.get("summary", ""))

# Legacy take_profit
out = compute_stop_pricing(
    strategy_config={"take_profit": {"type": "gain_from_entry", "value": 60}},
    symbol="X", entry_date="2024-06-15", entry_price=100.0, ohlc_fetcher=None,
)
tr = out["tp_record"]
check("legacy gain_from_entry tp_record has summary",
      isinstance(tr, dict) and tr.get("type") == "gain_from_entry"
      and "TP" in tr.get("summary", ""))

# Unset config -> both records None, no abort.
out = compute_stop_pricing(
    strategy_config={}, symbol="X", entry_date="2024-06-15",
    entry_price=100.0, ohlc_fetcher=None,
)
check("unset config: no records, no abort",
      out["stop_record"] is None and out["tp_record"] is None and not out["abort"])

# ATR mode with synthetic fetcher.
def fetcher_const(symbol, date, n):
    return [(102.0, 100.0, 101.0)] * n  # ATR=2

out = compute_stop_pricing(
    strategy_config={
        "stop_loss": {"type": "atr_multiple", "k": 2.0, "window_days": 20},
        "take_profit": {"type": "atr_multiple", "k": 4.0, "window_days": 20},
    },
    symbol="X", entry_date="2024-06-15", entry_price=100.0, ohlc_fetcher=fetcher_const,
)
check("ATR stop frozen at 96.0", approx(out["stop_price"], 96.0))
check("ATR tp frozen at 108.0", approx(out["take_profit_price"], 108.0))
sr = out["stop_record"]
check("ATR stop_record.type = atr_multiple",
      sr and sr["type"] == "atr_multiple" and sr["params"]["k"] == 2.0
      and sr["evidence"]["atr"] == 2.0 and approx(sr["evidence"]["frozen_price"], 96.0))
check("ATR stop_record.summary mentions ATR + frozen $",
      "ATR" in sr["summary"] and "$96.00" in sr["summary"])

# Insufficient bars -> abort.
def fetcher_short(symbol, date, n):
    return [(102.0, 100.0, 101.0)] * 3

out = compute_stop_pricing(
    strategy_config={"stop_loss": {"type": "atr_multiple", "k": 2.0, "window_days": 20}},
    symbol="X", entry_date="2024-06-15", entry_price=100.0, ohlc_fetcher=fetcher_short,
)
check("insufficient OHLC -> abort=True",
      out["abort"] and out["stop_price"] is None)

# Realized-vol with constant prices -> sigma=0 -> abort.
out = compute_stop_pricing(
    strategy_config={"stop_loss": {"type": "realized_vol_multiple", "k": 2.0,
                                    "window_days": 20, "sigma_source": "historical"}},
    symbol="X", entry_date="2024-06-15", entry_price=100.0, ohlc_fetcher=fetcher_const,
)
check("realized_vol with sigma=0 -> abort=True", out["abort"])

# Realized-vol with alternating returns.
def fetcher_alternating(symbol, date, n):
    s = [100.0]
    sign = 1
    for _ in range(n - 1):
        s.append(s[-1] * _m.exp(0.01 * sign))
        sign *= -1
    return [(c * 1.005, c * 0.995, c) for c in s]

out = compute_stop_pricing(
    strategy_config={"stop_loss": {"type": "realized_vol_multiple", "k": 2.0,
                                    "window_days": 5, "sigma_source": "historical"}},
    symbol="X", entry_date="2024-06-15", entry_price=100.0, ohlc_fetcher=fetcher_alternating,
)
sigma = compute_realized_vol(
    [c for _, _, c in fetcher_alternating("X", "2024-06-15", 6)], 5, "historical",
)
expected_stop = 100.0 * (1 - 2.0 * sigma)
check(f"realized_vol stop ≈ entry*(1-k*sigma) "
      f"(expected={expected_stop:.4f}, got={out['stop_price']:.4f})",
      approx(out["stop_price"], expected_stop, tol=1e-6))
sr = out["stop_record"]
check("realized_vol stop_record.evidence.sigma matches",
      sr and approx(sr["evidence"]["sigma"], sigma, tol=1e-6))
check("realized_vol summary mentions σ + sigma_source",
      sr and "σ" in sr["summary"] and "historical" in sr["summary"])


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 60}")
print(f"Passed: {PASS}, Failed: {FAIL}")
sys.exit(0 if FAIL == 0 else 1)
