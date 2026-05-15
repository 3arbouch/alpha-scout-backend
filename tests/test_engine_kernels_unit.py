#!/usr/bin/env python3
"""
Unit tests for engine math kernels — pure-function checks on the boundaries
that drive trade decisions. Run:

    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_engine_kernels_unit.py

Covers:
    1. Position math (pnl_pct, days_held, market_value)
    2. Exit checks (check_stop_loss, check_take_profit, check_time_stop)
    3. is_rebalance_date
    4. combine_signals (logic="all" / "any")
    5. risk_parity sizing math (spec test — formula replica)
"""
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from backtest_engine import (
    Position,
    check_stop_loss,
    check_take_profit,
    check_time_stop,
    combine_signals,
    is_rebalance_date,
)
from stop_pricing import compute_realized_vol

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


def approx(a, b, tol=1e-9):
    if a is None or b is None:
        return False
    return abs(a - b) < tol


# ---------------------------------------------------------------------------
# 1. Position math
# ---------------------------------------------------------------------------
print("\n=== Position.pnl_pct ===")

pos = Position("AAA", "2024-01-01", entry_price=100.0, shares=10)
check("flat (price == entry) → 0%", approx(pos.pnl_pct(100.0), 0.0))
check("+10% gain", approx(pos.pnl_pct(110.0), 10.0))
check("-10% loss", approx(pos.pnl_pct(90.0), -10.0))
check("price → 2x → +100%", approx(pos.pnl_pct(200.0), 100.0))
check("price → 0 → -100%", approx(pos.pnl_pct(0.0), -100.0))

# Zero-entry guard
zero_pos = Position("Z", "2024-01-01", entry_price=0.0, shares=10)
check("entry_price=0 → pnl_pct returns 0 (guard)", zero_pos.pnl_pct(50.0) == 0)
neg_pos = Position("N", "2024-01-01", entry_price=-1.0, shares=10)
check("entry_price<0 → pnl_pct returns 0 (guard)", neg_pos.pnl_pct(50.0) == 0)

print("\n=== Position.market_value ===")
check("100 shares @ $5 = $500", approx(Position("X", "2024-01-01", 4.5, 100).market_value(5.0), 500.0))
check("zero shares = $0", approx(Position("X", "2024-01-01", 100, 0).market_value(50.0), 0.0))

print("\n=== Position.days_held ===")
p = Position("X", "2024-01-01", 100, 10)
check("same date → 0 days", p.days_held("2024-01-01") == 0)
check("next day → 1 day", p.days_held("2024-01-02") == 1)
check("after 30 days", p.days_held("2024-01-31") == 30)
check("calendar (not trading) days — spans weekend", p.days_held("2024-01-08") == 7)
# Year boundary
p2 = Position("X", "2023-12-30", 100, 10)
check("year boundary 2023-12-30 → 2024-01-02 = 3 days", p2.days_held("2024-01-02") == 3)
# Leap year (2024 is a leap year)
p3 = Position("X", "2024-02-28", 100, 10)
check("leap-year span Feb 28 → Mar 1 = 2 days", p3.days_held("2024-03-01") == 2)


# ---------------------------------------------------------------------------
# 2. Exit checks
# ---------------------------------------------------------------------------
print("\n=== check_stop_loss: no config → False ===")
pos = Position("A", "2024-01-01", entry_price=100, shares=10)
check("no stop_loss key → False", check_stop_loss(pos, 50, {}) is False)
check("stop_loss=None → False", check_stop_loss(pos, 50, {"stop_loss": None}) is False)

print("\n=== check_stop_loss: drawdown_from_entry ===")
cfg = {"stop_loss": {"type": "drawdown_from_entry", "value": -10}}
# threshold is -10%. pnl <= -10 triggers.
check("price=91 (pnl=-9%) → NOT triggered (above threshold)",
      check_stop_loss(pos, 91.0, cfg) is False)
check("price=90 (pnl=-10%) → triggered (== threshold, <=)",
      check_stop_loss(pos, 90.0, cfg) is True)
check("price=89 (pnl=-11%) → triggered (below threshold)",
      check_stop_loss(pos, 89.0, cfg) is True)
check("price=120 (positive pnl) → NOT triggered",
      check_stop_loss(pos, 120.0, cfg) is False)

print("\n=== check_stop_loss: vol-adaptive (atr_multiple / realized_vol_multiple) ===")
# Frozen stop_price = 92
frozen_pos = Position("A", "2024-01-01", 100, 10, stop_price=92.0)
cfg_atr = {"stop_loss": {"type": "atr_multiple", "multiple": 2}}
check("vol-adaptive: price=93 (above stop) → NOT triggered",
      check_stop_loss(frozen_pos, 93.0, cfg_atr) is False)
check("vol-adaptive: price=92 (== stop) → triggered (<=)",
      check_stop_loss(frozen_pos, 92.0, cfg_atr) is True)
check("vol-adaptive: price=91 (below stop) → triggered",
      check_stop_loss(frozen_pos, 91.0, cfg_atr) is True)

cfg_rv = {"stop_loss": {"type": "realized_vol_multiple", "multiple": 2}}
check("vol-adaptive realized_vol: price=91 → triggered",
      check_stop_loss(frozen_pos, 91.0, cfg_rv) is True)

# Missing stop_price → must not trigger
no_stop = Position("A", "2024-01-01", 100, 10)  # stop_price defaults to None
check("vol-adaptive with no frozen stop_price → False (safe abort)",
      check_stop_loss(no_stop, 50.0, cfg_atr) is False)


print("\n=== check_take_profit: no config / gain_from_entry ===")
check("no take_profit key → False", check_take_profit(pos, 200, {}) is False)

cfg_tp = {"take_profit": {"type": "gain_from_entry", "value": 20}}
# threshold +20%, >= triggers
check("price=119 (pnl=+19%) → NOT triggered",
      check_take_profit(pos, 119.0, cfg_tp) is False)
check("price=120 (pnl=+20%) → triggered (>=)",
      check_take_profit(pos, 120.0, cfg_tp) is True)
check("price=121 (pnl=+21%) → triggered",
      check_take_profit(pos, 121.0, cfg_tp) is True)


print("\n=== check_take_profit: above_peak ===")
# peak_price is set via __init__; pnl must exceed peak by tp.value pct
peak_pos = Position("A", "2024-01-01", entry_price=80, shares=10, peak_price=100)
cfg_ap = {"take_profit": {"type": "above_peak", "value": 10}}
# gain from peak = (price - 100) / 100 * 100; >= 10 triggers
check("above_peak: price=109 (9% above peak) → NOT triggered",
      check_take_profit(peak_pos, 109.0, cfg_ap) is False)
check("above_peak: price=110 (10% above peak) → triggered",
      check_take_profit(peak_pos, 110.0, cfg_ap) is True)
check("above_peak: price=111 (11% above peak) → triggered",
      check_take_profit(peak_pos, 111.0, cfg_ap) is True)

# above_peak with peak_price=0 should not trigger (guard)
zero_peak = Position("A", "2024-01-01", 80, 10, peak_price=0.001)  # __init__ replaces 0 w/ entry, so force tiny
# constructor uses peak_price OR entry_price, so peak_price=None / 0 → entry_price.
# Use entry_price=1e-9 to simulate a near-zero peak indirectly is awkward; skip.

print("\n=== check_take_profit: vol-adaptive (frozen tp_price) ===")
frozen_tp_pos = Position("A", "2024-01-01", 100, 10, take_profit_price=130.0)
cfg_tp_atr = {"take_profit": {"type": "atr_multiple", "multiple": 3}}
check("vol-adaptive: price=129 → NOT triggered",
      check_take_profit(frozen_tp_pos, 129.0, cfg_tp_atr) is False)
check("vol-adaptive: price=130 (== tp) → triggered (>=)",
      check_take_profit(frozen_tp_pos, 130.0, cfg_tp_atr) is True)
check("vol-adaptive: price=131 → triggered",
      check_take_profit(frozen_tp_pos, 131.0, cfg_tp_atr) is True)

no_tp = Position("A", "2024-01-01", 100, 10)
check("vol-adaptive tp with no frozen tp_price → False",
      check_take_profit(no_tp, 200.0, cfg_tp_atr) is False)


print("\n=== check_time_stop ===")
cfg_ts = {"time_stop": {"max_days": 30}}
check("no time_stop config → False",
      check_time_stop(pos, "2024-12-31", {}) is False)
# pos.entry_date = 2024-01-01
check("29 days held → NOT triggered (< 30)",
      check_time_stop(pos, "2024-01-30", cfg_ts) is False)
check("30 days held → triggered (>= 30)",
      check_time_stop(pos, "2024-01-31", cfg_ts) is True)
check("31 days held → triggered",
      check_time_stop(pos, "2024-02-01", cfg_ts) is True)


# ---------------------------------------------------------------------------
# 3. is_rebalance_date
# ---------------------------------------------------------------------------
print("\n=== is_rebalance_date ===")
check("frequency=none → False",
      is_rebalance_date("2024-06-01", "2024-01-01", "none") is False)
check("last_rebal=None → False",
      is_rebalance_date("2024-06-01", None, "quarterly") is False)
check("last_rebal=empty → False",
      is_rebalance_date("2024-06-01", "", "quarterly") is False)

# quarterly: 90 days threshold
check("quarterly, 89 days → False",
      is_rebalance_date("2024-04-09", "2024-01-11", "quarterly") is False)
check("quarterly, 90 days → True (>=)",
      is_rebalance_date("2024-04-10", "2024-01-11", "quarterly") is True)
check("quarterly, 100 days → True",
      is_rebalance_date("2024-04-20", "2024-01-11", "quarterly") is True)

# monthly: 30 days threshold
check("monthly, 29 days → False",
      is_rebalance_date("2024-01-30", "2024-01-01", "monthly") is False)
check("monthly, 30 days → True (>=)",
      is_rebalance_date("2024-01-31", "2024-01-01", "monthly") is True)
check("monthly, 31 days → True",
      is_rebalance_date("2024-02-01", "2024-01-01", "monthly") is True)


# ---------------------------------------------------------------------------
# 4. combine_signals
# ---------------------------------------------------------------------------
print("\n=== combine_signals: edge cases ===")
check("empty list → {}", combine_signals([]) == {})

single = {"AAA": {"2024-01-01": {"x": 1}}}
check("single signal set → pass-through (same object)",
      combine_signals([single]) is single)


print("\n=== combine_signals: logic='all' (AND) ===")
s1 = {"AAA": {"2024-01-01": 1, "2024-01-02": 2, "2024-01-03": 3},
      "BBB": {"2024-01-01": 1}}
s2 = {"AAA": {"2024-01-02": 20, "2024-01-03": 30},
      "BBB": {"2024-01-02": 2}}
out = combine_signals([s1, s2], logic="all")
check("AAA dates intersect to {2024-01-02, 2024-01-03}",
      set(out.get("AAA", {}).keys()) == {"2024-01-02", "2024-01-03"})
check("BBB dates: empty intersection → BBB dropped",
      "BBB" not in out)
# Metadata: scalar values become condition_N_value
val = out["AAA"]["2024-01-02"]
check("AAA 2024-01-02 metadata has condition_0_value=2 and condition_1_value=20",
      val.get("condition_0_value") == 2 and val.get("condition_1_value") == 20)


print("\n=== combine_signals: logic='any' (OR) ===")
out_any = combine_signals([s1, s2], logic="any")
check("AAA dates union to {01,02,03}",
      set(out_any.get("AAA", {}).keys()) == {"2024-01-01", "2024-01-02", "2024-01-03"})
check("BBB dates union to {01,02}",
      set(out_any.get("BBB", {}).keys()) == {"2024-01-01", "2024-01-02"})
# AAA 2024-01-01 was only in s1 — should have condition_0_value but not condition_1_value
v1 = out_any["AAA"]["2024-01-01"]
check("AAA 2024-01-01 (only s1) has condition_0_value=1, no condition_1",
      v1.get("condition_0_value") == 1 and "condition_1_value" not in v1)


print("\n=== combine_signals: dict metadata is merged with prefix ===")
s_d1 = {"AAA": {"2024-01-01": {"pe": 12.5, "rsi": 30}}}
s_d2 = {"AAA": {"2024-01-01": {"score": 0.8}}}
out_d = combine_signals([s_d1, s_d2], logic="all")
meta = out_d["AAA"]["2024-01-01"]
check("dict metadata: condition_0_pe=12.5",
      approx(meta.get("condition_0_pe"), 12.5))
check("dict metadata: condition_0_rsi=30",
      meta.get("condition_0_rsi") == 30)
check("dict metadata: condition_1_score=0.8",
      approx(meta.get("condition_1_score"), 0.8))


print("\n=== combine_signals: symbol in only one set (AND drops, OR keeps) ===")
s_only_a = {"AAA": {"2024-01-01": 1}}
s_only_b = {"BBB": {"2024-01-01": 1}}
out_and = combine_signals([s_only_a, s_only_b], logic="all")
check("AND with disjoint symbols → both dropped",
      out_and == {})
out_or = combine_signals([s_only_a, s_only_b], logic="any")
check("OR with disjoint symbols → both kept",
      set(out_or.keys()) == {"AAA", "BBB"})


# ---------------------------------------------------------------------------
# 5. Risk-parity sizing math (spec test — replicates the inline formula at
#    backtest_engine.py:2263-2287). If the impl changes, this test won't fail,
#    but the formula here documents the intended math.
# ---------------------------------------------------------------------------
print("\n=== risk_parity: inverse-vol weighting math ===")

def risk_parity_amounts(sigmas: dict, current_nav: float, max_positions: int) -> dict:
    """Replicates backtest_engine.py:2263-2287 sizing logic."""
    n_batch = len(sigmas)
    pool = (n_batch / max_positions) * current_nav
    inv = {s: 1.0 / sigmas[s] for s in sigmas if sigmas[s] > 0}
    total = sum(inv.values())
    weights = {s: v / total for s, v in inv.items()}
    return {s: pool * w for s, w in weights.items()}


# Equal vols → equal amounts (special case)
sigmas = {"A": 0.02, "B": 0.02, "C": 0.02}
amounts = risk_parity_amounts(sigmas, current_nav=100_000, max_positions=10)
check("equal vols → equal amounts",
      approx(amounts["A"], amounts["B"]) and approx(amounts["B"], amounts["C"]))
# pool = (3/10) * 100k = 30k. Each gets 10k.
check("equal vols: each = pool / n_batch = 10000",
      approx(amounts["A"], 10_000.0, tol=1e-6))

# Inverse-vol: lower vol → bigger weight
sigmas = {"LOW": 0.01, "HIGH": 0.04}  # LOW has 4x the inverse weight
amounts = risk_parity_amounts(sigmas, current_nav=100_000, max_positions=10)
# pool = (2/10)*100k = 20k. weights: LOW = (1/0.01)/(1/0.01 + 1/0.04) = 100/125 = 0.8
# HIGH = 25/125 = 0.2. → LOW gets 16k, HIGH gets 4k.
check("inverse-vol: LOW (sigma=0.01) gets 16000",
      approx(amounts["LOW"], 16_000.0, tol=1e-6))
check("inverse-vol: HIGH (sigma=0.04) gets 4000",
      approx(amounts["HIGH"], 4_000.0, tol=1e-6))
check("inverse-vol: pool conserved (sum == 20000)",
      approx(amounts["LOW"] + amounts["HIGH"], 20_000.0, tol=1e-6))

# 4-name case to confirm weights normalize to 1
sigmas = {"A": 0.01, "B": 0.02, "C": 0.03, "D": 0.04}
amounts = risk_parity_amounts(sigmas, current_nav=100_000, max_positions=10)
# pool = 4/10 * 100k = 40k. inv = [100, 50, 33.333, 25] sum=208.333.
# weights = [0.48, 0.24, 0.16, 0.12]
expected_total = 40_000.0
check("4-name: pool conservation",
      approx(sum(amounts.values()), expected_total, tol=1e-6))
check("4-name: A (lowest vol) has largest weight",
      amounts["A"] > amounts["B"] > amounts["C"] > amounts["D"])
# Hand-verify A's weight: (1/0.01) / (100+50+33.333+25) = 100/208.333 = 0.48
expected_a = 40_000 * (100.0 / (100.0 + 50.0 + 100/3 + 25.0))
check("4-name: A's amount matches hand-computed weight",
      approx(amounts["A"], expected_a, tol=1e-6))


print("\n=== risk_parity: pool sizing aggregate matches equal-weight slot ===")
# Per backtest_engine.py:2310: "Aggregate target for this batch matches
# equal_weight's: n_batch × (current_nav / max_positions)."
sigmas = {"A": 0.02, "B": 0.03}
nav = 50_000
max_pos = 5
amounts = risk_parity_amounts(sigmas, current_nav=nav, max_positions=max_pos)
eq_weight_slot = nav / max_pos  # 10,000
expected_pool = 2 * eq_weight_slot  # 20,000
check("pool == n_batch * (nav / max_positions)",
      approx(sum(amounts.values()), expected_pool, tol=1e-6))


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
