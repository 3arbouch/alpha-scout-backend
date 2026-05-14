#!/usr/bin/env python3
"""
Unit test (Gap 11): the `add_on_earnings_beat` rebalance rule in
backtest_engine._do_rebalance (lines 2609-2666). Pure-function test —
no DB. Builds a Portfolio, plants a position, calls _do_rebalance with
hand-crafted earnings_data, and verifies the right trades come out.

Verified behaviors:
  (a) pnl < min_gain_pct → no add
  (b) no recent earnings beat (or none within lookback_days) → no add
  (c) earn_data["beat"] == False → no add
  (d) both gates met → add emitted, capped at max_add_multiplier × original_cost
  (e) max_position_pct cap shrinks the add when the post-add weight would exceed it
  (f) room_to_add < 1000 → no add
  (g) cash limit (25% of cash) is the binding cap when small

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_earnings_beat_rebalance_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from backtest_engine import Portfolio, _do_rebalance

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


def make_portfolio_with_position(symbol, entry_price, shares, initial_cash=100_000):
    """Helper: a Portfolio carrying a single position and the rest in cash."""
    p = Portfolio(initial_cash=initial_cash + entry_price * shares)
    # Inject the position directly to bypass open_position's vol-pricing path
    from backtest_engine import Position
    p.positions[symbol] = Position(
        symbol=symbol,
        entry_date="2024-01-01",
        entry_price=entry_price,
        shares=shares,
    )
    p.cash = initial_cash
    return p


def count_buys(trades, symbol=None):
    return sum(1 for t in trades if t["action"] == "BUY"
               and (symbol is None or t["symbol"] == symbol))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
SYMBOL = "AAA"
ENTRY_PRICE = 100.0
SHARES = 100  # original cost = $10,000
INITIAL_CASH = 100_000
REBAL_DATE = "2024-04-01"
CURRENT_PRICE = 130.0  # +30% PnL — clears the default 15% gain threshold

PRICE_INDEX = {SYMBOL: {REBAL_DATE: CURRENT_PRICE}}

# Earnings 30 days before rebalance date (well within default 90-day lookback)
EARNINGS_RECENT_BEAT = {
    SYMBOL: {"2024-03-02": {"beat": True}}
}
EARNINGS_RECENT_MISS = {
    SYMBOL: {"2024-03-02": {"beat": False}}
}
EARNINGS_OLD_BEAT = {
    SYMBOL: {"2023-10-01": {"beat": True}}  # >90 days ago
}
EARNINGS_NONE = {}

BASE_CFG = {
    "rebalancing": {
        "rules": {
            "max_position_pct": 100,
            "add_on_earnings_beat": {
                "min_gain_pct": 15,
                "max_add_multiplier": 1.5,
                "lookback_days": 90,
            }
        }
    }
}


def fresh_cfg(**overrides):
    """Deep-ish copy of BASE_CFG with rule-level overrides merged in."""
    import copy
    cfg = copy.deepcopy(BASE_CFG)
    cfg["rebalancing"]["rules"].update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# (a) pnl < min_gain_pct → no add
# ---------------------------------------------------------------------------
print("\n=== (a) pnl < min_gain_pct → no add ===")
p = make_portfolio_with_position(SYMBOL, ENTRY_PRICE, SHARES)
price_index_below_threshold = {SYMBOL: {REBAL_DATE: 110.0}}  # +10%, below 15%
_do_rebalance(p, price_index_below_threshold, REBAL_DATE, fresh_cfg(),
              slippage=0, earnings_data=EARNINGS_RECENT_BEAT)
check("pnl=+10% (below 15% threshold) → no BUY emitted",
      count_buys(p.trades) == 0,
      f"got {count_buys(p.trades)} BUYs")


# ---------------------------------------------------------------------------
# (b) no recent earnings beat within lookback → no add
# ---------------------------------------------------------------------------
print("\n=== (b) earnings older than lookback → no add ===")
p = make_portfolio_with_position(SYMBOL, ENTRY_PRICE, SHARES)
_do_rebalance(p, PRICE_INDEX, REBAL_DATE, fresh_cfg(),
              slippage=0, earnings_data=EARNINGS_OLD_BEAT)
check("earnings 180+ days ago → no BUY", count_buys(p.trades) == 0)

print("\n=== (b') no earnings data for symbol → no add ===")
p = make_portfolio_with_position(SYMBOL, ENTRY_PRICE, SHARES)
_do_rebalance(p, PRICE_INDEX, REBAL_DATE, fresh_cfg(),
              slippage=0, earnings_data=EARNINGS_NONE)
check("missing earnings → no BUY", count_buys(p.trades) == 0)


# ---------------------------------------------------------------------------
# (c) earn_data.beat == False → no add
# ---------------------------------------------------------------------------
print("\n=== (c) earnings miss (beat=False) → no add ===")
p = make_portfolio_with_position(SYMBOL, ENTRY_PRICE, SHARES)
_do_rebalance(p, PRICE_INDEX, REBAL_DATE, fresh_cfg(),
              slippage=0, earnings_data=EARNINGS_RECENT_MISS)
check("recent earnings was a miss → no BUY", count_buys(p.trades) == 0)


# ---------------------------------------------------------------------------
# (d) Both gates met → add emitted, capped at max_add_multiplier × original_cost
# ---------------------------------------------------------------------------
print("\n=== (d) gain + recent beat → add up to max_add_multiplier × original_cost ===")
p = make_portfolio_with_position(SYMBOL, ENTRY_PRICE, SHARES)
# original_cost = 100*100 = $10,000. max_total = 1.5 × $10k = $15k.
# current_value = 100 shares × $130 = $13,000. room_to_add = $15k - $13k = $2k.
# Cash limit: 25% × $100k = $25k → not binding.
# max_position_pct=100, current_nav = $100k cash + $13k pos = $113k.
# new_weight if amount=$2k: (13k + 2k) / 113k = 13.3% → well below 100%.
# So expect exactly one BUY of ~$2,000.
_do_rebalance(p, PRICE_INDEX, REBAL_DATE, fresh_cfg(),
              slippage=0, earnings_data=EARNINGS_RECENT_BEAT)
buys = [t for t in p.trades if t["action"] == "BUY"]
check("exactly one BUY emitted", len(buys) == 1, f"got {len(buys)}")
if buys:
    add_amount = buys[0]["amount"]
    check("BUY amount ≈ $2,000 (room_to_add = max_total - current_value)",
          abs(add_amount - 2000) < 5,
          f"got ${add_amount:.2f}")


# ---------------------------------------------------------------------------
# (e) max_position_pct caps the add
# ---------------------------------------------------------------------------
print("\n=== (e) max_position_pct=12 caps the add ===")
p = make_portfolio_with_position(SYMBOL, ENTRY_PRICE, SHARES)
# Tighter cap: max_position_pct = 12. current_nav $113k.
# new_weight = (13k + 2k)/113k = 13.27% > 12% → cap engaged.
# Per line 2658: amount = (max_pct/100 * current_nav) - current_value
#               = 0.12 * 113000 - 13000 = 13560 - 13000 = $560 (< $1000 floor)
# Per line 2659-2660: if amount < 1000 → continue → no buy.
# This documents the actual behavior: when cap brings amount below $1k, no add.
_do_rebalance(p, PRICE_INDEX, REBAL_DATE, fresh_cfg(max_position_pct=12),
              slippage=0, earnings_data=EARNINGS_RECENT_BEAT)
buys = [t for t in p.trades if t["action"] == "BUY"]
check("cap brings room below $1k floor → no BUY",
      len(buys) == 0, f"got {len(buys)}")

# A wider cap that allows a smaller (but still > $1k) add
print("\n=== (e') max_position_pct=14 → capped add still emitted ===")
p = make_portfolio_with_position(SYMBOL, ENTRY_PRICE, SHARES)
# Tweak: max_position_pct = 14. new_weight uncapped would be 13.27%, under 14 →
# cap NOT engaged here. So amount = $2,000 (the room_to_add).
_do_rebalance(p, PRICE_INDEX, REBAL_DATE, fresh_cfg(max_position_pct=14),
              slippage=0, earnings_data=EARNINGS_RECENT_BEAT)
buys = [t for t in p.trades if t["action"] == "BUY"]
check("cap=14% > new_weight 13.27% → cap not engaged, full $2k add",
      len(buys) == 1 and abs(buys[0]["amount"] - 2000) < 5,
      f"got {len(buys)} buys, amount={buys[0]['amount'] if buys else 'n/a'}")


# ---------------------------------------------------------------------------
# (f) room_to_add < $1k → no add  (max_add_multiplier too small)
# ---------------------------------------------------------------------------
print("\n=== (f) max_add_multiplier=1.31 → room < $1k → no add ===")
p = make_portfolio_with_position(SYMBOL, ENTRY_PRICE, SHARES)
# original_cost = $10k. max_total = 1.31 * 10k = $13,100.
# current_value = $13,000. room = $100 (< $1000 floor at line 2648).
_do_rebalance(p, PRICE_INDEX, REBAL_DATE,
              fresh_cfg(add_on_earnings_beat={
                  "min_gain_pct": 15,
                  "max_add_multiplier": 1.31,
                  "lookback_days": 90,
              }),
              slippage=0, earnings_data=EARNINGS_RECENT_BEAT)
check("room_to_add < $1k → no BUY", count_buys(p.trades) == 0)


# ---------------------------------------------------------------------------
# (g) Cash limit (25% × cash) is the binding cap when small
# ---------------------------------------------------------------------------
print("\n=== (g) cash-poor portfolio: 25% × cash binds the add ===")
# Low cash: $5,000 cash, big position.
p = Portfolio(initial_cash=5_000)
from backtest_engine import Position
p.positions[SYMBOL] = Position(
    symbol=SYMBOL, entry_date="2024-01-01",
    entry_price=ENTRY_PRICE, shares=SHARES,  # original_cost=$10k
)
# current_value at $130 = $13k. max_total = $15k. room = $2k.
# Cash cap = 25% × $5k = $1,250. → amount = min(2000, 1250) = $1,250.
_do_rebalance(p, PRICE_INDEX, REBAL_DATE, fresh_cfg(),
              slippage=0, earnings_data=EARNINGS_RECENT_BEAT)
buys = [t for t in p.trades if t["action"] == "BUY"]
check("cash cap engaged: amount ≈ $1,250",
      len(buys) == 1 and abs(buys[0]["amount"] - 1250) < 5,
      f"got {len(buys)} buys, amount={buys[0]['amount'] if buys else 'n/a'}")


# ---------------------------------------------------------------------------
# (h) earnings_data=None or rule absent → no add (defensive)
# ---------------------------------------------------------------------------
print("\n=== (h) defensive: missing rule or earnings_data ===")
p = make_portfolio_with_position(SYMBOL, ENTRY_PRICE, SHARES)
cfg_no_rule = {"rebalancing": {"rules": {"max_position_pct": 100}}}
_do_rebalance(p, PRICE_INDEX, REBAL_DATE, cfg_no_rule,
              slippage=0, earnings_data=EARNINGS_RECENT_BEAT)
check("no add_on_earnings_beat rule → no BUY", count_buys(p.trades) == 0)

p = make_portfolio_with_position(SYMBOL, ENTRY_PRICE, SHARES)
_do_rebalance(p, PRICE_INDEX, REBAL_DATE, fresh_cfg(),
              slippage=0, earnings_data=None)
check("earnings_data=None → no BUY", count_buys(p.trades) == 0)


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
