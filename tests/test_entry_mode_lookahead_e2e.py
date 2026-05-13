#!/usr/bin/env python3
"""
Entry-mode regression: next_open must fill at open[D+1], not close[D].

Originally surfaced a same-bar-lookahead bug: entry_mode="next_open" filled
at close[D] on the signal date D (because price_index loaded only close, and
the engine took the else-branch of `if entry_mode == "next_close"` to fill
immediately). Fix: both modes queue for D+1; next_close fills at close[D+1],
next_open fills at open[D+1].

This test now asserts the FIXED behavior:
  • next_close → BUY on D+1 at close[D+1]
  • next_open  → BUY on D+1 at open[D+1]
  • For both, signal_date (D) precedes entry_date (D+1).

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_entry_mode_lookahead_e2e.py
"""
import copy
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from backtest_engine import run_backtest

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


# Tiny strategy: always-fire entry → every gating day produces a signal.
# Use only a few symbols, short window, no exits, no rebalancing.
STRATEGY_BASE = {
    "name": "Lookahead probe",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "NVDA"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 300000},
    "rebalancing": {"frequency": "none", "rules": {}},
    "backtest": {
        "start": "2024-01-15", "end": "2024-02-15",
        "entry_price": "next_close",
        "slippage_bps": 0,  # zero slippage so price comparisons are exact
    },
}


# Run twice — once with next_close, once with next_open
cfg_close = copy.deepcopy(STRATEGY_BASE)
cfg_close["backtest"]["entry_price"] = "next_close"

cfg_open = copy.deepcopy(STRATEGY_BASE)
cfg_open["backtest"]["entry_price"] = "next_open"

print("\n=== Running next_close mode ===")
r_close = run_backtest(cfg_close)

print("\n=== Running next_open mode ===")
r_open = run_backtest(cfg_open)


# ---------------------------------------------------------------------------
# Compare entry dates and prices for the first BUY of each symbol
# ---------------------------------------------------------------------------
def first_buy(trades, symbol):
    for t in trades:
        if t["symbol"] == symbol and t["action"] == "BUY":
            return t
    return None


# Load close prices for the test window from market.db so we can verify what
# "close_D" actually was on the signal day.
market_db = os.environ.get("MARKET_DB_PATH", "/home/mohamed/alpha-scout-backend/data/market.db")
conn = sqlite3.connect(market_db)
closes_aapl = dict(conn.execute(
    "SELECT date, close FROM prices WHERE symbol='AAPL' AND date BETWEEN '2024-01-15' AND '2024-02-15' ORDER BY date"
).fetchall())
opens_aapl = dict(conn.execute(
    "SELECT date, open FROM prices WHERE symbol='AAPL' AND date BETWEEN '2024-01-15' AND '2024-02-15' ORDER BY date"
).fetchall())
conn.close()

trading_dates_aapl = sorted(closes_aapl.keys())
first_trade_date = trading_dates_aapl[0]  # first day the always-signal can fire
second_trade_date = trading_dates_aapl[1]


print("\n=== 1. Entry-date alignment ===")
buy_close = first_buy(r_close["trades"], "AAPL")
buy_open = first_buy(r_open["trades"], "AAPL")
print(f"  next_close mode → AAPL first BUY on {buy_close['date']} at ${buy_close['price']}")
print(f"  next_open  mode → AAPL first BUY on {buy_open['date']}  at ${buy_open['price']}")
print(f"  First signal day in data: {first_trade_date}, second day: {second_trade_date}")
print(f"  close[{first_trade_date}]={closes_aapl[first_trade_date]} "
      f"close[{second_trade_date}]={closes_aapl[second_trade_date]} "
      f"open[{second_trade_date}]={opens_aapl[second_trade_date]}")


# ---------------------------------------------------------------------------
# 2. Both modes now queue for D+1 — verify entry dates and fill prices.
# ---------------------------------------------------------------------------
print("\n=== 2. Fill-price regression test ===")

check("next_close BUY date is D+1 (queued correctly)",
      buy_close["date"] == second_trade_date,
      f"got {buy_close['date']} expected {second_trade_date}")

check("next_close BUY price equals close[D+1]",
      abs(buy_close["price"] - closes_aapl[second_trade_date]) < 0.01,
      f"got {buy_close['price']} expected close[{second_trade_date}]={closes_aapl[second_trade_date]}")

check("next_open BUY date is D+1 (queued, NOT same-bar)",
      buy_open["date"] == second_trade_date,
      f"got {buy_open['date']} expected {second_trade_date} "
      f"(if it's {first_trade_date} the same-bar bug is back)")

check("next_open BUY price equals open[D+1]",
      abs(buy_open["price"] - opens_aapl[second_trade_date]) < 0.01,
      f"got {buy_open['price']} expected open[{second_trade_date}]={opens_aapl[second_trade_date]}")

check("next_open BUY price is NOT close[D] (lookahead guard)",
      abs(buy_open["price"] - closes_aapl[first_trade_date]) > 0.01,
      f"BUY price {buy_open['price']} matches close[D]={closes_aapl[first_trade_date]} — lookahead bug present!")


# ---------------------------------------------------------------------------
# 3. Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
