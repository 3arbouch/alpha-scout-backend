#!/usr/bin/env python3
"""
Unit test for get_experiment_stats / get_experiment_trades window filter.

Builds a synthetic trades table:
  - 4 training-period trades (window_label NULL)
  - 6 eval-window trades split across 2 windows (window_label set)

Then calls the tools with various window filters and verifies row counts
and the available_windows discovery.

Run: python3 tests/test_tools_window_filter_unit.py
"""
import asyncio
import json
import os
import sys
import tempfile

TMP_DB = tempfile.NamedTemporaryFile(suffix="_toolswin.db", delete=False)
TMP_DB.close()
os.environ["APP_DB_PATH"] = TMP_DB.name

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "auto_trader"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

# Force reload of auto_trader.schema with the temp DB path.
import importlib
import auto_trader.schema as _aschema  # noqa: E402
importlib.reload(_aschema)

from auto_trader.schema import get_db  # noqa: E402
from deploy_engine import persist_trades  # noqa: E402

# Set _RUN_ID to None so the tools don't add the run_id sub-query scope.
import auto_trader.tools as tools_mod  # noqa: E402
tools_mod._RUN_ID = None

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


# Ensure tables exist.
get_db().close()

EXP_ID = "tooltest123"


# ---- Insert training-period trades (4 trades: 2 BUYs + 2 SELLs) ----
training_trades = [
    {"date": "2020-01-02", "action": "BUY",  "symbol": "AAPL", "shares": 10, "price": 100, "amount": 1000},
    {"date": "2020-06-02", "action": "SELL", "symbol": "AAPL", "shares": 10, "price": 130, "amount": 1300,
     "pnl": 300, "pnl_pct": 30.0, "entry_date": "2020-01-02", "entry_price": 100, "days_held": 152,
     "reason": "take_profit"},
    {"date": "2020-07-01", "action": "BUY",  "symbol": "MSFT", "shares": 5, "price": 200, "amount": 1000},
    {"date": "2020-12-15", "action": "SELL", "symbol": "MSFT", "shares": 5, "price": 180, "amount": 900,
     "pnl": -100, "pnl_pct": -10.0, "entry_date": "2020-07-01", "entry_price": 200, "days_held": 167,
     "reason": "stop_loss"},
]
persist_trades("experiment", EXP_ID, training_trades, sleeve_label="Core", window_label=None)


# ---- Insert window A trades (2 SELLs round-trip) ----
window_a = [
    {"date": "2022-02-02", "action": "BUY",  "symbol": "AAPL", "shares": 10, "price": 150, "amount": 1500},
    {"date": "2022-08-02", "action": "SELL", "symbol": "AAPL", "shares": 10, "price": 170, "amount": 1700,
     "pnl": 200, "pnl_pct": 13.33, "entry_date": "2022-02-02", "entry_price": 150, "days_held": 181,
     "reason": "take_profit"},
]
persist_trades("experiment", EXP_ID, window_a, sleeve_label="Core", window_label="2022-01-01_2023-01-01")


# ---- Insert window B trades (different result) ----
window_b = [
    {"date": "2023-02-02", "action": "BUY",  "symbol": "GOOGL", "shares": 5, "price": 100, "amount": 500},
    {"date": "2023-09-15", "action": "SELL", "symbol": "GOOGL", "shares": 5, "price": 80, "amount": 400,
     "pnl": -100, "pnl_pct": -20.0, "entry_date": "2023-02-02", "entry_price": 100, "days_held": 225,
     "reason": "stop_loss"},
    {"date": "2023-03-01", "action": "BUY",  "symbol": "NVDA", "shares": 5, "price": 200, "amount": 1000},
    {"date": "2023-12-15", "action": "SELL", "symbol": "NVDA", "shares": 5, "price": 350, "amount": 1750,
     "pnl": 750, "pnl_pct": 75.0, "entry_date": "2023-03-01", "entry_price": 200, "days_held": 289,
     "reason": "time_stop"},
]
persist_trades("experiment", EXP_ID, window_b, sleeve_label="Core", window_label="2023-01-01_2024-01-01")


# ---- Row counts directly via SQL (sanity baseline) ----
conn = get_db()
sql_count = lambda where, params: conn.execute(
    f"SELECT COUNT(*) FROM trades WHERE source_type='experiment' AND source_id='{EXP_ID}' AND {where}",
    params,
).fetchone()[0]

n_all = conn.execute(
    f"SELECT COUNT(*) FROM trades WHERE source_type='experiment' AND source_id='{EXP_ID}'"
).fetchone()[0]
n_training = sql_count("window_label IS NULL", [])
n_wa = sql_count("window_label = ?", ["2022-01-01_2023-01-01"])
n_wb = sql_count("window_label = ?", ["2023-01-01_2024-01-01"])
print(f"SQL counts — all={n_all}, training={n_training}, win_a={n_wa}, win_b={n_wb}")
check("4 training trades inserted",       n_training == 4)
check("2 window-A trades inserted",       n_wa == 2)
check("4 window-B trades inserted",       n_wb == 4)
check("total = 10",                       n_all == 10)
conn.close()


def call_stats(window=None):
    handler = tools_mod.get_experiment_stats_tool.handler
    args = {"experiment_id": EXP_ID}
    if window:
        args["window"] = window
    res = asyncio.get_event_loop().run_until_complete(handler(args))
    return json.loads(res["content"][0]["text"])


def call_trades(window=None):
    handler = tools_mod.get_experiment_trades_tool.handler
    args = {"experiment_id": EXP_ID}
    if window:
        args["window"] = window
    res = asyncio.get_event_loop().run_until_complete(handler(args))
    return json.loads(res["content"][0]["text"])


# ---- get_experiment_stats: no filter ----
print("\nget_experiment_stats (no window filter):")
all_stats = call_stats()
check("available_windows discovered (sorted)",
      all_stats["available_windows"] == ["2022-01-01_2023-01-01", "2023-01-01_2024-01-01"],
      f"got {all_stats['available_windows']}")
check("filtered_by_window is null", all_stats["filtered_by_window"] is None)
check("totals total_trades = 10",   all_stats["totals"]["total_trades"] == 10)
check("totals closed_sells = 5",    all_stats["totals"]["closed_sells"] == 5)


# ---- get_experiment_stats: window='training' ----
print("\nget_experiment_stats (window=training):")
ts = call_stats(window="training")
check("training stats trade count = 4", ts["totals"]["total_trades"] == 4)
check("training stats closed = 2",       ts["totals"]["closed_sells"] == 2)
check("training filtered_by_window echo", ts["filtered_by_window"] == "training")


# ---- get_experiment_stats: window='2023-01-01_2024-01-01' ----
print("\nget_experiment_stats (window=2023-01-01_2024-01-01):")
wb = call_stats(window="2023-01-01_2024-01-01")
check("window-B stats trade count = 4", wb["totals"]["total_trades"] == 4)
check("window-B stats closed = 2",      wb["totals"]["closed_sells"] == 2)


# ---- get_experiment_trades: no filter (all) ----
print("\nget_experiment_trades (no window filter):")
at = call_trades()
check("all trades count = 10", at["trade_count"] == 10)


# ---- get_experiment_trades: window='training' ----
print("\nget_experiment_trades (window=training):")
tt = call_trades(window="training")
check("training trades count = 4", tt["trade_count"] == 4)


# ---- get_experiment_trades: window='2022-01-01_2023-01-01' ----
print("\nget_experiment_trades (window A):")
wa = call_trades(window="2022-01-01_2023-01-01")
check("window-A trades count = 2", wa["trade_count"] == 2)
check("window-A only contains AAPL",
      all(t["symbol"] == "AAPL" for t in wa["trades"]))


# ---- Sum invariant: training + win_a + win_b == total ----
print("\nInvariants:")
training_n = call_trades(window="training")["trade_count"]
win_a_n    = call_trades(window="2022-01-01_2023-01-01")["trade_count"]
win_b_n    = call_trades(window="2023-01-01_2024-01-01")["trade_count"]
total_n    = call_trades()["trade_count"]
check("training + window_a + window_b == total",
      training_n + win_a_n + win_b_n == total_n,
      f"{training_n}+{win_a_n}+{win_b_n} vs {total_n}")


os.unlink(TMP_DB.name)
print(f"\n{'=' * 50}\n{PASS} passed, {FAIL} failed\n{'=' * 50}")
sys.exit(0 if FAIL == 0 else 1)
