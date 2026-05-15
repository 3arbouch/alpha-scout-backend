#!/usr/bin/env python3
"""
Backtest ↔ deploy parity test (Gap 13).

Both backtest and deploy go through portfolio_engine.run_portfolio_backtest.
The only behavioral difference is `force_close_at_end`:

  • Backtest (True)  — closes all open positions on the last day. The close
                       happens AFTER the final record_nav call, so the
                       recorded NAV history does NOT include the close-out.
  • Deploy (False)   — leaves positions open; same NAV history.

Stronger parity than naively expected: nav_history is byte-identical in
both modes. The close-out only affects portfolio.cash / .positions, neither
of which feeds the NAV recorder. So metrics computed from nav_history
(total_return, Sharpe, max_drawdown) are also identical.

Verified:
  1. nav_history is byte-identical between modes
  2. Pre-final-day trades are byte-identical
  3. Final-day backtest trades are all SELLs with reason='backtest_end'
  4. Final NAV matches (identical, not just ≤)
  5. Reported metrics (total_return, Sharpe, max_drawdown) match
  6. Open positions in deploy match the closed-out positions in backtest
  7. Combined (portfolio-level) NAV matches across modes

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    python3 test_backtest_deploy_parity_e2e.py
"""
import copy
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine import run_portfolio_backtest

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


def trade_sig(t):
    """Normalized trade signature (excludes signal_detail which has timestamps etc.)."""
    return (
        t["date"], t["symbol"], t["action"], t.get("reason"),
        round(float(t.get("price", 0)), 4),
        round(float(t.get("shares", 0)), 6),
    )


# ---------------------------------------------------------------------------
# Config — small, fast, deterministic
# ---------------------------------------------------------------------------
STRATEGY = {
    "name": "Parity",
    "universe": {"type": "symbols",
                  "symbols": ["AAPL", "MSFT", "NVDA", "JNJ", "PG", "KO", "XOM", "CVX", "COP"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 5, "initial_allocation": 200000},
    "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 5},
    "rebalancing": {"frequency": "monthly", "mode": "equal_weight"},
    "backtest": {"start": "2024-01-01", "end": "2024-09-30",
                 "entry_price": "next_close", "slippage_bps": 10},
}
PORTFOLIO = {
    "name": "ParityPortfolio",
    "sleeves": [{"strategy_config": STRATEGY, "weight": 1.0,
                  "regime_gate": ["*"], "label": "Main"}],
    "regime_filter": False,
    "capital_flow": "to_cash",
    "backtest": {"start": "2024-01-01", "end": "2024-09-30",
                 "initial_capital": 200000, "slippage_bps": 10},
}


print("\nRunning BACKTEST mode (force_close_at_end=True)...")
r_bt = run_portfolio_backtest(copy.deepcopy(PORTFOLIO), force_close_at_end=True)
print("Running DEPLOY mode (force_close_at_end=False)...")
r_dp = run_portfolio_backtest(copy.deepcopy(PORTFOLIO), force_close_at_end=False)

# Extract per-sleeve raw results (where the trade ledger and nav_history live)
sleeve_bt = r_bt["sleeve_results"][0]
sleeve_dp = r_dp["sleeve_results"][0]
trades_bt = sleeve_bt["trades"]
trades_dp = sleeve_dp["trades"]
nav_bt = sleeve_bt["nav_history"]
nav_dp = sleeve_dp["nav_history"]


# ---------------------------------------------------------------------------
# 1. nav_history is byte-identical between modes (including final day)
# ---------------------------------------------------------------------------
print("\n=== 1. nav_history is identical between modes ===")

last_date = nav_bt[-1]["date"]
nav_bt_all = [(n["date"], n["nav"]) for n in nav_bt]
nav_dp_all = [(n["date"], n["nav"]) for n in nav_dp]

check("nav_history entry count matches",
      len(nav_bt) == len(nav_dp),
      f"bt={len(nav_bt)} dp={len(nav_dp)}")
check("entire NAV path is byte-equal (force-close happens AFTER record_nav)",
      nav_bt_all == nav_dp_all,
      f"first diff: {next(((a,b) for a,b in zip(nav_bt_all, nav_dp_all) if a != b), None)}")


# ---------------------------------------------------------------------------
# 2. Pre-final-day trade ledger is byte-identical
# ---------------------------------------------------------------------------
print("\n=== 2. Pre-final-day trades are identical ===")

trades_bt_pre = [trade_sig(t) for t in trades_bt if t["date"] < last_date]
trades_dp_all = [trade_sig(t) for t in trades_dp]  # deploy has no final-day forced sells

check("pre-final-day backtest trades equal all deploy trades",
      trades_bt_pre == trades_dp_all,
      f"bt_pre={len(trades_bt_pre)} dp={len(trades_dp_all)}")


# ---------------------------------------------------------------------------
# 3. Final-day backtest trades are all SELLs with reason='backtest_end'
# ---------------------------------------------------------------------------
print("\n=== 3. Backtest final-day = close-out SELLs only ===")

final_bt_trades = [t for t in trades_bt if t["date"] == last_date]
final_dp_trades = [t for t in trades_dp if t["date"] == last_date]

# Backtest's final-day trades should be the force-close. If there were already
# stops/TPs firing on the last day, those count too — but the engine doesn't
# emit BUYs after the final-day exit checks. So we expect all SELLs.
check("backtest has final-day trades (close-outs)", len(final_bt_trades) > 0,
      "no final-day trades — was the portfolio empty?")
check("all backtest final-day trades are SELLs",
      all(t["action"] == "SELL" for t in final_bt_trades),
      f"actions: {[t['action'] for t in final_bt_trades]}")

backtest_end_sells = [t for t in final_bt_trades if t.get("reason") == "backtest_end"]
check("at least one trade has reason='backtest_end'",
      len(backtest_end_sells) > 0)

# Deploy may have organic exits on the last day (stops/TPs); just verify NO
# backtest_end trades sneak in
check("deploy has zero 'backtest_end' trades",
      not any(t.get("reason") == "backtest_end" for t in trades_dp),
      f"unexpected backtest_end trades in deploy: "
      f"{[t for t in trades_dp if t.get('reason')=='backtest_end']}")


# ---------------------------------------------------------------------------
# 4. Final NAV is identical (force-close happens after record_nav)
# ---------------------------------------------------------------------------
print("\n=== 4. Final NAV is identical ===")

nav_final_bt = nav_bt[-1]["nav"]
nav_final_dp = nav_dp[-1]["nav"]
print(f"  backtest final NAV: ${nav_final_bt:,.2f}")
print(f"  deploy   final NAV: ${nav_final_dp:,.2f}")

check("final NAV is byte-equal across modes",
      nav_final_bt == nav_final_dp,
      f"bt={nav_final_bt} dp={nav_final_dp}")


# ---------------------------------------------------------------------------
# 5. Top-level metrics (total_return, Sharpe, max_drawdown) match
# ---------------------------------------------------------------------------
print("\n=== 5. Reported metrics match across modes ===")

m_bt = r_bt.get("metrics", {})
m_dp = r_dp.get("metrics", {})
for key in ("final_nav", "total_return_pct", "annualized_return_pct",
            "sharpe_ratio", "max_drawdown_pct"):
    bt_v = m_bt.get(key)
    dp_v = m_dp.get(key)
    check(f"metrics.{key}: bt={bt_v} == dp={dp_v}",
          bt_v == dp_v,
          f"differ: bt={bt_v} dp={dp_v}")


# ---------------------------------------------------------------------------
# 6. Deploy's open positions == symbols force-closed in backtest's final day
# ---------------------------------------------------------------------------
print("\n=== 6. Deploy open positions = backtest's force-closed symbols ===")

deploy_open_symbols = {p["symbol"] for p in sleeve_dp.get("open_positions", [])}
backtest_closeout_symbols = {t["symbol"] for t in backtest_end_sells}
check("symbol sets match",
      deploy_open_symbols == backtest_closeout_symbols,
      f"deploy={deploy_open_symbols} backtest_closeouts={backtest_closeout_symbols}")


# ---------------------------------------------------------------------------
# 7. Combined-NAV path identical pre-final-day (portfolio-level check)
# ---------------------------------------------------------------------------
print("\n=== 7. Combined NAV (portfolio-level) is identical pre-final-day ===")

cnav_bt = r_bt.get("combined_nav_history", [])
cnav_dp = r_dp.get("combined_nav_history", [])
cnav_bt_pre = [(n["date"], n["nav"]) for n in cnav_bt if n["date"] < last_date]
cnav_dp_pre = [(n["date"], n["nav"]) for n in cnav_dp if n["date"] < last_date]
check("pre-final-day combined_nav_history is byte-equal",
      cnav_bt_pre == cnav_dp_pre,
      f"first diff: {next(((a,b) for a,b in zip(cnav_bt_pre, cnav_dp_pre) if a != b), None)}")


print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
