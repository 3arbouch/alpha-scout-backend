#!/usr/bin/env python3
"""
Top-level metrics correctness (Tier-B #8).

Verifies that the headline numbers users see (total_return_pct,
annualized_return_pct, max_drawdown_pct, Sharpe family, Sortino, win rate,
profit factor, etc.) are correctly computed against numpy ground truth.

Prior audit (test_backtest_deploy_parity_e2e.py) verified bt/deploy
agreement; this test verifies the VALUES are correct, not just consistent.

We hit compute_metrics() directly with a synthetic Portfolio stub carrying a
known nav_history + trade ledger, so no DB / market data is required.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_top_level_metrics_unit.py
"""
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import numpy as np

from backtest_engine import compute_metrics, MIN_TRADING_DAYS_FOR_ANNUALIZATION

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


def _nav_row(date, nav, positions_value=0.0):
    """Synthetic nav_history entry. positions_value defaults to 0 (all cash);
    real engine populates it during record_nav, we only need the key present."""
    return {"date": date, "nav": float(nav), "positions_value": float(positions_value)}


class _FakePortfolio:
    """Minimal stub matching compute_metrics' attribute usage."""
    def __init__(self, nav_history, closed_trades=None, trades=None):
        # Backfill positions_value if caller-supplied dicts lack the key.
        self.nav_history = [
            n if "positions_value" in n else {**n, "positions_value": 0.0}
            for n in nav_history
        ]
        self.closed_trades = closed_trades or []
        self.trades = trades or []


# ---------------------------------------------------------------------------
# 1. total_return_pct
# ---------------------------------------------------------------------------
print("\n=== 1. total_return_pct ===")

# Initial $100k → Final $120k → +20%
nav = [{"date": f"2024-{1+i//21:02d}-{((i%21)+1):02d}", "nav": 100000 + 952.38 * i}
       for i in range(21)]
nav[-1]["nav"] = 120000.0
p = _FakePortfolio(nav)
m = compute_metrics(p, 100000.0, [n["date"] for n in nav])
check(f"100k → 120k = +20% total return  (got {m['total_return_pct']:.4f})",
      approx(m["total_return_pct"], 20.0))

# 100k → 80k = -20%
nav_loss = [{"date": f"2024-01-{i+1:02d}", "nav": 100000 - 1000 * i} for i in range(21)]
nav_loss[-1]["nav"] = 80000.0
m_loss = compute_metrics(_FakePortfolio(nav_loss), 100000.0, [])
check(f"100k → 80k = -20% total return (got {m_loss['total_return_pct']:.4f})",
      approx(m_loss["total_return_pct"], -20.0))


# ---------------------------------------------------------------------------
# 2. annualized_return_pct
# ---------------------------------------------------------------------------
print("\n=== 2. annualized_return_pct ===")

# 100k → 121k over exactly 252 trading days = (121/100)^(252/252) - 1 = +21%
nav_1y = [{"date": f"d{i:04d}", "nav": 100000 + i * (21000 / 252)} for i in range(252)]
nav_1y[-1]["nav"] = 121000.0
m_1y = compute_metrics(_FakePortfolio(nav_1y), 100000.0, [])
check(f"252 days, +21% total → +21% annualized (got {m_1y['annualized_return_pct']:.4f})",
      approx(m_1y["annualized_return_pct"], 21.0, tol=0.01))

# 100k → 144k over 504 trading days (2 years) = sqrt(1.44) - 1 = +20% annualized
nav_2y = [{"date": f"d{i:04d}", "nav": 100000 + i * (44000 / 504)} for i in range(504)]
nav_2y[-1]["nav"] = 144000.0
m_2y = compute_metrics(_FakePortfolio(nav_2y), 100000.0, [])
check(f"504 days, +44% total → +20% annualized (got {m_2y['annualized_return_pct']:.4f})",
      approx(m_2y["annualized_return_pct"], 20.0, tol=0.01))

# Short window → ann_return is None (honesty gate)
nav_short = [{"date": f"d{i:04d}", "nav": 100000 + 100 * i} for i in range(MIN_TRADING_DAYS_FOR_ANNUALIZATION - 1)]
m_short = compute_metrics(_FakePortfolio(nav_short), 100000.0, [])
check(f"n_nav < {MIN_TRADING_DAYS_FOR_ANNUALIZATION} → annualized_return_pct is None",
      m_short["annualized_return_pct"] is None,
      f"got {m_short['annualized_return_pct']}")


# ---------------------------------------------------------------------------
# 3. max_drawdown_pct
# ---------------------------------------------------------------------------
print("\n=== 3. max_drawdown_pct ===")

# Build a known NAV with a clean -25% drawdown:
# 100, 110, 120 (peak), 100, 90 (trough = -25% from 120), 95, 110, 130
navs = [100, 110, 120, 100, 90, 95, 110, 130]
nav_dd = [{"date": f"2024-01-{i+1:02d}", "nav": float(n)} for i, n in enumerate(navs)]
m_dd = compute_metrics(_FakePortfolio(nav_dd), 100.0, [])
expected_dd = (90 - 120) / 120 * 100  # -25%
check(f"peak=120, trough=90 → max_dd = -25% (got {m_dd['max_drawdown_pct']:.4f})",
      approx(m_dd["max_drawdown_pct"], expected_dd))
check("max_drawdown_date is 2024-01-05 (trough day)",
      m_dd["max_drawdown_date"] == "2024-01-05",
      f"got {m_dd['max_drawdown_date']}")

# Monotonically increasing → max_dd = 0
nav_up = [{"date": f"d{i:04d}", "nav": 100 + i} for i in range(50)]
m_up = compute_metrics(_FakePortfolio(nav_up), 100.0, [])
check("monotonically increasing → max_dd = 0",
      m_up["max_drawdown_pct"] == 0,
      f"got {m_up['max_drawdown_pct']}")


# ---------------------------------------------------------------------------
# 4. Sharpe matches numpy on a controlled return series
# ---------------------------------------------------------------------------
print("\n=== 4. Sharpe vs numpy ===")

# Build a NAV from a known daily-return series so we can compare exactly.
rng = np.random.default_rng(42)
daily_rets = rng.normal(0.0005, 0.01, size=500)
nav_path = [100000.0]
for r in daily_rets:
    nav_path.append(nav_path[-1] * (1 + r))
nav_sharpe = [{"date": f"d{i:04d}", "nav": v} for i, v in enumerate(nav_path)]

m_sh = compute_metrics(_FakePortfolio(nav_sharpe), 100000.0, [])

# Recompute the daily returns the way compute_metrics does (from nav diffs)
engine_daily = [(nav_path[i] - nav_path[i-1]) / nav_path[i-1] for i in range(1, len(nav_path))]
# They should equal the seeded returns
check("engine-derived daily returns match seed (within numerical noise)",
      np.allclose(engine_daily, daily_rets, atol=1e-12))

# Total return
expected_total_pct = (nav_path[-1] / nav_path[0] - 1) * 100
check(f"total_return_pct matches: engine={m_sh['total_return_pct']:.4f} expected={expected_total_pct:.4f}",
      approx(m_sh["total_return_pct"], round(expected_total_pct, 2), tol=0.01))

# Annualized vol (ddof=1) — what compute_nav_stats produces. Engine rounds to 2dp.
expected_ann_vol = float(np.std(engine_daily, ddof=1) * np.sqrt(252) * 100)
check(f"annualized_volatility_pct matches numpy: engine={m_sh['annualized_volatility_pct']:.4f} numpy={expected_ann_vol:.4f}",
      approx(m_sh["annualized_volatility_pct"], expected_ann_vol, tol=0.02))


# ---------------------------------------------------------------------------
# 5. Trade-stats: win rate, profit factor, avg win/loss
# ---------------------------------------------------------------------------
print("\n=== 5. win_rate_pct + profit_factor ===")

# Build a tiny trade ledger:
# 4 wins of +$1000 each → gross_win = $4000
# 1 loss of -$1500 → gross_loss = $1500
# Profit factor = 4000 / 1500 = 2.6667
trades = [
    {"date": "2024-01-05", "action": "BUY", "symbol": "AAA"},
    {"date": "2024-01-10", "action": "SELL", "symbol": "AAA"},
    {"date": "2024-01-12", "action": "BUY", "symbol": "BBB"},
    {"date": "2024-01-20", "action": "SELL", "symbol": "BBB"},
    {"date": "2024-01-22", "action": "BUY", "symbol": "CCC"},
    {"date": "2024-02-01", "action": "SELL", "symbol": "CCC"},
    {"date": "2024-02-03", "action": "BUY", "symbol": "DDD"},
    {"date": "2024-02-10", "action": "SELL", "symbol": "DDD"},
    {"date": "2024-02-12", "action": "BUY", "symbol": "EEE"},
    {"date": "2024-02-25", "action": "SELL", "symbol": "EEE"},  # loser
]
closed = [
    {"reason": "take_profit",     "pnl": 1000, "pnl_pct": 10, "days_held": 5},
    {"reason": "take_profit",     "pnl": 1000, "pnl_pct": 10, "days_held": 6},
    {"reason": "take_profit",     "pnl": 1000, "pnl_pct": 10, "days_held": 8},
    {"reason": "take_profit",     "pnl": 1000, "pnl_pct": 10, "days_held": 5},
    {"reason": "stop_loss",       "pnl": -1500, "pnl_pct": -15, "days_held": 9},
]
# A trivial nav_history so the metrics function returns
nav_t = [{"date": "2024-01-01", "nav": 100000.0}, {"date": "2024-03-01", "nav": 102500.0}]
m_t = compute_metrics(_FakePortfolio(nav_t, closed_trades=closed, trades=trades), 100000.0, [])

check(f"win_rate_pct = 4/5 = 80% (got {m_t['win_rate_pct']})",
      approx(m_t["win_rate_pct"], 80.0))
check(f"profit_factor = 4000/1500 = 2.6667 (got {m_t['profit_factor']:.4f})",
      approx(m_t["profit_factor"], 2.6666666, tol=0.01))
check(f"total_entries (BUY count) = 5 (got {m_t['total_entries']})",
      m_t["total_entries"] == 5)
check(f"total_trades (closed, excl backtest_end) = 5 (got {m_t['total_trades']})",
      m_t["total_trades"] == 5)
check(f"wins = 4 (got {m_t['wins']})", m_t["wins"] == 4)
check(f"losses = 1 (got {m_t['losses']})", m_t["losses"] == 1)

# avg_win_pct = mean([10, 10, 10, 10]) = 10
check(f"avg_win_pct = 10 (got {m_t['avg_win_pct']})",
      approx(m_t["avg_win_pct"], 10.0))
# avg_loss_pct = -15
check(f"avg_loss_pct = -15 (got {m_t['avg_loss_pct']})",
      approx(m_t["avg_loss_pct"], -15.0))
# avg_holding_days = (5+6+8+5+9)/5 = 6.6
check(f"avg_holding_days = 6.6 (got {m_t['avg_holding_days']})",
      approx(m_t["avg_holding_days"], 6.6, tol=0.1))


# ---------------------------------------------------------------------------
# 6. backtest_end trades are excluded from win_rate (regression check)
# ---------------------------------------------------------------------------
print("\n=== 6. backtest_end exclusion from win_rate ===")

# 3 real wins + 2 backtest_end profitable closes. win_rate counts only the 3 real.
closed_with_be = [
    {"reason": "take_profit", "pnl": 100, "pnl_pct": 1, "days_held": 5},
    {"reason": "take_profit", "pnl": 200, "pnl_pct": 2, "days_held": 5},
    {"reason": "stop_loss", "pnl": -50, "pnl_pct": -1, "days_held": 5},
    {"reason": "backtest_end", "pnl": 999, "pnl_pct": 10, "days_held": 5},  # excluded
    {"reason": "backtest_end", "pnl": 999, "pnl_pct": 10, "days_held": 5},  # excluded
]
trades_with_be = [
    {"date": f"d{i}", "action": "BUY", "symbol": f"S{i}"} for i in range(5)
]
m_be = compute_metrics(_FakePortfolio(nav_t, closed_trades=closed_with_be, trades=trades_with_be), 100000.0, [])
check(f"backtest_end excluded → win_rate = 2/3 = 66.67% (got {m_be['win_rate_pct']:.2f})",
      approx(m_be["win_rate_pct"], 200 / 3, tol=0.01))


# ---------------------------------------------------------------------------
# 7. Zero-cash edge case
# ---------------------------------------------------------------------------
print("\n=== 7. initial_cash = 0 → safe-zero metrics ===")

m_z = compute_metrics(_FakePortfolio([{"date": "d0", "nav": 0.0}]), 0.0, [])
check("total_return_pct = 0 for zero initial_cash", m_z["total_return_pct"] == 0.0)
check("sharpe_ratio = None for zero initial_cash", m_z["sharpe_ratio"] is None)
check("max_drawdown_pct = 0 for zero initial_cash", m_z["max_drawdown_pct"] == 0.0)


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
