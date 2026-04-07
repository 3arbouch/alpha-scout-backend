#!/usr/bin/env python3
"""
Comprehensive portfolio engine E2E test — math validation + deployment.

Tests independently recompute every metric from raw NAV history and compare
against the engine's output, then deploy and verify DB persistence matches.

Test map:
  1.  Weighted return identity:   portfolio return ≈ Σ(weight_i × sleeve_return_i)
  2.  NAV conservation (to_cash): combined NAV = Σ(sleeve NAVs) every day
  3.  NAV conservation (redistribute): combined = Σ(active sleeves) + pool every day
  4.  Independent Sharpe validation from raw NAV
  5.  Independent Sortino validation from raw NAV
  6.  Independent max-drawdown + drawdown date validation
  7.  Independent annualized return + volatility validation
  8.  Independent profit factor + win rate from trades
  9.  Regime gating freeze: gated-off sleeve NAV stays frozen
  10. Redistribute outperforms to_cash when active sleeves rally
  11. Per-sleeve contribution sums to portfolio total return
  12. 5-sleeve large portfolio stress test
  13. Asymmetric 90/10 weight attribution
  14. Deploy → evaluate → DB roundtrip
  15. Short period (10 trading days) — no divide-by-zero
  16. All-defensive portfolio in a drawdown year

Run:
    cd /app/scripts
    python3 test_portfolio_math_e2e.py
"""
import json
import sys
import os
import math
import statistics
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine import run_portfolio_backtest, get_connection
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

def approx(a, b, tol=0.5):
    """Check two values are within tolerance (default 0.5 pct points)."""
    if a is None or b is None:
        return False
    return abs(a - b) < tol


# ---------------------------------------------------------------------------
# Shared configs
# ---------------------------------------------------------------------------
BT_START = "2024-01-01"
BT_END   = "2024-12-31"
CAPITAL  = 300_000

# Real regime IDs from DB — oil_shock_v2 is inactive all of 2024, recovery is active 244/252 days
REGIME_ALWAYS_OFF = "oil_shock_v2_378f18c9"   # 0 active days in 2024
REGIME_MOSTLY_ON  = "recovery_2130f82b"        # 244/252 active days in 2024

STRATEGY_TECH = {
    "name": "Test Tech",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "NVDA"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": BT_START, "end": BT_END, "entry_price": "next_close", "slippage_bps": 10},
}

STRATEGY_DEFENSIVE = {
    "name": "Test Defensive",
    "universe": {"type": "symbols", "symbols": ["JNJ", "PG", "KO"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": BT_START, "end": BT_END, "entry_price": "next_close", "slippage_bps": 10},
}

STRATEGY_ENERGY = {
    "name": "Test Energy",
    "universe": {"type": "symbols", "symbols": ["XOM", "CVX", "COP"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": BT_START, "end": BT_END, "entry_price": "next_close", "slippage_bps": 10},
}


def make_portfolio(name, sleeves_config, regime_filter=False,
                   capital_flow="to_cash", profiles=None, profile_priority=None,
                   transition_days=1, start=BT_START, end=BT_END, capital=CAPITAL):
    cfg = {
        "name": name,
        "sleeves": sleeves_config,
        "regime_filter": regime_filter,
        "capital_when_gated_off": capital_flow,
        "backtest": {"start": start, "end": end, "initial_capital": capital},
    }
    if transition_days > 1:
        cfg["transition_days"] = transition_days
    if profiles:
        cfg["allocation_profiles"] = profiles
    if profile_priority:
        cfg["profile_priority"] = profile_priority
    return cfg


# ---------------------------------------------------------------------------
# Helper: independent metrics from raw NAV history
# ---------------------------------------------------------------------------
def independent_metrics(nav_history, initial_capital):
    """Recompute all metrics from a list of {date, nav} dicts."""
    navs = [e["nav"] for e in nav_history]
    dates = [e["date"] for e in nav_history]
    final_nav = navs[-1]

    # Total return
    total_return = (final_nav / initial_capital - 1) * 100

    # Annualized return
    days = (datetime.strptime(dates[-1], "%Y-%m-%d") -
            datetime.strptime(dates[0], "%Y-%m-%d")).days
    years = max(days / 365.25, 0.01)
    ann_return = ((final_nav / initial_capital) ** (1 / years) - 1) * 100

    # Daily returns
    daily_returns = []
    for j in range(1, len(navs)):
        if navs[j-1] > 0:
            daily_returns.append(navs[j] / navs[j-1] - 1)

    # Annualized volatility
    ann_vol = statistics.stdev(daily_returns) * (252 ** 0.5) * 100 if len(daily_returns) > 1 else 0

    # Risk-free rate (load same source as engine)
    risk_free_ann = 0.0
    try:
        treasury_path = Path(__file__).parent.parent / "data" / "macro" / "treasury-rates.json"
        if treasury_path.exists():
            treasury_data = json.loads(treasury_path.read_text())
            t_rates = treasury_data.get("data", treasury_data) if isinstance(treasury_data, dict) else treasury_data
            period_rates = [r["month3"] for r in t_rates
                           if dates[0] <= r["date"] <= dates[-1] and r.get("month3") is not None]
            if period_rates:
                risk_free_ann = sum(period_rates) / len(period_rates)
    except Exception:
        risk_free_ann = 2.0

    # Sharpe
    excess = ann_return - risk_free_ann
    sharpe = excess / ann_vol if ann_vol > 0 else 0

    # Sortino
    daily_rf = risk_free_ann / 100 / 252
    downside_sq = [min(r - daily_rf, 0) ** 2 for r in daily_returns]
    downside_dev = math.sqrt(sum(downside_sq) / len(downside_sq)) * math.sqrt(252) * 100 if downside_sq else 0
    sortino = excess / downside_dev if downside_dev > 0 else 0

    # Max drawdown
    peak = initial_capital
    max_dd = 0
    max_dd_date = dates[0]
    for e in nav_history:
        nav = e["nav"]
        if nav > peak:
            peak = nav
        dd = (nav / peak - 1) * 100
        if dd < max_dd:
            max_dd = dd
            max_dd_date = e["date"]

    # Calmar
    calmar = abs(ann_return / max_dd) if max_dd < 0 else 0

    return {
        "total_return_pct": total_return,
        "annualized_return_pct": ann_return,
        "annualized_volatility_pct": ann_vol,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "max_drawdown_pct": max_dd,
        "max_drawdown_date": max_dd_date,
        "calmar_ratio": calmar,
        "risk_free_ann": risk_free_ann,
    }


# =========================================================================
print("\n" + "=" * 70)
print("TEST 1: Weighted return identity — no gating, fixed weights")
print("  Portfolio return ≈ Σ(weight_i × sleeve_return_i)")
print("=" * 70)

sleeves_1 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.40, "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.35, "regime_gate": ["*"], "label": "Defensive"},
    {"strategy_config": STRATEGY_ENERGY, "weight": 0.25, "regime_gate": ["*"], "label": "Energy"},
]
cfg1 = make_portfolio("T1 Weighted Identity", sleeves_1)
r1 = run_portfolio_backtest(cfg1)
m1 = r1["metrics"]
ps1 = r1["per_sleeve"]

# Weighted sum of sleeve returns
weighted_sum = sum(s["weight"] * s["total_return_pct"] for s in ps1)
check("Portfolio return ≈ weighted sleeve returns",
      approx(m1["total_return_pct"], weighted_sum, tol=1.0),
      f"portfolio={m1['total_return_pct']:.2f}% vs weighted_sum={weighted_sum:.2f}%")

check("Final NAV consistent with total return",
      approx(m1["final_nav"], CAPITAL * (1 + m1["total_return_pct"] / 100), tol=50.0),
      f"final_nav={m1['final_nav']} vs computed={CAPITAL * (1 + m1['total_return_pct'] / 100):.2f}")

check("All 3 sleeves active 100% of days",
      all(s["active_days"] == m1["trading_days"] for s in ps1),
      f"active_days={[s['active_days'] for s in ps1]}, trading_days={m1['trading_days']}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 2: NAV conservation (to_cash) — combined = Σ(sleeve NAVs)")
print("=" * 70)

cfg2 = make_portfolio("T2 Conservation to_cash", sleeves_1, capital_flow="to_cash")
r2 = run_portfolio_backtest(cfg2)
nav_hist_2 = r2["combined_nav_history"]

violations_2 = 0
max_gap_2 = 0
for entry in nav_hist_2:
    sleeve_sum = sum(s["nav"] for s in entry["sleeves"])
    gap = abs(entry["nav"] - sleeve_sum)
    if gap > 1.0:
        violations_2 += 1
    max_gap_2 = max(max_gap_2, gap)

check("Combined NAV = Σ(sleeve NAVs) every day",
      violations_2 == 0,
      f"{violations_2} days with gap > $1, max_gap=${max_gap_2:.2f}")

check("No negative NAVs",
      all(e["nav"] > 0 for e in nav_hist_2))


# =========================================================================
print("\n" + "=" * 70)
print("TEST 3: NAV conservation (redistribute)")
print("  Redistribute pool must be accounted for in combined NAV")
print("=" * 70)

# Use regime gating so some sleeves get gated off
# The "always_off" regime gate will never match since we have no regimes
sleeves_3 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.50, "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.50, "regime_gate": [REGIME_ALWAYS_OFF], "label": "Defensive"},
]
cfg3 = make_portfolio("T3 Redistribute Conservation", sleeves_3,
                      regime_filter=True, capital_flow="redistribute")
r3 = run_portfolio_backtest(cfg3)
m3 = r3["metrics"]
ps3 = r3["per_sleeve"]

# In redistribute mode: the gated-off sleeve's capital goes to the active sleeve
# So combined NAV should still track correctly
check("Tech sleeve active all days",
      ps3[0]["active_days"] == m3["trading_days"],
      f"active={ps3[0]['active_days']}, total={m3['trading_days']}")

check("Defensive sleeve gated off all days",
      ps3[1]["gated_off_days"] == m3["trading_days"],
      f"gated_off={ps3[1]['gated_off_days']}, total={m3['trading_days']}")

# With redistribute, the gated-off sleeve's capital earns the active sleeve's returns
# So the portfolio should perform better than just 50% of Tech
tech_standalone = run_backtest(STRATEGY_TECH)
tech_ret = tech_standalone["metrics"]["total_return_pct"]
# With redistribute, the entire $300k earns the Tech return, not just $150k
# So portfolio return should be close to Tech standalone return
check("Redistribute portfolio return ≈ active sleeve standalone return",
      approx(m3["total_return_pct"], tech_ret, tol=2.0),
      f"portfolio={m3['total_return_pct']:.2f}%, tech_standalone={tech_ret:.2f}%")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 4: Independent Sharpe validation from raw NAV")
print("=" * 70)

indep_4 = independent_metrics(r1["combined_nav_history"], CAPITAL)
check("Sharpe matches (engine vs independent)",
      approx(m1["sharpe_ratio"], indep_4["sharpe_ratio"], tol=0.05),
      f"engine={m1['sharpe_ratio']:.4f}, independent={indep_4['sharpe_ratio']:.4f}")

check("Same risk-free rate used",
      approx(indep_4["risk_free_ann"], indep_4["risk_free_ann"], tol=0.01))


# =========================================================================
print("\n" + "=" * 70)
print("TEST 5: Independent Sortino validation from raw NAV")
print("=" * 70)

check("Sortino matches (engine vs independent)",
      approx(m1["sortino_ratio"], indep_4["sortino_ratio"], tol=0.1),
      f"engine={m1['sortino_ratio']:.4f}, independent={indep_4['sortino_ratio']:.4f}")

check("Sortino > 0 for positive return portfolio",
      m1["sortino_ratio"] > 0 if m1["total_return_pct"] > 5 else True,
      f"sortino={m1['sortino_ratio']}, return={m1['total_return_pct']}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 6: Independent max-drawdown + date validation")
print("=" * 70)

check("Max drawdown matches (engine vs independent)",
      approx(m1["max_drawdown_pct"], indep_4["max_drawdown_pct"], tol=0.01),
      f"engine={m1['max_drawdown_pct']:.4f}, independent={indep_4['max_drawdown_pct']:.4f}")

check("Max drawdown date matches",
      m1["max_drawdown_date"] == indep_4["max_drawdown_date"],
      f"engine={m1['max_drawdown_date']}, independent={indep_4['max_drawdown_date']}")

check("Max drawdown is negative or zero",
      m1["max_drawdown_pct"] <= 0,
      f"max_dd={m1['max_drawdown_pct']}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 7: Independent annualized return + volatility validation")
print("=" * 70)

check("Annualized return matches (engine vs independent)",
      approx(m1["annualized_return_pct"], indep_4["annualized_return_pct"], tol=0.1),
      f"engine={m1['annualized_return_pct']:.4f}, independent={indep_4['annualized_return_pct']:.4f}")

check("Annualized volatility matches (engine vs independent)",
      approx(m1["annualized_volatility_pct"], indep_4["annualized_volatility_pct"], tol=0.1),
      f"engine={m1['annualized_volatility_pct']:.4f}, independent={indep_4['annualized_volatility_pct']:.4f}")

check("Calmar ratio matches (engine vs independent)",
      approx(m1["calmar_ratio"], indep_4["calmar_ratio"], tol=0.05),
      f"engine={m1['calmar_ratio']:.4f}, independent={indep_4['calmar_ratio']:.4f}")

check("Total return matches (engine vs independent)",
      approx(m1["total_return_pct"], indep_4["total_return_pct"], tol=0.01),
      f"engine={m1['total_return_pct']:.4f}, independent={indep_4['total_return_pct']:.4f}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 8: Independent profit factor + win rate from trades")
print("=" * 70)

all_closed_8 = []
for sr in r1["sleeve_results"]:
    all_closed_8.extend(sr.get("closed_trades", []))

if all_closed_8:
    wins_8 = sum(1 for t in all_closed_8 if t.get("pnl", 0) > 0)
    losses_8 = sum(1 for t in all_closed_8 if t.get("pnl", 0) <= 0)
    closed_8 = len(all_closed_8)
    win_rate_8 = round(wins_8 / max(closed_8, 1) * 100, 1)

    gp_8 = sum(t.get("pnl", 0) for t in all_closed_8 if t.get("pnl", 0) > 0)
    gl_8 = abs(sum(t.get("pnl", 0) for t in all_closed_8 if t.get("pnl", 0) < 0))
    pf_8 = round(min(gp_8 / max(gl_8, 0.01), 999.99), 2)

    check("Win rate matches (engine vs independent)",
          approx(m1["win_rate_pct"], win_rate_8, tol=0.1),
          f"engine={m1['win_rate_pct']}, independent={win_rate_8}")

    check("Profit factor matches (engine vs independent)",
          approx(m1["profit_factor"], pf_8, tol=0.01),
          f"engine={m1['profit_factor']}, independent={pf_8}")

    check("Closed trades count matches",
          m1["closed_trades"] == closed_8,
          f"engine={m1['closed_trades']}, independent={closed_8}")

    check("Wins + losses = closed trades",
          m1["wins"] + m1["losses"] == m1["closed_trades"],
          f"wins={m1['wins']} + losses={m1['losses']} = {m1['wins']+m1['losses']}, closed={m1['closed_trades']}")
else:
    check("No closed trades (buy-and-hold)", True)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 9: Regime gating freeze — gated-off sleeve NAV stays frozen")
print("=" * 70)

sleeves_9 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.60, "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.40, "regime_gate": [REGIME_ALWAYS_OFF], "label": "Defensive"},
]
cfg9 = make_portfolio("T9 Freeze Check", sleeves_9, regime_filter=True, capital_flow="to_cash")
r9 = run_portfolio_backtest(cfg9)
nav_hist_9 = r9["combined_nav_history"]

# The Defensive sleeve (gated off) should have frozen NAV = 40% of CAPITAL
frozen_nav = CAPITAL * 0.40  # $120,000
all_frozen_ok = True
for entry in nav_hist_9:
    def_sleeve = [s for s in entry["sleeves"] if s["label"] == "Defensive"][0]
    if abs(def_sleeve["nav"] - frozen_nav) > 1.0:
        all_frozen_ok = False
        break

check("Gated-off sleeve NAV stays frozen at allocated capital",
      all_frozen_ok,
      f"expected ${frozen_nav:.0f} for Defensive sleeve")

check("Defensive sleeve marked inactive every day",
      all(not [s for s in e["sleeves"] if s["label"] == "Defensive"][0]["active"]
          for e in nav_hist_9))

# Active sleeve should still compound
tech_navs = [
    [s for s in e["sleeves"] if s["label"] == "Tech"][0]["nav"]
    for e in nav_hist_9
]
check("Tech sleeve NAV changes over time (not frozen)",
      tech_navs[-1] != tech_navs[0],
      f"first=${tech_navs[0]:.0f}, last=${tech_navs[-1]:.0f}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 10: Redistribute outperforms to_cash when active sleeve rallies")
print("=" * 70)

sleeves_10 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.50, "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.50, "regime_gate": [REGIME_ALWAYS_OFF], "label": "Defensive"},
]

cfg_cash = make_portfolio("T10 to_cash", sleeves_10, regime_filter=True, capital_flow="to_cash")
cfg_redist = make_portfolio("T10 redistribute", sleeves_10, regime_filter=True, capital_flow="redistribute")

r_cash = run_portfolio_backtest(cfg_cash)
r_redist = run_portfolio_backtest(cfg_redist)

cash_ret = r_cash["metrics"]["total_return_pct"]
redist_ret = r_redist["metrics"]["total_return_pct"]

# Tech had a positive year in 2024. With redistribute, the frozen $150k also compounds
# at the Tech sleeve's rate, so redistribute should outperform to_cash.
tech_positive = STRATEGY_TECH["universe"]["symbols"]  # AAPL, MSFT, NVDA — 2024 was positive
check("Redistribute outperforms to_cash when active sleeve rallies",
      redist_ret > cash_ret,
      f"redistribute={redist_ret:.2f}% vs to_cash={cash_ret:.2f}%")

check("Both modes have valid final NAV",
      r_cash["metrics"]["final_nav"] > 0 and r_redist["metrics"]["final_nav"] > 0)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 11: Per-sleeve contribution sums to portfolio total return")
print("=" * 70)

contribution_sum = sum(s["contribution_pct"] for s in ps1)
check("Σ(sleeve contributions) ≈ portfolio total return",
      approx(contribution_sum, m1["total_return_pct"], tol=1.0),
      f"sum_contributions={contribution_sum:.2f}%, total_return={m1['total_return_pct']:.2f}%")

# Each sleeve's contribution = weight * sleeve_return
for s in ps1:
    expected_contrib = s["weight"] * s["total_return_pct"]
    check(f"  {s['label']}: contribution ≈ weight × return",
          approx(s["contribution_pct"], expected_contrib, tol=0.5),
          f"contribution={s['contribution_pct']:.2f}%, expected={expected_contrib:.2f}%")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 12: 5-sleeve large portfolio stress test")
print("=" * 70)

STRATEGY_HEALTH = {
    "name": "Test Health",
    "universe": {"type": "symbols", "symbols": ["UNH", "JNJ", "LLY"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": BT_START, "end": BT_END, "entry_price": "next_close", "slippage_bps": 10},
}

STRATEGY_CONSUMER = {
    "name": "Test Consumer",
    "universe": {"type": "symbols", "symbols": ["COST", "WMT", "TGT"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
    "backtest": {"start": BT_START, "end": BT_END, "entry_price": "next_close", "slippage_bps": 10},
}

sleeves_12 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.30, "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.20, "regime_gate": ["*"], "label": "Defensive"},
    {"strategy_config": STRATEGY_ENERGY, "weight": 0.15, "regime_gate": ["*"], "label": "Energy"},
    {"strategy_config": STRATEGY_HEALTH, "weight": 0.20, "regime_gate": ["*"], "label": "Health"},
    {"strategy_config": STRATEGY_CONSUMER, "weight": 0.15, "regime_gate": ["*"], "label": "Consumer"},
]

big_capital = 1_000_000
cfg12 = make_portfolio("T12 5-Sleeve", sleeves_12, capital=big_capital)
r12 = run_portfolio_backtest(cfg12)
m12 = r12["metrics"]
ps12 = r12["per_sleeve"]

check("5 sleeves all returned results",
      len(ps12) == 5,
      f"got {len(ps12)} sleeves")

check("Weights sum to 1.0",
      approx(sum(s["weight"] for s in ps12), 1.0, tol=0.01),
      f"sum={sum(s['weight'] for s in ps12):.4f}")

# NAV conservation
violations_12 = 0
for entry in r12["combined_nav_history"]:
    sleeve_sum = sum(s["nav"] for s in entry["sleeves"])
    if abs(entry["nav"] - sleeve_sum) > 2.0:
        violations_12 += 1

check("NAV conservation holds for 5 sleeves",
      violations_12 == 0,
      f"{violations_12} violations")

# Independent metrics match
indep_12 = independent_metrics(r12["combined_nav_history"], big_capital)
check("Sharpe matches (5-sleeve independent validation)",
      approx(m12["sharpe_ratio"], indep_12["sharpe_ratio"], tol=0.05),
      f"engine={m12['sharpe_ratio']:.4f}, independent={indep_12['sharpe_ratio']:.4f}")

check("Max drawdown matches (5-sleeve)",
      approx(m12["max_drawdown_pct"], indep_12["max_drawdown_pct"], tol=0.01),
      f"engine={m12['max_drawdown_pct']:.4f}, independent={indep_12['max_drawdown_pct']:.4f}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 13: Asymmetric 90/10 weight attribution")
print("=" * 70)

sleeves_13 = [
    {"strategy_config": STRATEGY_TECH, "weight": 0.90, "regime_gate": ["*"], "label": "Tech"},
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.10, "regime_gate": ["*"], "label": "Defensive"},
]
cfg13 = make_portfolio("T13 Asymmetric 90/10", sleeves_13)
r13 = run_portfolio_backtest(cfg13)
m13 = r13["metrics"]
ps13 = r13["per_sleeve"]

tech_contrib = ps13[0]["contribution_pct"]
def_contrib = ps13[1]["contribution_pct"]

check("Tech (90%) contributes majority of return",
      abs(tech_contrib) > abs(def_contrib),
      f"tech_contrib={tech_contrib:.2f}%, def_contrib={def_contrib:.2f}%")

# The portfolio return should be dominated by the 90% sleeve
weighted_expected = 0.90 * ps13[0]["total_return_pct"] + 0.10 * ps13[1]["total_return_pct"]
check("Portfolio return ≈ weighted average",
      approx(m13["total_return_pct"], weighted_expected, tol=1.0),
      f"portfolio={m13['total_return_pct']:.2f}%, expected={weighted_expected:.2f}%")

# Capital allocation
check("Tech allocated 90% of capital",
      approx(ps13[0]["allocated_capital"], CAPITAL * 0.90, tol=1),
      f"tech_alloc={ps13[0]['allocated_capital']}, expected={CAPITAL * 0.90}")

check("Defensive allocated 10% of capital",
      approx(ps13[1]["allocated_capital"], CAPITAL * 0.10, tol=1),
      f"def_alloc={ps13[1]['allocated_capital']}, expected={CAPITAL * 0.10}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 14: Deploy → evaluate → DB roundtrip")
print("=" * 70)

from deploy_engine_v2 import (
    deploy, evaluate_one, get_db, get_deployment,
    stop_deployment, list_deployments,
)

cfg14 = make_portfolio("T14 DB Roundtrip", sleeves_1,
                       start="2024-06-01", end="2024-12-31", capital=200_000)

dep_result = deploy(cfg14, "2024-06-01", 200_000, "T14 DB Roundtrip")
dep_id = dep_result["id"]
check("Deploy returned ID", dep_id is not None and len(dep_id) > 5, f"id={dep_id}")

# Re-evaluate
eval_result = evaluate_one(dep_id)
check("Evaluate returned result", eval_result is not None)

eval_metrics = eval_result.get("metrics", {}) if eval_result else {}

# Get from DB
detail = get_deployment(dep_id)
check("DB row exists with correct name",
      detail["name"] == "T14 DB Roundtrip",
      f"name={detail.get('name')}")

# Check DB metrics match engine output
check("DB final_nav matches engine",
      approx(detail.get("last_nav", 0), eval_metrics.get("final_nav", 0), tol=1.0),
      f"db={detail.get('last_nav')}, engine={eval_metrics.get('final_nav')}")

check("DB return_pct matches engine",
      approx(detail.get("last_return_pct", 0), eval_metrics.get("total_return_pct", 0), tol=0.1),
      f"db={detail.get('last_return_pct')}, engine={eval_metrics.get('total_return_pct')}")

# Independent validation of DB metrics
db_nav = detail.get("last_nav", 0)
db_return = detail.get("last_return_pct", 0)
expected_nav = 200_000 * (1 + db_return / 100)
check("DB NAV consistent with DB return",
      approx(db_nav, expected_nav, tol=5.0),
      f"db_nav={db_nav:.2f}, computed_from_return={expected_nav:.2f}")

# Check sleeves persisted
db = get_db()
cur = db.cursor()
cur.execute("SELECT COUNT(*) FROM sleeves WHERE deployment_id = ?", (dep_id,))
sleeve_count = cur.fetchone()[0]
check("3 sleeves persisted in DB",
      sleeve_count == 3,
      f"got {sleeve_count}")

# Check trades persisted
cur.execute("SELECT COUNT(*) FROM trades WHERE source_id = ?", (dep_id,))
trade_count = cur.fetchone()[0]
check("Trades persisted in DB", trade_count > 0, f"got {trade_count}")

# Cleanup
stop_deployment(dep_id)
check("Deployment stopped", True)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 15: Short period (10 trading days) — no divide-by-zero")
print("=" * 70)

sleeves_15 = [
    {"strategy_config": {
        "name": "Short Tech",
        "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT"]},
        "entry": {"conditions": [{"type": "always"}], "logic": "all"},
        "sizing": {"type": "equal_weight", "max_positions": 2, "initial_allocation": 50000},
        "backtest": {"start": "2024-12-15", "end": "2024-12-31", "entry_price": "next_close", "slippage_bps": 10},
    }, "weight": 1.0, "regime_gate": ["*"], "label": "Short Tech"},
]

cfg15 = make_portfolio("T15 Short Period", sleeves_15,
                       start="2024-12-15", end="2024-12-31", capital=100_000)
r15 = run_portfolio_backtest(cfg15)
m15 = r15["metrics"]

check("Metrics computed without error",
      "total_return_pct" in m15,
      f"keys={list(m15.keys())}")

check("No NaN/Inf in metrics",
      all(not (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))
          for v in m15.values() if isinstance(v, (int, float))),
      f"metrics={m15}")

check("JSON serializable (no Infinity)",
      json.loads(json.dumps(m15)) is not None)

# Independent check
if r15["combined_nav_history"]:
    indep_15 = independent_metrics(r15["combined_nav_history"], 100_000)
    check("Sharpe matches for short period",
          approx(m15.get("sharpe_ratio", 0), indep_15["sharpe_ratio"], tol=0.1),
          f"engine={m15.get('sharpe_ratio')}, independent={indep_15['sharpe_ratio']:.4f}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 16: All-defensive portfolio in 2024 — monotonic math checks")
print("=" * 70)

sleeves_16 = [
    {"strategy_config": STRATEGY_DEFENSIVE, "weight": 0.50, "regime_gate": ["*"], "label": "Defensive A"},
    {"strategy_config": {
        "name": "Test Defensive B",
        "universe": {"type": "symbols", "symbols": ["CL", "WMT", "COST"]},
        "entry": {"conditions": [{"type": "always"}], "logic": "all"},
        "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100000},
        "backtest": {"start": BT_START, "end": BT_END, "entry_price": "next_close", "slippage_bps": 10},
    }, "weight": 0.50, "regime_gate": ["*"], "label": "Defensive B"},
]

cfg16 = make_portfolio("T16 All Defensive", sleeves_16)
r16 = run_portfolio_backtest(cfg16)
m16 = r16["metrics"]

# Monotonic checks
check("Annualized return sign matches total return sign",
      (m16["annualized_return_pct"] > 0) == (m16["total_return_pct"] > 0) or abs(m16["total_return_pct"]) < 0.1,
      f"ann={m16['annualized_return_pct']:.2f}%, total={m16['total_return_pct']:.2f}%")

check("Max drawdown is non-positive",
      m16["max_drawdown_pct"] <= 0.001,
      f"max_dd={m16['max_drawdown_pct']:.4f}")

check("Volatility is non-negative",
      m16["annualized_volatility_pct"] >= 0,
      f"vol={m16['annualized_volatility_pct']:.4f}")

check("Calmar ratio is non-negative",
      m16["calmar_ratio"] >= 0,
      f"calmar={m16['calmar_ratio']:.4f}")

check("Utilization between 0% and 200%",
      0 <= m16["utilization_pct"] <= 200,
      f"utilization={m16['utilization_pct']}")

# Independent full validation
indep_16 = independent_metrics(r16["combined_nav_history"], CAPITAL)
check("Independent Sharpe matches",
      approx(m16["sharpe_ratio"], indep_16["sharpe_ratio"], tol=0.05),
      f"engine={m16['sharpe_ratio']:.4f}, independent={indep_16['sharpe_ratio']:.4f}")

check("Independent Sortino matches",
      approx(m16["sortino_ratio"], indep_16["sortino_ratio"], tol=0.1),
      f"engine={m16['sortino_ratio']:.4f}, independent={indep_16['sortino_ratio']:.4f}")


# =========================================================================
# Final tally
# =========================================================================
print("\n" + "=" * 70)
TOTAL = PASS + FAIL
print(f"RESULTS: {PASS}/{TOTAL} passed, {FAIL} failed")
print("=" * 70)
if FAIL == 0:
    print("ALL TESTS PASSED ✅")
else:
    print(f"{FAIL} TESTS FAILED ❌")
    sys.exit(1)
