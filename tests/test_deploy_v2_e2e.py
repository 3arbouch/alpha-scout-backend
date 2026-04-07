#!/usr/bin/env python3
"""
End-to-end test for deploy_engine_v2 (unified deployment model).

Tests:
  1. Deploy a single strategy (auto-wrapped as portfolio)
  2. Deploy a portfolio
  3. Evaluate deployments
  4. Verify DB state after evaluation
  5. Control functions (pause/resume/stop)
  6. Migration from v1 tables
  7. Alert generation
  8. Backtest run persistence

Run:
    cd /app/scripts
    python3 test_deploy_v2_e2e.py
"""
import json
import sys
import os
import sqlite3
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from deploy_engine_v2 import (
    deploy, evaluate_one, evaluate_all, get_db,
    stop_deployment, pause_deployment, resume_deployment,
    list_deployments, get_deployment, set_alert_mode,
    persist_trades, persist_sleeves,
    wrap_strategy_as_portfolio, _is_strategy_config,
    migrate_from_v1,
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


# =========================================================================
print("\n" + "=" * 70)
print("TEST 1: _is_strategy_config detection")
print("=" * 70)

strategy_cfg = {
    "name": "Test Strategy",
    "universe": {"type": "symbols", "symbols": ["AAPL"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 1, "initial_allocation": 50000},
    "backtest": {"start": "2024-06-01", "end": "2024-06-30", "entry_price": "next_close", "slippage_bps": 10},
}

portfolio_cfg = {
    "name": "Test Portfolio",
    "strategies": [
        {"strategy_config": strategy_cfg, "weight": 1.0, "regime_gate": ["*"], "label": "Main"},
    ],
    "regime_filter": False,
    "capital_when_gated_off": "to_cash",
    "backtest": {"start": "2024-06-01", "end": "2024-06-30", "initial_capital": 50000},
}

check("Detects strategy config", _is_strategy_config(strategy_cfg))
check("Detects portfolio config", not _is_strategy_config(portfolio_cfg))


# =========================================================================
print("\n" + "=" * 70)
print("TEST 2: wrap_strategy_as_portfolio")
print("=" * 70)

wrapped = wrap_strategy_as_portfolio(strategy_cfg, 100000, "2024-01-01", "2024-12-31")
check("Wrapped has 'sleeves' key", "sleeves" in wrapped)
check("Wrapped has 1 sleeve", len(wrapped["sleeves"]) == 1)
check("Sleeve weight = 1.0", wrapped["sleeves"][0]["weight"] == 1.0)
check("Sleeve regime_gate = ['*']", wrapped["sleeves"][0]["regime_gate"] == ["*"])
check("Backtest capital set", wrapped["backtest"]["initial_capital"] == 100000)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 3: Deploy a single strategy")
print("=" * 70)

result_s = deploy(strategy_cfg, start_date="2024-06-01", capital=50000, name="E2E Test Strategy")
check("Deploy returned ID", "id" in result_s)
check("Deploy type is 'strategy'", result_s["type"] == "strategy")

deploy_id_s = result_s["id"]

# Verify in DB
conn = get_db()
row = conn.execute("SELECT * FROM deployments WHERE id = ?", (deploy_id_s,)).fetchone()
check("Deployment row exists", row is not None)
if row:
    check("Type = strategy", row["type"] == "strategy")
    check("Status = active", row["status"] == "active")
    check("Has last_nav (evaluated)", row["last_nav"] is not None and row["last_nav"] > 0,
          f"last_nav={row['last_nav']}")
    check("Num sleeves = 1", row["num_sleeves"] == 1)
    # Verify config is portfolio format
    cfg = json.loads(row["config_json"])
    check("Stored config has 'sleeves'", "sleeves" in cfg)

# Verify sleeves persisted
sleeve_rows = conn.execute("SELECT * FROM sleeves WHERE deployment_id = ?", (deploy_id_s,)).fetchall()
check("1 sleeve row persisted", len(sleeve_rows) == 1, f"got {len(sleeve_rows)}")

# Verify trades persisted
trade_count = conn.execute(
    "SELECT COUNT(*) FROM trades WHERE source_id = ?", (deploy_id_s,)
).fetchone()[0]
check("Trades persisted", trade_count > 0, f"trades={trade_count}")
conn.close()


# =========================================================================
print("\n" + "=" * 70)
print("TEST 4: Deploy a portfolio")
print("=" * 70)

portfolio_deploy_cfg = {
    "name": "E2E Test Portfolio",
    "strategies": [
        {"strategy_config": {
            "name": "Tech Sleeve",
            "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT"]},
            "entry": {"conditions": [{"type": "always"}], "logic": "all"},
            "sizing": {"type": "equal_weight", "max_positions": 2, "initial_allocation": 50000},
            "backtest": {"start": "2024-06-01", "end": "2024-06-30", "entry_price": "next_close", "slippage_bps": 10},
        }, "weight": 0.60, "regime_gate": ["*"], "label": "Tech"},
        {"strategy_config": {
            "name": "Defensive Sleeve",
            "universe": {"type": "symbols", "symbols": ["JNJ", "PG"]},
            "entry": {"conditions": [{"type": "always"}], "logic": "all"},
            "sizing": {"type": "equal_weight", "max_positions": 2, "initial_allocation": 50000},
            "backtest": {"start": "2024-06-01", "end": "2024-06-30", "entry_price": "next_close", "slippage_bps": 10},
        }, "weight": 0.40, "regime_gate": ["*"], "label": "Defensive"},
    ],
    "regime_filter": False,
    "capital_when_gated_off": "to_cash",
}

result_p = deploy(portfolio_deploy_cfg, start_date="2024-06-01", capital=100000, name="E2E Test Portfolio")
check("Deploy returned ID", "id" in result_p)
check("Deploy type is 'portfolio'", result_p["type"] == "portfolio")

deploy_id_p = result_p["id"]

conn = get_db()
row = conn.execute("SELECT * FROM deployments WHERE id = ?", (deploy_id_p,)).fetchone()
check("Portfolio deployment row exists", row is not None)
if row:
    check("Type = portfolio", row["type"] == "portfolio")
    check("Num sleeves = 2", row["num_sleeves"] == 2)
    check("Has last_nav", row["last_nav"] is not None and row["last_nav"] > 0)

sleeve_rows = conn.execute("SELECT * FROM sleeves WHERE deployment_id = ?", (deploy_id_p,)).fetchall()
check("2 sleeve rows persisted", len(sleeve_rows) == 2, f"got {len(sleeve_rows)}")

# Verify sleeve labels
labels = sorted(r["label"] for r in sleeve_rows)
check("Sleeve labels correct", labels == ["Defensive", "Tech"], f"got {labels}")
conn.close()


# =========================================================================
print("\n" + "=" * 70)
print("TEST 5: list_deployments (unified)")
print("=" * 70)

all_deps = list_deployments()
our_deps = [d for d in all_deps if d["id"] in (deploy_id_s, deploy_id_p)]
check("Both deployments in unified list", len(our_deps) == 2)

strategy_deps = list_deployments(deploy_type="strategy")
check("Filter by type=strategy works",
      any(d["id"] == deploy_id_s for d in strategy_deps))

portfolio_deps = list_deployments(deploy_type="portfolio")
check("Filter by type=portfolio works",
      any(d["id"] == deploy_id_p for d in portfolio_deps))


# =========================================================================
print("\n" + "=" * 70)
print("TEST 6: get_deployment (full detail)")
print("=" * 70)

detail = get_deployment(deploy_id_p)
check("Has metrics", "metrics" in detail)
check("Has nav_history", "nav_history" in detail)
check("Has sleeves detail", "sleeves" in detail)
if detail.get("sleeves"):
    check("Sleeve detail has trades", "trades" in detail["sleeves"][0])


# =========================================================================
print("\n" + "=" * 70)
print("TEST 7: Control functions")
print("=" * 70)

pause_deployment(deploy_id_s)
conn = get_db()
status = conn.execute("SELECT status FROM deployments WHERE id = ?", (deploy_id_s,)).fetchone()["status"]
check("Pause works", status == "paused")

resume_deployment(deploy_id_s)
status = conn.execute("SELECT status FROM deployments WHERE id = ?", (deploy_id_s,)).fetchone()["status"]
check("Resume works", status == "active")

stop_deployment(deploy_id_s)
status = conn.execute("SELECT status FROM deployments WHERE id = ?", (deploy_id_s,)).fetchone()["status"]
check("Stop works", status == "stopped")
conn.close()

# Verify stopped deployments are excluded from list by default
active = list_deployments()
check("Stopped excluded from default list", not any(d["id"] == deploy_id_s for d in active))

all_inc_stopped = list_deployments(include_stopped=True)
check("Stopped included with flag", any(d["id"] == deploy_id_s for d in all_inc_stopped))


# =========================================================================
print("\n" + "=" * 70)
print("TEST 8: Evaluate skips non-active deployments")
print("=" * 70)

# deploy_id_s is stopped, evaluate_one should skip it
result_skip = evaluate_one(deploy_id_s)
check("evaluate_one returns None for stopped", result_skip is None)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 9: DB metrics match engine output")
print("=" * 70)

# Re-evaluate the portfolio deployment and verify DB matches
result_eval = evaluate_one(deploy_id_p)
check("Evaluation returned result", result_eval is not None)

if result_eval:
    conn = get_db()
    row = conn.execute("SELECT * FROM deployments WHERE id = ?", (deploy_id_p,)).fetchone()
    engine_metrics = result_eval["metrics"]

    check("DB final_nav matches engine",
          abs((row["last_nav"] or 0) - engine_metrics.get("final_nav", 0)) < 1.0,
          f"db={row['last_nav']} engine={engine_metrics.get('final_nav')}")
    check("DB return matches engine",
          abs((row["last_return_pct"] or 0) - engine_metrics.get("total_return_pct", 0)) < 0.1,
          f"db={row['last_return_pct']} engine={engine_metrics.get('total_return_pct')}")
    check("DB sharpe matches engine",
          abs((row["last_sharpe_ratio"] or 0) - engine_metrics.get("sharpe_ratio", 0)) < 0.01,
          f"db={row['last_sharpe_ratio']} engine={engine_metrics.get('sharpe_ratio')}")

    # Verify total_trades = sum of all sleeve trades
    total_engine_trades = sum(len(sr.get("trades", [])) for sr in result_eval.get("sleeve_results", []))
    check("DB total_trades matches engine trade count",
          row["total_trades"] == total_engine_trades,
          f"db={row['total_trades']} engine={total_engine_trades}")
    conn.close()


# =========================================================================
print("\n" + "=" * 70)
print("TEST 10: Alert mode")
print("=" * 70)

result_am = set_alert_mode(deploy_id_p, True)
check("Set alert mode", result_am.get("alert_mode") == True)

result_am2 = set_alert_mode(deploy_id_p, False)
check("Unset alert mode", result_am2.get("alert_mode") == False)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 11: Migration from v1")
print("=" * 70)

# Migration should be idempotent and not crash
try:
    migrate_from_v1()
    check("Migration ran without error", True)
except Exception as e:
    check("Migration ran without error", False, str(e))


# =========================================================================
# Cleanup: stop test deployments
stop_deployment(deploy_id_p)

print()
print("=" * 70)
total = PASS + FAIL
print(f"RESULTS: {PASS}/{total} passed, {FAIL} failed")
if FAIL == 0:
    print("ALL TESTS PASSED ✅")
else:
    print(f"{FAIL} TESTS FAILED ❌")
print("=" * 70)

sys.exit(1 if FAIL > 0 else 0)
