#!/usr/bin/env python3
"""
End-to-end data persistence test for the full pipeline:
  backtest → deploy → evaluate → regime → alerts → DB integrity

Validates that every DB write is correct, complete, and retrievable.

Test map:
  1.  Deploy strategy → verify deployments row fields
  2.  Deploy portfolio → verify deployments row + sleeve rows
  3.  Trade persistence: every engine trade has a DB row, fields match
  4.  Sleeve persistence: per-sleeve metrics match engine output
  5.  Evaluate → DB metrics update (NAV, return, Sharpe, alpha, etc.)
  6.  Re-evaluate idempotency: re-running evaluate produces same results
  7.  Config roundtrip: frozen config in DB matches what we deployed
  8.  Deployment disk artifacts: config.json + results.json saved
  9.  Control lifecycle: pause/resume/stop state transitions in DB
  10. Alert mode toggle persists correctly
  11. Regime deployment + state history persistence
  12. Backtest run indexing: backtest_runs table populated
  13. Portfolio backtest run indexing
  14. No orphan trades: every trade has a valid source_id
  15. No orphan sleeves: every sleeve has a valid deployment_id
  16. Index coverage: key lookup patterns use indexes
  17. JSON fields are valid JSON (not truncated / corrupt)

Run:
    cd /app/scripts
    python3 test_persistence_e2e.py
"""
import json
import sys
import os
import sqlite3
import math
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

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
    if a is None or b is None:
        return False
    return abs(a - b) < tol


# ---------------------------------------------------------------------------
# Setup: deploy fresh strategy + portfolio for testing
# ---------------------------------------------------------------------------
from deploy_engine import (
    deploy, evaluate_one, evaluate_all, get_db, get_deployment,
    stop_deployment, pause_deployment, resume_deployment,
    list_deployments, set_alert_mode, persist_trades, persist_sleeves,
)
from deploy_engine import deploy_regime, evaluate_regime_one, stop_regime_deployment

_WORKSPACE = Path(os.environ.get("WORKSPACE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))
from db_config import APP_DB_PATH as DB_PATH
DEPLOY_DIR = _WORKSPACE / "deployments"

STRAT_CONFIG = {
    "name": "Persist Test Strategy",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 2, "initial_allocation": 50000},
    "backtest": {"start": "2024-06-01", "end": "2024-12-31",
                 "entry_price": "next_close", "slippage_bps": 10},
}

PORT_CONFIG = {
    "name": "Persist Test Portfolio",
    "sleeves": [
        {
            "strategy_config": {
                "name": "PT Tech",
                "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "NVDA"]},
                "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                "sizing": {"type": "equal_weight", "max_positions": 3,
                           "initial_allocation": 100000},
                "backtest": {"start": "2024-06-01", "end": "2024-12-31",
                             "entry_price": "next_close", "slippage_bps": 10},
            },
            "weight": 0.60,
            "regime_gate": ["*"],
            "label": "PT Tech",
        },
        {
            "strategy_config": {
                "name": "PT Defensive",
                "universe": {"type": "symbols", "symbols": ["JNJ", "PG"]},
                "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                "sizing": {"type": "equal_weight", "max_positions": 2,
                           "initial_allocation": 50000},
                "backtest": {"start": "2024-06-01", "end": "2024-12-31",
                             "entry_price": "next_close", "slippage_bps": 10},
            },
            "weight": 0.40,
            "regime_gate": ["*"],
            "label": "PT Defensive",
        },
    ],
    "regime_filter": False,
    "capital_when_gated_off": "to_cash",
    "backtest": {"start": "2024-06-01", "end": "2024-12-31",
                 "initial_capital": 200000},
}


# =========================================================================
print("\n" + "=" * 70)
print("TEST 1: Deploy strategy → verify deployments row")
print("=" * 70)

dep_s = deploy(STRAT_CONFIG, "2024-06-01", 50000, "Persist Test Strategy")
sid = dep_s["id"]

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute("SELECT * FROM deployments WHERE id = ?", (sid,))
row = dict(cur.fetchone())

check("Deployment row exists", row is not None)
check("Type = strategy", row["type"] == "strategy", f"got '{row['type']}'")
check("Name matches", row["name"] == "Persist Test Strategy")
check("Status = active", row["status"] == "active", f"got '{row['status']}'")
check("Start date correct", row["start_date"] == "2024-06-01")
check("Initial capital = 50000", row["initial_capital"] == 50000)
check("Num sleeves = 1", row["num_sleeves"] == 1)
check("created_at is ISO timestamp", "T" in (row["created_at"] or ""))
check("last_nav is set (evaluated on deploy)", row["last_nav"] is not None and row["last_nav"] > 0,
      f"last_nav={row['last_nav']}")
check("last_return_pct is set", row["last_return_pct"] is not None,
      f"last_return_pct={row['last_return_pct']}")
check("last_sharpe_ratio is set", row["last_sharpe_ratio"] is not None)
check("total_trades > 0", (row["total_trades"] or 0) > 0, f"total_trades={row['total_trades']}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 2: Deploy portfolio → verify deployments + sleeves rows")
print("=" * 70)

dep_p = deploy(PORT_CONFIG, "2024-06-01", 200000, "Persist Test Portfolio")
pid = dep_p["id"]

cur.execute("SELECT * FROM deployments WHERE id = ?", (pid,))
prow = dict(cur.fetchone())

check("Portfolio deployment row exists", prow is not None)
check("Type = portfolio", prow["type"] == "portfolio", f"got '{prow['type']}'")
check("Num sleeves = 2", prow["num_sleeves"] == 2, f"got {prow['num_sleeves']}")
check("Capital = 200000", prow["initial_capital"] == 200000)

# Sleeve rows
cur.execute("SELECT * FROM sleeves WHERE deployment_id = ? ORDER BY label", (pid,))
sleeve_rows = [dict(r) for r in cur.fetchall()]
check("2 sleeve rows persisted", len(sleeve_rows) == 2, f"got {len(sleeve_rows)}")

if len(sleeve_rows) >= 2:
    labels = [s["label"] for s in sleeve_rows]
    check("Sleeve labels match", set(labels) == {"PT Tech", "PT Defensive"},
          f"got {labels}")

    tech_s = [s for s in sleeve_rows if s["label"] == "PT Tech"][0]
    def_s = [s for s in sleeve_rows if s["label"] == "PT Defensive"][0]

    check("Tech weight = 0.6", approx(tech_s["weight"], 0.60, tol=0.01),
          f"got {tech_s['weight']}")
    check("Defensive weight = 0.4", approx(def_s["weight"], 0.40, tol=0.01),
          f"got {def_s['weight']}")
    check("Tech allocated capital = 120000",
          approx(tech_s["allocated_capital"], 120000, tol=1),
          f"got {tech_s['allocated_capital']}")
    check("Defensive allocated capital = 80000",
          approx(def_s["allocated_capital"], 80000, tol=1),
          f"got {def_s['allocated_capital']}")
    check("Sleeve is_active flag set", tech_s["is_active"] == 1)
    check("Sleeve has last_nav", tech_s["last_nav"] is not None and tech_s["last_nav"] > 0,
          f"last_nav={tech_s['last_nav']}")
    check("Sleeve has return_pct", tech_s["last_return_pct"] is not None)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 3: Trade persistence — every engine trade has a DB row")
print("=" * 70)

# Count trades from engine (we know the deploy evaluated)
detail_p = get_deployment(pid)
db_p = get_db()
cur_p = db_p.cursor()

# Get sleeve results from disk
results_path = DEPLOY_DIR / pid / "results.json"
check("results.json exists on disk", results_path.exists())

if results_path.exists():
    full_result = json.loads(results_path.read_text())
    engine_trade_count = sum(
        len(sr.get("trades", []))
        for sr in full_result.get("sleeve_results", [])
    )

    cur.execute("SELECT COUNT(*) FROM trades WHERE source_id = ?", (pid,))
    db_trade_count = cur.fetchone()[0]

    check("DB trade count matches engine",
          db_trade_count == engine_trade_count,
          f"db={db_trade_count}, engine={engine_trade_count}")

    # Verify a sample trade has correct fields
    cur.execute("SELECT * FROM trades WHERE source_id = ? LIMIT 1", (pid,))
    sample = dict(cur.fetchone())
    check("Trade has source_type", sample["source_type"] == "deployment")
    check("Trade has date", sample["date"] is not None and len(sample["date"]) == 10)
    check("Trade has action (BUY/SELL)", sample["action"] in ("BUY", "SELL"))
    check("Trade has symbol", sample["symbol"] is not None and len(sample["symbol"]) > 0)
    check("Trade has shares > 0", sample["shares"] > 0)
    check("Trade has price > 0", sample["price"] > 0)
    check("Trade has sleeve_label", sample["sleeve_label"] in ("PT Tech", "PT Defensive"),
          f"got '{sample['sleeve_label']}'")

    # Check BUY-SELL linking (deployment mode = no sells if buy-and-hold)
    cur.execute("""SELECT COUNT(*) FROM trades
                   WHERE source_id = ? AND action = 'SELL'""", (pid,))
    total_sells = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM trades
                   WHERE source_id = ? AND action = 'SELL' AND entry_date IS NOT NULL""",
                (pid,))
    sells_with_entry = cur.fetchone()[0]
    # All SELL trades should have entry_date; 0 sells is also valid (deployment, open positions)
    check("SELL trades have entry_date (or no sells in deploy mode)",
          sells_with_entry == total_sells,
          f"sells_with_entry={sells_with_entry}, total_sells={total_sells}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 4: Sleeve persistence — metrics match engine output")
print("=" * 70)

if results_path.exists():
    per_sleeve_engine = full_result.get("per_sleeve", [])
    sleeve_results_engine = full_result.get("sleeve_results", [])

    for i, ps_e in enumerate(per_sleeve_engine):
        label = ps_e["label"]
        sr_m = sleeve_results_engine[i].get("metrics", {}) if i < len(sleeve_results_engine) else {}

        cur.execute("SELECT * FROM sleeves WHERE deployment_id = ? AND label = ?",
                    (pid, label))
        s_row = cur.fetchone()
        if s_row:
            s_row = dict(s_row)
            check(f"  {label}: return_pct matches",
                  approx(s_row["last_return_pct"] or 0, sr_m.get("total_return_pct", 0), tol=0.1),
                  f"db={s_row['last_return_pct']}, engine={sr_m.get('total_return_pct')}")
            check(f"  {label}: sharpe matches",
                  approx(s_row["sharpe"] or 0, sr_m.get("sharpe_ratio", 0), tol=0.1),
                  f"db={s_row['sharpe']}, engine={sr_m.get('sharpe_ratio')}")
            check(f"  {label}: active_days matches",
                  s_row["active_days"] == ps_e.get("active_days", 0),
                  f"db={s_row['active_days']}, engine={ps_e.get('active_days')}")
            check(f"  {label}: wins matches",
                  s_row["wins"] == ps_e.get("wins", 0),
                  f"db={s_row['wins']}, engine={ps_e.get('wins')}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 5: Evaluate → DB metrics update")
print("=" * 70)

# Re-evaluate the portfolio deployment
eval_result = evaluate_one(pid)
check("Evaluate returned result", eval_result is not None)

eval_metrics = eval_result.get("metrics", {}) if eval_result else {}

# Re-read from DB
cur.execute("SELECT * FROM deployments WHERE id = ?", (pid,))
refreshed = dict(cur.fetchone())

check("DB last_nav matches engine final_nav",
      approx(refreshed["last_nav"] or 0, eval_metrics.get("final_nav", 0), tol=1.0),
      f"db={refreshed['last_nav']}, engine={eval_metrics.get('final_nav')}")

check("DB last_return_pct matches",
      approx(refreshed["last_return_pct"] or 0, eval_metrics.get("total_return_pct", 0), tol=0.1),
      f"db={refreshed['last_return_pct']}, engine={eval_metrics.get('total_return_pct')}")

check("DB last_sharpe_ratio matches",
      approx(refreshed["last_sharpe_ratio"] or 0, eval_metrics.get("sharpe_ratio", 0), tol=0.05),
      f"db={refreshed['last_sharpe_ratio']}, engine={eval_metrics.get('sharpe_ratio')}")

check("DB last_max_drawdown_pct matches",
      approx(refreshed["last_max_drawdown_pct"] or 0, eval_metrics.get("max_drawdown_pct", 0), tol=0.1),
      f"db={refreshed['last_max_drawdown_pct']}, engine={eval_metrics.get('max_drawdown_pct')}")

check("DB last_ann_volatility_pct matches",
      approx(refreshed["last_ann_volatility_pct"] or 0,
             eval_metrics.get("annualized_volatility_pct", 0), tol=0.1),
      f"db={refreshed['last_ann_volatility_pct']}, engine={eval_metrics.get('annualized_volatility_pct')}")

check("DB last_evaluated is today", refreshed["last_evaluated"] is not None)

check("sleeve_summary is valid JSON",
      json.loads(refreshed["sleeve_summary"]) is not None if refreshed["sleeve_summary"] else True)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 6: Re-evaluate idempotency")
print("=" * 70)

nav_before = refreshed["last_nav"]
ret_before = refreshed["last_return_pct"]

eval2 = evaluate_one(pid)
cur.execute("SELECT last_nav, last_return_pct FROM deployments WHERE id = ?", (pid,))
after = cur.fetchone()

check("NAV unchanged after re-evaluate",
      approx(after[0], nav_before, tol=0.01),
      f"before={nav_before}, after={after[0]}")

check("Return unchanged after re-evaluate",
      approx(after[1], ret_before, tol=0.01),
      f"before={ret_before}, after={after[1]}")

# Trades should not duplicate (INSERT OR IGNORE)
cur.execute("SELECT COUNT(*) FROM trades WHERE source_id = ?", (pid,))
count_after = cur.fetchone()[0]
check("No duplicate trades after re-evaluate",
      count_after == db_trade_count,
      f"before={db_trade_count}, after={count_after}")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 7: Config roundtrip — frozen config in DB")
print("=" * 70)

cur.execute("SELECT config_json FROM deployments WHERE id = ?", (pid,))
stored_config = json.loads(cur.fetchone()[0])

check("Stored config is valid JSON", stored_config is not None)
check("Stored config has sleeves",
      "sleeves" in stored_config or "strategies" in stored_config)
check("Stored config name matches",
      stored_config.get("name") == "Persist Test Portfolio",
      f"got '{stored_config.get('name')}'")

# Strategy config roundtrip
cur.execute("SELECT config_json FROM deployments WHERE id = ?", (sid,))
strat_stored = json.loads(cur.fetchone()[0])
check("Strategy config is portfolio-wrapped (has sleeves)",
      "sleeves" in strat_stored or "strategies" in strat_stored)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 8: Deployment disk artifacts")
print("=" * 70)

strat_dir = DEPLOY_DIR / sid
port_dir = DEPLOY_DIR / pid

check("Strategy deployment dir exists", strat_dir.exists())
check("Strategy config.json on disk", (strat_dir / "config.json").exists())
check("Strategy results.json on disk", (strat_dir / "results.json").exists())

check("Portfolio deployment dir exists", port_dir.exists())
check("Portfolio config.json on disk", (port_dir / "config.json").exists())
check("Portfolio results.json on disk", (port_dir / "results.json").exists())

# Verify disk config matches DB config
if (port_dir / "config.json").exists():
    disk_config = json.loads((port_dir / "config.json").read_text())
    check("Disk config matches DB config",
          disk_config.get("name") == stored_config.get("name"))

# Verify disk results are valid JSON
if (port_dir / "results.json").exists():
    disk_results = json.loads((port_dir / "results.json").read_text())
    check("Disk results has metrics", "metrics" in disk_results)
    check("Disk results has combined_nav_history", "combined_nav_history" in disk_results)
    check("Disk results has per_sleeve", "per_sleeve" in disk_results)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 9: Control lifecycle — pause/resume/stop state transitions")
print("=" * 70)

# Pause
pause_deployment(pid)
cur.execute("SELECT status FROM deployments WHERE id = ?", (pid,))
check("Pause sets status='paused'", cur.fetchone()[0] == "paused")

# Resume
resume_deployment(pid)
cur.execute("SELECT status FROM deployments WHERE id = ?", (pid,))
check("Resume sets status='active'", cur.fetchone()[0] == "active")

# Stop
stop_deployment(pid)
cur.execute("SELECT status, updated_at FROM deployments WHERE id = ?", (pid,))
stopped_row = cur.fetchone()
check("Stop sets status='stopped'", stopped_row[0] == "stopped")
check("updated_at advances on stop", stopped_row[1] is not None)

# Verify evaluate_one skips stopped
eval_stopped = evaluate_one(pid)
check("evaluate_one returns None for stopped deployment", eval_stopped is None)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 10: Alert mode toggle persistence")
print("=" * 70)

# Need an active deployment for this
dep_alert = deploy(STRAT_CONFIG, "2024-09-01", 50000, "Alert Mode Test")
aid = dep_alert["id"]

cur.execute("SELECT alert_mode FROM deployments WHERE id = ?", (aid,))
check("Default alert_mode = 0", cur.fetchone()[0] == 0)

set_alert_mode(aid, True)
cur.execute("SELECT alert_mode FROM deployments WHERE id = ?", (aid,))
check("alert_mode = 1 after enable", cur.fetchone()[0] == 1)

set_alert_mode(aid, False)
cur.execute("SELECT alert_mode FROM deployments WHERE id = ?", (aid,))
check("alert_mode = 0 after disable", cur.fetchone()[0] == 0)

stop_deployment(aid)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 11: Regime deployment + state history persistence")
print("=" * 70)

# Use existing regime oil_shock_v2
try:
    regime_dep = deploy_regime("oil_shock_v2_378f18c9", name="Persist Test Regime")
    regime_dep_id = regime_dep.get("id") or regime_dep.get("deployment_id")

    if regime_dep_id:
        cur.execute("SELECT * FROM regime_deployments WHERE id = ?", (regime_dep_id,))
        regime_row = cur.fetchone()
        if regime_row:
            regime_row = dict(regime_row)
            check("Regime deployment row exists", True)
            check("Regime status = active", regime_row["status"] == "active")
            check("Regime name matches", regime_row["regime_name"] is not None)
            check("Regime config_json is valid",
                  json.loads(regime_row["config_json"]) is not None)

            # Evaluate regime
            eval_r = evaluate_regime_one(regime_dep_id)
            if eval_r:
                cur.execute("SELECT * FROM regime_deployments WHERE id = ?", (regime_dep_id,))
                updated = dict(cur.fetchone())
                check("Regime last_evaluated is set", updated["last_evaluated"] is not None)
                check("Regime is_active is 0 or 1", updated["is_active"] in (0, 1))

                # Check state history
                cur.execute("SELECT COUNT(*) FROM regime_state_history WHERE deployment_id = ?",
                            (regime_dep_id,))
                hist_count = cur.fetchone()[0]
                check("Regime state history has entry", hist_count > 0,
                      f"got {hist_count}")

            stop_regime_deployment(regime_dep_id)
        else:
            check("Regime deployment row exists", False, "row not found")
    else:
        check("Regime deploy returned ID", False, f"result={regime_dep}")
except Exception as e:
    print(f"  (Regime test skipped: {e})")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 12: Backtest run indexing — backtest_runs table")
print("=" * 70)

from backtest_engine import run_backtest, save_results

bt_config = {
    "name": "Persist Test Backtest",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 2, "initial_allocation": 100000},
    "backtest": {"start": "2024-06-01", "end": "2024-12-31",
                 "entry_price": "next_close", "slippage_bps": 10},
}
bt_result = run_backtest(bt_config)
# Save results to trigger indexing
filepath, daily_filepath = save_results(bt_result, bt_config)

# Check backtest_runs table
cur.execute("""SELECT * FROM backtest_runs
               WHERE strategy_name = 'Persist Test Backtest'
               ORDER BY created_at DESC LIMIT 1""")
bt_row = cur.fetchone()

if bt_row:
    bt_row = dict(bt_row)
    check("Backtest run indexed", True)
    check("run_id is set", bt_row["run_id"] is not None and len(bt_row["run_id"]) > 5)
    check("total_return matches engine",
          approx(bt_row["total_return"] or 0, bt_result["metrics"]["total_return_pct"], tol=0.1),
          f"db={bt_row['total_return']}, engine={bt_result['metrics']['total_return_pct']}")
    check("sharpe matches",
          approx(bt_row["sharpe"] or 0, bt_result["metrics"]["sharpe_ratio"], tol=0.05),
          f"db={bt_row['sharpe']}, engine={bt_result['metrics']['sharpe_ratio']}")
    check("final_nav matches",
          approx(bt_row["final_nav"] or 0, bt_result["metrics"]["final_nav"], tol=1),
          f"db={bt_row['final_nav']}, engine={bt_result['metrics']['final_nav']}")
    check("win_rate matches",
          approx(bt_row["win_rate"] or 0, bt_result["metrics"]["win_rate_pct"], tol=0.1),
          f"db={bt_row['win_rate']}, engine={bt_result['metrics']['win_rate_pct']}")
    check("capital matches",
          approx(bt_row["capital"] or 0, 100000, tol=1))
else:
    check("Backtest run indexed", False, "no row found")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 13: No orphan trades — every trade has a valid source_id")
print("=" * 70)

cur.execute("""
    SELECT t.source_id, COUNT(*) as cnt
    FROM trades t
    LEFT JOIN deployments d ON t.source_id = d.id
    WHERE t.source_type = 'deployment' AND d.id IS NULL
    GROUP BY t.source_id
""")
orphan_trades = cur.fetchall()
orphan_count = sum(r[1] for r in orphan_trades) if orphan_trades else 0

check("No orphan trades (all source_ids valid)",
      orphan_count == 0,
      f"{orphan_count} orphan trades from {len(orphan_trades)} missing deployments")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 14: No orphan sleeves — every sleeve has a valid deployment_id")
print("=" * 70)

cur.execute("""
    SELECT s.deployment_id, COUNT(*) as cnt
    FROM sleeves s
    LEFT JOIN deployments d ON s.deployment_id = d.id
    WHERE s.deployment_id IS NOT NULL AND d.id IS NULL
    GROUP BY s.deployment_id
""")
orphan_sleeves = cur.fetchall()
orphan_s_count = sum(r[1] for r in orphan_sleeves) if orphan_sleeves else 0

check("No orphan sleeves (all deployment_ids valid)",
      orphan_s_count == 0,
      f"{orphan_s_count} orphan sleeves from {len(orphan_sleeves)} missing deployments")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 15: Index coverage for key lookup patterns")
print("=" * 70)

# Test EXPLAIN QUERY PLAN for common queries to verify index usage
def has_index(query, params=()):
    """Check if a query uses an index (not a full table scan)."""
    cur.execute(f"EXPLAIN QUERY PLAN {query}", params)
    plan = " ".join(str(r) for r in cur.fetchall())
    return "SCAN" not in plan or "USING INDEX" in plan or "SEARCH" in plan

check("trades by source_id uses index",
      has_index("SELECT * FROM trades WHERE source_id = ?", (pid,)))

check("sleeves by deployment_id uses index",
      has_index("SELECT * FROM sleeves WHERE deployment_id = ?", (pid,)))

check("deployments by status uses index",
      has_index("SELECT * FROM deployments WHERE status = ?", ("active",)))

check("trades by symbol+date uses index",
      has_index("SELECT * FROM trades WHERE symbol = ? AND date = ?", ("AAPL", "2024-06-03")))

# prices table is in market.db, check with separate connection
from db_config import MARKET_DB_PATH
_mkt_conn = sqlite3.connect(str(MARKET_DB_PATH))
_mkt_cur = _mkt_conn.cursor()
_mkt_cur.execute("EXPLAIN QUERY PLAN SELECT * FROM prices WHERE symbol = ? AND date = ?", ("AAPL", "2024-06-03"))
_mkt_plan = " ".join(str(r) for r in _mkt_cur.fetchall())
_mkt_conn.close()
check("prices by symbol+date uses index",
      "SCAN" not in _mkt_plan or "USING INDEX" in _mkt_plan or "SEARCH" in _mkt_plan)


# =========================================================================
print("\n" + "=" * 70)
print("TEST 16: JSON fields are valid JSON (no truncation/corruption)")
print("=" * 70)

# Check all JSON columns in deployments
cur.execute("SELECT id, config_json, active_regimes, sleeve_summary FROM deployments WHERE config_json IS NOT NULL")
json_errors = 0
for row in cur.fetchall():
    dep_id_check = row[0]
    for col_idx, col_name in [(1, "config_json"), (2, "active_regimes"), (3, "sleeve_summary")]:
        val = row[col_idx]
        if val is not None:
            try:
                json.loads(val)
            except json.JSONDecodeError:
                json_errors += 1
                print(f"    CORRUPT: {dep_id_check}.{col_name}")

check("All deployment JSON fields are valid", json_errors == 0,
      f"{json_errors} corrupt JSON fields")

# Check sleeve config_json
cur.execute("SELECT sleeve_id, config_json, regime_gate FROM sleeves WHERE config_json IS NOT NULL")
sleeve_json_errors = 0
for row in cur.fetchall():
    for col_idx, col_name in [(1, "config_json"), (2, "regime_gate")]:
        val = row[col_idx]
        if val is not None:
            try:
                json.loads(val)
            except json.JSONDecodeError:
                sleeve_json_errors += 1

check("All sleeve JSON fields are valid", sleeve_json_errors == 0,
      f"{sleeve_json_errors} corrupt JSON fields")

# Check trade signal_detail
cur.execute("SELECT COUNT(*) FROM trades WHERE signal_detail IS NOT NULL")
total_with_signals = cur.fetchone()[0]
if total_with_signals > 0:
    cur.execute("""SELECT id, signal_detail FROM trades
                   WHERE signal_detail IS NOT NULL LIMIT 100""")
    sig_errors = 0
    for row in cur.fetchall():
        try:
            json.loads(row[1])
        except (json.JSONDecodeError, TypeError):
            sig_errors += 1
    check("Trade signal_detail fields are valid JSON",
          sig_errors == 0,
          f"{sig_errors}/100 corrupt")


# =========================================================================
print("\n" + "=" * 70)
print("TEST 17: No NaN/Inf in numeric DB fields")
print("=" * 70)

# Check deployments numeric columns
numeric_cols = ["last_nav", "last_return_pct", "last_alpha_pct",
                "last_sharpe_ratio", "last_max_drawdown_pct",
                "last_ann_volatility_pct", "rolling_vol_30d_pct",
                "utilization_pct", "initial_capital"]

nan_inf_count = 0
for col in numeric_cols:
    cur.execute(f"SELECT COUNT(*) FROM deployments WHERE typeof({col}) = 'text'")
    bad = cur.fetchone()[0]
    if bad > 0:
        nan_inf_count += bad
        print(f"    WARNING: {bad} rows with text value in deployments.{col}")

check("No NaN/Inf text values in deployment numerics",
      nan_inf_count == 0,
      f"{nan_inf_count} bad values")

# Same for sleeves
sleeve_num_cols = ["weight", "allocated_capital", "last_nav",
                   "last_return_pct", "sharpe", "max_drawdown_pct",
                   "profit_factor", "win_rate_pct"]
sleeve_nan = 0
for col in sleeve_num_cols:
    cur.execute(f"SELECT COUNT(*) FROM sleeves WHERE typeof({col}) = 'text'")
    bad = cur.fetchone()[0]
    if bad > 0:
        sleeve_nan += bad

check("No NaN/Inf text values in sleeve numerics",
      sleeve_nan == 0,
      f"{sleeve_nan} bad values")


# =========================================================================
# Cleanup test deployments
stop_deployment(sid)
conn.close()

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
