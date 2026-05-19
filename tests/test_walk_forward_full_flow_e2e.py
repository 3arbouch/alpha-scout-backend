#!/usr/bin/env python3
"""
Full data-flow E2E for walk-forward eval — no LLM required.

Simulates one iteration of an agent run:
  1. Build a BacktestConfig with training + eval (2 windows).
  2. Run run_backtest → get training metrics + eval block.
  3. Resolve target_value via _resolve_target_value(median).
  4. Persist as an experiment row (with eval_metrics_json + target_aggregator).
  5. Persist training-period and eval-window trades with correct window_label.
  6. Render build_history_context — verify Eval line appears with the right
     min/med/max stats.
  7. Call get_experiment_stats/get_experiment_trades with window filters and
     verify the partition is correct (training + win_a + win_b == total).

This is the single most-load-bearing assertion: the agent on iteration N+1
sees the Eval line from iteration N's experiment, and can drill into each
window through the tools.

Run: python3 tests/test_walk_forward_full_flow_e2e.py

Requires market.db with prices for AAPL/MSFT/GOOGL covering 2023-01-01 to
2024-12-31. MARKET_DB_PATH env var to point at dev market_dev.db.
"""
import asyncio
import json
import os
import sys
import tempfile

TMP_DB = tempfile.NamedTemporaryFile(suffix="_e2e_full.db", delete=False)
TMP_DB.close()
os.environ["APP_DB_PATH"] = TMP_DB.name

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "auto_trader"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import importlib
import auto_trader.schema as _aschema
importlib.reload(_aschema)

from auto_trader.schema import get_db, log_experiment  # noqa: E402
from deploy_engine import persist_trades  # noqa: E402
from server.models.backtest import BacktestConfig, EvalBlock, WindowSpec  # noqa: E402
from runner import run_backtest, _resolve_target_value, build_history_context  # noqa: E402
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


# ---------- 1. Build BacktestConfig + portfolio ----------
print("\n1. Build config:")
cfg = BacktestConfig(
    training_start="2023-01-01", training_end="2023-06-30",
    initial_capital=50_000,
    eval=EvalBlock(
        start="2023-07-01", end="2024-07-01",
        spec=WindowSpec(window="6m", overlap="0d"),
    ),
)
check("config built", cfg.eval is not None)

PORTFOLIO = {
    "name": "E2EFullFlow",
    "sleeves": [{
        "label": "Tech", "weight": 1.0, "regime_gate": ["*"],
        "strategy_config": {
            "name": "e2e_full",
            "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "GOOGL"]},
            "entry": {"conditions": [{"type": "always"}], "logic": "all"},
            "stop_loss": {"type": "drawdown_from_entry", "value": -25, "cooldown_days": 30},
            "time_stop": {"max_days": 90},
            "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 3},
            "rebalancing": {"frequency": "none", "rules": {}},
            "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 50_000},
        },
    }],
    "regime_filter": False, "capital_when_gated_off": "to_cash",
}


# ---------- 2. Run backtest ----------
print("\n2. Run backtest (1 training + 2 eval windows):")
bt = run_backtest(PORTFOLIO, config=cfg)
check("backtest returned dict", isinstance(bt, dict))
check("training metrics present", isinstance(bt.get("metrics"), dict))
check("eval block present", "eval" in bt and isinstance(bt["eval"], dict))
check("eval has 2 windows", len(bt["eval"]["windows"]) == 2,
      f"got {len(bt['eval']['windows'])}")
check("eval aggregated has sharpe",
      "sharpe_ratio" in bt["eval"]["aggregated"])


# ---------- 3. Resolve target via median aggregator ----------
print("\n3. Resolve target_value (median sharpe across eval):")
tv = _resolve_target_value(bt["metrics"], bt["eval"]["aggregated"],
                            "sharpe_ratio", "median")
check("target_value is numeric", isinstance(tv, (int, float)))
check("target_value matches aggregated.median",
      tv == bt["eval"]["aggregated"]["sharpe_ratio"]["median"])


# ---------- 4. Persist experiment row ----------
print("\n4. Persist experiment + trades:")
eval_compact = {
    "spec": bt["eval"]["spec"],
    "aggregated": bt["eval"]["aggregated"],
    "windows": [
        {"label": w["label"], "start": w["start"], "end": w["end"], "metrics": w["metrics"]}
        for w in bt["eval"]["windows"]
    ],
}
exp_id = log_experiment(
    run_id="e2erun",
    iteration=1,
    thesis="Hard-coded test thesis: see if data flows through the full path.",
    assumptions=["assumes the universe is non-empty"],
    portfolio_config=PORTFOLIO,
    metrics=bt["metrics"],
    target_metric="sharpe_ratio",
    target_value=tv,
    conditions=[],
    conditions_met=True,
    decision="keep",
    best_value_so_far=0,
    backtest_start=cfg.training_start, backtest_end=cfg.training_end,
    initial_capital=cfg.initial_capital,
    eval_metrics_json=json.dumps(eval_compact),
    target_aggregator="median",
)
check("experiment row persisted", isinstance(exp_id, str) and len(exp_id) >= 8,
      f"got {exp_id}")

# Persist training trades and eval-window trades.
total_persisted = 0
for sleeve in bt["sleeve_trades"]:
    if sleeve["trades"]:
        total_persisted += persist_trades("experiment", exp_id, sleeve["trades"],
                                          sleeve_label=sleeve["label"])
for w in bt["eval"]["windows"]:
    for sleeve in w.get("sleeve_trades", []):
        if sleeve["trades"]:
            total_persisted += persist_trades("experiment", exp_id, sleeve["trades"],
                                              sleeve_label=sleeve["label"],
                                              window_label=w["label"])
print(f"  Total trades persisted: {total_persisted}")
check("at least 1 trade persisted (sanity)", total_persisted >= 1,
      f"got {total_persisted}")


# ---------- 5. Verify DB state ----------
print("\n5. DB state:")
conn = get_db()
row = conn.execute("SELECT eval_metrics_json, target_aggregator, target_value FROM experiments WHERE id = ?",
                   (exp_id,)).fetchone()
check("eval_metrics_json saved",       row[0] is not None)
check("target_aggregator='median' saved", row[1] == "median")
check("target_value saved",            row[2] is not None and abs(row[2] - tv) < 1e-9)

n_training = conn.execute(
    "SELECT COUNT(*) FROM trades WHERE source_id=? AND window_label IS NULL", (exp_id,)
).fetchone()[0]
n_wf = conn.execute(
    "SELECT COUNT(*) FROM trades WHERE source_id=? AND window_label IS NOT NULL", (exp_id,)
).fetchone()[0]
n_all = conn.execute(
    "SELECT COUNT(*) FROM trades WHERE source_id=?", (exp_id,)
).fetchone()[0]
print(f"  training={n_training}, eval-window={n_wf}, total={n_all}")
check("training + eval-window == total trades",
      n_training + n_wf == n_all)
conn.close()


# ---------- 6. History render ----------
print("\n6. build_history_context:")
rendered = build_history_context("e2erun", "sharpe_ratio")
check("rendered contains experiment",  "Experiment 1" in rendered)
check("rendered contains Eval line",   "**Eval (2 windows, 6m/0d):**" in rendered,
      "missing Eval line; rendered=\n" + rendered)
check("rendered contains target=median(...)",
      "target=median(sharpe_ratio)=" in rendered)
print("\n--- RENDERED ---")
print(rendered[:1500])
print("--- ... ---\n")


# ---------- 7. Tool round-trips ----------
print("\n7. Tool round-trips with window filter:")


def call_stats(window=None):
    handler = tools_mod.get_experiment_stats_tool.handler
    args = {"experiment_id": exp_id}
    if window:
        args["window"] = window
    res = asyncio.get_event_loop().run_until_complete(handler(args))
    return json.loads(res["content"][0]["text"])


def call_trades(window=None):
    handler = tools_mod.get_experiment_trades_tool.handler
    args = {"experiment_id": exp_id}
    if window:
        args["window"] = window
    res = asyncio.get_event_loop().run_until_complete(handler(args))
    return json.loads(res["content"][0]["text"])


all_stats = call_stats()
training_stats = call_stats(window="training")
check("available_windows discovered (2 windows)",
      len(all_stats["available_windows"]) == 2,
      f"got {all_stats['available_windows']}")
check("training trade count matches DB",
      training_stats["totals"]["total_trades"] == n_training,
      f"stats={training_stats['totals']['total_trades']} db={n_training}")

# Sum invariant via the tool: training + each window = all
tt = call_trades(window="training")["trade_count"]
window_labels = all_stats["available_windows"]
per_window_counts = [call_trades(window=w)["trade_count"] for w in window_labels]
total_tool = call_trades()["trade_count"]
check("tool sum invariant: training + sum(windows) == total",
      tt + sum(per_window_counts) == total_tool,
      f"{tt}+sum({per_window_counts}) vs {total_tool}")


os.unlink(TMP_DB.name)
print(f"\n{'=' * 50}\n{PASS} passed, {FAIL} failed\n{'=' * 50}")
sys.exit(0 if FAIL == 0 else 1)
