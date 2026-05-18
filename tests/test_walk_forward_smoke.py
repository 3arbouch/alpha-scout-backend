#!/usr/bin/env python3
"""
Integration smoke test for run_backtest's walk-forward eval path.

Runs a tiny portfolio (5 tech tickers, always-buy entry, ~6 months training,
1 year eval split into 2 windows). Validates:

  1. Legacy flat-arg call (no eval) still returns today's shape.
  2. New BacktestConfig call without eval matches legacy shape.
  3. New BacktestConfig call WITH eval returns the new shape:
     - training-period metrics + sleeve_trades still present
     - eval.windows list populated, each with metrics + sleeve_trades
     - eval.aggregated has per-metric {mean, median, min, max, p25, count}
     - eval.spec echoes the window/overlap config

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_walk_forward_smoke.py

Requires market.db with prices for at least AAPL, MSFT, GOOGL covering
2023-01-01 through 2024-12-31.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "auto_trader"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from server.models.backtest import BacktestConfig, EvalBlock, WindowSpec  # noqa: E402
from runner import run_backtest  # noqa: E402


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


def make_portfolio():
    """Minimal portfolio: 3 tech tickers, always-buy, 30-day time stop, top-3 momentum."""
    SYMBOLS = ["AAPL", "MSFT", "GOOGL"]
    return {
        "name": "WFSmoke",
        "sleeves": [{
            "label": "Tech", "weight": 1.0, "regime_gate": ["*"],
            "strategy_config": {
                "name": "wf_smoke",
                "universe": {"type": "symbols", "symbols": SYMBOLS},
                "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                "stop_loss": {"type": "drawdown_from_entry", "value": -25,
                              "cooldown_days": 30},
                "time_stop": {"max_days": 90},
                "ranking": {"by": "momentum_rank", "order": "desc", "top_n": 3},
                "rebalancing": {"frequency": "none", "rules": {}},
                "sizing": {"type": "equal_weight", "max_positions": 3,
                            "initial_allocation": 50_000},
            },
        }],
        "regime_filter": False,
        "capital_when_gated_off": "to_cash",
    }


PORTFOLIO = make_portfolio()


# ---------- 1. Legacy flat args (no eval) ----------
print("\n=== 1. Legacy flat-arg call (no eval) ===")
legacy = run_backtest(PORTFOLIO, start="2023-01-01", end="2023-06-30",
                      capital=50_000)
check("legacy returns dict", isinstance(legacy, dict),
      f"got {type(legacy)}")
if isinstance(legacy, dict):
    check("legacy has metrics", "metrics" in legacy)
    check("legacy has sleeve_trades", "sleeve_trades" in legacy)
    check("legacy has NO eval block (no eval configured)", "eval" not in legacy)
    check("legacy metrics has sharpe_ratio",
          "sharpe_ratio" in legacy["metrics"])


# ---------- 2. BacktestConfig without eval ----------
print("\n=== 2. BacktestConfig without eval ===")
cfg_no_eval = BacktestConfig(
    training_start="2023-01-01", training_end="2023-06-30",
    initial_capital=50_000,
)
res_no_eval = run_backtest(PORTFOLIO, config=cfg_no_eval)
check("config-no-eval returns dict", isinstance(res_no_eval, dict))
if isinstance(res_no_eval, dict):
    check("config-no-eval has metrics", "metrics" in res_no_eval)
    check("config-no-eval has sleeve_trades", "sleeve_trades" in res_no_eval)
    check("config-no-eval has NO eval block", "eval" not in res_no_eval)
    # Sanity: metrics should match the legacy call (same period, same config).
    if isinstance(legacy, dict):
        legacy_sharpe = legacy["metrics"].get("sharpe_ratio")
        new_sharpe = res_no_eval["metrics"].get("sharpe_ratio")
        check("legacy and config-no-eval produce same Sharpe",
              legacy_sharpe == new_sharpe,
              f"legacy={legacy_sharpe} new={new_sharpe}")


# ---------- 3. BacktestConfig WITH eval ----------
print("\n=== 3. BacktestConfig WITH eval (2 windows, contiguous 6m) ===")
cfg_with_eval = BacktestConfig(
    training_start="2023-01-01", training_end="2023-06-30",
    initial_capital=50_000,
    eval=EvalBlock(
        # end=2024-07-01 (inclusive) — gives 2 contiguous 6m windows; 2024-06-30
        # would drop the second window since 2024-07-01 > 2024-06-30.
        start="2023-07-01", end="2024-07-01",
        spec=WindowSpec(window="6m", overlap="0d"),
    ),
)
res_eval = run_backtest(PORTFOLIO, config=cfg_with_eval)
check("eval-call returns dict", isinstance(res_eval, dict))
if isinstance(res_eval, dict):
    check("training metrics still present", "metrics" in res_eval)
    check("training sleeve_trades still present", "sleeve_trades" in res_eval)
    check("eval block present", "eval" in res_eval)

if isinstance(res_eval, dict) and "eval" in res_eval:
    ev = res_eval["eval"]
    check("eval has windows list", isinstance(ev.get("windows"), list))
    check("eval has aggregated dict", isinstance(ev.get("aggregated"), dict))
    check("eval has spec echo", ev.get("spec") == {"window": "6m", "overlap": "0d"})
    check("eval generated 2 windows (Jul-Dec 2023, Jan-Jun 2024)",
          len(ev["windows"]) == 2, f"got {len(ev['windows'])}")

    if len(ev["windows"]) >= 1:
        w0 = ev["windows"][0]
        check("window 0 has label/start/end",
              all(k in w0 for k in ("label", "start", "end")))
        check("window 0 has metrics dict",
              isinstance(w0.get("metrics"), dict))
        check("window 0 has sleeve_trades list",
              isinstance(w0.get("sleeve_trades"), list))
        check("window 0 starts 2023-07-01", w0["start"] == "2023-07-01")
        check("window 0 ends 2024-01-01", w0["end"] == "2024-01-01")

    if len(ev["windows"]) >= 2:
        w1 = ev["windows"][1]
        check("window 1 starts where window 0 ended (contiguous)",
              w1["start"] == ev["windows"][0]["end"])
        check("window 1 ends 2024-07-01", w1["end"] == "2024-07-01")

    # Aggregated should contain at least sharpe and total_return across windows.
    agg = ev.get("aggregated", {})
    if "sharpe_ratio" in agg:
        sb = agg["sharpe_ratio"]
        check("aggregated sharpe has all 6 fields",
              all(k in sb for k in ("mean", "median", "min", "max", "p25", "count")))
        check("aggregated sharpe count == windows count",
              sb["count"] == len(ev["windows"]))
        check("aggregated sharpe min <= median <= max",
              sb["min"] <= sb["median"] <= sb["max"],
              f"min={sb['min']} med={sb['median']} max={sb['max']}")


# ---------- 4. BacktestConfig with eval whose window > span → zero windows ----------
print("\n=== 4. Eval whose window > eval span (graceful empty) ===")
cfg_too_big = BacktestConfig(
    training_start="2023-01-01", training_end="2023-06-30",
    initial_capital=50_000,
    eval=EvalBlock(
        start="2023-07-01", end="2023-12-31",
        spec=WindowSpec(window="2y", overlap="0d"),
    ),
)
res_empty = run_backtest(PORTFOLIO, config=cfg_too_big)
check("oversized window returns dict", isinstance(res_empty, dict))
if isinstance(res_empty, dict):
    check("training metrics still present", "metrics" in res_empty)
    check("eval block present with empty windows",
          res_empty.get("eval", {}).get("windows") == [])
    check("eval aggregated empty",
          res_empty.get("eval", {}).get("aggregated") == {})


print(f"\n{'=' * 50}\n{PASS} passed, {FAIL} failed\n{'=' * 50}")
sys.exit(0 if FAIL == 0 else 1)
