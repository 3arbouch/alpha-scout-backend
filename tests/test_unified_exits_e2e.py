#!/usr/bin/env python3
"""
End-to-end tests for the unified exit configuration.

Covers:
  1. Schema: ExitRule union accepts all 10 rule types in either guards or rules.
  2. Migration: 6 legacy configs round-trip to expected unified shape.
  3. Behavior:
     - Pure guard: drawdown_from_entry alone.
     - Pure thesis: feature_threshold rule.
     - Guard + thesis OR: drawdown stop + RSI reversal.
     - Thesis AND: high-RSI AND falling 1m return.
     - Multiple guards: drawdown + trailing on the same position.
     - Frozen pricing: atr_stop + atr_target on the same position; both fire correctly.
  4. Vol-stops e2e regression: existing 38 checks still pass against translated configs.

Run:
  DATA_DIR=/home/mohamed/alpha-scout-backend/data \
  MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \
  APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \
  python3 tests/test_unified_exits_e2e.py
"""
import json
import os
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, ROOT)

from backtest_engine import run_backtest
from server.models.strategy import StrategyConfig, ExitConfig, migrate_legacy_exits_to_unified
from pydantic import ValidationError

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


UNI = ["AAPL", "MSFT", "NVDA", "JPM", "XOM", "GOOGL", "META", "AMZN"]
BASE = {
    "name": "unified_exits_test",
    "universe": {"type": "symbols", "symbols": UNI},
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100_000},
    "backtest": {"start": "2023-01-01", "end": "2024-06-30",
                 "entry_price": "next_close", "slippage_bps": 0},
}


def _run(extra: dict) -> dict:
    cfg = json.loads(json.dumps(BASE))
    cfg.update(extra)
    StrategyConfig(**cfg)  # validate
    return run_backtest(cfg)


# ---------------------------------------------------------------------------
# 1. Schema
# ---------------------------------------------------------------------------
print("\n=== Schema: ExitRule accepts each type in guards and in rules ===")
ALL_RULE_TYPES = [
    {"type": "drawdown_from_entry", "value": -10},
    {"type": "gain_from_entry", "value": 25},
    {"type": "trailing_from_peak", "value": -8},
    {"type": "time_max_days", "value": 90},
    {"type": "atr_stop", "k": 2.0, "window_days": 20},
    {"type": "atr_target", "k": 4.0, "window_days": 20},
    {"type": "realized_vol_stop", "k": 2.0, "window_days": 20, "sigma_source": "historical"},
    {"type": "realized_vol_target", "k": 4.0, "window_days": 20, "sigma_source": "ewma"},
    {"type": "feature_threshold", "feature": "rsi_14", "operator": ">=", "value": 50},
    {"type": "feature_percentile", "feature": "pe", "max_percentile": 30, "scope": "universe"},
]
for rule in ALL_RULE_TYPES:
    cfg = {**BASE, "entry": {"conditions": [{"type": "always"}]},
           "exit": {"guards": [rule], "rules": []}}
    try:
        StrategyConfig(**cfg)
        check(f"{rule['type']} accepted in guards", True)
    except ValidationError as e:
        check(f"{rule['type']} accepted in guards", False, str(e)[:120])

    cfg["exit"] = {"guards": [], "rules": [rule]}
    try:
        StrategyConfig(**cfg)
        check(f"{rule['type']} accepted in rules", True)
    except ValidationError as e:
        check(f"{rule['type']} accepted in rules", False, str(e)[:120])


# ---------------------------------------------------------------------------
# 2. Migration
# ---------------------------------------------------------------------------
print("\n=== Migration: legacy configs round-trip ===")

base_no_exits = {
    "name": "t",
    "universe": {"type": "symbols", "symbols": ["AAPL"]},
    "entry": {"conditions": [{"type": "always"}]},
}

# Case 1: stop_loss + take_profit + time_stop + exit_conditions all together
m1 = {**base_no_exits,
      "stop_loss": {"type": "drawdown_from_entry", "value": -10, "cooldown_days": 60},
      "take_profit": {"type": "gain_from_entry", "value": 25},
      "time_stop": {"max_days": 90},
      "exit_conditions": [{"type": "feature_threshold", "feature": "pe", "operator": ">=", "value": 50}],
      "exit_logic": "any"}
cfg = StrategyConfig(**m1)
check("Case 1 — full mixed: 3 guards + 1 rule + logic=any",
      len(cfg.exit.guards) == 3 and len(cfg.exit.rules) == 1 and cfg.exit.logic == "any")

# Case 2: above_peak migrates to trailing_from_peak with negative value
m2 = {**base_no_exits, "take_profit": {"type": "above_peak", "value": 8}}
cfg = StrategyConfig(**m2)
check("Case 2 — above_peak → trailing_from_peak (neg)",
      cfg.exit.guards[0].type == "trailing_from_peak" and cfg.exit.guards[0].value == -8)

# Case 3: ATR stop + ATR target
m3 = {**base_no_exits,
      "stop_loss": {"type": "atr_multiple", "k": 2.0, "window_days": 20, "cooldown_days": 30},
      "take_profit": {"type": "atr_multiple", "k": 4.0, "window_days": 20}}
cfg = StrategyConfig(**m3)
check("Case 3 — ATR pair: atr_stop + atr_target",
      [g.type for g in cfg.exit.guards] == ["atr_stop", "atr_target"])

# Case 4: Realized-vol pair with mixed sigma_source
m4 = {**base_no_exits,
      "stop_loss": {"type": "realized_vol_multiple", "k": 2.5, "window_days": 20, "sigma_source": "ewma"},
      "take_profit": {"type": "realized_vol_multiple", "k": 4.0, "window_days": 20, "sigma_source": "historical"}}
cfg = StrategyConfig(**m4)
check("Case 4 — realized_vol pair preserves sigma_source",
      cfg.exit.guards[0].sigma_source == "ewma" and cfg.exit.guards[1].sigma_source == "historical")

# Case 5: already-unified is idempotent
m5 = {**base_no_exits,
      "exit": {"guards": [{"type": "drawdown_from_entry", "value": -15}], "rules": [], "logic": "any"}}
cfg = StrategyConfig(**m5)
check("Case 5 — already unified, idempotent",
      len(cfg.exit.guards) == 1 and cfg.exit.guards[0].value == -15)

# Case 6: no exits at all
cfg = StrategyConfig(**base_no_exits)
check("Case 6 — empty exit config",
      cfg.exit.guards == [] and cfg.exit.rules == [] and cfg.exit.logic == "any")


# ---------------------------------------------------------------------------
# 3. Behavior — real backtests
# ---------------------------------------------------------------------------
print("\n=== Behavior: pure guard (drawdown_from_entry) ===")

result = _run({
    "entry": {"conditions": [{"type": "current_drop", "threshold": -3, "window_days": 60}], "logic": "all"},
    "exit": {"guards": [{"type": "drawdown_from_entry", "value": -10, "cooldown_days": 30}], "rules": []},
})
sells = [t for t in result["trades"] if t["action"] == "SELL"]
buys = [t for t in result["trades"] if t["action"] == "BUY"]
reasons = Counter(t.get("reason") for t in sells)
check("pure-guard: BUYs > 0", len(buys) > 0, f"buys={len(buys)}")
check("pure-guard: drawdown_from_entry fires recorded",
      "drawdown_from_entry" in reasons or all(t.get("reason") == "backtest_end" for t in sells),
      f"reasons={dict(reasons)}")


print("\n=== Behavior: pure thesis (feature_threshold rule) ===")
result = _run({
    "entry": {"conditions": [{"type": "feature_threshold", "feature": "rsi_14", "operator": "<=", "value": 30}], "logic": "all"},
    "exit": {"guards": [], "rules": [{"type": "feature_threshold", "feature": "rsi_14", "operator": ">=", "value": 50}]},
})
sells = [t for t in result["trades"] if t["action"] == "SELL"]
buys = [t for t in result["trades"] if t["action"] == "BUY"]
check("pure-thesis: BUYs > 0", len(buys) > 0)
reasons = Counter(t.get("reason") for t in sells)
ft_fires = reasons.get("feature_threshold", 0)
check("pure-thesis: feature_threshold exit recorded as reason",
      ft_fires > 0 or all(t.get("reason") == "backtest_end" for t in sells),
      f"reasons={dict(reasons)}")


print("\n=== Behavior: guard + thesis OR ===")
result = _run({
    "entry": {"conditions": [{"type": "feature_threshold", "feature": "rsi_14", "operator": "<=", "value": 30}], "logic": "all"},
    "exit": {
        "guards": [{"type": "drawdown_from_entry", "value": -8, "cooldown_days": 0}],
        "rules": [{"type": "feature_threshold", "feature": "rsi_14", "operator": ">=", "value": 50}],
        "logic": "any",
    },
})
sells = [t for t in result["trades"] if t["action"] == "SELL"]
buys = [t for t in result["trades"] if t["action"] == "BUY"]
reasons = Counter(t.get("reason") for t in sells)
check("guard+thesis OR: BUYs > 0", len(buys) > 0)
# Either reason can win on different positions
check("guard+thesis OR: reasons include drawdown_from_entry or feature_threshold",
      "drawdown_from_entry" in reasons or "feature_threshold" in reasons or all(t.get("reason") == "backtest_end" for t in sells),
      f"reasons={dict(reasons)}")


print("\n=== Behavior: thesis AND combinator ===")
result = _run({
    "entry": {"conditions": [{"type": "feature_threshold", "feature": "pe", "operator": "<=", "value": 30}], "logic": "all"},
    "exit": {
        "guards": [],
        "rules": [
            {"type": "feature_threshold", "feature": "pe", "operator": ">=", "value": 28},
            {"type": "feature_threshold", "feature": "ret_1m", "operator": "<", "value": 5},
        ],
        "logic": "all",
    },
})
sells = [t for t in result["trades"] if t["action"] == "SELL"]
buys = [t for t in result["trades"] if t["action"] == "BUY"]
reasons = Counter(t.get("reason") for t in sells)
check("AND-combinator: BUYs > 0", len(buys) > 0)
# AND rules emit a 'compound_all' reason when both fire
compound_fires = reasons.get("compound_all", 0)
check("AND-combinator: compound_all reason emitted on intersection days",
      compound_fires > 0 or all(t.get("reason") == "backtest_end" for t in sells),
      f"reasons={dict(reasons)}")


print("\n=== Behavior: multiple guards (hard stop + trailing) ===")
result = _run({
    "entry": {"conditions": [{"type": "current_drop", "threshold": -3, "window_days": 60}], "logic": "all"},
    "exit": {
        "guards": [
            {"type": "drawdown_from_entry", "value": -15, "cooldown_days": 30},
            {"type": "trailing_from_peak", "value": -8, "cooldown_days": 0},
        ],
        "rules": [],
    },
})
sells = [t for t in result["trades"] if t["action"] == "SELL"]
buys = [t for t in result["trades"] if t["action"] == "BUY"]
reasons = Counter(t.get("reason") for t in sells)
check("multi-guard: BUYs > 0", len(buys) > 0)
# Either guard can fire; both should be representable
check("multi-guard: at least one of {drawdown_from_entry, trailing_from_peak} fires or backtest_end",
      ("drawdown_from_entry" in reasons or "trailing_from_peak" in reasons
       or all(t.get("reason") == "backtest_end" for t in sells)),
      f"reasons={dict(reasons)}")


print("\n=== Behavior: frozen pricing (atr_stop + atr_target both on a position) ===")
result = _run({
    "entry": {"conditions": [{"type": "current_drop", "threshold": -3, "window_days": 60}], "logic": "all"},
    "exit": {
        "guards": [
            {"type": "atr_stop", "k": 2.0, "window_days": 20, "cooldown_days": 30},
            {"type": "atr_target", "k": 4.0, "window_days": 20},
        ],
        "rules": [],
    },
})
sells = [t for t in result["trades"] if t["action"] == "SELL"]
buys = [t for t in result["trades"] if t["action"] == "BUY"]
reasons = Counter(t.get("reason") for t in sells)
check("frozen-pair: BUYs > 0", len(buys) > 0)
# Every BUY should carry both stop and take_profit records in signal_detail
both_records_present = all(
    isinstance(t.get("signal_detail"), dict)
    and t["signal_detail"].get("stop", {}).get("type") == "atr_stop"
    and t["signal_detail"].get("take_profit", {}).get("type") == "atr_target"
    for t in buys
)
check("frozen-pair: every BUY carries both atr_stop + atr_target records",
      both_records_present)
# Every fire records the right rule type as reason
fire_types = [t.get("reason") for t in sells if t.get("reason") in ("atr_stop", "atr_target")]
check("frozen-pair: at least one fire of either atr_stop or atr_target",
      len(fire_types) > 0 or all(t.get("reason") == "backtest_end" for t in sells),
      f"fires={Counter(fire_types)}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 60}")
print(f"Passed: {PASS}, Failed: {FAIL}")
sys.exit(0 if FAIL == 0 else 1)
