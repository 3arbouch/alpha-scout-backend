#!/usr/bin/env python3
"""
End-to-end tests for volatility-adaptive stop / take-profit modes.

Asserts:
  1. Legacy (drawdown_from_entry / gain_from_entry) backtests unchanged.
  2. ATR-multiple mode: backtest runs, signal_detail contains the stop
     metadata, and SELL trades flagged stop_loss/take_profit fire at prices
     consistent with the frozen levels (price <= frozen_stop on stop, etc).
  3. Realized-vol mode (historical + ewma): same shape of assertions.
  4. Schema-level rejection: k>10 and window<10 are rejected by the model.

Run:
  DATA_DIR=/home/mohamed/alpha-scout-backend/data \
  MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \
  APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \
  python3 tests/test_vol_adaptive_stops_e2e.py
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, ROOT)

from backtest_engine import run_backtest
from server.models.strategy import StrategyConfig
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


BASE = {
    "name": "vol_stops_e2e",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT", "NVDA"]},
    "entry": {
        "conditions": [{"type": "current_drop", "threshold": -3, "window_days": 60}],
        "logic": "all",
    },
    "sizing": {"type": "equal_weight", "max_positions": 3, "initial_allocation": 100_000},
    "backtest": {"start": "2023-01-01", "end": "2023-12-31",
                 "entry_price": "next_close", "slippage_bps": 10},
}


def _run(extra: dict) -> dict:
    cfg = json.loads(json.dumps(BASE))
    cfg.update(extra)
    # Validate via Pydantic before running, so config bugs surface as
    # ValidationError instead of opaque engine errors.
    StrategyConfig(**cfg)
    return run_backtest(cfg)


# ---------------------------------------------------------------------------
# 1. Schema-level rejection
# ---------------------------------------------------------------------------
print("\n=== Schema rejection ===")

try:
    StrategyConfig(**{**BASE, "stop_loss": {"type": "atr_multiple", "k": 11, "window_days": 20}})
    check("k>10 rejected", False, "k=11 was accepted")
except ValidationError:
    check("k>10 rejected", True)

try:
    StrategyConfig(**{**BASE, "stop_loss": {"type": "atr_multiple", "k": 2.0, "window_days": 5}})
    check("window<10 rejected", False, "window=5 was accepted")
except ValidationError:
    check("window<10 rejected", True)

try:
    StrategyConfig(**{**BASE, "stop_loss": {"type": "realized_vol_multiple", "k": 2.0,
                                             "window_days": 20, "sigma_source": "garch"}})
    check("sigma_source=garch rejected (Phase 1)", False, "garch accepted")
except ValidationError:
    check("sigma_source=garch rejected (Phase 1)", True)


# ---------------------------------------------------------------------------
# 2. Legacy regression — same config family must still produce trades
# ---------------------------------------------------------------------------
print("\n=== Legacy regression ===")

result_legacy = _run({
    "stop_loss": {"type": "drawdown_from_entry", "value": -10, "cooldown_days": 30},
    "take_profit": {"type": "gain_from_entry", "value": 15},
})
buys_legacy = [t for t in result_legacy["trades"] if t["action"] == "BUY"]
check("legacy drawdown_from_entry produces trades", len(buys_legacy) > 0,
      f"buys={len(buys_legacy)}")

# Canonical signal-record envelope: every record (entry, stop, take_profit)
# uses {type, config, observed}. Legacy modes emit records with empty observed.
legacy_record_ok = all(
    isinstance(t.get("signal_detail"), dict)
    and t["signal_detail"].get("stop", {}).get("type") == "drawdown_from_entry"
    and t["signal_detail"]["stop"].get("config", {}).get("value") == -10
    and t["signal_detail"]["stop"].get("observed") == {}
    and t["signal_detail"].get("take_profit", {}).get("type") == "gain_from_entry"
    and t["signal_detail"]["take_profit"].get("config") == {"value": 15.0}
    and t["signal_detail"]["take_profit"].get("observed") == {}
    for t in buys_legacy
)
check("legacy stop/tp records use {type, config, observed} envelope",
      legacy_record_ok)

# Entry signal records also use the canonical envelope.
entries_envelope_ok = all(
    isinstance(t["signal_detail"].get("entries"), list)
    and all(
        isinstance(e, dict)
        and "type" in e and "config" in e and "observed" in e
        and "values" not in e   # old key gone
        and "threshold" not in e   # was spread; now nested in config
        for e in t["signal_detail"]["entries"]
    )
    for t in buys_legacy
)
check("entry records use {type, config, observed} envelope (no spread/values)",
      entries_envelope_ok)

# When NO stop and NO take_profit configured, signal_detail stays a list (the
# original shape) — byte-for-byte legacy preservation.
result_no_exits = _run({
    # explicitly drop stop_loss / take_profit
})
buys_no_exits = [t for t in result_no_exits["trades"] if t["action"] == "BUY"]
no_exits_shape_ok = all(
    isinstance(t.get("signal_detail"), list)
    for t in buys_no_exits
) if buys_no_exits else True
check("no exits configured: signal_detail stays a list (legacy shape)",
      no_exits_shape_ok)


# ---------------------------------------------------------------------------
# 3. ATR-multiple mode
# ---------------------------------------------------------------------------
print("\n=== ATR-multiple mode ===")

result_atr = _run({
    "stop_loss": {"type": "atr_multiple", "k": 2.0, "window_days": 20, "cooldown_days": 30},
    "take_profit": {"type": "atr_multiple", "k": 4.0, "window_days": 20},
})
buys_atr = [t for t in result_atr["trades"] if t["action"] == "BUY"]
sells_atr = [t for t in result_atr["trades"] if t["action"] == "SELL"]
check("ATR mode produces buys", len(buys_atr) > 0, f"buys={len(buys_atr)}")

# Every BUY must have stop record in canonical envelope.
# Type names: legacy 'atr_multiple' migrates to 'atr_stop' under unified exits.
all_have_stop_meta = all(
    isinstance(t.get("signal_detail"), dict)
    and t["signal_detail"].get("stop", {}).get("type") == "atr_stop"
    and "atr" in t["signal_detail"]["stop"].get("observed", {})
    and "frozen_price" in t["signal_detail"]["stop"]["observed"]
    and "k" in t["signal_detail"]["stop"].get("config", {})
    for t in buys_atr
)
check("every BUY has ATR canonical-envelope stop record", all_have_stop_meta)

# Every SELL flagged atr_stop must execute at <= frozen stop_price (after slippage).
# Under unified exits, the SELL trade's `reason` is the rule's type, not "stop_loss".
sl_fires = [t for t in sells_atr if t.get("reason") == "atr_stop"]
print(f"  ATR stop-loss fires: {len(sl_fires)}")
for t in sl_fires:
    sd = t.get("signal_detail") or {}
    frozen = ((sd.get("stop") or {}).get("observed") or {}).get("frozen_price")
    if frozen is None:
        check(f"atr_stop SELL has frozen_price ({t['symbol']}@{t['date']})", False)
        continue
    ok = t["price"] <= frozen * 1.01
    check(f"ATR SELL price ≤ frozen stop ({t['symbol']}@{t['date']}: "
          f"price={t['price']:.2f}, frozen={frozen:.2f})", ok)

tp_fires = [t for t in sells_atr if t.get("reason") == "atr_target"]
print(f"  ATR take-profit fires: {len(tp_fires)}")
for t in tp_fires:
    sd = t.get("signal_detail") or {}
    frozen_tp = ((sd.get("take_profit") or {}).get("observed") or {}).get("frozen_price")
    if frozen_tp is None:
        check(f"atr_target SELL has frozen_price ({t['symbol']}@{t['date']})", False)
        continue
    ok = t["price"] >= frozen_tp * 0.99
    check(f"ATR SELL price ≥ frozen tp ({t['symbol']}@{t['date']}: "
          f"price={t['price']:.2f}, frozen={frozen_tp:.2f})", ok)


# ---------------------------------------------------------------------------
# 4. Realized-vol mode (historical)
# ---------------------------------------------------------------------------
print("\n=== Realized-vol-multiple (historical) ===")

result_rv_h = _run({
    "stop_loss": {"type": "realized_vol_multiple", "k": 2.0, "window_days": 20,
                  "sigma_source": "historical", "cooldown_days": 30},
    "take_profit": {"type": "realized_vol_multiple", "k": 4.0, "window_days": 20,
                    "sigma_source": "historical"},
})
buys_rv = [t for t in result_rv_h["trades"] if t["action"] == "BUY"]
check("realized_vol historical produces buys", len(buys_rv) > 0,
      f"buys={len(buys_rv)}")
all_have_meta = all(
    isinstance(t.get("signal_detail"), dict)
    and t["signal_detail"].get("stop", {}).get("type") == "realized_vol_stop"
    and t["signal_detail"]["stop"]["config"].get("sigma_source") == "historical"
    and "sigma" in t["signal_detail"]["stop"].get("observed", {})
    for t in buys_rv
)
check("every BUY has realized_vol historical canonical record", all_have_meta)

# Frozen stop_price should be entry * (1 - k * sigma)
for t in buys_rv[:3]:
    rec = t["signal_detail"]["stop"]
    sigma = rec["observed"]["sigma"]
    k = rec["config"]["k"]
    expected = t["price"] * (1 - k * sigma)
    actual = rec["observed"]["frozen_price"]
    ok = abs(expected - actual) < 0.01
    check(f"frozen_price = entry*(1-k*sigma) ({t['symbol']}: "
          f"expected={expected:.4f}, actual={actual:.4f})", ok)


# ---------------------------------------------------------------------------
# 5. Realized-vol mode (EWMA)
# ---------------------------------------------------------------------------
print("\n=== Realized-vol-multiple (EWMA) ===")

result_rv_e = _run({
    "stop_loss": {"type": "realized_vol_multiple", "k": 2.0, "window_days": 20,
                  "sigma_source": "ewma", "cooldown_days": 30},
    "take_profit": {"type": "gain_from_entry", "value": 30},  # mixed: legacy TP + new stop
})
buys_e = [t for t in result_rv_e["trades"] if t["action"] == "BUY"]
check("EWMA mode produces buys", len(buys_e) > 0)

# Mixed mode: stop is realized_vol_multiple/ewma, tp is legacy gain_from_entry.
# Both records present in canonical envelope.
mixed_ok = all(
    isinstance(t.get("signal_detail"), dict)
    and t["signal_detail"].get("stop", {}).get("type") == "realized_vol_stop"
    and t["signal_detail"]["stop"]["config"].get("sigma_source") == "ewma"
    and t["signal_detail"].get("take_profit", {}).get("type") == "gain_from_entry"
    and t["signal_detail"]["take_profit"].get("observed") == {}
    for t in buys_e
)
check("mixed mode: both stop+tp records use canonical envelope", mixed_ok)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 60}")
print(f"Passed: {PASS}, Failed: {FAIL}")
sys.exit(0 if FAIL == 0 else 1)
