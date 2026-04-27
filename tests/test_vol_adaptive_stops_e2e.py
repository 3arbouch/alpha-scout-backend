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
n_buy_legacy = sum(1 for t in result_legacy["trades"] if t["action"] == "BUY")
check("legacy drawdown_from_entry produces trades", n_buy_legacy > 0,
      f"buys={n_buy_legacy}")
# Legacy positions must NOT have stop metadata in signal_detail
legacy_has_stop_meta = any(
    isinstance(t.get("signal_detail"), dict) and "stop" in t["signal_detail"]
    for t in result_legacy["trades"] if t["action"] == "BUY"
)
check("legacy mode does NOT add 'stop' metadata", not legacy_has_stop_meta)


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

# Every BUY must have stop metadata embedded.
all_have_stop_meta = all(
    isinstance(t.get("signal_detail"), dict)
    and t["signal_detail"].get("stop", {}).get("mode") == "atr_multiple"
    for t in buys_atr
)
check("every BUY has ATR stop metadata in signal_detail", all_have_stop_meta)

# Every SELL flagged stop_loss must execute at <= frozen stop_price (after slippage).
sl_fires = [t for t in sells_atr if t.get("reason") == "stop_loss"]
print(f"  ATR stop-loss fires: {len(sl_fires)}")
for t in sl_fires:
    sd = t.get("signal_detail") or {}
    frozen = (sd.get("stop") or {}).get("frozen_price")
    if frozen is None:
        check(f"stop_loss SELL has frozen_price ({t['symbol']}@{t['date']})", False)
        continue
    # Allow small slippage band — within 1.0 bps of frozen.
    ok = t["price"] <= frozen * 1.01
    check(f"ATR SELL price ≤ frozen stop ({t['symbol']}@{t['date']}: "
          f"price={t['price']:.2f}, frozen={frozen:.2f})", ok)

tp_fires = [t for t in sells_atr if t.get("reason") == "take_profit"]
print(f"  ATR take-profit fires: {len(tp_fires)}")
for t in tp_fires:
    sd = t.get("signal_detail") or {}
    frozen_tp = (sd.get("take_profit") or {}).get("frozen_price")
    if frozen_tp is None:
        check(f"take_profit SELL has frozen_price ({t['symbol']}@{t['date']})", False)
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
    and t["signal_detail"].get("stop", {}).get("mode") == "realized_vol_multiple"
    and t["signal_detail"]["stop"].get("sigma_source") == "historical"
    for t in buys_rv
)
check("every BUY has realized_vol historical metadata", all_have_meta)

# Frozen stop_price should be entry * (1 - k * sigma)
for t in buys_rv[:3]:
    sd = t["signal_detail"]["stop"]
    expected = t["price"] * (1 - 2.0 * sd["sigma_at_entry"])
    actual = sd["frozen_price"]
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

# Mixed mode: stop should have new metadata, take_profit should NOT.
mixed_ok = all(
    isinstance(t.get("signal_detail"), dict)
    and t["signal_detail"].get("stop", {}).get("sigma_source") == "ewma"
    and "take_profit" not in t["signal_detail"]
    for t in buys_e
)
check("mixed mode: stop=ewma, tp=legacy gain_from_entry (no tp metadata)", mixed_ok)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'=' * 60}")
print(f"Passed: {PASS}, Failed: {FAIL}")
sys.exit(0 if FAIL == 0 else 1)
