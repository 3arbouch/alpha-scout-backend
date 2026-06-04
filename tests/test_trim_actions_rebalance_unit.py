"""
Unit tests for the two new portfolio-construction actions:

  1. take_profit action="trim_gain" (ratcheting partial take-profit) — the
     gain since a moving reference is skimmed, the reference resets to the
     current price, and the surviving shares keep riding.
  2. rebalancing mode="target_weight" — two-sided drift-control of held
     positions toward sizing-model target weights, with a no-trade band.

Pure-function / in-memory tests: no DB, no market data. Run with:
    python3 tests/test_trim_actions_rebalance_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from position_book import PositionBook, Position
from sleeve_signals import get_exit_recommendations
from portfolio_engine_v2 import _apply_target_weight_rebalance

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


def approx(a, b, tol=1e-4):
    return a is not None and b is not None and abs(a - b) < tol


def make_pos(symbol, entry, shares, ref=None):
    p = Position(
        sleeve_label="S", symbol=symbol, entry_date="2020-01-01",
        entry_price=entry, shares=shares, peak_price=entry, high_since_entry=entry,
    )
    p.tp_reference_price = ref
    return p


class FakeSleeve:
    def __init__(self, cfg, slippage_bps=0):
        self.config = cfg
        self.label = "S"
        self.slippage_bps = slippage_bps


# ===========================================================================
# 1. trim_gain take-profit
# ===========================================================================
print("\n=== 1. take_profit action=trim_gain ===")

TP_TRIM = {"take_profit": {"type": "gain_from_entry", "value": 65, "action": "trim_gain"}}

# Bought $100/sh × 10 = $1000 cost basis. At $165 (+65%): sell the gain, keep $1000.
pos = make_pos("X", 100.0, 10)
exits = get_exit_recommendations("S", TP_TRIM, "2020-02-01", {"X": pos}, {"X": {"2020-02-01": 165.0}})
check("fires at +65%", len(exits) == 1 and exits[0].reason == "take_profit", f"got {exits}")
if exits:
    d = exits[0]
    # trim_shares = 10 × (1 − 100/165) = 3.9394 ; remaining 6.0606 sh × $165 = $1000 kept
    check("trims the gain (~3.94 sh sold)", approx(d.shares, 10 * (1 - 100 / 165)), f"got {d.shares}")
    kept_value = (10 - d.shares) * 165.0
    check("keeps cost basis invested (~$1000)", approx(kept_value, 1000.0, tol=1e-2), f"got {kept_value}")
    check("detail marks trim_gain + new reference",
          d.detail.get("action") == "trim_gain" and approx(d.detail.get("new_reference"), 165.0),
          f"got {d.detail}")

# Below +65% (here +50%): no fire.
pos = make_pos("X", 100.0, 10)
exits = get_exit_recommendations("S", TP_TRIM, "2020-02-01", {"X": pos}, {"X": {"2020-02-01": 150.0}})
check("does NOT fire at +50%", exits == [], f"got {exits}")

# Ratchet: reference already moved to 165 (after a prior trim). At 165 the gain
# since reference is 0 → no fire; it must climb another +65% (to 272.25).
pos = make_pos("X", 100.0, 6.0606, ref=165.0)
exits = get_exit_recommendations("S", TP_TRIM, "2020-02-01", {"X": pos}, {"X": {"2020-02-01": 165.0}})
check("post-trim: does NOT re-fire at the reference", exits == [], f"got {exits}")

exits = get_exit_recommendations("S", TP_TRIM, "2020-02-01", {"X": pos}, {"X": {"2020-02-01": 272.25}})
check("ratchet re-fires after another +65% leg (165→272.25)", len(exits) == 1, f"got {exits}")

# Default action (no `action` key, as in existing live configs) → full close.
TP_ALL = {"take_profit": {"type": "gain_from_entry", "value": 65}}
pos = make_pos("X", 100.0, 10)
exits = get_exit_recommendations("S", TP_ALL, "2020-02-01", {"X": pos}, {"X": {"2020-02-01": 165.0}})
check("default action still sells all (shares=None)",
      len(exits) == 1 and exits[0].shares is None, f"got {exits}")


# ---------------------------------------------------------------------------
# 1a-whole. trim_gain respects whole-share sizing (floors the trim)
# ---------------------------------------------------------------------------
print("\n=== 1a-whole. trim_gain whole-share flooring ===")
# entry 169.10 × 31sh, price 290.79 (+72%): raw trim = 31×(1−169.10/290.79)=12.97
TP_TRIM_WHOLE = {
    "sizing": {"type": "risk_parity", "shares": "whole"},
    "take_profit": {"type": "gain_from_entry", "value": 65, "action": "trim_gain"},
}
pos = make_pos("MRVL", 169.10, 31)
exits = get_exit_recommendations("S", TP_TRIM_WHOLE, "2026-06-02", {"MRVL": pos}, {"MRVL": {"2026-06-02": 290.79}})
check("whole mode → trim floored to 12 shares (not 12.97)",
      len(exits) == 1 and exits[0].shares == 12.0, f"got {[e.shares for e in exits]}")
# fractional mode keeps the exact partial
TP_TRIM_FRAC = {
    "sizing": {"type": "risk_parity", "shares": "fractional"},
    "take_profit": {"type": "gain_from_entry", "value": 65, "action": "trim_gain"},
}
pos = make_pos("MRVL", 169.10, 31)
exits = get_exit_recommendations("S", TP_TRIM_FRAC, "2026-06-02", {"MRVL": pos}, {"MRVL": {"2026-06-02": 290.79}})
check("fractional mode → keeps exact 12.97 shares",
      len(exits) == 1 and abs(exits[0].shares - 31 * (1 - 169.10 / 290.79)) < 1e-6, f"got {[e.shares for e in exits]}")


# ===========================================================================
# 1b. trailing_peak scale-out
# ===========================================================================
print("\n=== 1b. take_profit type=trailing_peak ===")

# Trim 1/3 when price falls >=10% below the trailing high; arm once +20% up.
TP_TRAIL = {"take_profit": {"type": "trailing_peak", "drop_pct": 10,
                            "fraction": 1 / 3, "activate_gain_pct": 20}}


def run_trail(entry, high, price, ref=None):
    pos = make_pos("X", entry, 30, ref=None)
    pos.high_since_entry = high
    pos.trail_high = ref  # None ⇒ use high_since_entry
    # observe_price runs inside get_exit_recommendations; pass today's price.
    return pos, get_exit_recommendations(
        "S", TP_TRAIL, "2020-02-01", {"X": pos}, {"X": {"2020-02-01": price}})


# High $150 (from entry $100, +50% so armed); price $134 = −10.7% from high → trim.
pos, exits = run_trail(100.0, 150.0, 134.0)
check("trims on >=10% drop from high (armed)",
      len(exits) == 1 and approx(exits[0].shares, 30 / 3), f"got {exits}")
if exits:
    check("detail marks trailing_peak + reset", exits[0].detail.get("action") == "trailing_peak"
          and approx(exits[0].detail.get("reset_high"), 134.0), f"got {exits[0].detail}")

# Price only −5% below high → no trim (trend hasn't broken).
pos, exits = run_trail(100.0, 150.0, 142.5)
check("does NOT trim on a shallow (−5%) pullback", exits == [], f"got {exits}")

# Making a NEW high (price == high) → no trim, and never trims a runner.
pos, exits = run_trail(100.0, 150.0, 150.0)
check("does NOT trim at a fresh new high", exits == [], f"got {exits}")

# Not yet armed: high only +10% (< activate 20%) even with a big drop → no trim.
pos, exits = run_trail(100.0, 110.0, 95.0)
check("does NOT arm below activate_gain_pct (no underwater/early trim)", exits == [], f"got {exits}")

# fraction default 1.0 → full exit (shares=None).
TP_TRAIL_FULL = {"take_profit": {"type": "trailing_peak", "drop_pct": 10}}
pos = make_pos("X", 100.0, 30)
pos.high_since_entry = 150.0
exits = get_exit_recommendations("S", TP_TRAIL_FULL, "2020-02-01", {"X": pos}, {"X": {"2020-02-01": 134.0}})
check("default fraction=1.0 → full exit (shares=None)",
      len(exits) == 1 and exits[0].shares is None, f"got {exits}")


# ===========================================================================
# 2. target_weight rebalancing (two-sided, with band)
# ===========================================================================
print("\n=== 2. rebalancing mode=target_weight ===")

# Two equal-priced names: A is 80% (overweight), B is 20% (underweight).
# Equal-weight sizing → target 50/50. Invested = $10,000.
def fresh_book():
    book = PositionBook({"S": 0.0})  # no starting cash → trims must fund adds
    book.positions[("S", "A")] = make_pos("A", 100.0, 80)  # mv $8,000
    book.positions[("S", "B")] = make_pos("B", 100.0, 20)  # mv $2,000
    return book

PRICES = {"A": {"2020-02-01": 100.0}, "B": {"2020-02-01": 100.0}}

CFG_EW = {
    "sizing": {"type": "equal_weight", "max_positions": 2, "shares": "fractional"},
    "rebalancing": {"frequency": "monthly", "mode": "target_weight",
                    "rules": {"rebalance_band_pct": 0}},
}

book = fresh_book()
trades = _apply_target_weight_rebalance(FakeSleeve(CFG_EW), book, PRICES, "2020-02-01")
a = book.get("S", "A")
b = book.get("S", "B")
sells = [t for t in trades if t["action"] == "SELL"]
buys = [t for t in trades if t["action"] == "BUY"]
check("two-sided: one trim + one add", len(sells) == 1 and len(buys) == 1, f"got {trades}")
check("trims the overweight name A", sells and sells[0]["symbol"] == "A", f"got {sells}")
check("A trimmed toward target (~50 sh left of 80)", approx(a.shares, 50.0, tol=0.5), f"got {a.shares}")
check("adds to the underweight name B", buys and buys[0]["symbol"] == "B", f"got {buys}")
check("B increased above its 20 starting shares", b.shares > 20.0, f"got {b.shares}")

# No-trade band: both names are 30 percentage points off target; a 40pt band
# leaves everything untouched.
CFG_BAND = {
    "sizing": {"type": "equal_weight", "max_positions": 2, "shares": "fractional"},
    "rebalancing": {"frequency": "monthly", "mode": "target_weight",
                    "rules": {"rebalance_band_pct": 40}},
}
book = fresh_book()
trades = _apply_target_weight_rebalance(FakeSleeve(CFG_BAND), book, PRICES, "2020-02-01")
check("wide no-trade band suppresses all trades", trades == [], f"got {trades}")


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
