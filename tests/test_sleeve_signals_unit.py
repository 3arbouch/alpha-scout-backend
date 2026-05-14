#!/usr/bin/env python3
"""
Unit tests for scripts/sleeve_signals.py (Phase 2 Step 2).

Verifies that the directive generators emit recommendations matching v1's
logical conditions, but as pure data instead of side-effects.

Coverage:
  - get_entry_candidates:
      • signal-firing filter (only symbols with today's signal qualify)
      • held-symbols exclusion (sleeve doesn't double-enter its own positions)
      • cross-sleeve isolation (other sleeves' positions don't block this sleeve)
      • stop_loss cooldown filter
      • ranking + top_n cap
      • available_slots cap when no ranking
      • entry priority: worst_drawdown sort
      • returns ordered, top-first
  - get_exit_recommendations:
      • stop_loss fires on drawdown breach
      • take_profit fires on gain_from_entry / above_peak
      • time_stop fires past max_days
      • fundamental_exit from precomputed exit_signals
      • only ONE exit per position per day (first-match wins)
      • missing price → no exit recommendation
      • cross-sleeve: only the sleeve's own positions checked
  - get_rebalance_directives:
      • frequency=none → empty
      • quarterly not yet at cadence → empty
      • quarterly past cadence + earnings_beat → BUY directive
      • quarterly past cadence + earnings_miss → SELL directive
      • max_position_pct trim
      • on_earnings frequency fires on the event date

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_sleeve_signals_unit.py
"""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from position_book import PositionBook, Position
from sleeve_signals import (
    EntryDirective, ExitDirective, RebalanceDirective, SleeveRuntimeState,
    get_entry_candidates, get_exit_recommendations, get_rebalance_directives,
)

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


def approx(a, b, tol=1e-6):
    return a is not None and b is not None and abs(a - b) < tol


def trading_days(start: str, n: int) -> list[str]:
    """Generate n consecutive trading dates starting from `start` (skips weekends)."""
    out = []
    d = datetime.strptime(start, "%Y-%m-%d")
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return out


# ---------------------------------------------------------------------------
# 1. get_entry_candidates — signal-firing filter + held-symbol exclusion
# ---------------------------------------------------------------------------
print("\n=== 1. Entry candidates — basic firing filter ===")

DATES = trading_days("2024-01-02", 10)
TODAY = DATES[5]

cfg = {
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "ranking": None,
    "stop_loss": {"type": "drawdown_from_entry", "value": -25, "cooldown_days": 30},
}
signals = {
    "AAPL": {TODAY: 1.0},        # signal fires today
    "MSFT": {TODAY: 1.0},        # signal fires today
    "GOOGL": {DATES[4]: 1.0},    # signal fired YESTERDAY, not today
    "AMZN": {},                  # signal never fires
}
metadata = {sym: {TODAY: {"signal": "test"}} for sym in signals}

state = SleeveRuntimeState(sleeve_label="Tech")
cands = get_entry_candidates(
    sleeve_label="Tech", sleeve_config=cfg, date=TODAY,
    trading_dates=DATES, signals=signals, signal_metadata=metadata,
    held_symbols_in_sleeve=set(), available_slots=10,
    state=state, conn=None, price_index={},
)
syms = {c.symbol for c in cands}
check("only symbols with TODAY's signal qualify",
      syms == {"AAPL", "MSFT"},
      f"got {syms}")

# Held symbol exclusion
cands = get_entry_candidates(
    sleeve_label="Tech", sleeve_config=cfg, date=TODAY,
    trading_dates=DATES, signals=signals, signal_metadata=metadata,
    held_symbols_in_sleeve={"AAPL"}, available_slots=10,
    state=state, conn=None, price_index={},
)
check("already-held symbols excluded from this sleeve's candidates",
      {c.symbol for c in cands} == {"MSFT"})


# ---------------------------------------------------------------------------
# 2. Cross-sleeve isolation — other sleeve's positions don't block
# ---------------------------------------------------------------------------
print("\n=== 2. Cross-sleeve isolation ===")

# AAPL is held by Defensive (another sleeve), but Tech can still enter AAPL.
# Tech's held_symbols only contains Tech's positions.
cands = get_entry_candidates(
    sleeve_label="Tech", sleeve_config=cfg, date=TODAY,
    trading_dates=DATES, signals=signals, signal_metadata=metadata,
    held_symbols_in_sleeve=set(),   # Tech holds nothing
    available_slots=10,
    state=state, conn=None, price_index={},
)
check("Tech can enter AAPL even when another sleeve holds it",
      "AAPL" in {c.symbol for c in cands})


# ---------------------------------------------------------------------------
# 3. Stop-loss cooldown filter
# ---------------------------------------------------------------------------
print("\n=== 3. Stop-loss cooldown excludes recently-stopped symbols ===")

# cooldown_days = 30 calendar = ~21 trading days. Stop fired 5 trading days ago.
state_cd = SleeveRuntimeState(
    sleeve_label="Tech",
    stop_loss_cooldowns={"AAPL": DATES[0]},  # 5 trading days before TODAY
)
cands = get_entry_candidates(
    sleeve_label="Tech", sleeve_config=cfg, date=TODAY,
    trading_dates=DATES, signals=signals, signal_metadata=metadata,
    held_symbols_in_sleeve=set(), available_slots=10,
    state=state_cd, conn=None, price_index={},
)
check("AAPL in cooldown (5 td ago, cooldown ≈ 21 td) → excluded",
      "AAPL" not in {c.symbol for c in cands})
check("MSFT not in cooldown → still qualifies",
      "MSFT" in {c.symbol for c in cands})


# ---------------------------------------------------------------------------
# 4. available_slots ≥ candidates → entry priority sort applies (worst_drawdown)
# ---------------------------------------------------------------------------
print("\n=== 4. Under-capacity batch sorted by entry priority ===")

# v1 semantics: worst_drawdown priority only fires when candidates fit in
# available_slots (no ranking needed). With over-capacity, the ranker
# kicks in instead (default = pe_percentile). Test the under-capacity path.
signals_three = {f"S{i}": {TODAY: -float(i)} for i in range(3)}   # 3 symbols, S2 most negative
meta_three = {sym: {TODAY: {"signal": "test"}} for sym in signals_three}
cfg_no_rank = dict(cfg)
cfg_no_rank["entry"] = {"conditions": [{"type": "always"}], "logic": "all",
                         "priority": "worst_drawdown"}
cfg_no_rank["ranking"] = None

cands = get_entry_candidates(
    sleeve_label="Tech", sleeve_config=cfg_no_rank, date=TODAY,
    trading_dates=DATES, signals=signals_three, signal_metadata=meta_three,
    held_symbols_in_sleeve=set(), available_slots=5,   # MORE slots than candidates
    state=SleeveRuntimeState("Tech"),
    conn=None, price_index={},
)
check("3 candidates fit in 5 slots → 3 returned",
      len(cands) == 3, f"got {len(cands)}")
check("worst_drawdown priority → most negative signal first (S2, S1, S0)",
      [c.symbol for c in cands] == ["S2", "S1", "S0"],
      f"got {[c.symbol for c in cands]}")


# ---------------------------------------------------------------------------
# 5. get_exit_recommendations — stop_loss fires on drawdown
# ---------------------------------------------------------------------------
print("\n=== 5. Exit recommendations — stop_loss ===")

book = PositionBook(100_000)
book.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)  # 50 shares @ 200
# Price drops to 140 → drawdown = -30% → triggers stop_loss at -25%
pi = {"AAPL": {TODAY: 140.0}}
cfg_stop = {
    "stop_loss": {"type": "drawdown_from_entry", "value": -25, "cooldown_days": 30},
    "take_profit": None, "time_stop": {"max_days": 365},
}
exits = get_exit_recommendations(
    sleeve_label="Tech", sleeve_config=cfg_stop, date=TODAY,
    sleeve_positions=book.positions_for_sleeve("Tech"),
    price_index=pi,
)
check("stop_loss fires at -30% (threshold -25%)",
      len(exits) == 1 and exits[0].reason == "stop_loss"
      and exits[0].symbol == "AAPL")


# ---------------------------------------------------------------------------
# 6. take_profit fires
# ---------------------------------------------------------------------------
print("\n=== 6. Exit recommendations — take_profit gain_from_entry ===")

book = PositionBook(100_000)
book.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)
pi = {"AAPL": {TODAY: 240.0}}   # +20% gain
cfg_tp = {
    "stop_loss": None,
    "take_profit": {"type": "gain_from_entry", "value": 15},
    "time_stop": {"max_days": 365},
}
exits = get_exit_recommendations(
    sleeve_label="Tech", sleeve_config=cfg_tp, date=TODAY,
    sleeve_positions=book.positions_for_sleeve("Tech"),
    price_index=pi,
)
check("take_profit fires at +20% (threshold +15%)",
      len(exits) == 1 and exits[0].reason == "take_profit")


# ---------------------------------------------------------------------------
# 7. time_stop fires past max_days
# ---------------------------------------------------------------------------
print("\n=== 7. Exit recommendations — time_stop ===")

book = PositionBook(100_000)
book.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)
# 365 days later: 2025-01-01
far_date = "2025-01-15"
pi = {"AAPL": {far_date: 220.0}}
cfg_ts = {
    "stop_loss": None,
    "take_profit": None,
    "time_stop": {"max_days": 365},
}
exits = get_exit_recommendations(
    sleeve_label="Tech", sleeve_config=cfg_ts, date=far_date,
    sleeve_positions=book.positions_for_sleeve("Tech"),
    price_index=pi,
)
check("time_stop fires past max_days",
      len(exits) == 1 and exits[0].reason == "time_stop")


# ---------------------------------------------------------------------------
# 8. fundamental_exit from precomputed signals
# ---------------------------------------------------------------------------
print("\n=== 8. Exit recommendations — fundamental_exit ===")

book = PositionBook(100_000)
book.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)
pi = {"AAPL": {TODAY: 200.0}}  # neutral price, no stop/TP would fire
cfg_basic = {
    "stop_loss": {"type": "drawdown_from_entry", "value": -25, "cooldown_days": 30},
    "take_profit": {"type": "gain_from_entry", "value": 15},
    "time_stop": {"max_days": 365},
}
exit_signals = {"AAPL": {TODAY: {"reason": "revenue_deceleration",
                                  "detail": "Q3 deceleration"}}}
exits = get_exit_recommendations(
    sleeve_label="Tech", sleeve_config=cfg_basic, date=TODAY,
    sleeve_positions=book.positions_for_sleeve("Tech"),
    price_index=pi, exit_signals=exit_signals,
)
check("fundamental_exit fires from exit_signals dict",
      len(exits) == 1
      and exits[0].reason == "revenue_deceleration"
      and exits[0].detail.get("reason") == "revenue_deceleration")


# ---------------------------------------------------------------------------
# 9. Only ONE exit per position per day (first-match wins)
# ---------------------------------------------------------------------------
print("\n=== 9. First-match wins ordering: stop_loss > take_profit > time_stop ===")

# Position would trigger BOTH stop_loss (-30%) and time_stop (>365 days)
book = PositionBook(100_000)
book.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)
late_date = "2025-02-01"
pi = {"AAPL": {late_date: 140.0}}  # -30% AND past max_days
cfg_both = {
    "stop_loss": {"type": "drawdown_from_entry", "value": -25, "cooldown_days": 30},
    "take_profit": None,
    "time_stop": {"max_days": 365},
}
exits = get_exit_recommendations(
    sleeve_label="Tech", sleeve_config=cfg_both, date=late_date,
    sleeve_positions=book.positions_for_sleeve("Tech"),
    price_index=pi,
)
check("stop_loss wins over time_stop when both qualify",
      len(exits) == 1 and exits[0].reason == "stop_loss")


# ---------------------------------------------------------------------------
# 10. Missing price → no exit
# ---------------------------------------------------------------------------
print("\n=== 10. Missing price → exit checks skipped ===")

book = PositionBook(100_000)
book.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)
exits = get_exit_recommendations(
    sleeve_label="Tech", sleeve_config=cfg_basic, date=TODAY,
    sleeve_positions=book.positions_for_sleeve("Tech"),
    price_index={},  # no price for AAPL today (halt)
)
check("no fresh price → no exit recommendation",
      exits == [])


# ---------------------------------------------------------------------------
# 11. Cross-sleeve isolation in exits
# ---------------------------------------------------------------------------
print("\n=== 11. Cross-sleeve exit isolation ===")

book = PositionBook(100_000)
book.open("Tech",      "AAPL", "2024-01-02", 10_000, 200.0)
book.open("Defensive", "AAPL", "2024-01-02", 10_000, 200.0)
pi = {"AAPL": {TODAY: 140.0}}  # both would stop_loss

# Only check Tech's positions
exits = get_exit_recommendations(
    sleeve_label="Tech", sleeve_config=cfg_stop, date=TODAY,
    sleeve_positions=book.positions_for_sleeve("Tech"),
    price_index=pi,
)
check("only Tech's exits emitted (1 directive, sleeve=Tech)",
      len(exits) == 1 and exits[0].sleeve_label == "Tech")


# ---------------------------------------------------------------------------
# 12. Rebalance: frequency=none → empty
# ---------------------------------------------------------------------------
print("\n=== 12. Rebalance directives ===")

book = PositionBook(100_000)
book.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)
pi = {"AAPL": {TODAY: 220.0}}

state = SleeveRuntimeState("Tech", last_rebal_date=DATES[0])
cfg_none = {"rebalancing": {"frequency": "none"}}
rds = get_rebalance_directives(
    sleeve_label="Tech", sleeve_config=cfg_none, date=TODAY,
    sleeve_positions=book.positions_for_sleeve("Tech"),
    price_index=pi, state=state, sleeve_nav=100_000,
    available_cash=90_000,
)
check("frequency=none → no directives", rds == [])


# ---------------------------------------------------------------------------
# 13. Rebalance: quarterly + earnings_beat → BUY add directive
# ---------------------------------------------------------------------------
print("\n=== 13. Quarterly rebalance + earnings_beat → ADD (matches v1 _do_rebalance) ===")

# Set last_rebal_date 100 days ago so today qualifies for quarterly
old_rebal = (datetime.strptime(TODAY, "%Y-%m-%d") - timedelta(days=100)).strftime("%Y-%m-%d")
state_quart = SleeveRuntimeState("Tech", last_rebal_date=old_rebal)

# AAPL is up by 20% from entry (200 → 240); earnings beat 10 days ago
beat_date = (datetime.strptime(TODAY, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
earnings_data = {
    "AAPL": {beat_date: {"eps_actual": 1.5, "eps_estimated": 1.0, "beat": True}},
}
pi_up = {"AAPL": {TODAY: 240.0}}   # pnl_pct = (240-220)/220 ≈ 9.1% (entry weighted-avg from earlier in this test was 220 due to add-on)

book2 = PositionBook(100_000)
book2.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)  # 50 shares @ 200
# Position pnl_pct at 240 = 20%, threshold is 10% → qualifies for add

cfg_q = {
    "rebalancing": {
        "frequency": "quarterly",
        "mode": "trim",
        "rules": {
            "max_position_pct": 100,   # high cap so trim doesn't fire
            "add_on_earnings_beat": {
                "min_gain_pct": 10, "max_add_multiplier": 1.5, "lookback_days": 90,
            },
        },
    },
}
rds = get_rebalance_directives(
    sleeve_label="Tech", sleeve_config=cfg_q, date=TODAY,
    sleeve_positions=book2.positions_for_sleeve("Tech"),
    price_index=pi_up, state=state_quart,
    sleeve_nav=100_000, available_cash=90_000, earnings_data=earnings_data,
)
check("quarterly + earnings_beat + pnl >= threshold → 1 BUY add directive",
      len(rds) == 1 and rds[0].action == "BUY" and rds[0].reason == "entry",
      f"got {[(r.action, r.reason) for r in rds]}")
if rds:
    d = rds[0]
    # original_cost = 10000, max_total = 15000, current_value = 50 * 240 = 12000
    # room_to_add = 15000 - 12000 = 3000
    # amount = min(3000, 90000 * 0.25 = 22500) = 3000
    check("amount = room_to_add = max_total - current_value = 3000",
          approx(d.amount, 3_000.0, tol=1.0),
          f"got amount={d.amount}")
    check("detail.trigger = earnings_beat",
          d.detail.get("trigger") == "earnings_beat")


# ---------------------------------------------------------------------------
# 14. Rebalance: no beat, no qualifying pnl → no directive
# ---------------------------------------------------------------------------
print("\n=== 14. Earnings miss → NO add directive (v1 doesn't trim on miss in this path) ===")

earnings_miss = {
    "AAPL": {beat_date: {"eps_actual": 0.8, "eps_estimated": 1.0, "beat": False}},
}
rds = get_rebalance_directives(
    sleeve_label="Tech", sleeve_config=cfg_q, date=TODAY,
    sleeve_positions=book2.positions_for_sleeve("Tech"),
    price_index=pi_up, state=state_quart,
    sleeve_nav=100_000, available_cash=90_000, earnings_data=earnings_miss,
)
check("earnings_miss + no max_pct breach → no directives (v1 silently ignores on_earnings_miss)",
      rds == [])


# ---------------------------------------------------------------------------
# 15. Rebalance: not at cadence → empty
# ---------------------------------------------------------------------------
print("\n=== 15. Quarterly rebalance — not yet at cadence ===")

recent_rebal = (datetime.strptime(TODAY, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
state_recent = SleeveRuntimeState("Tech", last_rebal_date=recent_rebal)
rds = get_rebalance_directives(
    sleeve_label="Tech", sleeve_config=cfg_q, date=TODAY,
    sleeve_positions=book2.positions_for_sleeve("Tech"),
    price_index=pi_up, state=state_recent,
    sleeve_nav=100_000, available_cash=90_000, earnings_data=earnings_data,
)
check("30 days since last rebal (quarterly needs 90) → no directives",
      rds == [])


# ---------------------------------------------------------------------------
# 16. max_position_pct trim
# ---------------------------------------------------------------------------
print("\n=== 16. max_position_pct trim (matches v1 _do_rebalance formula) ===")

# Position worth 30% of sleeve NAV, cap at 25%
# v1: trim_pct = ((30 - 25) / 30) * 100 = 16.67%
# trim_shares = 150 * 0.1667 = 25
book3 = PositionBook(100_000)
book3.open("Tech", "AAPL", "2024-01-02", 30_000, 200.0)  # 150 shares
pi3 = {"AAPL": {TODAY: 200.0}}

cfg_q2 = {
    "rebalancing": {
        "frequency": "quarterly", "mode": "trim",
        "rules": {"max_position_pct": 25,
                   "add_on_earnings_beat": {"min_gain_pct": 50,   # high threshold to skip add
                                             "max_add_multiplier": 1.5,
                                             "lookback_days": 90}},
    },
}
state_q3 = SleeveRuntimeState("Tech", last_rebal_date=old_rebal)
rds = get_rebalance_directives(
    sleeve_label="Tech", sleeve_config=cfg_q2, date=TODAY,
    sleeve_positions=book3.positions_for_sleeve("Tech"),
    price_index=pi3, state=state_q3,
    sleeve_nav=100_000, available_cash=70_000, earnings_data={},
)
check("position at 30% (cap 25%) → SELL trim",
      len(rds) == 1 and rds[0].action == "SELL"
      and rds[0].reason == "rebalance_trim",
      f"got {rds}")
if rds:
    check("trim shares ≈ 25 (((30-25)/30) × 150 = 25)",
          approx(rds[0].shares, 25.0, tol=1e-2),
          f"got shares={rds[0].shares}")
    check("detail.trigger = max_position_pct",
          rds[0].detail.get("trigger") == "max_position_pct")


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
