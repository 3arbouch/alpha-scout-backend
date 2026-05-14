#!/usr/bin/env python3
"""
Unit tests for scripts/position_book.py (Phase 2 Step 1).

PositionBook is the single live position book for the v2 portfolio engine.
This test covers the data layer in isolation — no DB, no price index built
from real prices, no signal logic. Pure state machine.

Coverage:
  - Init: cash, empty positions, validation
  - open(): new position, add-on (weighted-avg basis), cash debit, slippage,
    cap at available cash, reject too-small amounts
  - sell(): full close, partial sell, pnl / pnl_pct / days_held math,
    slippage on sell, cash credit
  - Cross-sleeve: two sleeves hold same symbol → two positions, each
    sleeve's sell only affects its own
  - Accessors: positions_for_sleeve, has, num_positions, positions_value
  - record_nav(): snapshot fields, daily_pnl, per_sleeve_positions_value,
    stale-price fallback to high_since_entry

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_position_book_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from position_book import PositionBook, Position

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


# ---------------------------------------------------------------------------
# 1. Initialization
# ---------------------------------------------------------------------------
print("\n=== 1. Initialization ===")

b = PositionBook(100_000)
check("initial cash = 100k", b.cash == 100_000.0)
check("initial_cash property", b.initial_cash == 100_000.0)
check("no positions on a fresh book", len(b.all_positions()) == 0)
check("num_positions() == 0", b.num_positions() == 0)
check("nav_history empty", b.nav_history == [])
check("closed_trades empty", b.closed_trades == [])

# Negative initial cash → ValueError
try:
    PositionBook(-1)
    check("negative initial_cash should raise", False, "did not raise")
except ValueError:
    check("negative initial_cash raises ValueError", True)


# ---------------------------------------------------------------------------
# 2. open() — new position
# ---------------------------------------------------------------------------
print("\n=== 2. open() — new position, basic case ===")

b = PositionBook(100_000)
t = b.open("Tech", "AAPL", "2024-01-02", amount=10_000, exec_price=200.0)
check("open returns a trade record", t is not None)
check("BUY action", t["action"] == "BUY")
check("price = exec_price (no slippage)", approx(t["price"], 200.0))
check("shares = 10000/200 = 50", approx(t["shares"], 50.0))
check("amount = 10000", approx(t["amount"], 10_000.0))
check("sleeve_label = Tech", t["sleeve_label"] == "Tech")
check("reason defaults to 'entry'", t["reason"] == "entry")
check("cash debited: 100k - 10k = 90k", approx(b.cash, 90_000.0))
check("1 position open", b.num_positions() == 1)
check("has(Tech, AAPL) == True", b.has("Tech", "AAPL"))
check("has(Tech, MSFT) == False", not b.has("Tech", "MSFT"))


# ---------------------------------------------------------------------------
# 3. open() — slippage on BUY
# ---------------------------------------------------------------------------
print("\n=== 3. open() — BUY slippage applied ===")

b = PositionBook(100_000)
t = b.open("Tech", "AAPL", "2024-01-02", amount=10_000, exec_price=200.0,
            slippage_bps=10)  # 10 bps = 0.10%
# fill_price = 200 * 1.001 = 200.20
# shares = 10000 / 200.20 = 49.95
check("fill_price = exec * (1 + slippage)",
      approx(t["price"], 200.20, tol=0.001))
check("shares = amount / fill_price",
      approx(t["shares"], 10_000 / 200.20, tol=1e-4))
check("cash debited by amount, not exec*shares", approx(b.cash, 90_000.0))


# ---------------------------------------------------------------------------
# 4. open() — add-on within same sleeve+symbol
# ---------------------------------------------------------------------------
print("\n=== 4. open() — add-on merges with weighted-avg basis ===")

b = PositionBook(100_000)
b.open("Tech", "AAPL", "2024-01-02", amount=10_000, exec_price=200.0)
# After: shares=50, entry_price=200, cash=90k
b.open("Tech", "AAPL", "2024-01-15", amount=15_000, exec_price=250.0)
# Add-on: 60 new shares at 250 → total cost = 50*200 + 60*250 = 25000
# total shares = 110 → weighted-avg entry = 25000/110 ≈ 227.27
pos = b.get("Tech", "AAPL")
check("add-on merges into one Position", b.num_positions() == 1)
check("shares = 50 + 60 = 110", approx(pos.shares, 110.0))
check("weighted-avg entry_price ≈ 227.27",
      approx(pos.entry_price, 25_000 / 110, tol=1e-4))
check("entry_date preserved as original (2024-01-02)",
      pos.entry_date == "2024-01-02")
check("cash debited: 100k - 10k - 15k = 75k", approx(b.cash, 75_000.0))


# ---------------------------------------------------------------------------
# 5. open() — cross-sleeve same symbol = TWO positions
# ---------------------------------------------------------------------------
print("\n=== 5. open() — different sleeves, same symbol → two positions ===")

b = PositionBook(100_000)
b.open("Tech",       "AAPL", "2024-01-02", amount=10_000, exec_price=200.0)
b.open("Defensive",  "AAPL", "2024-01-02", amount=10_000, exec_price=200.0)
check("two separate positions tagged by sleeve", b.num_positions() == 2)
check("Tech.AAPL exists",      b.has("Tech", "AAPL"))
check("Defensive.AAPL exists", b.has("Defensive", "AAPL"))
check("Tech.AAPL shares = 50",      approx(b.get("Tech", "AAPL").shares, 50.0))
check("Defensive.AAPL shares = 50", approx(b.get("Defensive", "AAPL").shares, 50.0))
check("positions_for_sleeve(Tech) has only Tech's AAPL",
      list(b.positions_for_sleeve("Tech").keys()) == ["AAPL"]
      and b.positions_for_sleeve("Tech")["AAPL"].sleeve_label == "Tech")


# ---------------------------------------------------------------------------
# 6. open() — capped at available cash (with 1% buffer)
# ---------------------------------------------------------------------------
print("\n=== 6. open() — caps amount at cash × 0.99 buffer ===")

b = PositionBook(1_000)
t = b.open("Tech", "AAPL", "2024-01-02", amount=10_000, exec_price=100.0)
# cash=1000, max=990. amount capped to 990.
check("amount capped at cash * 0.99",
      t is not None and approx(t["amount"], 990.0))
check("cash after BUY = 10 (1% buffer)", approx(b.cash, 10.0, tol=0.01))

# Skipped when amount below min_amount (default $1)
b2 = PositionBook(0.5)
check("amount below min_amount → None", b2.open("X", "A", "d", 0.5, 100.0) is None)
check("zero amount → None", PositionBook(1000).open("X", "A", "d", 0, 100) is None)
check("negative price → None", PositionBook(1000).open("X", "A", "d", 100, -1) is None)


# ---------------------------------------------------------------------------
# 7. sell() — full close
# ---------------------------------------------------------------------------
print("\n=== 7. sell() — full close ===")

b = PositionBook(100_000)
b.open("Tech", "AAPL", "2024-01-02", amount=10_000, exec_price=200.0)
# After: 50 shares at 200, cash=90k
t = b.sell("Tech", "AAPL", "2024-01-10", exec_price=220.0, reason="take_profit")
check("SELL returns a trade record", t is not None)
check("action = SELL", t["action"] == "SELL")
check("reason = take_profit", t["reason"] == "take_profit")
check("shares sold = 50", approx(t["shares"], 50.0))
check("fill_price = exec (no slippage)", approx(t["price"], 220.0))
check("proceeds = 50 * 220 = 11000", approx(t["amount"], 11_000.0))
check("pnl = (220-200)*50 = 1000", approx(t["pnl"], 1_000.0))
check("pnl_pct = (220-200)/200 * 100 = 10", approx(t["pnl_pct"], 10.0))
check("entry_date = 2024-01-02", t["entry_date"] == "2024-01-02")
check("entry_price = 200", approx(t["entry_price"], 200.0))
check("days_held = 8 (jan 2 → jan 10)", t["days_held"] == 8)
check("position removed", not b.has("Tech", "AAPL"))
check("cash credited: 90k + 11k = 101k", approx(b.cash, 101_000.0))
check("closed_trades has 1 SELL", len(b.closed_trades) == 1)


# ---------------------------------------------------------------------------
# 8. sell() — partial
# ---------------------------------------------------------------------------
print("\n=== 8. sell() — partial sell ===")

b = PositionBook(100_000)
b.open("Tech", "AAPL", "2024-01-02", amount=10_000, exec_price=200.0)
# 50 shares
t = b.sell("Tech", "AAPL", "2024-01-10", exec_price=220.0, reason="rebalance_trim",
            shares=20)
check("partial sell: 20 shares", approx(t["shares"], 20.0))
check("proceeds = 20*220 = 4400", approx(t["amount"], 4_400.0))
check("pnl = 20*(220-200) = 400", approx(t["pnl"], 400.0))
pos = b.get("Tech", "AAPL")
check("position remains open with 30 shares", pos is not None and approx(pos.shares, 30.0))
check("entry_price unchanged on partial",
      approx(pos.entry_price, 200.0))
check("cash credited: 90k + 4400 = 94400",
      approx(b.cash, 94_400.0))


# ---------------------------------------------------------------------------
# 9. sell() — slippage on SELL
# ---------------------------------------------------------------------------
print("\n=== 9. sell() — SELL slippage applied ===")

b = PositionBook(100_000)
b.open("Tech", "AAPL", "2024-01-02", amount=10_000, exec_price=200.0)
t = b.sell("Tech", "AAPL", "2024-01-10", exec_price=220.0,
            reason="take_profit", slippage_bps=10)
# fill_price = 220 * (1 - 0.001) = 219.78
check("SELL receives exec * (1 - slippage)",
      approx(t["price"], 219.78, tol=0.001))
# proceeds = 50 * 219.78 = 10989
check("proceeds reflect post-slippage fill",
      approx(t["amount"], 50 * 219.78, tol=0.01))


# ---------------------------------------------------------------------------
# 10. sell() — cross-sleeve isolation
# ---------------------------------------------------------------------------
print("\n=== 10. sell() — only affects the named sleeve's position ===")

b = PositionBook(100_000)
b.open("Tech",      "AAPL", "2024-01-02", amount=10_000, exec_price=200.0)
b.open("Defensive", "AAPL", "2024-01-02", amount=10_000, exec_price=200.0)
b.sell("Tech", "AAPL", "2024-01-10", exec_price=220.0, reason="stop_loss")
check("Tech.AAPL closed", not b.has("Tech", "AAPL"))
check("Defensive.AAPL still open", b.has("Defensive", "AAPL"))
check("Defensive.AAPL shares unchanged",
      approx(b.get("Defensive", "AAPL").shares, 50.0))


# ---------------------------------------------------------------------------
# 11. sell() — edge cases
# ---------------------------------------------------------------------------
print("\n=== 11. sell() — edge cases ===")

b = PositionBook(100_000)
check("sell non-existent position → None",
      b.sell("Tech", "AAPL", "2024-01-10", 200.0, "stop_loss") is None)

b.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)
check("sell 0 shares → None",
      b.sell("Tech", "AAPL", "2024-01-10", 200.0, "trim", shares=0) is None)

# Asking to sell MORE than held → capped at held
t = b.sell("Tech", "AAPL", "2024-01-10", 200.0, "trim", shares=999)
check("sell more than held caps at held", t is not None and approx(t["shares"], 50.0))
check("position closed after over-sell-attempt", not b.has("Tech", "AAPL"))


# ---------------------------------------------------------------------------
# 12. positions_value + nav
# ---------------------------------------------------------------------------
print("\n=== 12. positions_value + nav ===")

b = PositionBook(100_000)
b.open("Tech",      "AAPL", "2024-01-02", 10_000, 200.0)  # 50 shares
b.open("Defensive", "JNJ",  "2024-01-02", 20_000, 100.0)  # 200 shares
# Cash after: 100k - 10k - 20k = 70k
price_index = {
    "AAPL": {"2024-01-10": 220.0},
    "JNJ":  {"2024-01-10": 110.0},
}
pv = b.positions_value(price_index, "2024-01-10")
# 50 * 220 + 200 * 110 = 11000 + 22000 = 33000
check("positions_value = 33000", approx(pv, 33_000.0))
nav = b.nav(price_index, "2024-01-10")
check("nav = cash + pv = 70k + 33k = 103k", approx(nav, 103_000.0))


# ---------------------------------------------------------------------------
# 13. record_nav() — snapshot fields
# ---------------------------------------------------------------------------
print("\n=== 13. record_nav() — snapshot fields ===")

b = PositionBook(100_000)
b.open("Tech",      "AAPL", "2024-01-02", 10_000, 200.0)
b.open("Defensive", "JNJ",  "2024-01-02", 20_000, 100.0)
price_index = {
    "AAPL": {"2024-01-10": 220.0},
    "JNJ":  {"2024-01-10": 110.0},
}
snap = b.record_nav(price_index, "2024-01-10")
check("snapshot.date = 2024-01-10", snap["date"] == "2024-01-10")
check("snapshot.nav = 103000", approx(snap["nav"], 103_000.0))
check("snapshot.cash = 70000", approx(snap["cash"], 70_000.0))
check("snapshot.positions_value = 33000", approx(snap["positions_value"], 33_000.0))
check("snapshot.num_positions = 2", snap["num_positions"] == 2)
check("per_sleeve_positions_value.Tech = 11000",
      approx(snap["per_sleeve_positions_value"]["Tech"], 11_000.0))
check("per_sleeve_positions_value.Defensive = 22000",
      approx(snap["per_sleeve_positions_value"]["Defensive"], 22_000.0))
check("daily_pnl = nav - initial = 3000 on first snapshot",
      approx(snap["daily_pnl"], 3_000.0))
check("positions detail keyed by symbol",
      set(snap["positions"].keys()) == {"AAPL", "JNJ"})


# ---------------------------------------------------------------------------
# 14. record_nav() — daily_pnl between snapshots
# ---------------------------------------------------------------------------
print("\n=== 14. record_nav() — daily_pnl across snapshots ===")

b = PositionBook(100_000)
b.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)  # 50 shares
price_index = {
    "AAPL": {
        "2024-01-10": 220.0,   # +10% → nav=101k, pnl=1k? wait: 50*220=11k, cash=90k → nav=101k
        "2024-01-11": 230.0,   # 50*230=11500, cash=90k → nav=101500
    }
}
snap1 = b.record_nav(price_index, "2024-01-10")
# nav1 = 90000 + 50*220 = 101000. daily_pnl (vs initial 100k) = 1000.
check("snap1.daily_pnl = 1000 (vs initial)", approx(snap1["daily_pnl"], 1_000.0))
snap2 = b.record_nav(price_index, "2024-01-11")
# nav2 = 90000 + 50*230 = 101500. daily_pnl = 500 (vs snap1).
check("snap2.daily_pnl = 500 (vs prior snap)", approx(snap2["daily_pnl"], 500.0))


# ---------------------------------------------------------------------------
# 15. record_nav() — stale price fallback
# ---------------------------------------------------------------------------
print("\n=== 15. record_nav() — stale price → high_since_entry fallback ===")

b = PositionBook(100_000)
b.open("Tech", "AAPL", "2024-01-02", 10_000, 200.0)  # 50 shares @ 200
# Day 2: AAPL hit 220 (observe_price updates high_since_entry)
b.record_nav({"AAPL": {"2024-01-03": 220.0}}, "2024-01-03")
# Day 3: AAPL price missing (halt). Should value at high_since_entry = 220.
snap = b.record_nav({}, "2024-01-04")  # no AAPL price
# Expected nav = cash 90k + 50 * 220 = 101k
check("stale-price day valued at high_since_entry",
      approx(snap["nav"], 101_000.0))


# ---------------------------------------------------------------------------
# 16. accessors round-trip
# ---------------------------------------------------------------------------
print("\n=== 16. accessors ===")

b = PositionBook(100_000)
b.open("Tech",      "AAPL", "2024-01-02", 10_000, 200.0)
b.open("Tech",      "MSFT", "2024-01-02", 10_000, 300.0)
b.open("Defensive", "JNJ",  "2024-01-02", 10_000, 100.0)

check("num_positions() = 3", b.num_positions() == 3)
check("num_positions(Tech) = 2", b.num_positions("Tech") == 2)
check("num_positions(Defensive) = 1", b.num_positions("Defensive") == 1)
check("symbols_held_by_sleeve(Tech) = {AAPL, MSFT}",
      b.symbols_held_by_sleeve("Tech") == {"AAPL", "MSFT"})
check("symbols_held_by_sleeve(Defensive) = {JNJ}",
      b.symbols_held_by_sleeve("Defensive") == {"JNJ"})
check("all_positions() returns 3", len(b.all_positions()) == 3)


# ---------------------------------------------------------------------------
# 17. Position math (days_held, pnl_pct, observe_price)
# ---------------------------------------------------------------------------
print("\n=== 17. Position math methods ===")

p = Position(sleeve_label="Tech", symbol="AAPL", entry_date="2024-01-02",
              entry_price=200.0, shares=50, peak_price=210, high_since_entry=200.0)
check("market_value(220) = 11000", approx(p.market_value(220), 11_000.0))
check("pnl_pct(220) = 10%", approx(p.pnl_pct(220), 10.0))
check("pnl_pct(180) = -10%", approx(p.pnl_pct(180), -10.0))
check("days_held(2024-01-10) = 8", p.days_held("2024-01-10") == 8)

p.observe_price(230)
check("observe_price(230) updates high_since_entry to 230",
      approx(p.high_since_entry, 230.0))
p.observe_price(225)
check("observe_price(225) leaves high_since_entry at 230 (not a new high)",
      approx(p.high_since_entry, 230.0))

# Zero-price edge
p2 = Position("X", "Y", "2024-01-02", entry_price=0, shares=10,
               peak_price=0, high_since_entry=0)
check("pnl_pct with zero entry_price returns 0 (safe)",
      approx(p2.pnl_pct(100), 0.0))


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
