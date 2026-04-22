"""Unit tests for scripts/portfolio_book.reconstruct_positions.

Verifies the two accounting identities on synthetic ledgers:
    cash + positions_value == portfolio_value
    total_realized + total_unrealized == portfolio_value - initial_capital
Plus shape checks: status, sleeves, num_round_trips, weight_pct.

Run: python3 -m auto_trader.test_portfolio_book
"""

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from portfolio_book import reconstruct_positions


def approx(a: float, b: float, tol: float = 1e-6) -> bool:
    return math.isclose(a, b, abs_tol=tol)


def _price(table: dict[str, float]):
    return lambda sym, _d: table.get(sym)


def assert_identities(book: dict, initial_capital: float, label: str = ""):
    pv = book["portfolio_value"]
    cash = book["cash"]
    pos_val = book["positions_value"]
    realized = book["total_realized_pnl"]
    unrealized = book["total_unrealized_pnl"]

    assert approx(cash + pos_val, pv), (
        f"[{label}] cash+positions_value != portfolio_value: "
        f"{cash:.4f} + {pos_val:.4f} = {cash + pos_val:.4f} vs {pv:.4f}"
    )
    assert approx(realized + unrealized, pv - initial_capital), (
        f"[{label}] realized+unrealized != pv-initial: "
        f"{realized + unrealized:.4f} vs {pv - initial_capital:.4f}"
    )


def test_full_round_trip_closed():
    trades = [
        {"date": "2025-01-02", "action": "BUY", "symbol": "AAPL", "shares": 100, "price": 150, "amount": 15000, "sleeve_label": "core"},
        {"date": "2025-02-01", "action": "SELL", "symbol": "AAPL", "shares": 100, "price": 180, "amount": 18000, "pnl": 3000, "entry_price": 150, "sleeve_label": "core"},
    ]
    book = reconstruct_positions(trades, 100000, "2025-03-01", _price({"AAPL": 200}))
    assert_identities(book, 100000, "full_round_trip")
    assert book["open_count"] == 0
    assert book["closed_count"] == 1
    p = book["positions"][0]
    assert p["status"] == "closed"
    assert approx(p["shares_held"], 0)
    assert approx(p["realized_pnl"], 3000)
    assert approx(p["unrealized_pnl"], 0)
    assert approx(p["weight_pct"], 0)
    assert p["num_round_trips"] == 1
    assert p["sleeves"] == ["core"]
    assert approx(book["cash"], 103000)  # 100000 - 15000 + 18000


def test_open_at_end():
    trades = [
        {"date": "2025-01-02", "action": "BUY", "symbol": "MSFT", "shares": 50, "price": 400, "amount": 20000, "sleeve_label": "tech"},
    ]
    book = reconstruct_positions(trades, 100000, "2025-06-01", _price({"MSFT": 450}))
    assert_identities(book, 100000, "open_at_end")
    assert book["open_count"] == 1
    assert book["closed_count"] == 0
    p = book["positions"][0]
    assert p["status"] == "open"
    assert approx(p["shares_held"], 50)
    assert approx(p["avg_entry"], 400)
    assert approx(p["current_price"], 450)
    assert approx(p["market_value"], 22500)
    assert approx(p["unrealized_pnl"], 2500)
    assert approx(p["realized_pnl"], 0)
    assert approx(book["cash"], 80000)


def test_scale_in_then_close():
    # Weighted-avg entry: 100@$50 + 100@$60 → avg $55, shares=200
    # Close all at $70 → realized = 200 * ($70 - $55) = $3000
    # The engine records entry_price=$55 on the SELL row.
    trades = [
        {"date": "2025-01-02", "action": "BUY", "symbol": "X", "shares": 100, "price": 50, "amount": 5000},
        {"date": "2025-01-10", "action": "BUY", "symbol": "X", "shares": 100, "price": 60, "amount": 6000},
        {"date": "2025-02-01", "action": "SELL", "symbol": "X", "shares": 200, "price": 70, "amount": 14000, "pnl": 3000, "entry_price": 55},
    ]
    book = reconstruct_positions(trades, 100000, "2025-03-01", _price({"X": 80}))
    assert_identities(book, 100000, "scale_in_close")
    p = book["positions"][0]
    assert p["status"] == "closed"
    assert approx(p["realized_pnl"], 3000)
    assert approx(book["cash"], 103000)  # 100k - 5k - 6k + 14k


def test_partial_exit_then_ride():
    # Engine behavior: buy 100@$50, sell 40@$60 partial (engine entry_price stays $50).
    # SELL row: shares=40, entry_price=50, pnl = 40 * (60-50) = 400.
    # Remaining: 60 @ $50 entry. At as_of price $70: unrealized = 60*(70-50)=1200.
    trades = [
        {"date": "2025-01-02", "action": "BUY", "symbol": "Y", "shares": 100, "price": 50, "amount": 5000},
        {"date": "2025-02-01", "action": "SELL", "symbol": "Y", "shares": 40, "price": 60, "amount": 2400, "pnl": 400, "entry_price": 50},
    ]
    book = reconstruct_positions(trades, 100000, "2025-03-01", _price({"Y": 70}))
    assert_identities(book, 100000, "partial_exit")
    p = book["positions"][0]
    assert p["status"] == "open"
    assert approx(p["shares_held"], 60)
    assert approx(p["avg_entry"], 50)
    assert approx(p["realized_pnl"], 400)
    assert approx(p["unrealized_pnl"], 1200)
    # cash = 100000 - 5000 + 2400 = 97400
    assert approx(book["cash"], 97400)


def test_reentry_after_close():
    # Close position, then open a new lot in the same symbol. entry_price resets.
    trades = [
        {"date": "2025-01-02", "action": "BUY", "symbol": "Z", "shares": 10, "price": 100, "amount": 1000},
        {"date": "2025-02-01", "action": "SELL", "symbol": "Z", "shares": 10, "price": 120, "amount": 1200, "pnl": 200, "entry_price": 100},
        {"date": "2025-03-01", "action": "BUY", "symbol": "Z", "shares": 5, "price": 150, "amount": 750},
    ]
    book = reconstruct_positions(trades, 100000, "2025-04-01", _price({"Z": 160}))
    assert_identities(book, 100000, "reentry")
    p = book["positions"][0]
    assert p["status"] == "open"
    assert approx(p["shares_held"], 5)
    assert approx(p["avg_entry"], 150)  # reset after close
    assert approx(p["realized_pnl"], 200)
    assert approx(p["unrealized_pnl"], 50)  # 5*(160-150)
    assert p["num_round_trips"] == 1
    # cash = 100k - 1000 + 1200 - 750 = 99450
    assert approx(book["cash"], 99450)


def test_multi_symbol_multi_sleeve():
    trades = [
        {"date": "2025-01-02", "action": "BUY", "symbol": "A", "shares": 10, "price": 100, "amount": 1000, "sleeve_label": "alpha"},
        {"date": "2025-01-02", "action": "BUY", "symbol": "B", "shares": 20, "price": 50, "amount": 1000, "sleeve_label": "beta"},
        {"date": "2025-02-01", "action": "SELL", "symbol": "A", "shares": 10, "price": 110, "amount": 1100, "pnl": 100, "entry_price": 100, "sleeve_label": "alpha"},
        # Symbol "B" traded in a second sleeve too
        {"date": "2025-02-10", "action": "BUY", "symbol": "B", "shares": 10, "price": 55, "amount": 550, "sleeve_label": "gamma"},
    ]
    book = reconstruct_positions(trades, 100000, "2025-03-01", _price({"A": 120, "B": 60}))
    assert_identities(book, 100000, "multi_sym")
    assert book["open_count"] == 1
    assert book["closed_count"] == 1

    # "B" aggregates across alpha and gamma sleeves
    b = next(p for p in book["positions"] if p["symbol"] == "B")
    assert sorted(b["sleeves"]) == ["beta", "gamma"]
    assert approx(b["shares_held"], 30)
    # weighted avg: (20*50 + 10*55) / 30 = 1550/30 = 51.6667
    assert approx(b["avg_entry"], 1550 / 30)


def test_weight_pct_sums_to_total_open_percent():
    trades = [
        {"date": "2025-01-02", "action": "BUY", "symbol": "A", "shares": 10, "price": 100, "amount": 1000},
        {"date": "2025-01-02", "action": "BUY", "symbol": "B", "shares": 20, "price": 50, "amount": 1000},
    ]
    book = reconstruct_positions(trades, 10000, "2025-03-01", _price({"A": 110, "B": 60}))
    assert_identities(book, 10000, "weights")
    total_w = sum(p["weight_pct"] for p in book["positions"])
    expected = book["positions_value"] / book["portfolio_value"] * 100
    assert approx(total_w, expected, tol=1e-4)


def test_missing_price_for_open():
    # Delisted or no bar on as_of — current_price falls back to 0. Identities still hold.
    trades = [
        {"date": "2025-01-02", "action": "BUY", "symbol": "DEAD", "shares": 10, "price": 100, "amount": 1000},
    ]
    book = reconstruct_positions(trades, 10000, "2025-03-01", _price({}))
    assert_identities(book, 10000, "missing_price")
    p = book["positions"][0]
    assert approx(p["current_price"], 0)
    assert approx(p["market_value"], 0)
    # unrealized = 0 - 1000 = -1000
    assert approx(p["unrealized_pnl"], -1000)


if __name__ == "__main__":
    tests = [
        test_full_round_trip_closed,
        test_open_at_end,
        test_scale_in_then_close,
        test_partial_exit_then_ride,
        test_reentry_after_close,
        test_multi_symbol_multi_sleeve,
        test_weight_pct_sums_to_total_open_percent,
        test_missing_price_for_open,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL  {t.__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(failed)
