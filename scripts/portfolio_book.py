"""
Position book reconstruction from the trade ledger.

The trade blotter (`trades` table) is the source of truth. This module derives
a portfolio's position book — per-ticker shares, cost basis, realized and
unrealized pnl, cash, portfolio value — from the ledger alone, evaluated at
any `as_of_date`.

Convention: the backtest/deploy engine uses WEIGHTED-AVERAGE cost basis.
Scaling into an existing position recomputes entry_price as the cost-weighted
average across all open shares (see Portfolio.open_position in
backtest_engine.py). This reconstructor replays that rule exactly, so
reconstructed numbers match what the engine recorded trade-by-trade.

Identity that always holds:
    cash + positions_value == portfolio_value
    total_realized + total_unrealized == portfolio_value - initial_capital
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Callable, Optional

# Ledger drift tolerance. The engine records shares to 4 decimal places
# (backtest_engine.Portfolio.open_position rounds at write time), so a sequence
# of partial exits can leave rounding crumbs below 1e-3 shares. Treat anything
# below this threshold as fully closed to match the engine's internal state.
SHARE_EPS = 1e-3


def reconstruct_positions(
    trades: list[dict],
    initial_capital: float,
    as_of_date: str,
    price_lookup: Callable[[str, str], Optional[float]],
) -> dict:
    """Rebuild a position book from a trade ledger.

    Parameters
    ----------
    trades : list[dict]
        Rows from the `trades` table. Required fields per row:
        date, action ("BUY"/"SELL"), symbol, shares, price, amount.
        Optional: pnl, entry_price, sleeve_label.
    initial_capital : float
        Starting cash for the portfolio.
    as_of_date : str
        YYYY-MM-DD; current_price is looked up as of this date.
    price_lookup : callable
        (symbol, as_of_date) -> close price or None when no bar is available.
        Callers typically pass a function that returns the most recent close
        on or before `as_of_date` to handle weekends/holidays/delistings.

    Returns
    -------
    dict with the same shape as server.api._build_position_book — matches the
    `/deployments/{id}/positions` response.
    """
    # Chronological order; BUY before SELL on the same date to avoid negative
    # intermediate share counts on a same-day flip.
    trades = sorted(
        trades,
        key=lambda t: (t["date"], 0 if t["action"] == "BUY" else 1),
    )

    per_symbol: dict[str, dict] = defaultdict(lambda: {
        "shares": 0.0,
        "entry_price": 0.0,       # weighted-avg while shares > 0
        "realized_pnl": 0.0,
        "realized_cost": 0.0,     # Σ (shares_sold × entry_price_at_sell)
        "num_round_trips": 0,
        "sleeves": [],
    })

    for t in trades:
        sym = t["symbol"]
        rec = per_symbol[sym]

        label = t.get("sleeve_label")
        if label and label not in rec["sleeves"]:
            rec["sleeves"].append(label)

        action = t["action"]
        shares = float(t["shares"] or 0)
        price = float(t["price"] or 0)

        if action == "BUY":
            if rec["shares"] <= SHARE_EPS:
                rec["shares"] = shares
                rec["entry_price"] = price
            else:
                total_cost = rec["shares"] * rec["entry_price"] + shares * price
                rec["shares"] += shares
                rec["entry_price"] = total_cost / rec["shares"] if rec["shares"] else 0
        elif action == "SELL":
            rec["shares"] -= shares
            rec["realized_pnl"] += float(t.get("pnl") or 0)
            rec["realized_cost"] += shares * float(t.get("entry_price") or 0)
            rec["num_round_trips"] += 1
            if rec["shares"] <= SHARE_EPS:
                rec["shares"] = 0.0
                rec["entry_price"] = 0.0

    positions: list[dict] = []
    total_open_cost = 0.0
    total_realized = 0.0
    total_unrealized = 0.0
    positions_value = 0.0

    for sym, rec in per_symbol.items():
        shares_held = rec["shares"]
        avg_entry = rec["entry_price"] if shares_held > 0 else 0.0
        cost_basis_open = shares_held * avg_entry
        current_price = price_lookup(sym, as_of_date) or 0.0
        market_value = shares_held * current_price
        unrealized = (market_value - cost_basis_open) if shares_held > 0 else 0.0

        total_cost = cost_basis_open + rec["realized_cost"]
        total_pnl = rec["realized_pnl"] + unrealized
        total_pnl_pct = (total_pnl / total_cost * 100) if total_cost else 0.0

        total_open_cost += cost_basis_open
        total_realized += rec["realized_pnl"]
        total_unrealized += unrealized
        positions_value += market_value

        positions.append({
            "symbol": sym,
            "status": "open" if shares_held > 0 else "closed",
            "shares_held": shares_held,
            "avg_entry": avg_entry,
            "current_price": current_price,
            "market_value": market_value,
            "unrealized_pnl": unrealized,
            "realized_pnl": rec["realized_pnl"],
            "total_pnl": total_pnl,
            "total_pnl_pct": total_pnl_pct,
            "num_round_trips": rec["num_round_trips"],
            "sleeves": rec["sleeves"],
        })

    cash = initial_capital - total_open_cost + total_realized
    portfolio_value = cash + positions_value
    total_pnl = total_realized + total_unrealized

    for p in positions:
        p["weight_pct"] = (
            (p["market_value"] / portfolio_value * 100)
            if portfolio_value and p["market_value"]
            else 0.0
        )

    positions.sort(key=lambda p: p["total_pnl"], reverse=True)

    return {
        "positions": positions,
        "cash": cash,
        "positions_value": positions_value,
        "portfolio_value": portfolio_value,
        "total_pnl": total_pnl,
        "total_realized_pnl": total_realized,
        "total_unrealized_pnl": total_unrealized,
        "open_count": sum(1 for p in positions if p["status"] == "open"),
        "closed_count": sum(1 for p in positions if p["status"] == "closed"),
    }


def make_price_lookup(market_db_path: str) -> Callable[[str, str], Optional[float]]:
    """Build a cached price-lookup backed by `prices` in market.db.

    Returns the close on `as_of` if present, else the most recent close on or
    before `as_of` (handles weekends/holidays/delistings).
    """
    cache: dict[tuple[str, str], Optional[float]] = {}

    def lookup(symbol: str, as_of: str) -> Optional[float]:
        key = (symbol, as_of)
        if key in cache:
            return cache[key]
        conn = sqlite3.connect(market_db_path)
        try:
            row = conn.execute(
                "SELECT close FROM prices WHERE symbol = ? AND date <= ? "
                "ORDER BY date DESC LIMIT 1",
                (symbol, as_of),
            ).fetchone()
        finally:
            conn.close()
        val = float(row[0]) if row and row[0] is not None else None
        cache[key] = val
        return val

    return lookup
