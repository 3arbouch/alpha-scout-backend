"""
PositionBook — single live position book for the v2 portfolio engine.

Phase 2 of the live-trading plan replaces v1's per-sleeve mini-simulations
+ portfolio-level lerp with one unified position book at the portfolio
level. Each Position is tagged with its `sleeve_label` for attribution,
but lives in one shared book keyed by (sleeve_label, symbol).

DISTINCT FROM `scripts/portfolio_book.py`: that module is a READ-ONLY
ledger→snapshot reconstructor used by the `/positions` API endpoint to
derive position state from a persisted trade log. This module is the
LIVE STATE used during simulation — it's the source of truth that the
trade log records.

Design choices:
  - Same-sleeve add-on (entry + later earnings_beat add) merges into ONE
    Position with weighted-average entry_price. Matches v1 semantics.
  - Same-symbol cross-sleeve (rare — two sleeves both holding AAPL) is
    stored as TWO separate Position records, one per sleeve. Each
    sleeve's exits only touch its own position. Cross-sleeve attribution
    is preserved.
  - All operations return trade-record dicts that the executor accumulates
    into the unified trade ledger. The book itself doesn't keep a trade
    log — the executor owns the ledger.
  - Cash is a single scalar. Debited on BUY, credited on SELL. No per-
    sleeve cash buckets — that's what gave v1 the dual-bookkeeping bug.

Trade-record schema (matches v1 Portfolio.open_position / close_position
for downstream compatibility with the existing trade log persistence):
    BUY:  {date, symbol, action="BUY", reason, price, shares, amount,
           sleeve_label, signal_detail?}
    SELL: {date, symbol, action="SELL", reason, price, shares, amount,
           pnl, pnl_pct, entry_date, entry_price, days_held,
           sleeve_label, signal_detail?}
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Position — one open lot, tagged with the sleeve that originated it.
# ---------------------------------------------------------------------------
@dataclass
class Position:
    """A single open lot in the portfolio, tagged with its sleeve.

    Same-sleeve add-ons merge into this object via PositionBook.open,
    updating `entry_price` to the weighted average across all shares.

    Position is logic-free state — exit checks (stop_loss, take_profit,
    time_stop) live in the executor / sleeve signal generators, not here.
    """
    sleeve_label: str
    symbol: str
    entry_date: str             # ISO date of the FIRST entry (preserved across add-ons)
    entry_price: float          # weighted-avg per-share cost basis
    shares: float
    peak_price: float           # for above_peak take_profit (pre-entry peak)
    high_since_entry: float     # trailing reference; updated on observe_price
    signal_detail: Optional[dict] = None
    stop_price: Optional[float] = None       # frozen stop (vol-adaptive modes)
    take_profit_price: Optional[float] = None  # frozen TP

    def market_value(self, price: float) -> float:
        return float(price) * self.shares

    def pnl_pct(self, price: float) -> float:
        if self.entry_price <= 0:
            return 0.0
        return ((float(price) - self.entry_price) / self.entry_price) * 100.0

    def days_held(self, current_date: str) -> int:
        d0 = datetime.strptime(self.entry_date, "%Y-%m-%d")
        d1 = datetime.strptime(current_date, "%Y-%m-%d")
        return (d1 - d0).days

    def observe_price(self, price: float) -> None:
        """Update trailing high since entry (used by above_peak TP / trailing stop)."""
        if price > self.high_since_entry:
            self.high_since_entry = price


# Wildcard sleeve label for single-pool (backward-compatibility) cash mode.
_DEFAULT_CASH_POOL = "*"


# ---------------------------------------------------------------------------
# PositionBook — the single unified live position state + cash.
# ---------------------------------------------------------------------------
class PositionBook:
    """Unified position book across all sleeves in a portfolio.

    Positions are keyed by (sleeve_label, symbol):
      - Same-sleeve add-ons merge into ONE Position (weighted-avg basis).
      - Two different sleeves holding the same symbol are TWO Positions.

    Cash is tracked PER-SLEEVE. Multi-sleeve fixed-weight portfolios need
    this so each sleeve's sizing respects its own NAV trajectory (matching
    v1's per-sleeve standalone simulation semantics).

    Two construction modes:
      - Scalar: PositionBook(100_000) creates a single-pool mode where all
        cash lives under sleeve label "*". open()/sell() with any sleeve_label
        fall through to that pool. Backward-compatible with single-sleeve usage.
      - Dict:   PositionBook({"Tech": 70_000, "Def": 30_000}) creates per-
        sleeve pools. open("Tech", ...) debits cash_by_sleeve["Tech"] only.
    """

    def __init__(self, initial_cash):
        if isinstance(initial_cash, (int, float)):
            if initial_cash < 0:
                raise ValueError(f"initial_cash must be non-negative, got {initial_cash}")
            self.cash_by_sleeve: dict[str, float] = {_DEFAULT_CASH_POOL: float(initial_cash)}
            self._initial_cash = float(initial_cash)
        elif isinstance(initial_cash, dict):
            negatives = {k: v for k, v in initial_cash.items() if v < 0}
            if negatives:
                raise ValueError(f"all per-sleeve cash values must be non-negative; got {negatives}")
            self.cash_by_sleeve = {k: float(v) for k, v in initial_cash.items()}
            self._initial_cash = sum(self.cash_by_sleeve.values())
        else:
            raise TypeError(
                "initial_cash must be a scalar or a dict[sleeve_label, amount]; "
                f"got {type(initial_cash).__name__}"
            )
        self.positions: dict[tuple[str, str], Position] = {}
        # SELL trades only; for win/loss / round-trip stats. Executor still
        # owns the full trade ledger (BUYs + SELLs).
        self.closed_trades: list[dict] = []
        self.nav_history: list[dict] = []

    # -----------------------------------------------------------------------
    # Cash accessors — per-sleeve aware
    # -----------------------------------------------------------------------
    @property
    def cash(self) -> float:
        """Total cash across all sleeve pools."""
        return sum(self.cash_by_sleeve.values())

    def sleeve_cash(self, sleeve_label: str) -> float:
        """Cash available to a specific sleeve. In single-pool mode (constructor
        called with a scalar), returns the shared pool's balance regardless of
        the label."""
        if sleeve_label in self.cash_by_sleeve:
            return self.cash_by_sleeve[sleeve_label]
        if _DEFAULT_CASH_POOL in self.cash_by_sleeve:
            return self.cash_by_sleeve[_DEFAULT_CASH_POOL]
        return 0.0

    def _resolve_cash_pool(self, sleeve_label: str) -> str:
        """Which pool key handles BUYs/SELLs for `sleeve_label`?
        Prefers the explicit per-sleeve pool, falls back to the shared wildcard."""
        if sleeve_label in self.cash_by_sleeve:
            return sleeve_label
        if _DEFAULT_CASH_POOL in self.cash_by_sleeve:
            return _DEFAULT_CASH_POOL
        # Neither exists — auto-create an empty per-sleeve pool so the BUY
        # safely returns None (insufficient funds) without KeyError.
        self.cash_by_sleeve[sleeve_label] = 0.0
        return sleeve_label

    def sleeve_nav(self, sleeve_label: str, price_index: dict, date: str) -> float:
        """sleeve cash + market value of positions tagged with this sleeve."""
        cash = self.sleeve_cash(sleeve_label)
        pv = 0.0
        for (lbl, _), pos in self.positions.items():
            if lbl != sleeve_label:
                continue
            price = price_index.get(pos.symbol, {}).get(date)
            if price is None:
                price = pos.high_since_entry
            pv += pos.market_value(price)
        return cash + pv

    # -----------------------------------------------------------------------
    # Accessors
    # -----------------------------------------------------------------------
    @property
    def initial_cash(self) -> float:
        return self._initial_cash

    def get(self, sleeve_label: str, symbol: str) -> Position | None:
        return self.positions.get((sleeve_label, symbol))

    def has(self, sleeve_label: str, symbol: str) -> bool:
        return (sleeve_label, symbol) in self.positions

    def positions_for_sleeve(self, sleeve_label: str) -> dict[str, Position]:
        """All open positions tagged with this sleeve, keyed by symbol."""
        return {sym: p for (lbl, sym), p in self.positions.items()
                if lbl == sleeve_label}

    def symbols_held_by_sleeve(self, sleeve_label: str) -> set[str]:
        return {sym for (lbl, sym) in self.positions if lbl == sleeve_label}

    def all_positions(self) -> list[Position]:
        return list(self.positions.values())

    def num_positions(self, sleeve_label: str | None = None) -> int:
        if sleeve_label is None:
            return len(self.positions)
        return sum(1 for (lbl, _) in self.positions if lbl == sleeve_label)

    def positions_value(self, price_index: dict, date: str) -> float:
        """Mark-to-market value of all open positions on `date`."""
        total = 0.0
        for pos in self.positions.values():
            price = price_index.get(pos.symbol, {}).get(date)
            if price is not None:
                total += pos.market_value(price)
            else:
                # Stale price (halt / data gap) — carry at last observed high
                total += pos.market_value(pos.high_since_entry)
        return total

    def nav(self, price_index: dict, date: str) -> float:
        return self.cash + self.positions_value(price_index, date)

    # -----------------------------------------------------------------------
    # Open / add-on
    # -----------------------------------------------------------------------
    def open(
        self,
        sleeve_label: str,
        symbol: str,
        date: str,
        amount: float,
        exec_price: float,
        peak_price: float | None = None,
        signal_detail: dict | None = None,
        stop_price: float | None = None,
        take_profit_price: float | None = None,
        slippage_bps: float = 0,
        reason: str = "entry",
        min_amount: float = 1.0,
        shares_mode: str | None = None,
    ) -> dict | None:
        """Open a new position or add to an existing (sleeve, symbol) position.

        `amount` is the dollar notional to deploy at `exec_price`. Slippage
        is applied at fill: BUY pays exec_price × (1 + slippage_bps / 10000).

        Behavior:
          - If amount exceeds cash × 0.99 (cash buffer), it's CAPPED, not
            rejected. Matches v1 convention so live-tradeable order sizing
            doesn't silently fail at the boundary.
          - If the capped amount is below `min_amount` (default $1), the
            trade is skipped and None is returned — too small to bother.
          - Add-ons within the same (sleeve, symbol) update entry_price to
            the weighted average across old + new shares. entry_date stays
            the ORIGINAL first-entry date (matches v1).
          - Different sleeves opening the same symbol stay as separate
            Position records.

        Returns the BUY trade record, or None if the trade was skipped.
        """
        if amount is None or amount <= min_amount:
            return None
        if slippage_bps:
            fill_price = float(exec_price) * (1.0 + slippage_bps / 10_000.0)
        else:
            fill_price = float(exec_price)
        if fill_price <= 0:
            return None

        # Per-sleeve cash buffer cap (1% buffer like v1)
        pool_key = self._resolve_cash_pool(sleeve_label)
        max_amount = self.cash_by_sleeve[pool_key] * 0.99
        if amount > max_amount:
            amount = max_amount
        if amount <= min_amount:
            return None

        shares = amount / fill_price
        # Whole-share constraint (real broker reality for non-fractional venues).
        # Mirrors v1 Portfolio.open_position (backtest_engine.py:2063-2068):
        # floor shares, recompute amount, skip if rounded to 0. Default
        # `fractional` matches v2's historical behavior; pass shares_mode="whole"
        # to enforce broker-realistic sizing.
        if shares_mode == "whole":
            import math
            shares = math.floor(shares)
            if shares <= 0:
                return None
            amount = shares * fill_price
        if shares <= 0:
            return None

        key = (sleeve_label, symbol)
        existing = self.positions.get(key)

        if existing is None:
            self.positions[key] = Position(
                sleeve_label=sleeve_label,
                symbol=symbol,
                entry_date=date,
                entry_price=fill_price,
                shares=shares,
                peak_price=peak_price if peak_price is not None else fill_price,
                high_since_entry=fill_price,
                signal_detail=signal_detail,
                stop_price=stop_price,
                take_profit_price=take_profit_price,
            )
        else:
            # Add-on: weighted average across all open shares for this sleeve+symbol.
            total_cost = existing.shares * existing.entry_price + shares * fill_price
            existing.shares += shares
            existing.entry_price = total_cost / existing.shares
            # Don't update entry_date (preserve first-entry semantics).
            # Don't update peak_price (pre-entry reference is from the original entry).

        self.cash_by_sleeve[pool_key] -= amount

        trade = {
            "date": date,
            "symbol": symbol,
            "action": "BUY",
            "reason": reason,
            # Rounding matches v1 backtest_engine.Portfolio.open_position so
            # ledger comparisons are byte-identical for parity tests.
            "price": round(fill_price, 2),
            "shares": round(shares, 4),
            "amount": round(amount, 2),
            "sleeve_label": sleeve_label,
        }
        if signal_detail is not None:
            trade["signal_detail"] = signal_detail
        return trade

    # -----------------------------------------------------------------------
    # Sell — partial or full
    # -----------------------------------------------------------------------
    def sell(
        self,
        sleeve_label: str,
        symbol: str,
        date: str,
        exec_price: float,
        reason: str,
        shares: float | None = None,
        slippage_bps: float = 0,
    ) -> dict | None:
        """Sell from the (sleeve, symbol) position.

        shares=None    → close the whole position.
        shares=X       → sell X (capped at held).

        Returns the SELL trade record with pnl/pnl_pct/days_held populated,
        or None if no such position exists or sell_shares ≤ 0.

        Slippage on SELL: receives exec_price × (1 - slippage_bps / 10000).
        """
        key = (sleeve_label, symbol)
        pos = self.positions.get(key)
        if pos is None:
            return None

        if shares is None:
            sell_shares = pos.shares
        else:
            sell_shares = min(float(shares), pos.shares)
        if sell_shares <= 0:
            return None

        if slippage_bps:
            fill_price = float(exec_price) * (1.0 - slippage_bps / 10_000.0)
        else:
            fill_price = float(exec_price)
        if fill_price <= 0:
            return None

        proceeds = sell_shares * fill_price
        cost_basis = sell_shares * pos.entry_price
        pnl = proceeds - cost_basis
        pnl_pct = ((fill_price - pos.entry_price) / pos.entry_price) * 100.0 \
            if pos.entry_price > 0 else 0.0
        days_held = pos.days_held(date)

        # Credit proceeds back to the sleeve's cash pool (same pool that paid for the BUY)
        pool_key = self._resolve_cash_pool(sleeve_label)
        self.cash_by_sleeve[pool_key] += proceeds

        trade = {
            "date": date,
            "symbol": symbol,
            "action": "SELL",
            "reason": reason,
            # Rounding matches v1 backtest_engine.Portfolio.close_position.
            "price": round(fill_price, 2),
            "shares": round(sell_shares, 4),
            "amount": round(proceeds, 2),
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
            "entry_date": pos.entry_date,
            "entry_price": round(pos.entry_price, 2),
            "days_held": days_held,
            "sleeve_label": sleeve_label,
        }
        if pos.signal_detail is not None:
            trade["signal_detail"] = pos.signal_detail
        self.closed_trades.append(trade)

        # Apply the sell to the position
        if sell_shares >= pos.shares - 1e-9:
            # Full close
            del self.positions[key]
        else:
            pos.shares -= sell_shares
            # entry_price unchanged on partial — the remaining lot's basis
            # is the same weighted-avg basis we sold from.

        return trade

    # -----------------------------------------------------------------------
    # NAV snapshot
    # -----------------------------------------------------------------------
    def record_nav(self, price_index: dict, date: str) -> dict:
        """Append a NAV snapshot and return it.

        Snapshot fields (mirror v1 nav_history for downstream compatibility):
          date, nav, cash, positions_value, num_positions, daily_pnl,
          daily_pnl_pct, positions (per-symbol detail),
          per_sleeve_positions_value (NEW — attribution).

        Stale-price handling: if `date` has no price for a held symbol, the
        position is valued at its last-observed high. Same as v1.
        """
        positions_value = 0.0
        position_details: dict[str, dict] = {}
        per_sleeve_pv: dict[str, float] = {}

        for (sleeve_label, sym), pos in self.positions.items():
            price = price_index.get(sym, {}).get(date)
            if price is None:
                price = pos.high_since_entry
            pos.observe_price(price)
            mv = pos.market_value(price)
            positions_value += mv
            per_sleeve_pv[sleeve_label] = per_sleeve_pv.get(sleeve_label, 0.0) + mv

            # Position detail. If two sleeves hold the same symbol, this
            # collapses them into one row keyed by symbol (matches v1).
            # `sleeve_label` here records the FIRST sleeve seen for the
            # symbol; per-sleeve breakdown is in `per_sleeve_positions_value`.
            d = position_details.setdefault(sym, {
                "price": round(price, 4),
                "shares": 0.0,
                "market_value": 0.0,
                "entry_price": pos.entry_price,
                "entry_date": pos.entry_date,
                "sleeve_label": sleeve_label,
            })
            d["shares"] = round(d["shares"] + pos.shares, 6)
            d["market_value"] = round(d["market_value"] + mv, 2)

        snapshot = {
            "date": date,
            "nav": round(self.cash + positions_value, 2),
            "cash": round(self.cash, 2),
            "positions_value": round(positions_value, 2),
            "num_positions": len(self.positions),
            "positions": position_details,
            "per_sleeve_positions_value": {k: round(v, 2) for k, v in per_sleeve_pv.items()},
        }
        if self.nav_history:
            prev_nav = self.nav_history[-1]["nav"]
            snapshot["daily_pnl"] = round(snapshot["nav"] - prev_nav, 2)
            snapshot["daily_pnl_pct"] = round(
                (snapshot["nav"] - prev_nav) / prev_nav * 100.0 if prev_nav > 0 else 0.0,
                4,
            )
        else:
            snapshot["daily_pnl"] = round(snapshot["nav"] - self._initial_cash, 2)
            snapshot["daily_pnl_pct"] = round(
                (snapshot["nav"] - self._initial_cash) / self._initial_cash * 100.0
                if self._initial_cash > 0 else 0.0, 4,
            )
        self.nav_history.append(snapshot)
        return snapshot

    # -----------------------------------------------------------------------
    # Inspection
    # -----------------------------------------------------------------------
    def __repr__(self) -> str:
        return (f"PositionBook(cash=${self.cash:,.2f}, "
                f"positions={len(self.positions)}, "
                f"nav_history_len={len(self.nav_history)})")
