"""In-process helpers that feed the analyst orchestrator.

Each function takes an experiment_id and returns a serializable dict (or list)
ready to drop into the analyst prompt context. No LLM calls here — these are
deterministic data fetchers / reconstructors.

The four helpers:
  - read_thesis(experiment_id)             — what the strategy said it would do
  - get_trade_log(experiment_id)           — every BUY/SELL with reason + P&L
  - get_position_contribution(experiment_id) — per-symbol realized P&L
  - get_portfolio_timeseries(experiment_id)  — daily NAV + drawdown,
        reconstructed from trades + market prices (no per-experiment NAV is
        persisted by the runner, so we mark-to-market day by day)
"""
from __future__ import annotations

import sqlite3
from typing import Any

from auto_trader.schema import get_db
from auto_trader.tools import _market_db


# ---------------------------------------------------------------------------
# 1. Thesis (trivial column read)
# ---------------------------------------------------------------------------
def read_thesis(experiment_id: str) -> dict[str, Any]:
    """Return thesis + assumptions + portfolio_config summary as a dict."""
    app = get_db()
    row = app.execute(
        """SELECT thesis, assumptions, portfolio_config, target_metric,
                  target_value, backtest_start, backtest_end, initial_capital,
                  run_id, iteration
           FROM experiments WHERE id = ?""", (experiment_id,)
    ).fetchone()
    app.close()
    if not row:
        return {"error": f"experiment {experiment_id} not found"}
    return {
        "experiment_id": experiment_id,
        "run_id": row["run_id"],
        "iteration": row["iteration"],
        "thesis": row["thesis"],
        "assumptions": row["assumptions"],
        "target_metric": row["target_metric"],
        "target_value": row["target_value"],
        "window": {"start": row["backtest_start"], "end": row["backtest_end"]},
        "initial_capital": row["initial_capital"],
        # portfolio_config is JSON-encoded; pass through as a string so the
        # caller can parse it once (and decide whether to summarize).
        "portfolio_config_json": row["portfolio_config"],
    }


# ---------------------------------------------------------------------------
# 2. Trade log
# ---------------------------------------------------------------------------
def get_trade_log(experiment_id: str,
                   max_rows: int = 2000,
                   ) -> dict[str, Any]:
    """All trades for an experiment, ordered by (date, action).

    Columns: date, action, symbol, shares, price, amount, reason,
    sleeve_label, pnl, pnl_pct, days_held, entry_date, entry_price.

    Capped at `max_rows` (default 2000). Returns counts + a `truncated` flag.
    """
    # window_label IS NULL = full backtest trades; non-null = walk-forward
    # sub-window trades. We want the full backtest (matches total_return_pct).
    app = get_db()
    cur = app.execute(
        """SELECT date, action, symbol, shares, price, amount,
                  reason, sleeve_label, pnl, pnl_pct, days_held,
                  entry_date, entry_price
           FROM trades
           WHERE source_type = 'experiment' AND source_id = ?
             AND window_label IS NULL
           ORDER BY date ASC, action ASC
           LIMIT ?""", (experiment_id, max_rows + 1)
    )
    rows = [dict(r) for r in cur.fetchall()]
    n_total = app.execute(
        """SELECT COUNT(*) FROM trades
           WHERE source_type='experiment' AND source_id=?
             AND window_label IS NULL""",
        (experiment_id,)
    ).fetchone()[0]
    app.close()
    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]
    return {
        "experiment_id": experiment_id,
        "n_trades_total": n_total,
        "n_returned": len(rows),
        "truncated": truncated,
        "trades": rows,
    }


# ---------------------------------------------------------------------------
# 3. Per-position contribution (realized P&L)
# ---------------------------------------------------------------------------
def get_position_contribution(experiment_id: str) -> dict[str, Any]:
    """Realized P&L aggregated by symbol (SELL trades only — open positions
    excluded because they have no closing print).

    Returns:
      - by_symbol: list[{symbol, n_round_trips, total_pnl, avg_pnl_pct,
                          win_rate, avg_days_held}]
      - winners_top5, losers_top5: pre-sliced for convenience
      - n_open_positions: count of names still open at experiment end
    """
    app = get_db()
    cur = app.execute(
        """SELECT symbol, pnl, pnl_pct, days_held
           FROM trades
           WHERE source_type='experiment' AND source_id=?
             AND window_label IS NULL
             AND action='SELL' AND pnl IS NOT NULL""", (experiment_id,)
    )
    by_symbol: dict[str, dict[str, Any]] = {}
    for sym, pnl, pct, dh in cur.fetchall():
        s = by_symbol.setdefault(sym, {
            "symbol": sym, "n_round_trips": 0, "total_pnl": 0.0,
            "wins": 0, "pct_sum": 0.0, "dh_sum": 0,
        })
        s["n_round_trips"] += 1
        s["total_pnl"] += float(pnl)
        if pnl > 0:
            s["wins"] += 1
        if pct is not None:
            s["pct_sum"] += float(pct)
        if dh is not None:
            s["dh_sum"] += int(dh)

    # Open positions: BUYs without a paired SELL by experiment end. Cheap
    # proxy via linked_trade_id (NULL on unclosed BUYs).
    n_open = app.execute(
        """SELECT COUNT(*) FROM trades
           WHERE source_type='experiment' AND source_id=?
             AND window_label IS NULL
             AND action='BUY' AND linked_trade_id IS NULL""", (experiment_id,)
    ).fetchone()[0]
    app.close()

    out_rows = []
    for s in by_symbol.values():
        n = s["n_round_trips"]
        out_rows.append({
            "symbol": s["symbol"],
            "n_round_trips": n,
            "total_pnl": round(s["total_pnl"], 2),
            "avg_pnl_pct": round(s["pct_sum"] / n, 3) if n else None,
            "win_rate": round(s["wins"] / n, 3) if n else None,
            "avg_days_held": round(s["dh_sum"] / n, 1) if n else None,
        })
    out_rows.sort(key=lambda r: r["total_pnl"], reverse=True)
    return {
        "experiment_id": experiment_id,
        "n_symbols": len(out_rows),
        "n_open_positions": n_open,
        "by_symbol": out_rows,
        "winners_top5": out_rows[:5],
        "losers_top5": list(reversed(out_rows[-5:])) if len(out_rows) >= 5 else [],
    }


# ---------------------------------------------------------------------------
# 4. Portfolio time series (reconstructed)
# ---------------------------------------------------------------------------
def get_portfolio_timeseries(experiment_id: str,
                              max_points: int = 260,
                              ) -> dict[str, Any]:
    """Daily NAV + drawdown for an experiment, reconstructed from the trade
    ledger using the same weighted-average cost-basis accounting as the
    canonical position-book reconstructor (scripts/portfolio_book.py).

    The runner doesn't persist per-experiment NAV, so we replay trades and
    snapshot the book at downsampled dates. The accounting identity is:

        cash       = initial_capital - Σ open_position_cost + Σ realized_pnl
        nav        = cash + Σ shares_held(sym) × close(sym, t)

    This is structurally different from naive ΣBUY/ΣSELL cash tracking —
    the engine uses weighted-average cost basis, and SELL `amount` includes
    cost recovery (not pure income). Using realized `pnl` from the SELL row
    is the only way to keep the books balanced.

    Returns:
      window:        {start, end, n_days}
      summary:       {initial_capital, final_nav, total_return_pct,
                      max_drawdown_pct, max_drawdown_date}
      series:        list[{date, nav, drawdown_pct}] — downsampled to
                     ≤ max_points uniformly across the window
    """
    app = get_db()
    erow = app.execute(
        """SELECT backtest_start, backtest_end, initial_capital
           FROM experiments WHERE id = ?""", (experiment_id,)
    ).fetchone()
    if not erow:
        app.close()
        return {"error": f"experiment {experiment_id} not found"}
    start, end = erow["backtest_start"], erow["backtest_end"]
    initial_capital = float(erow["initial_capital"] or 0)
    if not start or not end or initial_capital <= 0:
        app.close()
        return {"error": "experiment missing start/end/initial_capital"}

    trades = app.execute(
        """SELECT date, action, symbol, shares, price, amount, pnl, entry_price
           FROM trades
           WHERE source_type='experiment' AND source_id=?
             AND window_label IS NULL
           ORDER BY date ASC""", (experiment_id,)
    ).fetchall()
    app.close()
    if not trades:
        return {"error": "experiment has no trades"}

    symbols = sorted({t["symbol"] for t in trades})
    mkt = _market_db()
    placeholders = ",".join("?" * len(symbols))
    price_rows = mkt.execute(
        f"""SELECT date, symbol, close FROM prices
            WHERE symbol IN ({placeholders})
              AND date >= ? AND date <= ?
            ORDER BY date""", (*symbols, start, end)
    ).fetchall()
    cal_rows = mkt.execute(
        "SELECT DISTINCT date FROM prices WHERE symbol='SPY' "
        "AND date >= ? AND date <= ? ORDER BY date", (start, end)
    ).fetchall()
    mkt.close()
    if not cal_rows:
        return {"error": f"no SPY trading days in {start}..{end}"}

    trading_dates = [r["date"] for r in cal_rows]
    close_by_date_sym: dict[str, dict[str, float]] = {}
    for r in price_rows:
        if r["close"] is not None:
            close_by_date_sym.setdefault(r["date"], {})[r["symbol"]] = float(r["close"])

    # Sort trades: BUY before SELL on same date (matches portfolio_book.py:65
    # and the engine's intraday convention).
    sorted_trades = sorted(
        (dict(t) for t in trades),
        key=lambda t: (t["date"], 0 if t["action"] == "BUY" else 1),
    )
    trades_by_date: dict[str, list[dict]] = {}
    for t in sorted_trades:
        trades_by_date.setdefault(t["date"], []).append(t)

    # Weighted-average cost-basis state per symbol — mirrors
    # portfolio_book.reconstruct_positions exactly.
    SHARE_EPS = 1e-3
    per_symbol: dict[str, dict[str, float]] = {}
    last_close: dict[str, float] = {}
    nav_series: list[tuple[str, float]] = []

    def _get(sym: str) -> dict[str, float]:
        rec = per_symbol.get(sym)
        if rec is None:
            rec = {"shares": 0.0, "entry_price": 0.0, "realized_pnl": 0.0}
            per_symbol[sym] = rec
        return rec

    for d in trading_dates:
        for t in trades_by_date.get(d, []):
            sym = t["symbol"]
            shares = float(t["shares"] or 0)
            price = float(t["price"] or 0)
            rec = _get(sym)
            if t["action"] == "BUY":
                if rec["shares"] <= SHARE_EPS:
                    rec["shares"] = shares
                    rec["entry_price"] = price
                else:
                    total_cost = rec["shares"] * rec["entry_price"] + shares * price
                    rec["shares"] += shares
                    rec["entry_price"] = (
                        total_cost / rec["shares"] if rec["shares"] else 0.0
                    )
            elif t["action"] == "SELL":
                rec["shares"] -= shares
                rec["realized_pnl"] += float(t.get("pnl") or 0)
                if rec["shares"] <= SHARE_EPS:
                    rec["shares"] = 0.0
                    rec["entry_price"] = 0.0

        todays = close_by_date_sym.get(d, {})
        for sym, p in todays.items():
            last_close[sym] = p

        total_open_cost = 0.0
        total_realized = 0.0
        positions_value = 0.0
        for sym, rec in per_symbol.items():
            sh = rec["shares"]
            total_realized += rec["realized_pnl"]
            if sh <= 0:
                continue
            total_open_cost += sh * rec["entry_price"]
            p = todays.get(sym) or last_close.get(sym)
            if p is not None:
                positions_value += sh * p
            else:
                # No price for an open position — fall back to cost basis
                # so NAV doesn't artificially drop on a delisting day.
                positions_value += sh * rec["entry_price"]
        cash = initial_capital - total_open_cost + total_realized
        nav_series.append((d, cash + positions_value))

    if not nav_series:
        return {"error": "could not reconstruct any NAV points"}

    final_nav = nav_series[-1][1]
    total_return_pct = (final_nav / initial_capital - 1.0) * 100.0

    # Drawdown trajectory.
    peak = -float("inf")
    dd_pct_series: list[float] = []
    max_dd = 0.0
    max_dd_date: str | None = None
    for d, nav in nav_series:
        peak = max(peak, nav)
        dd = (nav / peak - 1.0) * 100.0 if peak > 0 else 0.0
        dd_pct_series.append(dd)
        if dd < max_dd:
            max_dd = dd
            max_dd_date = d

    # Downsample evenly to max_points so the prompt context stays small.
    n = len(nav_series)
    if n > max_points:
        step = n / max_points
        idxs = sorted({int(i * step) for i in range(max_points)})
        # Always include first and last point.
        idxs = sorted(set(idxs + [0, n - 1]))
    else:
        idxs = list(range(n))

    series_out = [
        {"date": nav_series[i][0],
         "nav": round(nav_series[i][1], 2),
         "drawdown_pct": round(dd_pct_series[i], 3)}
        for i in idxs
    ]

    return {
        "experiment_id": experiment_id,
        "window": {"start": start, "end": end, "n_days": n},
        "summary": {
            "initial_capital": initial_capital,
            "final_nav": round(final_nav, 2),
            "total_return_pct": round(total_return_pct, 3),
            "max_drawdown_pct": round(max_dd, 3),
            "max_drawdown_date": max_dd_date,
        },
        "series": series_out,
    }
