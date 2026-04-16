"""
Signal Evaluator & Ranker
==========================
Two tools for the auto-trader agent:

1. evaluate_signal — scan history for every time a signal fired, attach forward returns.
   Used by the agent during research to investigate signal patterns.

2. rank_signals — take multiple candidate signals, compute per-signal stats,
   run forward selection to find the optimal combination.
   Used by the agent after investigation to make evidence-based portfolio decisions.

Both tools reuse the existing precompute_condition() from backtest_engine.py
which already handles all 16 entry condition types.
"""

import sys
import sqlite3
import numpy as np
from pathlib import Path
from collections import defaultdict

# Add scripts to path
SCRIPT_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPT_DIR))

from backtest_engine import precompute_condition, load_earnings_data


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_universe(conn: sqlite3.Connection, sector: str | None) -> list[str]:
    """Get list of symbols, optionally filtered by sector."""
    cur = conn.cursor()
    if sector:
        cur.execute("SELECT symbol FROM universe_profiles WHERE sector = ?", (sector,))
    else:
        cur.execute("SELECT symbol FROM universe_profiles")
    return [row[0] for row in cur.fetchall()]


def _load_price_index(conn: sqlite3.Connection, symbols: list[str],
                      start: str, end: str) -> dict[str, dict[str, float]]:
    """Load {symbol: {date: close}} for all symbols in the period."""
    cur = conn.cursor()
    placeholders = ",".join("?" * len(symbols))
    cur.execute(
        f"SELECT symbol, date, close FROM prices "
        f"WHERE symbol IN ({placeholders}) AND date >= ? AND date <= ? "
        f"ORDER BY symbol, date",
        symbols + [start, end],
    )
    index = defaultdict(dict)
    for sym, date, close in cur.fetchall():
        index[sym][date] = close
    return dict(index)


def _get_trading_dates(conn: sqlite3.Connection, start: str, end: str) -> list[str]:
    """Get sorted list of trading dates in [start, end]."""
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT date FROM prices WHERE date >= ? AND date <= ? ORDER BY date",
        (start, end),
    )
    return [row[0] for row in cur.fetchall()]


def _horizon_to_trading_days(horizon: str) -> int:
    """Convert horizon string like '1m', '3m', '6m', '12m' to trading days."""
    h = horizon.strip().lower()
    if h.endswith("m"):
        months = int(h[:-1])
        return months * 21  # ~21 trading days per month
    elif h.endswith("y"):
        years = int(h[:-1])
        return years * 252
    elif h.endswith("d"):
        return int(h[:-1])
    else:
        raise ValueError(f"Unknown horizon format: {horizon}")


# ---------------------------------------------------------------------------
# evaluate_signal
# ---------------------------------------------------------------------------

def evaluate_signal(
    signal_config: dict,
    target_horizon: str,
    db_path: str,
    start: str,
    end: str,
    sector: str | None = None,
    universe: list[str] | None = None,
) -> dict:
    """
    Evaluate a single signal historically.

    Scans the full universe over the given period. Every time the signal fires
    for a stock, records the forward return at the target horizon.

    Args:
        signal_config: Entry condition config dict, e.g. {"type": "momentum_rank", "lookback": 63, "operator": ">=", "value": 80}
        target_horizon: Forward return horizon, e.g. "3m", "6m", "12m"
        db_path: Path to market.db
        start: Period start date (YYYY-MM-DD)
        end: Period end date (YYYY-MM-DD)
        sector: Optional sector filter
        universe: Optional explicit list of symbols (overrides sector)

    Returns:
        {
            "signal": signal_config,
            "target_horizon": target_horizon,
            "period": {"start": start, "end": end},
            "trigger_count": int,
            "win_count": int,
            "win_rate": float,
            "avg_return": float,
            "median_return": float,
            "std_return": float,
            "sharpe": float,
            "sample_events": [ ... top 20 events ... ],
        }
    """
    conn = sqlite3.connect(str(db_path))
    horizon_days = _horizon_to_trading_days(target_horizon)

    # Resolve universe
    if universe:
        symbols = universe
    else:
        symbols = _get_universe(conn, sector)

    if not symbols:
        conn.close()
        return {"error": "No symbols found for the given sector/universe."}

    # We need price data beyond `end` to compute forward returns
    # Extend the price loading window by the horizon
    trading_dates = _get_trading_dates(conn, start, end)
    if not trading_dates:
        conn.close()
        return {"error": "No trading dates found in the given period."}

    # Load extended price index (for forward return computation)
    # We need prices up to horizon_days after `end`
    extended_end_idx = len(trading_dates) - 1  # we'll load more below
    all_dates_extended = _get_trading_dates(conn, start, "2099-12-31")
    if len(all_dates_extended) > len(trading_dates) + horizon_days:
        extended_end = all_dates_extended[len(trading_dates) + horizon_days - 1]
    else:
        extended_end = all_dates_extended[-1] if all_dates_extended else end

    price_index = _load_price_index(conn, symbols, start, extended_end)

    # Filter symbols to only those with price data (avoids KeyError in
    # cross-sectional computations like momentum_rank)
    symbols = [s for s in symbols if s in price_index]

    # Build date-to-index map for forward lookups
    date_to_idx = {d: i for i, d in enumerate(all_dates_extended)}

    # Load earnings data if needed
    earnings_data = None
    if signal_config.get("type") == "earnings_momentum":
        earnings_data = load_earnings_data(symbols, conn)

    # Run precompute_condition — reuse the exact same logic as the backtest engine
    try:
        signal_data = precompute_condition(
            signal_config, symbols, conn, start, end,
            earnings_data=earnings_data, price_index=price_index,
        )
    except Exception as e:
        conn.close()
        return {"error": f"Signal computation failed: {str(e)}"}

    conn.close()

    # Collect trigger events with forward returns
    events = []
    for symbol, date_signals in signal_data.items():
        sym_prices = price_index.get(symbol, {})
        if not sym_prices:
            continue

        for date, metadata in date_signals.items():
            entry_price = sym_prices.get(date)
            if entry_price is None or entry_price <= 0:
                continue

            # Find the price `horizon_days` trading days forward
            entry_idx = date_to_idx.get(date)
            if entry_idx is None:
                continue

            fwd_idx = entry_idx + horizon_days
            if fwd_idx >= len(all_dates_extended):
                continue  # not enough forward data

            fwd_date = all_dates_extended[fwd_idx]
            fwd_price = sym_prices.get(fwd_date)
            if fwd_price is None or fwd_price <= 0:
                continue

            fwd_return = (fwd_price - entry_price) / entry_price
            events.append({
                "symbol": symbol,
                "date": date,
                "entry_price": round(entry_price, 2),
                "fwd_date": fwd_date,
                "fwd_price": round(fwd_price, 2),
                "fwd_return": round(fwd_return, 4),
            })

    # Compute stats
    if not events:
        return {
            "signal": signal_config,
            "target_horizon": target_horizon,
            "period": {"start": start, "end": end},
            "trigger_count": 0,
            "unique_stocks": 0,
            "win_count": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "median_return": 0.0,
            "std_return": 0.0,
            "sharpe": 0.0,
            "return_percentiles": {},
            "yearly_breakdown": [],
            "top_stocks": [],
            "bottom_stocks": [],
        }

    returns = np.array([e["fwd_return"] for e in events])
    win_count = int(np.sum(returns > 0))
    avg_ret = float(np.mean(returns))
    std_ret = float(np.std(returns)) if len(returns) > 1 else 0.0
    sharpe = avg_ret / std_ret if std_ret > 0 else 0.0

    # Return distribution percentiles
    return_percentiles = {
        "p10": round(float(np.percentile(returns, 10)), 4),
        "p25": round(float(np.percentile(returns, 25)), 4),
        "p50": round(float(np.percentile(returns, 50)), 4),
        "p75": round(float(np.percentile(returns, 75)), 4),
        "p90": round(float(np.percentile(returns, 90)), 4),
    }

    # Yearly breakdown
    yearly = defaultdict(list)
    for e in events:
        year = int(e["date"][:4])
        yearly[year].append(e["fwd_return"])

    yearly_breakdown = []
    for year in sorted(yearly):
        yr_returns = np.array(yearly[year])
        yr_std = float(np.std(yr_returns)) if len(yr_returns) > 1 else 0.0
        yearly_breakdown.append({
            "year": year,
            "triggers": len(yr_returns),
            "win_rate": round(float(np.sum(yr_returns > 0)) / len(yr_returns), 4),
            "avg_return": round(float(np.mean(yr_returns)), 4),
            "sharpe": round(float(np.mean(yr_returns)) / yr_std, 4) if yr_std > 0 else 0.0,
        })

    # Per-stock aggregation
    stock_stats = defaultdict(list)
    for e in events:
        stock_stats[e["symbol"]].append(e["fwd_return"])

    stock_summaries = []
    for sym, sym_returns in stock_stats.items():
        arr = np.array(sym_returns)
        stock_summaries.append({
            "symbol": sym,
            "triggers": len(arr),
            "win_rate": round(float(np.sum(arr > 0)) / len(arr), 4),
            "avg_return": round(float(np.mean(arr)), 4),
        })

    # Top 20 (best avg return) and bottom 20 (worst avg return)
    sorted_by_avg = sorted(stock_summaries, key=lambda s: s["avg_return"], reverse=True)
    top_stocks = sorted_by_avg[:20]
    bottom_stocks = sorted_by_avg[-20:][::-1]  # worst first

    return {
        "signal": signal_config,
        "target_horizon": target_horizon,
        "period": {"start": start, "end": end},
        "trigger_count": len(events),
        "unique_stocks": len(stock_stats),
        "win_count": win_count,
        "win_rate": round(win_count / len(events), 4),
        "avg_return": round(avg_ret, 4),
        "median_return": round(float(np.median(returns)), 4),
        "std_return": round(std_ret, 4),
        "sharpe": round(sharpe, 4),
        "return_percentiles": return_percentiles,
        "yearly_breakdown": yearly_breakdown,
        "top_stocks": top_stocks,
        "bottom_stocks": bottom_stocks,
    }


# ---------------------------------------------------------------------------
# rank_signals (forward selection)
# ---------------------------------------------------------------------------

def rank_signals(
    candidate_signals: list[dict],
    target_horizon: str,
    db_path: str,
    start: str,
    end: str,
    sector: str | None = None,
    universe: list[str] | None = None,
) -> dict:
    """
    Rank candidate signals and find the optimal combination via forward selection.

    Steps:
    1. Evaluate each signal independently (per-signal stats).
    2. Forward selection: start with best single signal, greedily add the
       next best combination, stop when adding hurts Sharpe.

    "Combining" signals means intersection: a trigger event counts only when
    ALL signals in the combination fire for the same stock on the same date.

    Args:
        candidate_signals: List of entry condition config dicts
        target_horizon: Forward return horizon, e.g. "6m"
        db_path: Path to market.db
        start: Period start date
        end: Period end date
        sector: Optional sector filter
        universe: Optional explicit list of symbols

    Returns:
        {
            "individual_signals": [ ... per-signal stats ... ],
            "forward_selection": [ ... step-by-step combination results ... ],
            "recommended_signals": [ ... winning signal configs ... ],
        }
    """
    conn = sqlite3.connect(str(db_path))
    horizon_days = _horizon_to_trading_days(target_horizon)

    # Resolve universe
    if universe:
        symbols = universe
    else:
        symbols = _get_universe(conn, sector)

    if not symbols:
        conn.close()
        return {"error": "No symbols found."}

    # Load extended price data
    all_dates_extended = _get_trading_dates(conn, start, "2099-12-31")
    trading_dates = _get_trading_dates(conn, start, end)
    if not trading_dates:
        conn.close()
        return {"error": "No trading dates found."}

    if len(all_dates_extended) > len(trading_dates) + horizon_days:
        extended_end = all_dates_extended[len(trading_dates) + horizon_days - 1]
    else:
        extended_end = all_dates_extended[-1] if all_dates_extended else end

    price_index = _load_price_index(conn, symbols, start, extended_end)

    # Filter symbols to only those with price data (avoids KeyError in
    # cross-sectional computations like momentum_rank)
    symbols = [s for s in symbols if s in price_index]

    date_to_idx = {d: i for i, d in enumerate(all_dates_extended)}

    # Load earnings data once (shared across signals)
    earnings_data = load_earnings_data(symbols, conn)

    # Step 1: Evaluate each signal independently
    # Store both stats and raw trigger sets for combination testing
    signal_triggers = []  # list of {(symbol, date): fwd_return} per signal
    individual_results = []

    for sig_config in candidate_signals:
        try:
            sig_data = precompute_condition(
                sig_config, symbols, conn, start, end,
                earnings_data=earnings_data, price_index=price_index,
            )
        except Exception as e:
            individual_results.append({
                "signal": sig_config,
                "error": str(e),
                "trigger_count": 0,
                "win_rate": 0.0,
                "avg_return": 0.0,
                "sharpe": 0.0,
            })
            signal_triggers.append({})
            continue

        # Build trigger map: {(symbol, date): fwd_return}
        triggers = {}
        for symbol, date_signals in sig_data.items():
            sym_prices = price_index.get(symbol, {})
            if not sym_prices:
                continue
            for date in date_signals:
                entry_price = sym_prices.get(date)
                if entry_price is None or entry_price <= 0:
                    continue
                entry_idx = date_to_idx.get(date)
                if entry_idx is None:
                    continue
                fwd_idx = entry_idx + horizon_days
                if fwd_idx >= len(all_dates_extended):
                    continue
                fwd_date = all_dates_extended[fwd_idx]
                fwd_price = sym_prices.get(fwd_date)
                if fwd_price is None or fwd_price <= 0:
                    continue
                fwd_return = (fwd_price - entry_price) / entry_price
                triggers[(symbol, date)] = fwd_return

        signal_triggers.append(triggers)

        # Compute stats
        returns = np.array(list(triggers.values())) if triggers else np.array([])
        if len(returns) > 0:
            win_count = int(np.sum(returns > 0))
            avg_ret = float(np.mean(returns))
            std_ret = float(np.std(returns)) if len(returns) > 1 else 0.0
            sharpe = avg_ret / std_ret if std_ret > 0 else 0.0
            individual_results.append({
                "signal": sig_config,
                "trigger_count": len(returns),
                "win_count": win_count,
                "win_rate": round(win_count / len(returns), 4),
                "avg_return": round(avg_ret, 4),
                "median_return": round(float(np.median(returns)), 4),
                "std_return": round(std_ret, 4),
                "sharpe": round(sharpe, 4),
            })
        else:
            individual_results.append({
                "signal": sig_config,
                "trigger_count": 0,
                "win_rate": 0.0,
                "avg_return": 0.0,
                "sharpe": 0.0,
            })

    conn.close()

    # Step 2: Forward selection
    n_signals = len(candidate_signals)
    if n_signals == 0:
        return {
            "individual_signals": [],
            "forward_selection": [],
            "recommended_signals": [],
        }

    remaining = set(range(n_signals))
    selected = []
    forward_selection_steps = []
    best_sharpe = -float("inf")

    while remaining:
        best_candidate = None
        best_candidate_sharpe = -float("inf")
        best_candidate_stats = None

        for idx in remaining:
            # Combine: intersection of selected + this candidate
            if not signal_triggers[idx]:
                continue

            if selected:
                # Intersection of all selected triggers + candidate
                combined_keys = set(signal_triggers[selected[0]].keys())
                for sel_idx in selected[1:]:
                    combined_keys &= set(signal_triggers[sel_idx].keys())
                combined_keys &= set(signal_triggers[idx].keys())
            else:
                combined_keys = set(signal_triggers[idx].keys())

            if len(combined_keys) < 5:
                # Too few triggers for reliable stats
                continue

            # Use the candidate signal's forward returns for the intersection
            # (all signals agree on these events, returns are the same regardless
            # of which signal's return we take — they're all the same stock+date)
            combined_returns = np.array([signal_triggers[idx][k] for k in combined_keys])
            avg_ret = float(np.mean(combined_returns))
            std_ret = float(np.std(combined_returns)) if len(combined_returns) > 1 else 0.0
            combo_sharpe = avg_ret / std_ret if std_ret > 0 else 0.0

            if combo_sharpe > best_candidate_sharpe:
                best_candidate = idx
                best_candidate_sharpe = combo_sharpe
                best_candidate_stats = {
                    "trigger_count": len(combined_keys),
                    "win_rate": round(float(np.sum(combined_returns > 0)) / len(combined_returns), 4),
                    "avg_return": round(avg_ret, 4),
                    "sharpe": round(combo_sharpe, 4),
                }

        if best_candidate is None:
            break

        # Check if adding this candidate improves Sharpe
        delta = round(best_candidate_sharpe - best_sharpe, 4) if best_sharpe > -float("inf") else None
        step = {
            "step": len(selected) + 1,
            "added_signal": candidate_signals[best_candidate],
            "combined_sharpe": round(best_candidate_sharpe, 4),
            "delta": delta,
            **best_candidate_stats,
        }

        if best_candidate_sharpe <= best_sharpe and len(selected) > 0:
            # Adding hurts — stop. Record as "dropped"
            step["verdict"] = "dropped"
            forward_selection_steps.append(step)
            break

        step["verdict"] = "kept"
        forward_selection_steps.append(step)
        selected.append(best_candidate)
        remaining.discard(best_candidate)
        best_sharpe = best_candidate_sharpe

    # Any remaining signals not tested (because earlier ones had no triggers)
    # are implicitly dropped

    recommended = [candidate_signals[i] for i in selected]

    return {
        "individual_signals": individual_results,
        "forward_selection": forward_selection_steps,
        "recommended_signals": recommended,
    }
