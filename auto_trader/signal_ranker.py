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

from backtest_engine import precompute_condition, load_earnings_data, SECTOR_ETF_MAP
from _nav_metrics import compute_nav_stats, load_risk_free_ann_pct


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_factor_portfolio_nav(
    entries: list[tuple[str, str]],
    price_index: dict,
    walk_dates: list[str],
    horizon_days: int,
) -> list[float]:
    """Daily-return series for an equal-weight long-only factor portfolio.

    Each entry = (symbol, entry_date) opens a unit-weight long position held
    for `horizon_days` trading days. On each day in `walk_dates`, the
    portfolio is the equal-weight basket of currently-open positions; daily
    return is the mean of constituents' simple returns from the previous day.

    Returns one return per day in `walk_dates` after the first (length =
    len(walk_dates) - 1). Days with an empty basket get 0.0 (cash).

    Walk semantics: positions opened on D contribute starting D+1 (first
    return is D → D+1). A position opened on D and held for H days closes
    after the return on D+H, i.e. it contributes returns on D+1 through D+H.
    """
    date_to_walk_idx = {d: i for i, d in enumerate(walk_dates)}
    by_entry_idx: dict[int, list[str]] = {}
    for sym, d in entries:
        idx = date_to_walk_idx.get(d)
        if idx is None:
            continue
        by_entry_idx.setdefault(idx, []).append(sym)

    open_positions: list[tuple[str, int]] = []  # (symbol, entry_walk_idx)
    daily_returns: list[float] = []

    for i in range(1, len(walk_dates)):
        # Positions opened at the start of day i-1 contribute starting day i.
        for sym in by_entry_idx.get(i - 1, []):
            open_positions.append((sym, i - 1))
        # Drop positions whose horizon has expired (held > horizon_days).
        open_positions = [
            (s, idx) for (s, idx) in open_positions if (i - idx) <= horizon_days
        ]
        if not open_positions:
            daily_returns.append(0.0)
            continue
        d_today = walk_dates[i]
        d_prev = walk_dates[i - 1]
        rets = []
        for sym, _ in open_positions:
            p = price_index.get(sym, {}).get(d_today)
            p_prev = price_index.get(sym, {}).get(d_prev)
            if p is not None and p_prev is not None and p_prev > 0:
                rets.append((p - p_prev) / p_prev)
        daily_returns.append(sum(rets) / len(rets) if rets else 0.0)

    return daily_returns


def _resolve_benchmark_ticker(
    conn: sqlite3.Connection, sector: str | None
) -> str:
    """Pick the sector ETF if it has price data, else fall back to SPY."""
    if sector and sector in SECTOR_ETF_MAP:
        candidate = SECTOR_ETF_MAP[sector]
        n = conn.execute(
            "SELECT COUNT(*) FROM prices WHERE symbol = ? LIMIT 1", (candidate,)
        ).fetchone()[0]
        if n > 0:
            return candidate
    return "SPY"


def _load_benchmark_returns(
    conn: sqlite3.Connection, ticker: str, walk_dates: list[str]
) -> list[float]:
    """Daily-return series for `ticker`, aligned to `walk_dates[1:]`.

    Length = len(walk_dates) - 1. Missing prices on either side of a
    consecutive pair → 0.0 for that day (rare for benchmark ETFs).
    """
    if not walk_dates:
        return []
    rows = conn.execute(
        "SELECT date, close FROM prices WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date",
        (ticker, walk_dates[0], walk_dates[-1]),
    ).fetchall()
    by_date = {d: c for d, c in rows}
    rets = []
    for i in range(1, len(walk_dates)):
        p = by_date.get(walk_dates[i])
        p_prev = by_date.get(walk_dates[i - 1])
        if p is not None and p_prev is not None and p_prev > 0:
            rets.append((p - p_prev) / p_prev)
        else:
            rets.append(0.0)
    return rets


def _ann_return_from_compounded(
    daily_returns: list[float], n_nav: int
) -> tuple[float, float | None]:
    """Compound daily returns → (total_return_pct, ann_return_pct).

    ann_return is None when the window is shorter than
    MIN_TRADING_DAYS_FOR_ANNUALIZATION (60), matching the backtest
    engine's honesty gate. Caller passes that to compute_nav_stats which
    silently disables Sharpe/Sortino when ann_return is None.
    """
    if not daily_returns:
        return 0.0, None
    cum = 1.0
    for r in daily_returns:
        cum *= 1 + r
    total_return_pct = (cum - 1) * 100
    if n_nav < 60:  # MIN_TRADING_DAYS_FOR_ANNUALIZATION
        return total_return_pct, None
    years = n_nav / 252.0
    ann_return_pct = (cum ** (1 / years) - 1) * 100
    return total_return_pct, ann_return_pct


def _resolve_ic_universe(
    conn: sqlite3.Connection,
    sector: str | None,
    universe: list[str] | None,
) -> list[str]:
    """IC cross-section universe: single names only, no benchmark ETFs.

    Explicit `universe` arg wins. Otherwise sector members from
    universe_profiles, else the full base universe. ETFs aren't in
    universe_profiles, so they're naturally excluded — which is what we
    want for cross-sectional ranking.
    """
    if universe:
        return list(universe)
    if sector:
        return [
            r[0]
            for r in conn.execute(
                "SELECT symbol FROM universe_profiles WHERE sector = ?", (sector,)
            ).fetchall()
        ]
    return [
        r[0]
        for r in conn.execute("SELECT symbol FROM universe_profiles").fetchall()
    ]


def _compute_ic(
    signal_config: dict,
    conn: sqlite3.Connection,
    start: str,
    end: str,
    sector: str | None,
    universe: list[str] | None,
    horizon_days: int,
    all_dates_extended: list[str],
    price_index: dict,
) -> dict:
    """Cross-sectional Information Coefficient (Spearman rank correlation).

    For each trading day D in [start, end - horizon_days], rank IC-universe
    names by factor value at D, rank by forward return D → D + horizon_days,
    compute Spearman rank correlation. Aggregate to ic_mean, ic_stdev, IR,
    and t-statistic.

    Continuous-factor IC (feature_threshold / feature_percentile): IC on the
    raw feature value, regardless of the threshold the agent picked.
    Other condition types: returned with `ic_basis = "binary"` and
    `reason = "binary_ic_not_yet_supported"` for now.

    Universe rules: sector members only (no benchmark ETFs); custom
    `universe` arg verbatim. <10 names total → return null block.
    Per-day filter: skip days with <10 valid cross-section or constant
    factor. n_observations < 5 total → return null block.
    """
    import numpy as np
    from scipy import stats as scipy_stats

    null_block = lambda reason, n_obs=0, basis=None, fact=None: {
        "ic_mean": None, "ic_stdev": None, "ir": None, "ic_t_stat": None,
        "n_observations": n_obs,
        "ic_basis": basis, "factor_used_for_ic": fact,
        "reason": reason,
    }

    ic_symbols = _resolve_ic_universe(conn, sector, universe)
    if len(ic_symbols) < 10:
        return null_block("insufficient_cross_section")

    ctype = signal_config.get("type")
    is_continuous = ctype in ("feature_threshold", "feature_percentile")
    if not is_continuous:
        return null_block(
            "binary_ic_not_yet_supported", basis="binary", fact=None
        )

    feature = signal_config.get("feature")
    if not feature:
        return null_block("missing_feature_in_signal_config", basis="continuous")

    # Bulk-load feature time series for the IC universe.
    from backtest_engine import _load_feature_series
    series = _load_feature_series(feature, ic_symbols, start, end, conn)
    if not series:
        return null_block(
            "no_feature_data", basis="continuous", fact=feature
        )

    # IC walk window: D ∈ [start, end] AND D + horizon_days < len(extended).
    date_to_ext_idx = {d: i for i, d in enumerate(all_dates_extended)}
    last_valid_idx = len(all_dates_extended) - horizon_days - 1
    ic_walk = [
        d for d in all_dates_extended
        if start <= d <= end and date_to_ext_idx[d] <= last_valid_idx
    ]
    if len(ic_walk) < 10:
        return null_block(
            "insufficient_walk_window", basis="continuous", fact=feature
        )

    n_days = len(ic_walk)
    n_names = len(ic_symbols)

    # Factor matrix via vectorized as-of lookup.
    # For each symbol's sorted feature dates, np.searchsorted maps each
    # walk date to the index of the most-recent at-or-before observation.
    ic_walk_arr = np.array(ic_walk)
    F = np.full((n_days, n_names), np.nan)
    for j, sym in enumerate(ic_symbols):
        pts = series.get(sym, [])
        if not pts:
            continue
        dates_sym = np.array([p[0] for p in pts])
        vals_sym = np.array([p[1] for p in pts], dtype=float)
        # 'right' so equal dates use the value AT date D (already known by close).
        idx = np.searchsorted(dates_sym, ic_walk_arr, side="right") - 1
        valid = idx >= 0
        F[valid, j] = vals_sym[idx[valid]]

    # Forward-return matrix — vectorized via aligned price arrays per symbol.
    R = np.full((n_days, n_names), np.nan)
    fwd_idxs = np.array([date_to_ext_idx[d] + horizon_days for d in ic_walk])
    fwd_dates = np.array([all_dates_extended[i] for i in fwd_idxs])
    for j, sym in enumerate(ic_symbols):
        sym_prices = price_index.get(sym, {})
        if not sym_prices:
            continue
        # Vectorized lookup via two parallel arrays
        p = np.array([sym_prices.get(d, np.nan) for d in ic_walk_arr], dtype=float)
        p_fwd = np.array([sym_prices.get(d, np.nan) for d in fwd_dates], dtype=float)
        valid = (~np.isnan(p)) & (~np.isnan(p_fwd)) & (p > 0) & (p_fwd > 0)
        R[valid, j] = (p_fwd[valid] - p[valid]) / p[valid]

    # Per-day Spearman = Pearson on ranks of jointly-valid cells.
    ic_values = []
    for i in range(n_days):
        f_row = F[i]
        r_row = R[i]
        valid = ~(np.isnan(f_row) | np.isnan(r_row))
        if valid.sum() < 10:
            continue
        f_v = f_row[valid]
        r_v = r_row[valid]
        if np.std(f_v) == 0:
            continue
        f_ranks = scipy_stats.rankdata(f_v)
        r_ranks = scipy_stats.rankdata(r_v)
        corr = np.corrcoef(f_ranks, r_ranks)[0, 1]
        if np.isnan(corr):
            continue
        ic_values.append(corr)

    n_obs = len(ic_values)
    if n_obs < 5:
        return null_block(
            "insufficient_cross_section", n_obs=n_obs,
            basis="continuous", fact=feature,
        )

    arr = np.array(ic_values)
    ic_mean = float(arr.mean())
    ic_stdev = float(arr.std(ddof=1)) if n_obs > 1 else 0.0
    ir = ic_mean / ic_stdev if ic_stdev > 0 else 0.0
    ic_t_stat = ir * (n_obs ** 0.5)

    return {
        "ic_mean": round(ic_mean, 4),
        "ic_stdev": round(ic_stdev, 4),
        "ir": round(ir, 4),
        "ic_t_stat": round(ic_t_stat, 4),
        "n_observations": n_obs,
        "ic_basis": "continuous",
        "factor_used_for_ic": feature,
    }


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

    Constructs a long-only equal-weight factor portfolio from the signal's
    triggers, holds each position for `target_horizon` trading days, and
    reports portfolio metrics computed with the same formulas as
    `run_backtest`. Sharpe / Sortino / alpha-vs-sector are directly
    comparable to the backtest's metrics — but represent an upper bound
    (no costs, no capacity caps, no exits other than the fixed hold).

    Args:
        signal_config: Entry condition config dict.
        target_horizon: Hold period and forward-return horizon, e.g. "3m".
        db_path: Path to market.db.
        start: Period start date (YYYY-MM-DD).
        end: Period end date (YYYY-MM-DD).
        sector: Optional sector filter (used for both universe and benchmark).
        universe: Optional explicit list of symbols (overrides sector).

    Returns: see implementation. Top-level keys include `portfolio_metrics`,
    `benchmark_used`, `trigger_count`, `unique_stocks`, `yearly_breakdown`,
    `top_stocks`, `bottom_stocks`.
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

    # Collect entries (every signal-fire with valid entry price) and events
    # (subset with full forward-return window — used for per-event diagnostics).
    entries: list[tuple[str, str]] = []
    events: list[dict] = []
    for symbol, date_signals in signal_data.items():
        sym_prices = price_index.get(symbol, {})
        if not sym_prices:
            continue

        for date in date_signals:
            entry_price = sym_prices.get(date)
            if entry_price is None or entry_price <= 0:
                continue
            entries.append((symbol, date))

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
            events.append({
                "symbol": symbol, "date": date,
                "entry_price": round(entry_price, 2),
                "fwd_date": fwd_date,
                "fwd_price": round(fwd_price, 2),
                "fwd_return": round(fwd_return, 4),
            })

    # === Factor-portfolio NAV + unified metrics ===
    walk_dates = trading_dates
    daily_returns = _build_factor_portfolio_nav(
        entries, price_index, walk_dates, horizon_days
    )
    n_nav = len(walk_dates)
    total_return_pct, ann_return_pct = _ann_return_from_compounded(daily_returns, n_nav)

    # Max drawdown from the cumulative NAV.
    cum = 1.0
    peak = 1.0
    max_dd = 0.0
    for r in daily_returns:
        cum *= 1 + r
        if cum > peak:
            peak = cum
        if peak > 0:
            dd = (cum - peak) / peak * 100
            if dd < max_dd:
                max_dd = dd

    rf_pct = load_risk_free_ann_pct(walk_dates[0], walk_dates[-1])
    nav_stats = compute_nav_stats(
        daily_returns, n_nav, total_return_pct, ann_return_pct, rf_pct
    )

    # Cross-sectional IC — cheap diagnostic of factor quality independent
    # of the threshold the agent picked.
    ic = _compute_ic(
        signal_config, conn, start, end, sector, universe,
        horizon_days, all_dates_extended, price_index,
    )

    # Benchmarks: market = SPY always; sector = mapped ETF when sector is set.
    market_rets = _load_benchmark_returns(conn, "SPY", walk_dates)
    _, market_ann = _ann_return_from_compounded(market_rets, n_nav)

    benchmark_used = "SPY"
    sector_ann = None
    if sector:
        sector_ticker = _resolve_benchmark_ticker(conn, sector)
        if sector_ticker != "SPY":
            sector_rets = _load_benchmark_returns(conn, sector_ticker, walk_dates)
            _, sector_ann = _ann_return_from_compounded(sector_rets, n_nav)
            benchmark_used = sector_ticker

    conn.close()

    def _r(v, ndigits=4):
        return None if v is None else round(v, ndigits)

    alpha_vs_market = (
        ann_return_pct - market_ann
        if (ann_return_pct is not None and market_ann is not None) else None
    )
    alpha_vs_sector = (
        ann_return_pct - sector_ann
        if (ann_return_pct is not None and sector_ann is not None) else None
    )

    portfolio_metrics = {
        "total_return_pct": round(total_return_pct, 2),
        "annualized_return_pct": _r(ann_return_pct, 2),
        "annualized_volatility_pct": _r(nav_stats["annualized_volatility_pct"], 2),
        "sharpe_ratio": _r(nav_stats["sharpe_ratio"], 4),
        "sharpe_ratio_annualized": _r(nav_stats["sharpe_ratio_annualized"], 4),
        "sharpe_ratio_period": _r(nav_stats["sharpe_ratio_period"], 4),
        "sharpe_basis": nav_stats["sharpe_basis"],
        "sortino_ratio": _r(nav_stats["sortino_ratio"], 4),
        "max_drawdown_pct": round(max_dd, 2),
        "alpha_vs_market_pct": _r(alpha_vs_market, 2),
        "alpha_vs_sector_pct": _r(alpha_vs_sector, 2),
        "trading_days": n_nav,
        "risk_free_rate_pct": round(rf_pct, 2),
    }

    # Per-event diagnostics — coverage of where/when the signal fires.
    if events:
        yearly = defaultdict(list)
        for e in events:
            yearly[int(e["date"][:4])].append(e["fwd_return"])
        yearly_breakdown = []
        for year in sorted(yearly):
            yr = yearly[year]
            yearly_breakdown.append({
                "year": year,
                "triggers": len(yr),
                "win_rate": round(sum(1 for r in yr if r > 0) / len(yr), 4),
                "avg_return": round(sum(yr) / len(yr), 4),
            })

        stock_stats: dict[str, list[float]] = defaultdict(list)
        for e in events:
            stock_stats[e["symbol"]].append(e["fwd_return"])
        stock_summaries = [
            {
                "symbol": sym,
                "triggers": len(rs),
                "win_rate": round(sum(1 for r in rs if r > 0) / len(rs), 4),
                "avg_return": round(sum(rs) / len(rs), 4),
            }
            for sym, rs in stock_stats.items()
        ]
        sorted_by_avg = sorted(stock_summaries, key=lambda s: s["avg_return"], reverse=True)
        top_stocks = sorted_by_avg[:20]
        bottom_stocks = sorted_by_avg[-20:][::-1]
        unique_stocks = len(stock_stats)
    else:
        yearly_breakdown = []
        top_stocks = []
        bottom_stocks = []
        unique_stocks = len({sym for sym, _ in entries})

    return {
        "signal": signal_config,
        "target_horizon": target_horizon,
        "period": {"start": start, "end": end},
        "portfolio_metrics": portfolio_metrics,
        "ic": ic,
        "benchmark_used": benchmark_used,
        "note": (
            "Long-only equal-weight factor portfolio: each signal-fire opens a "
            "unit-weight position held for the full horizon. No costs / no "
            "capacity caps / no early exits — upper bound on realizable Sharpe."
        ),
        "trigger_count": len(entries),
        "unique_stocks": unique_stocks,
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
