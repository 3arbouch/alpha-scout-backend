"""
Pure backtest simulation engine.

No database access, no file I/O. Accepts preloaded data, returns results.

Data flow:
    1. Caller loads prices, signals, earnings, benchmark from DB/files.
    2. This engine runs the simulation loop on that data.
    3. Caller persists the returned BacktestResult.

Usage:
    from server.engines.backtest_engine import run_backtest

    result = run_backtest(
        config=strategy_config_dict,
        price_index={"AAPL": {"2024-01-02": 150.0, ...}, ...},
        trading_dates=["2024-01-02", "2024-01-03", ...],
        signals={"AAPL": {"2024-01-15": -28.5}, ...},
        signal_metadata={"AAPL": {"2024-01-15": {...}}, ...},
    )
"""

from __future__ import annotations

import math
import random
import statistics
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Position & Portfolio (simulation state — not domain models)
# ---------------------------------------------------------------------------

class Position:
    """A single open position."""

    __slots__ = ("symbol", "entry_date", "entry_price", "shares",
                 "peak_price", "high_since_entry", "signal_detail")

    def __init__(self, symbol: str, entry_date: str, entry_price: float,
                 shares: float, peak_price: float | None = None,
                 signal_detail: dict | None = None):
        self.symbol = symbol
        self.entry_date = entry_date
        self.entry_price = entry_price
        self.shares = shares
        self.peak_price = peak_price or entry_price
        self.high_since_entry = entry_price
        self.signal_detail = signal_detail

    def market_value(self, current_price: float) -> float:
        return self.shares * current_price

    def pnl_pct(self, current_price: float) -> float:
        if self.entry_price <= 0:
            return 0.0
        return ((current_price - self.entry_price) / self.entry_price) * 100

    def days_held(self, current_date: str) -> int:
        entry_dt = datetime.strptime(self.entry_date, "%Y-%m-%d")
        current_dt = datetime.strptime(current_date, "%Y-%m-%d")
        return (current_dt - entry_dt).days


class Portfolio:
    """Portfolio state tracker."""

    def __init__(self, initial_cash: float):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: dict[str, Position] = {}
        self.trades: list[dict] = []
        self.nav_history: list[dict] = []
        self.closed_trades: list[dict] = []

    def open_position(self, symbol: str, date: str, price: float,
                      amount: float, slippage_bps: float = 0,
                      peak_price: float | None = None,
                      signal_detail: dict | None = None):
        exec_price = price * (1 + slippage_bps / 10000)
        shares = amount / exec_price

        if symbol in self.positions:
            pos = self.positions[symbol]
            total_cost = (pos.shares * pos.entry_price) + (shares * exec_price)
            pos.shares += shares
            pos.entry_price = total_cost / pos.shares
        else:
            self.positions[symbol] = Position(
                symbol=symbol, entry_date=date, entry_price=exec_price,
                shares=shares, peak_price=peak_price, signal_detail=signal_detail,
            )

        self.cash -= amount
        self.trades.append({
            "date": date, "symbol": symbol, "action": "BUY",
            "price": round(exec_price, 2), "shares": round(shares, 4),
            "amount": round(amount, 2), "signal_detail": signal_detail,
        })

    def close_position(self, symbol: str, date: str, price: float,
                       reason: str, slippage_bps: float = 0,
                       partial_pct: float = 100):
        if symbol not in self.positions:
            return

        pos = self.positions[symbol]
        exec_price = price * (1 - slippage_bps / 10000)

        if partial_pct >= 100:
            shares_to_sell = pos.shares
            del self.positions[symbol]
        else:
            shares_to_sell = pos.shares * (partial_pct / 100)
            pos.shares -= shares_to_sell

        proceeds = shares_to_sell * exec_price
        self.cash += proceeds

        cost_basis = shares_to_sell * pos.entry_price
        pnl = proceeds - cost_basis
        pnl_pct = ((exec_price - pos.entry_price) / pos.entry_price) * 100

        trade = {
            "date": date, "symbol": symbol, "action": "SELL",
            "reason": reason, "price": round(exec_price, 2),
            "shares": round(shares_to_sell, 4), "amount": round(proceeds, 2),
            "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
            "entry_date": pos.entry_date, "entry_price": round(pos.entry_price, 2),
            "days_held": pos.days_held(date), "signal_detail": pos.signal_detail,
        }
        self.trades.append(trade)
        self.closed_trades.append(trade)

    def nav(self, price_index: dict, date: str) -> float:
        positions_value = 0.0
        for symbol, pos in self.positions.items():
            price = price_index.get(symbol, {}).get(date)
            if price:
                positions_value += pos.market_value(price)
                if price > pos.high_since_entry:
                    pos.high_since_entry = price
        return self.cash + positions_value

    def record_nav(self, price_index: dict, date: str):
        positions_value = 0.0
        position_details = {}
        for symbol, pos in self.positions.items():
            price = price_index.get(symbol, {}).get(date)
            if price:
                mv = pos.market_value(price)
                positions_value += mv
                position_details[symbol] = {
                    "price": round(price, 2), "shares": round(pos.shares, 4),
                    "market_value": round(mv, 2), "pnl_pct": round(pos.pnl_pct(price), 2),
                    "entry_price": round(pos.entry_price, 2), "entry_date": pos.entry_date,
                    "days_held": pos.days_held(date),
                }

        total_nav = self.cash + positions_value
        prev_nav = self.nav_history[-1]["nav"] if self.nav_history else self.initial_cash
        daily_pnl = total_nav - prev_nav
        daily_pnl_pct = (daily_pnl / prev_nav * 100) if prev_nav > 0 else 0

        self.nav_history.append({
            "date": date, "nav": round(total_nav, 2),
            "cash": round(self.cash, 2),
            "positions_value": round(positions_value, 2),
            "num_positions": len(self.positions),
            "daily_pnl": round(daily_pnl, 2),
            "daily_pnl_pct": round(daily_pnl_pct, 4),
            "positions": position_details,
        })

    def position_weight(self, symbol: str, price_index: dict, date: str) -> float:
        total_nav = self.nav(price_index, date)
        if total_nav <= 0 or symbol not in self.positions:
            return 0.0
        price = price_index.get(symbol, {}).get(date, 0)
        return (self.positions[symbol].market_value(price) / total_nav) * 100


# ---------------------------------------------------------------------------
# Exit checks (pure)
# ---------------------------------------------------------------------------

def _check_stop_loss(pos: Position, current_price: float, config: dict) -> bool:
    sl = config.get("stop_loss")
    if not sl:
        return False
    if sl.get("type") == "drawdown_from_entry":
        return pos.pnl_pct(current_price) <= sl.get("value", -25)
    return False


def _check_take_profit(pos: Position, current_price: float, config: dict) -> bool:
    tp = config.get("take_profit")
    if not tp:
        return False
    tp_type = tp.get("type")
    tp_value = tp.get("value", 10)
    if tp_type == "gain_from_entry":
        return pos.pnl_pct(current_price) >= tp_value
    elif tp_type == "above_peak":
        if pos.peak_price and pos.peak_price > 0:
            gain = ((current_price - pos.peak_price) / pos.peak_price) * 100
            return gain >= tp_value
    return False


def _check_time_stop(pos: Position, current_date: str, config: dict) -> bool:
    ts = config.get("time_stop")
    if not ts:
        return False
    max_days = ts.get("max_days") or ts.get("days", 365)
    return pos.days_held(current_date) >= max_days


# ---------------------------------------------------------------------------
# Rebalancing (pure)
# ---------------------------------------------------------------------------

def _calendar_to_trading_days(calendar_days: int) -> int:
    return max(1, round(calendar_days * 5 / 7))


def _is_rebalance_date(date: str, last_rebal: str | None, frequency: str) -> bool:
    if frequency == "none" or not last_rebal:
        return False
    current = datetime.strptime(date, "%Y-%m-%d")
    last = datetime.strptime(last_rebal, "%Y-%m-%d")
    if frequency == "quarterly":
        return (current - last).days >= 90
    elif frequency == "monthly":
        return (current - last).days >= 30
    return False


def _do_rebalance(portfolio: Portfolio, price_index: dict, date: str,
                  config: dict, slippage: float, earnings_data: dict | None = None):
    """Trim-mode rebalance: clip overweight positions + earnings-beat adds."""
    rules = config.get("rebalancing", {}).get("rules", {})
    max_pct = rules.get("max_position_pct", 100)

    current_nav = portfolio.nav(price_index, date)
    if current_nav <= 0:
        return

    for symbol in list(portfolio.positions.keys()):
        weight = portfolio.position_weight(symbol, price_index, date)
        if weight > max_pct:
            trim_pct = ((weight - max_pct) / weight) * 100
            price = price_index.get(symbol, {}).get(date)
            if price:
                portfolio.close_position(symbol, date, price, "rebalance_trim",
                                         slippage, partial_pct=trim_pct)

    # Earnings-beat add
    add_on_beat = rules.get("add_on_earnings_beat")
    if add_on_beat and earnings_data:
        gain_threshold = add_on_beat.get("min_gain_pct", 15)
        max_add_multiplier = add_on_beat.get("max_add_multiplier", 1.5)
        lookback_days = add_on_beat.get("lookback_days", 90)
        current_nav = portfolio.nav(price_index, date)
        current_dt = datetime.strptime(date, "%Y-%m-%d")

        max_pct = config.get("rebalancing", {}).get("rules", {}).get("max_position_pct", 100)
        for symbol, pos in list(portfolio.positions.items()):
            price = price_index.get(symbol, {}).get(date)
            if not price or pos.pnl_pct(price) < gain_threshold:
                continue
            sym_earnings = earnings_data.get(symbol, {})
            recent_beat = False
            for earn_date, earn_data in sym_earnings.items():
                earn_dt = datetime.strptime(earn_date, "%Y-%m-%d")
                days_ago = (current_dt - earn_dt).days
                if 0 <= days_ago <= lookback_days and earn_data.get("beat"):
                    recent_beat = True
                    break
            if not recent_beat:
                continue
            # Add to position — up to max_add_multiplier of original size
            original_cost = pos.entry_price * pos.shares
            max_total = original_cost * max_add_multiplier
            current_value = pos.market_value(price)
            room_to_add = max_total - current_value
            if room_to_add <= 1000:
                continue
            amount = min(room_to_add, portfolio.cash * 0.25)  # Don't use more than 25% of cash
            if amount < 1000:
                continue
            # Check weight cap
            new_weight = ((current_value + amount) / current_nav) * 100
            if new_weight > max_pct:
                amount = (max_pct / 100 * current_nav) - current_value
                if amount < 1000:
                    continue
            portfolio.open_position(symbol, date, price, amount, slippage,
                                    peak_price=pos.peak_price)


def _do_equal_weight_rebalance(portfolio: Portfolio, price_index: dict, date: str,
                               config: dict, slippage: float, symbols: list[str],
                               signals: dict, signal_metadata: dict,
                               pe_series: dict | None = None):
    """Equal-weight rebalance with optional rotation via ranking."""
    current_nav = portfolio.nav(price_index, date)
    if current_nav <= 0:
        return

    max_positions = config["sizing"].get("max_positions", 10)
    ranking_config = config.get("ranking")

    if ranking_config:
        top_n = ranking_config.get("top_n", max_positions)
        candidates = [(sym, signals[sym][date])
                      for sym in symbols if date in signals.get(sym, {})]
        if candidates:
            ranked = _rank_candidates(candidates, config, date, price_index, pe_series)
            target_symbols = set(sym for sym, _ in ranked[:top_n])
        else:
            target_symbols = set(portfolio.positions.keys())
    else:
        target_symbols = set(portfolio.positions.keys())

    # Sell positions no longer in target set
    for symbol in list(portfolio.positions.keys()):
        if symbol not in target_symbols:
            price = price_index.get(symbol, {}).get(date)
            if price:
                portfolio.close_position(symbol, date, price, "rebalance_rotation", slippage)

    n_targets = len(target_symbols)
    if n_targets == 0:
        return

    current_nav = portfolio.nav(price_index, date)
    target_amount = current_nav / n_targets

    # Reweight existing
    for symbol in list(portfolio.positions.keys()):
        if symbol not in target_symbols:
            continue
        price = price_index.get(symbol, {}).get(date)
        if not price:
            continue
        pos = portfolio.positions[symbol]
        diff = target_amount - pos.market_value(price)
        if diff < -1000:
            trim_pct = (abs(diff) / pos.market_value(price)) * 100
            portfolio.close_position(symbol, date, price, "rebalance_trim",
                                     slippage, partial_pct=min(trim_pct, 99))
        elif diff > 1000 and portfolio.cash > 1000:
            add_amount = min(diff, portfolio.cash * 0.95)
            if add_amount >= 1000:
                sig_detail = signal_metadata.get(symbol, {}).get(date)
                portfolio.open_position(symbol, date, price, add_amount, slippage,
                                        signal_detail=sig_detail)

    # Buy new positions (rotation)
    current_nav = portfolio.nav(price_index, date)
    if len(target_symbols) == 0:
        return
    target_amount = current_nav / len(target_symbols)

    for symbol in target_symbols:
        if symbol in portfolio.positions:
            continue
        price = price_index.get(symbol, {}).get(date)
        if not price:
            continue
        amount = min(target_amount, portfolio.cash * 0.95)
        if amount < 1000:
            continue
        sig_detail = signal_metadata.get(symbol, {}).get(date)
        portfolio.open_position(symbol, date, price, amount, slippage,
                                signal_detail=sig_detail)


# ---------------------------------------------------------------------------
# Ranking (pure — no DB)
# ---------------------------------------------------------------------------

def _compute_ranking_scores(metric: str, symbols: list[str], date: str,
                            price_index: dict,
                            pe_series: dict | None = None) -> dict[str, float]:
    """Compute ranking scores for candidate symbols. Pure — no DB access."""
    scores: dict[str, float] = {}

    if metric == "pe_percentile":
        if not pe_series:
            return scores
        for symbol in symbols:
            series = pe_series.get(symbol)
            if not series:
                continue
            dates_only = [s[0] for s in series]
            idx = bisect_right(dates_only, date) - 1
            if idx >= 0:
                _, pe = series[idx]
                if 0 < pe < 500:
                    scores[symbol] = pe

    elif metric == "current_drop":
        for symbol in symbols:
            prices = price_index.get(symbol, {})
            sorted_dates = sorted(d for d in prices if d <= date)
            if len(sorted_dates) < 20:
                continue
            lookback = sorted_dates[-63:]
            peak = max(prices[d] for d in lookback)
            current = prices.get(date)
            if current and peak > 0:
                scores[symbol] = ((current - peak) / peak) * 100

    elif metric == "momentum_rank":
        for symbol in symbols:
            prices = price_index.get(symbol, {})
            sorted_dates = sorted(d for d in prices if d <= date)
            if len(sorted_dates) < 63:
                continue
            current = prices.get(date)
            past = prices.get(sorted_dates[-63])
            if current and past and past > 0:
                scores[symbol] = ((current - past) / past) * 100

    elif metric == "revenue_growth_yoy":
        # Can't rank by fundamentals without data — return empty
        pass

    elif metric == "margin_expanding":
        pass

    elif metric == "rsi":
        # Compute RSI using Wilder's smoothing (matches signals.compute_rsi)
        period = 14
        for symbol in symbols:
            prices = price_index.get(symbol, {})
            sorted_dates = sorted(d for d in prices if d <= date)
            if len(sorted_dates) < period + 2:
                continue
            closes = [prices[d] for d in sorted_dates]
            # Daily changes
            changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
            if len(changes) < period:
                continue
            # Seed with SMA of first `period` gains/losses
            avg_gain = sum(max(c, 0) for c in changes[:period]) / period
            avg_loss = sum(max(-c, 0) for c in changes[:period]) / period
            # Wilder's smoothing for remaining
            for i in range(period, len(changes)):
                gain = max(changes[i], 0)
                loss = max(-changes[i], 0)
                avg_gain = (avg_gain * (period - 1) + gain) / period
                avg_loss = (avg_loss * (period - 1) + loss) / period
            if avg_loss == 0:
                scores[symbol] = 100.0
            else:
                rs = avg_gain / avg_loss
                scores[symbol] = 100 - (100 / (1 + rs))

    return scores


def _rank_candidates(candidates: list[tuple], config: dict, date: str,
                     price_index: dict,
                     pe_series: dict | None = None) -> list[tuple]:
    """Rank entry candidates by configured metric. Pure — no DB."""
    ranking_config = config.get("ranking")
    symbols_in_play = [c[0] for c in candidates]

    if not ranking_config:
        metric = "pe_percentile"
        order = "asc"
    else:
        metric = ranking_config.get("by", "pe_percentile")
        order = ranking_config.get("order", "asc")

    scores = _compute_ranking_scores(metric, symbols_in_play, date,
                                     price_index, pe_series)
    if not scores:
        return candidates

    reverse = (order == "desc")
    scored = [(sym, dd) for sym, dd in candidates if sym in scores]
    unscored = [(sym, dd) for sym, dd in candidates if sym not in scores]
    scored.sort(key=lambda x: scores[x[0]], reverse=reverse)
    return scored + unscored


# ---------------------------------------------------------------------------
# Find pre-selloff peak (pure)
# ---------------------------------------------------------------------------

def _find_recent_peak(symbol: str, date: str, price_index: dict,
                      config: dict) -> float:
    prices = price_index.get(symbol, {})

    window_calendar = 90
    entry_config = config.get("entry", {})
    if "conditions" in entry_config:
        for condition in entry_config["conditions"]:
            if condition.get("type") in ("current_drop", "period_drop", "selloff"):
                window_calendar = condition.get("window_days", 90)
                break
    elif "trigger" in entry_config:
        window_calendar = entry_config["trigger"].get("window_days", 90)

    window_trading = _calendar_to_trading_days(window_calendar)
    sorted_dates = sorted(d for d in prices if d <= date)
    lookback = sorted_dates[-(window_trading * 2):] if len(sorted_dates) > window_trading * 2 else sorted_dates

    if not lookback:
        return prices.get(date, 0)
    return max(prices[d] for d in lookback)


# ---------------------------------------------------------------------------
# Benchmark (pure — takes prices as input)
# ---------------------------------------------------------------------------

def compute_benchmark(trading_dates: list[str], initial_cash: float,
                      benchmark_prices: dict[str, float],
                      benchmark_symbol: str = "SPY") -> dict | None:
    """
    Compute buy-and-hold benchmark.

    Args:
        trading_dates: sorted date strings.
        initial_cash: starting capital.
        benchmark_prices: {date: close_price} for the benchmark.
        benchmark_symbol: label for the benchmark.

    Returns:
        {symbol, nav_history, metrics} or None if insufficient data.
    """
    if not benchmark_prices or len(benchmark_prices) < 10:
        return None

    first_price = None
    nav_history = []
    prev_nav = initial_cash

    for date in trading_dates:
        price = benchmark_prices.get(date)
        if not price:
            continue
        if first_price is None:
            first_price = price

        nav = initial_cash * (price / first_price)
        daily_pnl = nav - prev_nav
        daily_pnl_pct = (daily_pnl / prev_nav * 100) if prev_nav > 0 else 0
        nav_history.append({
            "date": date, "nav": round(nav, 2),
            "daily_pnl_pct": round(daily_pnl_pct, 4),
        })
        prev_nav = nav

    if not nav_history:
        return None

    final_nav = nav_history[-1]["nav"]
    total_return = ((final_nav - initial_cash) / initial_cash) * 100
    calendar_days = (datetime.strptime(trading_dates[-1], "%Y-%m-%d") -
                     datetime.strptime(trading_dates[0], "%Y-%m-%d")).days
    years = max(calendar_days / 365.25, 0.01)
    ann_return = ((final_nav / initial_cash) ** (1 / years) - 1) * 100 if years > 0 else 0

    peak = 0.0
    max_dd = 0.0
    for point in nav_history:
        if point["nav"] > peak:
            peak = point["nav"]
        dd = ((point["nav"] - peak) / peak) * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    return {
        "symbol": benchmark_symbol,
        "nav_history": nav_history,
        "metrics": {
            "total_return_pct": round(total_return, 2),
            "annualized_return_pct": round(ann_return, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "final_nav": round(final_nav, 2),
        },
    }


# ---------------------------------------------------------------------------
# Performance metrics (pure)
# ---------------------------------------------------------------------------

def compute_metrics(portfolio: Portfolio, initial_cash: float,
                    trading_dates: list[str],
                    risk_free_rate_pct: float = 2.0) -> dict:
    """
    Compute summary performance metrics.

    Args:
        portfolio: Portfolio with completed simulation state.
        initial_cash: starting capital.
        trading_dates: sorted trading dates used in the simulation.
        risk_free_rate_pct: annualized risk-free rate in %.
    """
    nav_series = portfolio.nav_history
    if not nav_series:
        return {}

    final_nav = nav_series[-1]["nav"]
    total_return = ((final_nav - initial_cash) / initial_cash) * 100

    calendar_days = (datetime.strptime(trading_dates[-1], "%Y-%m-%d") -
                     datetime.strptime(trading_dates[0], "%Y-%m-%d")).days
    years = max(calendar_days / 365.25, 0.01)
    ann_return = ((final_nav / initial_cash) ** (1 / years) - 1) * 100 if years > 0 else 0

    # Max drawdown
    peak_nav = 0.0
    max_dd = 0.0
    max_dd_date = ""
    for point in nav_series:
        if point["nav"] > peak_nav:
            peak_nav = point["nav"]
        dd = ((point["nav"] - peak_nav) / peak_nav) * 100 if peak_nav > 0 else 0
        if dd < max_dd:
            max_dd = dd
            max_dd_date = point["date"]

    # Trade stats
    closed = portfolio.closed_trades
    total_entries = len([t for t in portfolio.trades if t["action"] == "BUY"])
    real_trades = [t for t in closed if t.get("reason") != "backtest_end"]
    wins = [t for t in real_trades if t["pnl"] > 0]
    losses = [t for t in real_trades if t["pnl"] <= 0]

    win_rate = (len(wins) / len(real_trades) * 100) if real_trades else 0
    all_wins = [t for t in closed if t["pnl"] > 0]
    win_rate_incl_open = (len(all_wins) / len(closed) * 100) if closed else 0
    avg_win = sum(t["pnl_pct"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnl_pct"] for t in losses) / len(losses) if losses else 0
    avg_days = sum(t["days_held"] for t in real_trades) / len(real_trades) if real_trades else 0

    # PnL by exit reason
    by_reason: dict[str, dict] = defaultdict(lambda: {"count": 0, "pnl": 0})
    for t in closed:
        reason = t.get("reason", "unknown")
        by_reason[reason]["count"] += 1
        by_reason[reason]["pnl"] += t["pnl"]

    # Profit factor
    gross_wins = sum(t["pnl"] for t in wins)
    gross_losses = abs(sum(t["pnl"] for t in losses))
    profit_factor = min(gross_wins / gross_losses, 999.99) if gross_losses > 0 else 999.99

    # Volatility metrics
    daily_returns = []
    for i in range(1, len(nav_series)):
        prev = nav_series[i - 1]["nav"]
        if prev > 0:
            daily_returns.append((nav_series[i]["nav"] - prev) / prev)

    if daily_returns and len(daily_returns) > 1:
        daily_std = statistics.stdev(daily_returns)
        ann_vol = daily_std * math.sqrt(252) * 100

        excess_return = ann_return - risk_free_rate_pct
        sharpe = excess_return / ann_vol if ann_vol > 0 else 0

        daily_rf = risk_free_rate_pct / 100 / 252
        downside_sq = [min(r - daily_rf, 0) ** 2 for r in daily_returns]
        downside_dev = math.sqrt(sum(downside_sq) / len(downside_sq)) * math.sqrt(252) * 100
        sortino = excess_return / downside_dev if downside_dev > 0 else 0
    else:
        ann_vol = 0
        sharpe = 0
        sortino = 0

    # Utilized capital metrics
    positions_values = [p["positions_value"] for p in nav_series]
    peak_utilized = max(positions_values) if positions_values else 0
    avg_utilized = sum(positions_values) / len(positions_values) if positions_values else 0
    total_pnl = final_nav - initial_cash
    return_on_utilized = (total_pnl / avg_utilized) * 100 if avg_utilized > 0 else 0
    utilization_pct = (avg_utilized / initial_cash) * 100 if initial_cash > 0 else 0

    return {
        "total_return_pct": round(total_return, 2),
        "annualized_return_pct": round(ann_return, 2),
        "max_drawdown_pct": round(max_dd, 2),
        "max_drawdown_date": max_dd_date,
        "final_nav": round(final_nav, 2),
        "total_entries": total_entries,
        "total_trades": len(real_trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(win_rate, 2),
        "win_rate_incl_open_pct": round(win_rate_incl_open, 2),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "avg_holding_days": round(avg_days, 1),
        "profit_factor": round(profit_factor, 2),
        "annualized_volatility_pct": round(ann_vol, 2),
        "sharpe_ratio": round(sharpe, 2),
        "sortino_ratio": round(sortino, 2),
        "risk_free_rate_pct": round(risk_free_rate_pct, 2),
        "peak_utilized_capital": round(peak_utilized, 2),
        "avg_utilized_capital": round(avg_utilized, 2),
        "utilization_pct": round(utilization_pct, 2),
        "return_on_utilized_capital_pct": round(return_on_utilized, 2),
        "by_exit_reason": dict(by_reason),
    }


# ---------------------------------------------------------------------------
# Main simulation loop (pure)
# ---------------------------------------------------------------------------

def run_backtest(
    config: dict,
    price_index: dict[str, dict[str, float]],
    trading_dates: list[str],
    signals: dict[str, dict[str, float]],
    signal_metadata: dict[str, dict[str, dict]] | None = None,
    exit_signals: dict[str, dict[str, dict]] | None = None,
    earnings_data: dict | None = None,
    pe_series: dict | None = None,
    benchmark_prices: dict[str, float] | None = None,
    risk_free_rate_pct: float = 2.0,
    force_close_at_end: bool = True,
) -> dict:
    """
    Run a pure backtest simulation. No database access, no file I/O.

    Args:
        config: Strategy config dict (with name, universe, entry, sizing, etc.).
        price_index: {symbol: {date: close_price}} for all universe symbols.
        trading_dates: sorted list of YYYY-MM-DD strings within the backtest range.
        signals: {symbol: {date: signal_value}} — precomputed entry signals.
        signal_metadata: {symbol: {date: metadata_dict}} — signal details carried to trades.
        exit_signals: {symbol: {date: exit_info_dict}} — fundamental exit triggers.
        earnings_data: {symbol: {date: {beat: bool, ...}}} — for earnings-based rebalancing.
        pe_series: {symbol: [(date, pe), ...]} — for PE ranking.
        benchmark_prices: {date: close_price} — for benchmark comparison.
        risk_free_rate_pct: annualized risk-free rate for Sharpe calculation.
        force_close_at_end: close all positions on last day (True for backtests).

    Returns:
        Result dict with: strategy, config, trades, closed_trades, open_positions,
        nav_history, metrics, benchmark.
    """
    signal_metadata = signal_metadata or {}
    exit_signals = exit_signals or {}

    # Config values
    initial_cash = config["sizing"]["initial_allocation"]
    slippage = config.get("backtest", {}).get("slippage_bps", 10)
    max_positions = config["sizing"].get("max_positions", 10)
    entry_mode = config.get("backtest", {}).get("entry_price", "next_close")
    entry_priority = config.get("entry", {}).get("priority", "worst_drawdown")

    cooldown_days = 0
    if config.get("stop_loss"):
        cooldown_calendar = config["stop_loss"].get("cooldown_days", 0)
        cooldown_days = _calendar_to_trading_days(cooldown_calendar) if cooldown_calendar > 0 else 0

    # Symbols with any signal data
    symbols = sorted(set(signals.keys()) | set(price_index.keys()))

    portfolio = Portfolio(initial_cash)
    pending_entries: list[tuple] = []
    last_rebal_date: str | None = None
    stop_loss_cooldowns: dict[str, str] = {}

    for i, date in enumerate(trading_dates):
        # --- Execute pending entries from previous day ---
        for symbol, peak_price, sig_detail in pending_entries:
            if symbol in portfolio.positions or len(portfolio.positions) >= max_positions:
                continue
            price = price_index.get(symbol, {}).get(date)
            if not price:
                continue

            current_nav = portfolio.nav(price_index, date)
            if current_nav <= 0:
                continue

            sizing_type = config["sizing"]["type"]
            if sizing_type == "equal_weight":
                amount = current_nav / max_positions
            elif sizing_type == "fixed_amount":
                amount = config["sizing"].get("amount_per_position", initial_cash / max_positions)
            else:
                amount = current_nav / max_positions

            amount = min(amount, portfolio.cash * 0.99)
            if amount < 1000:
                continue

            max_pct = config.get("rebalancing", {}).get("rules", {}).get("max_position_pct", 100)
            if (amount / current_nav) * 100 > max_pct:
                amount = current_nav * (max_pct / 100)

            portfolio.open_position(symbol, date, price, amount, slippage,
                                    peak_price=peak_price, signal_detail=sig_detail)

        pending_entries = []

        # --- Check exits ---
        for symbol, pos in list(portfolio.positions.items()):
            price = price_index.get(symbol, {}).get(date)
            if not price:
                continue

            if _check_stop_loss(pos, price, config):
                portfolio.close_position(symbol, date, price, "stop_loss", slippage)
                if cooldown_days > 0:
                    stop_loss_cooldowns[symbol] = date
                continue

            if _check_take_profit(pos, price, config):
                portfolio.close_position(symbol, date, price, "take_profit", slippage)
                continue

            if _check_time_stop(pos, date, config):
                portfolio.close_position(symbol, date, price, "time_stop", slippage)
                continue

            if exit_signals.get(symbol, {}).get(date):
                reason = exit_signals[symbol][date].get("reason", "fundamental_exit")
                portfolio.close_position(symbol, date, price, reason, slippage)
                continue

        # --- Rebalancing ---
        rebal_freq = config.get("rebalancing", {}).get("frequency", "none")
        if _is_rebalance_date(date, last_rebal_date, rebal_freq):
            rebal_mode = config.get("rebalancing", {}).get("mode", "trim")
            if rebal_mode == "equal_weight":
                _do_equal_weight_rebalance(portfolio, price_index, date, config,
                                           slippage, symbols, signals, signal_metadata,
                                           pe_series)
            else:
                _do_rebalance(portfolio, price_index, date, config, slippage, earnings_data)
            last_rebal_date = date
        elif last_rebal_date is None and len(portfolio.positions) > 0:
            last_rebal_date = date

        # --- New entries ---
        available_slots = max_positions - len(portfolio.positions) - len(pending_entries)
        if available_slots > 0:
            candidates = []
            date_idx = i

            for symbol in symbols:
                if symbol in portfolio.positions:
                    continue
                signal_data = signals.get(symbol, {})
                if date not in signal_data:
                    continue

                # Check stop-loss cooldown
                if cooldown_days > 0 and symbol in stop_loss_cooldowns:
                    sl_date = stop_loss_cooldowns[symbol]
                    try:
                        sl_idx = trading_dates.index(sl_date)
                    except ValueError:
                        sl_idx = -1
                    if sl_idx >= 0 and (date_idx - sl_idx) < cooldown_days:
                        continue

                drawdown = signal_data[date]
                candidates.append((symbol, drawdown))

            # Rank candidates
            if len(candidates) > available_slots:
                candidates = _rank_candidates(candidates, config, date,
                                              price_index, pe_series)
            elif entry_priority == "worst_drawdown":
                candidates.sort(key=lambda x: x[1])
            elif entry_priority == "random":
                random.shuffle(candidates)

            ranking_top_n = config.get("ranking", {}).get("top_n") if config.get("ranking") else None
            if ranking_top_n and len(candidates) > ranking_top_n:
                candidates = candidates[:ranking_top_n]

            for symbol, drawdown in candidates[:available_slots]:
                peak_price = _find_recent_peak(symbol, date, price_index, config)
                sig_detail = signal_metadata.get(symbol, {}).get(date)

                if entry_mode == "next_close":
                    pending_entries.append((symbol, peak_price, sig_detail))
                else:
                    price = price_index.get(symbol, {}).get(date)
                    if price:
                        current_nav = portfolio.nav(price_index, date)
                        amount = min(current_nav / max_positions, portfolio.cash * 0.99)
                        if amount >= 1000:
                            portfolio.open_position(symbol, date, price, amount, slippage,
                                                    peak_price=peak_price, signal_detail=sig_detail)

        # --- Record NAV ---
        portfolio.record_nav(price_index, date)

    # --- Close remaining positions ---
    if force_close_at_end and trading_dates:
        last_date = trading_dates[-1]
        for symbol in list(portfolio.positions.keys()):
            price = price_index.get(symbol, {}).get(last_date)
            if price:
                portfolio.close_position(symbol, last_date, price, "backtest_end", slippage)

    # --- Compute metrics ---
    metrics = compute_metrics(portfolio, initial_cash, trading_dates, risk_free_rate_pct)

    # --- Benchmark ---
    benchmark = None
    if benchmark_prices:
        benchmark = compute_benchmark(trading_dates, initial_cash, benchmark_prices)
        if benchmark:
            alpha = metrics["annualized_return_pct"] - benchmark["metrics"]["annualized_return_pct"]
            metrics["benchmark_return_pct"] = benchmark["metrics"]["total_return_pct"]
            metrics["benchmark_ann_return_pct"] = benchmark["metrics"]["annualized_return_pct"]
            metrics["alpha_ann_pct"] = round(alpha, 2)

    # --- Build open positions ---
    last_date = trading_dates[-1] if trading_dates else ""
    open_positions = []
    for symbol, pos in portfolio.positions.items():
        current_price = price_index.get(symbol, {}).get(last_date)
        if current_price:
            open_positions.append({
                "symbol": symbol,
                "entry_date": pos.entry_date,
                "entry_price": round(pos.entry_price, 2),
                "current_price": round(current_price, 2),
                "shares": round(pos.shares, 4),
                "market_value": round(pos.shares * current_price, 2),
                "cost_basis": round(pos.shares * pos.entry_price, 2),
                "pnl": round(pos.shares * (current_price - pos.entry_price), 2),
                "pnl_pct": round(pos.pnl_pct(current_price), 2),
                "days_held": pos.days_held(last_date),
            })

    return {
        "strategy": config["name"],
        "run_at": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "trades": portfolio.trades,
        "closed_trades": portfolio.closed_trades,
        "open_positions": open_positions,
        "nav_history": portfolio.nav_history,
        "metrics": metrics,
        "benchmark": benchmark,
    }
