"""
Shared NAV-derived performance stats: Sharpe, Sortino, volatility, risk-free.

Single source of truth so the backtest engine and the signal-research tool
(`auto_trader/signal_ranker.py`) report the same numbers under the same
formulas. Anything that derives from a daily-return series belongs here.
"""

from __future__ import annotations

import json
import math
import statistics
from pathlib import Path


ANNUALIZATION_TRADING_DAYS = 252


def load_risk_free_ann_pct(start_date: str, end_date: str) -> float:
    """Average 3-month Treasury yield (%) over [start, end].

    Returns 0.0 when the treasury data file is missing, 2.0 on parse error
    (matches backtest_engine's historical fallback), else the period mean.
    """
    risk_free_ann = 0.0
    try:
        treasury_path = (
            Path(__file__).parent.parent / "data" / "macro" / "treasury-rates.json"
        )
        if treasury_path.exists():
            treasury_data = json.loads(treasury_path.read_text())
            t_rates = (
                treasury_data.get("data", treasury_data)
                if isinstance(treasury_data, dict)
                else treasury_data
            )
            period_rates = [
                r["month3"]
                for r in t_rates
                if start_date <= r["date"] <= end_date and r.get("month3") is not None
            ]
            if period_rates:
                risk_free_ann = sum(period_rates) / len(period_rates)
    except Exception:
        risk_free_ann = 2.0
    return risk_free_ann


def compute_nav_stats(
    daily_returns: list[float],
    n_nav: int,
    total_return_pct: float,
    ann_return_pct: float | None,
    risk_free_ann_pct: float,
) -> dict:
    """Sharpe / Sortino / vol from a daily-return series, basis-aware on `n_nav`.

    Inputs:
        daily_returns: list of period-over-period simple returns (e.g. 0.0123).
        n_nav: number of NAV observations (typically len(daily_returns) + 1).
        total_return_pct: realized period return in percent.
        ann_return_pct: annualized return in percent, or None if the window is
            too short to annualize honestly. When None, all stats return None.
        risk_free_ann_pct: annualized risk-free rate in percent.

    Returns the same fields the backtest engine emits in its stats block:
        annualized_volatility_pct, sharpe_ratio, sharpe_ratio_annualized,
        sharpe_ratio_period, sharpe_basis, sortino_ratio.

    `sharpe_ratio` is basis-aware: period-Sharpe when the window is shorter
    than one trading year, annualized otherwise. Side fields are always
    populated so consumers can pick whichever basis suits.
    """
    if not daily_returns or ann_return_pct is None:
        return {
            "annualized_volatility_pct": None,
            "sharpe_ratio": None,
            "sharpe_ratio_annualized": None,
            "sharpe_ratio_period": None,
            "sharpe_basis": None,
            "sortino_ratio": None,
        }

    daily_std = statistics.stdev(daily_returns) if len(daily_returns) > 1 else 0
    ann_vol = daily_std * (ANNUALIZATION_TRADING_DAYS ** 0.5) * 100
    excess_return = ann_return_pct - risk_free_ann_pct
    sharpe_ann = excess_return / ann_vol if ann_vol > 0 else 0

    n = len(daily_returns)
    period_vol = daily_std * (n ** 0.5) * 100
    rf_period = risk_free_ann_pct * (n / ANNUALIZATION_TRADING_DAYS)
    sharpe_period = (
        (total_return_pct - rf_period) / period_vol if period_vol > 0 else 0
    )

    if n_nav < ANNUALIZATION_TRADING_DAYS:
        sharpe = sharpe_period
        sharpe_basis = "period"
    else:
        sharpe = sharpe_ann
        sharpe_basis = "annualized"

    daily_rf = risk_free_ann_pct / 100 / ANNUALIZATION_TRADING_DAYS
    downside_sq = [min(r - daily_rf, 0) ** 2 for r in daily_returns]
    downside_dev = (
        math.sqrt(sum(downside_sq) / len(downside_sq))
        * math.sqrt(ANNUALIZATION_TRADING_DAYS)
        * 100
    )
    sortino = excess_return / downside_dev if downside_dev > 0 else 0

    return {
        "annualized_volatility_pct": ann_vol,
        "sharpe_ratio": sharpe,
        "sharpe_ratio_annualized": sharpe_ann,
        "sharpe_ratio_period": sharpe_period,
        "sharpe_basis": sharpe_basis,
        "sortino_ratio": sortino,
    }
