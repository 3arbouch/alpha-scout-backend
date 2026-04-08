"""
Backtest result models.

Represents the output of running a strategy or portfolio through
the backtest engine: trades, daily NAV snapshots, and performance metrics.

Used by: backtest engine (output), API (response), backtest repo (persistence).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Trade
# ---------------------------------------------------------------------------

class TradeRecord(BaseModel):
    """A single executed trade (buy or sell)."""
    date: str
    symbol: str
    action: str = Field(description="'BUY' or 'SELL'.")
    price: float
    shares: float
    amount: float
    reason: str | None = Field(default=None, description="Exit reason: stop_loss, take_profit, time_stop, rebalance_trim, etc.")
    signal_detail: dict | None = Field(default=None, description="Entry signal metadata carried through to the sell.")
    # Sell-side fields (None for buys)
    entry_date: str | None = None
    entry_price: float | None = None
    pnl: float | None = None
    pnl_pct: float | None = None
    days_held: int | None = None


# ---------------------------------------------------------------------------
# Position Snapshot (inside daily NAV)
# ---------------------------------------------------------------------------

class PositionSnapshot(BaseModel):
    """Point-in-time state of a single open position."""
    symbol: str
    price: float
    shares: float
    market_value: float
    entry_price: float
    entry_date: str
    pnl_pct: float
    days_held: int


# ---------------------------------------------------------------------------
# Daily NAV Snapshot
# ---------------------------------------------------------------------------

class DailySnapshot(BaseModel):
    """Single day's portfolio state."""
    date: str
    nav: float
    cash: float
    positions_value: float
    num_positions: int
    daily_pnl: float
    daily_pnl_pct: float


# ---------------------------------------------------------------------------
# Performance Metrics
# ---------------------------------------------------------------------------

class BacktestMetrics(BaseModel):
    """Aggregate performance metrics for a backtest run."""
    total_return_pct: float
    annualized_return_pct: float
    annualized_volatility_pct: float = 0
    max_drawdown_pct: float
    max_drawdown_date: str | None = None
    final_nav: float
    sharpe_ratio: float = 0
    sortino_ratio: float = 0
    calmar_ratio: float | None = None
    profit_factor: float = 0
    risk_free_rate_pct: float = 0

    total_entries: int
    total_trades: int = 0
    closed_trades: int = 0
    wins: int
    losses: int
    win_rate_pct: float
    win_rate_incl_open_pct: float | None = None
    avg_win_pct: float = 0
    avg_loss_pct: float = 0
    avg_holding_days: float = 0

    peak_utilized_capital: float = 0
    avg_utilized_capital: float = 0
    utilization_pct: float = 0
    return_on_utilized_capital_pct: float = 0

    by_exit_reason: dict[str, int] | None = None


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

class BenchmarkResult(BaseModel):
    """Benchmark comparison (typically SPY)."""
    symbol: str = "SPY"
    total_return_pct: float = 0
    annualized_return_pct: float = 0
    max_drawdown_pct: float = 0
    sharpe_ratio: float = 0


# ---------------------------------------------------------------------------
# Full Backtest Result
# ---------------------------------------------------------------------------

class BacktestResult(BaseModel):
    """Complete output of a single-strategy backtest."""
    strategy: str = Field(description="Strategy name.")
    strategy_id: str | None = None
    run_id: str | None = None
    run_at: str | None = None
    config: dict = Field(description="Frozen strategy config used for this run.")
    metrics: BacktestMetrics
    trades: list[TradeRecord] = Field(default_factory=list)
    closed_trades: list[TradeRecord] = Field(default_factory=list)
    open_positions: list[PositionSnapshot] = Field(default_factory=list)
    nav_history: list[DailySnapshot] = Field(default_factory=list)
    benchmark: BenchmarkResult | None = None


# ---------------------------------------------------------------------------
# Portfolio Backtest Sleeve Result
# ---------------------------------------------------------------------------

class SleeveResult(BaseModel):
    """Per-sleeve result within a portfolio backtest."""
    label: str
    strategy_id: str | None = None
    weight: float
    regime_gates: list[str] = Field(default_factory=list)
    metrics: BacktestMetrics | None = None
    trades: list[TradeRecord] = Field(default_factory=list)
    closed_trades: list[TradeRecord] = Field(default_factory=list)
    open_positions: list[PositionSnapshot] = Field(default_factory=list)
    active_days: int = 0
    gated_off_days: int = 0


class PortfolioBacktestResult(BaseModel):
    """Complete output of a portfolio backtest."""
    portfolio_id: str | None = None
    name: str
    run_id: str | None = None
    config: dict
    metrics: BacktestMetrics
    per_sleeve: list[SleeveResult] = Field(default_factory=list)
    combined_nav_history: list[DailySnapshot] = Field(default_factory=list)
    regime_history: list[dict] = Field(default_factory=list)
    allocation_profile_history: list[dict] = Field(default_factory=list)
    benchmark: BenchmarkResult | None = None
