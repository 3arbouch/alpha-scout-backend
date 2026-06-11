"""
Backtest models — both input (how to run a backtest) and output (what it produced).

INPUT side (BacktestConfig + EvalBlock + WindowSpec): describes the periods,
capital, and any walk-forward eval sub-windows to run. Opinion-free — no
target metric, no agent concepts. Reusable from UI buttons, CLI, agent loops,
and deployment dry-runs.

OUTPUT side (BacktestResult, BacktestMetrics, ...): what a single backtest
produced. Per-window outputs aggregate at the consumer layer (agent / UI).

Used by: backtest engine (input + output), API (request + response), runner
loops (input for N+1 runs), backtest repo (persistence).
"""

from __future__ import annotations

import re
from typing import Literal

from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Walk-forward window spec (INPUT)
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([ymdYMD])\s*$")


def _parse_duration(s: str) -> tuple[int, int, int]:
    """Parse 'Ny' / 'Nm' / 'Nd' → (years, months, days).

    Used for window/overlap. Returns a tuple so callers can build relativedelta
    and also compare magnitudes deterministically (without ambiguous month
    arithmetic).
    """
    if not isinstance(s, str):
        raise ValueError(f"duration must be a string like '2y', '6m', '180d'; got {s!r}")
    m = _DURATION_RE.match(s)
    if not m:
        raise ValueError(f"duration must match Ny|Nm|Nd (e.g. '2y', '6m', '180d'); got {s!r}")
    n = int(m.group(1))
    unit = m.group(2).lower()
    if n < 0:
        raise ValueError(f"duration must be non-negative; got {s!r}")
    if unit == "y":
        return (n, 0, 0)
    if unit == "m":
        return (0, n, 0)
    return (0, 0, n)


def _duration_to_approx_days(years: int, months: int, days: int) -> float:
    """Approximate calendar days for ordering comparisons only.

    365.25/yr, 30.4375/mo. Not used for window generation — only to enforce
    `overlap < window` at config validation. Window generation uses the exact
    `relativedelta` constructed from `_parse_duration`.
    """
    return years * 365.25 + months * 30.4375 + days


class WindowSpec(BaseModel):
    """Length + overlap for walk-forward eval windows.

    Step between successive windows = window - overlap. `overlap=0` means
    contiguous (no shared days). `overlap` must be strictly less than `window`.
    """
    window: str = Field(description="Window length, e.g. '2y', '12m', '180d'. Must parse as Ny|Nm|Nd.")
    overlap: str = Field(default="0d", description="Overlap between successive windows. Must be < window. '0d' = contiguous.")

    @model_validator(mode="after")
    def _validate(self):
        w = _parse_duration(self.window)
        o = _parse_duration(self.overlap)
        if _duration_to_approx_days(*w) <= 0:
            raise ValueError(f"window must be > 0; got {self.window!r}")
        if _duration_to_approx_days(*o) >= _duration_to_approx_days(*w):
            raise ValueError(f"overlap ({self.overlap!r}) must be strictly less than window ({self.window!r})")
        return self

    def window_delta(self) -> relativedelta:
        y, m, d = _parse_duration(self.window)
        return relativedelta(years=y, months=m, days=d)

    def step_delta(self) -> relativedelta:
        wy, wm, wd = _parse_duration(self.window)
        oy, om, od = _parse_duration(self.overlap)
        # relativedelta supports subtraction.
        return relativedelta(years=wy - oy, months=wm - om, days=wd - od)


# ---------------------------------------------------------------------------
# Eval block (INPUT)
# ---------------------------------------------------------------------------


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class EvalBlock(BaseModel):
    """Optional walk-forward eval period for a backtest.

    The `start`/`end` bounds may overlap, be inside, or sit outside the
    training period — the runner runs an independent backtest per generated
    sub-window. Partial tail (final sub-window extends past `end`) is dropped.
    """
    start: str = Field(description="ISO date YYYY-MM-DD. First eval-window start.")
    end: str = Field(description="ISO date YYYY-MM-DD. Last eval-window must end on or before this.")
    spec: WindowSpec

    @model_validator(mode="after")
    def _validate(self):
        if not _DATE_RE.match(self.start) or not _DATE_RE.match(self.end):
            raise ValueError(f"eval.start/end must be ISO YYYY-MM-DD; got {self.start!r} / {self.end!r}")
        if self.start >= self.end:
            raise ValueError(f"eval.start ({self.start}) must be strictly before eval.end ({self.end})")
        return self


# ---------------------------------------------------------------------------
# Full backtest config (INPUT)
# ---------------------------------------------------------------------------


class BacktestConfig(BaseModel):
    """Opinion-free description of how to run a backtest.

    Does NOT carry any optimization target — that lives in ResearchRunConfig
    (server/models/research_run.py). A backtest is a pure function of this
    config + a strategy/portfolio definition.
    """
    training_start: str = Field(description="ISO date YYYY-MM-DD.")
    training_end: str = Field(description="ISO date YYYY-MM-DD.")
    initial_capital: float = Field(gt=0, description="Starting capital for the training-period backtest (and, independently, for each eval window).")
    sector: str | None = Field(default=None, description="Optional sector filter; passed through to the universe.")
    benchmark_sectors: list[str] | None = Field(default=None, description="Sectors whose ETFs form the sector benchmark. One → that ETF; many → cap-weighted blend. Defaults to [sector] when unset. Used when benchmark='sector'.")
    benchmark: Literal["market", "sector"] = Field(default="market", description="Which benchmark to compute alpha against. 'sector' requires `sector` or `benchmark_sectors` to be set.")
    eval: EvalBlock | None = Field(default=None, description="Optional walk-forward eval block. Absent = single training-period backtest (today's behavior).")

    @model_validator(mode="after")
    def _validate(self):
        if not _DATE_RE.match(self.training_start) or not _DATE_RE.match(self.training_end):
            raise ValueError(
                f"training_start/training_end must be ISO YYYY-MM-DD; got "
                f"{self.training_start!r} / {self.training_end!r}"
            )
        if self.training_start >= self.training_end:
            raise ValueError(
                f"training_start ({self.training_start}) must be strictly before training_end ({self.training_end})"
            )
        if self.benchmark == "sector" and not (self.sector or self.benchmark_sectors):
            raise ValueError("benchmark='sector' requires `sector` or `benchmark_sectors` to be set")
        return self

    @classmethod
    def from_legacy_args(
        cls,
        start: str,
        end: str,
        capital: float,
        sector: str | None = None,
        benchmark: str = "market",
        benchmark_sectors: list[str] | None = None,
    ) -> "BacktestConfig":
        """Build a `BacktestConfig` from today's flat-arg call sites (no eval)."""
        return cls(
            training_start=start,
            training_end=end,
            initial_capital=capital,
            sector=sector,
            benchmark_sectors=benchmark_sectors,
            benchmark=benchmark,  # type: ignore[arg-type]
            eval=None,
        )


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
    regime_gate: list[str] = Field(default_factory=list)
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
