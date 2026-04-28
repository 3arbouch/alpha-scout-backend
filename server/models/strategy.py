"""
Strategy configuration models — single source of truth.

Covers all entry/exit condition types, universe definition, position sizing,
stop loss, take profit, rebalancing, ranking, and the full StrategyConfig.

Used by: API validation, backtest engine, deploy engine, portfolio engine.
"""

from __future__ import annotations

from typing import Annotated, Literal
from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Entry Conditions (discriminated union on "type")
# ---------------------------------------------------------------------------

class CurrentDropCondition(BaseModel):
    """Stock is currently X% below its rolling N-day high."""
    type: Literal["current_drop"] = "current_drop"
    threshold: float = Field(le=0, description="Negative %. e.g. -25 = 25% below window high.")
    window_days: int = Field(default=90, ge=1, le=1000, description="Lookback window in calendar days.")


class PeriodDropCondition(BaseModel):
    """Worst peak-to-trough drawdown within a sliding N-day window exceeded X%."""
    type: Literal["period_drop"] = "period_drop"
    threshold: float = Field(le=0, description="Negative %.")
    window_days: int = Field(default=90, ge=1, le=1000, description="Lookback window in calendar days.")


class DailyDropCondition(BaseModel):
    """Single-day crash. Stock fell X% from yesterday's close."""
    type: Literal["daily_drop"] = "daily_drop"
    threshold: float = Field(le=0, description="Negative %. e.g. -8 = 8% single-day drop.")


class SelloffCondition(BaseModel):
    """Full drawdown cycle from ATH/52w peak. Active until recovery."""
    type: Literal["selloff"] = "selloff"
    threshold: float = Field(le=0, description="Negative %.")
    peak_window: Literal["all_time", "52w"] = Field(default="all_time")


class EarningsMomentumCondition(BaseModel):
    """Filters by recent earnings beat/miss pattern."""
    type: Literal["earnings_momentum"] = "earnings_momentum"
    lookback_quarters: int = Field(default=4, ge=1, le=8)
    min_beats: int = Field(default=2, ge=0, le=8)
    min_avg_surprise_pct: float | None = Field(default=None)
    no_recent_miss: bool = Field(default=False)


class PePercentileCondition(BaseModel):
    """PE percentile ranking within universe. Bottom N% get a signal."""
    type: Literal["pe_percentile"] = "pe_percentile"
    max_percentile: float = Field(default=30, description="Bottom N% = cheapest.")
    min_pe: float = Field(default=0, description="Floor PE to exclude near-zero spikes.")
    max_pe: float = Field(default=500, description="Cap to exclude outliers.")


class RevenueGrowthCondition(BaseModel):
    """Quarterly revenue YoY growth >= threshold. Fires on filing date."""
    type: Literal["revenue_growth_yoy"] = "revenue_growth_yoy"
    threshold: float = Field(default=50, description="Minimum YoY revenue growth %.")


class RevenueAcceleratingCondition(BaseModel):
    """Revenue YoY growth increasing for N consecutive quarters."""
    type: Literal["revenue_accelerating"] = "revenue_accelerating"
    min_quarters: int = Field(default=2, ge=1)


class MarginExpandingCondition(BaseModel):
    """Margin expanding YoY and sequentially for N consecutive quarters."""
    type: Literal["margin_expanding"] = "margin_expanding"
    metric: Literal["net_margin", "op_margin"] = Field(default="net_margin")
    min_quarters: int = Field(default=2, ge=1)


class MarginTurnaroundCondition(BaseModel):
    """Margin expanded >= threshold bps YoY for N consecutive quarters."""
    type: Literal["margin_turnaround"] = "margin_turnaround"
    metric: Literal["net_margin", "op_margin"] = Field(default="net_margin")
    threshold_bps: float = Field(default=1000, description="Minimum margin expansion in basis points YoY.")
    min_quarters: int = Field(default=2, ge=1)


class RelativePerformanceCondition(BaseModel):
    """Stock trailing return minus SPX trailing return > threshold."""
    type: Literal["relative_performance"] = "relative_performance"
    threshold: float = Field(default=20, description="Outperformance vs SPX in percentage points.")
    window_days: int = Field(default=126, description="Trading days lookback.")


class VolumeConvictionCondition(BaseModel):
    """Low volume consolidation with price above long-term average."""
    type: Literal["volume_conviction"] = "volume_conviction"
    short_window: int = Field(default=60)
    long_window: int = Field(default=252)
    ratio: float = Field(default=0.8, description="Short avg volume < ratio x long avg volume.")


class RsiCondition(BaseModel):
    """RSI indicator condition."""
    type: Literal["rsi"] = "rsi"
    period: int = Field(default=14, ge=2)
    operator: Literal[">", ">=", "<", "<=", "==", "!="] = Field(default="<=")
    value: float = Field(default=30, description="RSI threshold.")


class MomentumRankCondition(BaseModel):
    """Cross-sectional momentum percentile rank."""
    type: Literal["momentum_rank"] = "momentum_rank"
    lookback: int = Field(default=63, description="Trading days for momentum calc.")
    operator: Literal[">", ">=", "<", "<=", "==", "!="] = Field(default=">=")
    value: float = Field(default=75, description="Percentile rank threshold.")


class MaCrossoverCondition(BaseModel):
    """Moving average crossover signal."""
    type: Literal["ma_crossover"] = "ma_crossover"
    fast: int = Field(default=50)
    slow: int = Field(default=200)
    operator: Literal[">", ">=", "<", "<=", "==", "!="] = Field(default="==")
    value: int = Field(default=1, description="1 = fast above slow (golden cross).")


class VolumCapitulationCondition(BaseModel):
    """Volume spike indicating capitulation selling."""
    type: Literal["volume_capitulation"] = "volume_capitulation"
    window: int = Field(default=20)
    multiplier: float = Field(default=3.0, description="Volume must exceed multiplier x avg.")


class AlwaysCondition(BaseModel):
    """Every ticker qualifies on every trading day. For buy-and-hold / rotation strategies."""
    type: Literal["always"] = "always"


# ---------------------------------------------------------------------------
# Feature-table conditions — read from market.db features_daily.
# Values are point-in-time: latest quarterly report as-of the trading day,
# combined with that day's close for price-dependent ratios. Same values are
# queryable via `data-query` so agent research and engine execution agree.
# ---------------------------------------------------------------------------
FeatureName = Literal[
    "pe",          # market_cap / TTM net_income
    "ps",          # market_cap / TTM revenue
    "p_b",         # market_cap / total_equity
    "ev_ebitda",   # (market_cap + net_debt) / TTM ebitda
    "ev_sales",    # (market_cap + net_debt) / TTM revenue
    "fcf_yield",   # TTM free_cash_flow / market_cap, percent
    "div_yield",   # TTM |dividends_paid| / market_cap, percent
    "eps_yoy",     # latest Q eps_diluted vs same-Q prior year, percent
    "rev_yoy",     # latest Q revenue vs same-Q prior year, percent
]


class FeatureThresholdCondition(BaseModel):
    """Fires when the as-of feature value passes operator/value on that day."""
    type: Literal["feature_threshold"] = "feature_threshold"
    feature: FeatureName
    operator: Literal[">", ">=", "<", "<=", "==", "!="] = ">="
    value: float


class FeaturePercentileCondition(BaseModel):
    """Ranks symbols by feature on each trading day; bottom max_percentile get a signal.

    scope='universe' ranks across all active symbols; 'sector' ranks within GICS sector.
    Optional min_value / max_value filter outliers before ranking (e.g. min_pe=0 to
    exclude negative-earnings names from a cheap-PE screen).
    """
    type: Literal["feature_percentile"] = "feature_percentile"
    feature: FeatureName
    max_percentile: float = Field(default=30, ge=0, le=100)
    scope: Literal["universe", "sector"] = "universe"
    min_value: float | None = None
    max_value: float | None = None


class DaysToEarningsCondition(BaseModel):
    """Fires on trading days where the next earnings event is [min_days, max_days] away.

    Use to enter pre-earnings momentum (e.g. 0..5 days out) or to blackout
    positions around earnings (combine inversely in a sleeve).
    """
    type: Literal["days_to_earnings"] = "days_to_earnings"
    min_days: int = Field(default=0, ge=0)
    max_days: int = Field(default=7, ge=1)


class AnalystUpgradesCondition(BaseModel):
    """Net (upgrades - downgrades) in trailing window ≥ min_net_upgrades."""
    type: Literal["analyst_upgrades"] = "analyst_upgrades"
    window_days: int = Field(default=90, ge=1, le=365)
    min_net_upgrades: int = Field(default=2, ge=1)


EntryCondition = Annotated[
    CurrentDropCondition
    | PeriodDropCondition
    | DailyDropCondition
    | SelloffCondition
    | EarningsMomentumCondition
    | PePercentileCondition
    | RevenueGrowthCondition
    | RevenueAcceleratingCondition
    | MarginExpandingCondition
    | MarginTurnaroundCondition
    | RelativePerformanceCondition
    | VolumeConvictionCondition
    | RsiCondition
    | MomentumRankCondition
    | MaCrossoverCondition
    | VolumCapitulationCondition
    | AlwaysCondition
    | FeatureThresholdCondition
    | FeaturePercentileCondition
    | DaysToEarningsCondition
    | AnalystUpgradesCondition,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Exit Conditions
# ---------------------------------------------------------------------------

class RevenueDecelerationExit(BaseModel):
    """Revenue YoY growth declining for N consecutive quarters."""
    type: Literal["revenue_deceleration"] = "revenue_deceleration"
    min_quarters: int = Field(default=2, ge=1)
    require_margin_compression: bool = Field(default=True)
    metric: Literal["net_margin", "op_margin"] = Field(default="net_margin")


class MarginCollapseExit(BaseModel):
    """Margin contracting > threshold bps YoY for N consecutive quarters."""
    type: Literal["margin_collapse"] = "margin_collapse"
    metric: Literal["net_margin", "op_margin"] = Field(default="net_margin")
    threshold_bps: float = Field(default=-500, description="Negative bps. -500 = margin contracted 5pp YoY.")
    min_quarters: int = Field(default=2, ge=1)


ExitCondition = Annotated[
    RevenueDecelerationExit | MarginCollapseExit,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Strategy Sub-Configs
# ---------------------------------------------------------------------------

class UniverseConfig(BaseModel):
    """Defines which stocks the strategy can trade."""
    type: Literal["sector", "symbols", "all"] = Field(
        default="symbols",
        description="'sector' selects all tickers in a GICS sector; 'symbols' uses an explicit list; 'all' trades the full universe.",
    )
    sector: str | None = Field(default=None, description="GICS sector name. Required when type='sector'.")
    symbols: list[str] | None = Field(default=None, description="Explicit ticker list. Required when type='symbols'.")
    exclude: list[str] = Field(default_factory=list, description="Tickers to exclude.")

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_fields(cls, data):
        """Accept legacy 'tickers' field name and type value from old API clients / stored data."""
        if isinstance(data, dict):
            # "tickers" field → "symbols"
            if "tickers" in data and "symbols" not in data:
                data["symbols"] = data.pop("tickers")
            elif "tickers" in data:
                data.pop("tickers")
            # type value "tickers" → "symbols"
            if data.get("type") == "tickers":
                data["type"] = "symbols"
        return data


class EntryConfig(BaseModel):
    """Entry signal configuration."""
    conditions: list[EntryCondition] = Field(min_length=1, description="List of entry conditions.")
    logic: Literal["all", "any"] = Field(default="all", description="'all' = AND, 'any' = OR.")
    priority: Literal["worst_drawdown", "random"] = Field(
        default="worst_drawdown",
        description="How to rank candidates when multiple stocks trigger simultaneously.",
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_trigger(cls, data):
        """Accept legacy trigger/confirm format from old configs."""
        if isinstance(data, dict):
            if "trigger" in data and "conditions" not in data:
                trigger = data.pop("trigger")
                conditions = [trigger]
                confirm = data.pop("confirm", None) or []
                conditions.extend(confirm)
                data["conditions"] = conditions
                data["logic"] = "all"
            else:
                data.pop("trigger", None)
                data.pop("confirm", None)
        return data


class DrawdownFromEntryStop(BaseModel):
    """Fixed-percent stop. exit if pnl_pct <= value."""
    type: Literal["drawdown_from_entry"] = "drawdown_from_entry"
    value: float = Field(default=-35, description="Negative %. e.g. -35 = exit at 35% loss.")
    cooldown_days: int = Field(ge=0, default=90, description="Days before re-entering same ticker after stop.")


class AtrMultipleStop(BaseModel):
    """ATR-multiple stop. stop_price = entry - k * ATR(window_days), frozen at entry."""
    type: Literal["atr_multiple"] = "atr_multiple"
    k: float = Field(gt=0, le=10, description="ATR multiplier. Bounded so stops actually fire.")
    window_days: int = Field(ge=10, le=252, description="ATR lookback in trading bars.")
    cooldown_days: int = Field(ge=0, default=90, description="Days before re-entering same ticker after stop.")


class RealizedVolMultipleStop(BaseModel):
    """Realized-vol-multiple stop. stop_price = entry * (1 - k * sigma_daily), frozen at entry."""
    type: Literal["realized_vol_multiple"] = "realized_vol_multiple"
    k: float = Field(gt=0, le=10, description="Sigma multiplier.")
    window_days: int = Field(ge=10, le=252, description="Sigma lookback in trading bars.")
    sigma_source: Literal["historical", "ewma"] = Field(default="historical")
    cooldown_days: int = Field(ge=0, default=90, description="Days before re-entering same ticker after stop.")


StopLossConfig = Annotated[
    DrawdownFromEntryStop | AtrMultipleStop | RealizedVolMultipleStop,
    Field(discriminator="type"),
]


class GainFromEntryTP(BaseModel):
    """Fixed-percent take profit. exit if pnl_pct >= value."""
    type: Literal["gain_from_entry"] = "gain_from_entry"
    value: float = Field(default=60, description="Positive %. e.g. 60 = sell at 60% profit.")


class AbovePeakTP(BaseModel):
    """Trailing-style take profit relative to running peak."""
    type: Literal["above_peak"] = "above_peak"
    value: float = Field(default=60, description="Positive %. exit if (current - peak)/peak * 100 >= value.")


class AtrMultipleTP(BaseModel):
    """ATR-multiple take profit. tp_price = entry + k * ATR(window_days), frozen at entry."""
    type: Literal["atr_multiple"] = "atr_multiple"
    k: float = Field(gt=0, le=10, description="ATR multiplier.")
    window_days: int = Field(ge=10, le=252, description="ATR lookback in trading bars.")


class RealizedVolMultipleTP(BaseModel):
    """Realized-vol-multiple take profit. tp_price = entry * (1 + k * sigma_daily), frozen at entry."""
    type: Literal["realized_vol_multiple"] = "realized_vol_multiple"
    k: float = Field(gt=0, le=10, description="Sigma multiplier.")
    window_days: int = Field(ge=10, le=252, description="Sigma lookback in trading bars.")
    sigma_source: Literal["historical", "ewma"] = Field(default="historical")


TakeProfitConfig = Annotated[
    GainFromEntryTP | AbovePeakTP | AtrMultipleTP | RealizedVolMultipleTP,
    Field(discriminator="type"),
]


class TimeStopConfig(BaseModel):
    """Time-based exit."""
    max_days: int = Field(ge=1, description="Max holding period in calendar days.")

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_fields(cls, data):
        """Accept legacy 'days' field name from old API clients / stored data."""
        if isinstance(data, dict):
            if "days" in data and "max_days" not in data:
                data["max_days"] = data.pop("days")
            elif "days" in data:
                data.pop("days")
        return data


class RankingConfig(BaseModel):
    """Rank qualified candidates by a metric before applying max_positions."""
    by: Literal[
        # Legacy metrics (kept for backward compatibility with saved configs)
        "pe_percentile", "current_drop", "rsi",
        "momentum_rank", "revenue_growth_yoy", "margin_expanding",
        # features_daily columns — any feature is rankable
        "pe", "ps", "p_b", "ev_ebitda", "ev_sales",
        "fcf_yield", "div_yield", "eps_yoy", "rev_yoy",
    ] = Field(default="pe_percentile")
    order: Literal["asc", "desc"] = Field(default="asc", description="'asc' = lowest first.")
    top_n: int | None = Field(default=None, description="How many candidates to select. Defaults to max_positions.")


class RebalancingRules(BaseModel):
    """Rules applied during periodic rebalancing."""
    max_position_pct: float = Field(ge=1, le=100, default=25, description="Max weight (%) for any single position.")
    on_earnings_beat: Literal["hold", "trim", "add"] = Field(default="hold")
    on_earnings_miss: Literal["hold", "trim", "sell"] = Field(default="trim")
    trim_pct: float = Field(ge=0, le=100, default=50, description="% of position to trim.")
    add_on_earnings_beat: dict | None = Field(default=None, description="Config for adding on earnings beat: {min_gain_pct, max_add_multiplier, lookback_days}.")


class RebalancingConfig(BaseModel):
    """Periodic portfolio rebalancing."""
    frequency: Literal["none", "quarterly", "monthly", "on_earnings"] = Field(default="none")
    mode: Literal["trim", "equal_weight"] = Field(default="trim")
    rules: RebalancingRules = Field(default_factory=RebalancingRules)


class SizingConfig(BaseModel):
    """Position sizing."""
    type: Literal["equal_weight", "risk_parity", "fixed_amount"] = Field(default="equal_weight")
    max_positions: int = Field(ge=1, le=100, default=10)
    initial_allocation: float = Field(ge=0, default=1_000_000, description="Starting capital in USD.")


class BacktestParams(BaseModel):
    """Backtest simulation parameters (not part of live strategy logic)."""
    start: str = Field(default="2015-01-01", description="YYYY-MM-DD.")
    end: str = Field(default="2025-12-31", description="YYYY-MM-DD.")
    initial_capital: float = Field(default=1_000_000, ge=0, description="Starting capital in USD.")
    entry_price: Literal["next_close", "next_open"] = Field(default="next_close")
    slippage_bps: int = Field(ge=0, default=10, description="Slippage in basis points.")


# ---------------------------------------------------------------------------
# Full Strategy Config
# ---------------------------------------------------------------------------

class StrategyConfig(BaseModel):
    """Complete strategy definition. Single source of truth for all layers."""
    strategy_id: str | None = Field(default=None, description="Deterministic hash of core params. Computed on save.")
    name: str = Field(min_length=1, max_length=200)
    version: int = Field(default=1)
    universe: UniverseConfig
    entry: EntryConfig
    stop_loss: StopLossConfig | None = None
    take_profit: TakeProfitConfig | None = None
    time_stop: TimeStopConfig | None = None
    exit_conditions: list[ExitCondition] | None = None
    ranking: RankingConfig | None = None
    rebalancing: RebalancingConfig = Field(default_factory=RebalancingConfig)
    sizing: SizingConfig = Field(default_factory=SizingConfig)
    backtest: BacktestParams = Field(default_factory=BacktestParams)
    created_at: str | None = None
    updated_at: str | None = None
