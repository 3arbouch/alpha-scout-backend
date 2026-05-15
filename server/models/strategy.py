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
    # --- Valuation ---
    "pe",          # market_cap / TTM net_income
    "ps",          # market_cap / TTM revenue
    "p_b",         # market_cap / total_equity
    "ev_ebitda",   # (market_cap + net_debt) / TTM ebitda
    "ev_sales",    # (market_cap + net_debt) / TTM revenue
    # --- Yield ---
    "fcf_yield",   # TTM free_cash_flow / market_cap, percent
    "div_yield",   # TTM |dividends_paid| / market_cap, percent
    # --- Growth ---
    "eps_yoy",     # latest Q eps_diluted vs same-Q prior year, percent
    "rev_yoy",     # latest Q revenue vs same-Q prior year, percent
    # --- Quality (current margins, percent) ---
    "gross_margin",  # TTM gross_profit / TTM revenue × 100
    "op_margin",     # TTM operating_income / TTM revenue × 100
    "net_margin",    # TTM net_income / TTM revenue × 100
    # --- Quality (margin trajectory) ---
    "op_margin_yoy_delta",   # current op_margin minus same-Q prior year, percentage points
    "net_margin_yoy_delta",  # current net_margin minus same-Q prior year, percentage points
    # --- Growth acceleration ---
    "rev_yoy_accel",  # latest rev_yoy minus prior-quarter rev_yoy, percentage points
    "eps_yoy_accel",  # latest eps_yoy minus prior-quarter eps_yoy, percentage points
    # --- Balance-sheet quality ---
    "roe",            # TTM net_income / total_equity × 100, percent
    "roic",           # TTM operating_income / (total_equity + total_debt) × 100, percent — proxy (no tax adjustment)
    "debt_to_equity", # total_debt / total_equity, ratio
    # --- Returns / momentum (point-in-time, percent) ---
    "ret_1m",         # 21-trading-day total return
    "ret_3m",         # 63-trading-day total return
    "ret_6m",         # 126-trading-day total return
    "ret_12m",        # 252-trading-day total return
    "ret_12_1m",      # 12-month return excluding the most recent month — "Asness momentum"
    # --- Analyst flow ---
    "analyst_net_upgrades_30d",  # net analyst upgrades minus downgrades over trailing 30 days
    "analyst_net_upgrades_90d",  # same over trailing 90 days
    # --- Calendar / event ---
    "days_since_last_earnings",  # trading days since last reported earnings
    "days_to_next_earnings",     # trading days until next expected earnings (None if unknown)
    "pre_earnings_window_5d",    # 1 if days_to_next_earnings ≤ 5, else 0
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
    type: Literal["sector", "symbols", "all", "index"] = Field(
        default="symbols",
        description="'sector' selects all tickers in a GICS sector (NOT point-in-time — uses current classifications); "
                    "'symbols' uses an explicit list; "
                    "'all' trades every ticker in the prices table; "
                    "'index' is point-in-time membership of a major index (set 'index' field to sp500|nasdaq|dowjones). "
                    "'index' is the only PIT-aware option and is the recommended type for survivorship-correct backtests.",
    )
    sector: str | None = Field(default=None, description="GICS sector name. Required when type='sector'.")
    symbols: list[str] | None = Field(default=None, description="Explicit ticker list. Required when type='symbols'.")
    index: Literal["sp500", "nasdaq", "dowjones"] | None = Field(
        default=None,
        description="Index name. Required when type='index'. Membership is computed as-of each trading day from the historical-constituent change log.",
    )
    anchor_index: Literal["sp500", "nasdaq", "dowjones"] | None = Field(
        default=None,
        description="Optional with type='sector': intersect the sector tickers with the ever-members of this index over the backtest window. Gives a PIT-aware sector universe (sector classification itself remains today's GICS — historical sector tags aren't tracked).",
    )
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
    by: FeatureName | Literal[
        # Composite multi-factor score (requires StrategyConfig.composite_score block).
        "composite_score",
        # Legacy metrics (kept for backward compatibility with saved configs)
        "pe_percentile", "current_drop", "rsi",
        "momentum_rank", "revenue_growth_yoy", "margin_expanding",
    ] = Field(default="pe_percentile")
    order: Literal["asc", "desc"] = Field(default="asc", description="'asc' = lowest first.")
    top_n: int | None = Field(default=None, description="How many candidates to select. Defaults to max_positions.")


# ---------------------------------------------------------------------------
# Composite score (multi-factor continuous ranking)
# ---------------------------------------------------------------------------

class CompositeFactor(BaseModel):
    """One factor within a composite-score bucket.

    `name` is any registered feature (string, validated by the engine against
    the factor registry — covers all materialized + on-the-fly features).
    `sign` flips direction so 'higher = better' for the bucket after sign
    application: e.g. for low-PE-is-good, pass {name: "pe", sign: "-"}.
    """
    name: str = Field(min_length=1, description="Feature name from the registry.")
    sign: Literal["+", "-"] = Field(default="+", description="'-' inverts the factor (low = good).")


class CompositeBucket(BaseModel):
    """One bucket (economic family) inside the composite score.

    Bucket z-score at date t = average of rank-normalized z-scores of its
    member factors (after sign flips), computed cross-sectionally across the
    candidates remaining after entry filters.
    """
    factors: list[CompositeFactor] = Field(min_length=1, description="One or more factors in this bucket.")
    weight: float = Field(default=1.0, description="Raw weight; engine auto-normalizes so all bucket weights sum to 1.")


class CompositeScoreConfig(BaseModel):
    """Multi-factor continuous score for ranking candidates.

    score(stock, t) = Σ_b (weight_b / Σ_w) × mean_{f∈b}( sign_f · z(f, stock, t) )

    where z is computed cross-sectionally over the candidate set remaining
    after entry filters at date t. Engages when ranking.by == 'composite_score'.
    """
    buckets: dict[str, CompositeBucket] = Field(
        min_length=1,
        description="Bucket name → composition. Bucket names are display labels (momentum, quality, value, ...).",
    )
    standardization: Literal["rank", "z"] = Field(
        default="rank",
        description="'rank' = rank-then-normalize (robust to outliers; recommended). 'z' = (x-mean)/stdev.",
    )

    @model_validator(mode="after")
    def _validate_weights(self):
        total = sum(b.weight for b in self.buckets.values())
        if total <= 0:
            raise ValueError("composite_score: bucket weights must sum to a positive number")
        return self


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
    """Position sizing.

    risk_parity sizes each NEW position inversely proportional to its daily
    realized volatility over `vol_window_days`. Weights are normalized so the
    new positions still consume the same aggregate capital as equal_weight
    (i.e. n_new × current_nav / max_positions). Falls back to equal_weight
    for any candidate whose vol cannot be estimated (insufficient history).
    """
    type: Literal["equal_weight", "risk_parity", "fixed_amount"] = Field(default="equal_weight")
    max_positions: int = Field(ge=1, le=100, default=10)
    initial_allocation: float = Field(ge=0, default=1_000_000, description="Starting capital in USD.")
    vol_window_days: int = Field(default=20, ge=10, le=252,
                                  description="Window for risk_parity vol estimate. Ignored otherwise.")
    vol_source: Literal["historical", "ewma"] = Field(default="historical",
                                                      description="risk_parity vol source.")
    shares: Literal["fractional", "whole"] = Field(default="fractional",
                                                    description="Share rounding. 'whole' floors to integer shares (real broker constraint); 'fractional' allows partial shares (paper/idealized).")


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
    composite_score: CompositeScoreConfig | None = Field(
        default=None,
        description="Multi-factor continuous score. Engages when ranking.by == 'composite_score'.",
    )
    rebalancing: RebalancingConfig = Field(default_factory=RebalancingConfig)
    sizing: SizingConfig = Field(default_factory=SizingConfig)
    backtest: BacktestParams = Field(default_factory=BacktestParams)
    created_at: str | None = None
    updated_at: str | None = None
