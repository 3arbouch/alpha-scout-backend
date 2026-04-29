"""
Strategy configuration models — single source of truth.

Covers all entry/exit condition types, universe definition, position sizing,
stop loss, take profit, rebalancing, ranking, and the full StrategyConfig.

Used by: API validation, backtest engine, deploy engine, portfolio engine.
"""

from __future__ import annotations

from typing import Annotated, Literal
from pydantic import BaseModel, Field, model_validator

# Import the registry first — the library modules register features at import
# time, so feature_names() returns a complete tuple by the time we use it
# below to build the FeatureName Literal. Try both import paths so this
# module loads whether the API is started from the repo root or from server/.
try:
    from server.factors import feature_names as _registry_feature_names
except ModuleNotFoundError:
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parent.parent.parent))
    from server.factors import feature_names as _registry_feature_names

_FEATURE_NAMES = _registry_feature_names()


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
#
# FeatureName is generated from the factor registry (server/factors/library/*).
# Adding a feature there expands this enum on next process start — schema,
# agent prompt, and engine all update without any edit to this file.
# ---------------------------------------------------------------------------
FeatureName = Literal[*_FEATURE_NAMES]  # type: ignore[valid-type]


class FeatureThresholdCondition(BaseModel):
    """Fires when the as-of feature value passes operator/value on that day.

    `smoothing` (optional, 2–60) replaces today's raw value with the N-day SMA
    of the feature before applying the comparator. Useful for noisy factors
    (rsi_14, vol_z_20, ret_*, analyst_net_upgrades_30d) where single-day
    flicker generates false fires. Lookahead-clean by construction — the SMA
    at trading day T uses values through T only.
    """
    type: Literal["feature_threshold"] = "feature_threshold"
    feature: FeatureName
    operator: Literal[">", ">=", "<", "<=", "==", "!="] = ">="
    value: float
    smoothing: int | None = Field(
        default=None, ge=2, le=60,
        description="Optional N-day SMA window applied to the feature before the operator/value comparison. Reduces single-day noise on jittery factors. None = compare today's raw value.",
    )


class FeaturePercentileCondition(BaseModel):
    """Ranks symbols by feature on each trading day; bottom max_percentile get a signal.

    scope='universe' ranks across all active symbols; 'sector' ranks within GICS sector.
    Optional min_value / max_value filter outliers before ranking (e.g. min_pe=0 to
    exclude negative-earnings names from a cheap-PE screen).

    `smoothing` (optional, 2–60) applies an N-day SMA to each symbol's feature
    BEFORE the cross-sectional rank, so symbols are ranked on smoothed values
    rather than today's raw value. Stabilizes the rank when a factor is noisy.
    """
    type: Literal["feature_percentile"] = "feature_percentile"
    feature: FeatureName
    max_percentile: float = Field(default=30, ge=0, le=100)
    scope: Literal["universe", "sector"] = "universe"
    min_value: float | None = None
    max_value: float | None = None
    smoothing: int | None = Field(
        default=None, ge=2, le=60,
        description="Optional N-day SMA window applied to each symbol's feature BEFORE the cross-sectional rank.",
    )


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
    RevenueDecelerationExit | MarginCollapseExit
    # The same generic conditions used for entries are valid as exits — fire
    # on every (symbol, date) where the rule matches; the engine flags any
    # open position for exit on those dates. Lets a strategy say "exit when
    # the entry signal reverses" without inventing a parametric reversal type.
    | FeatureThresholdCondition | FeaturePercentileCondition,
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


# ---------------------------------------------------------------------------
# Unified Exit Rules
#
# Every exit type — hard stops, take-profits, trailing stops, time stops, and
# feature-driven thesis exits — is a member of one ExitRule discriminated
# union. Strategies put rules into ExitConfig.guards (always fire on their
# own) or ExitConfig.rules (combined via ExitConfig.logic). Position-state
# rules (drawdown_from_entry, gain_from_entry, trailing_from_peak, time_max_
# days, atr_*, realized_vol_*) are evaluated every bar against the open
# position. Event-driven rules (feature_threshold, feature_percentile) are
# precomputed across (symbol, date) and looked up.
#
# Lookahead semantics: position-state rules read the bar's close (known by
# EOD); atr_/realized_vol_ rules use OHLC strictly before entry; feature-
# driven rules use the same lookahead-clean factor pipeline as entries.
# ---------------------------------------------------------------------------

class DrawdownFromEntryExit(BaseModel):
    """Per-position state. Fires when pnl_pct ≤ value (negative)."""
    type: Literal["drawdown_from_entry"] = "drawdown_from_entry"
    value: float = Field(le=0, description="Negative %. e.g. -10 = exit at 10% loss from entry.")
    cooldown_days: int = Field(ge=0, default=90, description="Days before re-entering same ticker after fire.")


class GainFromEntryExit(BaseModel):
    """Per-position state. Fires when pnl_pct ≥ value (positive)."""
    type: Literal["gain_from_entry"] = "gain_from_entry"
    value: float = Field(gt=0, description="Positive %. e.g. 30 = exit at 30% gain from entry.")


class TrailingFromPeakExit(BaseModel):
    """Per-position state. Fires when current price has retraced N% from running peak.

    Distinct from drawdown_from_entry (compares to entry price): this fires
    even when the position is still in profit, capturing gains given back.
    """
    type: Literal["trailing_from_peak"] = "trailing_from_peak"
    value: float = Field(lt=0, description="Negative %. e.g. -8 = exit if current is 8% below peak price since entry.")
    cooldown_days: int = Field(ge=0, default=0)


class TimeMaxDaysExit(BaseModel):
    """Per-position state. Fires when calendar days held since entry ≥ value."""
    type: Literal["time_max_days"] = "time_max_days"
    value: int = Field(ge=1, description="Calendar-day cap on holding period.")


class AtrStopExit(BaseModel):
    """Per-position, frozen at entry. stop_price = entry − k × ATR(window). Fires on price ≤ stop."""
    type: Literal["atr_stop"] = "atr_stop"
    k: float = Field(gt=0, le=10)
    window_days: int = Field(ge=10, le=252)
    cooldown_days: int = Field(ge=0, default=90)


class AtrTargetExit(BaseModel):
    """Per-position, frozen at entry. tp_price = entry + k × ATR(window). Fires on price ≥ tp."""
    type: Literal["atr_target"] = "atr_target"
    k: float = Field(gt=0, le=10)
    window_days: int = Field(ge=10, le=252)


class RealizedVolStopExit(BaseModel):
    """Per-position, frozen at entry. stop_price = entry × (1 − k × sigma_daily). Fires on price ≤ stop."""
    type: Literal["realized_vol_stop"] = "realized_vol_stop"
    k: float = Field(gt=0, le=10)
    window_days: int = Field(ge=10, le=252)
    sigma_source: Literal["historical", "ewma"] = "historical"
    cooldown_days: int = Field(ge=0, default=90)


class RealizedVolTargetExit(BaseModel):
    """Per-position, frozen at entry. tp_price = entry × (1 + k × sigma_daily). Fires on price ≥ tp."""
    type: Literal["realized_vol_target"] = "realized_vol_target"
    k: float = Field(gt=0, le=10)
    window_days: int = Field(ge=10, le=252)
    sigma_source: Literal["historical", "ewma"] = "historical"


# ExitRule reuses FeatureThresholdCondition and FeaturePercentileCondition
# from the entry side — the same generic factor-driven rule shape works as a
# (symbol, date)-keyed exit signal. The factor catalog is registry-driven, so
# any of the registered factors (see server.factors) can be referenced.
ExitRule = Annotated[
    DrawdownFromEntryExit | GainFromEntryExit | TrailingFromPeakExit
    | TimeMaxDaysExit
    | AtrStopExit | AtrTargetExit
    | RealizedVolStopExit | RealizedVolTargetExit
    | FeatureThresholdCondition | FeaturePercentileCondition
    # Legacy fundamental-deterioration exit types — kept in the union so
    # stored configs that used them via exit_conditions migrate cleanly.
    # The engine routes them through _precompute_exit_signals_per_rule.
    | RevenueDecelerationExit | MarginCollapseExit,
    Field(discriminator="type"),
]


class ExitConfig(BaseModel):
    """Unified exit configuration. Replaces stop_loss + take_profit + time_stop
    + exit_conditions + exit_logic with one structured field.

    Two semantic tiers:
      - guards: any guard firing immediately closes the position, regardless
        of `logic`. Hard stops, take-profits at levels, time stops, any
        unconditional rule.
      - rules:  combined via `logic` ('any' = OR; 'all' = AND). Use for
        thesis exits (signal reversals, fundamental deterioration, compound
        conditions).

    A position exits when (any guard fires) OR (the rules combine to true).
    """
    guards: list[ExitRule] = Field(default_factory=list)
    rules: list[ExitRule] = Field(default_factory=list)
    logic: Literal["any", "all"] = Field(default="any")


def migrate_legacy_exits_to_unified(data: dict) -> dict:
    """Translate legacy exit fields into ExitConfig.

    Idempotent: if `exit` is already structured (with guards/rules), returns
    unchanged. Otherwise: fold stop_loss / take_profit / time_stop /
    exit_conditions / exit_logic into exit.guards / exit.rules / exit.logic
    and strip the legacy fields so downstream callers see only the unified shape.

    Type translations:
      stop_loss.drawdown_from_entry  → drawdown_from_entry (guard)
      stop_loss.atr_multiple         → atr_stop (guard)
      stop_loss.realized_vol_multiple → realized_vol_stop (guard)
      take_profit.gain_from_entry    → gain_from_entry (guard)
      take_profit.above_peak         → trailing_from_peak (guard, sign flipped to negative)
      take_profit.atr_multiple       → atr_target (guard)
      take_profit.realized_vol_multiple → realized_vol_target (guard)
      time_stop.max_days             → time_max_days (guard)
      exit_conditions[*]             → rules (unchanged shape)
      exit_logic                     → exit.logic

    Called both as a Pydantic model_validator and directly by the engine's
    run_backtest entry point, so engine callers that pass legacy-shaped dicts
    without going through model_validate also get migrated.
    """
    if not isinstance(data, dict):
        return data
    existing_exit = data.get("exit")
    if isinstance(existing_exit, dict) and (
        "guards" in existing_exit or "rules" in existing_exit
    ):
        return data

    guards: list = []
    rules: list = []

    sl = data.pop("stop_loss", None)
    if sl:
        t = sl.get("type")
        base = {k: v for k, v in sl.items() if k != "type"}
        if t == "drawdown_from_entry":
            guards.append({"type": "drawdown_from_entry", **base})
        elif t == "atr_multiple":
            guards.append({"type": "atr_stop", **base})
        elif t == "realized_vol_multiple":
            guards.append({"type": "realized_vol_stop", **base})

    tp = data.pop("take_profit", None)
    if tp:
        t = tp.get("type")
        base = {k: v for k, v in tp.items() if k != "type"}
        if t == "gain_from_entry":
            guards.append({"type": "gain_from_entry", **base})
        elif t == "above_peak":
            # above_peak's intent was trailing-from-peak with a positive value
            # meaning "exit on N% retracement from peak". Translate to
            # trailing_from_peak with -abs(value).
            v = base.pop("value", 60)
            guards.append({"type": "trailing_from_peak", "value": -abs(v), **base})
        elif t == "atr_multiple":
            guards.append({"type": "atr_target", **base})
        elif t == "realized_vol_multiple":
            guards.append({"type": "realized_vol_target", **base})

    ts = data.pop("time_stop", None)
    if ts:
        mx = ts.get("max_days") or ts.get("days")
        if mx:
            guards.append({"type": "time_max_days", "value": int(mx)})

    ec = data.pop("exit_conditions", None) or []
    for cond in ec:
        if isinstance(cond, dict):
            rules.append(cond)

    legacy_logic = data.pop("exit_logic", "any")

    if guards or rules:
        data["exit"] = {
            "guards": guards,
            "rules": rules,
            "logic": legacy_logic,
        }
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
    # Unified exit configuration. New strategies should use this exclusively.
    # The legacy fields below (stop_loss, take_profit, time_stop, exit_conditions,
    # exit_logic) are translated into `exit` via a load-time validator for
    # back-compat with stored configs; do not author new strategies using them.
    exit: ExitConfig = Field(default_factory=ExitConfig)
    stop_loss: StopLossConfig | None = Field(default=None,
        description="DEPRECATED — use exit.guards with drawdown_from_entry / atr_stop / realized_vol_stop instead.")
    take_profit: TakeProfitConfig | None = Field(default=None,
        description="DEPRECATED — use exit.guards with gain_from_entry / atr_target / realized_vol_target / trailing_from_peak instead.")
    time_stop: TimeStopConfig | None = Field(default=None,
        description="DEPRECATED — use exit.guards with time_max_days instead.")
    exit_conditions: list[ExitCondition] | None = Field(default=None,
        description="DEPRECATED — use exit.rules instead.")
    exit_logic: Literal["any", "all"] = Field(default="any",
        description="DEPRECATED — use exit.logic instead.")
    ranking: RankingConfig | None = None
    rebalancing: RebalancingConfig = Field(default_factory=RebalancingConfig)
    sizing: SizingConfig = Field(default_factory=SizingConfig)
    backtest: BacktestParams = Field(default_factory=BacktestParams)
    created_at: str | None = None
    updated_at: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_exits_to_unified_validator(cls, data):
        return migrate_legacy_exits_to_unified(data)
