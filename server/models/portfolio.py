"""
Portfolio configuration models.

A portfolio is a collection of strategy "sleeves", each with a capital weight
and optional regime gates. Supports dynamic allocation profiles that shift
weights based on active regimes.

Used by: portfolio engine, deploy engine, API.
"""

from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, Field, model_validator

from .strategy import StrategyConfig, BacktestParams
from .regime import RegimeCondition


# ---------------------------------------------------------------------------
# Sleeve: one strategy slot inside a portfolio
# ---------------------------------------------------------------------------

class SleeveConfig(BaseModel):
    """A single strategy within a portfolio.

    Exactly one of strategy_id, config_path, or strategy_config must be set.
    """
    label: str = Field(description="Display name for this sleeve.")
    weight: float = Field(ge=0, le=1, description="Capital allocation weight (0-1). All sleeve weights must sum to 1.0.")

    # Strategy reference — one of these three:
    strategy_id: str | None = Field(default=None, description="Reference to saved strategy by ID.")
    config_path: str | None = Field(default=None, description="Path to strategy config JSON file.")
    strategy_config: StrategyConfig | None = Field(default=None, description="Inline strategy config.")

    # Regime gating
    regime_gate: list[str] = Field(
        default_factory=list,
        description="List of regime_ids. Empty or ['*'] = always active. Non-empty = active only when at least one gated regime is on.",
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_fields(cls, data):
        """Accept legacy field names from old API clients / stored data."""
        if isinstance(data, dict):
            # "config" → "strategy_config"
            if "config" in data and "strategy_config" not in data:
                data["strategy_config"] = data.pop("config")
            elif "config" in data:
                data.pop("config")
            # "name" → "label" (old format used "name" for sleeve label)
            if "name" in data and "label" not in data:
                data["label"] = data.pop("name")
        return data


# ---------------------------------------------------------------------------
# Allocation Profiles: dynamic weight shifts based on regimes
# ---------------------------------------------------------------------------

class AllocationProfile(BaseModel):
    """Named weight set for dynamic allocation.

    When this profile's trigger regimes are ALL active, its weights override
    the default sleeve weights.
    """
    trigger: list[str] = Field(
        default_factory=list,
        description="Regime IDs that must ALL be active for this profile to engage. Empty = default profile.",
    )
    weights: dict[str, float] = Field(
        description="Sleeve label -> weight mapping. Include 'Cash' key for unallocated.",
    )


# ---------------------------------------------------------------------------
# Inline Regime Definitions
# ---------------------------------------------------------------------------

class InlineRegimeDefinition(BaseModel):
    """Regime defined inline within a portfolio config (instead of referencing a saved regime)."""
    conditions: list[RegimeCondition] = Field(min_length=1)
    logic: Literal["all", "any"] = Field(default="all")
    # Hysteresis: filter short-lived condition flips at the source by requiring
    # K consecutive days of confirming evidence before activation/deactivation.
    # Defaults: 3 days each — long enough to filter most 1-2 day spikes, short
    # enough not to miss confirmed regime changes. Symmetric exit by default;
    # deviate when you have a specific reason.
    entry_persistence_days: int = Field(
        default=3, ge=1,
        description="Consecutive days entry conditions must hold before the regime activates. Default 3.",
    )
    exit_persistence_days: int = Field(
        default=3, ge=1,
        description="Consecutive days exit conditions must hold before the regime deactivates. Default 3.",
    )


# ---------------------------------------------------------------------------
# Full Portfolio Config
# ---------------------------------------------------------------------------

class PortfolioConfig(BaseModel):
    """Complete portfolio definition.

    Smoothing + rebalance defaults (regime persistence, asymmetric transitions,
    drift threshold) are applied uniformly to every config by the engine.
    Explicit fields on the config always win over the defaults. There is no
    versioning — the latest engine behavior applies to every backtest.
    """
    portfolio_id: str | None = Field(default=None, description="Deterministic hash of core params.")
    name: str = Field(min_length=1)
    sleeves: list[SleeveConfig] = Field(min_length=1)

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_fields(cls, data):
        """Accept legacy field names from old API clients / stored data."""
        if isinstance(data, dict):
            # "strategies" → "sleeves"
            if "strategies" in data and "sleeves" not in data:
                data["sleeves"] = data.pop("strategies")
            elif "strategies" in data:
                data.pop("strategies")
            # "capital_flow" → "capital_when_gated_off"
            if "capital_flow" in data and "capital_when_gated_off" not in data:
                data["capital_when_gated_off"] = data.pop("capital_flow")
            elif "capital_flow" in data:
                data.pop("capital_flow")
        return data

    # Regime gating
    regime_filter: bool = Field(
        default=True,
        description="Enable/disable regime gating. When False, all sleeves are always active.",
    )

    # Regime definitions (inline, keyed by regime_id)
    regime_definitions: dict[str, InlineRegimeDefinition] | None = Field(
        default=None,
        description="Inline regime configs keyed by regime_id.",
    )

    # Capital flow when a sleeve is gated off
    capital_when_gated_off: Literal["to_cash", "redistribute"] = Field(
        default="to_cash",
        description="'to_cash' = park as cash. 'redistribute' = allocate to active sleeves.",
    )

    # Dynamic allocation
    allocation_profiles: dict[str, AllocationProfile] | None = Field(
        default=None,
        description="Named weight sets for dynamic allocation. First profile whose triggers are met wins.",
    )
    profile_priority: list[str] | None = Field(
        default=None,
        description="Ordered list of profile names. Walk top-down, first match wins. Must end with 'default'.",
    )
    transition_days: int = Field(
        default=1, ge=1,
        description="Legacy symmetric lerp duration between allocation profiles. Used for both directions only when both asymmetric fields below are unset (None).",
    )
    transition_days_to_defensive: int | None = Field(
        default=1, ge=1,
        description="Trading days to lerp toward a profile with LOWER equity weight (more cash). Default 1 — fast escape on confirmed risk. Direction is determined by comparing total non-Cash weight before vs after.",
    )
    transition_days_to_offensive: int | None = Field(
        default=3, ge=1,
        description="Trading days to lerp toward a profile with HIGHER equity weight (less cash). Default 3 — patient redeployment.",
    )
    rebalance_threshold: float = Field(
        default=0.05, ge=0.0, le=1.0,
        description="Drift tolerance for portfolio-level rebalancing. When the actual sleeve weight differs from the allocation_profile target by more than this fraction (e.g., 0.05 = 5%), the engine emits rebalance trades to bring the portfolio back to target. When set to 0, the portfolio rebalances continuously every day (institutional 'leveraged-ETF' pattern). Default 0.05 = 5% drift tolerance, the institutional balanced-portfolio standard. Regime-driven profile changes and lerp days bypass this threshold (the contract itself is changing, not just drift).",
    )

    backtest: BacktestParams = Field(default_factory=BacktestParams)
    created_at: str | None = None
    updated_at: str | None = None
