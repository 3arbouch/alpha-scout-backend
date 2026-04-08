"""
Regime configuration models.

A regime is a named macro condition (e.g. "Oil Shock", "Risk Off") defined
by conditions on macro indicator series with AND/OR logic.

Used by: portfolio engine (regime gating), deploy engine, API.
"""

from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, Field


class RegimeCondition(BaseModel):
    """Single condition on a macro series."""
    series: str = Field(description="Macro series key (e.g. 'vix', 'brent_vs_50dma_pct', 'hy_spread_zscore').")
    operator: Literal[">", ">=", "<", "<=", "==", "!="] = Field(description="Comparison operator.")
    value: float = Field(description="Threshold value.")


class RegimeConfig(BaseModel):
    """Complete regime definition."""
    regime_id: str | None = Field(default=None, description="Unique ID, generated on save.")
    name: str = Field(min_length=1, description="Human-readable name (e.g. 'Oil Shock').")

    # Entry conditions (when does the regime turn ON?)
    entry_conditions: list[RegimeCondition] = Field(min_length=1)
    entry_logic: Literal["all", "any"] = Field(default="all", description="'all' = AND, 'any' = OR.")

    # Exit conditions (when does the regime turn OFF?)
    # If omitted, exit = inverse of entry.
    exit_conditions: list[RegimeCondition] | None = Field(default=None)
    exit_logic: Literal["all", "any"] = Field(default="any")

    min_hold_days: int = Field(default=0, ge=0, description="Minimum trading days before exit conditions are checked.")

    created_at: str | None = None
    updated_at: str | None = None
