"""Feature registry.

A FeatureDef is the single declaration of one feature: its name, its compute
function, its declared dependencies, its materialization mode, and metadata
the agent's catalog block reads.

Adding a feature = writing one @register_feature in server/factors/library/
and (if precomputed) running the daily update job to backfill the new column.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Literal

Materialization = Literal["precomputed", "on_the_fly"]
Category = Literal["value", "yield", "growth", "quality", "momentum",
                    "volatility", "volume", "calendar", "sentiment", "macro"]


@dataclass(frozen=True)
class FeatureDef:
    name: str
    compute: Callable
    deps: tuple[str, ...]
    materialization: Materialization
    category: Category
    unit: str
    description: str
    is_factor: bool = True


_REGISTRY: dict[str, FeatureDef] = {}


def register_feature(
    *,
    name: str,
    compute: Callable,
    deps: tuple[str, ...] | list[str],
    materialization: Materialization,
    category: Category,
    unit: str,
    description: str,
    is_factor: bool = True,
) -> FeatureDef:
    """Register a feature. Called at import time from library modules."""
    if name in _REGISTRY:
        raise ValueError(f"feature '{name}' already registered")
    fd = FeatureDef(
        name=name,
        compute=compute,
        deps=tuple(deps),
        materialization=materialization,
        category=category,
        unit=unit,
        description=description,
        is_factor=is_factor,
    )
    _REGISTRY[name] = fd
    return fd


def get(name: str) -> FeatureDef:
    if name not in _REGISTRY:
        raise KeyError(f"unknown feature '{name}' — registered: {sorted(_REGISTRY)}")
    return _REGISTRY[name]


def feature_names() -> tuple[str, ...]:
    """Names of every registered feature, sorted for stability."""
    return tuple(sorted(_REGISTRY))


def all_features() -> tuple[FeatureDef, ...]:
    return tuple(_REGISTRY[n] for n in feature_names())


def materialized_features() -> tuple[FeatureDef, ...]:
    return tuple(f for f in all_features() if f.materialization == "precomputed")


def on_the_fly_features() -> tuple[FeatureDef, ...]:
    return tuple(f for f in all_features() if f.materialization == "on_the_fly")
