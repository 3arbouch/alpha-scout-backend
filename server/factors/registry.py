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
    """One feature's identity, math, and execution metadata.

    Two compute interfaces — exactly one is set per feature, determined by
    `materialization`:

    - precomputed → `compute(ctx)` returns the scalar value for one
      (symbol, date). The daily update job calls this once per (symbol, date)
      and writes the result to features_daily.

    - on_the_fly → `compute_series(symbol, prices)` returns a {date: value}
      dict over the symbol's full price history. The engine calls this once
      per symbol per backtest. Streaming algorithms (Wilder RSI, momentum)
      are O(1) amortized per date this way; per-date compute would be O(n).

    `prices` for compute_series is a list of (date, close) pairs in
    ascending date order — the symbol's full available history.
    """
    name: str
    deps: tuple[str, ...]
    materialization: Materialization
    category: Category
    unit: str
    description: str
    is_factor: bool = True
    compute: Callable | None = None
    compute_series: Callable | None = None


_REGISTRY: dict[str, FeatureDef] = {}


def register_feature(
    *,
    name: str,
    deps: tuple[str, ...] | list[str],
    materialization: Materialization,
    category: Category,
    unit: str,
    description: str,
    is_factor: bool = True,
    compute: Callable | None = None,
    compute_series: Callable | None = None,
) -> FeatureDef:
    """Register a feature. Called at import time from library modules.

    Validates that the supplied compute interface matches the materialization:
    precomputed requires `compute`, on_the_fly requires `compute_series`.
    """
    if name in _REGISTRY:
        raise ValueError(f"feature '{name}' already registered")
    if materialization == "precomputed" and compute is None:
        raise ValueError(f"feature '{name}': precomputed requires compute=")
    if materialization == "on_the_fly" and compute_series is None:
        raise ValueError(f"feature '{name}': on_the_fly requires compute_series=")
    fd = FeatureDef(
        name=name,
        compute=compute,
        compute_series=compute_series,
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
