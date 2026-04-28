"""Factor / feature registry.

Single declaration site for every feature the system computes. The daily
update job, the Pydantic schema, the agent's catalog block, and the backtest
engine all derive their view of features from this module.

See server/factors/registry.py for the registry itself, and
server/factors/library/ for the actual feature definitions.
"""
from .registry import (
    FeatureDef,
    register_feature,
    get,
    feature_names,
    all_features,
    materialized_features,
    on_the_fly_features,
)
# Importing the library modules has the side-effect of registering features.
from .library import valuation, yield_, growth, momentum  # noqa: F401

__all__ = [
    "FeatureDef",
    "register_feature",
    "get",
    "feature_names",
    "all_features",
    "materialized_features",
    "on_the_fly_features",
]
