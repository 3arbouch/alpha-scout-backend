"""
Research-run config — the agent's optimization-loop configuration.

A ResearchRunConfig wraps a BacktestConfig (which is opinion-free) and adds the
agent's value judgement: which scalar to climb (target_metric) and any
hard constraints (conditions). Only meaningful when an agent loop is involved.

A one-off backtest from a UI button uses BacktestConfig directly and does not
construct one of these. Deployments do not embed a target_metric either.

Used by: API run-create endpoint (input), auto_trader runner (driving the loop).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from .backtest import BacktestConfig


# Aggregator names supported on TargetMetric.aggregator. 'overall' is the
# default and means "read the scalar straight off the training-period
# backtest result". Anything else reads from the eval-window aggregated dict.
#
# Direction note:
#   preserve metric direction: overall, mean, median, min, max, p10, p25
#   minimized (consistency):   stdev, iqr, range
#   maximized (signal/noise):  snr
Aggregator = Literal[
    "overall",
    "mean", "median", "min", "max", "p10", "p25",
    "stdev", "iqr", "range", "snr",
]


# Single source of truth for what aggregators exist, how to display them, and
# how they behave. Exposed via GET /auto-trader/aggregators so the frontend
# doesn't have to hardcode this list. Keep in sync with `Aggregator` above
# and `AGGREGATOR_DIRECTION` in auto_trader/runner.py — there's a consistency
# test (tests/test_aggregator_catalog_unit.py) that fails if they drift.
AGGREGATOR_CATALOG: list[dict] = [
    {
        "id": "overall",
        "label": "Overall (training period)",
        "group": "no_aggregation",
        "direction": "preserve_metric",
        "requires_eval": False,
        "recommended": False,
        "description": (
            "Use the training-period scalar. Today's behavior — eval is "
            "shown as supporting evidence but not optimized against."
        ),
    },
    {
        "id": "mean",
        "label": "Mean",
        "group": "central_tendency",
        "direction": "preserve_metric",
        "requires_eval": True,
        "recommended": False,
        "description": (
            "Average across eval windows. Better at large N (10+); median "
            "is generally more robust at smaller N."
        ),
    },
    {
        "id": "median",
        "label": "Median",
        "group": "central_tendency",
        "direction": "preserve_metric",
        "requires_eval": True,
        "recommended": True,
        "description": (
            "Median across eval windows. Robust to one outlier window — "
            "the best default for walk-forward optimization."
        ),
    },
    {
        "id": "min",
        "label": "Min (worst window)",
        "group": "tail",
        "direction": "preserve_metric",
        "requires_eval": True,
        "recommended": False,
        "description": (
            "Worst window dominates. Most conservative target — agent "
            "hunts strategies that work everywhere, sometimes at the cost "
            "of being mediocre everywhere."
        ),
    },
    {
        "id": "p10",
        "label": "10th percentile",
        "group": "tail",
        "direction": "preserve_metric",
        "requires_eval": True,
        "recommended": False,
        "description": (
            "Smoothed worst-case. Less sensitive than min to a single "
            "anomalous window."
        ),
    },
    {
        "id": "p25",
        "label": "25th percentile",
        "group": "tail",
        "direction": "preserve_metric",
        "requires_eval": True,
        "recommended": False,
        "description": (
            "Coarser smoothed worst-case. Less aggressive than p10 about "
            "penalizing the tail."
        ),
    },
    {
        "id": "max",
        "label": "Max (best window)",
        "group": "tail",
        "direction": "preserve_metric",
        "requires_eval": True,
        "recommended": False,
        "description": (
            "Cherry-picks the best window. Almost never the right target "
            "— exposes strategies to regime-specific overfitting."
        ),
    },
    {
        "id": "stdev",
        "label": "Std deviation",
        "group": "dispersion",
        "direction": "minimize",
        "requires_eval": True,
        "recommended": False,
        "description": (
            "Sample standard deviation across eval windows. Agent "
            "minimizes — rewards consistency. Undefined when N < 2."
        ),
    },
    {
        "id": "iqr",
        "label": "Interquartile range",
        "group": "dispersion",
        "direction": "minimize",
        "requires_eval": True,
        "recommended": False,
        "description": (
            "p75 − p25 across eval windows. Robust spread; less sensitive "
            "to outliers than stdev. Agent minimizes."
        ),
    },
    {
        "id": "range",
        "label": "Range",
        "group": "dispersion",
        "direction": "minimize",
        "requires_eval": True,
        "recommended": False,
        "description": (
            "max − min across eval windows. Simplest spread measure. "
            "Agent minimizes."
        ),
    },
    {
        "id": "snr",
        "label": "Signal-to-noise (mean ÷ std)",
        "group": "consistency",
        "direction": "maximize",
        "requires_eval": True,
        "recommended": False,
        "description": (
            "\"Sharpe of Sharpes\" — high mean and low variance combined. "
            "Agent maximizes. Honest caveat: at small N (<10 windows), "
            "SNR estimates are noisy."
        ),
    },
]


class TargetMetric(BaseModel):
    """The single scalar the agent climbs.

    name      — any scalar field in BacktestMetrics (e.g. sharpe_ratio,
                alpha_ann_pct, annualized_volatility_pct, max_drawdown_pct).
    aggregator — how to derive the scalar from a backtest result.
                'overall' (default) reads from the training-period metrics.
                'median'/'mean'/'min'/'max'/'p25' read the per-eval-window
                aggregate. Requires backtest.eval to be set.

    Locked at run creation. Changing the aggregator mid-run would invalidate
    previously persisted target_value scalars; treat ResearchRunConfig as
    immutable per run.
    """
    name: str = Field(description="Metric field name in BacktestMetrics.")
    aggregator: Aggregator = Field(default="overall", description="How to reduce per-window metrics to a single scalar. 'overall' = training-period scalar.")


class ResearchRunConfig(BaseModel):
    """Full description of an agent research run.

    Embeds a BacktestConfig (the opinion-free 'how to run a backtest' part)
    and adds the agent-loop concerns: target metric, constraints, model, and
    iteration budget.
    """
    backtest: BacktestConfig
    target_metric: TargetMetric
    conditions: list[dict] = Field(default_factory=list, description="Hard constraints on metrics (e.g. {'metric': 'max_drawdown_pct', 'operator': '>', 'value': -25}).")
    model: str = Field(default="opus-4-7", description="LLM model id or short alias.")
    max_iterations: int = Field(default=50, ge=1, description="Max agent iterations.")

    @model_validator(mode="after")
    def _check_aggregator_eval(self):
        if self.target_metric.aggregator != "overall" and self.backtest.eval is None:
            raise ValueError(
                f"target_metric.aggregator={self.target_metric.aggregator!r} requires "
                f"backtest.eval to be set; 'overall' is the only aggregator valid "
                f"without an eval block"
            )
        return self
