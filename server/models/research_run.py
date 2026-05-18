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
