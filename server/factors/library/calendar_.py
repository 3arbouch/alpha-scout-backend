"""Earnings-calendar features.

These are point-in-time: at date T, the next earnings date is the earliest
scheduled date strictly after T. The earnings table carries past actuals AND
scheduled future dates, so this is lookahead-free — companies announce dates
weeks in advance.

Distances are reported in calendar days (not trading days) for simplicity.
"""
from __future__ import annotations

from ..context import ComputeContext
from ..registry import register_feature


def _days_to_next_earnings(ctx: ComputeContext) -> int | None:
    return ctx.days_to_next_earnings


def _days_since_last_earnings(ctx: ComputeContext) -> int | None:
    return ctx.days_since_last_earnings


def _pre_earnings_window_5d(ctx: ComputeContext) -> int | None:
    """1 if next earnings ≤ 5 calendar days away, else 0. None if no scheduled earnings."""
    d = ctx.days_to_next_earnings
    if d is None:
        return None
    return 1 if d <= 5 else 0


register_feature(
    name="days_to_next_earnings", compute=_days_to_next_earnings,
    deps=("earnings.date",),
    materialization="precomputed", category="calendar", unit="days",
    description="Calendar days from today to next scheduled earnings date.",
    is_factor=False,
)
register_feature(
    name="days_since_last_earnings", compute=_days_since_last_earnings,
    deps=("earnings.date",),
    materialization="precomputed", category="calendar", unit="days",
    description="Calendar days from most recent earnings event to today.",
    is_factor=False,
)
register_feature(
    name="pre_earnings_window_5d", compute=_pre_earnings_window_5d,
    deps=("earnings.date",),
    materialization="precomputed", category="calendar", unit="count",
    description="1 if next earnings is within 5 calendar days, else 0.",
    is_factor=False,
)
