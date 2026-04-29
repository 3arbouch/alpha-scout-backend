"""Analyst-flow features.

Net-upgrades counts within a trailing window. Upgrades and downgrades both
come from the analyst_grades table's `action` column (values: 'upgrade',
'downgrade', 'maintain'). Window is [self.date − N + 1, self.date] inclusive.
"""
from __future__ import annotations

from ..context import ComputeContext
from ..registry import register_feature


def _net_upgrades(ctx: ComputeContext, days: int) -> int | None:
    rows = ctx.grades_in_window(days)
    if not rows:
        return 0
    up = sum(1 for _, action in rows if action == "upgrade")
    down = sum(1 for _, action in rows if action == "downgrade")
    return up - down


def _net_upgrades_30d(ctx): return _net_upgrades(ctx, 30)
def _net_upgrades_90d(ctx): return _net_upgrades(ctx, 90)


register_feature(
    name="analyst_net_upgrades_30d", compute=_net_upgrades_30d,
    deps=("analyst_grades.action",),
    materialization="precomputed", category="sentiment", unit="count",
    description="(upgrades − downgrades) in trailing 30 calendar days. "
                "Positive = analysts turning more bullish.",
)
register_feature(
    name="analyst_net_upgrades_90d", compute=_net_upgrades_90d,
    deps=("analyst_grades.action",),
    materialization="precomputed", category="sentiment", unit="count",
    description="(upgrades − downgrades) in trailing 90 calendar days.",
)
