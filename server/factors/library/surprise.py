"""Earnings-surprise factor: SUE (standardized unexpected earnings).

sue — the classic post-earnings-announcement-drift (PEAD) signal: the latest
  EPS surprise (actual − consensus estimate), standardized by the dispersion of
  recent surprises. A bigger beat *relative to how surprising this name usually
  is* predicts continued positive drift, so HIGHER is better.

  SUE = (eps_actual − eps_estimated)_latest / stdev(trailing surprises)

Standardization choice: divide by the SAMPLE stdev (ddof=1) of the trailing
window of surprises (default last 8 announced quarters, including the latest),
the analyst-based SUE convention — not price-scaling. Requires >= min_obs
surprises and a non-degenerate (>0) stdev, else None. All point-in-time: only
earnings announced on or before the as-of date are used.
"""
from __future__ import annotations

import statistics

from ..context import ComputeContext
from ..registry import register_feature


def _sue(surprises: list[float], min_obs: int = 4) -> float | None:
    """Latest surprise / sample stdev of the trailing surprise window.

    SUE is a z-score; when trailing surprises are near-identical the stdev
    collapses toward zero and the ratio explodes. Clip to a sane z-range so a
    degenerate-σ row can't blow up a z-standardized cross-section (a |z| beyond
    ~20 carries no extra information).
    """
    if not surprises or len(surprises) < min_obs:
        return None
    sd = statistics.stdev(surprises)          # sample stdev (ddof=1)
    if sd <= 0:
        return None
    v = surprises[-1] / sd
    return max(-20.0, min(20.0, v))


def _earnings_surprise(ctx: ComputeContext) -> float | None:
    return _sue(ctx.surprises_asof(max_n=8))


register_feature(
    name="sue", compute=_earnings_surprise,
    deps=("earnings.eps_actual", "earnings.eps_estimated"),
    materialization="precomputed", category="sentiment", unit="ratio",
    description="Standardized unexpected earnings: latest (eps_actual − eps_estimated) / "
                "stdev of trailing 8 surprises (PEAD). Higher = stronger positive surprise.",
)
