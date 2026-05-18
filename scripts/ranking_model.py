"""Presentation-ready ranking-model schema for frontend consumption.

Frontend wants a single, stable structure that describes how a sleeve scores
tickers — formula, normalized bucket weights, factor labels, sign conventions
— rendered as a "Scoring Model" card. The raw ``strategy_config.ranking`` +
``composite_score`` blocks are the *machine-readable* truth; this module
projects them into a *frontend-readable* shape so client code never has to
normalize weights, resolve factor labels, or assemble the formula string.

Public entry point: :func:`build_ranking_model(strategy_config)`.

Returned shape (composite_score case)::

    {
      "type":            "composite_score",
      "standardization": "rank",   # or "zscore"
      "top_n":           15,
      "order":           "desc",
      "formula":         "score(s) = Σ_b (w_b / Σ_w) · mean_{f ∈ b}(sign_f · z(f, s, t))",
      "buckets": [
        {
          "name": "quality", "label": "Quality",
          "weight": 1.0, "weight_normalized": 0.40,
          "factors": [
            { "name": "op_margin", "label": "Operating Margin",
              "sign": "+", "category": "quality",
              "description": "EBIT / revenue. Positive = healthy operating profitability." },
            ...
          ]
        }, ...
      ]
    }

Non-composite ranking (e.g. ``ranking.by == "momentum_rank"``) returns the
slim shape ``{"type": "momentum_rank", "top_n": 15, "order": "desc"}``.

The output is **fully reproducible from inputs**. No DB queries, no run-time
state. Safe to call from any context that has the strategy_config dict.
"""
from __future__ import annotations

from typing import Any

# Factor registry — best-effort import. When unavailable (e.g. unit tests that
# stub out the server.* tree), we fall back to a humanized name and skip the
# `category` / `description` enrichment. The frontend gets the same shape,
# just with less metadata per factor.
try:
    from server.factors import get as _get_feature  # type: ignore
except ImportError:    # pragma: no cover — only fires in isolated test envs
    _get_feature = None  # type: ignore


# Short overrides for bucket / factor display labels. Long descriptions come
# from the factor registry's `description` field, which is authoritative for
# the formula tooltip but too verbose for the card title.
_HUMANIZE_OVERRIDES = {
    # acronyms / domain shorthand
    "ev": "EV", "roe": "ROE", "roic": "ROIC", "roa": "ROA", "fcf": "FCF",
    "eps": "EPS", "rev": "Revenue", "ebit": "EBIT", "ebitda": "EBITDA",
    "pe": "P/E", "yoy": "YoY", "mom": "MoM", "qoq": "QoQ", "ttm": "TTM",
    "rsi": "RSI", "atr": "ATR", "vol": "Vol", "div": "Dividend",
    "20d": "20-Day", "60d": "60-Day", "252d": "252-Day", "30d": "30-Day",
    "90d": "90-Day", "12m": "12-Month", "6m": "6-Month", "1m": "1-Month",
    "12_1m": "12-Month (ex 1-Month)",
    # stay-lowercase / short connector words
    "to": "to", "vs": "vs", "of": "of", "and": "and", "the": "the",
    "accel": "Acceleration", "alltime": "All-Time",
}


def _humanize_token(tok: str) -> str:
    lo = tok.lower()
    if lo in _HUMANIZE_OVERRIDES:
        return _HUMANIZE_OVERRIDES[lo]
    return tok.capitalize()


def _humanize_snake(s: str) -> str:
    """``op_margin`` → ``Op Margin``; ``ret_12_1m`` → ``Return 12-Month (ex 1-Month)``."""
    if not s:
        return s
    # Treat 12_1m as a single conceptual token by detecting numeric_numeric_unit.
    # Otherwise just split on _ and humanize each piece.
    return " ".join(_humanize_token(t) for t in s.split("_") if t)


# Specific factor-name → display-label overrides for the common cases where the
# registry's `description` is too long and the humanized name is too cryptic.
# Frontend can still render `description` as tooltip text.
_FACTOR_LABEL_OVERRIDES = {
    "op_margin":        "Operating Margin",
    "roic":             "Return on Invested Capital",
    "roe":              "Return on Equity",
    "roa":              "Return on Assets",
    "fcf_yield":        "Free Cash Flow Yield",
    "div_yield":        "Dividend Yield",
    "ev_ebitda":        "EV / EBITDA",
    "ev_sales":         "EV / Sales",
    "ev_ebit":          "EV / EBIT",
    "debt_to_equity":   "Debt / Equity",
    "ret_12_1m":        "12-Month Return (ex Recent Month)",
    "ret_6m":           "6-Month Return",
    "ret_3m":           "3-Month Return",
    "ret_1m":           "1-Month Return",
    "rev_yoy":          "Revenue YoY Growth",
    "rev_yoy_accel":    "Revenue YoY Acceleration",
    "eps_yoy":          "EPS YoY Growth",
    "eps_yoy_accel":    "EPS YoY Acceleration",
    "drawdown_252d":    "52-Week Drawdown",
    "drawdown_60d":     "60-Day Drawdown",
    "drawdown_alltime": "All-Time Drawdown",
    "dollar_vol_20":    "20-Day Dollar Volume",
    "days_since_last_earnings": "Days Since Last Earnings",
    "days_to_next_earnings":    "Days to Next Earnings",
    "analyst_net_upgrades_30d": "Net Analyst Upgrades (30d)",
    "analyst_net_upgrades_90d": "Net Analyst Upgrades (90d)",
}


def factor_label(name: str) -> str:
    """Short display label for a factor (card title-friendly)."""
    if name in _FACTOR_LABEL_OVERRIDES:
        return _FACTOR_LABEL_OVERRIDES[name]
    return _humanize_snake(name)


def bucket_label(name: str) -> str:
    """Short display label for a bucket (e.g. ``quality`` → ``Quality``)."""
    return _humanize_snake(name)


def _factor_meta(name: str) -> dict[str, str | None]:
    """Pull category + description from the registry when available, else None."""
    if _get_feature is None:
        return {"category": None, "description": None}
    try:
        fd = _get_feature(name)
        return {"category": fd.category, "description": fd.description}
    except KeyError:
        return {"category": None, "description": None}


# Formula string is the same shape for both standardization modes; the only
# thing that changes is the *meaning* of ``z(f, s, t)`` (which the frontend
# can spell out from `standardization` if it wants).
_COMPOSITE_FORMULA = (
    "score(s) = Σ_b (w_b / Σ_w) · mean_{f ∈ b}(sign_f · z(f, s, t))"
)


def _composite_ranking_model(composite_cfg: dict, ranking_cfg: dict) -> dict:
    buckets_raw = composite_cfg.get("buckets") or {}
    raw_weights = {b: float(bdef.get("weight", 1.0) or 0.0)
                   for b, bdef in buckets_raw.items() if isinstance(bdef, dict)}
    total_w = sum(w for w in raw_weights.values() if w > 0) or 1.0

    out_buckets: list[dict] = []
    for bname, bdef in buckets_raw.items():
        if not isinstance(bdef, dict):
            continue
        bw = float(bdef.get("weight", 1.0) or 0.0)
        factors_out: list[dict] = []
        for f in bdef.get("factors") or []:
            if isinstance(f, dict):
                fname = f.get("name")
                fsign = f.get("sign", "+")
            else:
                # Tolerate Pydantic model instances by attribute access.
                fname = getattr(f, "name", None)
                fsign = getattr(f, "sign", "+")
            if not fname:
                continue
            meta = _factor_meta(fname)
            factors_out.append({
                "name":        fname,
                "label":       factor_label(fname),
                "sign":        fsign,
                "category":    meta["category"],
                "description": meta["description"],
            })
        out_buckets.append({
            "name":              bname,
            "label":             bucket_label(bname),
            "weight":            bw,
            "weight_normalized": round(bw / total_w, 6) if total_w else 0.0,
            "factors":           factors_out,
        })

    return {
        "type":            "composite_score",
        "standardization": composite_cfg.get("standardization", "rank"),
        "top_n":           int(ranking_cfg.get("top_n") or 0) or None,
        "order":           ranking_cfg.get("order", "desc"),
        "formula":         _COMPOSITE_FORMULA,
        "buckets":         out_buckets,
    }


def build_ranking_model(strategy_config: dict) -> dict | None:
    """Project a strategy's ranking config into the presentation-ready shape.

    Returns ``None`` if the strategy_config has no ranking block at all (the
    frontend then knows to render nothing). Returns a dict for both
    composite_score and non-composite ranking modes — the ``type`` field
    discriminates."""
    if not isinstance(strategy_config, dict):
        return None
    ranking = strategy_config.get("ranking") or {}
    by = ranking.get("by")
    if not by:
        return None

    if by == "composite_score":
        comp = strategy_config.get("composite_score") or {}
        # If `by` is composite_score but the composite block is missing, still
        # emit a slim model so the frontend can show *something* — the engine
        # would fail at run time, but the deployment payload should be
        # introspection-safe.
        if not comp:
            return {
                "type":  "composite_score",
                "top_n": int(ranking.get("top_n") or 0) or None,
                "order": ranking.get("order", "desc"),
                "buckets": [],
                "formula": _COMPOSITE_FORMULA,
                "standardization": "rank",
            }
        return _composite_ranking_model(comp, ranking)

    # Single-metric ranking — slim shape. ``by`` is the metric the engine
    # sorts on (e.g. ``momentum_rank``, ``signal_order``, ``rsi``).
    meta = _factor_meta(by) if by else {"category": None, "description": None}
    return {
        "type":        by,
        "label":       factor_label(by),
        "category":    meta["category"],
        "description": meta["description"],
        "top_n":       int(ranking.get("top_n") or 0) or None,
        "order":       ranking.get("order", "desc"),
    }
