"""Single source of truth: what is an experiment's declared eligible universe?

The strategy config in `experiments.portfolio_config` declares the universe
of names the strategy is allowed to pick from. Every analyst tool that needs
a reference universe (exposures, attribution, factor analysis) should read
the universe from HERE so the math agrees across tools.

A portfolio may have multiple sleeves with different universes — we union
them. The result is one of three flavors:

  ('all',      None)                   → broad market (use 'all' precomputed lookups)
  ('Technology', None)                 → single GICS sector (use sector precomputed lookups)
  ('custom',   ['AAPL', 'MSFT', ...])  → custom symbol list (compute on-the-fly)
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any

from auto_trader.schema import get_db


# Match build_factor_returns_daily.SECTOR_UNIVERSES — kept in sync manually.
_GICS_SECTORS = {
    "Technology", "Healthcare", "Financial Services", "Industrials",
    "Consumer Cyclical", "Consumer Defensive", "Energy", "Real Estate",
    "Communication Services", "Utilities", "Basic Materials",
}


def _sleeve_universe(sleeve_cfg: dict[str, Any], market_conn: sqlite3.Connection
                      ) -> tuple[str, list[str] | None]:
    """One sleeve → (kind, symbols).

    kind ∈ {'all', 'sector:<name>', 'symbols'}.
    symbols is a list when kind is 'symbols' or resolvable; None for 'all'.
    """
    sc = sleeve_cfg.get("strategy_config") or {}
    u = sc.get("universe") or {}
    utype = u.get("type")

    if utype == "symbols":
        syms = u.get("symbols") or []
        return "symbols", sorted(set(syms))
    if utype == "sector":
        sec = u.get("sector")
        if sec in _GICS_SECTORS:
            return f"sector:{sec}", None
        return "all", None
    # 'all', 'index', or unrecognized → broad
    return "all", None


def _sector_symbols(market_conn: sqlite3.Connection, sector: str) -> list[str]:
    rows = market_conn.execute(
        "SELECT symbol FROM universe_profiles WHERE sector = ?", (sector,)
    ).fetchall()
    return sorted({r[0] for r in rows})


def resolve_experiment_universe(experiment_id: str,
                                 market_conn: sqlite3.Connection,
                                 ) -> tuple[str, list[str] | None, list[str]]:
    """Return (universe_id, symbols, sleeve_kinds).

    universe_id ∈ {'all', '<SectorName>', 'custom'}.
    symbols     = list of eligible symbols (None when universe_id == 'all'
                  AND no concrete list is required; in practice we return
                  None to signal "use broad features_daily universe").
    sleeve_kinds = the per-sleeve kind strings, for diagnostics.

    Rules:
      - All sleeves declare the SAME single GICS sector → that sector
      - All sleeves declare 'all' → 'all'
      - Anything else (mixed sectors, symbols lists, mixed kinds) → 'custom'
        with the unioned symbol set
    """
    app = get_db()
    row = app.execute(
        "SELECT portfolio_config FROM experiments WHERE id = ?", (experiment_id,)
    ).fetchone()
    app.close()
    if not row or not row["portfolio_config"]:
        return "all", None, []

    try:
        cfg = json.loads(row["portfolio_config"])
    except (TypeError, json.JSONDecodeError):
        return "all", None, []

    sleeves = cfg.get("sleeves") or cfg.get("strategies") or []
    if not sleeves:
        # Top-level strategy_config? Treat as a single-sleeve wrapper.
        if cfg.get("universe"):
            sleeves = [{"strategy_config": cfg}]
        else:
            return "all", None, []

    kinds: list[str] = []
    per_sleeve_symbols: list[list[str] | None] = []
    for s in sleeves:
        kind, syms = _sleeve_universe(s, market_conn)
        kinds.append(kind)
        per_sleeve_symbols.append(syms)

    # Case 1: all sleeves the same single sector
    distinct_kinds = set(kinds)
    if len(distinct_kinds) == 1:
        only = next(iter(distinct_kinds))
        if only == "all":
            return "all", None, kinds
        if only.startswith("sector:"):
            sec = only.split(":", 1)[1]
            return sec, _sector_symbols(market_conn, sec), kinds

    # Case 2: anything else → union all symbols. For sector sleeves, we expand
    # to their sector's full symbol list; 'all' contributes nothing concrete.
    union: set[str] = set()
    has_all = False
    for kind, syms in zip(kinds, per_sleeve_symbols):
        if kind == "all":
            has_all = True
            continue
        if kind.startswith("sector:"):
            sec = kind.split(":", 1)[1]
            union.update(_sector_symbols(market_conn, sec))
            continue
        if kind == "symbols" and syms:
            union.update(syms)
    if has_all and not union:
        return "all", None, kinds
    if not union:
        return "all", None, kinds
    return "custom", sorted(union), kinds
