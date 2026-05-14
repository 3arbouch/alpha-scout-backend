#!/usr/bin/env python3
"""
AlphaScout Portfolio Engine
============================
Runs multiple strategies in parallel with shared capital pool and regime gating.

A portfolio is a collection of strategies, each with:
- A capital weight (e.g. 25%)
- A list of regime gates (strategy is only active when at least one gated regime is active)
- Its own universe, entry/exit signals, sizing

The engine:
1. Pre-computes regime series for the backtest window
2. Runs each strategy independently via run_backtest()
3. Applies regime gating: when a strategy's regime gate is OFF, its positions are closed
4. Tracks combined NAV = sum of per-strategy NAVs + unallocated cash

Usage as library:
    from portfolio_engine import run_portfolio_backtest

    result = run_portfolio_backtest(portfolio_config)

Usage as CLI:
    python3 portfolio_engine.py portfolio_config.json
    python3 portfolio_engine.py portfolio_config.json --start 2020-01-01 --end 2026-03-28
"""

import os
import sys
import json
import hashlib
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from copy import deepcopy

sys.path.insert(0, str(Path(__file__).parent))

from backtest_engine import (
    run_backtest, load_strategy, validate_strategy, compute_benchmark,
    stamp_strategy_id, get_connection, build_price_index, resolve_universe,
)
from regime import evaluate_regime_series, evaluate_regime_series_with_stats

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
from db_config import MARKET_DB_PATH as DB_PATH, APP_DB_PATH
WORKSPACE = Path(os.environ.get("WORKSPACE", "/app"))
STRATEGIES_DIR = WORKSPACE / "strategies"


def get_config_schema() -> dict:
    """
    Authoritative portfolio config schema — generated from the Pydantic models.

    Usage:
        python3 -c "from portfolio_engine import get_config_schema; import json; print(json.dumps(get_config_schema(), indent=2))"
    """
    sys.path.insert(0, str(Path(__file__).parent.parent / "server"))
    from models.portfolio import PortfolioConfig
    return PortfolioConfig.model_json_schema()


def _lookup_symbols_sector(symbols: list[str]) -> str | None:
    """Look up the common sector for a list of symbols in universe_profiles.

    Returns the sector name if all symbols map to the same non-empty sector.
    Returns None if they span multiple sectors or any are unknown.
    """
    if not symbols:
        return None
    try:
        conn = sqlite3.connect(str(DB_PATH))
        placeholders = ",".join("?" * len(symbols))
        rows = conn.execute(
            f"SELECT symbol, sector FROM universe_profiles WHERE symbol IN ({placeholders})",
            list(symbols),
        ).fetchall()
        conn.close()
    except Exception:
        return None
    found = {sym: (sec or "") for sym, sec in rows}
    # Every requested symbol must be classified into the same non-empty sector
    if len(found) != len(symbols):
        return None  # at least one symbol missing from universe_profiles
    sectors = {sec for sec in found.values() if sec}
    if len(sectors) != 1 or "" in found.values():
        return None
    return sectors.pop()


def _infer_sleeve_sector(sleeve_config: dict) -> str | None:
    """Determine a sleeve's effective sector from its universe config.

    - type='sector' → declared sector
    - type='symbols' → common sector of all symbols (via universe_profiles)
    - type='all' or anything else → None (multi-sector)
    """
    universe = sleeve_config.get("universe", {}) or {}
    utype = universe.get("type")
    if utype == "sector":
        return universe.get("sector") or None
    if utype == "symbols":
        symbols = universe.get("symbols") or []
        return _lookup_symbols_sector(symbols)
    return None


def _infer_portfolio_sector(sleeves: list[dict]) -> str | None:
    """Determine a portfolio's effective sector by combining per-sleeve sectors.

    Returns the common sector if all sleeves share the same one, else None.
    """
    if not sleeves:
        return None
    per_sleeve = [_infer_sleeve_sector(s["config"]) for s in sleeves]
    if any(s is None for s in per_sleeve):
        return None  # at least one sleeve is multi-sector or unknown
    distinct = set(per_sleeve)
    return distinct.pop() if len(distinct) == 1 else None


def compute_portfolio_id(config: dict) -> str:
    """Deterministic ID from portfolio config."""
    key_parts = {
        "sleeves": config.get("sleeves", config.get("strategies", [])),
        "regime_filter": config.get("regime_filter", False),
        "capital_when_gated_off": config.get("capital_when_gated_off", config.get("capital_flow", "to_cash")),
    }
    raw = json.dumps(key_parts, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Load strategy configs
# ---------------------------------------------------------------------------
def _resolve_strategy_config(strategy_ref: dict) -> dict:
    """
    Resolve a sleeve's strategy reference to a validated config.

    Three paths:
      1. strategy_config (inline) → validate directly, stamp strategy_id
      2. config_path → load from file & validate
      3. strategy_id → look up from DB (strategies table), validate

    Returns a validated strategy config with a strategy_id.
    Raises ValueError if config is invalid or strategy not found.
    """
    if "strategy_config" in strategy_ref or "config" in strategy_ref:
        # Inline config — validate directly (no temp files)
        inline = deepcopy(strategy_ref.get("strategy_config") or strategy_ref["config"])

        # Ensure backtest block exists (portfolio overrides it later, but
        # validate_strategy() requires it for validation)
        if "backtest" not in inline:
            inline["backtest"] = {
                "start": "2015-01-01", "end": "2025-12-31",
                "entry_price": "next_close", "slippage_bps": 10,
            }

        stamp_strategy_id(inline)
        return validate_strategy(inline)

    if "config_path" in strategy_ref:
        path = Path(strategy_ref["config_path"])
        if not path.is_absolute():
            path = STRATEGIES_DIR / path
        return load_strategy(str(path))

    if "strategy_id" in strategy_ref:
        sid = strategy_ref["strategy_id"]
        # Look up from app DB (strategies are app state, not market data)
        import sqlite3 as _sqlite3
        conn = _sqlite3.connect(str(APP_DB_PATH))
        conn.row_factory = _sqlite3.Row
        try:
            row = conn.execute(
                "SELECT config FROM strategies WHERE strategy_id = ?", (sid,)
            ).fetchone()
        finally:
            conn.close()
        if row:
            config = json.loads(row[0])
            return validate_strategy(config)

        # Fallback: check strategies/ dir for legacy files
        for f in STRATEGIES_DIR.glob("*.json"):
            try:
                cfg = json.loads(f.read_text())
                if cfg.get("strategy_id") == sid:
                    return validate_strategy(cfg)
            except Exception:
                continue
        raise ValueError(f"Strategy {sid} not found in DB or {STRATEGIES_DIR}")

    raise ValueError(
        "Sleeve must reference a strategy via 'strategy_id', 'config_path', or 'strategy_config'."
    )


# ---------------------------------------------------------------------------
# Load regime configs from DB
# ---------------------------------------------------------------------------
def _load_regime_configs(regime_ids: list[str], conn) -> tuple[list[dict], dict]:
    """
    Load regime configs from the regimes table.

    Returns:
        (configs, id_to_name_map) where id_to_name_map = {regime_id: regime_name}
    """
    configs = []
    id_to_name = {}
    cur = conn.cursor()
    for rid in regime_ids:
        cur.execute("SELECT config FROM regimes WHERE regime_id = ?", (rid,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Regime {rid} not found in database")
        cfg = json.loads(row[0])
        configs.append(cfg)
        id_to_name[rid] = cfg["name"]
    return configs, id_to_name


# ---------------------------------------------------------------------------
# Portfolio Backtest
# ---------------------------------------------------------------------------
def run_portfolio_backtest(portfolio_config: dict, force_close_at_end: bool = True) -> dict:
    """
    Run a multi-strategy portfolio backtest with regime gating.

    Portfolio config format:
    {
        "name": "Barbell + Clock",
        "strategies": [
            {
                "strategy_id": "abc123",  // or config_path or inline config
                "weight": 0.25,
                "regime_gate": ["oil_shock_id", "credit_stress_id"],
                "label": "Energy Momentum"   // optional display name
            },
            {
                "strategy_id": "def456",
                "weight": 0.50,
                "regime_gate": ["*"],  // always active
                "label": "Defensive Quality"
            }
        ],
        "regime_filter": true,  // enable/disable regime gating
        "capital_flow": "to_cash" | "redistribute",
        "backtest": {
            "start": "2020-01-01",
            "end": "2026-03-28",
            "initial_capital": 1000000
        }
    }
    """
    name = portfolio_config.get("name", "Unnamed Portfolio")
    strategies_refs = portfolio_config.get("sleeves", portfolio_config.get("strategies", []))
    regime_enabled = portfolio_config.get("regime_filter", True)
    capital_flow = portfolio_config.get("capital_when_gated_off",
                                       portfolio_config.get("capital_flow", "to_cash"))
    bt_config = portfolio_config["backtest"]
    bt_start = bt_config["start"]
    bt_end = bt_config["end"]
    initial_capital = bt_config.get("initial_capital", 1000000)
    # Universal smoothing + rebalance defaults. Applied to any config that
    # doesn't explicitly override them. Explicit fields on the config always
    # win over these defaults.
    _DEFAULT_ENTRY_PERSIST = 3
    _DEFAULT_EXIT_PERSIST = 3
    _DEFAULT_TD_DEF = 1
    _DEFAULT_TD_OFF = 3

    transition_days = max(1, portfolio_config.get("transition_days", 1))
    td_def_raw = portfolio_config.get("transition_days_to_defensive", _DEFAULT_TD_DEF)
    td_off_raw = portfolio_config.get("transition_days_to_offensive", _DEFAULT_TD_OFF)
    transition_days_to_defensive = max(1, int(td_def_raw)) if td_def_raw is not None else None
    transition_days_to_offensive = max(1, int(td_off_raw)) if td_off_raw is not None else None
    asymmetric_active = (transition_days_to_defensive is not None or
                         transition_days_to_offensive is not None)
    transition_label = (
        f"to_defensive={transition_days_to_defensive or transition_days}d/"
        f"to_offensive={transition_days_to_offensive or transition_days}d"
        if asymmetric_active
        else f"{transition_days}d"
    )

    print(f"=" * 70)
    print(f"PORTFOLIO BACKTEST: {name}")
    print(f"Capital: ${initial_capital:,.0f} | Period: {bt_start} to {bt_end}")
    print(f"Regime filter: {'ON' if regime_enabled else 'OFF'} | Capital flow: {capital_flow} | Transition: {transition_label}")
    print(f"=" * 70)

    # -----------------------------------------------------------------------
    # Step 1: Resolve all strategy configs and set their capital allocation
    # -----------------------------------------------------------------------
    # When allocation_profiles are present, the sleeve's static `weight` is
    # used at runtime only as a fallback (when no profile matches). The
    # day-by-day exposure comes from the active profile, which can take any
    # sleeve to 0% or 100% regardless of its static weight. So a sleeve with
    # static weight=0 is a valid "dormant in default, activated in some
    # profile" pattern. To support that, we run every sleeve's standalone
    # backtest at FULL initial_capital when allocation_profiles is present —
    # the math layer's daily-weight overlay handles the actual exposure,
    # and rebalance trade emission remains numerically correct (the dollar
    # delta is set by incremental_nav × day_weight, independent of the
    # sleeve's standalone allocated_capital). Without allocation_profiles
    # (fixed-weight mode), allocated_capital = initial_capital × weight as
    # before, since the combined NAV is the sum of sleeve NAVs at their
    # declared shares.
    has_allocation_profiles = bool(portfolio_config.get("allocation_profiles"))

    sleeves = []
    total_weight = sum(s["weight"] for s in strategies_refs)
    if not has_allocation_profiles and abs(total_weight - 1.0) > 0.01:
        # Only enforce-and-normalize sleeve weights when running in fixed-
        # weight mode. With allocation_profiles, sleeve.weight is largely
        # symbolic; profile weights are the truth.
        print(f"WARNING: Strategy weights sum to {total_weight:.2f}, not 1.0. Normalizing.")
        for s in strategies_refs:
            s["weight"] = s["weight"] / total_weight

    for i, ref in enumerate(strategies_refs):
        config = _resolve_strategy_config(ref)
        label = ref.get("label", config.get("name", f"Strategy {i+1}"))
        weight = ref["weight"]
        regime_gate = ref.get("regime_gate", ["*"])
        if has_allocation_profiles:
            # Run every sleeve at full initial_capital so dormant (weight=0)
            # sleeves still have a meaningful standalone simulation. Day-by-
            # day exposure is governed by the active allocation profile.
            allocated_capital = initial_capital
        else:
            allocated_capital = initial_capital * weight

        # Override the strategy's backtest range and capital
        config["backtest"] = {
            "start": bt_start,
            "end": bt_end,
            "slippage_bps": config.get("backtest", {}).get("slippage_bps", 10),
            "entry_price": config.get("backtest", {}).get("entry_price", "next_close"),
        }
        config["sizing"]["initial_allocation"] = allocated_capital

        sleeves.append({
            "label": label,
            "weight": weight,
            "regime_gate": regime_gate,
            "config": config,
            "allocated_capital": allocated_capital,
        })

        print(f"\n  [{i+1}] {label}")
        print(f"      Weight: {weight*100:.0f}% (${allocated_capital:,.0f})")
        print(f"      Regime gate: {regime_gate}")
        print(f"      Universe: {config.get('universe', {})}")

    # -----------------------------------------------------------------------
    # Step 2: Pre-compute regime series (if regime gating is enabled)
    # -----------------------------------------------------------------------
    regime_series = {}  # {date: [active_regime_names]}
    regime_persistence_stats = {}  # {regime_name: {entry_persistence_days, raw_entry_met_days, active_days, n_activations, ...}}
    regime_id_to_name = {}  # {regime_id: regime_name}
    all_regime_ids = set()

    # Dynamic allocation profiles
    allocation_profiles = portfolio_config.get("allocation_profiles", None)
    profile_priority = portfolio_config.get("profile_priority", [])

    if regime_enabled:
        for sleeve in sleeves:
            for rid in sleeve["regime_gate"]:
                if rid != "*":
                    all_regime_ids.add(rid)

        # Also collect regime IDs from allocation profile triggers
        if allocation_profiles:
            for pname, pdef in allocation_profiles.items():
                if pname != "default" and isinstance(pdef, dict):
                    for rid in pdef.get("trigger", []):
                        all_regime_ids.add(rid)

        if all_regime_ids:
            print(f"\n  Loading {len(all_regime_ids)} regime definitions...")

            # Resolve inline regime_definitions first, fall back to DB
            inline_defs = portfolio_config.get("regime_definitions") or {}
            inline_ids = set()
            db_ids = []
            regime_configs = []

            for rid in all_regime_ids:
                if rid in inline_defs:
                    inline_ids.add(rid)
                    defn = inline_defs[rid]
                    # Convert InlineRegimeDefinition to engine format:
                    # engine expects {name, conditions/entry_conditions, logic/entry_logic, ...}
                    rc = {"name": rid}
                    if isinstance(defn, dict):
                        rc.update(defn)
                    else:
                        # Pydantic model instance — convert to dict
                        rc.update(defn if isinstance(defn, dict) else defn.model_dump())
                    regime_configs.append(rc)
                    regime_id_to_name[rid] = rid
                    print(f"    {rid}: resolved from inline regime_definitions")
                else:
                    db_ids.append(rid)

            # Load remaining from DB
            if db_ids:
                import sqlite3 as _sqlite3
                _app_conn = _sqlite3.connect(str(APP_DB_PATH))
                _app_conn.row_factory = _sqlite3.Row
                db_configs, db_id_to_name = _load_regime_configs(db_ids, _app_conn)
                _app_conn.close()
                regime_configs.extend(db_configs)
                regime_id_to_name.update(db_id_to_name)

            # Stamp version-aware persistence defaults onto each regime config
            # before evaluation, so v2 strategies get persistence=3/3 even if
            # the regime didn't explicitly declare it. Explicit values win.
            for rc in regime_configs:
                rc.setdefault("entry_persistence_days", _DEFAULT_ENTRY_PERSIST)
                rc.setdefault("exit_persistence_days", _DEFAULT_EXIT_PERSIST)

            print(f"  Computing regime series {bt_start} to {bt_end}...")
            regime_series, regime_persistence_stats = evaluate_regime_series_with_stats(
                bt_start, bt_end, regime_configs
            )
            
            # Count active days per regime
            from collections import Counter
            counts = Counter()
            for date, active in regime_series.items():
                for r in active:
                    counts[r] += 1
            total_days = len(regime_series)
            print(f"  {total_days} trading days evaluated")
            for rname, cnt in counts.most_common():
                print(f"    {rname}: {cnt} days ({cnt/total_days*100:.1f}%)")

    # -----------------------------------------------------------------------
    # Step 2.5: Pre-compute per-sleeve gate dates for execution-level gating
    # -----------------------------------------------------------------------
    # Each sleeve gets a set of dates where new entries are allowed.
    #
    # Two independent sources can gate a sleeve off on a given day:
    #
    #   1. regime_gate on the sleeve — when set to a list of regime IDs, the
    #      sleeve is active only on days when at least one of those regimes
    #      is firing. ["*"], [], or regime disabled = always-on at this layer.
    #
    #   2. allocation_profiles at the portfolio — when an allocation profile
    #      assigns the sleeve weight 0 for a given day's active regimes, the
    #      sleeve should be sidelined: the portfolio-level lerp will pull its
    #      capital to 0%, and the sleeve's own simulation must stop emitting
    #      entry / rebalance trades on those days, or it produces phantom
    #      trades that don't move portfolio NAV but pollute the trade log.
    #
    # Effective gate = intersection of the two. A sleeve is "on" only when
    # both layers allow it.
    #
    # Without this, regime_gate=[] silently meant "always active" even when
    # allocation_profiles zero'd the sleeve out — producing the v44-style bug
    # where the trade ledger shows entries during gated-off regimes.
    def _profile_weights_for_regimes(active_regime_names):
        """Resolve the active allocation profile for a given day's regimes.

        Mirrors the in-loop _resolve_profile semantics but doesn't depend on
        transition state. Used to derive gate dates from allocation profiles
        before the main NAV loop runs. Returns the inner `.weights` dict, or
        None if allocation_profiles aren't configured.
        """
        if not allocation_profiles or not profile_priority:
            return None
        for pname in profile_priority:
            if pname == "default":
                return allocation_profiles.get("default", {}).get("weights", {})
            pdef = allocation_profiles.get(pname, {})
            triggers = pdef.get("trigger", [])
            if not triggers:
                continue
            trigger_names = {regime_id_to_name.get(rid, rid) for rid in triggers}
            if trigger_names and trigger_names.issubset(active_regime_names):
                return pdef.get("weights", {})
        return allocation_profiles.get("default", {}).get("weights", {}) if "default" in allocation_profiles else None

    # Build per-day allocation-profile weights once, reuse per sleeve below.
    profile_weights_by_date = {}
    if regime_enabled and allocation_profiles and profile_priority:
        for date, active in regime_series.items():
            w = _profile_weights_for_regimes(set(active))
            if w is not None:
                profile_weights_by_date[date] = w

    sleeve_gate_dates = []
    for sleeve in sleeves:
        label = sleeve["label"]
        gate = sleeve["regime_gate"]

        # Layer 1: regime_gate dates ("always-on" when ["*"], [], or disabled).
        if not regime_enabled or gate == ["*"] or not gate:
            regime_dates_on = None  # sentinel: all dates allowed
        else:
            gated_names = {regime_id_to_name.get(rid, rid) for rid in gate}
            regime_dates_on = {
                date for date, active in regime_series.items()
                if gated_names & set(active)
            }

        # Layer 2: allocation-profile non-zero-weight dates. None when no
        # allocation_profiles are configured.
        if profile_weights_by_date:
            alloc_dates_on = {
                d for d, w in profile_weights_by_date.items()
                if float(w.get(label, 0) or 0) > 0
            }
        else:
            alloc_dates_on = None

        # Intersect — sleeve is on iff BOTH layers allow it.
        if regime_dates_on is None and alloc_dates_on is None:
            dates_on = None  # always-on (no gating at all)
        elif regime_dates_on is None:
            dates_on = alloc_dates_on
        elif alloc_dates_on is None:
            dates_on = regime_dates_on
        else:
            dates_on = regime_dates_on & alloc_dates_on

        sleeve_gate_dates.append(dates_on)

        if dates_on is not None and regime_series:
            total = len(regime_series)
            on = len(dates_on)
            # Indicate which layers contributed when both are present.
            layers = []
            if regime_dates_on is not None:
                layers.append("regime_gate")
            if alloc_dates_on is not None:
                layers.append("allocation_profiles")
            src = "+".join(layers)
            print(f"  Sleeve '{label}' gate ({src}): {on}/{total} days active ({on/total*100:.1f}%)")

    # -----------------------------------------------------------------------
    # Step 2.6: Build shared price index for all sleeve universes
    # -----------------------------------------------------------------------
    all_sleeve_symbols = set()
    conn = get_connection()
    for sleeve in sleeves:
        sleeve_symbols = resolve_universe(sleeve["config"], conn)
        all_sleeve_symbols.update(sleeve_symbols)
    # Include benchmark ticker
    all_sleeve_symbols.add("SPY")

    print(f"\n  Building shared price index for {len(all_sleeve_symbols)} tickers...")
    shared_price_index, shared_open_index, shared_trading_dates = build_price_index(
        list(all_sleeve_symbols), conn)
    conn.close()
    shared_pi = (shared_price_index, shared_open_index, shared_trading_dates)

    # -----------------------------------------------------------------------
    # Step 3: Run each strategy backtest independently
    # -----------------------------------------------------------------------
    sleeve_results = []
    for i, sleeve in enumerate(sleeves):
        label = sleeve["label"]
        print(f"\n{'─' * 60}")
        print(f"Running sleeve [{i+1}]: {label}")
        print(f"{'─' * 60}")

        result = run_backtest(sleeve["config"], force_close_at_end=force_close_at_end,
                              shared_price_index=shared_pi,
                              gate_dates=sleeve_gate_dates[i])
        sleeve_results.append(result)

    # -----------------------------------------------------------------------
    # Step 4: Build combined NAV with regime gating
    # -----------------------------------------------------------------------
    print(f"\n{'─' * 60}")
    print(f"Computing combined portfolio NAV with regime gating...")
    print(f"{'─' * 60}")

    # Get all trading dates from sleeve NAV histories
    all_dates = set()
    for result in sleeve_results:
        for entry in result["nav_history"]:
            all_dates.add(entry["date"])
    all_dates = sorted(all_dates)

    # Build per-sleeve NAV, positions_value, and per-position lookups.
    # The per-position lookup is needed for portfolio-level rebalance trade
    # emission (Phase 1 of the rebalance-execution feature): on each lerp day
    # we trim or top-up across the sleeve's currently-held positions.
    sleeve_nav_lookup = []
    sleeve_pv_lookup = []
    sleeve_positions_lookup = []
    for result in sleeve_results:
        nav_map = {entry["date"]: entry["nav"] for entry in result["nav_history"]}
        pv_map = {entry["date"]: entry.get("positions_value", 0) for entry in result["nav_history"]}
        pos_map = {entry["date"]: entry.get("positions") or {} for entry in result["nav_history"]}
        sleeve_positions_lookup.append(pos_map)
        sleeve_nav_lookup.append(nav_map)
        sleeve_pv_lookup.append(pv_map)

    # Compute combined NAV with regime gating and dynamic allocation
    combined_nav_history = []
    regime_history = []
    allocation_profile_history = []
    sleeve_active_days = [0] * len(sleeves)
    sleeve_gated_off_days = [0] * len(sleeves)
    prev_profile_name = None

    # --- Per-sleeve tracking ---
    # Sleeve backtests now execute with regime gating (gate_dates), so their
    # raw NAV histories already reflect gated execution. For fixed-weight mode,
    # the combined NAV is simply the sum of sleeve NAVs.
    # For redistribute mode, capital from gated-off sleeves earns active sleeves' return.
    n_sleeves = len(sleeves)
    sleeve_frozen_capital = [0.0] * n_sleeves

    # --- Redistribute tracking ---
    redistribute_balance = 0.0
    prev_gate_status = None

    # Helper: resolve active allocation profile for a given date
    def _resolve_profile(active_regimes_today):
        """Walk profile_priority, return (profile_name, weights_dict).

        Always returns the inner `.weights` sub-dict of a profile, never the
        outer profile wrapper. Previously the "default" branch returned the
        whole {trigger, weights, ...} dict, which both crashed the transition
        summary printer and made per-sleeve weight lookups silently return 0.
        """
        if not allocation_profiles or not profile_priority:
            return None, None
        for pname in profile_priority:
            if pname == "default":
                return "default", allocation_profiles.get("default", {}).get("weights", {})
            pdef = allocation_profiles.get(pname, {})
            triggers = pdef.get("trigger", [])
            if not triggers:
                continue
            # All trigger regimes must be active (by name)
            trigger_names = {regime_id_to_name.get(rid, rid) for rid in triggers}
            if trigger_names and trigger_names.issubset(set(active_regimes_today)):
                return pname, pdef.get("weights", {})
        # Fallback to default
        if "default" in allocation_profiles:
            return "default", allocation_profiles.get("default", {}).get("weights", {})
        return None, None

    all_dates_index = {d: idx for idx, d in enumerate(all_dates)}

    # --- Transition state for gradual profile switches ---
    # When transition_days > 1 (or asymmetric variants resolve >1), we lerp
    # weights from old profile to new over N days. `effective_days` is the
    # per-flip lerp duration: with the symmetric legacy field it's the same
    # for every flip; with the asymmetric variant it's chosen at flip time
    # based on direction (defensive vs. offensive).
    transition_state = {
        "active": False,
        "from_weights": None,
        "to_weights": None,
        "start_idx": 0,
        "end_idx": 0,
        "effective_days": transition_days,
        "target_profile": None,
    }

    def _equity_weight(weights: dict[str, float]) -> float:
        """Sum of weights for non-Cash sleeves. The 'Cash' label is the
        convention for unallocated capital in allocation_profiles."""
        return sum(w for k, w in (weights or {}).items() if k.lower() != "cash")

    def _resolve_transition_days(from_weights, to_weights):
        """Pick the lerp duration for THIS flip based on direction."""
        if not asymmetric_active:
            return transition_days
        eq_from = _equity_weight(from_weights)
        eq_to = _equity_weight(to_weights)
        if eq_to < eq_from:
            return transition_days_to_defensive or transition_days
        if eq_to > eq_from:
            return transition_days_to_offensive or transition_days
        # Equal equity weight — fall back to legacy.
        return transition_days

    def _get_effective_weights(date_idx, target_profile_name, target_weights, sleeve_labels):
        """
        Return effective weights for today, accounting for in-progress transitions.
        Updates transition_state as a side effect.
        """
        nonlocal prev_profile_name

        # Has the target profile changed since we last set up a transition?
        if target_profile_name != transition_state.get("target_profile"):
            # Determine starting weights for the new transition.
            if transition_state["active"]:
                # Mid-transition retarget: snapshot the currently-interpolated
                # weights as the new "from".
                eff = max(1, transition_state["effective_days"])
                progress = min(1.0, (date_idx - transition_state["start_idx"]) / eff)
                from_w = transition_state["from_weights"]
                to_w = transition_state["to_weights"]
                current_weights = {
                    lbl: from_w.get(lbl, 0.0)
                         + (to_w.get(lbl, 0.0) - from_w.get(lbl, 0.0)) * progress
                    for lbl in sleeve_labels
                }
            elif prev_profile_name and allocation_profiles:
                prev_def = allocation_profiles.get(prev_profile_name, {})
                # Accept both shapes: wrapped {trigger, weights} (production)
                # and bare {sleeve_label: weight} (legacy test fixtures).
                pw = prev_def.get("weights", prev_def)
                current_weights = {lbl: pw.get(lbl, 0.0) for lbl in sleeve_labels}
            else:
                # First profile of the run — apply target directly, no lerp.
                prev_profile_name = target_profile_name
                allocation_profile_history.append({
                    "date": all_dates[date_idx],
                    "profile_name": target_profile_name,
                    "weights": target_weights,
                    "transition": "instant (initial)",
                })
                transition_state["target_profile"] = target_profile_name
                transition_state["active"] = False
                transition_state["to_weights"] = target_weights
                return target_weights

            # Pick this flip's lerp duration based on direction.
            effective_days = _resolve_transition_days(current_weights, target_weights)

            if effective_days <= 1:
                # Instant snap (or asymmetric=fast in this direction).
                allocation_profile_history.append({
                    "date": all_dates[date_idx],
                    "profile_name": target_profile_name,
                    "weights": target_weights,
                    "transition": "instant",
                    "from_weights": current_weights,
                })
                prev_profile_name = target_profile_name
                transition_state["target_profile"] = target_profile_name
                transition_state["active"] = False
                transition_state["to_weights"] = target_weights
                return target_weights

            # Begin gradual lerp over effective_days.
            transition_state["active"] = True
            transition_state["from_weights"] = current_weights
            transition_state["to_weights"] = target_weights
            transition_state["start_idx"] = date_idx
            transition_state["end_idx"] = date_idx + effective_days
            transition_state["effective_days"] = effective_days
            transition_state["target_profile"] = target_profile_name
            allocation_profile_history.append({
                "date": all_dates[date_idx],
                "profile_name": target_profile_name,
                "weights": target_weights,
                "transition": f"gradual over {effective_days} days",
                "from_weights": current_weights,
            })
            prev_profile_name = target_profile_name

        # Compute interpolated weights for today.
        if transition_state["active"]:
            eff = max(1, transition_state["effective_days"])
            elapsed = date_idx - transition_state["start_idx"]
            progress = min(1.0, elapsed / eff)
            if progress >= 1.0:
                transition_state["active"] = False
                return transition_state["to_weights"]
            from_w = transition_state["from_weights"]
            to_w = transition_state["to_weights"]
            return {
                lbl: from_w.get(lbl, 0.0)
                     + (to_w.get(lbl, 0.0) - from_w.get(lbl, 0.0)) * progress
                for lbl in sleeve_labels
            }

        # Settled at the current target — return its weights. (Previously the
        # function fell off the end here and returned None, which silently
        # reverted to static sleeve weights for any settled-non-default day.)
        return transition_state.get("to_weights") or target_weights

        return target_weights

    sleeve_labels = [s["label"] for s in sleeves]

    # --- Incremental NAV for dynamic allocation profiles ---
    # When allocation_profiles are active, we can't sum sleeve NAVs directly
    # because sleeves run with fixed capital but profiles change weights.
    # Instead, track portfolio NAV incrementally: each day's return is the
    # weighted average of sleeve daily returns according to the current profile.
    incremental_nav = initial_capital  # running portfolio NAV for dynamic mode

    # --- Rebalance trade emission state ---
    # When allocation_profiles are active, the engine tracks each sleeve's
    # ACTUAL dollar exposure separately from its target exposure. Each day:
    #
    #   carried[i] = prev_actual[i] × (1 + sleeve_daily_return)  (mark-to-market)
    #   target[i]  = realized_nav × day_weights[i]               (today's contract)
    #   drift[i]   = |actual_weight[i] - target_weight[i]|       (relative gap)
    #
    # WHEN to rebalance:
    #   - allocation_profiles absent: never (fixed-weight portfolio).
    #   - allocation_profiles present: when max drift > rebalance_threshold,
    #     OR the profile target just changed, OR mid-lerp.
    #
    # On a rebalance day, trades are emitted to bring carried → target,
    # allocated proportionally across currently-held positions. Slippage on
    # the trade dollars is realized as NAV drag.
    REBALANCE_DOLLAR_TOLERANCE = 1.0  # below this, no trade
    emit_rebalance_trades = bool(portfolio_config.get("allocation_profiles"))
    rebalance_threshold = float(portfolio_config.get("rebalance_threshold", 0.05))
    prev_target_dollars = [0.0] * len(sleeves)
    rebalance_trades_by_sleeve = [[] for _ in sleeves]
    cumulative_rebalance_slippage = 0.0
    rebalance_event_count = 0
    rebalance_skipped_days = 0  # days where threshold filtered out drift correction
    prev_settled_profile = None
    if emit_rebalance_trades:
        threshold_label = (
            f"threshold={rebalance_threshold*100:.1f}%"
            if rebalance_threshold > 0 else "continuous"
        )
        print(f"  Rebalance policy: {threshold_label}")

    # Helper: compute raw daily return for a sleeve
    def _sleeve_daily_return(i, date, prev_date):
        """Single-day return for sleeve i. Returns 0.0 if data missing."""
        if prev_date is None:
            return 0.0
        curr_raw = sleeve_nav_lookup[i].get(date, sleeves[i]["allocated_capital"])
        prev_raw = sleeve_nav_lookup[i].get(prev_date, sleeves[i]["allocated_capital"])
        if prev_raw > 0:
            return curr_raw / prev_raw - 1
        return 0.0

    for date in all_dates:
        date_idx = all_dates_index[date]
        prev_date = all_dates[date_idx - 1] if date_idx > 0 else None

        # Determine which regimes are active today
        active_regimes_today = []
        if regime_enabled and date in regime_series:
            active_regimes_today = regime_series[date]

        regime_history.append({
            "date": date,
            "active_regimes": active_regimes_today,
        })

        # Resolve dynamic allocation profile (target — may be transitioned to gradually)
        target_profile_name, target_profile_weights = _resolve_profile(active_regimes_today)

        # Apply transition smoothing (lerp over transition_days)
        if target_profile_weights is not None:
            effective_weights = _get_effective_weights(
                date_idx, target_profile_name, target_profile_weights, sleeve_labels
            )
            profile_weights = effective_weights
            profile_name = target_profile_name
        else:
            profile_weights = None
            profile_name = None

        # Determine per-sleeve weights for today
        day_weights = []
        for i, sleeve in enumerate(sleeves):
            if profile_weights is not None:
                w = profile_weights.get(sleeve["label"], 0.0)
            else:
                w = sleeve["weight"]
            day_weights.append(w)

        # Determine gate status per sleeve.
        # Empty list [] is treated as ["*"] (always on) — matches the schema
        # contract ("Empty or ['*'] = always active") and the equivalent
        # handling in the Step 2.5 gate-dates precompute above.
        gate_status = []
        for i, sleeve in enumerate(sleeves):
            gate = sleeve["regime_gate"]
            is_gated_on = False
            if not regime_enabled or gate == ["*"] or not gate:
                is_gated_on = True
            else:
                gated_names = {regime_id_to_name.get(rid, rid) for rid in gate}
                if gated_names & set(active_regimes_today):
                    is_gated_on = True
            # Also gate off if dynamic weight is 0
            if day_weights[i] == 0.0 and profile_weights is not None:
                is_gated_on = False
            gate_status.append(is_gated_on)

        sleeve_navs = []
        use_redistribute = capital_flow == "redistribute"

        # --- Handle gate transitions for redistribute mode ---
        if use_redistribute:
            if prev_gate_status is not None:
                for i in range(n_sleeves):
                    sleeve_nav_today = sleeve_nav_lookup[i].get(date, sleeves[i]["allocated_capital"])
                    if prev_gate_status[i] and not gate_status[i]:
                        # Sleeve just gated OFF — track its capital for redistribution
                        sleeve_frozen_capital[i] = sleeve_nav_today
                        redistribute_balance += sleeve_nav_today
                    elif not prev_gate_status[i] and gate_status[i]:
                        # Sleeve just gated ON — restore frozen capital
                        if sleeve_frozen_capital[i] > 0:
                            redistribute_balance = max(0, redistribute_balance - sleeve_frozen_capital[i])
                            sleeve_frozen_capital[i] = 0.0
            else:
                # First day: sleeves that start gated off
                for i in range(n_sleeves):
                    if not gate_status[i]:
                        sleeve_nav_today = sleeve_nav_lookup[i].get(date, sleeves[i]["allocated_capital"])
                        sleeve_frozen_capital[i] = sleeve_nav_today
                        redistribute_balance += sleeve_nav_today

        # --- Compound redistribute balance with active sleeves' avg return ---
        if use_redistribute and redistribute_balance > 0 and prev_date is not None:
            active_return_sum = 0.0
            active_w_sum = 0.0
            for i in range(n_sleeves):
                if gate_status[i]:
                    daily_ret = _sleeve_daily_return(i, date, prev_date)
                    active_return_sum += daily_ret * day_weights[i]
                    active_w_sum += day_weights[i]
            if active_w_sum > 0:
                avg_daily_ret = active_return_sum / active_w_sum
                redistribute_balance *= (1 + avg_daily_ret)

        # --- Compute combined NAV ---
        combined_nav = 0.0

        if profile_weights is not None:
            # DYNAMIC PROFILE MODE: incremental NAV using weighted daily returns
            if prev_date is not None:
                # For redistribute: rescale active weights to sum to 1.0
                # so idle capital is implicitly redistributed
                effective_day_weights = list(day_weights)
                if use_redistribute:
                    active_w_sum = sum(w for i, w in enumerate(effective_day_weights)
                                       if w > 0 and sleeves[i]["label"] != "Cash")
                    if active_w_sum > 0 and active_w_sum < 0.999:
                        scale = 1.0 / active_w_sum
                        effective_day_weights = [
                            w * scale if w > 0 and sleeves[i]["label"] != "Cash" else w
                            for i, w in enumerate(effective_day_weights)
                        ]

                weighted_return = 0.0
                for i, sleeve in enumerate(sleeves):
                    w = effective_day_weights[i]
                    if w > 0 and sleeve["label"] != "Cash":
                        daily_ret = _sleeve_daily_return(i, date, prev_date)
                        weighted_return += w * daily_ret
                incremental_nav *= (1 + weighted_return)

            # --- Threshold-aware rebalance trade emission (v2+) ---
            # The math layer's `weighted_return` above produces the portfolio
            # NAV under an "ideal daily rebalance" assumption. With threshold
            # rebalancing (v3), we don't actually rebalance daily — but the
            # NAV approximation stays close because drift is bounded by the
            # threshold (5% × daily volatility ≈ tens of bps drift in NAV).
            #
            # The trade book IS faithful: trades fire only when drift exceeds
            # the threshold OR when the profile target changes (regime flip /
            # mid-lerp). Slippage is realized on actual trade days only.
            if emit_rebalance_trades and prev_date is not None:
                # Compute today's per-sleeve target and carried exposure.
                sleeve_targets = []
                sleeve_carried = []
                for i, sleeve in enumerate(sleeves):
                    if sleeve["label"].lower() == "cash":
                        sleeve_targets.append(0.0)
                        sleeve_carried.append(0.0)
                        continue
                    target = incremental_nav * day_weights[i]
                    sleeve_ret = _sleeve_daily_return(i, date, prev_date)
                    carried = prev_target_dollars[i] * (1 + sleeve_ret)
                    sleeve_targets.append(target)
                    sleeve_carried.append(carried)

                # Drift = max absolute weight gap between target and carried.
                max_drift = 0.0
                if incremental_nav > 0:
                    for i in range(len(sleeves)):
                        if sleeves[i]["label"].lower() == "cash":
                            continue
                        actual_w = sleeve_carried[i] / incremental_nav
                        drift = abs(day_weights[i] - actual_w)
                        if drift > max_drift:
                            max_drift = drift

                # Profile flips and mid-lerp days bypass the threshold —
                # the contract itself is changing.
                profile_changed = (
                    target_profile_name != prev_settled_profile
                    or transition_state.get("active", False)
                )
                must_rebalance = profile_changed or (max_drift > rebalance_threshold)

                if must_rebalance:
                    slippage_drag_today = 0.0
                    profile_label = profile_name or "default"
                    reason = f"rebalance_to_{profile_label}"
                    for i, sleeve in enumerate(sleeves):
                        if sleeve["label"].lower() == "cash":
                            continue
                        delta = sleeve_targets[i] - sleeve_carried[i]
                        if abs(delta) <= REBALANCE_DOLLAR_TOLERANCE:
                            continue
                        positions = sleeve_positions_lookup[i].get(date, {}) or {}
                        total_pv = sum(p.get("market_value", 0) for p in positions.values())
                        if total_pv <= 0:
                            continue
                        fraction = delta / total_pv
                        action = "SELL" if delta < 0 else "BUY"
                        for symbol, p in positions.items():
                            trade_shares = abs(p.get("shares", 0) * fraction)
                            price = p.get("price", 0)
                            trade_dollars = trade_shares * price
                            if trade_dollars < 1.0:
                                continue
                            rebalance_trades_by_sleeve[i].append({
                                "date": date,
                                "symbol": symbol,
                                "action": action,
                                "shares": round(trade_shares, 4),
                                "price": round(price, 2),
                                "amount": round(trade_dollars, 2),
                                "reason": reason,
                            })
                        rebalance_event_count += 1
                        slip_bps = sleeves[i]["config"].get("backtest", {}).get("slippage_bps", 10)
                        slippage_drag_today += abs(delta) * slip_bps / 10000

                    if slippage_drag_today > 0:
                        cumulative_rebalance_slippage += slippage_drag_today
                        incremental_nav -= slippage_drag_today

                    # After rebalance, the new "previous target" anchor for
                    # next day's drift comparison is today's (post-slippage)
                    # target, in line with what we now hold.
                    for i in range(len(sleeves)):
                        prev_target_dollars[i] = incremental_nav * day_weights[i]
                else:
                    # Within threshold — let the portfolio drift. Tomorrow's
                    # carry-forward starts from today's drifted holdings,
                    # not the ideal target.
                    rebalance_skipped_days += 1
                    for i in range(len(sleeves)):
                        prev_target_dollars[i] = sleeve_carried[i]

                # Track most-recent settled profile for next day's flip check.
                if not transition_state.get("active", False):
                    prev_settled_profile = target_profile_name
            elif emit_rebalance_trades:
                # First day (prev_date is None): seed prev_target_dollars at
                # today's target so day 1's carry-forward has a basis.
                for i in range(len(sleeves)):
                    prev_target_dollars[i] = incremental_nav * day_weights[i]
                prev_settled_profile = target_profile_name

            combined_nav = incremental_nav

            # Track per-sleeve stats and build display
            for i, sleeve in enumerate(sleeves):
                w = day_weights[i]
                is_active = gate_status[i] and w > 0
                if is_active:
                    sleeve_active_days[i] += 1
                else:
                    sleeve_gated_off_days[i] += 1
                display_nav = incremental_nav * w if w > 0 else 0
                # Compute positions_value for this sleeve: scale raw PV by weight
                raw_nav = sleeve_nav_lookup[i].get(date, sleeves[i]["allocated_capital"])
                raw_pv = sleeve_pv_lookup[i].get(date, 0)
                sleeve_pv = display_nav * (raw_pv / raw_nav) if raw_nav > 0 and is_active else 0
                sleeve_navs.append({
                    "label": sleeve["label"],
                    "nav": round(display_nav, 2),
                    "positions_value": round(sleeve_pv, 2),
                    "active": is_active,
                    "weight": round(w, 4),
                })

        else:
            # FIXED WEIGHT MODE: sum of raw sleeve NAVs (sleeves already gated)
            for i, sleeve in enumerate(sleeves):
                raw_nav = sleeve_nav_lookup[i].get(date, sleeves[i]["allocated_capital"])
                raw_pv = sleeve_pv_lookup[i].get(date, 0)

                if gate_status[i]:
                    sleeve_active_days[i] += 1
                else:
                    sleeve_gated_off_days[i] += 1

                if not use_redistribute or gate_status[i]:
                    # Active sleeve or to_cash mode: NAV counts directly
                    combined_nav += raw_nav
                # redistribute: gated-off capital is in redistribute_balance

                sleeve_navs.append({
                    "label": sleeve["label"],
                    "nav": round(raw_nav, 2),
                    "positions_value": round(raw_pv, 2),
                    "active": gate_status[i],
                    "weight": round(day_weights[i], 4),
                })

            if use_redistribute:
                combined_nav += redistribute_balance

        # Track gate status for next day's transition detection
        prev_gate_status = list(gate_status)

        total_positions_value = sum(s["positions_value"] for s in sleeve_navs)
        combined_nav_history.append({
            "date": date,
            "nav": round(combined_nav, 2),
            "positions_value": round(total_positions_value, 2),
            "sleeves": sleeve_navs,
        })

    # -----------------------------------------------------------------------
    # Step 5: Compute portfolio-level metrics
    # -----------------------------------------------------------------------
    if combined_nav_history:
        first_nav = combined_nav_history[0]["nav"]
        last_nav = combined_nav_history[-1]["nav"]
        total_return = (last_nav / initial_capital - 1) * 100

        # Annualized return — gated by trading-day sample size, consistent
        # with the per-sleeve and benchmark sides. Below the gate the metric
        # would be a wild extrapolation of noise; we report None instead.
        from backtest_engine import MIN_TRADING_DAYS_FOR_ANNUALIZATION
        n_nav = len(combined_nav_history)
        if n_nav >= MIN_TRADING_DAYS_FOR_ANNUALIZATION:
            years = n_nav / 252.0
            ann_return = ((last_nav / initial_capital) ** (1 / years) - 1) * 100
        else:
            ann_return = None

        # Max drawdown
        peak = initial_capital
        max_dd = 0
        for entry in combined_nav_history:
            nav = entry["nav"]
            if nav > peak:
                peak = nav
            dd = (nav / peak - 1) * 100
            if dd < max_dd:
                max_dd = dd

        # Risk-free rate (load from treasury data, same as backtest engine)
        risk_free_ann = 0.0
        try:
            treasury_path = Path(__file__).parent.parent / "data" / "macro" / "treasury-rates.json"
            if treasury_path.exists():
                import json as _json
                treasury_data = _json.loads(treasury_path.read_text())
                t_rates = treasury_data.get("data", treasury_data) if isinstance(treasury_data, dict) else treasury_data
                period_rates = [r["month3"] for r in t_rates
                               if all_dates[0] <= r["date"] <= all_dates[-1] and r.get("month3") is not None]
                if period_rates:
                    risk_free_ann = sum(period_rates) / len(period_rates)
        except Exception:
            risk_free_ann = 2.0  # conservative default

        # Sharpe / Sortino / Calmar — all built from the gated ann_return.
        # When ann_return is None (short window), the entire group is None.
        daily_returns = []
        for j in range(1, len(combined_nav_history)):
            prev = combined_nav_history[j-1]["nav"]
            curr = combined_nav_history[j]["nav"]
            if prev > 0:
                daily_returns.append(curr / prev - 1)

        if daily_returns and ann_return is not None:
            import statistics
            import math
            daily_std = statistics.stdev(daily_returns) if len(daily_returns) > 1 else 0
            ann_vol = daily_std * (252 ** 0.5) * 100
            excess_return = ann_return - risk_free_ann
            sharpe_ann = excess_return / ann_vol if ann_vol > 0 else 0

            # Period-basis sharpe: for short windows the annualized number is a
            # noisy extrapolation (std-err grows with 1/sqrt(years)). Compute a
            # period sharpe that matches the displayed period return so the UI
            # is self-consistent on short deployments. Basis flips at 252 days.
            n = len(daily_returns)
            period_vol = daily_std * (n ** 0.5) * 100
            rf_period = risk_free_ann * (n / 252.0)
            period_return = total_return  # already computed
            sharpe_period = (period_return - rf_period) / period_vol if period_vol > 0 else 0

            if n_nav < 252:
                sharpe = sharpe_period
                sharpe_basis = "period"
            else:
                sharpe = sharpe_ann
                sharpe_basis = "annualized"

            daily_rf = risk_free_ann / 100 / 252
            downside_sq = [min(r - daily_rf, 0) ** 2 for r in daily_returns]
            downside_dev = math.sqrt(sum(downside_sq) / len(downside_sq)) * math.sqrt(252) * 100
            sortino = excess_return / downside_dev if downside_dev > 0 else 0

            # Calmar = ann return / |max drawdown|
            calmar = abs(ann_return / max_dd) if max_dd < 0 else 0
        else:
            ann_vol = None
            sharpe = None
            sharpe_ann = None
            sharpe_period = None
            sharpe_basis = None
            sortino = None
            calmar = None

        # Max drawdown date
        peak = initial_capital
        max_dd_date = all_dates[0]
        running_dd = 0
        for entry in combined_nav_history:
            nav = entry["nav"]
            if nav > peak:
                peak = nav
            dd = (nav / peak - 1) * 100
            if dd < running_dd:
                running_dd = dd
                max_dd_date = entry["date"]

        # Total trades and win rate from closed_trades arrays (more reliable than metrics counters)
        all_closed = [t for sr in sleeve_results for t in sr.get("closed_trades", [])]
        total_entries = sum(sr.get("metrics", {}).get("total_entries", 0) for sr in sleeve_results)
        total_wins = sum(1 for t in all_closed if t.get("pnl", 0) > 0)
        total_losses = sum(1 for t in all_closed if t.get("pnl", 0) <= 0)
        closed = len(all_closed)
        win_rate = round(total_wins / max(closed, 1) * 100, 1)

        import statistics

        # Profit factor
        gross_profit = sum(t.get("pnl", 0) for t in all_closed if t.get("pnl", 0) > 0)
        gross_loss = abs(sum(t.get("pnl", 0) for t in all_closed if t.get("pnl", 0) < 0))
        profit_factor = round(min(gross_profit / max(gross_loss, 0.01), 999.99), 2)

        # Avg holding days
        holding_days = [t.get("days_held", 0) for t in all_closed if t.get("days_held")]
        avg_holding_days = round(statistics.mean(holding_days), 1) if holding_days else 0

        # Utilization — based on actual positions_value (capital deployed in positions),
        # not sleeve NAV (which includes idle cash within each sleeve)
        positions_values = [entry.get("positions_value", 0) for entry in combined_nav_history]
        peak_utilized_capital = max(positions_values) if positions_values else 0
        avg_utilized_capital = statistics.mean(positions_values) if positions_values else 0
        utilization_pct = round(avg_utilized_capital / max(initial_capital, 1) * 100, 1)
        total_pnl = last_nav - initial_capital
        return_on_utilized_capital_pct = round(
            (total_pnl / avg_utilized_capital) * 100 if avg_utilized_capital > 0 else 0, 2
        )

        # Realized vs unrealized split. When force_close_at_end=False, positions
        # remaining at the last bar haven't played out — their pnl is "paper"
        # until the next signal-driven exit. Report both so consumers can tell
        # how much of the final number is locked in vs still at risk.
        positions_value_at_end = combined_nav_history[-1].get("positions_value", 0) if combined_nav_history else 0
        total_realized_pnl = sum(t.get("pnl", 0) or 0 for t in all_closed)
        total_unrealized_pnl = round(total_pnl - total_realized_pnl, 2)
        open_positions_count = sum(
            len(sr.get("open_positions", [])) for sr in sleeve_results
        )
        open_position_fraction_pct = round(
            (positions_value_at_end / last_nav * 100) if last_nav else 0, 2
        )

        def _r(v, ndigits=2):
            return None if v is None else round(v, ndigits)

        portfolio_metrics = {
            "initial_capital": initial_capital,
            "final_nav": round(last_nav, 2),
            "total_return_pct": round(total_return, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "max_drawdown_date": max_dd_date,
            # Statistical metrics — None on short windows; UI renders "—".
            "annualized_return_pct": _r(ann_return),
            "annualized_volatility_pct": _r(ann_vol),
            # `sharpe_ratio` is the basis-aware "displayed" value: period sharpe
            # for windows <252 trading days, annualized otherwise. The side
            # fields are always populated so callers can show both if needed.
            "sharpe_ratio": _r(sharpe),
            "sharpe_ratio_annualized": _r(sharpe_ann),
            "sharpe_ratio_period": _r(sharpe_period),
            "sharpe_basis": sharpe_basis,
            "sortino_ratio": _r(sortino),
            "calmar_ratio": _r(calmar),
            # Sample-size signal so consumers can render explanatory text.
            "trading_days": n_nav,
            "min_days_for_annualization": MIN_TRADING_DAYS_FOR_ANNUALIZATION,
            "stats_partial": n_nav < MIN_TRADING_DAYS_FOR_ANNUALIZATION,
            "profit_factor": profit_factor,
            "total_entries": total_entries,
            "closed_trades": closed,
            "wins": total_wins,
            "losses": total_losses,
            "win_rate_pct": win_rate,
            "avg_holding_days": avg_holding_days,
            "utilization_pct": utilization_pct,
            "peak_utilized_capital": round(peak_utilized_capital, 2),
            "avg_utilized_capital": round(avg_utilized_capital, 2),
            "return_on_utilized_capital_pct": return_on_utilized_capital_pct,
            # Realized vs unrealized split (see note above).
            "total_realized_pnl": round(total_realized_pnl, 2),
            "total_unrealized_pnl": total_unrealized_pnl,
            "positions_value_at_end": round(positions_value_at_end, 2),
            "open_positions_count": open_positions_count,
            "open_position_fraction_pct": open_position_fraction_pct,
            "trading_days": len(all_dates),
            "years": round(n_nav / 252.0, 2),
        }
    else:
        portfolio_metrics = {}

    # Per-sleeve summary with volatility, sharpe, and contribution
    per_sleeve_summary = []
    portfolio_total_return_dollar = last_nav - initial_capital if combined_nav_history else 0

    for i, sleeve in enumerate(sleeves):
        result = sleeve_results[i]
        metrics = result.get("metrics", {})

        # Compute contribution to portfolio return (weight-adjusted)
        sleeve_return_dollar = sleeve["allocated_capital"] * (metrics.get("total_return_pct", 0) / 100)
        contribution_pct = round(sleeve_return_dollar / max(initial_capital, 1) * 100, 2)

        # Compute win rate and profit factor from closed_trades (more reliable)
        sleeve_closed = result.get("closed_trades", [])
        s_wins = sum(1 for t in sleeve_closed if t.get("pnl", 0) > 0)
        s_losses = sum(1 for t in sleeve_closed if t.get("pnl", 0) <= 0)
        s_closed = len(sleeve_closed)
        s_win_rate = round(s_wins / max(s_closed, 1) * 100, 1)
        s_gp = sum(t.get("pnl", 0) for t in sleeve_closed if t.get("pnl", 0) > 0)
        s_gl = abs(sum(t.get("pnl", 0) for t in sleeve_closed if t.get("pnl", 0) < 0))
        s_pf = round(min(s_gp / max(s_gl, 0.01), 999.99), 2) if s_closed > 0 else 0

        per_sleeve_summary.append({
            "label": sleeve["label"],
            "weight": sleeve["weight"],
            "allocated_capital": sleeve["allocated_capital"],
            "regime_gate": sleeve["regime_gate"],
            "active_days": sleeve_active_days[i],
            "gated_off_days": sleeve_gated_off_days[i],
            "total_return_pct": metrics.get("total_return_pct", 0),
            "max_drawdown_pct": metrics.get("max_drawdown_pct", 0),
            # Statistical fields pass through as None on short windows
            # rather than collapsing to 0 (which would look like a real result).
            "annualized_return_pct": metrics.get("annualized_return_pct"),
            "annualized_volatility_pct": metrics.get("annualized_volatility_pct"),
            "sharpe_ratio": metrics.get("sharpe_ratio"),
            "sortino_ratio": metrics.get("sortino_ratio"),
            "total_entries": metrics.get("total_entries", 0),
            "closed_trades": s_closed,
            "wins": s_wins,
            "losses": s_losses,
            "win_rate_pct": s_win_rate,
            "profit_factor": s_pf,
            "contribution_pct": contribution_pct,
        })

    # Compute benchmark sector — infer from all sleeves' universes.
    # Uses declared sector when type='sector', or looks up symbols in
    # universe_profiles when type='symbols'. All sleeves must resolve to
    # the same sector; otherwise fall back to market benchmark only.
    bench_sector = _infer_portfolio_sector(sleeves)
    from backtest_engine import SECTOR_ETF_MAP

    ann_return = portfolio_metrics.get("annualized_return_pct")
    portfolio_total_return = portfolio_metrics.get("total_return_pct")

    def _populate_benchmark_fields(bench: dict | None, prefix: str):
        """Copy realized period metrics always; annualized alpha only when both
        portfolio and benchmark have a meaningful annualized return."""
        if not bench:
            return
        bm = bench["metrics"]
        bench_total = bm.get("total_return_pct")
        bench_ann = bm.get("annualized_return_pct")

        # Realized period metrics — always set when benchmark exists.
        portfolio_metrics[f"{prefix}_benchmark_return_pct"] = bench_total
        if portfolio_total_return is not None and bench_total is not None:
            portfolio_metrics[f"period_excess_vs_{prefix}_pct"] = round(
                portfolio_total_return - bench_total, 2)

        # Annualized stats — only when both sides cleared the sample-size gate.
        if bench_ann is not None and ann_return is not None:
            portfolio_metrics[f"alpha_vs_{prefix}_pct"] = round(ann_return - bench_ann, 2)
            portfolio_metrics[f"{prefix}_benchmark_ann_return_pct"] = bench_ann
        else:
            portfolio_metrics[f"alpha_vs_{prefix}_pct"] = None
            portfolio_metrics[f"{prefix}_benchmark_ann_return_pct"] = None

    # Market benchmark (SPY) — always compute
    print(f"\nComputing benchmark (S&P 500)...")
    market_benchmark = compute_benchmark(all_dates, initial_capital, sector=None)
    _populate_benchmark_fields(market_benchmark, "market")
    if market_benchmark:
        # Backward-compat aliases for older readers (experiments table, UIs).
        portfolio_metrics["benchmark_return_pct"] = market_benchmark["metrics"].get("total_return_pct")
        portfolio_metrics["benchmark_ann_return_pct"] = market_benchmark["metrics"].get("annualized_return_pct")
        portfolio_metrics["alpha_ann_pct"] = portfolio_metrics.get("alpha_vs_market_pct")

    # Sector benchmark — compute if single-sector portfolio
    benchmark = market_benchmark
    sector_benchmark = None
    if bench_sector and bench_sector in SECTOR_ETF_MAP:
        print(f"Computing benchmark ({SECTOR_ETF_MAP[bench_sector]})...")
        sector_benchmark = compute_benchmark(all_dates, initial_capital, sector=bench_sector)
        _populate_benchmark_fields(sector_benchmark, "sector")
        if sector_benchmark:
            benchmark = sector_benchmark

    # -----------------------------------------------------------------------
    # Step 6: Print summary
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 70}")
    print(f"PORTFOLIO RESULTS: {name}")
    print(f"{'=' * 70}")
    def _fmt(v, spec="+.2f", suffix="%"):
        return "—" if v is None else f"{v:{spec}}{suffix}"

    print(f"  Total Return:     {_fmt(portfolio_metrics.get('total_return_pct'))}")
    print(f"  Annualized:       {_fmt(portfolio_metrics.get('annualized_return_pct'))}")
    print(f"  Max Drawdown:     {_fmt(portfolio_metrics.get('max_drawdown_pct'), '.2f')}")
    print(f"  Sharpe Ratio:     {_fmt(portfolio_metrics.get('sharpe_ratio'), '.2f', '')}")
    if market_benchmark:
        print(f"  Benchmark (SPY):  {_fmt(portfolio_metrics.get('market_benchmark_return_pct'))}")
        if portfolio_metrics.get("period_excess_vs_market_pct") is not None:
            print(f"  Excess vs Market: {_fmt(portfolio_metrics.get('period_excess_vs_market_pct'))} (period)")
        if portfolio_metrics.get("alpha_vs_market_pct") is not None:
            print(f"  Alpha vs Market:  {_fmt(portfolio_metrics.get('alpha_vs_market_pct'))} (annualized)")
    if sector_benchmark:
        print(f"  Benchmark ({SECTOR_ETF_MAP.get(bench_sector, '?')}):  "
              f"{_fmt(portfolio_metrics.get('sector_benchmark_return_pct'))}")
        if portfolio_metrics.get("period_excess_vs_sector_pct") is not None:
            print(f"  Excess vs Sector: {_fmt(portfolio_metrics.get('period_excess_vs_sector_pct'))} (period)")
        if portfolio_metrics.get("alpha_vs_sector_pct") is not None:
            print(f"  Alpha vs Sector:  {_fmt(portfolio_metrics.get('alpha_vs_sector_pct'))} (annualized)")

    print(f"\n  Per-Sleeve Breakdown:")
    for s in per_sleeve_summary:
        active_pct = s["active_days"] / max(s["active_days"] + s["gated_off_days"], 1) * 100
        print(f"    {s['label']} ({s['weight']*100:.0f}%): "
              f"{s['total_return_pct']:+.1f}% total, "
              f"{s.get('closed_trades', 0)} closed ({s.get('wins',0)}W/{s.get('losses',0)}L), "
              f"PF {s.get('profit_factor', 0):.1f}, "
              f"active {active_pct:.0f}% of days")

    # Allocation profile transitions
    if allocation_profile_history:
        print(f"\n  Allocation Profile Transitions: {len(allocation_profile_history)}")
        for t in allocation_profile_history[:20]:
            w_str = ", ".join(f"{k}:{v*100:.0f}%" for k, v in t["weights"].items())
            print(f"    {t['date']}: {t['profile_name']} → {w_str}")
        if len(allocation_profile_history) > 20:
            print(f"    ... and {len(allocation_profile_history) - 20} more")

    # Regime transitions
    if regime_history:
        transitions = []
        prev_regimes = []
        for entry in regime_history:
            if entry["active_regimes"] != prev_regimes:
                transitions.append(entry)
                prev_regimes = entry["active_regimes"]
        print(f"\n  Regime Transitions: {len(transitions)}")
        for t in transitions[:20]:  # Show first 20
            label = ", ".join(t["active_regimes"]) if t["active_regimes"] else "(none)"
            print(f"    {t['date']}: {label}")
        if len(transitions) > 20:
            print(f"    ... and {len(transitions) - 20} more")

    # Build a compact smoothing_summary block from raw counters + transition log.
    # This is the diagnostic surface the auto-trader agent reads next iteration
    # to decide whether the smoothing knobs (regime persistence + asymmetric
    # transition_days) actually filtered events / shaped exposure.
    smoothing_summary = _build_smoothing_summary(
        regime_persistence_stats=regime_persistence_stats,
        allocation_profile_history=allocation_profile_history,
        sleeves=sleeves,
        transition_days_legacy=transition_days,
        transition_days_to_defensive=transition_days_to_defensive,
        transition_days_to_offensive=transition_days_to_offensive,
        n_trading_days=len(combined_nav_history),
    )

    # Merge per-sleeve rebalance trades into each sleeve_result's trade list
    # so downstream consumers (deploy_engine.persist_trades, agent history
    # context, dashboards) see them as part of the sleeve's trade log,
    # tagged with `reason` starting with "rebalance_to_". Sleeve metrics
    # (Sharpe, MDD, etc.) are unchanged — those are computed from the sleeve's
    # standalone NAV trajectory, not from its trade list.
    n_rebalance_trades = 0
    for i, sleeve_result in enumerate(sleeve_results):
        if rebalance_trades_by_sleeve[i]:
            sleeve_result.setdefault("trades", []).extend(rebalance_trades_by_sleeve[i])
            n_rebalance_trades += len(rebalance_trades_by_sleeve[i])

    rebalance_summary = {
        "rebalance_active": emit_rebalance_trades,
        "rebalance_threshold": rebalance_threshold,
        "n_rebalance_events": rebalance_event_count,
        "n_rebalance_trades": n_rebalance_trades,
        "n_drift_days_filtered": rebalance_skipped_days,  # threshold filter wins
        "cumulative_slippage_dollars": round(cumulative_rebalance_slippage, 2),
        "cumulative_slippage_pct_of_initial": (
            round(cumulative_rebalance_slippage / initial_capital * 100, 4)
            if initial_capital > 0 else 0
        ),
    }
    if emit_rebalance_trades and rebalance_event_count > 0:
        print(f"\n  Portfolio rebalances: {rebalance_event_count} events, "
              f"{n_rebalance_trades} trade records, "
              f"${cumulative_rebalance_slippage:,.2f} slippage "
              f"({rebalance_summary['cumulative_slippage_pct_of_initial']:.4f}% of initial), "
              f"{rebalance_skipped_days} days within threshold filter")

    return {
        "portfolio": name,
        "portfolio_id": compute_portfolio_id(portfolio_config),
        "run_at": datetime.now(timezone.utc).isoformat(),
        "config": portfolio_config,
        "metrics": portfolio_metrics,
        "per_sleeve": per_sleeve_summary,
        "sleeve_results": sleeve_results,
        "combined_nav_history": combined_nav_history,
        "regime_history": regime_history,
        "allocation_profile_history": allocation_profile_history,
        "smoothing_summary": smoothing_summary,
        "rebalance_summary": rebalance_summary,
        # Benchmarks
        "benchmark": benchmark,                          # legacy: primary benchmark (sector if available, else market)
        "benchmark_market": market_benchmark,            # always SPY time series + metrics
        "benchmark_sector": sector_benchmark,            # sector ETF time series (None if multi-sector)
    }


def _build_smoothing_summary(*, regime_persistence_stats, allocation_profile_history,
                             sleeves, transition_days_legacy,
                             transition_days_to_defensive, transition_days_to_offensive,
                             n_trading_days):
    """Compose a small diagnostic block summarizing the smoothing knobs' effect.

    Shape (always present; counts are zero / lists empty when smoothing is off):
        {
            "regime_persistence": {
                <regime_name>: {entry_persistence_days, exit_persistence_days,
                                raw_entry_met_days, active_days, n_activations,
                                n_deactivations, filtered_short_entry_runs}
            },
            "transition_lerp": {
                "asymmetric": bool, "transition_days_legacy": int,
                "transition_days_to_defensive": int|None,
                "transition_days_to_offensive": int|None,
                "n_transitions_total": int,
                "n_transitions_defensive": int,
                "n_transitions_offensive": int,
                "n_transitions_lateral": int,
                "lerp_days_active": int,
                "lerp_days_settled": int,
            }
        }
    Direction is inferred from each transition event's profile target weights.
    """
    # ----- Regime persistence block -----
    regime_block = {}
    for name, st in (regime_persistence_stats or {}).items():
        regime_block[name] = {
            "entry_persistence_days": st.get("entry_persistence_days", 1),
            "exit_persistence_days": st.get("exit_persistence_days", 1),
            "raw_entry_met_days": st.get("raw_entry_met_days", 0),
            "active_days": st.get("active_days", 0),
            "n_activations": st.get("n_activations", 0),
            "n_deactivations": st.get("n_deactivations", 0),
            "filtered_short_entry_runs": st.get("filtered_short_entry_runs", 0),
        }

    # ----- Transition lerp block -----
    n_def = n_off = n_lat = 0
    lerp_days_active = 0
    asymmetric = (transition_days_to_defensive is not None
                  or transition_days_to_offensive is not None)

    def _equity(weights):
        return sum(w for k, w in (weights or {}).items() if k.lower() != "cash")

    prev_target_weights = None
    for ev in (allocation_profile_history or []):
        target_weights = ev.get("weights") or {}
        from_weights = ev.get("from_weights") or prev_target_weights
        if from_weights is not None:
            eq_from = _equity(from_weights)
            eq_to = _equity(target_weights)
            if eq_to < eq_from:
                n_def += 1
            elif eq_to > eq_from:
                n_off += 1
            else:
                n_lat += 1
        # Sum lerp days from "gradual over N days" markers.
        tdesc = ev.get("transition") or ""
        if "gradual over" in tdesc:
            try:
                lerp_days_active += int(tdesc.split()[2])
            except (IndexError, ValueError):
                pass
        prev_target_weights = target_weights

    transition_block = {
        "asymmetric": asymmetric,
        "transition_days_legacy": transition_days_legacy,
        "transition_days_to_defensive": transition_days_to_defensive,
        "transition_days_to_offensive": transition_days_to_offensive,
        "n_transitions_total": len(allocation_profile_history or []),
        "n_transitions_defensive": n_def,
        "n_transitions_offensive": n_off,
        "n_transitions_lateral": n_lat,
        "lerp_days_active": min(lerp_days_active, n_trading_days),
        "lerp_days_settled": max(0, n_trading_days - lerp_days_active),
    }

    return {
        "regime_persistence": regime_block,
        "transition_lerp": transition_block,
    }


# ---------------------------------------------------------------------------
# DB Schema for portfolio backtest runs
# ---------------------------------------------------------------------------
PORTFOLIO_BACKTEST_SCHEMA = """
CREATE TABLE IF NOT EXISTS portfolio_backtest_runs (
    run_id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL,
    portfolio_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    initial_capital REAL NOT NULL,
    final_nav REAL,
    total_return_pct REAL,
    annualized_return_pct REAL,
    annualized_volatility_pct REAL,
    max_drawdown_pct REAL,
    max_drawdown_date TEXT,
    sharpe_ratio REAL,
    sortino_ratio REAL,
    calmar_ratio REAL,
    profit_factor REAL,
    total_entries INTEGER,
    closed_trades INTEGER,
    wins INTEGER,
    losses INTEGER,
    win_rate_pct REAL,
    avg_holding_days REAL,
    utilization_pct REAL,
    trading_days INTEGER,
    benchmark_return_pct REAL,
    alpha_ann_pct REAL,
    regime_transitions INTEGER,
    num_sleeves INTEGER,
    per_sleeve_json TEXT,
    config_json TEXT,
    results_path TEXT
);
CREATE INDEX IF NOT EXISTS idx_pbt_portfolio_id ON portfolio_backtest_runs(portfolio_id);
CREATE INDEX IF NOT EXISTS idx_pbt_created_at ON portfolio_backtest_runs(created_at);
"""


def _ensure_portfolio_backtest_table():
    from schema import init_db
    conn = sqlite3.connect(str(APP_DB_PATH))
    init_db(conn)
    conn.close()


# ---------------------------------------------------------------------------
# Save results
# ---------------------------------------------------------------------------
def save_portfolio_results(result: dict, output_dir: str = None) -> Path:
    """Save portfolio backtest results to JSON file + DB row."""
    if output_dir is None:
        output_dir = WORKSPACE / "backtest" / "portfolio_results"
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pid = result["portfolio_id"]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"portfolio_{pid}_{ts}.json"
    path = output_dir / filename
    run_id = path.stem

    # Strip large nav histories from sleeve_results to save space
    slim = deepcopy(result)
    for sr in slim.get("sleeve_results", []):
        sr.pop("nav_history", None)
        sr.pop("benchmark", None)

    path.write_text(json.dumps(slim, indent=2, default=str))
    print(f"\nResults saved to: {path}")

    # Save to DB
    m = result.get("metrics", {})
    config = result.get("config", {})
    bt = config.get("backtest", {})
    regime_history = result.get("regime_history", [])
    regime_transitions = sum(
        1 for i in range(1, len(regime_history))
        if regime_history[i]["active_regimes"] != regime_history[i - 1]["active_regimes"]
    ) if len(regime_history) > 1 else 0

    try:
        _ensure_portfolio_backtest_table()
        conn = sqlite3.connect(str(APP_DB_PATH))
        conn.execute("""
            INSERT OR REPLACE INTO portfolio_backtest_runs (
                run_id, portfolio_id, portfolio_name, created_at,
                start_date, end_date, initial_capital,
                final_nav, total_return_pct, annualized_return_pct,
                annualized_volatility_pct, max_drawdown_pct, max_drawdown_date,
                sharpe_ratio, sortino_ratio, calmar_ratio, profit_factor,
                total_entries, closed_trades, wins, losses, win_rate_pct,
                avg_holding_days, utilization_pct, trading_days,
                benchmark_return_pct, alpha_ann_pct,
                regime_transitions, num_sleeves, per_sleeve_json,
                config_json, results_path
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_id, pid, result.get("portfolio", ""),
            result.get("run_at", datetime.now(timezone.utc).isoformat()),
            bt.get("start", ""), bt.get("end", ""), bt.get("initial_capital", 0),
            m.get("final_nav"), m.get("total_return_pct"), m.get("annualized_return_pct"),
            m.get("annualized_volatility_pct"), m.get("max_drawdown_pct"), m.get("max_drawdown_date"),
            m.get("sharpe_ratio"), m.get("sortino_ratio"), m.get("calmar_ratio"), m.get("profit_factor"),
            m.get("total_entries"), m.get("closed_trades"), m.get("wins"), m.get("losses"),
            m.get("win_rate_pct"), m.get("avg_holding_days"), m.get("utilization_pct"),
            m.get("trading_days"), m.get("benchmark_return_pct"), m.get("alpha_ann_pct"),
            regime_transitions, len(result.get("per_sleeve", [])),
            json.dumps(result.get("per_sleeve", []), default=str),
            json.dumps(config, default=str), str(path),
        ))
        conn.commit()
        conn.close()
        print(f"  → DB row saved: {run_id}")
    except Exception as e:
        print(f"  ⚠ DB save failed: {e}")

    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="AlphaScout Portfolio Backtest")
    parser.add_argument("config", help="Path to portfolio config JSON")
    parser.add_argument("--start", help="Override backtest start date")
    parser.add_argument("--end", help="Override backtest end date")
    parser.add_argument("--capital", type=float, help="Override initial capital")
    parser.add_argument("--save", action="store_true", help="Save results to file")

    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text())

    if args.start:
        config["backtest"]["start"] = args.start
    if args.end:
        config["backtest"]["end"] = args.end
    if args.capital:
        config["backtest"]["initial_capital"] = args.capital

    result = run_portfolio_backtest(config)

    if args.save:
        saved_path = save_portfolio_results(result)

        # Persist trades per sleeve to DB (single source of truth)
        try:
            from deploy_engine import persist_trades
            run_id = saved_path.stem
            sleeve_results = result.get("sleeve_results", [])
            per_sleeve = result.get("per_sleeve", [])
            total_persisted = 0
            for i, sr in enumerate(sleeve_results):
                label = per_sleeve[i].get("label") if i < len(per_sleeve) else f"sleeve_{i}"
                sleeve_trades = sr.get("trades", [])
                if sleeve_trades:
                    n = persist_trades("backtest", run_id, sleeve_trades,
                                       deployment_type="portfolio",
                                       sleeve_label=label)
                    total_persisted += n
            if total_persisted:
                print(f"  💾 {total_persisted} trade(s) persisted to DB across {len(sleeve_results)} sleeves")

            # Persist sleeve-level data
            from deploy_engine import persist_sleeves
            portfolio_id = result.get("portfolio_id")
            n_sleeves = persist_sleeves("backtest", run_id, result,
                                        portfolio_id=portfolio_id)
            if n_sleeves:
                print(f"  📊 {n_sleeves} sleeve(s) persisted to DB")
        except Exception as e:
            print(f"  ⚠ Trade/sleeve persist failed: {e}")


if __name__ == "__main__":
    main()
