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
from regime import evaluate_regime_series

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
from db_config import MARKET_DB_PATH as DB_PATH, APP_DB_PATH
WORKSPACE = Path(os.environ.get("WORKSPACE", "/app"))
STRATEGIES_DIR = WORKSPACE / "strategies"


def get_config_schema() -> dict:
    """
    Authoritative portfolio config schema.
    
    Usage:
        python3 -c "from portfolio_engine import get_config_schema; import json; print(json.dumps(get_config_schema(), indent=2))"
    """
    return {
        "name": {"type": "string", "required": True, "description": "Portfolio name"},
        "sleeves": {
            "type": "array",
            "required": True,
            "description": "Array of sleeve objects. Each sleeve wraps a strategy with a capital weight and optional regime gates.",
            "item_schema": {
                "name": {"type": "string", "required": True, "description": "Sleeve display name"},
                "weight": {"type": "float", "required": True, "description": "Capital allocation weight. All sleeve weights must sum to 1.0."},
                "regime_gates": {"type": "string[]", "default": [], "description": "List of regime_ids. Empty = always active. Non-empty = active only when ≥1 gated regime is on."},
                "strategy_id": {"type": "string", "description": "Reference an existing strategy by ID (preferred). Looks up in strategies/ dir."},
                "config_path": {"type": "string", "description": "Path to strategy config JSON file. Validated on load."},
                "strategy_config": {"type": "object", "description": "Inline strategy config. Validated against strategy schema, auto-saved to strategies/ with a strategy_id. Use strategy_id instead when possible."},
            },
        },
        "regime_definitions": {
            "optional": True,
            "description": "Inline regime configs keyed by regime_id. Each has conditions array + logic. Alternative: regimes already in DB are auto-loaded by ID.",
            "item_schema": {
                "conditions": {"type": "array", "description": "Array of {series, operator, threshold}. series = any key from macro_indicators or macro_derived tables."},
                "logic": {"type": "string", "values": ["and", "or"], "default": "and"},
            },
        },
        "capital_when_gated_off": {
            "type": "string",
            "values": ["to_cash", "redistribute"],
            "default": "to_cash",
            "description": "'to_cash' = park gated-off capital as cash. 'redistribute' = proportionally allocate to active sleeves.",
        },
        "allocation_profiles": {
            "optional": True,
            "description": "Named weight sets for dynamic allocation. Each profile (except 'default') has a 'trigger' array of regime_ids — all must be active. Keys are sleeve names, values are weights. Include 'Cash' key for unallocated.",
            "example": {
                "default": {"Tech": 0.60, "Defensives": 0.40},
                "oil_shock": {"trigger": ["oil_crisis_301db3ee"], "Tech": 0.30, "Defensives": 0.50, "Cash": 0.20},
            },
        },
        "profile_priority": {
            "optional": True,
            "type": "string[]",
            "description": "Ordered list of profile names. Walk top→bottom, first profile whose trigger regimes are ALL active wins. Must end with 'default'.",
        },
        "transition_days": {
            "optional": True,
            "type": "integer",
            "default": 1,
            "description": "Number of trading days to linearly transition between allocation profiles. 1 = instant (legacy). Higher values smooth rebalancing over N days, preventing equity curve jumps.",
        },
        "backtest": {
            "start": {"type": "string", "required": True, "description": "YYYY-MM-DD"},
            "end": {"type": "string", "required": True, "description": "YYYY-MM-DD"},
        },
    }


def compute_portfolio_id(config: dict) -> str:
    """Deterministic ID from portfolio config."""
    key_parts = {
        "strategies": config.get("strategies", []),
        "regime_filter": config.get("regime_filter", False),
        "capital_flow": config.get("capital_flow", "to_cash"),
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
    transition_days = max(1, portfolio_config.get("transition_days", 1))

    print(f"=" * 70)
    print(f"PORTFOLIO BACKTEST: {name}")
    print(f"Capital: ${initial_capital:,.0f} | Period: {bt_start} to {bt_end}")
    print(f"Regime filter: {'ON' if regime_enabled else 'OFF'} | Capital flow: {capital_flow} | Transition: {transition_days}d")
    print(f"=" * 70)

    # -----------------------------------------------------------------------
    # Step 1: Resolve all strategy configs and set their capital allocation
    # -----------------------------------------------------------------------
    sleeves = []
    total_weight = sum(s["weight"] for s in strategies_refs)
    if abs(total_weight - 1.0) > 0.01:
        print(f"WARNING: Strategy weights sum to {total_weight:.2f}, not 1.0. Normalizing.")
        for s in strategies_refs:
            s["weight"] = s["weight"] / total_weight

    for i, ref in enumerate(strategies_refs):
        config = _resolve_strategy_config(ref)
        label = ref.get("label", config.get("name", f"Strategy {i+1}"))
        weight = ref["weight"]
        regime_gate = ref.get("regime_gate", ["*"])
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
            import sqlite3 as _sqlite3
            _app_conn = _sqlite3.connect(str(APP_DB_PATH))
            _app_conn.row_factory = _sqlite3.Row
            regime_configs, regime_id_to_name = _load_regime_configs(list(all_regime_ids), _app_conn)
            _app_conn.close()

            print(f"  Computing regime series {bt_start} to {bt_end}...")
            regime_series = evaluate_regime_series(bt_start, bt_end, regime_configs)
            
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
    # Step 2.5: Build shared price index for all sleeve universes
    # -----------------------------------------------------------------------
    all_sleeve_symbols = set()
    conn = get_connection()
    for sleeve in sleeves:
        sleeve_symbols = resolve_universe(sleeve["config"], conn)
        all_sleeve_symbols.update(sleeve_symbols)
    # Include benchmark ticker
    all_sleeve_symbols.add("SPY")

    print(f"\n  Building shared price index for {len(all_sleeve_symbols)} tickers...")
    shared_price_index, shared_trading_dates = build_price_index(list(all_sleeve_symbols), conn)
    conn.close()
    shared_pi = (shared_price_index, shared_trading_dates)

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
                              shared_price_index=shared_pi)
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

    # Build per-sleeve NAV and positions_value lookups
    sleeve_nav_lookup = []
    sleeve_pv_lookup = []
    for result in sleeve_results:
        nav_map = {entry["date"]: entry["nav"] for entry in result["nav_history"]}
        pv_map = {entry["date"]: entry.get("positions_value", 0) for entry in result["nav_history"]}
        sleeve_nav_lookup.append(nav_map)
        sleeve_pv_lookup.append(pv_map)

    # Compute combined NAV with regime gating and dynamic allocation
    combined_nav_history = []
    regime_history = []
    allocation_profile_history = []
    sleeve_active_days = [0] * len(sleeves)
    sleeve_gated_off_days = [0] * len(sleeves)
    prev_profile_name = None

    # --- Per-sleeve adjusted NAV ---
    # Sleeve backtests run independently (without regime awareness). Instead of
    # tracking "phantom gains" to subtract later, we track an adjusted NAV per
    # sleeve that only compounds daily returns when the sleeve is active.
    # When gated off, the adjusted NAV freezes (to_cash) or moves to a
    # redistribute balance pool.
    n_sleeves = len(sleeves)
    sleeve_adj_nav = [s["allocated_capital"] for s in sleeves]
    sleeve_frozen_capital = [0.0] * n_sleeves  # capital put into redistribute at gateoff

    # --- Redistribute tracking ---
    redistribute_balance = 0.0
    prev_gate_status = None

    # Helper: resolve active allocation profile for a given date
    def _resolve_profile(active_regimes_today):
        """Walk profile_priority, return (profile_name, weights_dict)."""
        if not allocation_profiles or not profile_priority:
            return None, None
        for pname in profile_priority:
            if pname == "default":
                return "default", allocation_profiles.get("default", {})
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
            return "default", allocation_profiles.get("default", {})
        return None, None

    all_dates_index = {d: idx for idx, d in enumerate(all_dates)}

    # --- Transition state for gradual profile switches ---
    # When transition_days > 1, we lerp weights from old profile to new over N days.
    transition_state = {
        "active": False,          # currently transitioning?
        "from_weights": None,     # {label: weight} at transition start
        "to_weights": None,       # {label: weight} target
        "start_idx": 0,           # index in all_dates where transition began
        "end_idx": 0,             # index where transition completes
        "target_profile": None,   # name of target profile
    }

    def _get_effective_weights(date_idx, target_profile_name, target_weights, sleeve_labels):
        """
        Return effective weights for today, accounting for in-progress transitions.
        Updates transition_state as a side effect.
        """
        nonlocal prev_profile_name

        if transition_days <= 1:
            # Instant mode (legacy behavior)
            if target_profile_name != prev_profile_name:
                prev_profile_name = target_profile_name
                allocation_profile_history.append({
                    "date": all_dates[date_idx],
                    "profile_name": target_profile_name,
                    "weights": target_weights,
                })
            return target_weights

        # Check if target profile changed (new transition needed)
        if target_profile_name != transition_state.get("target_profile"):
            # Determine starting weights: if mid-transition, use current interpolated weights
            if transition_state["active"]:
                progress = min(1.0, (date_idx - transition_state["start_idx"]) / max(transition_days, 1))
                from_w = transition_state["from_weights"]
                to_w = transition_state["to_weights"]
                current_weights = {}
                for lbl in sleeve_labels:
                    w0 = from_w.get(lbl, 0.0)
                    w1 = to_w.get(lbl, 0.0)
                    current_weights[lbl] = w0 + (w1 - w0) * progress
            elif prev_profile_name and allocation_profiles:
                # Use the previous settled profile's weights
                prev_def = allocation_profiles.get(prev_profile_name, {})
                if prev_profile_name == "default":
                    current_weights = {lbl: prev_def.get(lbl, 0.0) for lbl in sleeve_labels}
                else:
                    pw = prev_def.get("weights", prev_def)
                    current_weights = {lbl: pw.get(lbl, 0.0) for lbl in sleeve_labels}
            else:
                # First profile — no transition, apply directly
                prev_profile_name = target_profile_name
                allocation_profile_history.append({
                    "date": all_dates[date_idx],
                    "profile_name": target_profile_name,
                    "weights": target_weights,
                    "transition": "instant (initial)",
                })
                transition_state["target_profile"] = target_profile_name
                transition_state["active"] = False
                return target_weights

            transition_state["active"] = True
            transition_state["from_weights"] = current_weights
            transition_state["to_weights"] = target_weights
            transition_state["start_idx"] = date_idx
            transition_state["end_idx"] = date_idx + transition_days
            transition_state["target_profile"] = target_profile_name

            allocation_profile_history.append({
                "date": all_dates[date_idx],
                "profile_name": target_profile_name,
                "weights": target_weights,
                "transition": f"gradual over {transition_days} days",
                "from_weights": current_weights,
            })
            prev_profile_name = target_profile_name

        # Compute interpolated weights
        if transition_state["active"]:
            elapsed = date_idx - transition_state["start_idx"]
            progress = min(1.0, elapsed / transition_days)

            if progress >= 1.0:
                transition_state["active"] = False
                return transition_state["to_weights"]

            from_w = transition_state["from_weights"]
            to_w = transition_state["to_weights"]
            blended = {}
            for lbl in sleeve_labels:
                w0 = from_w.get(lbl, 0.0)
                w1 = to_w.get(lbl, 0.0)
                blended[lbl] = w0 + (w1 - w0) * progress
            return blended

        return target_weights

    sleeve_labels = [s["label"] for s in sleeves]

    # --- Incremental NAV for dynamic allocation profiles ---
    # When allocation_profiles are active, we can't sum sleeve NAVs directly
    # because sleeves run with fixed capital but profiles change weights.
    # Instead, track portfolio NAV incrementally: each day's return is the
    # weighted average of sleeve daily returns according to the current profile.
    incremental_nav = initial_capital  # running portfolio NAV for dynamic mode

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

        # Determine gate status per sleeve
        gate_status = []
        for i, sleeve in enumerate(sleeves):
            gate = sleeve["regime_gate"]
            is_gated_on = False
            if not regime_enabled or gate == ["*"]:
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

        # --- Handle gate transitions ---
        if prev_gate_status is not None:
            for i in range(n_sleeves):
                if prev_gate_status[i] and not gate_status[i]:
                    # Sleeve just gated OFF — freeze its adjusted NAV
                    if use_redistribute:
                        sleeve_frozen_capital[i] = sleeve_adj_nav[i]
                        redistribute_balance += sleeve_adj_nav[i]
                elif not prev_gate_status[i] and gate_status[i]:
                    # Sleeve just gated ON — restore frozen capital
                    if use_redistribute and sleeve_frozen_capital[i] > 0:
                        redistribute_balance = max(0, redistribute_balance - sleeve_frozen_capital[i])
                        sleeve_frozen_capital[i] = 0.0
        else:
            # First day: sleeves that start gated off
            for i in range(n_sleeves):
                if not gate_status[i] and use_redistribute:
                    sleeve_frozen_capital[i] = sleeve_adj_nav[i]
                    redistribute_balance += sleeve_adj_nav[i]

        # --- Compound active sleeves' adjusted NAV ---
        for i in range(n_sleeves):
            if gate_status[i] and prev_date is not None:
                daily_ret = _sleeve_daily_return(i, date, prev_date)
                sleeve_adj_nav[i] *= (1 + daily_ret)
            # If gated off: sleeve_adj_nav[i] stays frozen

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
            # FIXED WEIGHT MODE: sum of per-sleeve adjusted NAVs
            for i, sleeve in enumerate(sleeves):
                if gate_status[i]:
                    combined_nav += sleeve_adj_nav[i]
                    sleeve_active_days[i] += 1
                else:
                    if not use_redistribute:
                        # to_cash: frozen capital still counts toward NAV
                        combined_nav += sleeve_adj_nav[i]
                    # redistribute: gated-off capital is in redistribute_balance
                    sleeve_gated_off_days[i] += 1

                # Compute positions_value: scale raw PV by adj_nav/raw_nav ratio
                raw_nav = sleeve_nav_lookup[i].get(date, sleeves[i]["allocated_capital"])
                raw_pv = sleeve_pv_lookup[i].get(date, 0)
                sleeve_pv = sleeve_adj_nav[i] * (raw_pv / raw_nav) if raw_nav > 0 and gate_status[i] else 0
                sleeve_navs.append({
                    "label": sleeve["label"],
                    "nav": round(sleeve_adj_nav[i], 2),
                    "positions_value": round(sleeve_pv, 2),
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

        # Annualized return
        days = (datetime.strptime(all_dates[-1], "%Y-%m-%d") -
                datetime.strptime(all_dates[0], "%Y-%m-%d")).days
        years = max(days / 365.25, 0.01)
        ann_return = ((last_nav / initial_capital) ** (1 / years) - 1) * 100

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

        # Sharpe = (annualized return - risk-free rate) / annualized volatility
        if len(combined_nav_history) > 1:
            daily_returns = []
            for j in range(1, len(combined_nav_history)):
                prev = combined_nav_history[j-1]["nav"]
                curr = combined_nav_history[j]["nav"]
                if prev > 0:
                    daily_returns.append(curr / prev - 1)
            if daily_returns:
                import statistics
                import math
                ann_vol = statistics.stdev(daily_returns) * (252 ** 0.5) * 100 if len(daily_returns) > 1 else 0
                excess_return = ann_return - risk_free_ann
                sharpe = excess_return / ann_vol if ann_vol > 0 else 0
            else:
                sharpe = 0
                ann_vol = 0
        else:
            sharpe = 0
            ann_vol = 0
            daily_returns = []

        # Sortino = (annualized return - risk-free rate) / annualized downside deviation
        # Downside deviation: sqrt(mean([min(r - daily_rf, 0)^2 for ALL r])) * sqrt(252)
        if daily_returns:
            daily_rf = risk_free_ann / 100 / 252  # annual % to daily decimal
            downside_sq = [min(r - daily_rf, 0) ** 2 for r in daily_returns]
            downside_dev = math.sqrt(sum(downside_sq) / len(downside_sq)) * math.sqrt(252) * 100
            excess_return = ann_return - risk_free_ann
            sortino = excess_return / downside_dev if downside_dev > 0 else 0
        else:
            sortino = 0

        # Calmar ratio (ann return / max DD)
        calmar = abs(ann_return / max_dd) if max_dd < 0 else 0

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

        portfolio_metrics = {
            "initial_capital": initial_capital,
            "final_nav": round(last_nav, 2),
            "total_return_pct": round(total_return, 2),
            "annualized_return_pct": round(ann_return, 2),
            "annualized_volatility_pct": round(ann_vol, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "max_drawdown_date": max_dd_date,
            "sharpe_ratio": round(sharpe, 2),
            "sortino_ratio": round(sortino, 2),
            "calmar_ratio": round(calmar, 2),
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
            "trading_days": len(all_dates),
            "years": round(years, 2),
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
            "annualized_return_pct": metrics.get("annualized_return_pct", 0),
            "annualized_volatility_pct": metrics.get("annualized_volatility_pct", 0),
            "max_drawdown_pct": metrics.get("max_drawdown_pct", 0),
            "sharpe_ratio": metrics.get("sharpe_ratio", 0),
            "sortino_ratio": metrics.get("sortino_ratio", 0),
            "total_entries": metrics.get("total_entries", 0),
            "closed_trades": s_closed,
            "wins": s_wins,
            "losses": s_losses,
            "win_rate_pct": s_win_rate,
            "profit_factor": s_pf,
            "contribution_pct": contribution_pct,
        })

    # Compute benchmark — use sector ETF for single-sleeve, SPX for multi-sleeve
    bench_sector = None
    if len(sleeves) == 1:
        bench_sector = sleeves[0]["config"].get("universe", {}).get("sector")
    from backtest_engine import SECTOR_ETF_MAP

    ann_return = portfolio_metrics.get("annualized_return_pct", 0)

    # Market benchmark (SPY) — always compute
    print(f"\nComputing benchmark (S&P 500)...")
    market_benchmark = compute_benchmark(all_dates, initial_capital, sector=None)
    if market_benchmark:
        market_ann = market_benchmark["metrics"]["annualized_return_pct"]
        portfolio_metrics["alpha_vs_market_pct"] = round(ann_return - market_ann, 2)
        portfolio_metrics["market_benchmark_return_pct"] = market_benchmark["metrics"]["total_return_pct"]
        # Keep backward compat fields
        portfolio_metrics["benchmark_return_pct"] = market_benchmark["metrics"]["total_return_pct"]
        portfolio_metrics["benchmark_ann_return_pct"] = market_benchmark["metrics"]["annualized_return_pct"]
        portfolio_metrics["alpha_ann_pct"] = portfolio_metrics["alpha_vs_market_pct"]

    # Sector benchmark — compute if single-sector portfolio
    benchmark = market_benchmark
    if bench_sector and bench_sector in SECTOR_ETF_MAP:
        print(f"Computing benchmark ({SECTOR_ETF_MAP[bench_sector]})...")
        sector_benchmark = compute_benchmark(all_dates, initial_capital, sector=bench_sector)
        if sector_benchmark:
            sector_ann = sector_benchmark["metrics"]["annualized_return_pct"]
            portfolio_metrics["alpha_vs_sector_pct"] = round(ann_return - sector_ann, 2)
            portfolio_metrics["sector_benchmark_return_pct"] = sector_benchmark["metrics"]["total_return_pct"]
            benchmark = sector_benchmark  # use sector as primary for display

    # -----------------------------------------------------------------------
    # Step 6: Print summary
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 70}")
    print(f"PORTFOLIO RESULTS: {name}")
    print(f"{'=' * 70}")
    print(f"  Total Return:     {portfolio_metrics.get('total_return_pct', 0):+.2f}%")
    print(f"  Annualized:       {portfolio_metrics.get('annualized_return_pct', 0):+.2f}%")
    print(f"  Max Drawdown:     {portfolio_metrics.get('max_drawdown_pct', 0):.2f}%")
    print(f"  Sharpe Ratio:     {portfolio_metrics.get('sharpe_ratio', 0):.2f}")
    if market_benchmark:
        print(f"  Benchmark (SPY):  {portfolio_metrics.get('market_benchmark_return_pct', 0):+.2f}%")
        print(f"  Alpha vs Market:  {portfolio_metrics.get('alpha_vs_market_pct', 0):+.2f}%")
    if portfolio_metrics.get("alpha_vs_sector_pct") is not None:
        print(f"  Benchmark ({SECTOR_ETF_MAP.get(bench_sector, '?')}):  {portfolio_metrics.get('sector_benchmark_return_pct', 0):+.2f}%")
        print(f"  Alpha vs Sector:  {portfolio_metrics.get('alpha_vs_sector_pct', 0):+.2f}%")

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
        "benchmark": benchmark,
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
    conn = sqlite3.connect(str(APP_DB_PATH))
    conn.executescript(PORTFOLIO_BACKTEST_SCHEMA)
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
