#!/usr/bin/env python3
"""
AlphaScout Regime Detector
===========================
Config-driven macro regime classifier. Regimes are user-defined with arbitrary
names, conditions on any macro_indicators/macro_derived series, and AND/OR logic.

Usage as library:
    from regime import evaluate_regimes, evaluate_regime_series

    regimes_config = [
        {
            "name": "oil_shock",
            "conditions": [
                {"series": "brent_vs_50dma_pct", "operator": ">", "value": 30},
                {"series": "vix", "operator": ">", "value": 25}
            ],
            "logic": "all"
        }
    ]

    active = evaluate_regimes("2026-03-28", regimes_config)
    # -> ["oil_shock"]

    series = evaluate_regime_series("2026-01-01", "2026-03-28", regimes_config)
    # -> {"2026-03-28": ["oil_shock"], "2026-03-27": ["oil_shock"], ...}

Usage as CLI:
    python3 regime.py evaluate --config regime_config.json --date 2026-03-28
    python3 regime.py series --config regime_config.json --start 2026-01-01 --end 2026-03-28
    python3 regime.py series --config regime_config.json --start 2026-01-01 --end 2026-03-28 --csv
"""

import os
import sqlite3
import argparse
import json
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
from db_config import MARKET_DB_PATH as DB_PATH

OPERATORS = {
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


def get_connection():
    return sqlite3.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# Core: load macro values for a date
# ---------------------------------------------------------------------------
def _load_macro_values(date: str, series_keys: list[str], conn) -> dict:
    """
    Load macro values for a specific date. For each series, returns the most
    recent value on or before `date` (handles weekends/holidays and monthly series).

    Returns: {series_key: value} — missing series have value None.
    """
    result = {}
    cur = conn.cursor()
    for key in series_keys:
        # Try macro_indicators first, then macro_derived
        for table in ("macro_indicators", "macro_derived"):
            cur.execute(
                f"SELECT value FROM {table} WHERE series = ? AND date <= ? ORDER BY date DESC LIMIT 1",
                (key, date),
            )
            row = cur.fetchone()
            if row is not None:
                result[key] = row[0]
                break
        else:
            result[key] = None
    return result


def _load_macro_values_bulk(dates: list[str], series_keys: list[str], conn) -> dict:
    """
    Bulk-load macro values for many dates. More efficient than calling
    _load_macro_values per date.

    Returns: {date: {series_key: value}}
    """
    if not dates or not series_keys:
        return {}

    cur = conn.cursor()

    # Load full time series for each key, then do point-in-time lookups
    series_data = {}  # {key: [(date, value), ...]} sorted by date
    for key in series_keys:
        for table in ("macro_indicators", "macro_derived"):
            cur.execute(
                f"SELECT date, value FROM {table} WHERE series = ? ORDER BY date ASC",
                (key,),
            )
            rows = cur.fetchall()
            if rows:
                series_data[key] = rows
                break
        else:
            series_data[key] = []

    # For each date, binary search for most recent value <= date
    import bisect
    result = {}
    for date in dates:
        values = {}
        for key in series_keys:
            data = series_data[key]
            if not data:
                values[key] = None
                continue
            # bisect on date strings (ISO format sorts correctly)
            idx = bisect.bisect_right([r[0] for r in data], date) - 1
            if idx >= 0:
                values[key] = data[idx][1]
            else:
                values[key] = None
        result[date] = values
    return result


# ---------------------------------------------------------------------------
# Evaluate conditions
# ---------------------------------------------------------------------------
def _evaluate_condition(condition: dict, values: dict) -> bool:
    """Evaluate a single condition against macro values."""
    series = condition["series"]
    operator = condition["operator"]
    threshold = condition["value"]

    actual = values.get(series)
    if actual is None:
        return False  # missing data = condition not met

    op_fn = OPERATORS.get(operator)
    if op_fn is None:
        raise ValueError(f"Unknown operator: {operator}. Valid: {list(OPERATORS.keys())}")

    return op_fn(actual, threshold)


def _evaluate_conditions(conditions: list[dict], logic: str, values: dict) -> bool:
    """Evaluate a list of conditions with AND/OR logic."""
    if not conditions:
        return False

    results = [_evaluate_condition(c, values) for c in conditions]

    if logic == "all":
        return all(results)
    elif logic == "any":
        return any(results)
    else:
        raise ValueError(f"Unknown logic: {logic}. Valid: 'all', 'any'")


def _evaluate_regime(regime_config: dict, values: dict) -> bool:
    """Evaluate whether a single regime's ENTRY conditions are met (stateless check)."""
    # Support both old format (conditions) and new format (entry_conditions)
    conditions = regime_config.get("entry_conditions", regime_config.get("conditions", []))
    logic = regime_config.get("entry_logic", regime_config.get("logic", "all"))
    return _evaluate_conditions(conditions, logic, values)


def _evaluate_regime_exit(regime_config: dict, values: dict) -> bool:
    """Evaluate whether a regime's EXIT conditions are met."""
    exit_conditions = regime_config.get("exit_conditions", [])
    if not exit_conditions:
        # No explicit exit conditions — fall back to "exit when entry conditions no longer met"
        return not _evaluate_regime(regime_config, values)

    exit_logic = regime_config.get("exit_logic", "any")
    return _evaluate_conditions(exit_conditions, exit_logic, values)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _collect_series_keys(regime_configs: list[dict]) -> set:
    """Collect all macro series keys needed across all regime configs."""
    keys = set()
    for rc in regime_configs:
        for cond in rc.get("entry_conditions", rc.get("conditions", [])):
            keys.add(cond["series"])
        for cond in rc.get("exit_conditions", []):
            keys.add(cond["series"])
    return keys

def evaluate_regimes(date: str, regime_configs: list[dict], conn=None) -> list[str]:
    """
    Evaluate all regime configs for a single date.

    Args:
        date: YYYY-MM-DD
        regime_configs: list of regime config dicts with name, conditions, logic
        conn: optional SQLite connection

    Returns:
        List of active regime names.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    all_keys = _collect_series_keys(regime_configs)
    values = _load_macro_values(date, list(all_keys), conn)

    if own_conn:
        conn.close()

    active = []
    for rc in regime_configs:
        if _evaluate_regime(rc, values):
            active.append(rc["name"])

    return active


def evaluate_regime_series(start: str, end: str, regime_configs: list[dict],
                           conn=None) -> dict:
    """
    Evaluate all regime configs for every trading date in range.
    Uses stateful evaluation with entry/exit conditions and cooldown.

    Regime config supports:
        - "conditions" + "logic" (legacy: symmetric entry=exit, no cooldown)
        - "entry_conditions" + "entry_logic" (new: explicit entry)
        - "exit_conditions" + "exit_logic" (new: explicit exit, defaults to inverse of entry)
        - "min_hold_days" (new: minimum trading days before exit conditions are checked)

    State machine per regime:
        inactive → (entry conditions met) → active_cooldown
        active_cooldown → (cooldown elapsed) → active_monitoring
        active_monitoring → (exit conditions met) → inactive

    Args:
        start: start date YYYY-MM-DD
        end: end date YYYY-MM-DD
        regime_configs: list of regime config dicts

    Returns:
        dict of {date: [active_regime_names]}
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    # Get trading dates from prices table (AAPL as proxy for trading calendar)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT date FROM prices WHERE symbol = 'AAPL' AND date >= ? AND date <= ? ORDER BY date",
        (start, end),
    )
    dates = [r[0] for r in cur.fetchall()]

    if not dates:
        if own_conn:
            conn.close()
        return {}

    # Collect all series keys needed
    all_keys = set()
    for rc in regime_configs:
        for cond in rc.get("entry_conditions", rc.get("conditions", [])):
            all_keys.add(cond["series"])
        for cond in rc.get("exit_conditions", []):
            all_keys.add(cond["series"])

    # Bulk load
    all_values = _load_macro_values_bulk(dates, list(all_keys), conn)

    if own_conn:
        conn.close()

    # State machine per regime: track activation date and state
    # States: "inactive", "cooldown", "monitoring"
    regime_states = {}
    for rc in regime_configs:
        name = rc["name"]
        regime_states[name] = {
            "state": "inactive",
            "activated_day_idx": None,  # index into dates[] when activated
            "min_hold_days": rc.get("min_hold_days", 0),
        }

    # Evaluate each date with state tracking
    result = {}
    for day_idx, date in enumerate(dates):
        values = all_values.get(date, {})
        active = []

        for rc in regime_configs:
            name = rc["name"]
            st = regime_states[name]

            if st["state"] == "inactive":
                # Check entry conditions
                if _evaluate_regime(rc, values):
                    st["state"] = "cooldown" if st["min_hold_days"] > 0 else "monitoring"
                    st["activated_day_idx"] = day_idx
                    active.append(name)

            elif st["state"] == "cooldown":
                # Active but in cooldown — don't check exit yet
                days_held = day_idx - st["activated_day_idx"]
                active.append(name)
                if days_held >= st["min_hold_days"]:
                    st["state"] = "monitoring"

            elif st["state"] == "monitoring":
                # Active and past cooldown — check exit
                if _evaluate_regime_exit(rc, values):
                    st["state"] = "inactive"
                    st["activated_day_idx"] = None
                else:
                    active.append(name)

        result[date] = active

    return result


def get_regime_details(date: str, regime_configs: list[dict], conn=None) -> dict:
    """
    Evaluate regimes and return detailed breakdown including actual values
    and per-condition results. Useful for dashboards.

    Returns:
        {
            "date": "2026-03-28",
            "active_regimes": ["oil_shock"],
            "regimes": {
                "oil_shock": {
                    "active": true,
                    "conditions": [
                        {"series": "brent_vs_50dma_pct", "operator": ">", "value": 30,
                         "actual": 42.5, "met": true},
                        ...
                    ]
                }
            }
        }
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    all_keys = _collect_series_keys(regime_configs)
    values = _load_macro_values(date, list(all_keys), conn)

    if own_conn:
        conn.close()

    active_regimes = []
    regimes_detail = {}

    for rc in regime_configs:
        name = rc["name"]

        # Entry conditions
        entry_conditions = rc.get("entry_conditions", rc.get("conditions", []))
        entry_logic = rc.get("entry_logic", rc.get("logic", "all"))
        entry_detail = []
        for cond in entry_conditions:
            actual = values.get(cond["series"])
            met = _evaluate_condition(cond, values)
            entry_detail.append({
                "series": cond["series"],
                "operator": cond["operator"],
                "threshold": cond["value"],
                "actual": actual,
                "met": met,
            })

        # Exit conditions
        exit_conditions = rc.get("exit_conditions", [])
        exit_logic = rc.get("exit_logic", "any")
        exit_detail = []
        for cond in exit_conditions:
            actual = values.get(cond["series"])
            met = _evaluate_condition(cond, values)
            exit_detail.append({
                "series": cond["series"],
                "operator": cond["operator"],
                "threshold": cond["value"],
                "actual": actual,
                "met": met,
            })

        entry_met = _evaluate_regime(rc, values)
        exit_met = _evaluate_regime_exit(rc, values) if exit_conditions else None

        if entry_met:
            active_regimes.append(name)

        detail = {
            "active": entry_met,
            "entry_logic": entry_logic,
            "entry_conditions": entry_detail,
            "min_hold_days": rc.get("min_hold_days", 0),
        }
        if exit_conditions:
            detail["exit_logic"] = exit_logic
            detail["exit_conditions"] = exit_detail
            detail["exit_met"] = exit_met

        regimes_detail[name] = detail

    return {
        "date": date,
        "active_regimes": active_regimes,
        "regimes": regimes_detail,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="AlphaScout Regime Detector")
    subparsers = parser.add_subparsers(dest="command")

    # evaluate command
    eval_parser = subparsers.add_parser("evaluate", help="Evaluate regimes for a date")
    eval_parser.add_argument("--config", required=True, help="Path to regime config JSON")
    eval_parser.add_argument("--date", required=True, help="Date YYYY-MM-DD")
    eval_parser.add_argument("--detail", action="store_true", help="Show per-condition detail")

    # series command
    series_parser = subparsers.add_parser("series", help="Evaluate regimes over date range")
    series_parser.add_argument("--config", required=True, help="Path to regime config JSON")
    series_parser.add_argument("--start", required=True, help="Start date")
    series_parser.add_argument("--end", required=True, help="End date")
    series_parser.add_argument("--csv", action="store_true", help="Output as CSV")

    args = parser.parse_args()

    if args.command == "evaluate":
        with open(args.config) as f:
            regime_configs = json.load(f)
        if isinstance(regime_configs, dict):
            regime_configs = regime_configs.get("regimes", [regime_configs])

        if args.detail:
            result = get_regime_details(args.date, regime_configs)
            print(json.dumps(result, indent=2))
        else:
            active = evaluate_regimes(args.date, regime_configs)
            if active:
                print(f"Active regimes on {args.date}: {', '.join(active)}")
            else:
                print(f"No regimes active on {args.date}")

    elif args.command == "series":
        with open(args.config) as f:
            regime_configs = json.load(f)
        if isinstance(regime_configs, dict):
            regime_configs = regime_configs.get("regimes", [regime_configs])

        series = evaluate_regime_series(args.start, args.end, regime_configs)

        if args.csv:
            # Get all regime names
            all_names = [rc["name"] for rc in regime_configs]
            print("date," + ",".join(all_names))
            for date in sorted(series):
                active = series[date]
                flags = ["1" if n in active else "0" for n in all_names]
                print(f"{date},{','.join(flags)}")
        else:
            for date in sorted(series):
                active = series[date]
                if active:
                    print(f"{date}: {', '.join(active)}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
