"""
Pure regime evaluation engine.

No database access, no file I/O. Accepts macro values as input,
returns which regimes are active.

Usage:
    from server.engines.regime_engine import evaluate_regimes, evaluate_regime_series

    active = evaluate_regimes(macro_values, configs)
    series = evaluate_regime_series(trading_dates, macro_values_by_date, configs)
"""

from __future__ import annotations

from typing import Literal


# ---------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------

OPERATORS: dict[str, callable] = {
    ">":  lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<":  lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


# ---------------------------------------------------------------------------
# Condition evaluation (pure)
# ---------------------------------------------------------------------------

def _evaluate_condition(condition: dict, values: dict[str, float | None]) -> bool:
    """Evaluate a single condition against macro values."""
    series = condition["series"]
    operator = condition["operator"]
    threshold = condition["value"]

    actual = values.get(series)
    if actual is None:
        return False

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


def _check_entry(regime_config: dict, values: dict) -> bool:
    """Check whether a regime's entry conditions are met."""
    conditions = regime_config.get("entry_conditions", regime_config.get("conditions", []))
    logic = regime_config.get("entry_logic", regime_config.get("logic", "all"))
    return _evaluate_conditions(conditions, logic, values)


def _check_exit(regime_config: dict, values: dict) -> bool:
    """Check whether a regime's exit conditions are met."""
    exit_conditions = regime_config.get("exit_conditions", [])
    if not exit_conditions:
        # No explicit exit → exit when entry conditions no longer met
        return not _check_entry(regime_config, values)
    exit_logic = regime_config.get("exit_logic", "any")
    return _evaluate_conditions(exit_conditions, exit_logic, values)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def collect_series_keys(regime_configs: list[dict]) -> set[str]:
    """Return all macro series keys needed to evaluate the given regime configs."""
    keys: set[str] = set()
    for rc in regime_configs:
        for cond in rc.get("entry_conditions", rc.get("conditions", [])):
            keys.add(cond["series"])
        for cond in rc.get("exit_conditions", []):
            keys.add(cond["series"])
    return keys


def evaluate_regimes(
    macro_values: dict[str, float | None],
    regime_configs: list[dict],
) -> list[str]:
    """
    Stateless evaluation: which regimes' entry conditions are met right now?

    Args:
        macro_values: {series_key: value} for the evaluation date.
        regime_configs: list of regime config dicts (with name, conditions/entry_conditions, logic).

    Returns:
        List of active regime names.
    """
    return [rc["name"] for rc in regime_configs if _check_entry(rc, macro_values)]


def evaluate_regime_series(
    trading_dates: list[str],
    macro_values_by_date: dict[str, dict[str, float | None]],
    regime_configs: list[dict],
) -> dict[str, list[str]]:
    """
    Stateful evaluation over a date range with entry/exit conditions and cooldown.

    State machine per regime:
        inactive -> (entry met) -> cooldown -> (cooldown elapsed) -> monitoring -> (exit met) -> inactive

    Args:
        trading_dates: sorted list of YYYY-MM-DD strings.
        macro_values_by_date: {date: {series_key: value}}.
        regime_configs: list of regime config dicts.

    Returns:
        {date: [active_regime_names]}
    """
    # Initialize state per regime
    states: dict[str, dict] = {}
    for rc in regime_configs:
        states[rc["name"]] = {
            "state": "inactive",
            "activated_day_idx": None,
            "min_hold_days": rc.get("min_hold_days", 0),
        }

    result: dict[str, list[str]] = {}

    for day_idx, date in enumerate(trading_dates):
        values = macro_values_by_date.get(date, {})
        active: list[str] = []

        for rc in regime_configs:
            name = rc["name"]
            st = states[name]

            if st["state"] == "inactive":
                if _check_entry(rc, values):
                    st["state"] = "cooldown" if st["min_hold_days"] > 0 else "monitoring"
                    st["activated_day_idx"] = day_idx
                    active.append(name)

            elif st["state"] == "cooldown":
                days_held = day_idx - st["activated_day_idx"]
                active.append(name)
                if days_held >= st["min_hold_days"]:
                    st["state"] = "monitoring"

            elif st["state"] == "monitoring":
                if _check_exit(rc, values):
                    st["state"] = "inactive"
                    st["activated_day_idx"] = None
                else:
                    active.append(name)

        result[date] = active

    return result


def get_regime_details(
    macro_values: dict[str, float | None],
    regime_configs: list[dict],
    date: str | None = None,
) -> dict:
    """
    Evaluate regimes and return detailed per-condition breakdown.

    Returns:
        {
            "date": "...",
            "active_regimes": [...],
            "regimes": {
                "regime_name": {
                    "active": bool,
                    "entry_logic": str,
                    "entry_conditions": [{series, operator, threshold, actual, met}, ...],
                    "exit_conditions": [...],  # if defined
                }
            }
        }
    """
    active_regimes: list[str] = []
    regimes_detail: dict[str, dict] = {}

    for rc in regime_configs:
        name = rc["name"]

        # Entry conditions
        entry_conditions = rc.get("entry_conditions", rc.get("conditions", []))
        entry_logic = rc.get("entry_logic", rc.get("logic", "all"))
        entry_detail = []
        for cond in entry_conditions:
            actual = macro_values.get(cond["series"])
            met = _evaluate_condition(cond, macro_values)
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
            actual = macro_values.get(cond["series"])
            met = _evaluate_condition(cond, macro_values)
            exit_detail.append({
                "series": cond["series"],
                "operator": cond["operator"],
                "threshold": cond["value"],
                "actual": actual,
                "met": met,
            })

        entry_met = _check_entry(rc, macro_values)
        if entry_met:
            active_regimes.append(name)

        detail: dict = {
            "active": entry_met,
            "entry_logic": entry_logic,
            "entry_conditions": entry_detail,
            "min_hold_days": rc.get("min_hold_days", 0),
        }
        if exit_conditions:
            detail["exit_logic"] = exit_logic
            detail["exit_conditions"] = exit_detail
            detail["exit_met"] = _check_exit(rc, macro_values)

        regimes_detail[name] = detail

    return {
        "date": date,
        "active_regimes": active_regimes,
        "regimes": regimes_detail,
    }
