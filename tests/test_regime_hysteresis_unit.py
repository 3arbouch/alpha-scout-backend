#!/usr/bin/env python3
"""
Regime state-machine unit test (Tier-A #3).

Verifies the persistence + min_hold_days hysteresis in
scripts/regime.py::evaluate_regime_series_with_stats by feeding hand-crafted
macro series and asserting the activation / deactivation timeline.

State machine (per regime):
  inactive → entry_true for K consec days → cooldown (if min_hold>0) else monitoring
  cooldown → days_held since activation reaches min_hold_days → monitoring
  monitoring → exit_true for K consec days → inactive

Test approach: bypass the DB by monkey-patching _load_macro_values_bulk to
return a synthetic {date: {series: value}} dict. Then call the public
evaluate_regime_series_with_stats and inspect the {date: [active]} dict.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    python3 test_regime_hysteresis_unit.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import regime as regime_mod
from regime import evaluate_regime_series_with_stats

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


def install_fake_macros(values_by_date):
    """Monkey-patch _load_macro_values_bulk to return values_by_date directly."""
    def _fake_bulk(dates, series_keys, conn):
        return {d: values_by_date.get(d, {}) for d in dates}
    regime_mod._load_macro_values_bulk = _fake_bulk


def install_fake_trading_dates(dates):
    """Monkey-patch the trading-date fetcher to return our synthetic dates.

    The function reads from prices for AAPL — we patch sqlite3.Connection
    via a fake cursor returned by get_connection.
    """
    class FakeCursor:
        def __init__(self, dates):
            self._dates = dates
            self._result = []
        def execute(self, sql, params=()):
            # The only query is for AAPL trading dates
            if "FROM prices" in sql:
                start, end = params
                self._result = [(d,) for d in self._dates if start <= d <= end]
            return self
        def fetchall(self):
            return self._result
        def fetchone(self):
            return self._result[0] if self._result else None

    class FakeConn:
        def __init__(self, dates):
            self._dates = dates
        def cursor(self):
            return FakeCursor(self._dates)
        def close(self):
            pass
        def execute(self, sql, params=()):
            return self.cursor().execute(sql, params)

    regime_mod.get_connection = lambda: FakeConn(dates)


def daily_dates(n, start_idx=1):
    """Generate n consecutive ISO date strings starting from 2024-01-start_idx."""
    from datetime import datetime, timedelta
    base = datetime(2024, 1, 1)
    return [(base + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(start_idx - 1, start_idx - 1 + n)]


# ---------------------------------------------------------------------------
# 1. persistence=1, no min_hold: regime activates SAME day entry is met
# ---------------------------------------------------------------------------
print("\n=== 1. persistence=1, min_hold=0 — instant activation ===")

dates = daily_dates(10)
vals = {dates[0]: {"vix": 15}, dates[1]: {"vix": 15}, dates[2]: {"vix": 30},
        dates[3]: {"vix": 30}, dates[4]: {"vix": 30}, dates[5]: {"vix": 15},
        dates[6]: {"vix": 15}, dates[7]: {"vix": 15}, dates[8]: {"vix": 15},
        dates[9]: {"vix": 15}}
install_fake_macros(vals)
install_fake_trading_dates(dates)
config = [{
    "name": "shock",
    "entry_conditions": [{"series": "vix", "operator": ">=", "value": 25}],
    "entry_logic": "all",
    # No exit_conditions → defaults to inverse of entry: vix < 25 ⇒ exit
    "entry_persistence_days": 1,
    "exit_persistence_days": 1,
    "min_hold_days": 0,
}]
series, stats = evaluate_regime_series_with_stats(dates[0], dates[-1], config)

check("d0 (vix=15) → inactive", "shock" not in series[dates[0]])
check("d1 (vix=15) → inactive", "shock" not in series[dates[1]])
check("d2 (vix=30, persist=1) → ACTIVE same day", "shock" in series[dates[2]])
check("d3 (vix=30) → active", "shock" in series[dates[3]])
check("d4 (vix=30) → active", "shock" in series[dates[4]])
check("d5 (vix=15, exit_persist=1) → INACTIVE same day", "shock" not in series[dates[5]])
check("n_activations = 1", stats["shock"]["n_activations"] == 1)
check("n_deactivations = 1", stats["shock"]["n_deactivations"] == 1)


# ---------------------------------------------------------------------------
# 2. entry_persistence=3: regime needs 3 consec days of entry before activating
# ---------------------------------------------------------------------------
print("\n=== 2. entry_persistence=3 — filtering 1-day spikes ===")

dates = daily_dates(12)
# d0: low, d1: spike, d2: low, d3-d5: 3-day high (should activate on d5),
# d6+: low (should deactivate)
vix_vals = [10, 30, 10, 30, 30, 30, 10, 10, 10, 10, 10, 10]
vals = {d: {"vix": v} for d, v in zip(dates, vix_vals)}
install_fake_macros(vals)
install_fake_trading_dates(dates)
config = [{
    "name": "shock",
    "entry_conditions": [{"series": "vix", "operator": ">=", "value": 25}],
    "entry_logic": "all",
    "entry_persistence_days": 3,
    "exit_persistence_days": 1,
    "min_hold_days": 0,
}]
series, stats = evaluate_regime_series_with_stats(dates[0], dates[-1], config)

check("d1 (1-day spike) → still inactive", "shock" not in series[dates[1]])
check("d3 (1st high day after gap) → still inactive (need 3 consec)", "shock" not in series[dates[3]])
check("d4 (2nd consec high) → still inactive", "shock" not in series[dates[4]])
check("d5 (3rd consec high, persist=3) → ACTIVATE", "shock" in series[dates[5]])
check("d6 (vix=10, exit_persist=1) → INACTIVE same day", "shock" not in series[dates[6]])
check("filtered_short_entry_runs = 1 (the d1 spike)",
      stats["shock"]["filtered_short_entry_runs"] == 1,
      f"got {stats['shock']['filtered_short_entry_runs']}")


# ---------------------------------------------------------------------------
# 3. min_hold_days=3: regime stays in cooldown for 3 days post-activation
# ---------------------------------------------------------------------------
print("\n=== 3. min_hold_days=3 — exit blocked during cooldown ===")

dates = daily_dates(12)
# d0: low, d1: HIGH (activate, persist=1), d2: low (would exit, but in cooldown),
# d3: low (still cooldown), d4: low (still cooldown if 3 days starting d1),
# d5: now in monitoring — can exit if persist threshold met
vix_vals = [10, 30, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
vals = {d: {"vix": v} for d, v in zip(dates, vix_vals)}
install_fake_macros(vals)
install_fake_trading_dates(dates)
config = [{
    "name": "shock",
    "entry_conditions": [{"series": "vix", "operator": ">=", "value": 25}],
    "entry_logic": "all",
    "entry_persistence_days": 1,
    "exit_persistence_days": 1,
    "min_hold_days": 3,
}]
series, stats = evaluate_regime_series_with_stats(dates[0], dates[-1], config)

check("d0 → inactive", "shock" not in series[dates[0]])
check("d1 (vix=30) → ACTIVATE (cooldown start)", "shock" in series[dates[1]])
check("d2 (vix=10, in cooldown 1d) → still active",
      "shock" in series[dates[2]],
      "exit blocked by min_hold")
check("d3 (vix=10, in cooldown 2d) → still active",
      "shock" in series[dates[3]])
check("d4 (vix=10, in cooldown 3d → end) → still active",
      "shock" in series[dates[4]])
# Day 5: now in monitoring; on d5 the exit check fires (exit_persist=1) → inactive
check("d5 (now monitoring, vix=10 → exit_persist met) → INACTIVE",
      "shock" not in series[dates[5]])


# ---------------------------------------------------------------------------
# 4. exit_persistence=3: regime needs 3 consec days of exit before deactivating
# ---------------------------------------------------------------------------
print("\n=== 4. exit_persistence=3 — filtering 1-day exit blips ===")

dates = daily_dates(12)
# d0: low, d1: high (activate), d2-d4: high (in regime), d5: low (exit day 1),
# d6: high (reset exit counter), d7-d9: low (3 consec → deactivate on d9)
vix_vals = [10, 30, 30, 30, 30, 10, 30, 10, 10, 10, 30, 30]
vals = {d: {"vix": v} for d, v in zip(dates, vix_vals)}
install_fake_macros(vals)
install_fake_trading_dates(dates)
config = [{
    "name": "shock",
    "entry_conditions": [{"series": "vix", "operator": ">=", "value": 25}],
    "entry_logic": "all",
    "entry_persistence_days": 1,
    "exit_persistence_days": 3,
    "min_hold_days": 0,
}]
series, stats = evaluate_regime_series_with_stats(dates[0], dates[-1], config)

check("d1 (activate) → active", "shock" in series[dates[1]])
check("d5 (exit day 1, persist=3) → still active",
      "shock" in series[dates[5]])
check("d6 (high again, exit counter reset) → still active",
      "shock" in series[dates[6]])
check("d7 (exit day 1 again) → still active", "shock" in series[dates[7]])
check("d8 (exit day 2 consec) → still active", "shock" in series[dates[8]])
check("d9 (exit day 3, persist met) → DEACTIVATE",
      "shock" not in series[dates[9]])


# ---------------------------------------------------------------------------
# 5. Re-activation cycle — counters reset cleanly
# ---------------------------------------------------------------------------
print("\n=== 5. multi-cycle: activate → deactivate → re-activate ===")

dates = daily_dates(15)
# d0-d2 low, d3-d5 high (activate d3), d6-d8 low (deactivate d6 if persist=1),
# d9-d11 low, d12-d14 high (re-activate d12)
vix_vals = [10, 10, 10, 30, 30, 30, 10, 10, 10, 10, 10, 10, 30, 30, 30]
vals = {d: {"vix": v} for d, v in zip(dates, vix_vals)}
install_fake_macros(vals)
install_fake_trading_dates(dates)
config = [{
    "name": "shock",
    "entry_conditions": [{"series": "vix", "operator": ">=", "value": 25}],
    "entry_logic": "all",
    "entry_persistence_days": 1,
    "exit_persistence_days": 1,
    "min_hold_days": 0,
}]
series, stats = evaluate_regime_series_with_stats(dates[0], dates[-1], config)

check("d3 → active (1st activation)", "shock" in series[dates[3]])
check("d6 → inactive (1st deactivation)", "shock" not in series[dates[6]])
check("d12 → active (2nd activation)", "shock" in series[dates[12]])
check("n_activations = 2", stats["shock"]["n_activations"] == 2)
check("n_deactivations = 1 (still active at end)",
      stats["shock"]["n_deactivations"] == 1)


# ---------------------------------------------------------------------------
# 6. No lookahead in the bulk macro loader: bisect on (date <= target)
# ---------------------------------------------------------------------------
print("\n=== 6. bisect lookahead semantics — point-in-time ===")

# Restore real bulk loader for this test, plug in real DB to verify bisect
import importlib
importlib.reload(regime_mod)
from regime import _load_macro_values_bulk
import sqlite3
conn = sqlite3.connect("/home/mohamed/alpha-scout-backend/data/market.db")

# VIX is a daily series. On 2024-03-15 the value should be VIX[2024-03-15] not
# any later date. Pick a date that we know has data.
res = _load_macro_values_bulk(["2024-03-15"], ["vix"], conn)
target = res["2024-03-15"]["vix"]

# Directly fetch via SQL to compare
direct = conn.execute(
    "SELECT value FROM macro_indicators WHERE series='vix' AND date='2024-03-15'"
).fetchone()
check("bulk loader returns VIX[D] for exact-match daily date",
      target is not None and direct is not None and target == direct[0],
      f"bulk={target} direct={direct}")

# What if no value on that exact date? The bisect should return the latest
# observation strictly <= the target — never anything dated AFTER the target.
res2 = _load_macro_values_bulk(["2024-03-16"], ["vix"], conn)  # Saturday → no row
target2 = res2["2024-03-16"]["vix"]
# Whatever it returned, it must come from a date <= 2024-03-16
prior = conn.execute(
    "SELECT date FROM macro_indicators WHERE series='vix' AND date <= '2024-03-16' "
    "ORDER BY date DESC LIMIT 1"
).fetchone()[0]
prior_v = conn.execute(
    "SELECT value FROM macro_indicators WHERE series='vix' AND date=?", (prior,)
).fetchone()[0]
check(f"weekend lookup → most recent prior weekday VIX ({prior}={prior_v})",
      target2 == prior_v)

# Critical: monthly publication lag NOT corrected by the bisect — known limitation.
# Confirm this on cpi as a documented characterization, not a passing assertion:
res3 = _load_macro_values_bulk(["2024-04-01"], ["cpi"], conn)
cpi_target = res3["2024-04-01"]["cpi"]
cpi_stored = conn.execute(
    "SELECT value FROM macro_indicators WHERE series='cpi' AND date='2024-04-01'"
).fetchone()
check("DOCUMENTED LIMITATION: bulk returns CPI[2024-04-01] = published-in-May value",
      cpi_target is not None and cpi_stored is not None and cpi_target == cpi_stored[0],
      "see auto_trader / regime audit notes — affects 0 production deployments")
conn.close()


# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
