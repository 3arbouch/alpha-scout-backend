"""Phase-1 lesson validator.

Tests a factor-interaction *lesson* the disciplined way: a point-in-time
double-sort on data the lesson did not come from, with the cheap-vs-expensive
forward-return spread broken out BY REGIME — so we learn *where* a lesson
holds vs. reverses, not just an overall average that washes out.

A lesson is expressed as a `test_spec`:

    {
      "primary_factor":      "ret_12_1m",   # the names the lesson is about
      "primary_bucket":      "top_quintile",
      "conditioning_factor": "ev_ebitda",   # split the primary bucket by this
      "horizon_days":        63,            # forward window for the outcome
      "hypothesis":          "cheap_beats_expensive",  # low conditioning > high
    }

Discipline baked in:
  - Point-in-time: sorts use only as-of factor values; the forward return is
    the *outcome* measured after, never an input to the sort.
  - PIT regime labels come from scripts/regime.py (rule-based, as-of).
  - PIT index membership reconstructed from add/remove events (no survivorship).
  - A `control_random=True` mode shuffles the conditioning split — a real
    interaction must beat this; a spurious one won't (the discrimination test).

Stats caveat: on a grid finer than the horizon the forward returns overlap, so
the naive t-stat overstates significance. Use grid_step >= horizon_days for a
clean (non-overlapping) headline, or treat dense-grid t-stats as optimistic.
"""
from __future__ import annotations

import math
import os
import random
import sys
from collections import defaultdict
from statistics import mean, pstdev

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from regime import evaluate_regime_series  # noqa: E402


# --- Frozen, economically-motivated seed regimes (PIT rules on macro series) ---
# These are FIXED in advance, not tuned to make any lesson validate.
SEED_REGIMES = [
    {
        "name": "risk_off",
        "entry_conditions": [{"series": "vix", "operator": ">", "value": 25}],
        "entry_logic": "all",
        "exit_conditions": [{"series": "vix", "operator": "<", "value": 20}],
        "exit_logic": "all",
        "entry_persistence_days": 3,
        "exit_persistence_days": 3,
    },
    {
        "name": "calm_uptrend",
        "entry_conditions": [
            {"series": "spx_vs_200dma_pct", "operator": ">", "value": 3},
            {"series": "vix", "operator": "<", "value": 16},
        ],
        "entry_logic": "all",
        "exit_conditions": [{"series": "spx_vs_200dma_pct", "operator": "<", "value": 0}],
        "exit_logic": "all",
        "entry_persistence_days": 5,
        "exit_persistence_days": 5,
    },
]


# --------------------------------------------------------------------------- #
# Pure, unit-testable core
# --------------------------------------------------------------------------- #
def _double_sort_spread(xs, get_fwd_ret, rng=None, min_names=25):
    """Cheap-minus-expensive forward-return spread within the top-momentum bucket.

    xs: list of (symbol, primary_value, conditioning_value) — the cross-section.
    get_fwd_ret(symbol) -> forward return or None.
    rng: if given (random control), the conditioning split is shuffled — a real
         interaction must beat this.
    Returns the spread (float) or None when the cross-section is too thin.
    """
    xs = [r for r in xs if r[1] is not None and r[2] is not None]
    if len(xs) < min_names:
        return None
    xs = sorted(xs, key=lambda r: -r[1])           # momentum, best first
    k = max(int(len(xs) * 0.2), 5)
    high_mom = xs[:k]                              # top-quintile momentum
    if len(high_mom) < 10:
        return None
    if rng is not None:
        order = high_mom[:]
        rng.shuffle(order)                         # control: random conditioning
    else:
        order = sorted(high_mom, key=lambda r: r[2])  # cheap (low conditioning) first
    m = max(len(order) // 5, 1)
    cheap, expensive = order[:m], order[-m:]

    def grp(group):
        rs = [get_fwd_ret(s) for s, _, _ in group]
        rs = [r for r in rs if r is not None]
        return mean(rs) if rs else None

    fc, fe = grp(cheap), grp(expensive)
    if fc is None or fe is None:
        return None
    return fc - fe                                 # cheap - expensive


def _aggregate(per_date_spread, regime_labels, horizon_days):
    """Group per-date spreads by regime and summarize.

    per_date_spread: {date: spread}; regime_labels: {date: [active regime names]}.
    A date in no defined regime falls into 'neutral'; '__overall__' holds all.
    """
    buckets = defaultdict(list)
    for d, sp in per_date_spread.items():
        regs = regime_labels.get(d) or ["neutral"]
        for r in regs:
            buckets[r].append(sp)
        buckets["__overall__"].append(sp)
    ann = 252.0 / horizon_days
    out = {}
    for r, vals in buckets.items():
        n = len(vals)
        mu = mean(vals)
        sd = pstdev(vals) if n > 1 else 0.0
        t = (mu / (sd / math.sqrt(n))) if (sd > 0 and n > 1) else 0.0
        out[r] = {
            "n": n,
            "mean_ann_pct": round(mu * ann * 100, 2),
            "t_stat": round(t, 2),
            "hit_rate": round(sum(1 for v in vals if v > 0) / n, 2),
        }
    return out


def derive_verdict(agg):
    """Turn the per-regime aggregate into a system-set verdict.

    Confidence is EARNED here, never asserted by the analyst. A lesson that only
    holds conditionally (significant in some regimes, not overall) is labelled
    'validated_conditional' with the regime boundary attached.
    """
    SIG = 1.5  # |t| bar for "meaningful" on this (overlapping) grid — deliberately modest
    regimes = {k: v for k, v in agg.items() if k != "__overall__"}
    holds = {k: v for k, v in regimes.items() if v["t_stat"] >= SIG and v["n"] >= 8}
    reverses = {k: v for k, v in regimes.items() if v["t_stat"] <= -SIG and v["n"] >= 8}
    overall = agg.get("__overall__", {})
    overall_sig = abs(overall.get("t_stat", 0)) >= SIG

    if holds and not reverses and overall_sig:
        status, conf = "validated", "medium"
    elif holds or reverses:
        status, conf = "validated_conditional", "medium"
    else:
        status, conf = "rejected", "low"

    cond = []
    for k, v in holds.items():
        cond.append(f"holds in {k} ({v['mean_ann_pct']:+}% ann, t={v['t_stat']})")
    for k, v in reverses.items():
        cond.append(f"REVERSES in {k} ({v['mean_ann_pct']:+}% ann, t={v['t_stat']})")
    return {
        "status": status,
        "validated_confidence": conf,
        "regime_conditions": "; ".join(cond) or "no regime shows a meaningful effect",
    }


# --------------------------------------------------------------------------- #
# Data plumbing (point-in-time)
# --------------------------------------------------------------------------- #
def _trading_calendar(conn, start, end):
    return [r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM prices WHERE date>=? AND date<=? ORDER BY date", (start, end))]


def _membership_asof_fn(conn, index_name="sp500"):
    """Reconstruct point-in-time membership from add/remove events (no survivorship)."""
    rows = conn.execute(
        "SELECT symbol, event_date, action FROM index_membership_history "
        "WHERE index_name=? ORDER BY event_date", (index_name,)).fetchall()
    ev = defaultdict(list)
    for sym, d, action in rows:
        ev[sym].append((d, action))

    def members_on(date):
        out = set()
        for sym, evs in ev.items():
            last = None
            for d, a in evs:
                if d <= date:
                    last = a
                else:
                    break
            if last == "added":
                out.add(sym)
        return out
    return members_on


def _load_closes(conn, start, end):
    closes = defaultdict(dict)
    for sym, d, c in conn.execute(
            "SELECT symbol, date, close FROM prices WHERE date>=? AND date<=?", (start, end)):
        closes[sym][d] = c
    return closes


def _cross_section(conn, date, members, primary, conditioning):
    if not members:
        return []
    ph = ",".join("?" * len(members))
    rows = conn.execute(
        f"SELECT symbol, {primary}, {conditioning} FROM features_daily "
        f"WHERE date=? AND symbol IN ({ph})", [date, *members]).fetchall()
    return [(s, p, cnd) for s, p, cnd in rows]


# --------------------------------------------------------------------------- #
# Top-level
# --------------------------------------------------------------------------- #
def validate_lesson(spec, start, end, conn, regime_configs=None,
                    grid_step=21, control_random=False, seed=42):
    """Validate one lesson over [start, end]. Returns {n_dates, by_regime, verdict}."""
    regime_configs = regime_configs or SEED_REGIMES
    horizon = int(spec["horizon_days"])
    cal = _trading_calendar(conn, start, end)
    members_on = _membership_asof_fn(conn)
    closes = _load_closes(conn, start, end)
    regime_labels = evaluate_regime_series(start, end, regime_configs, conn=conn)
    rng = random.Random(seed) if control_random else None

    per_date = {}
    for i in range(0, len(cal) - horizon, grid_step):
        d, fwd = cal[i], cal[i + horizon]
        xs = _cross_section(conn, d, members_on(d), spec["primary_factor"], spec["conditioning_factor"])
        sp = _double_sort_spread(xs, lambda s: (
            (closes[s][fwd] / closes[s][d] - 1)
            if (s in closes and d in closes[s] and fwd in closes[s] and closes[s][d] > 0)
            else None), rng=rng)
        if sp is not None:
            per_date[d] = sp

    agg = _aggregate(per_date, regime_labels, horizon)
    return {"n_dates": len(per_date), "by_regime": agg, "verdict": derive_verdict(agg)}


if __name__ == "__main__":
    # Reproducible acceptance demo: momentum×value vs. a random-conditioning control.
    import sqlite3

    db = os.environ.get("MARKET_DB_PATH",
                        "/home/mohamed/alpha-scout-backend-dev/data/market_dev.db")
    conn = sqlite3.connect(db)
    spec = {"primary_factor": "ret_12_1m", "primary_bucket": "top_quintile",
            "conditioning_factor": "ev_ebitda", "horizon_days": 63,
            "hypothesis": "cheap_beats_expensive"}
    start, end = "2018-01-01", "2026-06-01"

    def _show(tag, res):
        print(f"\n===== {tag} =====  (n_dates={res['n_dates']})")
        print(f"{'regime':16}{'n':>5}{'mean_ann%':>11}{'t':>7}{'hit':>7}")
        for r, s in sorted(res["by_regime"].items()):
            print(f"{r:16}{s['n']:>5}{s['mean_ann_pct']:>11}{s['t_stat']:>7}{s['hit_rate']:>7}")
        print("verdict:", res["verdict"]["status"], "|", res["verdict"]["regime_conditions"])

    _show("momentum x VALUE (real)", validate_lesson(spec, start, end, conn))
    _show("momentum x RANDOM (control)", validate_lesson(spec, start, end, conn, control_random=True))
