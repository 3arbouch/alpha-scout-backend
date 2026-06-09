"""Lesson pipeline: operationalize → validate → score → persist (Phases 2,3,5,6).

Turns the analyst's candidate claims into *certified* knowledge:

  candidate claim ──(operationalize)──▶ test_spec
       │                                   │
       └──────────────(validate on HOLDOUT, per regime)──────────────┐
                                                                      ▼
                                            system-set verdict written back to
                                            memo_items (status / validated_confidence
                                            / regime_conditions). Rejected lessons are
                                            KEPT (graveyard), never deleted.

A *regime* proposal is just another candidate (a claim about a conditioning
variable) — validate_regime_candidate gates it on recurrence + conditioning power
before it's allowed to partition anything.

Reuses lesson_validator.py for the actual testing (single source of truth) and
scripts/regime.py for point-in-time regime labels.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lesson_validator import validate_lesson, SEED_REGIMES  # noqa: E402
from regime import evaluate_regime_series  # noqa: E402

# Columns the pipeline adds to memo_items (additive, nullable — safe migration).
_NEW_COLUMNS = {
    "test_spec": "TEXT",
    "validation_status": "TEXT",       # candidate | validated | validated_conditional | rejected
    "validated_confidence": "TEXT",    # system-set (distinct from analyst's `confidence`)
    "regime_conditions": "TEXT",
    "last_validated_at": "TEXT",
}

_HYPOTHESES = {"cheap_beats_expensive", "expensive_beats_cheap", "low_beats_high", "high_beats_low"}


def migrate_memo_items(app_conn):
    """Idempotently add the validation columns to memo_items (additive, nullable)."""
    existing = {r[1] for r in app_conn.execute("PRAGMA table_info(memo_items)")}
    added = []
    for col, typ in _NEW_COLUMNS.items():
        if col not in existing:
            app_conn.execute(f"ALTER TABLE memo_items ADD COLUMN {col} {typ}")
            added.append(col)
    app_conn.commit()
    return added


# --------------------------------------------------------------------------- #
# Phase 2 — operationalize (pure, testable)
# --------------------------------------------------------------------------- #
def operationalize_claim(claim, known_factors=None):
    """Validate/normalize a claim's test_spec. Returns a clean spec or None.

    A claim is only testable if it carries a well-formed `test_spec`. Anything
    that can't be specced (a vibe like "avoid hype") is rejected here — the
    platitude filter. `known_factors`, if given, gates factor names so the spec
    can't reference a factor that doesn't exist.
    """
    spec = claim.get("test_spec") if isinstance(claim, dict) else None
    if not isinstance(spec, dict):
        return None
    pf, cf = spec.get("primary_factor"), spec.get("conditioning_factor")
    horizon = spec.get("horizon_days")
    hyp = spec.get("hypothesis")
    if not (pf and cf) or pf == cf:
        return None
    if not isinstance(horizon, int) or horizon <= 0:
        return None
    if hyp not in _HYPOTHESES:
        return None
    if known_factors is not None and (pf not in known_factors or cf not in known_factors):
        return None
    return {
        "primary_factor": pf,
        "primary_bucket": spec.get("primary_bucket", "top_quintile"),
        "conditioning_factor": cf,
        "horizon_days": horizon,
        "hypothesis": hyp,
    }


def _known_factors(market_conn):
    return {r[1] for r in market_conn.execute("PRAGMA table_info(features_daily)")}


# --------------------------------------------------------------------------- #
# Phase 3 + 6 — confidence engine / persistence / graveyard / re-test
# --------------------------------------------------------------------------- #
def validate_candidate_lessons(app_conn, market_conn, holdout_start, holdout_end,
                               regime_configs=None, only_candidates=True, limit=None,
                               run_id=None):
    """Validate candidate lessons on the holdout; write the verdict back.

    Pulls factor-interaction claims, operationalizes them, runs the PIT
    double-sort validator on the holdout (split by regime), and persists
    status / validated_confidence / regime_conditions. Rejected lessons are
    marked (status='rejected') and KEPT — the graveyard. Re-running on a later
    window is Phase-6 re-test (promote/demote as evidence accrues).
    """
    regime_configs = regime_configs or SEED_REGIMES
    migrate_memo_items(app_conn)   # idempotent — self-heal schema on the runtime DB
    known = _known_factors(market_conn)
    params = []
    where = "WHERE kind='factor_interaction' AND test_spec IS NOT NULL"
    if only_candidates:
        where += " AND (validation_status IS NULL OR validation_status='candidate')"
    if run_id:
        where += " AND run_id=?"
        params.append(run_id)
    sql = f"SELECT id, claim, test_spec FROM memo_items {where} ORDER BY created_at"
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = app_conn.execute(sql, params).fetchall()

    now = datetime.now(timezone.utc).isoformat()
    summary = {"validated": 0, "validated_conditional": 0, "rejected": 0, "skipped": 0}
    for mid, claim_text, spec_json in rows:
        try:
            claim = {"test_spec": json.loads(spec_json)}
        except (TypeError, json.JSONDecodeError):
            summary["skipped"] += 1
            continue
        spec = operationalize_claim(claim, known_factors=known)
        if spec is None:
            summary["skipped"] += 1
            continue
        res = validate_lesson(spec, holdout_start, holdout_end, market_conn,
                              regime_configs=regime_configs)
        v = res["verdict"]
        app_conn.execute(
            "UPDATE memo_items SET validation_status=?, validated_confidence=?, "
            "regime_conditions=?, last_validated_at=?, promotion_count=COALESCE(promotion_count,0)+? , "
            "falsified=? WHERE id=?",
            (v["status"], v["validated_confidence"], v["regime_conditions"], now,
             1 if v["status"].startswith("validated") else 0,
             1 if v["status"] == "rejected" else 0, mid),
        )
        summary[v["status"]] = summary.get(v["status"], 0) + 1
    app_conn.commit()
    return summary


# --------------------------------------------------------------------------- #
# Phase 5 — regime proposals (a regime is a candidate claim about a partition)
# --------------------------------------------------------------------------- #
def validate_regime_candidate(rule_spec, market_conn, start, end,
                              min_episodes=3, min_active_days=60):
    """Gate a proposed regime before it may condition any lesson.

    A proposal must be (a) PIT-expressible — it IS a regime_config of macro rules;
    (b) recurring — ≥ min_episodes distinct activations across history; and
    (c) have conditioning power — forward market returns differ in-regime vs out.
    Returns a verdict dict.
    """
    name = rule_spec.get("name", "candidate")
    # (a) PIT-expressible: every referenced series must exist
    series_ok = True
    referenced = []
    for key in ("entry_conditions", "exit_conditions", "conditions"):
        for c in rule_spec.get(key, []) or []:
            referenced.append(c.get("series"))
    macro = {r[0] for tbl in ("macro_indicators", "macro_derived")
             for r in market_conn.execute(f"SELECT DISTINCT series FROM {tbl}")}
    missing = [s for s in referenced if s and s not in macro]
    if missing:
        return {"status": "rejected", "reason": f"unknown macro series {missing} (not PIT-expressible)"}

    labels = evaluate_regime_series(start, end, [rule_spec], conn=market_conn)
    active_dates = sorted(d for d, regs in labels.items() if name in regs)
    # (b) recurrence: count distinct episodes (gaps > 5 trading rows = new episode)
    episodes = 0
    prev = None
    cal = [r[0] for r in market_conn.execute(
        "SELECT DISTINCT date FROM prices WHERE date>=? AND date<=? ORDER BY date", (start, end))]
    idx = {d: i for i, d in enumerate(cal)}
    for d in active_dates:
        if prev is None or (d in idx and prev in idx and idx[d] - idx[prev] > 5):
            episodes += 1
        prev = d
    if episodes < min_episodes or len(active_dates) < min_active_days:
        return {"status": "rejected",
                "reason": f"insufficient recurrence: {episodes} episodes / {len(active_dates)} active days"}

    # (c) conditioning power: forward 21d market (SPX) return in-regime vs out
    spx = dict(market_conn.execute(
        "SELECT date, value FROM macro_indicators WHERE series='spx' AND date>=? AND date<=?",
        (start, end)))
    in_r, out_r = [], []
    H = 21
    for i in range(len(cal) - H):
        d, fwd = cal[i], cal[i + H]
        if d in spx and fwd in spx and spx[d]:
            r = spx[fwd] / spx[d] - 1
            (in_r if name in (labels.get(d) or []) else out_r).append(r)
    import math
    from statistics import mean, pstdev
    if len(in_r) < 20 or len(out_r) < 20:
        return {"status": "rejected", "reason": "too few in/out observations for conditioning test"}
    diff = mean(in_r) - mean(out_r)
    se = math.sqrt(pstdev(in_r) ** 2 / len(in_r) + pstdev(out_r) ** 2 / len(out_r))
    t = diff / se if se > 0 else 0.0
    conditions = abs(t) >= 1.5
    return {
        "status": "validated" if conditions else "rejected",
        "episodes": episodes,
        "active_days": len(active_dates),
        "in_vs_out_fwd21_diff_pct": round(diff * 100, 2),
        "t_stat": round(t, 2),
        "reason": "recurring + conditions market returns" if conditions
                  else f"recurs but no conditioning power (t={round(t,2)})",
    }
