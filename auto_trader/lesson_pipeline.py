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
from lesson_validator import (  # noqa: E402
    validate_lesson, validate_lesson_panel, SEED_REGIMES, VALIDATED_STATUSES,
)
from regime import evaluate_regime_series  # noqa: E402

# Columns the pipeline adds to memo_items (additive, nullable — safe migration).
_NEW_COLUMNS = {
    "test_spec": "TEXT",
    "validation_status": "TEXT",       # candidate | unconditional | validated | validated_conditional | regime_reversing | rejected
    "validated_confidence": "TEXT",    # system-set (distinct from analyst's `confidence`)
    "regime_conditions": "TEXT",
    "validation_windows": "TEXT",      # per-window IS/OOS panel summary (panel path)
    "last_validated_at": "TEXT",
}

# Registry of analyst-proposed regimes that passed the gate (seed + accepted).
_REGIME_TABLE = """CREATE TABLE IF NOT EXISTS lesson_regimes (
    name         TEXT PRIMARY KEY,
    rule_spec    TEXT NOT NULL,
    status       TEXT NOT NULL,        -- validated | rejected
    episodes     INTEGER,
    active_days  INTEGER,
    t_stat       REAL,
    reason       TEXT,
    created_at   TEXT,
    updated_at   TEXT
)"""


def _is_validated(status):
    return status in VALIDATED_STATUSES

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
                               run_id=None, eval_windows=None, train_span=None):
    """Validate candidate lessons; write the verdict back.

    Two modes:
      - PANEL (eval_windows given): per-window × per-regime panel across the
        walk-forward blocks, IS/OOS-tagged via train_span. Yields the richer
        taxonomy (unconditional / validated_conditional / regime_reversing /
        rejected) plus a per-window summary.
      - HOLDOUT (no eval_windows): single-window double-sort on [holdout_start,
        holdout_end], the legacy behavior.

    Regimes default to the validated registry (seed + analyst-proposed that
    passed the gate). Rejected lessons are KEPT (graveyard), never deleted.
    """
    migrate_memo_items(app_conn)   # idempotent — self-heal schema on the runtime DB
    if regime_configs is None:
        regime_configs = load_validated_regime_configs(app_conn)
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
    summary = {"skipped": 0}
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
        if eval_windows:
            res = validate_lesson_panel(spec, market_conn, eval_windows,
                                        regime_configs=regime_configs, train_span=train_span)
            v = res["verdict"]
            windows_summary = v.get("windows_summary")
            regime_conditions = v["regime_conditions"]
            if v.get("oos_persistence"):
                regime_conditions += f" | {v['oos_persistence']}"
        else:
            res = validate_lesson(spec, holdout_start, holdout_end, market_conn,
                                  regime_configs=regime_configs)
            v = res["verdict"]
            windows_summary = None
            regime_conditions = v["regime_conditions"]
        app_conn.execute(
            "UPDATE memo_items SET validation_status=?, validated_confidence=?, "
            "regime_conditions=?, validation_windows=?, last_validated_at=?, "
            "promotion_count=COALESCE(promotion_count,0)+?, falsified=? WHERE id=?",
            (v["status"], v["validated_confidence"], regime_conditions, windows_summary, now,
             1 if _is_validated(v["status"]) else 0,
             1 if v["status"] == "rejected" else 0, mid),
        )
        summary[v["status"]] = summary.get(v["status"], 0) + 1
    app_conn.commit()
    return summary


# --------------------------------------------------------------------------- #
# Regime registry — seed + analyst-proposed regimes that passed the gate
# --------------------------------------------------------------------------- #
def migrate_lesson_regimes(app_conn):
    """Idempotently create the validated-regime registry table."""
    app_conn.execute(_REGIME_TABLE)
    app_conn.commit()


def load_validated_regime_configs(app_conn):
    """SEED_REGIMES + every analyst-proposed regime that passed the gate.

    These are the regime_configs the per-regime panel slices by. Falls back to
    just the seed set if the registry table doesn't exist yet.
    """
    try:
        rows = app_conn.execute(
            "SELECT rule_spec FROM lesson_regimes WHERE status='validated'").fetchall()
    except Exception:
        return list(SEED_REGIMES)
    extra = []
    for (rs,) in rows:
        try:
            extra.append(json.loads(rs))
        except (TypeError, json.JSONDecodeError):
            pass
    return list(SEED_REGIMES) + extra


def register_regime_candidates(app_conn, market_conn, proposals, start, end,
                               min_episodes=3):
    """Gate each proposed regime (PIT + recurrence + conditioning power); persist.

    proposals: list of regime rule_spec dicts ({name, entry_conditions, ...}).
    Accepted (status='validated') regimes become available to condition lessons
    via load_validated_regime_configs. Returns {validated, rejected} counts.
    """
    migrate_lesson_regimes(app_conn)
    now = datetime.now(timezone.utc).isoformat()
    summary = {"validated": 0, "rejected": 0}
    for rs in proposals:
        if not isinstance(rs, dict) or not rs.get("name"):
            summary["rejected"] += 1
            continue
        verdict = validate_regime_candidate(rs, market_conn, start, end,
                                            min_episodes=min_episodes)
        st = verdict["status"]
        app_conn.execute(
            "INSERT INTO lesson_regimes (name, rule_spec, status, episodes, active_days, "
            "t_stat, reason, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(name) DO UPDATE SET rule_spec=excluded.rule_spec, "
            "status=excluded.status, episodes=excluded.episodes, "
            "active_days=excluded.active_days, t_stat=excluded.t_stat, "
            "reason=excluded.reason, updated_at=excluded.updated_at",
            (rs["name"], json.dumps(rs), st, verdict.get("episodes"),
             verdict.get("active_days"), verdict.get("t_stat"), verdict.get("reason"),
             now, now),
        )
        summary[st] = summary.get(st, 0) + 1
    app_conn.commit()
    return summary


def validate_candidate_regimes(app_conn, market_conn, start, end,
                               run_id=None, min_episodes=3):
    """Pull analyst regime proposals (memo_items kind='regime') and register them.

    The analyst proposes a regime as a memo_item whose test_spec IS a
    regime_config (name + macro conditions). This gates them and writes the
    verdict back to both the registry and the originating memo_item.
    """
    migrate_memo_items(app_conn)
    where = "WHERE kind='regime' AND test_spec IS NOT NULL"
    params = []
    if run_id:
        where += " AND run_id=?"
        params.append(run_id)
    rows = app_conn.execute(f"SELECT id, test_spec FROM memo_items {where}", params).fetchall()
    proposals, id_by_name = [], {}
    for mid, ts in rows:
        try:
            rs = json.loads(ts)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(rs, dict) and rs.get("name"):
            proposals.append(rs)
            id_by_name[rs["name"]] = mid
    summary = register_regime_candidates(app_conn, market_conn, proposals, start, end,
                                         min_episodes=min_episodes)
    now = datetime.now(timezone.utc).isoformat()
    for name, mid in id_by_name.items():
        row = app_conn.execute("SELECT status FROM lesson_regimes WHERE name=?", (name,)).fetchone()
        if row:
            app_conn.execute(
                "UPDATE memo_items SET validation_status=?, last_validated_at=? WHERE id=?",
                (row[0], now, mid))
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
