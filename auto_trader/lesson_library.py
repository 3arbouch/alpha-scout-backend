"""Lesson library — Phase 2. The cross-run INDEX over per-run reports.

Per-run reports (Phase 1) are the raw documents. This folds their *spec'd*
lessons — the deterministic factor-interaction claims — into one accumulated,
deduplicated, searchable store keyed by hash(universe + canonical test_spec).

Merge semantics (a reduce, not a concatenation, not last-wins):
  - new claim                -> insert (repetition_count=1)
  - same claim, new run       -> bump repetition_count, append run to source_runs,
                                 union regime conditions, refresh latest verdict
  - same claim, SAME run      -> idempotent refresh (report regeneration must not
                                 double-count) — guarded by source_runs membership
  - contradictory regimes     -> flag has_conflict; NEVER overwrite

Only scope_type='market' (spec'd lessons) is populated. Free-text / construction
lessons stay in the per-run reports; 'construction' is reserved for later.
"""
from __future__ import annotations

import os
import sys
import json
import hashlib
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
sys.path.insert(0, os.path.join(_HERE, ".."))
sys.path.insert(0, os.path.join(_HERE, "..", "scripts"))

VALIDATED_STATUSES = ("unconditional", "validated", "validated_conditional", "regime_reversing")


def _canonical_spec(test_spec: str) -> str:
    """Sorted-key JSON so logically-equal specs hash identically."""
    try:
        return json.dumps(json.loads(test_spec), sort_keys=True)
    except Exception:
        return (test_spec or "").strip()


def _identity(universe: str | None, canonical_spec: str) -> str:
    raw = f"{universe or ''}|{canonical_spec}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _spec_factors(canonical_spec: str) -> tuple[str | None, str | None]:
    try:
        s = json.loads(canonical_spec)
        return s.get("primary_factor"), s.get("conditioning_factor")
    except Exception:
        return None, None


def _run_status(statuses: list[str]) -> str:
    """Collapse a claim's statuses WITHIN one run to a single run-level verdict."""
    if any(s in VALIDATED_STATUSES for s in statuses):
        # Prefer the most informative validated label present.
        for pref in ("unconditional", "validated", "regime_reversing", "validated_conditional"):
            if pref in statuses:
                return pref
        return "validated_conditional"
    if "rejected" in statuses:
        return "rejected"
    return "candidate"


def _union_regimes(existing: str | None, incoming: str | None) -> tuple[str, bool]:
    """Merge regime-condition strings, distinct, and detect a sign conflict
    (same regime appears as both 'holds' and 'REVERSES')."""
    parts = []
    for blob in (existing, incoming):
        if blob:
            for p in str(blob).split(";"):
                p = p.strip()
                if p and p not in parts:
                    parts.append(p)
    joined = "; ".join(parts)
    low = joined.lower()
    conflict = ("holds in" in low and "reverses" in low)
    return joined, conflict


def _collect_specd(run_id: str) -> list[dict]:
    from auto_trader.analyst import recall_memo_items
    from auto_trader.schema import get_db
    from auto_trader.lesson_pipeline import migrate_memo_items
    try:
        c = get_db()
        try:
            migrate_memo_items(c)
        finally:
            c.close()
    except Exception:
        pass
    rows = recall_memo_items(run_id=run_id, forward_looking_only=False,
                             include_falsified=True, limit=10000)
    return [r for r in rows if r.get("test_spec")]


def fold_run_lessons(run_id: str) -> dict:
    """Fold one run's spec'd lessons into the library. Idempotent per run.

    Returns a summary {inserted, updated, claims}. Safe to call repeatedly — a
    re-fold of the same run refreshes in place without double-counting.
    """
    from auto_trader.schema import get_db

    lessons = _collect_specd(run_id)

    # Group within the run by canonical identity (one run-occurrence per claim).
    groups: dict[str, dict] = {}
    for L in lessons:
        canon = _canonical_spec(L["test_spec"])
        ident = _identity(L.get("universe"), canon)
        g = groups.setdefault(ident, {
            "universe": L.get("universe"), "canon": canon,
            "statuses": [], "regimes": [], "claim": L.get("claim"),
            "mechanism": L.get("mechanism"), "confidence": L.get("validated_confidence"),
        })
        g["statuses"].append(L.get("validation_status") or "candidate")
        if L.get("regime_conditions"):
            g["regimes"].append(L["regime_conditions"])
        # Prefer a validated representative's claim/mechanism/confidence.
        if (L.get("validation_status") in VALIDATED_STATUSES) and L.get("claim"):
            g["claim"] = L.get("claim")
            g["mechanism"] = L.get("mechanism")
            g["confidence"] = L.get("validated_confidence")

    inserted = updated = 0
    now = datetime.now(timezone.utc).isoformat()
    conn = get_db()
    try:
        for ident, g in groups.items():
            run_status = _run_status(g["statuses"])
            held = run_status in VALIDATED_STATUSES
            rejected = run_status == "rejected"
            incoming_regimes = "; ".join(dict.fromkeys(g["regimes"])) or None
            primary, conditioning = _spec_factors(g["canon"])

            row = conn.execute(
                "SELECT * FROM lesson_library WHERE id=?", (ident,)).fetchone()
            if row is None:
                conn.execute(
                    """INSERT INTO lesson_library
                       (id, scope_type, universe, test_spec, primary_factor,
                        conditioning_factor, claim, mechanism, regime_conditions,
                        latest_status, latest_confidence, repetition_count,
                        times_validated, times_rejected, has_conflict, source_runs,
                        first_seen_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (ident, "market", g["universe"], g["canon"], primary,
                     conditioning, g["claim"], g["mechanism"], incoming_regimes,
                     run_status, g["confidence"], 1, 1 if held else 0,
                     1 if rejected else 0,
                     1 if (incoming_regimes and "reverses" in incoming_regimes.lower()
                           and "holds in" in incoming_regimes.lower()) else 0,
                     json.dumps([run_id]), now, now),
                )
                inserted += 1
            else:
                d = dict(row)
                src = json.loads(d["source_runs"]) if d.get("source_runs") else []
                regimes, conflict = _union_regimes(d.get("regime_conditions"),
                                                   incoming_regimes)
                if run_id in src:
                    # Idempotent re-fold (report regenerated) — refresh, no bump.
                    conn.execute(
                        """UPDATE lesson_library SET regime_conditions=?, latest_status=?,
                           latest_confidence=?, has_conflict=?, claim=?, mechanism=?,
                           updated_at=? WHERE id=?""",
                        (regimes, run_status, g["confidence"],
                         1 if (conflict or d["has_conflict"]) else 0,
                         g["claim"] or d["claim"], g["mechanism"] or d["mechanism"],
                         now, ident),
                    )
                else:
                    src.append(run_id)
                    conn.execute(
                        """UPDATE lesson_library SET regime_conditions=?, latest_status=?,
                           latest_confidence=?, repetition_count=repetition_count+1,
                           times_validated=times_validated+?, times_rejected=times_rejected+?,
                           has_conflict=?, source_runs=?, claim=?, mechanism=?, updated_at=?
                           WHERE id=?""",
                        (regimes, run_status, g["confidence"],
                         1 if held else 0, 1 if rejected else 0,
                         1 if (conflict or d["has_conflict"]) else 0,
                         json.dumps(src), g["claim"] or d["claim"],
                         g["mechanism"] or d["mechanism"], now, ident),
                    )
                updated += 1
        conn.commit()
    finally:
        conn.close()

    return {"inserted": inserted, "updated": updated, "claims": len(groups)}
