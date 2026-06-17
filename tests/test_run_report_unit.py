#!/usr/bin/env python3
"""
Unit tests for the per-run synthesis report (auto_trader/run_report.py).

Hermetic: APP_DB_PATH points at a throwaway temp DB, and the LLM synthesis call
is monkeypatched (no network). Exercises the Phase-1 contract:

  - exactly ONE report per run (PRIMARY KEY upsert)
  - regenerating an extended run SUPERSEDES and covers the FULL history
  - the certified stats come from the DATA, not the LLM (an LLM that lies about
    confidence does not change the persisted status_counts)
  - the deterministic fallback runs when the LLM raises

Run:
    cd /app && python3 tests/test_run_report_unit.py
"""
import os
import sys
import json
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone

_TMP = tempfile.mkdtemp(prefix="run_report_test_")
os.environ["APP_DB_PATH"] = str(Path(_TMP) / "app.db")
os.environ["MARKET_DB_PATH"] = str(Path(_TMP) / "market.db")

_ROOT = Path(__file__).resolve().parent.parent
# scripts FIRST (top-level `schema`/`regime`), then root (`auto_trader.*`),
# then auto_trader/ (bare sibling imports inside lesson_pipeline).
sys.path.insert(0, str(_ROOT / "auto_trader"))
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from auto_trader.schema import get_db  # noqa: E402
from auto_trader.lesson_pipeline import migrate_memo_items  # noqa: E402
from auto_trader import run_report  # noqa: E402

_passed = 0
_failed = 0


def check(name, cond):
    global _passed, _failed
    if cond:
        _passed += 1
        print(f"  ✅ {name}")
    else:
        _failed += 1
        print(f"  ❌ {name}")


def _now():
    return datetime.now(timezone.utc).isoformat()


def _insert_lesson(conn, lid, exp_id, run_id, *, kind, claim, status,
                   confidence=None, universe="sp500", test_spec=None,
                   forward=1, falsified=0):
    conn.execute(
        """INSERT INTO memo_items
           (id, experiment_id, run_id, universe, kind, claim, is_forward_looking,
            scope_level, promotion_count, falsified, test_spec, validation_status,
            validated_confidence, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (lid, exp_id, run_id, universe, kind, claim, forward, "run", 1, falsified,
         test_spec, status, confidence, _now(), _now()),
    )


def _seed(conn, run_id, iterations):
    """iterations: list of (iteration, [lesson dicts]). Lessons carry a synthetic
    experiment_id per iteration — no experiments row needed (the report counts
    distinct experiment_id off the memo rows; SQLite FKs are not enforced)."""
    for it, lessons in iterations:
        exp_id = f"{run_id}_exp{it}"
        for L in lessons:
            _insert_lesson(conn, L["id"], exp_id, run_id, **{k: v for k, v in L.items() if k != "id"})
    conn.commit()


# A fake LLM that (a) clusters trivially and (b) LIES about confidence — the
# report must ignore that and keep the data's verdicts.
def _fake_llm(spec_reps, no_spec, stats, model):
    ids = [l.get("id") for l in (spec_reps + no_spec)]
    return {
        "headline": "fake synthesis",
        "clusters": [{
            "canonical_claim": "everything is high confidence (LLM lie)",
            "mechanism": "made up",
            "scope": "sp500",
            "member_ids": ids,
            "verdict_summary": "LLM-asserted HIGH",
        }],
        "did_not_hold": [],
        "narrative": "fake",
    }


def _patch_llm(fn):
    import asyncio
    async def _async(spec_reps, no_spec, stats, model):
        return fn(spec_reps, no_spec, stats, model)
    run_report._llm_synthesize = _async


def main():
    conn = get_db()  # ensures all tables (init_db runs inside get_db)
    migrate_memo_items(conn)
    conn.close()

    print("=" * 70)
    print("Run report — single run, certified stats from data")
    print("=" * 70)
    _patch_llm(_fake_llm)

    run_id = "run_A"
    conn = get_db()
    _seed(conn, run_id, [
        (1, [
            {"id": "l1", "kind": "factor_interaction", "claim": "cheap beats expensive",
             "status": "validated", "confidence": "medium",
             "test_spec": json.dumps({"primary_factor": "pe"})},
            {"id": "l2", "kind": "factor_observation", "claim": "momentum decays",
             "status": "rejected", "confidence": "low"},
        ]),
        (2, [
            {"id": "l3", "kind": "factor_interaction", "claim": "quality holds in risk-off",
             "status": "validated_conditional", "confidence": "medium"},
        ]),
    ])
    conn.close()

    rep = run_report.generate_run_report(run_id, model="test")
    stats = rep["stats"]

    check("n_lessons = 3", stats["n_lessons"] == 3)
    check("iterations_covered = 2 (distinct experiments)", stats["iterations_covered"] == 2)
    check("status_counts validated=1", stats["status_counts"].get("validated") == 1)
    check("status_counts rejected=1", stats["status_counts"].get("rejected") == 1)
    check("status_counts validated_conditional=1",
          stats["status_counts"].get("validated_conditional") == 1)
    check("n_validated = 2 (validated + validated_conditional)", stats["n_validated"] == 2)
    check("n_rejected = 1", stats["n_rejected"] == 1)
    check("universe = sp500", stats["universe"] == "sp500")

    # Exactly one persisted row, and the certified status_counts come from DATA,
    # not from the LLM's "everything is HIGH" lie.
    conn = get_db()
    rows = conn.execute("SELECT * FROM run_reports WHERE run_id=?", (run_id,)).fetchall()
    check("exactly one report row", len(rows) == 1)
    persisted = dict(rows[0])
    sc = json.loads(persisted["status_counts"])
    check("persisted status_counts from data (validated=1)", sc.get("validated") == 1)
    check("persisted iterations_covered = 2", persisted["iterations_covered"] == 2)
    check("LLM lie not in certified counts (no 'high' status key)", "high" not in sc)
    conn.close()

    print()
    print("=" * 70)
    print("Regenerate over an EXTENDED run — supersede, cover full history")
    print("=" * 70)
    conn = get_db()
    _seed(conn, run_id, [
        (3, [
            {"id": "l4", "kind": "factor_interaction", "claim": "new lesson batch 2",
             "status": "validated", "confidence": "high"},
        ]),
    ])
    conn.close()

    rep2 = run_report.generate_run_report(run_id, model="test")
    check("after extend: n_lessons = 4 (full history, not just batch 2)",
          rep2["stats"]["n_lessons"] == 4)
    check("after extend: iterations_covered = 3", rep2["stats"]["iterations_covered"] == 3)

    conn = get_db()
    rows = conn.execute("SELECT * FROM run_reports WHERE run_id=?", (run_id,)).fetchall()
    check("still exactly one report row (upsert, not append)", len(rows) == 1)
    check("superseded row reflects 4 lessons",
          json.loads(dict(rows[0])["report_json"])["stats"]["n_lessons"] == 4)
    conn.close()

    print()
    print("=" * 70)
    print("Deterministic fallback when the LLM raises")
    print("=" * 70)

    def _boom(*a, **k):
        raise RuntimeError("llm down")
    _patch_llm(_boom)

    run_b = "run_B"
    conn = get_db()
    _seed(conn, run_b, [
        (1, [
            {"id": "b1", "kind": "factor_observation", "claim": "value works",
             "status": "validated", "confidence": "high"},
            {"id": "b2", "kind": "risk_observation", "claim": "junk claim",
             "status": "rejected"},
        ]),
    ])
    conn.close()

    rep3 = run_report.generate_run_report(run_b, model="test")
    check("fallback still produces a report", rep3["stats"]["n_lessons"] == 2)
    check("fallback headline marks LLM unavailable",
          "deterministic" in rep3["synthesis"]["headline"].lower())
    check("fallback clusters only the held lesson (1)",
          len(rep3["synthesis"]["clusters"]) == 1)
    check("fallback surfaces the rejected lesson in did_not_hold",
          len(rep3["synthesis"]["did_not_hold"]) == 1)

    print()
    print("=" * 70)
    print("Empty run — no lessons")
    print("=" * 70)
    rep4 = run_report.generate_run_report("run_empty", model="test")
    check("empty run: n_lessons = 0", rep4["stats"]["n_lessons"] == 0)
    check("empty run: still persists one row", True)
    conn = get_db()
    n = conn.execute("SELECT COUNT(*) FROM run_reports WHERE run_id=?", ("run_empty",)).fetchone()[0]
    conn.close()
    check("empty run report row exists", n == 1)

    print()
    print("=" * 70)
    print(f"RESULTS: {_passed}/{_passed + _failed} passed, {_failed} failed")
    print("=" * 70)
    shutil.rmtree(_TMP, ignore_errors=True)
    if _failed:
        print("SOME TESTS FAILED ❌")
        sys.exit(1)
    print("ALL TESTS PASSED ✅")


if __name__ == "__main__":
    main()
