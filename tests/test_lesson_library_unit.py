#!/usr/bin/env python3
"""
Unit tests for the cross-run lessons library fold (auto_trader/lesson_library.py).

Hermetic: temp APP_DB_PATH, no LLM. Exercises the Phase-2 merge contract:

  - new claim inserts; same claim from a new run bumps repetition + union regimes
  - re-folding the SAME run is idempotent (no double-count — report regeneration)
  - same spec, different universe -> distinct claims
  - regime sign conflict (holds + REVERSES) -> has_conflict
  - validated vs candidate vs rejected accounted separately

Run:
    cd /app && python3 tests/test_lesson_library_unit.py
"""
import os
import sys
import json
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone

_TMP = tempfile.mkdtemp(prefix="lesson_lib_test_")
os.environ["APP_DB_PATH"] = str(Path(_TMP) / "app.db")
os.environ["MARKET_DB_PATH"] = str(Path(_TMP) / "market.db")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "auto_trader"))
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from auto_trader.schema import get_db  # noqa: E402
from auto_trader.lesson_pipeline import migrate_memo_items  # noqa: E402
from auto_trader import lesson_library  # noqa: E402

_passed = _failed = 0


def check(name, cond):
    global _passed, _failed
    if cond:
        _passed += 1; print(f"  ✅ {name}")
    else:
        _failed += 1; print(f"  ❌ {name}")


def _now():
    return datetime.now(timezone.utc).isoformat()


def _lesson(conn, lid, run_id, *, spec, universe="sp500", status="validated",
            confidence="medium", regimes=None, kind="factor_interaction"):
    conn.execute(
        """INSERT INTO memo_items
           (id, experiment_id, run_id, universe, kind, claim, is_forward_looking,
            scope_level, promotion_count, falsified, test_spec, validation_status,
            validated_confidence, regime_conditions, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (lid, f"{run_id}_e", run_id, universe, kind, f"claim {lid}", 1, "run", 1, 0,
         json.dumps(spec), status, confidence, regimes, _now(), _now()),
    )
    conn.commit()


def _get(ident):
    conn = get_db()
    row = conn.execute("SELECT * FROM lesson_library WHERE id=?", (ident,)).fetchone()
    conn.close()
    return dict(row) if row else None


SPEC = {"primary_factor": "debt_to_equity", "conditioning_factor": "gross_profitability",
        "hypothesis": "low_beats_high", "horizon_days": 63}
IDENT = lesson_library._identity("sp500", lesson_library._canonical_spec(json.dumps(SPEC)))


def main():
    conn = get_db(); migrate_memo_items(conn); conn.close()

    print("=" * 70); print("Fold run A — insert new claim"); print("=" * 70)
    conn = get_db()
    _lesson(conn, "a1", "runA", spec=SPEC, status="validated",
            regimes="holds in risk_off")
    conn.close()
    s = lesson_library.fold_run_lessons("runA")
    check("runA: inserted=1", s["inserted"] == 1)
    e = _get(IDENT)
    check("entry exists", e is not None)
    check("repetition_count=1", e["repetition_count"] == 1)
    check("times_validated=1", e["times_validated"] == 1)
    check("primary_factor extracted", e["primary_factor"] == "debt_to_equity")
    check("conditioning_factor extracted", e["conditioning_factor"] == "gross_profitability")
    check("source_runs=[runA]", json.loads(e["source_runs"]) == ["runA"])
    check("no conflict yet", e["has_conflict"] == 0)

    print(); print("=" * 70); print("Fold run B — same claim, new run -> bump + union"); print("=" * 70)
    conn = get_db()
    _lesson(conn, "b1", "runB", spec=SPEC, status="validated_conditional",
            regimes="holds in calm_uptrend")
    conn.close()
    lesson_library.fold_run_lessons("runB")
    e = _get(IDENT)
    check("repetition_count=2", e["repetition_count"] == 2)
    check("times_validated=2", e["times_validated"] == 2)
    check("source_runs has both", set(json.loads(e["source_runs"])) == {"runA", "runB"})
    check("regimes unioned", "risk_off" in e["regime_conditions"] and "calm_uptrend" in e["regime_conditions"])

    print(); print("=" * 70); print("Re-fold run A — idempotent (no double-count)"); print("=" * 70)
    lesson_library.fold_run_lessons("runA")
    e = _get(IDENT)
    check("repetition_count STILL 2 (idempotent)", e["repetition_count"] == 2)
    check("times_validated STILL 2", e["times_validated"] == 2)
    check("source_runs unchanged", set(json.loads(e["source_runs"])) == {"runA", "runB"})

    print(); print("=" * 70); print("Same spec, different universe -> distinct claim"); print("=" * 70)
    conn = get_db()
    _lesson(conn, "t1", "runT", spec=SPEC, universe="tech", status="validated")
    conn.close()
    lesson_library.fold_run_lessons("runT")
    ident_tech = lesson_library._identity("tech", lesson_library._canonical_spec(json.dumps(SPEC)))
    check("tech entry is a separate row", ident_tech != IDENT and _get(ident_tech) is not None)
    check("sp500 entry still repetition=2", _get(IDENT)["repetition_count"] == 2)

    print(); print("=" * 70); print("Regime sign conflict -> has_conflict"); print("=" * 70)
    conn = get_db()
    _lesson(conn, "c1", "runC", spec=SPEC,
            status="regime_reversing",
            regimes="holds in risk_off; REVERSES in calm_uptrend")
    conn.close()
    lesson_library.fold_run_lessons("runC")
    e = _get(IDENT)
    check("has_conflict=1 after contradictory regimes", e["has_conflict"] == 1)
    check("repetition_count=3 (runA,B,C)", e["repetition_count"] == 3)

    print(); print("=" * 70); print("Candidate + rejected accounting"); print("=" * 70)
    SPEC2 = {"primary_factor": "ret_12_1m", "hypothesis": "high_beats_low", "horizon_days": 21}
    id2 = lesson_library._identity("sp500", lesson_library._canonical_spec(json.dumps(SPEC2)))
    conn = get_db()
    _lesson(conn, "d1", "runD", spec=SPEC2, status="candidate", confidence=None)
    conn.close()
    lesson_library.fold_run_lessons("runD")
    e2 = _get(id2)
    check("candidate folded (repetition=1)", e2["repetition_count"] == 1)
    check("candidate times_validated=0", e2["times_validated"] == 0)
    conn = get_db()
    _lesson(conn, "e1", "runE", spec=SPEC2, status="rejected", confidence="low")
    conn.close()
    lesson_library.fold_run_lessons("runE")
    e2 = _get(id2)
    check("rejected counted (times_rejected=1)", e2["times_rejected"] == 1)
    check("repetition_count=2", e2["repetition_count"] == 2)
    check("times_validated still 0", e2["times_validated"] == 0)

    print(); print("=" * 70)
    print(f"RESULTS: {_passed}/{_passed + _failed} passed, {_failed} failed")
    print("=" * 70)
    shutil.rmtree(_TMP, ignore_errors=True)
    if _failed:
        print("SOME TESTS FAILED ❌"); sys.exit(1)
    print("ALL TESTS PASSED ✅")


if __name__ == "__main__":
    main()
