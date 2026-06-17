"""Per-run synthesis report — Phase 1 of the lessons library.

One report per run (UPSERT on run_id): regenerating over an extended run
supersedes its earlier version, always synthesized over the run's FULL memo_item
history — never just the latest batch.

Division of labour (the load-bearing principle):
  - The LLM SYNTHESIZES: it clusters semantically-equivalent lessons across
    experiments and writes one canonical claim + mechanism per cluster.
  - The DATA CERTIFIES: every count, validation status, and confidence is read
    off the validator's verdicts already on memo_items. The LLM never asserts
    confidence or effect size — if the model tries, we ignore it and attach the
    figures from the rows.

Phase 1 is single-run only: no cross-run library, no priors. Those are Phase 2+.
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
# Order matters: scripts FIRST so top-level `schema`/`regime` resolve to scripts/,
# then repo root (for `auto_trader.*`), then auto_trader/ (for bare sibling
# imports like `lesson_validator` used inside lesson_pipeline).
sys.path.insert(0, _HERE)
sys.path.insert(0, os.path.join(_HERE, ".."))
sys.path.insert(0, os.path.join(_HERE, "..", "scripts"))

# Verdict statuses that count as a real (non-rejected) finding — mirrors
# lesson_validator.VALIDATED_STATUSES so the report and the validator agree.
VALIDATED_STATUSES = ("unconditional", "validated", "validated_conditional", "regime_reversing")


def _collect_run_lessons(run_id: str) -> list[dict]:
    """Every memo_item for the run — including rejected/falsified ones, so the
    report can report what did NOT hold, not just what did."""
    from auto_trader.analyst import recall_memo_items
    # Self-heal the validation columns recall_memo_items selects — they live
    # behind the lesson pipeline's migration, which a validate_lessons=false run
    # never triggers. Idempotent.
    try:
        from auto_trader.schema import get_db
        from auto_trader.lesson_pipeline import migrate_memo_items
        _c = get_db()
        try:
            migrate_memo_items(_c)
        finally:
            _c.close()
    except Exception:
        pass
    return recall_memo_items(
        run_id=run_id,
        forward_looking_only=False,
        include_falsified=True,
        limit=10000,
    )


def _deterministic_stats(lessons: list[dict]) -> dict:
    """Counts taken straight from the rows — the certified half of the report."""
    status_counts = Counter((l.get("validation_status") or "candidate") for l in lessons)
    confidence_counts = Counter(
        (l.get("validated_confidence") or "unset") for l in lessons)
    universes = [l.get("universe") for l in lessons if l.get("universe")]
    distinct_universes = sorted(set(universes))
    iterations = {l.get("experiment_id") for l in lessons if l.get("experiment_id")}
    n_validated = sum(1 for l in lessons
                      if (l.get("validation_status") in VALIDATED_STATUSES))
    n_rejected = status_counts.get("rejected", 0)
    return {
        "n_lessons": len(lessons),
        "iterations_covered": len(iterations),
        "status_counts": dict(status_counts),
        "confidence_counts": dict(confidence_counts),
        "n_validated": n_validated,
        "n_rejected": n_rejected,
        "universe": (distinct_universes[0] if len(distinct_universes) == 1
                     else ("mixed" if distinct_universes else None)),
        "distinct_universes": distinct_universes,
    }


def _dedupe_by_spec(lessons: list[dict]) -> tuple[list[dict], list[dict]]:
    """Deterministic first pass: lessons sharing an identical test_spec are the
    same operationalized claim — collapse them before paying for LLM clustering.
    Returns (representatives, all_with_spec_grouped_meta_unused). Lessons without
    a test_spec fall through untouched for the LLM to cluster semantically."""
    by_spec: dict[str, list[dict]] = {}
    no_spec: list[dict] = []
    for l in lessons:
        spec = l.get("test_spec")
        if spec:
            by_spec.setdefault(spec, []).append(l)
        else:
            no_spec.append(l)
    reps = []
    for spec, group in by_spec.items():
        rep = dict(group[0])
        rep["_spec_group_size"] = len(group)
        rep["_member_ids"] = [g.get("id") for g in group]
        reps.append(rep)
    return reps, no_spec


def _build_synthesis_prompt(spec_reps: list[dict], no_spec: list[dict],
                            stats: dict) -> str:
    """Lessons in, clustering instructions out. We hand the model the verdicts
    but tell it NOT to re-judge them — its job is grouping and wording only."""
    def _fmt(l: dict) -> dict:
        return {
            "id": l.get("id"),
            "kind": l.get("kind"),
            "claim": l.get("claim"),
            "mechanism": l.get("mechanism"),
            "evidence_summary": l.get("evidence_summary"),
            "validation_status": l.get("validation_status") or "candidate",
            "validated_confidence": l.get("validated_confidence"),
            "regime_conditions": l.get("regime_conditions"),
            "universe": l.get("universe"),
        }
    payload = {
        "spec_deduped_lessons": [_fmt(l) for l in spec_reps],
        "free_text_lessons": [_fmt(l) for l in no_spec],
        "run_stats": {k: stats[k] for k in
                      ("n_lessons", "iterations_covered", "status_counts",
                       "n_validated", "n_rejected")},
    }
    return (
        "Below are the lessons an optimization run produced, each already carrying "
        "a system-set validation verdict. Synthesize them into a concise report.\n\n"
        "STRICT RULES:\n"
        "1. Cluster semantically-equivalent lessons (the same bet said different "
        "ways) into one canonical claim. Reference the member lesson `id`s.\n"
        "2. For each cluster write: a one-sentence canonical claim, the mechanism "
        "(why it might hold), and the scope (universe / regime) it applies to.\n"
        "3. Do NOT invent or re-judge confidence, effect sizes, or validation "
        "status — those are set by the validator and will be attached from the "
        "data. Report the verdicts as given.\n"
        "4. Separately note which lessons did NOT hold (status 'rejected') so the "
        "graveyard is visible, not hidden.\n\n"
        "Return ONLY valid JSON of the form:\n"
        '{"headline": "<=200 chars overall takeaway", '
        '"clusters": [{"canonical_claim": str, "mechanism": str, "scope": str, '
        '"member_ids": [str], "verdict_summary": str}], '
        '"did_not_hold": [{"claim": str, "member_ids": [str]}], '
        '"narrative": "2-4 sentence prose summary"}\n\n'
        f"LESSONS:\n{json.dumps(payload, indent=2, default=str)}\n"
    )


def _deterministic_synthesis(spec_reps: list[dict], no_spec: list[dict]) -> dict:
    """Fallback when the LLM is unavailable or returns junk: group by kind, no
    semantic clustering. Keeps the report useful without the model."""
    clusters = []
    held = [l for l in (spec_reps + no_spec)
            if (l.get("validation_status") in VALIDATED_STATUSES)]
    by_kind: dict[str, list[dict]] = {}
    for l in held:
        by_kind.setdefault(l.get("kind") or "observation", []).append(l)
    for kind, group in by_kind.items():
        clusters.append({
            "canonical_claim": f"{len(group)} {kind} lesson(s) held this run",
            "mechanism": None,
            "scope": ", ".join(sorted({g.get("universe") for g in group if g.get("universe")})) or None,
            "member_ids": [g.get("id") for g in group],
            "verdict_summary": "; ".join(
                f"{g.get('validation_status')}/{g.get('validated_confidence')}" for g in group),
        })
    did_not_hold = [
        {"claim": l.get("claim"), "member_ids": [l.get("id")]}
        for l in (spec_reps + no_spec) if l.get("validation_status") == "rejected"
    ]
    return {
        "headline": f"{len(held)} lesson(s) held, {len(did_not_hold)} rejected "
                    "(deterministic synthesis — LLM unavailable)",
        "clusters": clusters,
        "did_not_hold": did_not_hold,
        "narrative": "LLM synthesis unavailable; lessons grouped by kind.",
    }


async def _llm_synthesize(spec_reps: list[dict], no_spec: list[dict],
                          stats: dict, model: str) -> dict:
    """Call the analyst's one-shot LLM helper and parse strict JSON. Raises on
    any failure so the caller can fall back to deterministic synthesis."""
    from auto_trader.analyst import _call_opus
    prompt = _build_synthesis_prompt(spec_reps, no_spec, stats)
    system = ("You are a quant research lead summarizing one optimization run's "
              "lessons. You cluster and word findings; you NEVER assign or alter "
              "confidence or statistical verdicts. Output strict JSON only.")
    text, _meta = await _call_opus(system, prompt, model=model)
    # Tolerate a fenced ```json block.
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```", 2)[1]
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
    return json.loads(cleaned)


def _render_markdown(run_id: str, stats: dict, synthesis: dict) -> str:
    lines = [
        f"# Run report — {run_id}",
        "",
        f"**Universe:** {stats['universe']}  |  "
        f"**Experiments synthesized:** {stats['iterations_covered']}  |  "
        f"**Lessons:** {stats['n_lessons']}  "
        f"({stats['n_validated']} held, {stats['n_rejected']} rejected)",
        "",
        f"_{synthesis.get('headline', '')}_",
        "",
        "## What held",
    ]
    for c in synthesis.get("clusters", []):
        lines.append(f"- **{c.get('canonical_claim')}**")
        if c.get("mechanism"):
            lines.append(f"  - Mechanism: {c['mechanism']}")
        if c.get("scope"):
            lines.append(f"  - Scope: {c['scope']}")
        if c.get("verdict_summary"):
            lines.append(f"  - Verdict (from validator): {c['verdict_summary']}")
    dnh = synthesis.get("did_not_hold", [])
    if dnh:
        lines += ["", "## Did not hold out-of-sample (kept, not deleted)"]
        for d in dnh:
            lines.append(f"- {d.get('claim')}")
    if synthesis.get("narrative"):
        lines += ["", "## Summary", synthesis["narrative"]]
    return "\n".join(lines)


def _persist(run_id: str, stats: dict, synthesis: dict, report_md: str,
             model: str) -> None:
    from auto_trader.schema import get_db
    conn = get_db()
    try:
        report_json = json.dumps({"stats": stats, "synthesis": synthesis},
                                 default=str)
        now = datetime.now(timezone.utc).isoformat()
        # UPSERT — one report per run; regeneration supersedes.
        conn.execute(
            """INSERT INTO run_reports
                 (run_id, universe, iterations_covered, n_lessons, status_counts,
                  report_md, report_json, model, generated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(run_id) DO UPDATE SET
                 universe=excluded.universe,
                 iterations_covered=excluded.iterations_covered,
                 n_lessons=excluded.n_lessons,
                 status_counts=excluded.status_counts,
                 report_md=excluded.report_md,
                 report_json=excluded.report_json,
                 model=excluded.model,
                 generated_at=excluded.generated_at""",
            (run_id, stats["universe"], stats["iterations_covered"],
             stats["n_lessons"], json.dumps(stats["status_counts"]),
             report_md, report_json, model, now),
        )
        conn.commit()
    finally:
        conn.close()


def generate_run_report(run_id: str, model: str = "claude-opus-4-7") -> dict:
    """Synthesize one run's lessons into a single (upserted) report.

    Reads ALL of the run's memo_items, computes deterministic stats, asks the
    LLM to cluster/canonicalize (falling back to a deterministic grouping if the
    model is unavailable), and persists one report per run. Returns the report
    dict. Safe to call repeatedly — it always supersedes.
    """
    import asyncio

    lessons = _collect_run_lessons(run_id)
    stats = _deterministic_stats(lessons)
    spec_reps, no_spec = _dedupe_by_spec(lessons)

    if lessons:
        try:
            synthesis = asyncio.run(_llm_synthesize(spec_reps, no_spec, stats, model))
        except Exception:
            synthesis = _deterministic_synthesis(spec_reps, no_spec)
    else:
        synthesis = {"headline": "No lessons recorded this run.", "clusters": [],
                     "did_not_hold": [], "narrative": ""}

    report_md = _render_markdown(run_id, stats, synthesis)
    _persist(run_id, stats, synthesis, report_md, model)
    return {"run_id": run_id, "stats": stats, "synthesis": synthesis,
            "report_md": report_md}
