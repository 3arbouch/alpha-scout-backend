"""Analyst-pass orchestrator.

Given an experiment_id, gather post-trade inputs (thesis, trade log, position
contributions, NAV time series, factor attribution), ask Opus to write a memo
explaining WHY the experiment earned what it did, then extract structured
"items" (claims) for later recall. Persists both the markdown memo and the
structured items.

The orchestrator is intentionally side-effecting: one call → one row in
analyst_memos + N rows in memo_items, both keyed by experiment_id.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any

from auto_trader.analyst_inputs import (
    get_portfolio_timeseries,
    get_position_contribution,
    get_trade_log,
    read_thesis,
)
from auto_trader.attribution import compute_attribution
from auto_trader.schema import get_db

DEFAULT_MODEL = "claude-opus-4-7"

SYSTEM_PROMPT = """You are the post-trade analyst for a quantitative backtesting
system. Each experiment you review is one strategy iteration: a thesis,
backtest results, trade log, factor attribution.

Your job is to write a short post-mortem memo (markdown) explaining:
  1. What happened — return, drawdown, key trades.
  2. Why — which factors paid, which positions drove the result.
  3. Did the thesis hold — match the agent's predictions against the data.
  4. What's worth remembering — durable claims a future iteration should know.

Then extract the durable claims as STRUCTURED RESEARCH RECORDS — each one
modelled on the kind of post-trade note a real quant shop would write. Each
item carries:

  - kind: one of {factor_observation, factor_interaction, trade_pattern,
                  risk_observation, thesis_validation, regime_observation, anomaly}
  - claim: 1-2 sentence testable assertion. Sharp, falsifiable, specific.
    Bad:  "Momentum worked."
    Good: "12-1 month momentum (ret_12_1m) delivered positive cross-sectional
           alpha in S&P 500 large caps over 2019-2025."
  - mechanism: 1-2 sentences on WHY this is true. The causal story. Reference
    behavioral / structural / regime drivers, not just statistics. If you
    don't have a mechanism, the claim is likely a pattern-match — say so.
  - evidence_summary: concrete numbers that support the claim. IC, t-stat,
    Q5-Q1 spread, hit rate, n_trades, top contributing positions, sub-window
    behavior. Specific, not hand-wavy. Bullet-style, one line, comma-separated.
    Example: "IC=+0.041 at 63d; Q5-Q1 spread=+8.2% ann; n=247 trades;
              top drivers FICO +18%, NVDA +12%."
  - confidence: 'high' | 'medium' | 'low'. High = multiple sub-windows agree,
    large effect, mechanism plausible. Medium = one window or one regime.
    Low = single position, single trade, or noisy signal.
  - caveats: known failure modes, regime dependencies, hidden tilts. When
    does this break? What might be confounding it? One sentence. Use "none"
    only if you're truly confident there are no obvious caveats.
  - implication: what the NEXT iteration should DO about this. Actionable,
    not "consider X" — specific: "drop the quality overlay", "cap energy
    exposure to 10%", "use ret_12_1m only, not ret_3m". One sentence.
  - is_forward_looking: true if the claim PREDICTS future behavior (so a later
    experiment could falsify it). False for backward-looking observations
    ("this run lost money on energy") — those are not falsifiable.
  - universe: sector slug or "global". Use the experiment's universe when
    the claim is universe-specific; "global" when it transcends sectors.
  - test_spec: REQUIRED for kind=factor_interaction — the machine-testable form
    of the claim, so the pipeline can validate it out-of-sample, per regime.
    Shape: {"primary_factor": "<factor>", "conditioning_factor": "<factor>",
            "horizon_days": 63, "hypothesis": "cheap_beats_expensive"}
    hypothesis ∈ {cheap_beats_expensive, expensive_beats_cheap, low_beats_high,
    high_beats_low}. If you cannot express the interaction as a test_spec, it is
    a platitude — do not emit it as a factor_interaction.
  - kind=regime: propose a NEW market regime to condition lessons on, when the
    existing regimes don't capture a state you think matters (e.g. rising-rate,
    curve-inverted, credit-stress). test_spec IS a regime_config of point-in-time
    macro rules: {"name": "rising_rate", "entry_conditions": [{"series":
    "treasury_10y", "operator": ">", "value": 4.0}], "entry_logic": "all",
    "exit_conditions": [...], "exit_logic": "all"}. It only becomes usable if it
    is PIT-expressible, RECURS (≥3 distinct episodes), and actually separates
    forward market returns — a one-off period gets rejected. Series must be ones
    that exist in the macro tables (vix, treasury_*, hy_spread, spx_vs_200dma_pct,
    real_fed_funds, dxy, core_pce_yoy, …).

Be skeptical. Distinguish noise from signal. If the result was driven by one
position or one window, say so in caveats AND lower confidence accordingly.
Do NOT inflate confidence to make claims sound stronger — false confidence is
the most expensive mistake an analyst can make.

Output format (EXACTLY this structure, no commentary outside the tags):

<memo>
# (markdown memo here — 200-500 words, use sections)
</memo>

<items>
[
  {
    "kind": "...",
    "claim": "...",
    "mechanism": "...",
    "evidence_summary": "...",
    "confidence": "high|medium|low",
    "caveats": "...",
    "implication": "...",
    "is_forward_looking": true,
    "universe": "..."
  },
  ...
]
</items>
"""


def _short_id(*parts: str) -> str:
    raw = ":".join(parts) + ":" + datetime.now(timezone.utc).isoformat()
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _build_user_prompt(experiment_id: str,
                       thesis: dict,
                       trade_log: dict,
                       contrib: dict,
                       ts: dict,
                       attribution: dict | None) -> str:
    """Render the per-experiment data block. Kept compact so the prompt fits
    in cache-friendly bounds; verbose tables are summarized rather than dumped."""
    parts: list[str] = [f"# Experiment {experiment_id}\n"]

    # Thesis
    parts.append("## Thesis & objective")
    parts.append(f"- run_id: {thesis.get('run_id')}, iteration: {thesis.get('iteration')}")
    parts.append(f"- target: {thesis.get('target_metric')} = {thesis.get('target_value')}")
    parts.append(f"- window: {thesis.get('window')}")
    parts.append(f"- initial_capital: ${thesis.get('initial_capital'):,.0f}")
    parts.append(f"- thesis: {thesis.get('thesis') or '(none)'}")
    if thesis.get("assumptions"):
        parts.append(f"- assumptions: {thesis.get('assumptions')}")
    parts.append("")

    # NAV / drawdown summary
    summary = ts.get("summary", {}) if "error" not in ts else {}
    parts.append("## Performance summary")
    if summary:
        parts.append(f"- total_return: {summary.get('total_return_pct')}%")
        parts.append(f"- final_nav: ${summary.get('final_nav', 0):,.0f}")
        parts.append(f"- max_drawdown: {summary.get('max_drawdown_pct')}% on {summary.get('max_drawdown_date')}")
        parts.append(f"- n_days: {ts.get('window', {}).get('n_days')}")
    else:
        parts.append(f"- (timeseries unavailable: {ts.get('error')})")
    parts.append("")

    # Position contribution
    parts.append("## Position contribution (realized P&L)")
    parts.append(f"- {contrib.get('n_symbols')} distinct symbols, {contrib.get('n_open_positions')} still open at end")
    parts.append("- Top 5 winners:")
    for w in contrib.get("winners_top5") or []:
        parts.append(f"    - {w['symbol']}: P&L=${w['total_pnl']:,.0f}, "
                     f"n={w['n_round_trips']}, avg_pct={w['avg_pnl_pct']}%, "
                     f"win_rate={w['win_rate']}, avg_days_held={w['avg_days_held']}")
    parts.append("- Top 5 losers:")
    for l in contrib.get("losers_top5") or []:
        parts.append(f"    - {l['symbol']}: P&L=${l['total_pnl']:,.0f}, "
                     f"n={l['n_round_trips']}, avg_pct={l['avg_pnl_pct']}%, "
                     f"win_rate={l['win_rate']}, avg_days_held={l['avg_days_held']}")
    parts.append("")

    # Trade log: don't dump every row. Send a sample + reason breakdown.
    parts.append("## Trade activity")
    parts.append(f"- total trades: {trade_log.get('n_trades_total')}")
    trades = trade_log.get("trades") or []
    if trades:
        reasons: dict[str, int] = {}
        for t in trades:
            r = t.get("reason") or "?"
            reasons[r] = reasons.get(r, 0) + 1
        parts.append(f"- reason breakdown: {dict(sorted(reasons.items(), key=lambda kv: -kv[1]))}")
        parts.append("- Sample first 5 + last 5 trades:")
        sample = trades[:5] + (trades[-5:] if len(trades) > 5 else [])
        for t in sample:
            parts.append(f"    {t['date']} {t['action']:4s} {t['symbol']:6s} "
                         f"sh={t['shares']:.1f} px=${t['price']:.2f} "
                         f"pnl=${t.get('pnl') or 0:,.0f} ({t.get('reason')})")
    parts.append("")

    # Factor attribution
    parts.append("## Factor attribution (alpha decomposition)")
    if attribution and "error" not in attribution:
        alpha = attribution.get("alpha", {})
        parts.append(f"- benchmark: {attribution['benchmark']['label']}")
        parts.append(f"- alpha_log_total: {alpha.get('log_pp')} pp  (ann: {alpha.get('log_ann_pp')} pp)")
        parts.append(f"- residual: {attribution.get('residual_log_pp')} pp")
        parts.append(f"- fraction explained by factors: {attribution.get('fraction_explained')}")
        parts.append(f"- attribution universe: {attribution.get('attribution_universe')}")
        parts.append("- Per-factor contributions (contribution_log_pp):")
        factors = attribution.get("factors", {})
        ranked = sorted(
            [(f, d.get("contribution_log_pp")) for f, d in factors.items()
             if d.get("contribution_log_pp") is not None],
            key=lambda kv: abs(kv[1] or 0), reverse=True,
        )
        for f, c in ranked[:10]:
            d = factors[f]
            parts.append(f"    - {f}: c={c} pp  (z={d.get('exposure_z')}, "
                         f"factor_return={d.get('factor_log_return_pp')} pp, "
                         f"category={d.get('category')})")
    else:
        parts.append(f"- (attribution unavailable: {attribution.get('error') if attribution else 'not computed'})")
    parts.append("")

    parts.append("---")
    parts.append("Write the memo + items now.")
    return "\n".join(parts)


_MEMO_RE = re.compile(r"<memo>\s*(.*?)\s*</memo>", re.DOTALL)
_ITEMS_RE = re.compile(r"<items>\s*(.*?)\s*</items>", re.DOTALL)


def _parse_response(text: str) -> tuple[str, list[dict]]:
    memo_match = _MEMO_RE.search(text)
    items_match = _ITEMS_RE.search(text)
    memo = memo_match.group(1).strip() if memo_match else text.strip()
    items: list[dict] = []
    if items_match:
        try:
            items = json.loads(items_match.group(1).strip())
            if not isinstance(items, list):
                items = []
        except json.JSONDecodeError:
            items = []
    return memo, items


async def _call_opus(system_prompt: str, user_prompt: str,
                     model: str = DEFAULT_MODEL) -> tuple[str, dict]:
    """One-shot LLM call via claude_agent_sdk. Returns (text, metadata)."""
    from claude_agent_sdk import query, ClaudeAgentOptions

    opts: dict[str, Any] = {
        "model": model,
        "system_prompt": system_prompt,
        "allowed_tools": [],
        "max_turns": 1,
        "permission_mode": "default",
    }
    if model == "claude-opus-4-7":
        opts["thinking"] = {"type": "adaptive"}

    pieces: list[str] = []
    result_text: str | None = None
    async for message in query(prompt=user_prompt,
                                options=ClaudeAgentOptions(**opts)):
        if hasattr(message, "result") and message.result:
            result_text = message.result
        elif type(message).__name__ == "AssistantMessage":
            content = getattr(message, "content", [])
            for block in (content if isinstance(content, list) else [content]):
                if type(block).__name__ == "TextBlock":
                    pieces.append(block.text)
    full = result_text if result_text is not None else "\n".join(pieces)
    return full, {}


def _persist_attribution(attribution: dict, experiment_id: str, run_id: str) -> None:
    """Cache attribution output. Idempotent (REPLACE)."""
    if not attribution or "error" in attribution:
        return
    contributions = {
        f: d.get("contribution_log_pp")
        for f, d in (attribution.get("factors") or {}).items()
    }
    n_factors = sum(1 for v in contributions.values() if v is not None)
    method = attribution.get("diagnostics", {}).get("factor_return_path", "unknown")
    universe = attribution.get("attribution_universe")
    app = get_db()
    app.execute(
        """INSERT OR REPLACE INTO attributions
           (experiment_id, run_id, universe, alpha_log_total, residual,
            contributions, method, n_factors, computed_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (experiment_id, run_id, universe,
         attribution.get("alpha", {}).get("log_pp"),
         attribution.get("residual_log_pp"),
         json.dumps(contributions),
         method, n_factors,
         datetime.now(timezone.utc).isoformat()),
    )
    app.commit()
    app.close()


def _persist_memo_and_items(experiment_id: str, run_id: str,
                            content: str, items: list[dict],
                            universe_default: str | None,
                            model: str, tokens_in: int, tokens_out: int,
                            duration_seconds: float) -> None:
    """Replace prior memo + items for this experiment. Idempotent."""
    now = datetime.now(timezone.utc).isoformat()
    app = get_db()
    # Memo: PRIMARY KEY = experiment_id, so REPLACE overwrites cleanly.
    app.execute(
        """INSERT OR REPLACE INTO analyst_memos
           (experiment_id, run_id, content, model, tokens_in, tokens_out,
            duration_seconds, created_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (experiment_id, run_id, content, model, tokens_in, tokens_out,
         duration_seconds, now),
    )
    # Items: clear old, insert new.
    app.execute("DELETE FROM memo_items WHERE experiment_id = ?", (experiment_id,))
    for item in items:
        if not isinstance(item, dict):
            continue
        claim = item.get("claim")
        kind = item.get("kind")
        if not claim or not kind:
            continue
        item_id = _short_id(experiment_id, str(kind), str(claim))
        confidence = item.get("confidence")
        if isinstance(confidence, str):
            confidence = confidence.strip().lower() or None
            if confidence not in ("high", "medium", "low"):
                confidence = None
        else:
            confidence = None
        # A factor-interaction claim carrying a test_spec enters as a `candidate`
        # for the validation pipeline (lesson_pipeline.validate_candidate_lessons).
        ts = item.get("test_spec")
        test_spec_json = json.dumps(ts) if isinstance(ts, dict) else None
        vstatus = "candidate" if (test_spec_json and kind in ("factor_interaction", "regime")) else None
        app.execute(
            """INSERT INTO memo_items
               (id, experiment_id, run_id, universe, kind, claim,
                mechanism, evidence_summary, confidence, caveats, implication,
                is_forward_looking, scope_level, promotion_count,
                falsified, test_spec, validation_status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (item_id, experiment_id, run_id,
             item.get("universe") or universe_default,
             kind, claim,
             item.get("mechanism") or None,
             item.get("evidence_summary") or None,
             confidence,
             item.get("caveats") or None,
             item.get("implication") or None,
             1 if item.get("is_forward_looking") else 0,
             "run", 1, 0, test_spec_json, vstatus, now, now),
        )
    app.commit()
    app.close()


async def analyst_pass(experiment_id: str,
                       model: str = DEFAULT_MODEL,
                       ) -> dict[str, Any]:
    """Run the post-trade analyst on one experiment.

    Steps:
      1. Gather thesis, trade log, position contributions, NAV time series.
      2. Compute factor attribution and cache it.
      3. Ask Opus to write a memo + structured items.
      4. Persist memo and items.

    Async because the underlying LLM call uses claude_agent_sdk.query() and
    the runner already runs inside an asyncio loop. For sync callers (CLI,
    scripts, REPL), use `analyst_pass_sync(...)`.
    """
    t0 = time.time()
    thesis = read_thesis(experiment_id)
    if thesis.get("error"):
        return {"experiment_id": experiment_id, "error": thesis["error"]}

    run_id = thesis["run_id"]
    trade_log = get_trade_log(experiment_id, max_rows=200)
    contrib = get_position_contribution(experiment_id)
    ts = get_portfolio_timeseries(experiment_id, max_points=60)
    try:
        attribution = compute_attribution(experiment_id)
    except Exception as e:
        attribution = {"error": f"compute_attribution raised: {e}"}

    _persist_attribution(attribution, experiment_id, run_id)

    user_prompt = _build_user_prompt(experiment_id, thesis, trade_log, contrib, ts, attribution)
    text, _meta = await _call_opus(SYSTEM_PROMPT, user_prompt, model=model)
    memo, items = _parse_response(text)

    universe_default = (
        attribution.get("attribution_universe")
        if isinstance(attribution, dict) and "error" not in attribution
        else None
    )
    duration = time.time() - t0
    _persist_memo_and_items(
        experiment_id, run_id, memo, items, universe_default,
        model=model, tokens_in=0, tokens_out=0,
        duration_seconds=duration,
    )

    return {
        "experiment_id": experiment_id,
        "run_id": run_id,
        "memo_chars": len(memo),
        "n_items": len(items),
        "duration_seconds": round(duration, 2),
    }


def analyst_pass_sync(experiment_id: str,
                      model: str = DEFAULT_MODEL,
                      ) -> dict[str, Any]:
    """Sync wrapper for CLI / scripts that aren't already in an event loop."""
    return asyncio.run(analyst_pass(experiment_id, model=model))


# ---------------------------------------------------------------------------
# Recall helpers — used by MCP tools and by the history renderer
# ---------------------------------------------------------------------------
def recall_memo_items(run_id: str | None = None,
                      experiment_id: str | None = None,
                      universe: str | None = None,
                      kind: str | None = None,
                      scope_level: str | None = None,
                      forward_looking_only: bool = True,
                      include_falsified: bool = False,
                      validated_only: bool = False,
                      limit: int = 20) -> list[dict]:
    """Query memo_items by filter. Sorted by promotion_count DESC, recency DESC.

    Defaults bias toward forward-looking, non-falsified claims — what a future
    iteration should actually act on.
    """
    where = []
    params: list[Any] = []
    if run_id:
        where.append("run_id = ?"); params.append(run_id)
    if experiment_id:
        where.append("experiment_id = ?"); params.append(experiment_id)
    if universe:
        where.append("universe = ?"); params.append(universe)
    if kind:
        where.append("kind = ?"); params.append(kind)
    if scope_level:
        where.append("scope_level = ?"); params.append(scope_level)
    if forward_looking_only:
        where.append("is_forward_looking = 1")
    if not include_falsified:
        where.append("falsified = 0")
    if validated_only:
        where.append("validation_status IN "
                     "('unconditional', 'validated', 'validated_conditional', 'regime_reversing')")
    cols = """id, experiment_id, run_id, universe, kind, claim,
              mechanism, evidence_summary, confidence, caveats, implication,
              is_forward_looking, scope_level, promotion_count,
              falsified, falsified_reason, falsified_by_experiment,
              validation_status, validated_confidence, regime_conditions,
              validation_windows, last_validated_at, test_spec,
              created_at, updated_at"""

    def _run(select_cols):
        sql = f"SELECT {select_cols} FROM memo_items"
        if where:
            sql_w = sql + " WHERE " + " AND ".join(where)
        else:
            sql_w = sql
        sql_w += " ORDER BY promotion_count DESC, created_at DESC LIMIT ?"
        return app.execute(sql_w, params + [limit]).fetchall()

    app = get_db()
    try:
        rows = _run(cols)
    except sqlite3.OperationalError:
        # validation_windows not migrated yet on this DB — degrade gracefully.
        rows = _run(cols.replace("validation_windows, ", ""))
    app.close()
    return [dict(r) for r in rows]


def read_memo(experiment_id: str) -> dict[str, Any]:
    """Fetch the full markdown memo for one experiment."""
    app = get_db()
    row = app.execute(
        """SELECT experiment_id, run_id, content, model, tokens_in, tokens_out,
                  duration_seconds, created_at
           FROM analyst_memos WHERE experiment_id = ?""", (experiment_id,)
    ).fetchone()
    app.close()
    if not row:
        return {"error": f"no memo for experiment {experiment_id}"}
    return dict(row)


def render_memo_items_for_experiment(experiment_id: str,
                                     limit: int = 20) -> str:
    """Compact per-experiment block of analyst items.

    Returns "" if there are no items so the caller can append unconditionally.
    Header-less; caller is expected to render under an experiment subheader.
    """
    items = recall_memo_items(
        experiment_id=experiment_id,
        forward_looking_only=True, include_falsified=False, limit=limit,
    )
    if not items:
        return ""
    lines = ["**Analyst observations on this experiment:**"]
    for it in items:
        conf = it.get("confidence")
        conf_tag = f" ({conf} confidence)" if conf else ""
        lines.append(
            f"- [{it['kind']}, {it.get('universe') or 'global'}]{conf_tag} {it['claim']}"
        )
        vline = _validation_line(it)
        if vline:
            lines.append(f"  - {vline}")
        if it.get("mechanism"):
            lines.append(f"  - Mechanism: {it['mechanism']}")
        if it.get("evidence_summary"):
            lines.append(f"  - Evidence: {it['evidence_summary']}")
        if it.get("caveats") and it["caveats"].strip().lower() != "none":
            lines.append(f"  - Caveats: {it['caveats']}")
        if it.get("implication"):
            lines.append(f"  - Implication: {it['implication']}")
    return "\n".join(lines)


def _validation_line(it: dict) -> str | None:
    """One-line validation-status tag for a memo item, or None if not applicable.

    Only factor-interaction claims carry a validation_status. The tag tells the
    trader whether the claim survived out-of-sample testing — and in which
    regimes — so it can weight a proven lesson over an untested hypothesis.
    """
    status = it.get("validation_status")
    if not status:
        return None
    vconf = it.get("validated_confidence")
    vconf_tag = f", OOS confidence {vconf}" if vconf else ""
    # regime_conditions is the validator's human-readable summary, e.g.
    # "holds in risk_off (+12.3% ann, t=2.1); REVERSES in calm_uptrend (...)".
    regimes = (it.get("regime_conditions") or "").strip()
    regime_tag = f" [{regimes}]" if regimes else ""
    # validation_windows is the per-window IS/OOS panel, e.g. "IS:+8%(t2.1) OOS:+9%(t2.0)".
    windows = (it.get("validation_windows") or "").strip()
    win_tag = f" Windows: {windows}." if windows else ""
    if status == "candidate":
        return ("⚠ Validation: UNVALIDATED candidate — not yet tested out-of-sample. "
                "Treat as an untested hypothesis, not evidence.")
    if status == "unconditional":
        return (f"✓ Validation: UNCONDITIONAL — holds across all regimes{vconf_tag}{regime_tag}. "
                f"Regime adds no conditioning — apply always (most robust).{win_tag}")
    if status == "regime_reversing":
        return (f"⚠ Validation: REGIME-DEPENDENT — the sign FLIPS by regime{regime_tag}. "
                f"The pooled effect HIDES this; only trade it conditional on the active regime.{win_tag}")
    if status == "validated":
        return f"✓ Validation: held out-of-sample{vconf_tag}{regime_tag}.{win_tag}"
    if status == "validated_conditional":
        return (f"✓ Validation: held out-of-sample only CONDITIONALLY{vconf_tag}{regime_tag}. "
                f"Applies only when that regime currently holds.{win_tag}")
    # rejected / falsified-in-validation: surface it plainly.
    return f"✗ Validation: did NOT hold out-of-sample (status={status}){regime_tag}. Do not rely on this."
