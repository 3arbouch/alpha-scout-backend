#!/usr/bin/env python3
"""
End-to-end: regime proposal → gate → registry → per-window×per-regime panel →
write-back → context render. Runs against the REAL dev market DB (read-only) and
a throwaway in-memory app DB.

Discovery period (training) = 2016–2021; eval/walk-forward windows = 2022–2026
(OOS), plus one window overlapping training (IS) to prove the IS/OOS tag.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    python3 test_lesson_panel_e2e.py
"""
import json
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "auto_trader"))
sys.path.insert(0, os.path.join(HERE, ".."))

import lesson_pipeline as lp
from lesson_validator import validate_lesson_panel
from lesson_pipeline import validate_regime_candidate

MARKET_DB = os.path.join(HERE, "..", "data", "market_dev.db")
PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name} — {detail}")


mkt = sqlite3.connect(MARKET_DB)

# ---- minimal app DB with just memo_items (the columns the pipeline needs) ----
app = sqlite3.connect(":memory:")
app.row_factory = sqlite3.Row
app.execute("""CREATE TABLE memo_items (
    id TEXT PRIMARY KEY, experiment_id TEXT, run_id TEXT, universe TEXT, kind TEXT,
    claim TEXT, mechanism TEXT, evidence_summary TEXT, confidence TEXT, caveats TEXT,
    implication TEXT, is_forward_looking INTEGER DEFAULT 0, scope_level TEXT DEFAULT 'run',
    promotion_count INTEGER DEFAULT 1, falsified INTEGER DEFAULT 0,
    created_at TEXT, updated_at TEXT)""")

RUN = "run_e2e"
EXP = "exp_e2e"

# A factor-interaction lesson candidate (momentum × value double-sort).
app.execute(
    "INSERT INTO memo_items (id, experiment_id, run_id, kind, claim, created_at, updated_at) "
    "VALUES ('lesson1', ?, ?, 'factor_interaction', "
    "'Within high-momentum names, cheap (low ev_ebitda) beats expensive', 'now', 'now')",
    (EXP, RUN))
lp.migrate_memo_items(app)
app.execute("UPDATE memo_items SET test_spec=?, validation_status='candidate' WHERE id='lesson1'",
            (json.dumps({"primary_factor": "ret_12_1m", "conditioning_factor": "ev_ebitda",
                         "horizon_days": 63, "hypothesis": "cheap_beats_expensive"}),))

# Regime proposal A: "elevated_rates" = 10y > 3% — only 2 episodes (2018-19, 2022+),
# so the RECURRENCE gate should REJECT it. Demonstrates the gate firing.
elevated_rates = {
    "name": "elevated_rates",
    "entry_conditions": [{"series": "treasury_10y", "operator": ">", "value": 3.0}],
    "entry_logic": "all",
    "exit_conditions": [{"series": "treasury_10y", "operator": "<", "value": 2.5}],
    "exit_logic": "all",
    "entry_persistence_days": 3, "exit_persistence_days": 3,
}
# Regime proposal B: "high_vol" = vix > 20 — recurs many times (2015-16, 2018, 2020,
# 2022, …), so it should clear recurrence and (likely) conditioning power.
high_vol = {
    "name": "high_vol",
    "entry_conditions": [{"series": "vix", "operator": ">", "value": 20}],
    "entry_logic": "all",
    "exit_conditions": [{"series": "vix", "operator": "<", "value": 17}],
    "exit_logic": "all",
    "entry_persistence_days": 3, "exit_persistence_days": 3,
}
for rid, spec in [("regime1", elevated_rates), ("regime2", high_vol)]:
    app.execute(
        "INSERT INTO memo_items (id, experiment_id, run_id, kind, claim, test_spec, "
        "validation_status, created_at, updated_at) VALUES (?, ?, ?, 'regime', ?, ?, "
        "'candidate', 'now', 'now')",
        (rid, EXP, RUN, f"regime {spec['name']}", json.dumps(spec)))
app.commit()

print("=== regime gate (direct) ===")
gv_e = validate_regime_candidate(elevated_rates, mkt, "2015-01-01", "2026-06-01")
gv_h = validate_regime_candidate(high_vol, mkt, "2015-01-01", "2026-06-01")
print(f"    elevated_rates: {gv_e['status']} — {gv_e.get('reason')}")
print(f"    high_vol:       {gv_h['status']} | episodes={gv_h.get('episodes')} "
      f"days={gv_h.get('active_days')} t={gv_h.get('t_stat')}")
check("elevated_rates REJECTED by recurrence gate (only 2 episodes)",
      gv_e["status"] == "rejected" and "insufficient recurrence" in gv_e.get("reason", ""), str(gv_e))
check("high_vol clears PIT + recurrence (≥3 episodes)",
      gv_h.get("episodes", 0) >= 3, str(gv_h))

print("\n=== validate_candidate_regimes → registry write-back ===")
rsum = lp.validate_candidate_regimes(app, mkt, "2015-01-01", "2026-06-01", run_id=RUN)
print(f"    regime summary: {rsum}")
reg_rows = app.execute("SELECT name, status, episodes FROM lesson_regimes").fetchall()
print(f"    registry: {[dict(r) for r in reg_rows]}")
check("regime registry has both proposals", len(reg_rows) == 2, str(reg_rows))
configs = lp.load_validated_regime_configs(app)
names = [c.get("name") for c in configs]
check("seed regimes always loaded", "risk_off" in names and "calm_uptrend" in names, str(names))
hv_validated = app.execute("SELECT status FROM lesson_regimes WHERE name='high_vol'").fetchone()[0]
check("validated regime (high_vol) loaded into configs iff it passed",
      ("high_vol" in names) == (hv_validated == "validated"), f"{hv_validated} / {names}")
mi_status = app.execute("SELECT validation_status FROM memo_items WHERE id='regime1'").fetchone()[0]
check("rejected regime memo_item written back as rejected", mi_status == "rejected", mi_status)

print("\n=== panel: per-window × per-regime over real eval windows ===")
# IS window overlaps training (2016-2021); the rest are OOS (post-2021).
eval_windows = [
    ("2018-01-01", "2020-01-01", "2018_2020"),   # IS (overlaps training)
    ("2022-01-01", "2024-01-01", "2022_2024"),   # OOS
    ("2024-01-01", "2026-01-01", "2024_2026"),   # OOS
]
train_span = ("2016-01-01", "2021-12-31")
res = validate_lesson_panel(
    {"primary_factor": "ret_12_1m", "conditioning_factor": "ev_ebitda",
     "horizon_days": 63, "hypothesis": "cheap_beats_expensive"},
    mkt, eval_windows, regime_configs=configs, train_span=train_span)
print(f"    n_dates={res['n_dates']}  status={res['verdict']['status']}")
print("    per-window:")
for w in res["per_window"]:
    print(f"      {w['label']:12} {'OOS' if w['is_oos'] else 'IS ':3} "
          f"n={w['n']:>3} {w['mean_ann_pct']:+7.2f}% t={w['t_stat']:+.2f}")
print("    per-regime:")
for r, s in sorted(res["per_regime"].items()):
    print(f"      {r:16} n={s['n']:>3} {s['mean_ann_pct']:+7.2f}% t={s['t_stat']:+.2f}")
print(f"    windows_summary: {res['verdict'].get('windows_summary')}")
print(f"    oos_persistence: {res['verdict'].get('oos_persistence')}")

check("panel produced grid spreads", res["n_dates"] > 0, str(res["n_dates"]))
check("per_window has 3 windows", len(res["per_window"]) == 3, str(len(res["per_window"])))
is_flags = {w["label"]: w["is_oos"] for w in res["per_window"]}
check("2018_2020 tagged IS (overlaps training)", is_flags.get("2018_2020") is False, str(is_flags))
check("2022_2024 tagged OOS", is_flags.get("2022_2024") is True, str(is_flags))
check("a validated agent regime shows up as a panel row",
      ("high_vol" not in names) or ("high_vol" in res["per_regime"]),
      str(list(res["per_regime"].keys())))

print("\n=== full pipeline write-back via validate_candidate_lessons (panel mode) ===")
lsum = lp.validate_candidate_lessons(
    app, mkt, None, None, run_id=RUN,
    eval_windows=eval_windows, train_span=train_span, regime_configs=configs)
print(f"    lesson summary: {lsum}")
row = app.execute(
    "SELECT validation_status, validated_confidence, regime_conditions, validation_windows "
    "FROM memo_items WHERE id='lesson1'").fetchone()
print(f"    lesson1 status={row['validation_status']!r} conf={row['validated_confidence']!r}")
print(f"    regime_conditions: {row['regime_conditions']}")
print(f"    validation_windows: {row['validation_windows']}")
check("lesson got a real verdict",
      row["validation_status"] in ("unconditional", "validated", "validated_conditional",
                                   "regime_reversing", "rejected"),
      row["validation_status"])
check("validation_windows populated", bool(row["validation_windows"]), str(row["validation_windows"]))
check("regime_conditions carries OOS persistence note",
      "OOS" in (row["regime_conditions"] or ""), row["regime_conditions"])

print("\n=== render in trader context (analyst._validation_line) ===")
import importlib
analyst = importlib.import_module("auto_trader.analyst")
line = analyst._validation_line(dict(row))
print(f"    {line}")
check("rendered line non-empty + shows Windows", line and "Windows:" in line, str(line))

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(1 if FAIL else 0)
