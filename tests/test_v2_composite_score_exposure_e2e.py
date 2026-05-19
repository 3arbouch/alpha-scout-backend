#!/usr/bin/env python3
"""
V2 composite_score exposure + math reproducibility.

Three surfaces, one capture point:

  1. config.sleeves[i].ranking_model — the static formula (Phase A).
  2. trade.signal_detail.ranking      — per-pick provenance on every BUY (Phase B).
  3. result.ranking_history            — per (date, sleeve) event for the
                                          ranking-explorer endpoint (Phase C).

The math invariant (Phase D):

  engine_score(s) ≈ Σ_b weight_normalized · mean(z_signed for f in bucket where z is not None)

verified to within 1e-9 on every (event, candidate) tuple. If this ever
fires, the composite_score math has drifted and the audit story is broken.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend-dev/data/market_dev.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    PYTHONPATH=../scripts python3 test_v2_composite_score_exposure_e2e.py
"""
import copy
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

from portfolio_engine_v2 import run_portfolio_backtest as run_v2

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} — {detail}")


# ---------------------------------------------------------------------------
# Fixture: single sleeve, composite_score ranking, quarterly rebalance →
# multiple ranking events without making the run heavy.
# ---------------------------------------------------------------------------
UNIVERSE = ["AAPL", "MSFT", "NVDA", "GOOG", "META", "AMZN", "AVGO", "AMD",
            "INTC", "MU", "QCOM", "ADBE", "CRM", "ORCL", "NFLX", "TXN"]


def composite_config(start="2024-01-01", end="2025-06-30"):
    return {
        "name": "CompositeExposureTest",
        "sleeves": [{
            "label": "Core", "weight": 1.0, "regime_gate": [],
            "strategy_config": {
                "name": "Core", "universe": {"type": "symbols", "symbols": UNIVERSE},
                "entry": {"conditions": [{"type": "always"}], "logic": "all"},
                "ranking": {"by": "composite_score", "order": "desc", "top_n": 5},
                "composite_score": {
                    "buckets": {
                        "quality":  {"factors": [{"name": "op_margin"},
                                                  {"name": "roic"}],
                                      "weight": 1.0},
                        "momentum": {"factors": [{"name": "ret_12_1m"},
                                                  {"name": "ret_6m"}],
                                      "weight": 1.0},
                    },
                    "standardization": "rank",
                },
                "rebalancing": {"frequency": "quarterly",
                                 "rules": {"max_position_pct": 25}},
                "sizing": {"type": "equal_weight", "max_positions": 5,
                            "initial_allocation": 1_000_000},
                "stop_loss": {"type": "drawdown_from_entry", "value": -25,
                               "cooldown_days": 60},
                "backtest": {"start": start, "end": end,
                              "entry_price": "next_close", "slippage_bps": 10},
            },
        }],
        "backtest": {"start": start, "end": end, "initial_capital": 1_000_000},
    }


# ---------------------------------------------------------------------------
print("\n=== Running v2 backtest with composite_score ranking ===")
result = run_v2(copy.deepcopy(composite_config()))

config_out = result.get("config", {})
trades = result["trades"]
ranking_history = result.get("ranking_history", [])
sleeves_out = config_out.get("sleeves", [])

print(f"  trades:                    {len(trades)}")
print(f"  ranking_history events:    {len(ranking_history)}")
print(f"  sleeves in config_out:     {len(sleeves_out)}")


# ---------------------------------------------------------------------------
print("\n=== 1. Phase A: ranking_model on each sleeve ===")
sleeve = sleeves_out[0] if sleeves_out else {}
rm = sleeve.get("ranking_model")
check("sleeve has ranking_model field", rm is not None)
if rm:
    check("ranking_model.type == composite_score",
          rm.get("type") == "composite_score",
          f"got {rm.get('type')!r}")
    check("ranking_model.standardization == rank",
          rm.get("standardization") == "rank")
    check("ranking_model.top_n == 5", rm.get("top_n") == 5)
    check("ranking_model.formula is present and non-empty",
          isinstance(rm.get("formula"), str) and len(rm["formula"]) > 0)
    buckets = rm.get("buckets") or []
    check("ranking_model has 2 buckets (quality, momentum)",
          len(buckets) == 2, f"got {len(buckets)}")
    # Normalized weights sum to 1.0
    norm_sum = sum(b.get("weight_normalized", 0) for b in buckets)
    check("bucket weight_normalized sums to 1.0",
          abs(norm_sum - 1.0) < 1e-9, f"got {norm_sum}")
    # Each bucket has a label, weight, weight_normalized, factors
    for b in buckets:
        check(f"bucket '{b.get('name')}' has label",
              isinstance(b.get("label"), str) and b["label"])
        check(f"bucket '{b.get('name')}' has factors",
              isinstance(b.get("factors"), list) and len(b["factors"]) > 0)
        for f in b["factors"]:
            check(f"factor '{f.get('name')}' has label",
                  isinstance(f.get("label"), str) and f["label"],
                  f"got {f}")
            check(f"factor '{f.get('name')}' has sign",
                  f.get("sign") in ("+", "-"),
                  f"got {f.get('sign')!r}")


# ---------------------------------------------------------------------------
print("\n=== 2. Phase B: signal_detail.ranking on every BUY trade ===")
buys = [t for t in trades if t.get("action") == "BUY" and t.get("reason") == "entry"]
buys_with_ranking = [t for t in buys if isinstance(t.get("signal_detail"), dict)
                       and "ranking" in t["signal_detail"]]
check(f"at least one BUY trade emitted ({len(buys)} BUYs)", len(buys) > 0)
check("every entry BUY has signal_detail.ranking",
      len(buys_with_ranking) == len(buys),
      f"{len(buys_with_ranking)} of {len(buys)}")

if buys_with_ranking:
    sample = buys_with_ranking[0]["signal_detail"]["ranking"]
    print(f"\n  sample ranking block on {buys_with_ranking[0]['symbol']}:")
    print(f"    by={sample.get('by')}  rank={sample.get('rank')}/{sample.get('out_of')}  score={sample.get('score'):.4f}")
    check("ranking.by == composite_score on sample",
          sample.get("by") == "composite_score")
    check("ranking has rank, out_of, score",
          sample.get("rank") is not None and sample.get("out_of") is not None
          and sample.get("score") is not None)
    check("ranking has buckets dict", isinstance(sample.get("buckets"), dict))


# ---------------------------------------------------------------------------
print("\n=== 3. Phase C: ranking_history shape ===")
check("ranking_history non-empty", len(ranking_history) > 0)
if ranking_history:
    ev = ranking_history[0]
    check("event has date / sleeve_label / candidates",
          all(k in ev for k in ("date", "sleeve_label", "candidates")))
    cands = ev.get("candidates") or []
    selected = [c for c in cands if c.get("selected")]
    check("event has n_candidates == len(candidates)",
          ev.get("n_candidates") == len(cands))
    check("event has at least one selected candidate", len(selected) > 0)
    # Rank ordering — selected candidates should be at the top by rank.
    if cands:
        ranks = [c["rank"] for c in cands if c.get("rank") is not None]
        check("ranks are 1-indexed and contiguous",
              ranks == list(range(1, len(ranks) + 1)),
              f"first 5 ranks: {ranks[:5]}")
        check("all selected candidates have rank <= top_n_cutoff",
              all(c["rank"] <= (ev.get("top_n_cutoff") or len(cands))
                  for c in selected))


# ---------------------------------------------------------------------------
print("\n=== 4. Phase D: math reproducibility invariant ===")
# For every ranking event, every scored candidate:
#   engine_score ≈ Σ_b weight_normalized · mean(z_signed for f in bucket where z is not None)
violations = 0
checked_rows = 0
for ev in ranking_history:
    if ev.get("by") != "composite_score":
        continue
    for cand in ev.get("candidates", []):
        engine_score = cand.get("score")
        buckets = cand.get("buckets")
        if engine_score is None or not isinstance(buckets, dict):
            continue
        repro = 0.0
        any_factor = False
        for _bname, bd in buckets.items():
            wn = bd.get("weight_normalized") or 0
            factors = bd.get("factors") or {}
            z_signed_vals = [
                fd["z_signed"] for fd in factors.values()
                if fd.get("z_signed") is not None
            ]
            if not z_signed_vals:
                continue
            any_factor = True
            bucket_z = sum(z_signed_vals) / len(z_signed_vals)
            repro += wn * bucket_z
        if not any_factor:
            continue
        checked_rows += 1
        if abs(repro - engine_score) > 1e-9:
            violations += 1
            if violations <= 3:
                print(f"    VIOLATION  {ev.get('date')} {cand.get('symbol')}: "
                      f"engine={engine_score:.9f} repro={repro:.9f} delta={repro-engine_score:.2e}")

check(f"reproduced engine score from buckets on every row ({checked_rows} checked)",
      violations == 0, f"{violations} violations")


# ---------------------------------------------------------------------------
print("\n=== 5. Selected candidates are the top-N by rank ===")
# For each event, the candidates marked selected==True should be exactly
# the top n_selected by rank.
mismatches = 0
for ev in ranking_history:
    cands = ev.get("candidates") or []
    n_sel = ev.get("n_selected") or 0
    if not cands or n_sel == 0:
        continue
    top = sorted(cands, key=lambda c: (c.get("rank") is None, c.get("rank")))[:n_sel]
    if not all(c.get("selected") for c in top):
        mismatches += 1
check("selected candidates == top-N by rank on every event",
      mismatches == 0, f"{mismatches} mismatching events")


# ---------------------------------------------------------------------------
print("\n=== 6. Trade-level ranking matches ranking_history for same (date, symbol) ===")
# Build a quick index: {(date, sleeve, sym): score from ranking_history}
index = {}
for ev in ranking_history:
    for cand in ev.get("candidates", []):
        index[(ev["date"], ev["sleeve_label"], cand["symbol"])] = cand.get("score")
trade_vs_event_mismatches = 0
checked_trades = 0
for t in buys_with_ranking:
    rk = t["signal_detail"]["ranking"]
    key = (t["date"], t["sleeve_label"], t["symbol"])
    event_score = index.get(key)
    if event_score is None or rk.get("score") is None:
        continue
    checked_trades += 1
    if abs(event_score - rk["score"]) > 1e-9:
        trade_vs_event_mismatches += 1
check(f"trade.ranking.score == ranking_history candidate score ({checked_trades} trades)",
      trade_vs_event_mismatches == 0,
      f"{trade_vs_event_mismatches} mismatches")


# ---------------------------------------------------------------------------
print(f"\n{'='*60}\nPASSED: {PASS}\nFAILED: {FAIL}\n{'='*60}")
sys.exit(0 if FAIL == 0 else 1)
