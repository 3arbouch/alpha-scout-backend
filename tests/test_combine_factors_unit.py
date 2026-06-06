"""
Unit test for alpha_combine.combine_factors (Piece 0 + 1).

Builds a tiny synthetic market DB with ONE planted factor that equals the
forward return (so it must be recovered with sign '+' and high OOS IC) and one
pure-noise factor, then checks the tool's invariants:
  - returns a well-formed composite_score block; weights sum to 1
  - the planted factor gets sign '+' and the larger weight
  - combined OOS IC is strongly positive (signal recovered out-of-sample)
  - the noise factor gets ~0 weight / IC

Run: python3 tests/test_combine_factors_unit.py
"""
import os, sys, tempfile, sqlite3
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from alpha_combine import combine_factors

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  ✅ {name}")
    else: FAIL += 1; print(f"  ❌ {name} — {detail}")


def build_synth_db(path, n_sym=40, n_days=400, H=21):
    rng = np.random.default_rng(0)
    dates = [f"2018-{1+(i//28)%12:02d}-{1+i%28:02d}" for i in range(n_days)]
    # ensure strictly increasing unique date strings
    dates = [f"20{18+i//350:02d}-{1+(i//28)%12:02d}-{1+i%28:02d}" for i in range(n_days)]
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE prices (symbol TEXT, date TEXT, close REAL)")
    con.execute("CREATE TABLE features_daily (symbol TEXT, date TEXT, good REAL, noise REAL)")
    for s in range(n_sym):
        sym = f"S{s:03d}"
        rets = rng.normal(0, 0.02, n_days)
        close = 100 * np.cumprod(1 + rets)
        con.executemany("INSERT INTO prices VALUES (?,?,?)",
                        [(sym, dates[i], float(close[i])) for i in range(n_days)])
        rows = []
        for i in range(n_days):
            if i + H < n_days:
                fwd = close[i + H] / close[i] - 1.0      # the exact label combine_factors will compute
                good = fwd + rng.normal(0, 1e-4)         # planted: good ≈ forward return
            else:
                good = None                              # no label → dropped
            rows.append((sym, dates[i], good, float(rng.normal())))
        con.executemany("INSERT INTO features_daily VALUES (?,?,?,?)", rows)
    con.commit(); con.close()
    return dates


print("=== combine_factors: planted-signal recovery + invariants ===")
tmp = tempfile.mkdtemp(); db = os.path.join(tmp, "synth.db")
dates = build_synth_db(db)
res = combine_factors(["good", "noise"], horizon="21d", method="ic_weighted",
                      shrinkage=0.1, sector=None, start=dates[0], end=dates[-1], db_path=db)

check("no error / returns composite_score block",
      "error" not in res and "composite_score" in res, str(res)[:200])
if "composite_score" in res:
    d = res["diagnostics"]
    w = {p["factor"]: p["weight"] for p in d["per_factor"]}
    sgn = {p["factor"]: p["sign"] for p in d["per_factor"]}
    check("weights sum to 1", abs(sum(w.values()) - 1.0) < 1e-6, f"sum={sum(w.values())}")
    check("planted factor 'good' has sign '+'", sgn["good"] == "+", f"got {sgn}")
    check("planted factor outweighs noise", w["good"] > w["noise"], f"got {w}")
    check("combined OOS IC strongly positive (signal recovered)",
          d["combined_ic_oos"] is not None and d["combined_ic_oos"] > 0.5,
          f"OOS={d['combined_ic_oos']}")
    check("equal-weight OOS baseline present", d["equal_weight_ic_oos"] is not None)
    check("per-factor IC: good >> noise",
          abs([p for p in d["per_factor"] if p["factor"] == "good"][0]["rank_ic"]) >
          abs([p for p in d["per_factor"] if p["factor"] == "noise"][0]["rank_ic"]) + 0.3,
          str(d["per_factor"]))

print("\n" + "=" * 56)
print(f"PASSED: {PASS}\nFAILED: {FAIL}")
print("=" * 56)
sys.exit(0 if FAIL == 0 else 1)
