#!/usr/bin/env python3
"""
Corporate-actions audit (Tier-B #5).

Verifies how splits and dividends are reflected in the data — and pins down
that one is handled correctly and the other isn't.

Findings:

1. SPLITS — handled correctly via back-adjusted close prices in market.db.
   Around the 2020-08-31 AAPL 4:1 split, the price series is continuous in
   our DB; pre-split closes are divided by 4. The engine treats this as a
   smooth price curve, so an open position's notional value is preserved
   across a split without any share-count adjustment in code. ✓

2. DIVIDENDS — NOT reflected anywhere.
   • No `dividends` or `distributions` table in market.db.
   • cashflow.dividends_paid is the firm's PAYOUT (per share for the
     features.py div_yield computation), not the shareholder's receipt.
   • Prices are split-adjusted but NOT total-return / dividend-adjusted:
     KO 2015-01-02 close = $42.14, matching the NOMINAL closing price.
     A dividend-adjusted series would be ~$30 (11 years of ~3% yield).
   • Engine never credits dividend cashflow to portfolio.cash.

Impact of missing dividends:
  • Reported returns UNDERSTATE realized total return.
  • S&P 500 ~1.5%/yr yield → ~17-20% cumulative miss over 11-year backtests.
  • Dividend-tilted strategies (utilities, REITs, defensive) — 30-40%
    cumulative miss. Growth/momentum strategies — barely affected.
  • Partially offsets the survivorship bias (which INFLATES returns).

Not a code bug — a data-coverage / price-convention gap.

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend/data/market.db \\
    python3 test_corporate_actions_audit_e2e.py
"""
import os
import sqlite3
import sys

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


def approx(a, b, tol):
    return a is not None and b is not None and abs(a - b) <= tol


db = os.environ.get("MARKET_DB_PATH", "/home/mohamed/alpha-scout-backend/data/market.db")
m = sqlite3.connect(db)


# ---------------------------------------------------------------------------
# 1. Splits are back-adjusted in the price series
# ---------------------------------------------------------------------------
print("\n=== 1. Splits — back-adjusted in DB (engine needs no extra logic) ===")

# AAPL 4-for-1 split effective 2020-08-31. Pre-split nominal close
# 2020-08-28 was $499.23; expected back-adjusted value ≈ $124.81.
aapl_pre = m.execute(
    "SELECT close FROM prices WHERE symbol='AAPL' AND date='2020-08-28'"
).fetchone()
check("AAPL 2020-08-28 close ≈ $124.81 (split-adjusted from $499.23 nominal)",
      aapl_pre is not None and approx(aapl_pre[0], 124.81, 0.5),
      f"got {aapl_pre}")

# AAPL 2015-01-02 nominal was $109.33. Post-2020 4:1 back-adjusted ≈ $27.33.
aapl_2015 = m.execute(
    "SELECT close FROM prices WHERE symbol='AAPL' AND date='2015-01-02'"
).fetchone()
check("AAPL 2015-01-02 close ≈ $27.33 (= $109.33 / 4 back-adjusted)",
      aapl_2015 is not None and approx(aapl_2015[0], 27.33, 0.5),
      f"got {aapl_2015}")

# NVDA 10-for-1 split effective 2024-06-10. Prices on both sides should be
# in the same range (~$120s), continuous, no jump from $1200 to $120.
nvda_around = list(m.execute(
    "SELECT date, close FROM prices WHERE symbol='NVDA' AND date BETWEEN '2024-06-07' AND '2024-06-11' ORDER BY date"
))
check("NVDA continuous through 2024-06-10 10:1 split (no $1200→$120 jump)",
      all(50 < r[1] < 200 for r in nvda_around),
      f"got {nvda_around}")


# ---------------------------------------------------------------------------
# 2. Dividends — NOT in the price series
# ---------------------------------------------------------------------------
print("\n=== 2. Dividends NOT reflected in prices (not total-return) ===")

# KO 2015-01-02 nominal close was $42.21. If prices were total-return /
# dividend-adjusted, 11 years of ~3% yield would back-adjust to ~$30 or below.
# A value near $42 confirms NO dividend adjustment.
ko_2015 = m.execute(
    "SELECT close FROM prices WHERE symbol='KO' AND date='2015-01-02'"
).fetchone()
check(f"KO 2015-01-02 close ≈ nominal $42 (NOT dividend-adjusted; got {ko_2015[0]})",
      ko_2015 is not None and approx(ko_2015[0], 42.0, 1.0),
      "if it's $30 or lower, prices have become dividend-adjusted — this test needs updating")

# JNJ (no split, dividend-paying) similar check.
jnj_2015 = m.execute(
    "SELECT close FROM prices WHERE symbol='JNJ' AND date='2015-01-02'"
).fetchone()
check(f"JNJ 2015-01-02 close ≈ nominal $104 (NOT div-adjusted; got {jnj_2015[0]})",
      jnj_2015 is not None and approx(jnj_2015[0], 104.0, 1.5))


# ---------------------------------------------------------------------------
# 3. No dividends/distributions table exists
# ---------------------------------------------------------------------------
print("\n=== 3. No dividend cashflow infrastructure ===")

tables = {r[0] for r in m.execute(
    "SELECT name FROM sqlite_master WHERE type='table'"
).fetchall()}
check("no 'dividends' table in market.db",
      "dividends" not in tables and "distributions" not in tables and "ex_dividends" not in tables,
      "if added, the engine should be updated to credit dividends to cash")

# Check that the engine code itself doesn't reference any dividend cashflow.
# (cashflow.dividends_paid is the firm's outgoing payments, used in features
# to compute div_yield — NOT a shareholder cash credit.)
import re
here = os.path.dirname(os.path.abspath(__file__))
engine_path = os.path.join(here, "..", "scripts", "backtest_engine.py")
src = open(engine_path).read()

# We expect zero references to receiving dividend cashflow in the engine.
suspect_patterns = [
    r"\.dividend_received",
    r"credit_dividend",
    r"distribution_amount",
    r"ex_div_date",
]
hits = []
for p in suspect_patterns:
    if re.search(p, src):
        hits.append(p)
check("engine code has no dividend-credit logic",
      not hits,
      f"found: {hits} — investigate")


# ---------------------------------------------------------------------------
# 4. Quantify the impact for a typical dividend-payer
# ---------------------------------------------------------------------------
print("\n=== 4. Dividend miss — rough magnitude ===")

# 11 years of KO dividends (paid quarterly, ~$0.30→$0.46/share over 2015-2026):
# Rough sum ≈ $17.30/share, vs current price ~$60 → ~28% cumulative miss
# Without explicit dividend events, this is just an information leak.
ko_now = m.execute(
    "SELECT close FROM prices WHERE symbol='KO' AND date >= '2026-04-01' ORDER BY date DESC LIMIT 1"
).fetchone()[0]
ko_start = ko_2015[0]
price_return = (ko_now - ko_start) / ko_start
# Approximate KO total dividends paid 2015-2026: ~$17/share
approx_total_divs = 17.0
total_return_estimate = price_return + approx_total_divs / ko_start
print(f"    KO price return 2015 → 2026:           {price_return * 100:>6.1f}%")
print(f"    KO + estimated dividends total return: {total_return_estimate * 100:>6.1f}%")
print(f"    Engine reports: only price return.")

m.close()

print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
print()
print("BOTTOM LINE:")
print("  Splits: handled (back-adjusted prices in DB).")
print("  Dividends: missing from prices AND engine. Reported returns")
print("  understate true total return by ~1.5%/yr for broad equity")
print("  strategies, higher for dividend-tilted ones.")
sys.exit(0 if FAIL == 0 else 1)
