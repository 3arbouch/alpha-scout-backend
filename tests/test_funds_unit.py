#!/usr/bin/env python3
"""
Unit tests for the funds layer (scripts/funds.py).

Hermetic: a fake `deploy_engine` (fixed nav_history + position book) is injected
into sys.modules, and APP_DB_PATH points at a throwaway temp DB. No market data,
no real deployment — the funds math is exercised in isolation.

Synthetic deployment:
  nav_history (daily):  100000, 105000, 110000, 115000, 120000
  dates:                06-03(W23,Mon) .. 06-14(W24,Fri)
  base_nav_per_unit=100 → NAV/unit: 100,105,110,115,120 ; return: 0,5,10,15,20%
  position book (pv=100000): AAPL 0.60 @200 (mv 60000), MSFT 0.40 @400 (mv 40000)

Run:
    cd /app && python3 tests/test_funds_unit.py
"""
import os
import sys
import types
import tempfile
import shutil
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

# --- temp DB BEFORE importing funds (it reads APP_DB_PATH at import time) ----
_TMP = tempfile.mkdtemp(prefix="funds_test_")
os.environ["APP_DB_PATH"] = str(Path(_TMP) / "app.db")
os.environ["MARKET_DB_PATH"] = str(Path(_TMP) / "market.db")

# --- fake deploy_engine (funds imports it lazily inside each function) -------
DEPLOY_ID = "dep_test_1"
DEPLOY = {
    "id": DEPLOY_ID,
    "start_date": "2024-06-03",
    "last_evaluated": "2024-06-14",
    "initial_capital": 100000,
    "sleeves": [{"label": "S"}],          # opaque to funds (book is faked)
    "nav_history": [
        {"date": "2024-06-03", "nav": 100000},
        {"date": "2024-06-05", "nav": 105000},
        {"date": "2024-06-07", "nav": 110000},
        {"date": "2024-06-10", "nav": 115000},
        {"date": "2024-06-14", "nav": 120000},
    ],
}
BOOK = {
    "portfolio_value": 100000,
    "positions": [
        {"symbol": "AAPL", "status": "open", "current_price": 200, "market_value": 60000},
        {"symbol": "MSFT", "status": "open", "current_price": 400, "market_value": 40000},
        {"symbol": "OLD",  "status": "closed", "current_price": 0,  "market_value": 0},
    ],
}
_fake = types.ModuleType("deploy_engine")
_fake.get_deployment = lambda did: DEPLOY if did == DEPLOY_ID else None
_fake.build_position_book = lambda sleeves, cap: BOOK
sys.modules["deploy_engine"] = _fake

import funds  # noqa: E402

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


def approx(a, b, tol=1e-6):
    return a is not None and b is not None and abs(a - b) < tol


def section(title):
    print("\n" + "=" * 70 + f"\n{title}\n" + "=" * 70)


# ===========================================================================
section("CRUD + NAV/unit index")
fund = funds.create_fund("Alpha Fund", DEPLOY_ID)
fid = fund["id"]
check("create_fund returns row", fund is not None and fund["name"] == "Alpha Fund")
check("inception = deployment start_date", fund["inception_date"] == "2024-06-03",
      fund["inception_date"])
check("base_nav_per_unit default 100", fund["base_nav_per_unit"] == 100.0)
check("get_fund roundtrips", funds.get_fund(fid)["id"] == fid)
check("list_funds contains it", any(f["id"] == fid for f in funds.list_funds()))

try:
    funds.create_fund("Bad", "nope")
    check("create_fund unknown deployment raises", False)
except ValueError:
    check("create_fund unknown deployment raises", True)

series = funds.nav_per_unit_series(fid)
check("daily series has 5 points", len(series) == 5, str(len(series)))
check("inception NAV/unit == base (100)", approx(series[0]["nav_per_unit"], 100.0))
check("last NAV/unit == 120", approx(series[-1]["nav_per_unit"], 120.0))
check("return_pct rebased (last = 20%)", approx(series[-1]["return_pct"], 20.0))
check("mid NAV/unit == 110 (06-07)",
      approx(next(p["nav_per_unit"] for p in series if p["date"] == "2024-06-07"), 110.0))

weekly = funds.nav_per_unit_series(fid, weekly=True)
check("weekly sample = 2 points (W23,W24)", len(weekly) == 2, str(len(weekly)))
check("W23 weekly = last of week (06-07, 110)",
      weekly[0]["date"] == "2024-06-07" and approx(weekly[0]["nav_per_unit"], 110.0))
check("W24 weekly = last of week (06-14, 120)",
      weekly[1]["date"] == "2024-06-14" and approx(weekly[1]["nav_per_unit"], 120.0))

# nav_per_unit_on
d, n = funds.nav_per_unit_on(fid, None)
check("nav_per_unit_on(None) = latest", d == "2024-06-14" and approx(n, 120.0))
d, n = funds.nav_per_unit_on(fid, "2024-06-09")
check("nav_per_unit_on(date) = latest on/before", d == "2024-06-07" and approx(n, 110.0))
try:
    funds.nav_per_unit_on(fid, "2024-06-01")
    check("nav_per_unit_on before inception raises", False)
except ValueError:
    check("nav_per_unit_on before inception raises", True)

# publish_weekly (immutable)
pub = funds.publish_weekly(fid)
check("publish_weekly inserts 2", pub["newly_published"] == 2, str(pub))
pub2 = funds.publish_weekly(fid)
check("publish_weekly is idempotent (0 new)", pub2["newly_published"] == 0, str(pub2))
check("published_nav returns 2 rows", len(funds.published_nav(fid)) == 2)


# ===========================================================================
section("Investors + units ledger")
inv = funds.create_investor("Jane Doe", email="jane@x.com")
iid = inv["id"]
check("create_investor", inv["name"] == "Jane Doe")
check("get_investor roundtrips", funds.get_investor(iid)["id"] == iid)

# subscribe 1100 at the 06-07 NAV/unit (110) → 10 units
sub = funds.subscribe(fid, iid, 1100, as_of="2024-06-07")
check("subscribe units = amount/nav (10)", approx(sub["units"], 10.0), str(sub))
check("subscribe priced at dealing nav (110)", approx(sub["nav_per_unit"], 110.0))
check("subscribe date = dealing date", sub["date"] == "2024-06-07")

try:
    funds.subscribe(fid, iid, 0)
    check("subscribe non-positive raises", False)
except ValueError:
    check("subscribe non-positive raises", True)

# redeem 4 units at latest NAV/unit (120) → proceeds 480, holding 6
red = funds.redeem(fid, iid, units=4)
check("redeem signs units negative", approx(red["units"], -4.0), str(red))
check("redeem proceeds = units*nav (480)", approx(red["proceeds"], 480.0))

try:
    funds.redeem(fid, iid, amount=100, units=1)
    check("redeem needs exactly one of amount/units", False)
except ValueError:
    check("redeem needs exactly one of amount/units", True)

try:
    funds.redeem(fid, iid, units=999)
    check("over-redemption blocked", False)
except ValueError:
    check("over-redemption blocked", True)


# ===========================================================================
section("Statement + reconciliation")
st = funds.investor_statement(fid, iid)
# held 6 units; net invested 1100 - 480 = 620; current value 6*120 = 720; gain 100
check("units_held = 6", approx(st["units_held"], 6.0), str(st["units_held"]))
check("net_invested = 620", approx(st["net_invested"], 620.0, 0.01), str(st["net_invested"]))
check("contributions = 1100 (subs only)", approx(st["contributions"], 1100.0, 0.01))
check("current_value = 720", approx(st["current_value"], 720.0, 0.01))
check("gain = 100", approx(st["gain"], 100.0, 0.01), str(st["gain"]))
check("return_on_capital positive", st["return_on_capital_pct"] > 0)
check("money_weighted_irr present & positive",
      st["money_weighted_irr_pct"] is not None and st["money_weighted_irr_pct"] > 0)
check("fund_return_pct (TWR since entry 110→120 = 9.09%)",
      approx(st["fund_return_pct"], 9.09, 0.01), str(st["fund_return_pct"]))

fi = funds.fund_investors(fid)
check("units_outstanding = 6", approx(fi["units_outstanding"], 6.0))
check("AUM = units * nav (720)", approx(fi["aum"], 720.0, 0.01))
# Σ position value reconciles to AUM
sum_val = round(sum(p["current_value"] for p in fi["positions"]), 2)
check("Σ position value == AUM", approx(sum_val, fi["aum"], 0.01), f"{sum_val} vs {fi['aum']}")
check("investor_count = 1", fi["investor_count"] == 1)

# _xirr sanity: -1000 now, +1100 a year later ≈ 10%
irr = funds._xirr([("2023-01-01", -1000.0), ("2024-01-01", 1100.0)])
check("_xirr ≈ 0.10 for 10% over 1y", approx(irr, 0.10, 1e-3), str(irr))


# ===========================================================================
section("subscription_orders (replicate book)")
so = funds.subscription_orders(fid, 1000)
check("orders replicate weights (2 orders)", so["order_count"] == 2, str(so["order_count"]))
o_by = {o["symbol"]: o for o in so["orders"]}
check("AAPL weight 0.6", approx(o_by["AAPL"]["weight"], 0.6))
check("AAPL shares = 600/200 = 3", approx(o_by["AAPL"]["shares"], 3.0), str(o_by["AAPL"]["shares"]))
check("MSFT shares = 400/400 = 1", approx(o_by["MSFT"]["shares"], 1.0))
check("fractional invests fully (residual 0)", approx(so["residual_cash"], 0.0, 0.01),
      str(so["residual_cash"]))
check("closed positions excluded", "OLD" not in o_by)

so_w = funds.subscription_orders(fid, 1100, whole=True)
ow = {o["symbol"]: o for o in so_w["orders"]}
# AAPL 660/200=3.3→3 (600), MSFT 440/400=1.1→1 (400); invested 1000, residual 100
check("whole-share rounds down (AAPL 3)", approx(ow["AAPL"]["shares"], 3.0))
check("whole-share residual = 100", approx(so_w["residual_cash"], 100.0, 0.01),
      str(so_w["residual_cash"]))


# ===========================================================================
section("Commingled execution ledger (fund_orders)")
# Fresh fund/investor so AUM == one clean subscription of 1100 (no redemptions).
f2 = funds.create_fund("Ledger Fund", DEPLOY_ID)["id"]
i2 = funds.create_investor("Bob")["id"]
funds.subscribe(f2, i2, 1100)   # at latest nav 120; cash basis is the $ amount

book0 = funds.fund_actual_book(f2)
check("actual book cash = net cash in (1100)", approx(book0["cash"], 1100.0, 0.01),
      str(book0["cash"]))
check("actual book no holdings yet", book0["holdings_value"] == 0)
check("actual book AUM = 1100", approx(book0["aum"], 1100.0, 0.01))

gen = funds.generate_orders(f2)
# targets × AUM 1100: AAPL 660/200=3.3, MSFT 440/400=1.1 ; both BUY
check("generate_orders writes 2 orders", gen["order_count"] == 2, str(gen["order_count"]))
check("buy_value ≈ 1100", approx(gen["buy_value"], 1100.0, 0.01), str(gen["buy_value"]))
check("sell_value = 0", approx(gen["sell_value"], 0.0))
g_by = {o["symbol"]: o for o in gen["orders"]}
check("AAPL order shares = 3.3", approx(g_by["AAPL"]["shares"], 3.3, 1e-4), str(g_by["AAPL"]["shares"]))
check("all orders pending", all(o["status"] == "pending" for o in gen["orders"]))

pending = funds.list_orders(f2, status="pending")
check("list_orders(pending) = 2", len(pending) == 2)

# fill one order individually
one = pending[0]
filled = funds.record_fill(one["id"])
check("record_fill marks executed", filled["status"] == "executed")
try:
    funds.record_fill(one["id"])
    check("double-fill blocked", False)
except PermissionError:
    check("double-fill blocked", True)

# fill the rest of the batch
fb = funds.fill_batch(gen["batch_id"])
check("fill_batch fills remaining (1)", fb["filled"] == 1, str(fb))

# after full fill, book should be ~fully invested, cash ≈ 0
book1 = funds.fund_actual_book(f2)
check("post-fill cash ≈ 0", approx(book1["cash"], 0.0, 0.01), str(book1["cash"]))
check("post-fill holdings ≈ 1100", approx(book1["holdings_value"], 1100.0, 0.01),
      str(book1["holdings_value"]))
check("post-fill AUM preserved ≈ 1100", approx(book1["aum"], 1100.0, 0.01))
check("post-fill 2 positions", len(book1["positions"]) == 2)


# ===========================================================================
section("Deletion guards")
try:
    funds.delete_fund(f2)
    check("delete_fund with live units blocked", False)
except PermissionError:
    check("delete_fund with live units blocked", True)

try:
    funds.delete_investor(i2)
    check("delete_investor with live units blocked", False)
except PermissionError:
    check("delete_investor with live units blocked", True)

# redeem everything in f2, then delete cleanly
held = funds.fund_investors(f2)["positions"][0]["units"]
funds.redeem(f2, i2, units=held)
res = funds.delete_fund(f2)
check("delete_fund after redemption succeeds", res["deleted"] == f2)
check("delete_fund reports ~0 live units", approx(res["live_units_at_delete"], 0.0, 1e-3))
check("delete_fund removes fund row", funds.get_fund(f2) is None)

# force delete a fund that still holds units (original fid holds 6)
forced = funds.delete_fund(fid, force=True)
check("force delete with live units", forced["deleted"] == fid)
check("force delete reports the live units", approx(forced["live_units_at_delete"], 6.0, 1e-3))

try:
    funds.delete_fund("ghost")
    check("delete_fund unknown raises ValueError", False)
except ValueError:
    check("delete_fund unknown raises ValueError", True)

# investor i2 now holds nothing → deletes cleanly
dres = funds.delete_investor(i2)
check("delete_investor after redemption succeeds", dres["deleted"] == i2)


# ===========================================================================
shutil.rmtree(_TMP, ignore_errors=True)
print("\n" + "=" * 70)
TOTAL = PASS + FAIL
print(f"RESULTS: {PASS}/{TOTAL} passed, {FAIL} failed")
print("=" * 70)
if FAIL:
    print(f"{FAIL} TESTS FAILED ❌")
    sys.exit(1)
print("ALL TESTS PASSED ✅")
