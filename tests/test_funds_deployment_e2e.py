#!/usr/bin/env python3
"""
End-to-end: a REAL deployed strategy's returns, transformed into a fund.

Unlike test_funds_unit.py (which fakes deploy_engine), this deploys an actual
strategy against real market data, wraps a fund around it, and verifies the
integration contract:

  * the fund NAV/unit index IS the deployment's real cumulative-return path,
    rebased to base_nav_per_unit at inception;
  * subscription_orders replicate the deployment's REAL holding weights;
  * generate_orders → fill → fund_actual_book reconciles (fills convert cash to
    holdings without changing AUM, mirroring the deployment's cash/invested split).

All assertions are derived from the live deployment (no hardcoded prices/returns),
so the test stays valid as market data advances.

Isolation: a throwaway APP_DB (never touches app_dev.db); real MARKET_DB for prices.

Run:
    python3 tests/test_funds_deployment_e2e.py
"""
import os
import sys
import shutil
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent

# --- env BEFORE importing db_config/deploy_engine/funds ----------------------
_TMP = tempfile.mkdtemp(prefix="funds_e2e_")
os.environ["APP_DB_PATH"] = str(Path(_TMP) / "app.db")        # throwaway, isolated
os.environ.setdefault("MARKET_DB_PATH", str(_REPO / "data" / "market_dev.db"))  # real prices
os.environ["WORKSPACE"] = str(_REPO)                          # deployments/ dir lives here

sys.path.insert(0, str(_REPO / "scripts"))

import deploy_engine as de  # noqa: E402
import funds                # noqa: E402

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
    return a is not None and b is not None and abs(a - b) < tol


def section(title):
    print("\n" + "=" * 70 + f"\n{title}\n" + "=" * 70)


STRAT = {
    "name": "Funds E2E Strategy",
    "universe": {"type": "symbols", "symbols": ["AAPL", "MSFT"]},
    "entry": {"conditions": [{"type": "always"}], "logic": "all"},
    "sizing": {"type": "equal_weight", "max_positions": 2, "initial_allocation": 100000},
    "backtest": {"start": "2024-06-01", "end": "2024-12-31",
                 "entry_price": "next_close", "slippage_bps": 10},
}

dep_id = None
try:
    # =======================================================================
    section("Deploy a real strategy")
    dep = de.deploy(STRAT, "2024-06-01", 100000, "Funds E2E Strategy")
    dep_id = dep["id"]
    d = de.get_deployment(dep_id)
    nav_history = [p for p in (d.get("nav_history") or [])
                   if p.get("nav") is not None]
    check("deployment has real nav_history", len(nav_history) > 2, str(len(nav_history)))
    book = de.build_position_book(d.get("sleeves") or [], d.get("initial_capital") or 0)
    open_syms = {p["symbol"] for p in book["positions"] if p.get("status") == "open"}
    pv = book["portfolio_value"]
    sum_w = sum(p["market_value"] for p in book["positions"]
                if p.get("status") == "open") / pv
    check("deployment has open positions", len(open_syms) > 0, str(open_syms))
    print(f"    invested weight = {sum_w:.4f} of portfolio (rest is cash)")

    # =======================================================================
    section("Fund NAV/unit == real strategy return, rebased")
    fund = funds.create_fund("Real Returns Fund", dep_id)
    fid = fund["id"]
    inception = fund["inception_date"]

    nh = [p for p in nav_history if p["date"] >= inception]
    base_nav = nh[0]["nav"]
    exp_last_npu = round(100.0 * nh[-1]["nav"] / base_nav, 4)
    exp_last_ret = round((nh[-1]["nav"] / base_nav - 1.0) * 100.0, 4)

    series = funds.nav_per_unit_series(fid)
    check("series length == deployment nav points (since inception)",
          len(series) == len(nh), f"{len(series)} vs {len(nh)}")
    check("inception NAV/unit == base (100)", approx(series[0]["nav_per_unit"], 100.0, 1e-9),
          str(series[0]["nav_per_unit"]))
    check("last NAV/unit == strategy return rebased",
          approx(series[-1]["nav_per_unit"], exp_last_npu, 1e-3),
          f"{series[-1]['nav_per_unit']} vs {exp_last_npu}")
    check("last return_pct == strategy cumulative return",
          approx(series[-1]["return_pct"], exp_last_ret, 1e-3),
          f"{series[-1]['return_pct']} vs {exp_last_ret}")
    check("series passes through the REAL deployment NAV",
          approx(series[-1]["deployment_nav"], nh[-1]["nav"], 1e-6),
          f"{series[-1]['deployment_nav']} vs {nh[-1]['nav']}")
    print(f"    fund total return = {series[-1]['return_pct']}%  "
          f"(strategy NAV {base_nav:,.0f} → {nh[-1]['nav']:,.0f})")

    # =======================================================================
    section("subscription_orders replicate the REAL book weights")
    amount = 50000.0
    so = funds.subscription_orders(fid, amount)
    order_syms = {o["symbol"] for o in so["orders"]}
    check("orders only for real open positions", order_syms <= open_syms,
          f"{order_syms} vs {open_syms}")
    check("invested + residual == amount",
          approx(so["invested"] + so["residual_cash"], amount, 0.01),
          f"{so['invested']} + {so['residual_cash']}")
    # invested fraction mirrors the deployment's invested weight
    check("invested ≈ amount × deployment invested-weight",
          approx(so["invested"], amount * sum_w, max(5.0, amount * 2e-3)),
          f"{so['invested']} vs {amount * sum_w:.2f}")
    check("per-order weight ≈ realized weight (low drift, fractional)",
          so["max_weight_drift_pct"] < 0.5, str(so["max_weight_drift_pct"]))

    # =======================================================================
    section("generate_orders → fill → fund_actual_book reconciles")
    inv = funds.create_investor("E2E Investor")
    iid = inv["id"]
    funds.subscribe(fid, iid, amount)          # cash basis for the fund's real book

    book0 = funds.fund_actual_book(fid)
    check("pre-fill cash == subscription", approx(book0["cash"], amount, 0.01),
          str(book0["cash"]))
    check("pre-fill no holdings", book0["holdings_value"] == 0)
    check("pre-fill AUM == subscription", approx(book0["aum"], amount, 0.01))

    gen = funds.generate_orders(fid)
    check("generate_orders produced buys", gen["order_count"] > 0, str(gen["order_count"]))
    check("buy_value ≈ AUM × invested-weight",
          approx(gen["buy_value"], amount * sum_w, max(5.0, amount * 3e-3)),
          f"{gen['buy_value']} vs {amount * sum_w:.2f}")
    check("generated orders only for real holdings",
          {o["symbol"] for o in gen["orders"]} <= open_syms)

    fb = funds.fill_batch(gen["batch_id"])
    check("fill_batch executed all orders", fb["filled"] == gen["order_count"], str(fb))

    book1 = funds.fund_actual_book(fid)
    check("AUM preserved across fills (cash→holdings)",
          approx(book1["aum"], book0["aum"], max(5.0, amount * 5e-3)),
          f"{book1['aum']} vs {book0['aum']}")
    check("post-fill holdings ≈ invested fraction",
          approx(book1["holdings_value"], amount * sum_w, max(10.0, amount * 5e-3)),
          f"{book1['holdings_value']} vs {amount * sum_w:.2f}")
    check("post-fill holdings match real positions",
          {p["symbol"] for p in book1["positions"]} <= open_syms)
    print(f"    fund real book: cash {book1['cash']:,.2f} + holdings "
          f"{book1['holdings_value']:,.2f} = AUM {book1['aum']:,.2f}")

    # =======================================================================
    section("Investor statement tracks the real fund return since entry")
    # Re-subscribe a second investor at an EARLY dealing date so the fund's
    # time-weighted return since entry is real and positive.
    early = series[max(1, len(series) // 4)]
    inv2 = funds.create_investor("Early Investor")
    funds.subscribe(fid, inv2["id"], 10000, as_of=early["date"])
    st = funds.investor_statement(fid, inv2["id"])
    _, nav_now = funds.nav_per_unit_on(fid, None)
    exp_twr = round((nav_now / early["nav_per_unit"] - 1.0) * 100, 2)
    check("statement fund_return_pct == real TWR since entry",
          approx(st["fund_return_pct"], exp_twr, 0.01),
          f"{st['fund_return_pct']} vs {exp_twr}")
    check("entry NAV/unit == early dealing NAV",
          approx(st["entry_nav_per_unit"], early["nav_per_unit"], 1e-6))

finally:
    # ---- cleanup: temp DB + on-disk deployment artifacts -------------------
    shutil.rmtree(_TMP, ignore_errors=True)
    if dep_id:
        shutil.rmtree(_REPO / "deployments" / dep_id, ignore_errors=True)

print("\n" + "=" * 70)
TOTAL = PASS + FAIL
print(f"RESULTS: {PASS}/{TOTAL} passed, {FAIL} failed")
print("=" * 70)
if FAIL:
    print(f"{FAIL} TESTS FAILED ❌")
    sys.exit(1)
print("ALL TESTS PASSED ✅")
