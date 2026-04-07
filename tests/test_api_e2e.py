#!/usr/bin/env python3
"""
API E2E Test Suite
==================
Tests the FastAPI endpoints directly via TestClient (no running server needed).
Validates that the API layer works correctly after refactoring.

Run:
    cd /app/scripts && python3 test_api_e2e.py
"""

import os
import sys
import json

# Ensure server/ and scripts/ are on the path
WORKSPACE = os.environ.get("WORKSPACE", os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(WORKSPACE, "server"))
sys.path.insert(0, os.path.join(WORKSPACE, "scripts"))
os.chdir(os.path.join(WORKSPACE, "server"))

from fastapi.testclient import TestClient

# Disable API key for testing
os.environ["ALPHASCOUT_API_KEY"] = ""

from api import app, _sync_universe_profiles

# ---------------------------------------------------------------------------
# Test framework
# ---------------------------------------------------------------------------
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


client = TestClient(app)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 1: Health endpoint")
print("=" * 70)
r = client.get("/health")
check("Status 200", r.status_code == 200)
data = r.json()
check("Returns status ok", data.get("status") == "ok")
check("Has db_exists field", "db_exists" in data)
check("Has db_size_mb field", "db_size_mb" in data)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 2: Universe profiles (SQL table)")
print("=" * 70)
r = client.get("/api/universe?limit=10")
check("Status 200", r.status_code == 200)
data = r.json()
check("Has total field", "total" in data)
check("Has data array", isinstance(data.get("data"), list))
check("Total > 0", data["total"] > 0, f"total={data.get('total')}")
check("Returned ≤ 10 rows", len(data["data"]) <= 10)
if data["data"]:
    row = data["data"][0]
    check("Row has symbol", "symbol" in row)
    check("Row has name", "name" in row)
    check("Row has sector", "sector" in row)
    check("Row has industry", "industry" in row)
    check("Row has market_cap", "market_cap" in row)
    check("Row has exchange", "exchange" in row)
    check("Row has country", "country" in row)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 3: Universe filtering")
print("=" * 70)
r = client.get("/api/universe?sector=Technology&limit=5")
check("Tech filter 200", r.status_code == 200)
data = r.json()
check("Tech filter returns data", data["total"] > 0)
if data["data"]:
    check("All results are Technology", all(d["sector"] == "Technology" for d in data["data"]))

r = client.get("/api/universe?min_market_cap=1000000000000&limit=100")
check("Market cap filter 200", r.status_code == 200)
data = r.json()
check("Trillion+ returns data", data["total"] > 0 and data["total"] < 521,
      f"total={data.get('total')}")
if data["data"]:
    check("All results ≥ 1T", all(d["market_cap"] >= 1e12 for d in data["data"]))

r = client.get("/api/universe?sector=Technology&sort=market_cap&order=desc&limit=3")
data = r.json()
if len(data["data"]) >= 2:
    check("Sort by market_cap desc works",
          data["data"][0]["market_cap"] >= data["data"][1]["market_cap"])

# ===================================================================
print("\n" + "=" * 70)
print("TEST 4: Search endpoint")
print("=" * 70)
r = client.get("/api/search?q=AAPL")
check("Search 200", r.status_code == 200)
data = r.json()
check("Search returns results", data["total"] > 0)
if data["data"]:
    check("AAPL in results", any(d["symbol"] == "AAPL" for d in data["data"]))
    row = data["data"][0]
    check("Result has symbol", "symbol" in row)
    check("Result has name", "name" in row)
    check("Result has market_cap", "market_cap" in row)

r = client.get("/api/search?q=apple&limit=5")
check("Case-insensitive search 200", r.status_code == 200)
data = r.json()
check("Apple search finds results", data["total"] > 0)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 5: Universe sync endpoint")
print("=" * 70)
r = client.post("/api/universe/sync")
check("Sync 200", r.status_code == 200)
data = r.json()
check("Sync returns count", data.get("profiles_synced", 0) > 0,
      f"synced={data.get('profiles_synced')}")

# ===================================================================
print("\n" + "=" * 70)
print("TEST 6: Prices endpoint")
print("=" * 70)
r = client.get("/api/prices/AAPL?limit=5")
check("Prices 200", r.status_code == 200)
data = r.json()
check("Prices has data", len(data.get("data", [])) > 0)
check("Prices has total", "total" in data)
check("Prices has symbol", data.get("symbol") == "AAPL")

# ===================================================================
print("\n" + "=" * 70)
print("TEST 7: Fundamentals endpoints")
print("=" * 70)
for endpoint in ["/api/income/AAPL", "/api/balance/AAPL", "/api/cashflow/AAPL"]:
    r = client.get(f"{endpoint}?limit=3")
    check(f"{endpoint} returns 200", r.status_code == 200)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 8: Earnings endpoint")
print("=" * 70)
r = client.get("/api/earnings/AAPL?limit=5")
check("Earnings 200", r.status_code == 200)
data = r.json()
check("Earnings has data", len(data.get("data", [])) > 0)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 9: Strategies CRUD")
print("=" * 70)
r = client.get("/strategies")
check("List strategies 200", r.status_code == 200)
strategies = r.json()
check("Strategies is a list", isinstance(strategies, list))
check("Has strategies", len(strategies) > 0)
if strategies:
    sid = strategies[0].get("strategy_id")
    if sid:
        r = client.get(f"/strategies/{sid}")
        check(f"Get strategy {sid[:8]}... 200", r.status_code == 200)
        sdata = r.json()
        check("Strategy has name", "name" in sdata)
        check("Strategy has entry", "entry" in sdata)
        check("Strategy has universe", "universe" in sdata)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 10: Backtest runs listing")
print("=" * 70)
r = client.get("/backtest/runs?limit=5")
check("Backtest runs 200", r.status_code == 200)
data = r.json()
check("Has total", "total" in data)
check("Has data array", isinstance(data.get("data"), list))
if data["data"]:
    run = data["data"][0]
    check("Run has run_id", "run_id" in run)
    check("Run has metrics", "metrics" in run)
    check("Run has strategy_name", "strategy_name" in run)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 11: Backtest search")
print("=" * 70)
r = client.get("/backtest/search?min_sharpe=0&limit=5")
check("Backtest search 200", r.status_code == 200)
data = r.json()
check("Search has data", isinstance(data.get("data"), list))

# ===================================================================
print("\n" + "=" * 70)
print("TEST 12: Deployments listing")
print("=" * 70)
r = client.get("/strategies/deployments?include_stopped=true")
check("Deployments 200", r.status_code == 200)
data = r.json()
check("Has total", "total" in data)
check("Has data array", isinstance(data.get("data"), list))
if data["data"]:
    dep = data["data"][0]
    check("Deployment has id", "id" in dep)
    check("Deployment has status", "status" in dep)
    check("Deployment has last_nav", "last_nav" in dep)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 13: Trades listing")
print("=" * 70)
r = client.get("/trades?limit=5")
check("Trades 200", r.status_code == 200)
data = r.json()
check("Trades has total", "total" in data)
check("Trades has data", isinstance(data.get("data"), list))

# ===================================================================
print("\n" + "=" * 70)
print("TEST 14: Portfolios CRUD")
print("=" * 70)
r = client.get("/portfolios")
check("List portfolios 200", r.status_code == 200)
portfolios = r.json()
check("Portfolios is a list", isinstance(portfolios, list))
if portfolios:
    pid = portfolios[0].get("portfolio_id")
    if pid:
        r = client.get(f"/portfolios/{pid}")
        check(f"Get portfolio {pid[:8]}... 200", r.status_code == 200)
        pdata = r.json()
        check("Portfolio has name", "name" in pdata)
        check("Portfolio has config", "config" in pdata)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 15: Portfolio deployments")
print("=" * 70)
r = client.get("/portfolios/deployments?include_stopped=true")
check("Portfolio deployments 200", r.status_code == 200)
data = r.json()
check("Returns a list", isinstance(data, list))

# ===================================================================
print("\n" + "=" * 70)
print("TEST 16: Regimes")
print("=" * 70)
r = client.get("/regimes")
check("List regimes 200", r.status_code == 200)
regimes = r.json()
check("Regimes is a list", isinstance(regimes, list))
if regimes:
    rid = regimes[0].get("regime_id")
    if rid:
        r = client.get(f"/regimes/{rid}")
        check(f"Get regime {rid[:8]}... 200", r.status_code == 200)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 17: Macro dashboard")
print("=" * 70)
r = client.get("/api/macro/dashboard")
check("Macro dashboard 200", r.status_code == 200)
data = r.json()
check("Has as_of", "as_of" in data)
check("Has brent", "brent" in data)
check("Has vix", "vix" in data)
check("Has spx", "spx" in data)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 18: Macro series")
print("=" * 70)
r = client.get("/macro/series")
check("Macro series 200", r.status_code == 200)
data = r.json()
check("Has indicators", "indicators" in data)
check("Has derived", "derived" in data)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 19: Sleeves listing")
print("=" * 70)
r = client.get("/sleeves?limit=5")
check("Sleeves 200", r.status_code == 200)
data = r.json()
check("Has total", "total" in data)
check("Has data array", isinstance(data.get("data"), list))

# ===================================================================
print("\n" + "=" * 70)
print("TEST 20: Alerts today")
print("=" * 70)
r = client.get("/alerts/today")
check("Alerts today 200", r.status_code == 200)
data = r.json()
check("Has date", "date" in data)
check("Has total_alerts", "total_alerts" in data)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 21: Stats endpoint")
print("=" * 70)
r = client.get("/api/stats")
check("Stats 200", r.status_code == 200)
data = r.json()
check("Has db_tables", "db_tables" in data)
check("Has db_size_mb", "db_size_mb" in data)

# ===================================================================
print("\n" + "=" * 70)
print("TEST 22: Response format consistency checks")
print("=" * 70)
# All paginated endpoints should have total + data
paginated_endpoints = [
    "/api/universe?limit=1",
    "/api/prices/AAPL?limit=1",
    "/api/earnings/AAPL?limit=1",
    "/trades?limit=1",
    "/backtest/runs?limit=1",
    "/sleeves?limit=1",
]
for ep in paginated_endpoints:
    r = client.get(ep)
    if r.status_code == 200:
        d = r.json()
        check(f"{ep} has 'total'", "total" in d, f"keys={list(d.keys())}")
        check(f"{ep} has 'data'", "data" in d, f"keys={list(d.keys())}")

# ===================================================================
print("\n" + "=" * 70)
print("TEST 23: Error handling")
print("=" * 70)
r = client.get("/api/prices/INVALIDTICKERTOOLONG123")
check("Invalid symbol → 400", r.status_code == 400)

r = client.get("/strategies/nonexistent_id_12345")
check("Missing strategy → 404", r.status_code == 404)

r = client.get("/portfolios/nonexistent_id_12345")
check("Missing portfolio → 404", r.status_code == 404)

r = client.get("/regimes/nonexistent_id_12345")
check("Missing regime → 404", r.status_code == 404)

# ===================================================================
# Summary
# ===================================================================
print("\n" + "=" * 70)
print(f"RESULTS: {PASS}/{PASS + FAIL} passed, {FAIL} failed")
if FAIL == 0:
    print("ALL TESTS PASSED ✅")
else:
    print(f"{FAIL} TESTS FAILED ❌")
print("=" * 70)

sys.exit(0 if FAIL == 0 else 1)
