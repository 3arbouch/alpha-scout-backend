#!/usr/bin/env python3
"""
End-to-end test for ISIN/CUSIP identifier columns + API endpoints.

Covers the new `isin` and `cusip` columns on universe_profiles, the
backfill flow, and the two new endpoints:
  - GET /api/symbols/{symbol}/identifiers      (forward lookup)
  - GET /api/symbols/lookup?isin=US0378331005  (reverse lookup)

Run:
    cd /home/mohamed/alpha-scout-backend-dev/tests
    MARKET_DB_PATH=/home/mohamed/alpha-scout-backend-dev/data/market_dev.db \\
    APP_DB_PATH=/home/mohamed/alpha-scout-backend/data/app_dev.db \\
    API_KEY=changeme \\
    python3 test_isin_identifiers_e2e.py
"""
import os
import sqlite3
import sys
import urllib.request
import urllib.error
import json


PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


# ---------------------------------------------------------------------------
# 1. Schema: universe_profiles has isin + cusip columns
# ---------------------------------------------------------------------------
print("\n=== 1. Schema columns present ===")
db_path = os.environ.get("MARKET_DB_PATH",
                         "/home/mohamed/alpha-scout-backend-dev/data/market_dev.db")
c = sqlite3.connect(db_path)
cols = {r[1] for r in c.execute("PRAGMA table_info(universe_profiles)").fetchall()}
check("isin column exists", "isin" in cols)
check("cusip column exists", "cusip" in cols)
check("idx_up_isin index exists",
      any(r[0] == "idx_up_isin" for r in c.execute(
          "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='universe_profiles'"
      ).fetchall()))


# ---------------------------------------------------------------------------
# 2. Known ISINs are populated after backfill
# ---------------------------------------------------------------------------
print("\n=== 2. ISIN values for known tickers ===")
KNOWN = {
    "AAPL": "US0378331005",
    "MSFT": "US5949181045",
    "NVDA": "US67066G1040",
}
for sym, expected in KNOWN.items():
    r = c.execute("SELECT isin FROM universe_profiles WHERE symbol = ?", (sym,)).fetchone()
    got = r[0] if r else None
    check(f"{sym} ISIN = {expected}", got == expected, f"got {got!r}")


# ---------------------------------------------------------------------------
# 3. API endpoints — only run if API is up at the expected port
# ---------------------------------------------------------------------------
api_port = int(os.environ.get("API_PORT", 8091))
api_key = os.environ.get("API_KEY", "")
base = f"http://127.0.0.1:{api_port}"


def _get(path, expect_status=200):
    req = urllib.request.Request(f"{base}{path}",
                                 headers={"X-API-Key": api_key} if api_key else {})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        return 0, None


print(f"\n=== 3. API endpoints (against {base}) ===")
status, _ = _get("/health")
if status == 0:
    print(f"  ⚠️  API at {base} unreachable; skipping endpoint tests")
else:
    # Forward lookup
    status, body = _get("/api/symbols/AAPL/identifiers")
    check(f"GET /api/symbols/AAPL/identifiers returns 200 (got {status})",
          status == 200)
    if status == 200:
        check("response.isin = US0378331005",
              body.get("isin") == "US0378331005",
              f"got {body.get('isin')!r}")
        check("response.symbol = AAPL", body.get("symbol") == "AAPL")
        check("response.cusip = 037833100",
              body.get("cusip") == "037833100",
              f"got {body.get('cusip')!r}")

    # Reverse lookup by ISIN
    status, body = _get("/api/symbols/lookup?isin=US0378331005")
    check(f"GET /api/symbols/lookup?isin=AAPL's ISIN returns 200 (got {status})",
          status == 200)
    if status == 200:
        check("reverse lookup symbol = AAPL", body.get("symbol") == "AAPL")

    # Reverse lookup by CUSIP
    status, body = _get("/api/symbols/lookup?cusip=594918104")
    check(f"GET /api/symbols/lookup?cusip=MSFT's CUSIP returns 200 (got {status})",
          status == 200)
    if status == 200:
        check("reverse lookup symbol = MSFT", body.get("symbol") == "MSFT")

    # Invalid: providing zero or multiple identifiers
    status, _ = _get("/api/symbols/lookup", expect_status=400)
    check(f"GET /api/symbols/lookup with no params returns 400 (got {status})",
          status == 400)
    status, _ = _get("/api/symbols/lookup?isin=US0378331005&cusip=037833100",
                     expect_status=400)
    check(f"GET /api/symbols/lookup with two params returns 400 (got {status})",
          status == 400)

    # 404 for unknown ISIN
    status, _ = _get("/api/symbols/lookup?isin=XX0000000000")
    check(f"unknown ISIN returns 404 (got {status})", status == 404)

    # /api/universe now includes isin/cusip
    status, body = _get("/api/universe?limit=5")
    if status == 200 and body.get("data"):
        check("universe rows include isin field",
              "isin" in body["data"][0])
        check("universe rows include cusip field",
              "cusip" in body["data"][0])


print("\n" + "=" * 60)
print(f"PASSED: {PASS}")
print(f"FAILED: {FAIL}")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
