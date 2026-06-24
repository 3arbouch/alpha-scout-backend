# Funds API

Manage funds, investors, subscriptions/redemptions, and investor reporting.

## Basics

- **Base URL:** dev `http://localhost:8091`, prod `http://localhost:8090`
- **Auth:** every request needs the `X-API-Key` header (a Bearer JWT also works).
- **Interactive docs:** the live server auto-serves Swagger UI at `/docs`, ReDoc at
  `/redoc`, and the raw OpenAPI spec at `/openapi.json`.

```bash
export API=http://localhost:8090
export KEY=your-api-key
curl -s -H "X-API-Key: $KEY" "$API/funds" | jq
```

## Concepts

- A **fund** wraps one **deployment** (strategy). Its **NAV per unit** is the
  deployment's return index rebased so that `inception == base_nav_per_unit`
  (default 100).
- Investors hold **units**. A **subscription** buys units at the dealing-date
  NAV (`units = amount / NAV`); a **redemption** sells them. The ledger is the
  source of truth — an investor's holding is the signed sum of their units.
- Dealing dates: pass `as_of` (`YYYY-MM-DD`) to price at a specific date;
  omit it to use the latest NAV.

## Funds

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/funds` | Create a fund wrapping a deployment |
| `GET` | `/funds` | List funds |
| `GET` | `/funds/{fund_id}` | Fund detail |
| `PATCH` | `/funds/{fund_id}` | Rename (`{"name": "..."}`) |
| `GET` | `/funds/{fund_id}/nav` | NAV/unit series (`?weekly=true`, `?published=true`) |
| `POST` | `/funds/{fund_id}/publish` | Snapshot weekly NAV into the immutable published series (idempotent) |
| `DELETE` | `/funds/{fund_id}` | Delete a fund (`?force=true` if it still has investors) |

```bash
curl -s -X POST "$API/funds" -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d '{
  "name": "Tech Growth Fund",
  "deployment_id": "tech_quality_growth_vol_composite_v1_17664772",
  "base_nav_per_unit": 100.0,
  "currency": "USD"
}'
```

## Investors

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/investors` | Create an investor (`{"name","email?","notes?"}`) |
| `GET` | `/investors` | List investors |
| `DELETE` | `/investors/{investor_id}` | **Global** delete: removes the investor + ALL transactions in every fund (`?force=true` if they still hold units anywhere) |

## Subscriptions & redemptions

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/funds/{fund_id}/subscribe` | Buy units worth `amount` |
| `POST` | `/funds/{fund_id}/redeem` | Redeem by `amount` **or** `units` |
| `GET` | `/funds/{fund_id}/investors` | All investor positions + reconciliation (Σ value == AUM) |
| `GET` | `/funds/{fund_id}/investors/{investor_id}/statement` | Per-account performance (value, gain, return on capital, IRR, fund return) |
| `DELETE` | `/funds/{fund_id}/investors/{investor_id}` | **Per-fund exit:** redeem the investor's full remaining holding in THIS fund at the dealing-date NAV. Keeps the investor record and any other-fund holdings. |

```bash
# subscribe
curl -s -X POST "$API/funds/$FUND/subscribe" -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"investor_id": "inv_123", "amount": 50000}'

# redeem 100 units (or use {"amount": 25000})
curl -s -X POST "$API/funds/$FUND/redeem" -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"investor_id": "inv_123", "units": 100}'

# exit an investor from one fund (full redemption, history preserved)
curl -s -X DELETE "$API/funds/$FUND/investors/inv_123" -H "X-API-Key: $KEY"
```

### Removing an investor: which endpoint?

- `DELETE /funds/{fund_id}/investors/{investor_id}` — investor leaves **one fund**
  (full redemption at current NAV, ledger preserved). Other funds untouched.
  404 if they were never subscribed to that fund.
- `DELETE /investors/{investor_id}?force=true` — delete the investor **everywhere**
  (wipes their record + all transactions in all funds).

## Reports (PDF)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/funds/{fund_id}/report.pdf` | Fund monthly report: performance summary, NAV/unit curve vs market (SPY) + sector benchmark, LLM commentary + outlook, top contributors/detractors, sector exposure, investor roll, disclosures. `?include_commentary=false` skips the LLM narrative. |
| `GET` | `/funds/{fund_id}/investors/{investor_id}/statement.pdf` | Per-investor statement PDF |

```bash
curl -s -H "X-API-Key: $KEY" "$API/funds/$FUND/report.pdf" -o fund_report.pdf
```

## Typical flow

```bash
# 1. create an investor
INV=$(curl -s -X POST "$API/investors" -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"name":"Jane Doe","email":"jane@example.com"}' | jq -r .id)

# 2. subscribe them to a fund
curl -s -X POST "$API/funds/$FUND/subscribe" -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d "{\"investor_id\":\"$INV\",\"amount\":100000}"

# 3. check their statement
curl -s -H "X-API-Key: $KEY" "$API/funds/$FUND/investors/$INV/statement" | jq

# 4. later, exit them from the fund
curl -s -X DELETE "$API/funds/$FUND/investors/$INV" -H "X-API-Key: $KEY" | jq
```

## Errors

- `400` — bad input (e.g. redeeming more units than held, non-positive amount).
- `404` — fund/investor not found, or investor not subscribed to that fund.
- `409` — global investor/fund delete blocked because units are still held
  (retry with `?force=true`).
</content>
</invoke>
