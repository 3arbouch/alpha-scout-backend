"""Funds — unitized NAV/share layer over a deployment.

A fund wraps a live deployment (the strategy) and exposes a NAV-per-unit index:
the deployment's cumulative-return path rebased so that at inception
NAV/unit == base_nav_per_unit (default $100). Investor units are notional
(Option A) — they don't change the underlying book.

Public API:
    create_fund(name, deployment_id, inception_date=None, base_nav_per_unit=100.0)
    get_fund(fund_id) / list_funds()
    nav_per_unit_series(fund_id, weekly=False) -> [{date, nav_per_unit, deployment_nav, return_pct}]
    publish_weekly(fund_id) -> snapshot weekly NAV/unit into fund_nav_history (immutable)
"""
import os
import re
import sqlite3
import hashlib
from datetime import datetime, timezone
from pathlib import Path

_BASE = Path(os.environ.get("DATA_DIR", str(Path(__file__).resolve().parent.parent / "data")))
APP_DB_PATH = os.environ.get("APP_DB_PATH", str(_BASE / "app.db"))


def _conn() -> sqlite3.Connection:
    from schema import init_db
    conn = sqlite3.connect(APP_DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_id(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower().strip()).strip("_")[:40]
    suffix = hashlib.md5(f"{name}:{_now()}".encode()).hexdigest()[:8]
    return f"{slug}_{suffix}"


def _deployment_nav_history(deployment_id: str) -> tuple[list[dict], dict]:
    """(nav_history, deployment) for a deployment, via deploy_engine."""
    from deploy_engine import get_deployment
    d = get_deployment(deployment_id)
    if not d:
        raise ValueError(f"Deployment '{deployment_id}' not found")
    return (d.get("nav_history") or []), d


# --------------------------------------------------------------------------- #
# CRUD
# --------------------------------------------------------------------------- #
def create_fund(name: str, deployment_id: str, inception_date: str | None = None,
                base_nav_per_unit: float = 100.0, currency: str = "USD",
                dealing_frequency: str = "weekly") -> dict:
    nav_history, d = _deployment_nav_history(deployment_id)
    if not nav_history:
        raise ValueError(f"Deployment '{deployment_id}' has no NAV history yet")
    inception = inception_date or d.get("start_date") or nav_history[0]["date"]

    fund_id = _gen_id(name)
    now = _now()
    conn = _conn()
    try:
        conn.execute(
            """INSERT INTO funds (id, name, deployment_id, inception_date,
                   base_nav_per_unit, currency, dealing_frequency, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)""",
            (fund_id, name, deployment_id, inception, base_nav_per_unit,
             currency, dealing_frequency, now),
        )
        conn.commit()
    finally:
        conn.close()
    return get_fund(fund_id)


def get_fund(fund_id: str) -> dict | None:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM funds WHERE id = ?", (fund_id,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def rename_fund(fund_id: str, name: str) -> dict:
    name = name.strip()
    if not name:
        raise ValueError("Fund name cannot be empty")
    conn = _conn()
    try:
        cur = conn.execute(
            "UPDATE funds SET name = ?, updated_at = ? WHERE id = ?",
            (name, _now(), fund_id),
        )
        conn.commit()
    finally:
        conn.close()
    if cur.rowcount == 0:
        raise ValueError(f"Fund '{fund_id}' not found")
    return get_fund(fund_id)


def list_funds() -> list[dict]:
    conn = _conn()
    try:
        rows = conn.execute("SELECT * FROM funds ORDER BY created_at DESC").fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# NAV/unit index
# --------------------------------------------------------------------------- #
def nav_per_unit_series(fund_id: str, weekly: bool = False) -> list[dict]:
    """Daily (or weekly) NAV/unit series, rebased so inception == base_nav_per_unit.

    NAV/unit(t) = base * deployment_NAV(t) / deployment_NAV(inception).
    """
    fund = get_fund(fund_id)
    if not fund:
        raise ValueError(f"Fund '{fund_id}' not found")
    nav_history, _ = _deployment_nav_history(fund["deployment_id"])
    inception = fund["inception_date"]
    base = fund["base_nav_per_unit"]

    pts = [(p["date"], p["nav"]) for p in nav_history
           if p.get("nav") is not None and p["date"] >= inception]
    if not pts:
        return []
    base_nav = pts[0][1]
    if not base_nav:
        return []

    series = [{
        "date": d,
        "nav_per_unit": round(base * nav / base_nav, 4),
        "deployment_nav": nav,
        "return_pct": round((nav / base_nav - 1.0) * 100.0, 4),
    } for d, nav in pts]

    if weekly:
        series = _weekly_sample(series)
    return series


def _weekly_sample(series: list[dict]) -> list[dict]:
    """Last point of each ISO week (the weekly dealing NAV)."""
    by_week: dict[tuple, dict] = {}
    for pt in series:  # ascending → last write per week wins
        y, w, _ = datetime.strptime(pt["date"], "%Y-%m-%d").isocalendar()
        by_week[(y, w)] = pt
    return list(by_week.values())


def publish_weekly(fund_id: str) -> dict:
    """Snapshot the weekly NAV/unit into fund_nav_history. Immutable: existing
    published dates are never overwritten (INSERT OR IGNORE)."""
    weekly = nav_per_unit_series(fund_id, weekly=True)
    now = _now()
    conn = _conn()
    inserted = 0
    try:
        for pt in weekly:
            cur = conn.execute(
                """INSERT OR IGNORE INTO fund_nav_history
                       (fund_id, date, nav_per_unit, deployment_nav, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (fund_id, pt["date"], pt["nav_per_unit"], pt["deployment_nav"], now),
            )
            inserted += cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return {"fund_id": fund_id, "weekly_points": len(weekly), "newly_published": inserted}


def published_nav(fund_id: str) -> list[dict]:
    """The immutable published weekly NAV/unit series."""
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT date, nav_per_unit, deployment_nav FROM fund_nav_history "
            "WHERE fund_id = ? ORDER BY date", (fund_id,)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def nav_per_unit_on(fund_id: str, date: str | None = None) -> tuple[str, float]:
    """NAV/unit on `date` (the latest point on or before it). Latest if date is None."""
    series = nav_per_unit_series(fund_id)
    if not series:
        raise ValueError(f"Fund '{fund_id}' has no NAV history")
    if date is None:
        last = series[-1]
        return last["date"], last["nav_per_unit"]
    eligible = [p for p in series if p["date"] <= date]
    if not eligible:
        raise ValueError(f"No NAV/unit on or before {date} (before fund inception)")
    pt = eligible[-1]
    return pt["date"], pt["nav_per_unit"]


# --------------------------------------------------------------------------- #
# Investors + units ledger
# --------------------------------------------------------------------------- #
def create_investor(name: str, email: str | None = None, notes: str | None = None) -> dict:
    inv_id = _gen_id(name)
    now = _now()
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO investors (id, name, email, notes, created_at) VALUES (?, ?, ?, ?, ?)",
            (inv_id, name, email, notes, now))
        conn.commit()
    finally:
        conn.close()
    return get_investor(inv_id)


def get_investor(investor_id: str) -> dict | None:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM investors WHERE id = ?", (investor_id,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def list_investors() -> list[dict]:
    conn = _conn()
    try:
        rows = conn.execute("SELECT * FROM investors ORDER BY created_at DESC").fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def _holding_units(conn: sqlite3.Connection, fund_id: str, investor_id: str) -> float:
    row = conn.execute(
        "SELECT COALESCE(SUM(units), 0) u FROM investor_transactions WHERE fund_id = ? AND investor_id = ?",
        (fund_id, investor_id)).fetchone()
    return row["u"] or 0.0


def subscribe(fund_id: str, investor_id: str, amount: float, as_of: str | None = None) -> dict:
    """Buy units worth `amount` at the NAV/unit of the dealing date (`as_of` or latest)."""
    if amount <= 0:
        raise ValueError("amount must be positive")
    if not get_fund(fund_id):
        raise ValueError(f"Fund '{fund_id}' not found")
    if not get_investor(investor_id):
        raise ValueError(f"Investor '{investor_id}' not found")
    nav_date, nav = nav_per_unit_on(fund_id, as_of)
    units = round(amount / nav, 6)
    tx_id = _gen_id(f"sub_{investor_id}")
    now = _now()
    conn = _conn()
    try:
        conn.execute(
            """INSERT INTO investor_transactions
                   (id, investor_id, fund_id, date, type, amount, nav_per_unit, units, created_at)
               VALUES (?, ?, ?, ?, 'subscription', ?, ?, ?, ?)""",
            (tx_id, investor_id, fund_id, nav_date, amount, nav, units, now))
        conn.commit()
    finally:
        conn.close()
    return {"id": tx_id, "type": "subscription", "fund_id": fund_id, "investor_id": investor_id,
            "date": nav_date, "amount": amount, "nav_per_unit": nav, "units": units}


def redeem(fund_id: str, investor_id: str, amount: float | None = None,
           units: float | None = None, as_of: str | None = None) -> dict:
    """Redeem by `units` or by `amount` at the dealing-date NAV/unit. Signed negative in the ledger."""
    if (amount is None) == (units is None):
        raise ValueError("provide exactly one of amount or units")
    nav_date, nav = nav_per_unit_on(fund_id, as_of)
    u = units if units is not None else amount / nav
    u = round(u, 6)
    if u <= 0:
        raise ValueError("redemption units/amount must be positive")
    conn = _conn()
    try:
        held = _holding_units(conn, fund_id, investor_id)
        if u > held + 1e-6:
            raise ValueError(f"Cannot redeem {u} units; investor holds {held:.6f}")
        proceeds = round(u * nav, 2)
        tx_id = _gen_id(f"red_{investor_id}")
        conn.execute(
            """INSERT INTO investor_transactions
                   (id, investor_id, fund_id, date, type, amount, nav_per_unit, units, created_at)
               VALUES (?, ?, ?, ?, 'redemption', ?, ?, ?, ?)""",
            (tx_id, investor_id, fund_id, nav_date, -proceeds, nav, -u, _now()))
        conn.commit()
    finally:
        conn.close()
    return {"id": tx_id, "type": "redemption", "fund_id": fund_id, "investor_id": investor_id,
            "date": nav_date, "proceeds": proceeds, "nav_per_unit": nav, "units": -u}


def remove_investor_from_fund(fund_id: str, investor_id: str,
                              as_of: str | None = None) -> dict:
    """Exit an investor from a single fund: redeem their full remaining holding at
    the dealing-date NAV, leaving a redemption on the ledger. The investor record
    and any holdings in OTHER funds are untouched. ValueError if the fund/investor
    don't exist or the investor was never subscribed to this fund."""
    if not get_fund(fund_id):
        raise ValueError(f"Fund '{fund_id}' not found")
    if not get_investor(investor_id):
        raise ValueError(f"Investor '{investor_id}' not found")
    conn = _conn()
    try:
        n_tx = conn.execute(
            "SELECT COUNT(*) c FROM investor_transactions WHERE fund_id = ? AND investor_id = ?",
            (fund_id, investor_id)).fetchone()["c"]
        held = round(_holding_units(conn, fund_id, investor_id), 6)
    finally:
        conn.close()
    if n_tx == 0:
        raise ValueError(f"Investor '{investor_id}' is not subscribed to fund '{fund_id}'")
    if held <= 1e-6:
        return {"fund_id": fund_id, "investor_id": investor_id, "redeemed_units": 0.0,
                "proceeds": 0.0, "redemption": None,
                "note": "investor already holds no units in this fund"}
    red = redeem(fund_id, investor_id, units=held, as_of=as_of)
    return {"fund_id": fund_id, "investor_id": investor_id, "redeemed_units": held,
            "proceeds": red["proceeds"], "redemption": red}


# --------------------------------------------------------------------------- #
# Per-account performance
# --------------------------------------------------------------------------- #
def _xirr(cashflows: list[tuple[str, float]]) -> float | None:
    """Money-weighted (annualized) return. cashflows: (date, amount); investments
    negative, distributions/terminal value positive. Newton + bisection fallback."""
    if len(cashflows) < 2:
        return None
    dates = [datetime.strptime(d, "%Y-%m-%d") for d, _ in cashflows]
    amts = [a for _, a in cashflows]
    if not (any(a > 0 for a in amts) and any(a < 0 for a in amts)):
        return None
    t0 = min(dates)
    yrs = [(dt - t0).days / 365.0 for dt in dates]

    def npv(r):
        return sum(a / (1.0 + r) ** y for a, y in zip(amts, yrs))

    r = 0.1
    for _ in range(100):
        f = npv(r)
        d = sum(-y * a / (1.0 + r) ** (y + 1) for a, y in zip(amts, yrs))
        if abs(d) < 1e-12:
            break
        nr = r - f / d
        if nr <= -0.9999:
            nr = -0.99
        if abs(nr - r) < 1e-9:
            r = nr
            break
        r = nr
    if r == r and abs(npv(r)) < 1e-3:
        return round(r, 6)
    # bisection fallback
    lo, hi = -0.9999, 100.0
    flo, fhi = npv(lo), npv(hi)
    if flo * fhi > 0:
        return None
    for _ in range(300):
        mid = (lo + hi) / 2
        fm = npv(mid)
        if abs(fm) < 1e-7:
            return round(mid, 6)
        if flo * fm < 0:
            hi, fhi = mid, fm
        else:
            lo, flo = mid, fm
    return round((lo + hi) / 2, 6)


def investor_statement(fund_id: str, investor_id: str) -> dict:
    """Full per-account performance: units, value, $ gain, return on capital
    (simple + money-weighted IRR), and the fund's time-weighted return."""
    fund = get_fund(fund_id)
    if not fund:
        raise ValueError(f"Fund '{fund_id}' not found")
    investor = get_investor(investor_id)
    if not investor:
        raise ValueError(f"Investor '{investor_id}' not found")

    conn = _conn()
    try:
        txns = [dict(r) for r in conn.execute(
            "SELECT date, type, amount, nav_per_unit, units FROM investor_transactions "
            "WHERE fund_id = ? AND investor_id = ? ORDER BY date, created_at",
            (fund_id, investor_id)).fetchall()]
    finally:
        conn.close()
    if not txns:
        raise ValueError("Investor has no transactions in this fund")

    units_held = round(sum(t["units"] for t in txns), 6)
    net_invested = round(sum(t["amount"] for t in txns), 2)   # subs + , redemptions -
    contributions = round(sum(t["amount"] for t in txns if t["amount"] > 0), 2)
    nav_date, nav_now = nav_per_unit_on(fund_id, None)
    current_value = round(units_held * nav_now, 2)
    gain = round(current_value - net_invested, 2)
    roc_simple = round(gain / net_invested * 100, 2) if net_invested else None

    # Money-weighted IRR: investor outflows negative, terminal value positive.
    cashflows = [(t["date"], -t["amount"]) for t in txns]
    cashflows.append((nav_date, current_value))
    irr = _xirr(cashflows)

    entry = txns[0]
    # Fund time-weighted return over the holding period (for context).
    fund_twr = round((nav_now / entry["nav_per_unit"] - 1.0) * 100, 2) if entry["nav_per_unit"] else None

    return {
        "fund_id": fund_id, "fund_name": fund["name"],
        "investor_id": investor_id, "investor_name": investor["name"],
        "units_held": units_held,
        "entry_date": entry["date"], "entry_nav_per_unit": entry["nav_per_unit"],
        "nav_per_unit_now": nav_now, "as_of": nav_date,
        "contributions": contributions,
        "net_invested": net_invested,
        "current_value": current_value,
        "gain": gain,
        "return_on_capital_pct": roc_simple,            # simple, cumulative
        "money_weighted_irr_pct": round(irr * 100, 2) if irr is not None else None,  # annualized
        "fund_return_pct": fund_twr,                    # time-weighted, since entry
        "transactions": txns,
    }


def fund_investors(fund_id: str) -> dict:
    """All investor positions in a fund + reconciliation (Σ value == AUM)."""
    if not get_fund(fund_id):
        raise ValueError(f"Fund '{fund_id}' not found")
    nav_date, nav_now = nav_per_unit_on(fund_id, None)
    conn = _conn()
    try:
        rows = conn.execute(
            """SELECT investor_id, COALESCE(SUM(units),0) units, COALESCE(SUM(amount),0) net_invested
               FROM investor_transactions WHERE fund_id = ? GROUP BY investor_id HAVING ABS(units) > 1e-9""",
            (fund_id,)).fetchall()
        names = {r["id"]: r["name"] for r in conn.execute("SELECT id, name FROM investors").fetchall()}
    finally:
        conn.close()
    positions = []
    total_units = 0.0
    for r in rows:
        u = round(r["units"], 6)
        val = round(u * nav_now, 2)
        positions.append({
            "investor_id": r["investor_id"], "investor_name": names.get(r["investor_id"], ""),
            "units": u, "net_invested": round(r["net_invested"], 2),
            "current_value": val, "gain": round(val - r["net_invested"], 2),
        })
        total_units += u
    positions.sort(key=lambda p: p["current_value"], reverse=True)
    return {
        "fund_id": fund_id, "as_of": nav_date, "nav_per_unit": nav_now,
        "units_outstanding": round(total_units, 6),
        "aum": round(total_units * nav_now, 2),
        "investor_count": len(positions),
        "positions": positions,
    }


# --------------------------------------------------------------------------- #
# Execution helper — cash subscription -> order list (replicate the book)
# --------------------------------------------------------------------------- #
def subscription_orders(fund_id: str, amount: float, whole: bool = False) -> dict:
    """Convert a cash subscription into the share orders to execute (e.g. in IB).

    Replicates the fund deployment's CURRENT holdings weights (each open
    position's market value / portfolio NAV), scaled to `amount`. Reports the
    residual cash left after rounding (and, in whole-share mode, any names too
    small to buy a single share)."""
    fund = get_fund(fund_id)
    if not fund:
        raise ValueError(f"Fund '{fund_id}' not found")
    if amount <= 0:
        raise ValueError("amount must be positive")

    from deploy_engine import get_deployment, build_position_book
    d = get_deployment(fund["deployment_id"])
    if not d:
        raise ValueError(f"Deployment '{fund['deployment_id']}' not found")
    book = build_position_book(d.get("sleeves") or [], d.get("initial_capital") or 0)
    pv = book.get("portfolio_value") or 0
    if pv <= 0:
        raise ValueError("Deployment has no portfolio value to replicate")

    nav_history = d.get("nav_history") or []
    as_of = d.get("last_evaluated") or (nav_history[-1]["date"] if nav_history else None)

    orders, skipped = [], []
    invested = 0.0
    for p in book["positions"]:
        if p.get("status") != "open":
            continue
        price = p.get("current_price") or 0
        weight = (p["market_value"] / pv) if pv else 0.0
        target_value = weight * amount
        if price <= 0:
            skipped.append({"symbol": p["symbol"], "reason": "no price", "target_value": round(target_value, 2)})
            continue
        raw = target_value / price
        shares = float(int(raw)) if whole else round(raw, 6)
        if shares <= 0:
            skipped.append({"symbol": p["symbol"], "reason": "below 1 share",
                            "target_value": round(target_value, 2), "price": round(price, 4)})
            continue
        order_value = round(shares * price, 2)
        invested += order_value
        orders.append({
            "symbol": p["symbol"], "side": "BUY",
            "weight": round(weight, 6),
            "target_value": round(target_value, 2),
            "price": round(price, 4),
            "shares": shares,
            "order_value": order_value,
            "weight_actual": round(order_value / amount, 6),
        })

    orders.sort(key=lambda o: o["order_value"], reverse=True)
    invested = round(invested, 2)
    residual = round(amount - invested, 2)
    max_drift = max((abs(o["weight_actual"] - o["weight"]) for o in orders), default=0.0)
    return {
        "fund_id": fund_id, "deployment_id": fund["deployment_id"], "as_of": as_of,
        "amount": round(amount, 2), "share_mode": "whole" if whole else "fractional",
        "orders": orders, "order_count": len(orders),
        "invested": invested,
        "residual_cash": residual,
        "residual_pct": round(residual / amount * 100, 3) if amount else 0,
        "max_weight_drift_pct": round(max_drift * 100, 3),
        "skipped": skipped,
    }


# --------------------------------------------------------------------------- #
# Commingled execution ledger (fund_orders)
# --------------------------------------------------------------------------- #
def _targets_and_prices(deployment_id: str):
    """(target weights, prices, deployment) from the deployment's current holdings."""
    from deploy_engine import get_deployment, build_position_book
    d = get_deployment(deployment_id)
    if not d:
        raise ValueError(f"Deployment '{deployment_id}' not found")
    book = build_position_book(d.get("sleeves") or [], d.get("initial_capital") or 0)
    pv = book.get("portfolio_value") or 0
    weights, prices = {}, {}
    for p in book["positions"]:
        if p.get("status") == "open" and (p.get("current_price") or 0) > 0:
            if pv:
                weights[p["symbol"]] = p["market_value"] / pv
            prices[p["symbol"]] = p["current_price"]
    return weights, prices, d


def _market_prices(symbols) -> dict:
    """Latest close per symbol from market.db — fallback for names the deployment
    no longer holds (dropped from the ranking) but the fund still owns."""
    symbols = [s for s in symbols]
    if not symbols:
        return {}
    mdb = os.environ.get("MARKET_DB_PATH", str(_BASE / "market.db"))
    conn = sqlite3.connect(mdb)
    out = {}
    try:
        for s in symbols:
            r = conn.execute(
                "SELECT close FROM prices WHERE symbol = ? ORDER BY date DESC LIMIT 1", (s,)).fetchone()
            if r and r[0]:
                out[s] = r[0]
    finally:
        conn.close()
    return out


def symbol_sectors(symbols) -> dict:
    """symbol -> GICS sector from market.db universe_profiles (for exposure tables).
    Symbols with no profile row are simply absent from the result."""
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        return {}
    mdb = os.environ.get("MARKET_DB_PATH", str(_BASE / "market.db"))
    conn = sqlite3.connect(mdb)
    out = {}
    try:
        qs = ",".join("?" * len(symbols))
        for sym, sec in conn.execute(
            f"SELECT symbol, sector FROM universe_profiles WHERE symbol IN ({qs})",
            symbols,
        ):
            if sec:
                out[sym] = sec
    finally:
        conn.close()
    return out


def fund_actual_book(fund_id: str) -> dict:
    """The fund's REAL book: holdings from executed fills, cash from investor
    net flows +/- fills, marked at current prices. This is what IB should hold."""
    fund = get_fund(fund_id)
    if not fund:
        raise ValueError(f"Fund '{fund_id}' not found")
    _, prices, d = _targets_and_prices(fund["deployment_id"])
    conn = _conn()
    try:
        net_cash_in = conn.execute(
            "SELECT COALESCE(SUM(amount),0) a FROM investor_transactions WHERE fund_id = ?",
            (fund_id,)).fetchone()["a"] or 0.0
        fills = conn.execute(
            "SELECT symbol, side, fill_shares, fill_price FROM fund_orders "
            "WHERE fund_id = ? AND status = 'executed'", (fund_id,)).fetchall()
    finally:
        conn.close()
    holdings: dict[str, float] = {}
    cash = net_cash_in
    for f in fills:
        sh, px = (f["fill_shares"] or 0.0), (f["fill_price"] or 0.0)
        if f["side"] == "BUY":
            holdings[f["symbol"]] = holdings.get(f["symbol"], 0.0) + sh
            cash -= sh * px
        else:
            holdings[f["symbol"]] = holdings.get(f["symbol"], 0.0) - sh
            cash += sh * px
    missing = [s for s, sh in holdings.items() if abs(sh) > 1e-9 and s not in prices]
    if missing:
        prices = {**prices, **_market_prices(missing)}

    positions, holdings_mv = [], 0.0
    for sym, sh in holdings.items():
        if abs(sh) < 1e-4:   # sub-0.0001-share dust (rounding) is not a real position
            continue
        px = prices.get(sym, 0.0)
        mv = sh * px
        holdings_mv += mv
        positions.append({"symbol": sym, "shares": round(sh, 6),
                          "price": round(px, 4), "market_value": round(mv, 2)})
    positions.sort(key=lambda p: p["market_value"], reverse=True)
    nav_history = d.get("nav_history") or []
    return {
        "fund_id": fund_id,
        "as_of": d.get("last_evaluated") or (nav_history[-1]["date"] if nav_history else None),
        "cash": round(cash, 2), "holdings_value": round(holdings_mv, 2),
        "aum": round(cash + holdings_mv, 2), "positions": positions,
    }


def generate_orders(fund_id: str, whole: bool = False, source: str = "daily") -> dict:
    """Compute the orders to bring the fund's real book to the deployed target
    weights (× current AUM), and write them as a pending batch. Run this daily /
    after subscriptions: it absorbs new cash (buys), redemptions (sells), and
    rebalances (deltas) in one shot."""
    fund = get_fund(fund_id)
    if not fund:
        raise ValueError(f"Fund '{fund_id}' not found")
    weights, prices, _ = _targets_and_prices(fund["deployment_id"])
    book = fund_actual_book(fund_id)
    aum = book["aum"]
    if aum <= 0:
        raise ValueError("Fund has no AUM yet (no subscriptions)")
    cur = {p["symbol"]: p for p in book["positions"]}
    now = _now()
    batch_id = _gen_id(f"{fund_id[:12]}_batch")
    batch_date = book["as_of"] or now[:10]

    orders = []
    for sym in set(weights) | set(cur):
        price = prices.get(sym) or cur.get(sym, {}).get("price") or 0
        if price <= 0:
            continue
        delta_val = weights.get(sym, 0.0) * aum - cur.get(sym, {}).get("market_value", 0.0)
        raw = delta_val / price
        shares = float(int(raw)) if whole else round(raw, 6)
        if (whole and abs(shares) < 1) or (not whole and abs(shares) < 1e-4):
            continue
        mag = abs(shares)
        orders.append({
            "id": _gen_id(f"ord_{sym}"), "fund_id": fund_id, "batch_id": batch_id,
            "batch_date": batch_date, "source": source, "symbol": sym,
            "side": "BUY" if shares > 0 else "SELL", "shares": round(mag, 6),
            "est_price": round(price, 4), "est_value": round(mag * price, 2),
            "status": "pending", "created_at": now,
        })
    orders.sort(key=lambda o: (o["side"], -o["est_value"]))
    conn = _conn()
    try:
        conn.executemany(
            """INSERT INTO fund_orders (id, fund_id, batch_id, batch_date, source, symbol,
                   side, shares, est_price, est_value, status, created_at)
               VALUES (:id,:fund_id,:batch_id,:batch_date,:source,:symbol,:side,:shares,
                       :est_price,:est_value,:status,:created_at)""", orders)
        conn.commit()
    finally:
        conn.close()
    return {
        "fund_id": fund_id, "batch_id": batch_id, "batch_date": batch_date, "source": source,
        "share_mode": "whole" if whole else "fractional", "aum": aum,
        "order_count": len(orders),
        "buy_value": round(sum(o["est_value"] for o in orders if o["side"] == "BUY"), 2),
        "sell_value": round(sum(o["est_value"] for o in orders if o["side"] == "SELL"), 2),
        "orders": orders,
    }


def list_orders(fund_id: str, status: str | None = None, batch_id: str | None = None) -> list[dict]:
    q = "SELECT * FROM fund_orders WHERE fund_id = ?"
    params: list = [fund_id]
    if status:
        q += " AND status = ?"; params.append(status)
    if batch_id:
        q += " AND batch_id = ?"; params.append(batch_id)
    q += " ORDER BY created_at DESC, side, est_value DESC"
    conn = _conn()
    try:
        rows = conn.execute(q, params).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def record_fill(order_id: str, fill_price: float | None = None,
                fill_shares: float | None = None) -> dict:
    """Mark an order executed with the actual IB fill (defaults to est_price/shares)."""
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM fund_orders WHERE id = ?", (order_id,)).fetchone()
        if not row:
            raise ValueError(f"Order '{order_id}' not found")
        if row["status"] == "executed":
            raise PermissionError("Order already executed")
        fp = fill_price if fill_price is not None else row["est_price"]
        fs = fill_shares if fill_shares is not None else row["shares"]
        conn.execute(
            "UPDATE fund_orders SET status='executed', fill_price=?, fill_shares=?, fill_time=? WHERE id=?",
            (fp, fs, _now(), order_id))
        conn.commit()
    finally:
        conn.close()
    return {"order_id": order_id, "status": "executed", "fill_price": fp, "fill_shares": fs}


def fill_batch(batch_id: str, at_estimate: bool = True) -> dict:
    """Mark every pending order in a batch executed at its estimated price
    (convenience when you fill the whole batch at/near the marks)."""
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT id, est_price, shares FROM fund_orders WHERE batch_id = ? AND status = 'pending'",
            (batch_id,)).fetchall()
        now = _now()
        for r in rows:
            conn.execute(
                "UPDATE fund_orders SET status='executed', fill_price=?, fill_shares=?, fill_time=? WHERE id=?",
                (r["est_price"], r["shares"], now, r["id"]))
        conn.commit()
    finally:
        conn.close()
    return {"batch_id": batch_id, "filled": len(rows)}


# --------------------------------------------------------------------------- #
# Deletion (guarded: refuse to drop anything with live units unless forced)
# --------------------------------------------------------------------------- #
def delete_fund(fund_id: str, force: bool = False) -> dict:
    """Delete a fund + its NAV history + its transactions. Blocked (PermissionError)
    if investors still hold units, unless force=True. ValueError if not found."""
    conn = _conn()
    try:
        if not conn.execute("SELECT 1 FROM funds WHERE id = ?", (fund_id,)).fetchone():
            raise ValueError(f"Fund '{fund_id}' not found")
        live = round(conn.execute(
            "SELECT COALESCE(SUM(units), 0) u FROM investor_transactions WHERE fund_id = ?",
            (fund_id,)).fetchone()["u"] or 0.0, 6)
        if abs(live) > 1e-6 and not force:
            raise PermissionError(
                f"Fund has {live} units held by investors — redeem them or pass force=true")
        n = conn.execute("SELECT COUNT(*) c FROM investor_transactions WHERE fund_id = ?",
                         (fund_id,)).fetchone()["c"]
        conn.execute("DELETE FROM investor_transactions WHERE fund_id = ?", (fund_id,))
        conn.execute("DELETE FROM fund_nav_history WHERE fund_id = ?", (fund_id,))
        conn.execute("DELETE FROM fund_orders WHERE fund_id = ?", (fund_id,))
        conn.execute("DELETE FROM funds WHERE id = ?", (fund_id,))
        conn.commit()
    finally:
        conn.close()
    return {"deleted": fund_id, "transactions_removed": n, "live_units_at_delete": live}


def delete_investor(investor_id: str, force: bool = False) -> dict:
    """Delete an investor + their transactions. Blocked if they hold units anywhere
    unless force=True. ValueError if not found."""
    conn = _conn()
    try:
        if not conn.execute("SELECT 1 FROM investors WHERE id = ?", (investor_id,)).fetchone():
            raise ValueError(f"Investor '{investor_id}' not found")
        live = round(conn.execute(
            "SELECT COALESCE(SUM(units), 0) u FROM investor_transactions WHERE investor_id = ?",
            (investor_id,)).fetchone()["u"] or 0.0, 6)
        if abs(live) > 1e-6 and not force:
            raise PermissionError(
                f"Investor still holds {live} units — redeem first or pass force=true")
        n = conn.execute("SELECT COUNT(*) c FROM investor_transactions WHERE investor_id = ?",
                         (investor_id,)).fetchone()["c"]
        conn.execute("DELETE FROM investor_transactions WHERE investor_id = ?", (investor_id,))
        conn.execute("DELETE FROM investors WHERE id = ?", (investor_id,))
        conn.commit()
    finally:
        conn.close()
    return {"deleted": investor_id, "transactions_removed": n, "live_units_at_delete": live}
