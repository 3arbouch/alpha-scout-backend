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
