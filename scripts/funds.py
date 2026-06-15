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
