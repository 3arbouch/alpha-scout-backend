"""Daily composite-score persistence for deployed scoring strategies.

A composite score is a pure function of (universe, composite_score config,
factor values in features_daily as-of the date) — independent of the portfolio's
positions/NAV. So we can compute the full DAILY score+rank panel for a
deployment standalone (and backfill it retroactively), reusing the engine's own
`rank_candidates_with_detail` so scores match exactly what the strategy sees.

Persisted per (deployment, sleeve, date, symbol): the composite score, its
cross-sectional rank, whether it would be selected (within the ranking cutoff),
and whether it was actually held that day. Per-bucket/per-factor detail is NOT
stored (heavy); it is recomputed on demand for a single day via day_detail().
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def _market_conn():
    from backtest_engine import get_connection
    return get_connection()


def _app_conn():
    from deploy_engine import get_db
    return get_db()


def init_table(conn) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS deployment_scores (
            deployment_id TEXT NOT NULL,
            sleeve_label  TEXT NOT NULL,
            date          TEXT NOT NULL,
            symbol        TEXT NOT NULL,
            score         REAL,
            rank          INTEGER,
            selected      INTEGER NOT NULL DEFAULT 0,
            held          INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (deployment_id, sleeve_label, date, symbol)
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_depscores_dep_date "
        "ON deployment_scores (deployment_id, date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_depscores_dep_symbol "
        "ON deployment_scores (deployment_id, symbol)"
    )


def _held_by_date(deploy_id: str) -> dict:
    """{date: set(symbols held)} from the deployment's persisted nav_history."""
    from deploy_engine import DEPLOYMENTS_DIR
    results = Path(DEPLOYMENTS_DIR) / deploy_id / "results.json"
    if not results.exists():
        return {}
    try:
        data = json.loads(results.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    out: dict[str, set] = {}
    for entry in data.get("combined_nav_history", []):
        d = entry.get("date")
        if not d:
            continue
        out[d] = set((entry.get("positions") or {}).keys())
    return out


def _scoring_sleeves(config: dict) -> list[dict]:
    """Sleeves whose ranking is composite_score (others have nothing to score)."""
    out = []
    for sleeve in config.get("sleeves", []):
        scfg = sleeve.get("strategy_config", {})
        if (scfg.get("ranking") or {}).get("by") == "composite_score":
            out.append(sleeve)
    return out


def compute_and_persist(deploy_id: str, full: bool = False) -> dict:
    """Compute + upsert the daily score panel for a deployment.

    Incremental by default: only fills dates after the latest already stored.
    `full=True` recomputes the whole history from the deployment start_date.
    Returns a small summary dict.
    """
    from backtest_engine import (
        build_price_index, rank_candidates_with_detail, _load_feature_series,
        resolve_universe,
    )

    app = _app_conn()
    init_table(app)
    dep = app.execute(
        "SELECT config_json, start_date FROM deployments WHERE id = ?", (deploy_id,)
    ).fetchone()
    if not dep:
        app.close()
        raise ValueError(f"Deployment '{deploy_id}' not found")
    config = json.loads(dep["config_json"])
    start_date = dep["start_date"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    since = None
    if not full:
        row = app.execute(
            "SELECT MAX(date) AS m FROM deployment_scores WHERE deployment_id = ?",
            (deploy_id,),
        ).fetchone()
        since = row["m"] if row else None

    held = _held_by_date(deploy_id)
    mconn = _market_conn()
    rows: list[tuple] = []
    dates_written: set = set()

    for sleeve in _scoring_sleeves(config):
        scfg = sleeve["strategy_config"]
        label = sleeve.get("label", "sleeve")
        ranking = scfg.get("ranking") or {}
        top_n = ranking.get("top_n") or (scfg.get("sizing") or {}).get("max_positions")

        symbols = resolve_universe(scfg, mconn)
        if not symbols:
            continue
        price_index, _open, all_dates = build_price_index(symbols, mconn)
        dates = [d for d in all_dates
                 if start_date <= d <= today and (since is None or d > since)]
        if not dates:
            continue

        # Precompute factor series once over the needed window.
        composite_cfg = scfg.get("composite_score") or {}
        factor_names: set = set()
        for b in (composite_cfg.get("buckets") or {}).values():
            for f in b.get("factors", []):
                factor_names.add(f["name"] if isinstance(f, dict) else f)
        composite_series = {
            fn: _load_feature_series(fn, symbols, dates[0], dates[-1], mconn,
                                     price_index=price_index)
            for fn in factor_names
        }

        candidates = [(s, {}) for s in symbols]
        for date in dates:
            _sorted, info = rank_candidates_with_detail(
                candidates, scfg, mconn, date, price_index,
                composite_series=composite_series,
            )
            scores = info.get("scores") or {}
            ranks = info.get("ranks") or {}
            held_set = held.get(date, set())
            for sym in symbols:
                sc = scores.get(sym)
                if sc is None:
                    continue
                rk = ranks.get(sym)
                selected = 1 if (top_n and rk is not None and rk <= top_n) else 0
                rows.append((deploy_id, label, date, sym, float(sc),
                             rk, selected, 1 if sym in held_set else 0))
            dates_written.add(date)

    if rows:
        app.executemany(
            "INSERT OR REPLACE INTO deployment_scores "
            "(deployment_id, sleeve_label, date, symbol, score, rank, selected, held) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        app.commit()
    mconn.close()
    app.close()
    return {
        "deployment_id": deploy_id,
        "rows_written": len(rows),
        "dates": len(dates_written),
        "from": (min(dates_written) if dates_written else None),
        "to": (max(dates_written) if dates_written else None),
        "mode": "full" if full else "incremental",
    }


# ---------------------------------------------------------------------------
# Read helpers (back the API endpoints)
# ---------------------------------------------------------------------------

def series(deploy_id: str, symbols: list[str] | None = None,
           start: str | None = None, end: str | None = None,
           sleeve: str | None = None) -> dict:
    """{sleeve_label: {symbol: [{date, score, rank, selected, held}, ...]}}."""
    app = _app_conn()
    q = ("SELECT sleeve_label, symbol, date, score, rank, selected, held "
         "FROM deployment_scores WHERE deployment_id = ?")
    params: list = [deploy_id]
    if sleeve:
        q += " AND sleeve_label = ?"; params.append(sleeve)
    if symbols:
        q += f" AND symbol IN ({','.join('?' * len(symbols))})"; params += symbols
    if start:
        q += " AND date >= ?"; params.append(start)
    if end:
        q += " AND date <= ?"; params.append(end)
    q += " ORDER BY sleeve_label, symbol, date"
    out: dict = {}
    for r in app.execute(q, params):
        out.setdefault(r["sleeve_label"], {}).setdefault(r["symbol"], []).append({
            "date": r["date"], "score": r["score"], "rank": r["rank"],
            "selected": bool(r["selected"]), "held": bool(r["held"]),
        })
    app.close()
    return out


def ranks(deploy_id: str, start: str | None = None, end: str | None = None,
          sleeve: str | None = None) -> list:
    """Per-date FULL-universe leaderboard (no cap): one entry per (date, sleeve)
    with every scored symbol ordered by rank, each tagged selected/held."""
    app = _app_conn()
    q = ("SELECT sleeve_label, date, symbol, score, rank, selected, held "
         "FROM deployment_scores WHERE deployment_id = ?")
    params: list = [deploy_id]
    if sleeve:
        q += " AND sleeve_label = ?"; params.append(sleeve)
    if start:
        q += " AND date >= ?"; params.append(start)
    if end:
        q += " AND date <= ?"; params.append(end)
    q += " ORDER BY sleeve_label, date, rank"
    grouped: dict = {}
    for r in app.execute(q, params):
        key = (r["date"], r["sleeve_label"])
        grouped.setdefault(key, []).append({
            "symbol": r["symbol"], "score": r["score"], "rank": r["rank"],
            "selected": bool(r["selected"]), "held": bool(r["held"]),
        })
    app.close()
    return [
        {"date": d, "sleeve_label": sl, "n": len(rowset), "rows": rowset}
        for (d, sl), rowset in sorted(grouped.items())
    ]


def day_detail(deploy_id: str, date: str, sleeve: str | None = None) -> dict:
    """Full leaderboard for ONE day WITH per-bucket/per-factor detail, recomputed
    on the fly (detail isn't persisted). Mirrors a ranking_history event shape so
    the frontend can reuse its 'why this pick' renderer for any date."""
    from backtest_engine import (
        build_price_index, rank_candidates_with_detail, _load_feature_series,
        resolve_universe,
    )
    app = _app_conn()
    dep = app.execute(
        "SELECT config_json FROM deployments WHERE id = ?", (deploy_id,)
    ).fetchone()
    if not dep:
        app.close()
        raise ValueError(f"Deployment '{deploy_id}' not found")
    config = json.loads(dep["config_json"])
    app.close()

    held = _held_by_date(deploy_id).get(date, set())
    mconn = _market_conn()
    sleeves_out = []
    for sl in _scoring_sleeves(config):
        scfg = sl["strategy_config"]
        label = sl.get("label", "sleeve")
        if sleeve and label != sleeve:
            continue
        ranking = scfg.get("ranking") or {}
        top_n = ranking.get("top_n") or (scfg.get("sizing") or {}).get("max_positions")
        symbols = resolve_universe(scfg, mconn)
        price_index, _open, _dates = build_price_index(symbols, mconn)
        composite_cfg = scfg.get("composite_score") or {}
        factor_names: set = set()
        for b in (composite_cfg.get("buckets") or {}).values():
            for f in b.get("factors", []):
                factor_names.add(f["name"] if isinstance(f, dict) else f)
        composite_series = {
            fn: _load_feature_series(fn, symbols, date, date, mconn, price_index=price_index)
            for fn in factor_names
        }
        _sorted, info = rank_candidates_with_detail(
            [(s, {}) for s in symbols], scfg, mconn, date, price_index,
            composite_series=composite_series,
        )
        scores = info.get("scores") or {}
        ranks_ = info.get("ranks") or {}
        details = info.get("details") or {}
        candidates = []
        for sym in symbols:
            if sym not in scores:
                continue
            rk = ranks_.get(sym)
            candidates.append({
                "symbol": sym, "score": scores[sym], "rank": rk,
                "selected": bool(top_n and rk is not None and rk <= top_n),
                "held": sym in held,
                "buckets": details.get(sym, {}).get("buckets"),
            })
        candidates.sort(key=lambda c: (c["rank"] is None, c["rank"] or 0))
        sleeves_out.append({
            "sleeve_label": label, "by": info.get("by"), "order": info.get("order"),
            "standardization": (composite_cfg.get("standardization") or "rank"),
            "top_n_cutoff": top_n, "n_candidates": len(candidates),
            "n_selected": sum(c["selected"] for c in candidates),
            "candidates": candidates,
        })
    mconn.close()
    return {"deployment_id": deploy_id, "date": date, "sleeves": sleeves_out}
