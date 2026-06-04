"""
alpha_combine — solve cross-sectional factor weights from data (Level-1 combiner)
+ a leakage-safe out-of-sample rank-IC estimator (purged k-fold).

This is the math behind the `combine_factors` agent tool. It turns a set of
factors into a *solved* composite_score weight block — IC-optimal, decorrelated,
shrunk toward equal weight — instead of hand-set bucket weights, and reports the
out-of-sample IC of the combination via purged cross-validation so the agent can
tell whether the weighting generalizes.

Pure numpy/scipy. No engine change: the output is a normal `composite_score`
block the backtest engine already consumes.

Key correctness properties:
  - Labels are FORWARD returns over `horizon`; the last `horizon` trading days
    of the window have no label and are dropped (boundary purge), so no row uses
    a price beyond the data the caller is allowed to see (`end`/stop_date).
  - Factors are rank-standardized cross-sectionally per date (matches the
    engine's composite_score `standardization: "rank"`).
  - Purged k-fold embargoes `horizon` days around each test block so overlapping
    forward-return windows don't leak train→test.
"""
from __future__ import annotations

import sqlite3
import numpy as np

# Numeric features available in features_daily (the agent's factor menu).
_HORIZON_DAYS = {"1m": 21, "21d": 21, "3m": 63, "63d": 63,
                 "6m": 126, "126d": 126, "12m": 252, "252d": 252}


def _horizon_to_days(h: str) -> int:
    if h in _HORIZON_DAYS:
        return _HORIZON_DAYS[h]
    if isinstance(h, str) and h.endswith("d") and h[:-1].isdigit():
        return int(h[:-1])
    raise ValueError(f"unrecognized horizon {h!r}; use e.g. '63d','3m','6m','12m'")


_FWD_CACHE: dict = {}  # (horizon_days, start, end, sector) -> {(symbol,date): fwd_ret}


def _forward_returns(db_path, horizon_days, start, end, sector):
    """Forward returns per (symbol, date) at the horizon. Cached on the window —
    independent of the factor set, so sweeping different factors over the same
    window doesn't re-read prices."""
    key = (horizon_days, start, end, sector)
    if key in _FWD_CACHE:
        return _FWD_CACHE[key]
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    sym_filter, params_sym = "", []
    if sector:
        sym_filter = "AND symbol IN (SELECT symbol FROM universe_profiles WHERE sector = ?)"
        params_sym = [sector]
    pr = conn.execute(
        f"SELECT symbol, date, close FROM prices "
        f"WHERE close IS NOT NULL {('AND date >= ?') if start else ''} "
        f"{('AND date <= ?') if end else ''} {sym_filter} ORDER BY symbol, date",
        ([start] if start else []) + ([end] if end else []) + params_sym,
    ).fetchall()
    conn.close()
    fwd = {}
    cur_sym, closes, dts = None, [], []
    def _flush(sym, dts, closes):
        c = np.asarray(closes, float)
        for i in range(len(c) - horizon_days):
            if c[i] > 0:
                fwd[(sym, dts[i])] = c[i + horizon_days] / c[i] - 1.0
    for r in pr:
        if r["symbol"] != cur_sym:
            if cur_sym is not None:
                _flush(cur_sym, dts, closes)
            cur_sym, closes, dts = r["symbol"], [], []
        closes.append(r["close"]); dts.append(r["date"])
    if cur_sym is not None:
        _flush(cur_sym, dts, closes)
    _FWD_CACHE[key] = fwd
    return fwd


def _load_panel(db_path, factors, horizon_days, start, end, sector):
    """Return (dates[str], X[n,k], y[n]) — features rank-ready + forward returns.

    Rows without a full-horizon label (last H bars) are dropped; `end` caps both
    feature dates and price history, so no label reaches past it.
    """
    fwd = _forward_returns(db_path, horizon_days, start, end, sector)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    sym_filter, params_sym = "", []
    if sector:
        sym_filter = "AND symbol IN (SELECT symbol FROM universe_profiles WHERE sector = ?)"
        params_sym = [sector]

    # --- features ---
    cols = ", ".join(factors)
    frows = conn.execute(
        f"SELECT symbol, date, {cols} FROM features_daily "
        f"WHERE 1=1 {('AND date >= ?') if start else ''} "
        f"{('AND date <= ?') if end else ''} {sym_filter} "
        f"ORDER BY date, symbol",
        ([start] if start else []) + ([end] if end else []) + params_sym,
    ).fetchall()
    conn.close()

    dates, Xrows, yrows = [], [], []
    for r in frows:
        key = (r["symbol"], r["date"])
        label = fwd.get(key)
        if label is None:
            continue
        feats = [r[f] for f in factors]
        if any(v is None for v in feats):
            continue
        dates.append(r["date"]); Xrows.append(feats); yrows.append(label)

    if not yrows:
        return np.array([]), np.zeros((0, len(factors))), np.array([])
    return np.array(dates), np.asarray(Xrows, float), np.asarray(yrows, float)


def _date_groups(dates):
    """Contiguous (start, stop) index slices per unique date (dates sorted)."""
    uniq, idx = np.unique(dates, return_index=True)
    bounds = list(idx) + [len(dates)]
    return uniq, [(bounds[i], bounds[i + 1]) for i in range(len(uniq))]


def _rank01_cols(M):
    """Rank-standardize each COLUMN of M to [-0.5, 0.5] (ordinal ties), vectorized."""
    n = M.shape[0]
    if n < 2:
        return np.zeros_like(M)
    r = M.argsort(0).argsort(0).astype(float)
    return r / (n - 1) - 0.5


def _ranked_panel(X, y, groups):
    """Rank-standardize each factor and y cross-sectionally per date (vectorized)."""
    Xr = np.empty_like(X)
    yr = np.empty_like(y)
    for s, e in groups:
        if e - s < 2:
            Xr[s:e] = 0.0; yr[s:e] = 0.0
            continue
        Xr[s:e] = _rank01_cols(X[s:e])
        yr[s:e] = _rank01_cols(y[s:e].reshape(-1, 1)).ravel()
    return Xr, yr


def _factor_ic(Xr, yr, groups):
    """Mean per-date rank-IC for each factor (Spearman = Pearson on ranks), vectorized."""
    k = Xr.shape[1]
    acc = np.zeros(k); n = 0
    for s, e in groups:
        if e - s < 5:
            continue
        n += 1
        xc = Xr[s:e] - Xr[s:e].mean(0)
        yc = yr[s:e] - yr[s:e].mean()
        den = np.sqrt((xc * xc).sum(0)) * np.sqrt((yc * yc).sum())
        with np.errstate(divide="ignore", invalid="ignore"):
            acc += np.where(den > 0, (xc * yc[:, None]).sum(0) / den, 0.0)
    return acc / max(n, 1)


def _solve_weights(ic_aligned, Sigma, method, shrinkage):
    """Solve nonneg weights summing to 1. ic_aligned >= 0 (already sign-aligned)."""
    k = len(ic_aligned)
    if method == "equal":
        w = np.ones(k)
    elif method == "ic_weighted":
        w = np.clip(ic_aligned, 0, None)
    else:  # ic_optimal: Σ^{-1} · IC with diagonal shrinkage for stability
        S = 0.8 * Sigma + 0.2 * np.diag(np.diag(Sigma)) + 1e-4 * np.eye(k)
        try:
            w = np.linalg.solve(S, ic_aligned)
        except np.linalg.LinAlgError:
            w = np.clip(ic_aligned, 0, None)
    w = np.clip(w, 0, None)
    if w.sum() <= 0:
        w = np.ones(k)
    w = w / w.sum()
    # shrink toward equal weight
    w = (1 - shrinkage) * w + shrinkage * (np.ones(k) / k)
    return w / w.sum()


def _combined_ic(Xr_signed, yr, groups, w):
    """Per-date IC of the weighted-combined score (Xr already sign-aligned)."""
    score = Xr_signed @ w
    acc = 0.0; n = 0
    for s, e in groups:
        if e - s < 5:
            continue
        sc = score[s:e] - score[s:e].mean()
        yc = yr[s:e] - yr[s:e].mean()
        den = np.sqrt((sc * sc).sum() * (yc * yc).sum())
        if den > 0:
            n += 1
            acc += float((sc * yc).sum() / den)
    return acc / max(n, 1)


def _purged_oos_ic(Xr_signed, yr, dates, method, shrinkage, k_folds=5, embargo_days=63):
    """Leakage-safe OOS rank-IC of the combination (Piece 0).

    Split unique dates into k contiguous blocks. For each test block, fit weights
    on the other blocks MINUS an embargo of `embargo_days` trading days around the
    test block (so overlapping forward-return windows can't leak), then score the
    test block. Returns (mean_oos_ic, per_fold). Also used for the equal-weight
    baseline by passing method='equal'.
    """
    uniq, groups = _date_groups(dates)
    nU = len(uniq)
    if nU < k_folds * 2:
        return None, []
    # Map each row to its unique-date index once → fold masks are pure integer
    # comparisons (no per-row Python set lookups).
    row_uidx = np.searchsorted(uniq, dates)
    fold_edges = np.linspace(0, nU, k_folds + 1).astype(int)
    per_fold = []
    for i in range(k_folds):
        t0, t1 = fold_edges[i], fold_edges[i + 1]
        lo = max(0, t0 - embargo_days); hi = min(nU, t1 + embargo_days)
        test_mask = (row_uidx >= t0) & (row_uidx < t1)
        train_mask = (row_uidx < lo) | (row_uidx >= hi)
        if train_mask.sum() < 100 or test_mask.sum() < 50:
            continue
        # fit on train
        _, tg = _date_groups(dates[train_mask])
        ic_tr = _factor_ic(Xr_signed[train_mask], yr[train_mask], tg)
        Sig_tr = np.cov(Xr_signed[train_mask].T) if Xr_signed.shape[1] > 1 else np.array([[1.0]])
        w = _solve_weights(np.clip(ic_tr, 0, None), np.atleast_2d(Sig_tr), method, shrinkage)
        # score test
        _, teg = _date_groups(dates[test_mask])
        per_fold.append(_combined_ic(Xr_signed[test_mask], yr[test_mask], teg, w))
    if not per_fold:
        return None, []
    return float(np.mean(per_fold)), [round(x, 4) for x in per_fold]


_PANEL_CACHE: dict = {}  # (factors, horizon, start, end, sector) -> ranked panel + base IC


def _prepare_panel(db_path, factors, horizon, start, end, sector):
    """Load + rank + base-IC, cached on the data-defining args (independent of
    method/shrinkage, so the agent can sweep those for free)."""
    key = (tuple(factors), horizon, start, end, sector)
    if key in _PANEL_CACHE:
        return _PANEL_CACHE[key]
    H = _horizon_to_days(horizon)
    dates, X, y = _load_panel(db_path, factors, H, start, end, sector)
    if len(y) < 200:
        out = {"error": f"insufficient labeled rows ({len(y)}) for {factors} over window"}
        _PANEL_CACHE[key] = out
        return out
    uniq, groups = _date_groups(dates)
    Xr, yr = _ranked_panel(X, y, groups)
    ic = _factor_ic(Xr, yr, groups)
    signs = np.where(ic >= 0, 1.0, -1.0)
    out = {"dates": dates, "uniq": uniq, "Xr_signed": Xr * signs, "yr": yr,
           "ic": ic, "signs": signs, "H": H}
    _PANEL_CACHE[key] = out
    return out


def combine_factors(factors, horizon="63d", method="ic_optimal", shrinkage=0.3,
                    sector=None, start=None, end=None, db_path=None):
    """Solve a composite_score weight block from data + report OOS IC.

    Returns a dict: {composite_score block, diagnostics}. See module docstring.
    """
    if not factors or len(factors) < 1:
        return {"error": "provide at least one factor"}

    p = _prepare_panel(db_path, factors, horizon, start, end, sector)
    if "error" in p:
        return {"error": p["error"]}
    dates, uniq, Xr_signed, yr = p["dates"], p["uniq"], p["Xr_signed"], p["yr"]
    ic, signs, H = p["ic"], p["signs"], p["H"]
    _, groups = _date_groups(dates)
    ic_aligned = np.abs(ic)
    Sigma = np.cov(Xr_signed.T) if len(factors) > 1 else np.array([[1.0]])

    w = _solve_weights(ic_aligned, np.atleast_2d(Sigma), method, shrinkage)

    is_ic = _combined_ic(Xr_signed, yr, groups, w)                       # in-sample
    oos_ic, oos_folds = _purged_oos_ic(Xr_signed, yr, dates, method, shrinkage, embargo_days=H)
    eq_oos, _ = _purged_oos_ic(Xr_signed, yr, dates, "equal", shrinkage, embargo_days=H)

    # Build the engine-ready composite_score block (one factor per bucket so the
    # solved per-factor weights are honored exactly).
    buckets = {}
    for f, wt, sg, raw_ic in zip(factors, w, signs, ic):
        buckets[f] = {"weight": round(float(wt), 4),
                      "factors": [{"name": f, "sign": "+" if sg > 0 else "-"}]}

    return {
        "composite_score": {
            "type": "composite_score",
            "standardization": "rank",
            "buckets": buckets,
        },
        "diagnostics": {
            "horizon": horizon,
            "method": method,
            "shrinkage": shrinkage,
            "n_rows": int(len(yr)),
            "n_dates": int(len(uniq)),
            "per_factor": [
                {"factor": f, "rank_ic": round(float(raw), 4),
                 "sign": "+" if sg > 0 else "-", "weight": round(float(wt), 4)}
                for f, raw, sg, wt in zip(factors, ic, signs, w)
            ],
            "combined_ic_in_sample": round(is_ic, 4),
            "combined_ic_oos": None if oos_ic is None else round(oos_ic, 4),
            "combined_ic_oos_folds": oos_folds,
            "equal_weight_ic_oos": None if eq_oos is None else round(eq_oos, 4),
            "verdict": _verdict(oos_ic, eq_oos, is_ic),
        },
    }


def _verdict(oos, eq, is_ic):
    if oos is None:
        return "insufficient data for OOS estimate"
    parts = []
    if oos <= 0:
        parts.append("OOS IC <= 0: combination does not generalize — do not use as-is")
    elif eq is not None and oos < eq:
        parts.append("solved weights underperform equal-weight OOS — prefer equal/ic_weighted")
    else:
        parts.append("solved weights generalize and beat equal-weight OOS")
    if is_ic and oos is not None and is_ic > 2 * max(oos, 1e-6):
        parts.append("large in-sample/OOS gap — overfit risk, lean on more shrinkage")
    return "; ".join(parts)
