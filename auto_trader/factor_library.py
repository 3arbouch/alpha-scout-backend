"""
Factor Library Analyzer
=======================
Phase-1 quant research bench. One tool: `analyze_factor_library` builds factor
cards for all (or a subset of) registered features over (universe, window):

  - IC per horizon (Spearman cross-sectional rank correlation), monthly walk
  - Newey-West HAC t-statistic adjusted for overlapping forward returns
  - Sector + ln(market_cap) neutralized IC (residualized factor)
  - Quintile spread (Q1..Q5 mean ann return, monotonicity, Q5-Q1 Sharpe)
  - Top-decile turnover

Cached: SQLite-backed payload by hash(universe, start, end, settings).
Re-uses signal_ranker._resolve_ic_universe and backtest_engine._load_feature_series.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import sys
import time
from bisect import bisect_right
from pathlib import Path
from typing import Iterable

import numpy as np

# Wire scripts/ into path so backtest_engine.{_load_feature_series, etc.} is importable.
SCRIPT_DIR = Path(__file__).parent.parent / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from backtest_engine import _load_feature_series  # noqa: E402

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from server.factors import feature_names as _all_feature_names  # noqa: E402


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_HORIZONS_TD = (21, 63, 126, 252)
DEFAULT_BUCKETS = 5
SHARES_LAG_DAYS = 90  # conservative point-in-time market-cap availability lag
SCHEMA_VERSION = "v2"  # bumped: payload now includes orthogonality block
DEFAULT_ORTHO_HORIZON_TD = 63   # which horizon's Q5-Q1 spread powers the return-corr matrix
DEFAULT_N_CLUSTERS = 10
TOP_NEIGHBORS_K = 5


# Economic categorization — static mapping by factor construction. Used for
# diversification rules and as the prior compared against the data-derived
# clusters. NEVER changes with universe / window — it's a property of the
# formula, not the data.
FACTOR_CATEGORY: dict[str, str] = {
    # Momentum / reversal / technical
    "ret_1m":        "reversal",
    "ret_3m":        "momentum",
    "ret_6m":        "momentum",
    "ret_12m":       "momentum",
    "ret_12_1m":     "momentum",
    "rsi_14":        "technical",
    # Value (price-based ratios)
    "pe":            "value",
    "ps":            "value",
    "p_b":           "value",
    "ev_ebitda":     "value",
    "ev_sales":      "value",
    # Yield
    "fcf_yield":     "yield",
    "div_yield":     "yield",
    # Growth
    "eps_yoy":       "growth",
    "rev_yoy":       "growth",
    "eps_yoy_accel": "growth",
    "rev_yoy_accel": "growth",
    # Quality
    "gross_margin":         "quality",
    "op_margin":            "quality",
    "net_margin":           "quality",
    "op_margin_yoy_delta":  "quality",
    "net_margin_yoy_delta": "quality",
    "roe":                  "quality",
    "roic":                 "quality",
    # Leverage
    "debt_to_equity": "leverage",
    # Risk / drawdown / liquidity
    "drawdown_60d":    "risk",
    "drawdown_252d":   "risk",
    "drawdown_alltime":"risk",
    "vol_z_20":        "risk",
    "dollar_vol_20":   "liquidity",
    # Sentiment
    "analyst_net_upgrades_30d": "sentiment",
    "analyst_net_upgrades_90d": "sentiment",
    # Calendar (earnings)
    "days_since_last_earnings": "calendar",
    "days_to_next_earnings":    "calendar",
    "pre_earnings_window_5d":   "calendar",
}


# ---------------------------------------------------------------------------
# Universe + panel loading
# ---------------------------------------------------------------------------

def _resolve_universe(conn: sqlite3.Connection, sector, universe) -> list[str]:
    if universe:
        return sorted(set(universe))
    if sector:
        rows = conn.execute(
            "SELECT symbol FROM universe_profiles WHERE sector = ?", (sector,)
        ).fetchall()
        return sorted({r[0] for r in rows})
    rows = conn.execute("SELECT symbol FROM universe_profiles").fetchall()
    return sorted({r[0] for r in rows})


def _load_price_index(conn, symbols: list[str], start: str, end: str) -> dict[str, dict[str, float]]:
    """{sym: {date: close}} for the requested window."""
    placeholders = ",".join("?" * len(symbols))
    rows = conn.execute(
        f"SELECT symbol, date, close FROM prices "
        f"WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ? AND close IS NOT NULL "
        f"ORDER BY symbol, date",
        (*symbols, start, end),
    ).fetchall()
    out: dict[str, dict[str, float]] = {}
    for s, d, c in rows:
        out.setdefault(s, {})[d] = float(c)
    return out


def _trading_dates(price_index: dict, start: str, end: str) -> list[str]:
    s: set = set()
    for d_map in price_index.values():
        for d in d_map:
            if start <= d <= end:
                s.add(d)
    return sorted(s)


def _monthly_rebalance_dates(trading_dates: list[str]) -> list[str]:
    """First trading date of each (year, month) in `trading_dates`."""
    if not trading_dates:
        return []
    seen = set()
    out = []
    for d in trading_dates:
        key = d[:7]  # YYYY-MM
        if key not in seen:
            seen.add(key)
            out.append(d)
    return out


def _load_shares_panel(conn, symbols: list[str]) -> dict[str, list[tuple[str, float]]]:
    """{sym: [(available_date, shares_diluted), ...]} sorted by available_date.
    available_date = period_end (`income.date`) + SHARES_LAG_DAYS days.
    """
    from datetime import datetime, timedelta
    placeholders = ",".join("?" * len(symbols))
    rows = conn.execute(
        f"SELECT symbol, date, shares_diluted FROM income "
        f"WHERE symbol IN ({placeholders}) AND shares_diluted IS NOT NULL "
        f"ORDER BY symbol, date",
        symbols,
    ).fetchall()
    out: dict[str, list] = {}
    for s, d, sh in rows:
        avail = (datetime.strptime(d, "%Y-%m-%d") + timedelta(days=SHARES_LAG_DAYS)).strftime("%Y-%m-%d")
        out.setdefault(s, []).append((avail, float(sh)))
    for s in out:
        out[s].sort(key=lambda r: r[0])
    return out


def _load_sector_map(conn, symbols: list[str]) -> dict[str, str]:
    placeholders = ",".join("?" * len(symbols))
    rows = conn.execute(
        f"SELECT symbol, sector FROM universe_profiles WHERE symbol IN ({placeholders})",
        symbols,
    ).fetchall()
    return {s: (sec or "Unknown") for s, sec in rows}


# ---------------------------------------------------------------------------
# As-of bisect (feature panel)
# ---------------------------------------------------------------------------

def _build_feature_matrix(
    feature: str,
    rebal_dates: list[str],
    symbols: list[str],
    conn,
    price_index: dict,
) -> np.ndarray:
    """F[t, s] = most-recent feature value at-or-before rebal_dates[t]. NaN if none.

    Uses _load_feature_series (registry-aware) so on-the-fly features work via
    `compute_series`. Window is widened 1y before by _load_feature_series
    itself; we widen `end` by max horizon at the call site.
    """
    series = _load_feature_series(
        feature, symbols, rebal_dates[0], rebal_dates[-1], conn,
        price_index=price_index,
    )
    T, N = len(rebal_dates), len(symbols)
    F = np.full((T, N), np.nan, dtype=np.float64)
    rebal_arr = np.array(rebal_dates)
    for j, sym in enumerate(symbols):
        pts = series.get(sym)
        if not pts:
            continue
        # pts is [(date, value)] ascending. Bisect right then -1 for as-of.
        dates = np.array([p[0] for p in pts])
        vals = np.array([p[1] for p in pts], dtype=np.float64)
        idx = np.searchsorted(dates, rebal_arr, side="right") - 1
        valid = idx >= 0
        F[valid, j] = vals[idx[valid]]
    return F


def _build_forward_return_matrix(
    rebal_dates: list[str],
    horizons_td: tuple[int, ...],
    all_trading_dates: list[str],
    symbols: list[str],
    price_index: dict,
) -> dict[int, np.ndarray]:
    """R[h][t, s] = simple return from close(rebal_dates[t]) to close(rebal_dates[t]+h td).
    NaN when the forward date doesn't exist in trading_dates or close missing.
    """
    date_to_idx = {d: i for i, d in enumerate(all_trading_dates)}
    rebal_idx = np.array([date_to_idx.get(d, -1) for d in rebal_dates])
    valid_t = rebal_idx >= 0

    T = len(rebal_dates)
    N = len(symbols)
    out: dict[int, np.ndarray] = {}
    for h in horizons_td:
        R = np.full((T, N), np.nan, dtype=np.float64)
        fwd_idx = rebal_idx + h
        # mark valid rows
        valid_rows = valid_t & (fwd_idx < len(all_trading_dates))
        rebal_d = [rebal_dates[t] if valid_rows[t] else None for t in range(T)]
        fwd_d = [all_trading_dates[fwd_idx[t]] if valid_rows[t] else None for t in range(T)]
        for j, sym in enumerate(symbols):
            sym_p = price_index.get(sym)
            if not sym_p:
                continue
            for t in range(T):
                if not valid_rows[t]:
                    continue
                p0 = sym_p.get(rebal_d[t])
                p1 = sym_p.get(fwd_d[t])
                if p0 is None or p1 is None or p0 <= 0 or p1 <= 0:
                    continue
                R[t, j] = (p1 - p0) / p0
        out[h] = R
    return out


# ---------------------------------------------------------------------------
# Per-date Spearman IC (vectorized via Pearson-on-ranks of jointly-valid cells)
# ---------------------------------------------------------------------------

def _spearman_per_date(F: np.ndarray, R: np.ndarray, min_n: int = 10) -> tuple[np.ndarray, np.ndarray]:
    """Returns (ic, valid_mask) of length T.
    For each row t, take cells where both F[t,:] and R[t,:] are finite, rank within
    that subset (avg method for ties), compute Pearson correlation of the ranks.
    Skip if n < min_n or either side has zero variance.
    """
    T = F.shape[0]
    ic = np.full(T, np.nan)
    for t in range(T):
        fr = F[t, :]
        rr = R[t, :]
        m = np.isfinite(fr) & np.isfinite(rr)
        n = int(m.sum())
        if n < min_n:
            continue
        f_v = fr[m]
        r_v = rr[m]
        # Rank with average ties (matches scipy.stats.spearmanr / rankdata).
        f_rank = _rankdata_avg(f_v)
        r_rank = _rankdata_avg(r_v)
        # Pearson on ranks.
        fm = f_rank - f_rank.mean()
        rm = r_rank - r_rank.mean()
        denom = math.sqrt((fm * fm).sum() * (rm * rm).sum())
        if denom == 0.0:
            continue
        ic[t] = float((fm * rm).sum() / denom)
    return ic, np.isfinite(ic)


def _rankdata_avg(a: np.ndarray) -> np.ndarray:
    """Average-rank for ties; identical to scipy.stats.rankdata(a, 'average')."""
    n = len(a)
    order = np.argsort(a, kind="mergesort")
    sorted_a = a[order]
    ranks = np.empty(n)
    i = 0
    while i < n:
        j = i + 1
        while j < n and sorted_a[j] == sorted_a[i]:
            j += 1
        avg = (i + j - 1) / 2.0 + 1.0  # 1-based ranks
        ranks[order[i:j]] = avg
        i = j
    return ranks


# ---------------------------------------------------------------------------
# Newey-West HAC t-stat for the mean IC with overlapping returns
# ---------------------------------------------------------------------------

def _newey_west_tstat(ic: np.ndarray, lags: int) -> float:
    """t-stat for H0: mean(ic) = 0 with HAC variance (Bartlett kernel, `lags` lags).
    `lags` should be (horizon_td / walk_step_td) - 1, i.e., the number of
    overlap-induced autocorrelations to absorb. Returns NaN if not enough obs.
    """
    x = ic[np.isfinite(ic)]
    n = len(x)
    if n < max(5, lags + 2):
        return float("nan")
    mu = x.mean()
    dev = x - mu
    # gamma_0 (with ddof=0 to match the HAC variance derivation)
    g0 = float((dev * dev).mean())
    var = g0
    for k in range(1, lags + 1):
        w = 1.0 - k / (lags + 1)
        gk = float((dev[k:] * dev[:-k]).mean())
        var += 2.0 * w * gk
    var = max(var, 1e-18)
    se = math.sqrt(var / n)
    if se == 0.0:
        return float("nan")
    return mu / se


# ---------------------------------------------------------------------------
# Sector + ln(market_cap) neutralization (cross-sectional regression residuals)
# ---------------------------------------------------------------------------

def _neutralize_factor_panel(
    F: np.ndarray,
    rebal_dates: list[str],
    symbols: list[str],
    price_index: dict,
    shares_panel: dict[str, list[tuple[str, float]]],
    sector_map: dict[str, str],
) -> np.ndarray:
    """For each row t, residualize F[t, :] against sector dummies + ln(mcap[t, :]).
    Returns F_neutral of same shape.

    Cells with NaN F or missing mcap are kept NaN in the output. Each row's
    regression uses only valid cells; constant column for intercept is included
    via the sector dummies (one column per sector, no separate intercept).
    """
    T, N = F.shape
    F_neut = np.full_like(F, np.nan)

    # Compute mcap[t, j] = close[t] * shares_as_of(t).
    shares_at = np.full((T, N), np.nan)
    for j, sym in enumerate(symbols):
        sp = shares_panel.get(sym)
        if not sp:
            continue
        dates = np.array([p[0] for p in sp])
        vals = np.array([p[1] for p in sp], dtype=np.float64)
        idx = np.searchsorted(dates, np.array(rebal_dates), side="right") - 1
        valid = idx >= 0
        shares_at[valid, j] = vals[idx[valid]]
    closes = np.full((T, N), np.nan)
    for j, sym in enumerate(symbols):
        sp = price_index.get(sym)
        if not sp:
            continue
        for t, d in enumerate(rebal_dates):
            v = sp.get(d)
            if v is not None:
                closes[t, j] = v
    mcap = closes * shares_at
    ln_mcap = np.where(mcap > 0, np.log(mcap), np.nan)

    # Sector design (one column per sector).
    sectors_unique = sorted({s for s in sector_map.values()})
    sec_idx = {s: i for i, s in enumerate(sectors_unique)}
    K = len(sectors_unique)
    S = np.zeros((N, K), dtype=np.float64)
    for j, sym in enumerate(symbols):
        S[j, sec_idx[sector_map.get(sym, "Unknown")]] = 1.0

    for t in range(T):
        f = F[t, :]
        lm = ln_mcap[t, :]
        valid = np.isfinite(f) & np.isfinite(lm)
        n = int(valid.sum())
        if n < (K + 2):
            continue
        # Design: sector dummies + ln_mcap (K+1 columns). No separate intercept;
        # the dummies absorb it. Drop empty sector columns to avoid rank issues.
        X = np.hstack([S[valid, :], lm[valid].reshape(-1, 1)])
        col_active = X.sum(axis=0) != 0
        X = X[:, col_active]
        y = f[valid]
        # Least-squares; residual = y - X @ beta_hat.
        try:
            beta, *_ = np.linalg.lstsq(X, y, rcond=None)
            resid = y - X @ beta
        except np.linalg.LinAlgError:
            continue
        F_neut[t, valid] = resid
    return F_neut


# ---------------------------------------------------------------------------
# Quintile spread + top-decile turnover
# ---------------------------------------------------------------------------

def _bucket_returns(F: np.ndarray, R: np.ndarray, buckets: int) -> np.ndarray:
    """B[t, b] = equal-weighted mean of R[t, :] over the cells whose F[t, :]
    falls in bucket b (1-indexed quintile by value, ascending → b=0 lowest).
    NaN where the bucket is empty or row has insufficient names.
    """
    T = F.shape[0]
    B = np.full((T, buckets), np.nan)
    for t in range(T):
        f = F[t, :]
        r = R[t, :]
        m = np.isfinite(f) & np.isfinite(r)
        n = int(m.sum())
        if n < buckets * 2:  # need at least 2 per bucket
            continue
        f_v = f[m]
        r_v = r[m]
        order = np.argsort(f_v, kind="mergesort")
        # split into `buckets` near-equal groups
        edges = np.linspace(0, n, buckets + 1).astype(int)
        for b in range(buckets):
            i0, i1 = edges[b], edges[b + 1]
            if i1 > i0:
                idx = order[i0:i1]
                B[t, b] = float(r_v[idx].mean())
    return B


def _spread_series(B: np.ndarray) -> np.ndarray:
    """Q_top − Q_bottom time series. NaN where any of the relevant buckets are NaN."""
    if B.size == 0:
        return np.array([])
    return B[:, -1] - B[:, 0]


# ---------------------------------------------------------------------------
# Phase 2 — orthogonality / factor zoo
# ---------------------------------------------------------------------------

def _xsec_rank_corr_matrix(F_by_feature: dict[str, np.ndarray]) -> tuple[list[str], np.ndarray]:
    """For each pair (f_i, f_j): at each rebalance date t, take cells where both
    F_i[t, :] and F_j[t, :] are finite, compute Spearman rank corr, then average
    per-date correlations across t.

    Returns (ordered_features, matrix [n_feat x n_feat] symmetric, diag=1).
    """
    features = sorted(F_by_feature.keys())
    n = len(features)
    M = np.zeros((n, n), dtype=np.float64)
    for i in range(n):
        M[i, i] = 1.0
    for i in range(n):
        Fi = F_by_feature[features[i]]
        T = Fi.shape[0]
        for j in range(i + 1, n):
            Fj = F_by_feature[features[j]]
            vals = []
            for t in range(T):
                a = Fi[t, :]
                b = Fj[t, :]
                m = np.isfinite(a) & np.isfinite(b)
                if int(m.sum()) < 10:
                    continue
                ar = _rankdata_avg(a[m])
                br = _rankdata_avg(b[m])
                am = ar - ar.mean()
                bm = br - br.mean()
                denom = math.sqrt((am * am).sum() * (bm * bm).sum())
                if denom > 0:
                    vals.append(float((am * bm).sum() / denom))
            v = float(np.mean(vals)) if vals else 0.0
            M[i, j] = v
            M[j, i] = v
    return features, M


def _factor_return_corr_matrix(
    spread_by_feature: dict[str, np.ndarray], min_overlap: int = 10
) -> tuple[list[str], np.ndarray]:
    """Pearson correlation of the Q_top − Q_bottom spread time series across
    features (jointly-valid time-overlap). Symmetric, diag=1, missing→0.
    """
    features = sorted(spread_by_feature.keys())
    n = len(features)
    M = np.zeros((n, n), dtype=np.float64)
    for i in range(n):
        M[i, i] = 1.0
    for i in range(n):
        a_full = spread_by_feature[features[i]]
        for j in range(i + 1, n):
            b_full = spread_by_feature[features[j]]
            L = min(len(a_full), len(b_full))
            if L < min_overlap:
                continue
            a = a_full[:L]
            b = b_full[:L]
            m = np.isfinite(a) & np.isfinite(b)
            if int(m.sum()) < min_overlap:
                continue
            av = a[m]; bv = b[m]
            am = av - av.mean()
            bm = bv - bv.mean()
            denom = math.sqrt((am * am).sum() * (bm * bm).sum())
            if denom > 0:
                v = float((am * bm).sum() / denom)
                M[i, j] = v
                M[j, i] = v
    return features, M


def _hierarchical_clusters(
    features: list[str], xsec_M: np.ndarray, k: int
) -> dict[str, int]:
    """Agglomerative clustering with average linkage on distance = 1 − |corr|.
    Returns {feature: cluster_id}. cluster_ids are 1-indexed.
    """
    from scipy.cluster.hierarchy import linkage, fcluster
    n = len(features)
    # condensed distance vector (upper triangle, no diagonal)
    D = 1.0 - np.abs(xsec_M)
    np.fill_diagonal(D, 0.0)
    D = np.clip(D, 0.0, None)
    cond = []
    for i in range(n):
        for j in range(i + 1, n):
            cond.append(float(D[i, j]))
    cond = np.array(cond)
    if len(cond) == 0:
        return {f: 1 for f in features}
    Z = linkage(cond, method="average")
    k_eff = max(1, min(int(k), n))
    labels = fcluster(Z, t=k_eff, criterion="maxclust")
    return {features[i]: int(labels[i]) for i in range(n)}


def _top_neighbors_per_feature(
    features: list[str], xsec_M: np.ndarray, k: int
) -> dict[str, list[list]]:
    """For each feature, top-K most |corr|-related neighbors (signed corr returned)."""
    n = len(features)
    out: dict[str, list[list]] = {}
    for i in range(n):
        row = xsec_M[i, :].copy()
        row[i] = 0.0  # exclude self
        # sort by |corr| desc
        order = np.argsort(-np.abs(row))
        kk = min(k, n - 1)
        out[features[i]] = [
            [features[j], round(float(xsec_M[i, j]), 4)] for j in order[:kk]
        ]
    return out


def _category_agreement(
    cluster_assignments: dict[str, int], categories: dict[str, str]
) -> dict[str, dict]:
    """For each cluster: dominant economic category, fraction of members agreeing."""
    from collections import Counter, defaultdict
    by_cluster: dict[int, list[str]] = defaultdict(list)
    for feat, cid in cluster_assignments.items():
        by_cluster[cid].append(feat)
    out: dict[str, dict] = {}
    for cid, members in by_cluster.items():
        cats = [categories.get(m, "unknown") for m in members]
        c = Counter(cats)
        dom_cat, dom_n = c.most_common(1)[0]
        out[str(cid)] = {
            "members": sorted(members),
            "dominant_category": dom_cat,
            "agreement": round(dom_n / len(members), 4),
            "size": len(members),
        }
    return out


def _orthogonality_block(
    F_by_feature: dict[str, np.ndarray],
    spread_by_feature_primary: dict[str, np.ndarray],
    primary_horizon_td: int,
    k_clusters: int,
) -> dict:
    feats, xsec_M = _xsec_rank_corr_matrix(F_by_feature)
    feats_r, ret_M = _factor_return_corr_matrix(spread_by_feature_primary)
    # `feats_r` should equal `feats`; assert silently and align.
    if feats_r != feats:
        # Reorder ret_M to match xsec ordering for clean payload.
        idx = [feats_r.index(f) for f in feats]
        ret_M = ret_M[np.ix_(idx, idx)]
    cluster_assign = _hierarchical_clusters(feats, xsec_M, k_clusters)
    cluster_info = _category_agreement(cluster_assign, FACTOR_CATEGORY)
    neighbors = _top_neighbors_per_feature(feats, xsec_M, TOP_NEIGHBORS_K)
    return {
        "features": feats,
        "economic_categories": {f: FACTOR_CATEGORY.get(f, "unknown") for f in feats},
        "xsec_rank_corr": {
            "description": "Per-date Spearman of (F_i, F_j) cross-sections, averaged across rebalance dates",
            "matrix": [[round(float(x), 4) for x in row] for row in xsec_M],
        },
        f"factor_return_corr_{primary_horizon_td}d": {
            "description": f"Pearson of Q_top-Q_bottom spread time series at {primary_horizon_td}-day horizon",
            "matrix": [[round(float(x), 4) for x in row] for row in ret_M],
        },
        "data_clusters": {
            "method": "scipy.cluster.hierarchy.linkage(average)",
            "distance": "1 - |xsec_rank_corr|",
            "k": k_clusters,
            "assignments": cluster_assign,
            "by_cluster": cluster_info,
        },
        "top_neighbors_per_feature": neighbors,
    }


def _spread_summary(B: np.ndarray, horizon_td: int, walk_step_td: int) -> dict:
    """Summary stats of the (top minus bottom) spread time series.
    Annualization: each obs is a holding-period return over horizon_td. Mean is
    annualized by multiplying by (252 / horizon_td). Sharpe = mean_ann / std_ann,
    where std_ann = std_per_period * sqrt(252 / horizon_td).
    Monotonicity: Spearman corr of bucket index vs mean bucket return.
    """
    if B.size == 0:
        return _empty_spread()
    valid_rows = np.isfinite(B).all(axis=1)
    if valid_rows.sum() < 5:
        return _empty_spread()
    B_v = B[valid_rows]
    spread = B_v[:, -1] - B_v[:, 0]
    mean_p = float(spread.mean())
    std_p = float(spread.std(ddof=1)) if len(spread) > 1 else 0.0
    ann_factor = 252.0 / horizon_td
    mean_ann_pct = mean_p * ann_factor * 100.0
    sharpe = (mean_p * ann_factor) / (std_p * math.sqrt(ann_factor)) if std_p > 0 else 0.0
    bucket_means = B_v.mean(axis=0)
    # Monotonicity = Spearman of (bucket_index, bucket_mean)
    ranks_x = np.arange(len(bucket_means), dtype=np.float64) + 1.0
    ranks_y = _rankdata_avg(bucket_means)
    xm = ranks_x - ranks_x.mean()
    ym = ranks_y - ranks_y.mean()
    denom = math.sqrt((xm * xm).sum() * (ym * ym).sum())
    monot = float((xm * ym).sum() / denom) if denom > 0 else 0.0
    return {
        "n_periods": int(len(spread)),
        "bucket_mean_ann_pct": [round(float(x) * ann_factor * 100.0, 4) for x in bucket_means],
        "spread_ann_pct": round(mean_ann_pct, 4),
        "spread_sharpe": round(sharpe, 4),
        "monotonicity": round(monot, 4),
    }


def _empty_spread() -> dict:
    return {
        "n_periods": 0, "bucket_mean_ann_pct": [], "spread_ann_pct": None,
        "spread_sharpe": None, "monotonicity": None,
    }


def _top_bucket_turnover(F: np.ndarray, symbols: list[str], buckets: int) -> float | None:
    """Average per-period turnover of the TOP bucket (by F): |A Δ B| / (2 * |top|)."""
    T = F.shape[0]
    top_sets: list[set] = []
    for t in range(T):
        f = F[t, :]
        m = np.isfinite(f)
        n = int(m.sum())
        if n < buckets * 2:
            top_sets.append(set())
            continue
        f_v = f[m]
        idx_in_full = np.where(m)[0]
        order = np.argsort(f_v, kind="mergesort")
        edges = np.linspace(0, n, buckets + 1).astype(int)
        top = idx_in_full[order[edges[buckets - 1]: edges[buckets]]]
        top_sets.append({symbols[j] for j in top})
    deltas = []
    for t in range(1, T):
        a, b = top_sets[t - 1], top_sets[t]
        if not a or not b:
            continue
        sym_d = (a ^ b)
        deltas.append(len(sym_d) / (2.0 * max(len(b), 1)))
    if not deltas:
        return None
    return round(float(np.mean(deltas)), 4)


# ---------------------------------------------------------------------------
# Per-feature aggregator
# ---------------------------------------------------------------------------

def _feature_card(
    feature: str,
    rebal_dates: list[str],
    symbols: list[str],
    conn,
    price_index: dict,
    R_by_h: dict[int, np.ndarray],
    sector_map: dict[str, str],
    shares_panel: dict,
    horizons_td: tuple[int, ...],
    buckets: int,
    neutralize: bool,
    walk_step_td: int,
) -> dict:
    F = _build_feature_matrix(feature, rebal_dates, symbols, conn, price_index)
    F_neut = (
        _neutralize_factor_panel(F, rebal_dates, symbols, price_index, shares_panel, sector_map)
        if neutralize else None
    )

    ic_raw = {}
    ic_neut = {}
    spread_per_h = {}
    for h in horizons_td:
        R = R_by_h[h]
        ic_series, _ = _spearman_per_date(F, R)
        n_obs = int(np.isfinite(ic_series).sum())
        if n_obs >= 5:
            mu = float(np.nanmean(ic_series))
            sd = float(np.nanstd(ic_series, ddof=1)) if n_obs > 1 else 0.0
            ir = mu / sd if sd > 0 else 0.0
            lags = max(0, int(math.ceil(h / walk_step_td)) - 1)
            t_nw = _newey_west_tstat(ic_series, lags)
            hit = float(np.nanmean(ic_series > 0))
        else:
            mu = sd = ir = t_nw = hit = float("nan")
        ic_raw[h] = {
            "n_obs": n_obs,
            "mean": round(mu, 6) if np.isfinite(mu) else None,
            "std": round(sd, 6) if np.isfinite(sd) else None,
            "ir": round(ir, 6) if np.isfinite(ir) else None,
            "t_nw": round(t_nw, 6) if np.isfinite(t_nw) else None,
            "hit_rate": round(hit, 4) if np.isfinite(hit) else None,
            "lags": lags if n_obs >= 5 else None,
        }
        if F_neut is not None:
            ic_series_n, _ = _spearman_per_date(F_neut, R)
            n_obs_n = int(np.isfinite(ic_series_n).sum())
            if n_obs_n >= 5:
                mun = float(np.nanmean(ic_series_n))
                sdn = float(np.nanstd(ic_series_n, ddof=1)) if n_obs_n > 1 else 0.0
                irn = mun / sdn if sdn > 0 else 0.0
                lags_n = max(0, int(math.ceil(h / walk_step_td)) - 1)
                tn = _newey_west_tstat(ic_series_n, lags_n)
                hitn = float(np.nanmean(ic_series_n > 0))
            else:
                mun = sdn = irn = tn = hitn = float("nan")
            ic_neut[h] = {
                "n_obs": n_obs_n,
                "mean": round(mun, 6) if np.isfinite(mun) else None,
                "std": round(sdn, 6) if np.isfinite(sdn) else None,
                "ir": round(irn, 6) if np.isfinite(irn) else None,
                "t_nw": round(tn, 6) if np.isfinite(tn) else None,
                "hit_rate": round(hitn, 4) if np.isfinite(hitn) else None,
            }
        # Quintile spread for this horizon
        B = _bucket_returns(F, R, buckets)
        spread_per_h[h] = _spread_summary(B, h, walk_step_td)

    turnover = _top_bucket_turnover(F, symbols, buckets)
    # Spread time series at the primary ortho horizon (for factor-return corr).
    primary_h = DEFAULT_ORTHO_HORIZON_TD if DEFAULT_ORTHO_HORIZON_TD in R_by_h else horizons_td[0]
    spread_primary_series = _spread_series(_bucket_returns(F, R_by_h[primary_h], buckets))

    # Normalize horizon keys to strings so JSON-cached + freshly-computed
    # payloads are byte-equal.
    card = {
        "feature": feature,
        "category": FACTOR_CATEGORY.get(feature, "unknown"),
        "ic_raw_by_h": {str(h): v for h, v in ic_raw.items()},
        "ic_neutral_by_h": (
            {str(h): v for h, v in ic_neut.items()} if neutralize else None
        ),
        "spread_by_h": {str(h): v for h, v in spread_per_h.items()},
        "turnover_top_bucket": turnover,
    }
    # Internal-only fields, stripped before serialization.
    card["_F"] = F
    card["_spread_primary_series"] = spread_primary_series
    return card


# ---------------------------------------------------------------------------
# SQLite cache
# ---------------------------------------------------------------------------

def _cache_db_path(app_db_path: str | Path) -> Path:
    return Path(app_db_path)


def _ensure_cache_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS factor_card_cache (
          cache_key      TEXT PRIMARY KEY,
          universe_label TEXT,
          start_date     TEXT,
          end_date       TEXT,
          schema_version TEXT,
          payload        BLOB,
          computed_at    TEXT
        )
        """
    )
    conn.commit()


def _cache_key(args: dict) -> str:
    canon = json.dumps(args, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def analyze_factor_library(
    sector: str | None = None,
    universe: list[str] | None = None,
    start: str = "2015-01-01",
    end: str = "2025-01-01",
    features: list[str] | None = None,
    horizons_td: Iterable[int] = DEFAULT_HORIZONS_TD,
    buckets: int = DEFAULT_BUCKETS,
    neutralize: bool = True,
    walk: str = "monthly",
    use_cache: bool = True,
    market_db_path: str | None = None,
    app_db_path: str | None = None,
) -> dict:
    """Build factor cards for every (or specified) feature over (universe, [start,end]).

    Returns:
      {
        "metadata": {...},
        "factors": [ <feature_card>, ... ]
      }
    """
    if walk != "monthly":
        raise ValueError("walk must be 'monthly' for v1")
    horizons_td = tuple(int(x) for x in horizons_td)
    walk_step_td = 21  # ≈ monthly

    import os
    market_db_path = (
        market_db_path
        or os.environ.get("MARKET_DB_PATH")
        or "/home/mohamed/alpha-scout-backend-dev/data/market_dev.db"
    )
    app_db_path = (
        app_db_path
        or os.environ.get("APP_DB_PATH")
        or "/home/mohamed/alpha-scout-backend/data/app_dev.db"
    )

    market_conn = sqlite3.connect(market_db_path)
    syms = _resolve_universe(market_conn, sector, universe)
    if len(syms) < 10:
        market_conn.close()
        return {"error": "universe_too_small", "n_symbols": len(syms)}

    if features is None:
        features = sorted(_all_feature_names())
    features = sorted(set(features))

    args_for_key = {
        "sector": sector, "universe": syms if universe else None,
        "start": start, "end": end, "features": features,
        "horizons_td": list(horizons_td), "buckets": buckets,
        "neutralize": neutralize, "walk": walk,
        "schema_version": SCHEMA_VERSION,
    }
    cache_key = _cache_key(args_for_key)

    if use_cache:
        app_conn = sqlite3.connect(app_db_path)
        _ensure_cache_table(app_conn)
        row = app_conn.execute(
            "SELECT payload FROM factor_card_cache WHERE cache_key = ?", (cache_key,)
        ).fetchone()
        if row is not None:
            payload = json.loads(row[0])
            payload["metadata"]["cached"] = True
            app_conn.close()
            market_conn.close()
            return payload
        app_conn.close()

    t_start = time.time()
    # Extend `end` by max horizon to load enough forward prices.
    from datetime import datetime, timedelta
    end_ext = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=int(max(horizons_td)) * 2)).strftime("%Y-%m-%d")
    # Extend `start` 1y back so as-of bisects find fundamentals from prior year.
    start_ext = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=400)).strftime("%Y-%m-%d")

    price_index = _load_price_index(market_conn, syms, start_ext, end_ext)
    all_trading = _trading_dates(price_index, start_ext, end_ext)
    in_window_trading = [d for d in all_trading if start <= d <= end]
    rebal_dates = _monthly_rebalance_dates(in_window_trading)
    if len(rebal_dates) < 12:
        market_conn.close()
        return {"error": "not_enough_rebalance_dates", "n_rebal": len(rebal_dates)}

    R_by_h = _build_forward_return_matrix(rebal_dates, horizons_td, all_trading, syms, price_index)
    sector_map = _load_sector_map(market_conn, syms)
    shares_panel = _load_shares_panel(market_conn, syms) if neutralize else {}

    cards = []
    F_by_feature: dict[str, np.ndarray] = {}
    spread_by_feature: dict[str, np.ndarray] = {}
    for f in features:
        try:
            card = _feature_card(
                f, rebal_dates, syms, market_conn, price_index, R_by_h,
                sector_map, shares_panel,
                horizons_td, buckets, neutralize, walk_step_td,
            )
            # Pull internal panels then strip
            F_by_feature[f] = card.pop("_F")
            spread_by_feature[f] = card.pop("_spread_primary_series")
            cards.append(card)
        except Exception as e:
            cards.append({"feature": f, "error": str(e)})

    # Phase 2 — orthogonality / zoo
    ortho = None
    if len(F_by_feature) >= 2:
        primary_h = DEFAULT_ORTHO_HORIZON_TD if DEFAULT_ORTHO_HORIZON_TD in R_by_h else horizons_td[0]
        ortho = _orthogonality_block(
            F_by_feature, spread_by_feature,
            primary_horizon_td=primary_h,
            k_clusters=DEFAULT_N_CLUSTERS,
        )

    compute_seconds = round(time.time() - t_start, 2)
    payload = {
        "metadata": {
            "universe_label": sector or (f"custom:{len(syms)}_syms" if universe else "all"),
            "n_symbols": len(syms),
            "window": {"start": start, "end": end},
            "n_rebalances": len(rebal_dates),
            "walk": walk,
            "walk_step_td": walk_step_td,
            "horizons_td": list(horizons_td),
            "buckets": buckets,
            "neutralize_against": (["sector_dummy", "ln_market_cap"] if neutralize else []),
            "ortho_primary_horizon_td": DEFAULT_ORTHO_HORIZON_TD,
            "n_clusters": DEFAULT_N_CLUSTERS,
            "schema_version": SCHEMA_VERSION,
            "compute_seconds": compute_seconds,
            "cached": False,
        },
        "factors": cards,
        "orthogonality": ortho,
    }

    if use_cache:
        app_conn = sqlite3.connect(app_db_path)
        _ensure_cache_table(app_conn)
        app_conn.execute(
            "INSERT OR REPLACE INTO factor_card_cache "
            "(cache_key, universe_label, start_date, end_date, schema_version, payload, computed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
            (cache_key, payload["metadata"]["universe_label"], start, end, SCHEMA_VERSION,
             json.dumps(payload)),
        )
        app_conn.commit()
        app_conn.close()

    market_conn.close()
    return payload
