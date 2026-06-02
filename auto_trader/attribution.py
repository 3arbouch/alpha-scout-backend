"""Post-trade factor attribution for completed experiments.

Decomposes a portfolio's alpha (return in excess of benchmark) into
per-factor contributions plus a residual, all in **log-return space**
so quantities are additive in time:

    α_log  =  Σ_f  c_f  +  ε
    c_f    =  z_f  ×  R_f^log  ×  κ_f

where:
  α_log   = log(1 + r_p) − log(1 + r_bench), in pp
            r_p is the portfolio's period return; r_bench is the
            benchmark over the same window — sector ETF when populated,
            else market (SPY).
  z_f     = position-weighted z-score of the portfolio's exposure to
            factor f at the holdings snapshot (universe-wide z, mean 0
            std 1 across the features_daily universe on snapshot date).
  R_f^log = Σ_t log(1 + spread_pp(t)/100) × 100, in pp — cumulative log
            factor return over the window. spread_pp is sign-flipped for
            "lower" factors so R_f > 0 always means "the named bet paid."
  κ_f     = scaling constant. v1 uses κ=1 (no calibration). Residual ε
            absorbs the slop.

Snapshot policy (v1): ONE snapshot at the window midpoint. Strategies
with high turnover will show in the residual. Daily-weighted exposure
is a v2 follow-up.

Outputs are presented BOTH cumulative (over the window) AND annualized
(divided by years), since PMs read annualized.

Intended as a deterministic helper — called by the (future)
compute_attribution MCP tool and the analyst_pass orchestrator.
"""
from __future__ import annotations

import datetime as dt
import math
import sqlite3
from typing import Any

from auto_trader.tools import (
    CANONICAL_FACTOR_COLUMNS,
    FACTOR_META,
    _compute_zscores,
    _market_db,
    _resolve_experiment_holdings,
    _resolve_trading_date,
)
from auto_trader.schema import get_db
from auto_trader.factor_returns import compute_factor_returns_adhoc
from auto_trader.universe import resolve_experiment_universe


KAPPA_DEFAULT: dict[str, float] = {f: 1.0 for f in CANONICAL_FACTOR_COLUMNS}


def _to_log_pp(pp: float | None) -> float | None:
    """pp (arithmetic) → log return in pp. Returns None on None input or
    when the underlying return would invert the firm (return ≤ -100%)."""
    if pp is None:
        return None
    r = float(pp) / 100.0
    if r <= -0.99999:
        return None
    return math.log1p(r) * 100.0


def _pick_snapshot_date(start: str, end: str, market_conn: sqlite3.Connection
                        ) -> str | None:
    """Trading day closest to the window midpoint."""
    d0 = dt.date.fromisoformat(start)
    d1 = dt.date.fromisoformat(end)
    midpoint = d0 + (d1 - d0) / 2
    return _resolve_trading_date(market_conn, midpoint.isoformat())


def _period_factor_log_returns(market_conn: sqlite3.Connection,
                                start: str, end: str,
                                factors: list[str],
                                universe_id: str = "all",
                                ) -> tuple[dict[str, float], dict[str, int]]:
    """Cumulative log factor return per factor over (start, end] in pp,
    looked up against `universe_id` ('all' or a sector name).
    """
    placeholders = ",".join("?" * len(factors))
    rows = market_conn.execute(
        f"""SELECT date, factor, spread_pp
            FROM factor_returns_daily
            WHERE date > ? AND date <= ?
              AND factor IN ({placeholders})
              AND universe_id = ?
            ORDER BY factor, date""",
        (start, end, *factors, universe_id),
    ).fetchall()
    cum_log: dict[str, float] = {}
    n_days: dict[str, int] = {}
    for r in rows:
        f = r["factor"]
        contrib_log = _to_log_pp(r["spread_pp"])
        if contrib_log is None:
            continue
        cum_log[f] = cum_log.get(f, 0.0) + contrib_log
        n_days[f] = n_days.get(f, 0) + 1
    return cum_log, n_days


# Universe resolution is delegated to auto_trader.universe — driven by the
# strategy's declared config, not realized holdings. See that module for the
# decision tree.


def _position_weighted_exposure(weights: dict[str, float],
                                 z_per_symbol: dict[str, dict[str, float | None]],
                                 factors: list[str]
                                 ) -> dict[str, float | None]:
    """Position-weighted z-score per factor. Symbols with NaN z dropped and
    remaining weights renormalized for THAT factor — same logic as
    analyze_portfolio_exposures so the two stay aligned."""
    out: dict[str, float | None] = {}
    for f in factors:
        num = 0.0
        denom = 0.0
        for sym, w in weights.items():
            z = z_per_symbol.get(sym, {}).get(f)
            if z is None:
                continue
            num += w * float(z)
            denom += w
        out[f] = (num / denom) if denom > 0 else None
    return out


def _pick_benchmark(row) -> tuple[float | None, str]:
    """Return (benchmark_return_pp, label).

    Prefer sector benchmark when populated (single-sector or v2 engine
    multi-sector primary ETF); fall back to market benchmark.
    """
    sec = row["sector_benchmark_return_pct"] if "sector_benchmark_return_pct" in row.keys() else None
    mkt = row["market_benchmark_return_pct"] if "market_benchmark_return_pct" in row.keys() else None
    if sec is not None:
        return float(sec), "sector"
    if mkt is not None:
        return float(mkt), "market"
    return None, "none"


def compute_attribution(experiment_id: str,
                        kappa: dict[str, float] | None = None,
                        ) -> dict[str, Any]:
    """Compute alpha-decomposition for one experiment, in log-return space.

    Output is intentionally dual-presented: cumulative (over the window)
    AND annualized — PMs read annualized; cumulative is for auditability.
    """
    kappa = kappa or KAPPA_DEFAULT

    # ---- experiment row ----
    app = get_db()
    row = app.execute(
        """SELECT backtest_start, backtest_end, total_return_pct,
                  market_benchmark_return_pct, sector_benchmark_return_pct,
                  alpha_vs_market_pct, alpha_vs_sector_pct
           FROM experiments WHERE id = ?""", (experiment_id,)
    ).fetchone()
    app.close()
    if not row:
        return {"error": f"experiment {experiment_id} not found"}
    start, end = row["backtest_start"], row["backtest_end"]
    port_return = row["total_return_pct"]
    if start is None or end is None:
        return {"error": "experiment missing backtest_start/backtest_end"}
    if port_return is None:
        return {"error": "experiment has no total_return_pct (not evaluated)"}

    bench_return, bench_label = _pick_benchmark(row)
    if bench_return is None:
        return {"error": "experiment has no benchmark return — cannot compute alpha LHS"}

    # ---- log-space LHS ----
    r_p_log = _to_log_pp(float(port_return))
    r_b_log = _to_log_pp(bench_return)
    if r_p_log is None or r_b_log is None:
        return {"error": "return ≤ -100% — cannot take log"}
    alpha_log = r_p_log - r_b_log

    # ---- snapshot at midpoint, fall back to end ----
    mkt = _market_db()
    snapshot = _pick_snapshot_date(start, end, mkt)
    if not snapshot:
        mkt.close()
        return {"error": f"no trading day in window {start}..{end}"}
    weights, snap_date, err = _resolve_experiment_holdings(experiment_id, snapshot)
    if err or not weights:
        weights, snap_date, err = _resolve_experiment_holdings(experiment_id, end)
        if err or not weights:
            mkt.close()
            return {"error": f"could not reconstruct holdings: {err}"}

    # Pick attribution universe from the strategy's DECLARED config (not
    # realized holdings). Same universe is used for both sides of
    # c_f = z_f × R_f so they're consistent.
    universe_id, universe_symbols, sleeve_kinds = resolve_experiment_universe(
        experiment_id, mkt,
    )

    z_per_symbol, z_stats = _compute_zscores(
        mkt, snap_date, CANONICAL_FACTOR_COLUMNS,
        universe_symbols=universe_symbols,
    )
    if not z_per_symbol:
        mkt.close()
        return {"error": f"no features at snapshot {snap_date}"}

    exposures = _position_weighted_exposure(weights, z_per_symbol, CANONICAL_FACTOR_COLUMNS)

    # Factor-return lookup:
    #   - 'all' / sector → precomputed table (fast)
    #   - 'custom'       → on-the-fly compute against exact symbol set
    if universe_id == "custom":
        factor_returns_log, factor_n_days = compute_factor_returns_adhoc(
            mkt, universe_symbols, start, end,
        )
    else:
        factor_returns_log, factor_n_days = _period_factor_log_returns(
            mkt, start, end, CANONICAL_FACTOR_COLUMNS, universe_id=universe_id,
        )
    mkt.close()

    # ---- annualization scale ----
    d0 = dt.date.fromisoformat(start)
    d1 = dt.date.fromisoformat(end)
    years = max((d1 - d0).days / 365.25, 1e-9)

    factors_block: dict[str, dict[str, Any]] = {}
    sum_c_log = 0.0
    for f in CANONICAL_FACTOR_COLUMNS:
        cat, _direction = FACTOR_META.get(f, ("custom", "higher"))
        z = exposures.get(f)
        rf_log = factor_returns_log.get(f)
        k = kappa.get(f, 1.0)
        c_log: float | None = None
        if z is not None and rf_log is not None:
            c_log = z * rf_log * k
            sum_c_log += c_log
        factors_block[f] = {
            "category": cat,
            "exposure_z": round(z, 4) if z is not None else None,
            "factor_log_return_pp": round(rf_log, 4) if rf_log is not None else None,
            "factor_log_return_ann_pp": round(rf_log / years, 4) if rf_log is not None else None,
            "kappa": k,
            "contribution_log_pp": round(c_log, 4) if c_log is not None else None,
            "contribution_ann_pp": round(c_log / years, 4) if c_log is not None else None,
            "n_days_in_factor_return": factor_n_days.get(f, 0),
        }

    residual_log = alpha_log - sum_c_log
    denom = max(abs(alpha_log), 1e-9)
    fraction_explained = min(abs(sum_c_log) / denom, 1.0)

    return {
        "experiment_id": experiment_id,
        "window": {
            "start": start, "end": end,
            "snapshot_date": snap_date,
            "years": round(years, 3),
            "n_trading_days": max(factor_n_days.values(), default=0),
        },
        "benchmark": {
            "label": bench_label,
            "arithmetic_return_pp": round(float(bench_return), 4),
            "log_return_pp": round(r_b_log, 4),
            "log_return_ann_pp": round(r_b_log / years, 4),
        },
        "portfolio": {
            "arithmetic_return_pp": round(float(port_return), 4),
            "log_return_pp": round(r_p_log, 4),
            "log_return_ann_pp": round(r_p_log / years, 4),
        },
        "alpha": {
            "log_pp": round(alpha_log, 4),
            "log_ann_pp": round(alpha_log / years, 4),
        },
        "n_positions_at_snapshot": len(weights),
        "factors": factors_block,
        "sum_contributions_log_pp": round(sum_c_log, 4),
        "sum_contributions_ann_pp": round(sum_c_log / years, 4),
        "residual_log_pp": round(residual_log, 4),
        "residual_ann_pp": round(residual_log / years, 4),
        "fraction_explained": round(fraction_explained, 4),
        "attribution_universe": universe_id,
        "attribution_universe_size": (len(universe_symbols) if universe_symbols
                                       else None),
        "diagnostics": {
            "kappa_calibrated": False,
            "exposure_snapshot_policy": "window_midpoint",
            "lhs": f"log(1+r_p) - log(1+r_{bench_label})",
            "z_score_universe": universe_id,
            "factor_return_universe": universe_id,
            "factor_return_path": "adhoc" if universe_id == "custom" else "precomputed",
            "sleeve_universe_kinds": sleeve_kinds,
            "factor_coverage": {f: z_stats[f][2] for f in CANONICAL_FACTOR_COLUMNS if f in z_stats},
        },
    }
