"""
Custom tools for the auto-trader agent.

query_market_data — run SQL against market.db with date filtering (no future data leakage).
validate_portfolio — validate a portfolio config against the engine schema.
evaluate_signal — test how a signal performed historically (forward returns).
rank_signals — find the optimal combination of candidate signals via forward selection.

Registered as MCP server tools for the Claude Agent SDK.
"""

import os
import sys
import re
import json
import sqlite3
from pathlib import Path
from typing import Any

# Add scripts to path for engine imports
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from claude_agent_sdk import SdkMcpTool, tool, create_sdk_mcp_server

MCP_SERVER_NAME = "auto_trader"

MARKET_DB_PATH = Path(os.environ.get("MARKET_DB_PATH",
    str(Path(__file__).parent.parent / "data" / "market.db")))

# Filters — set by create_auto_trader_tools() at runtime
_STOP_DATE: str | None = None
_START_DATE: str | None = None
_SECTOR: str | None = None
_RUN_ID: str | None = None

# Canonical factor set for portfolio exposure analysis. Pulled from
# features_daily — the 13 factors most commonly used for risk/style attribution.
# Each entry: (column_name, direction) where direction declares the natural sign
# of the bet: "higher" means a high z-score = more of the named bet; "lower"
# means high z-score = LESS of the named bet (e.g. high pe = LESS value tilt).
# The tool flips the sign for "lower" factors when rolling up to bet_summary so
# +bet always means "more of the category bet."
CANONICAL_FACTORS: dict[str, list[tuple[str, str]]] = {
    "momentum":  [("ret_12_1m", "higher"), ("ret_3m", "higher"), ("ret_1m", "higher")],
    "value":     [("pe", "lower"), ("ev_ebitda", "lower")],
    "yield":     [("fcf_yield", "higher")],
    "growth":    [("rev_yoy", "higher"), ("eps_yoy", "higher"), ("rev_yoy_accel", "higher")],
    "quality":   [("roe", "higher"), ("gross_margin", "higher"), ("debt_to_equity", "lower")],
    "sentiment": [("analyst_net_upgrades_90d", "higher")],
}

# Flat list of factor column names (kept in insertion order for stable output).
CANONICAL_FACTOR_COLUMNS: list[str] = [
    col for factors in CANONICAL_FACTORS.values() for (col, _) in factors
]

# {factor_col: (category, direction)}
FACTOR_META: dict[str, tuple[str, str]] = {
    col: (cat, direction)
    for cat, factors in CANONICAL_FACTORS.items()
    for (col, direction) in factors
}


# Tables that have a date column (for silent filtering)
DATE_COLUMN_MAP = {
    "prices": "date",
    "income": "date",
    "balance": "date",
    "cashflow": "date",
    "earnings": "date",
    "analyst_grades": "date",
    "insider_trades": "transaction_date",
    "macro_indicators": "date",
    "macro_derived": "date",
    "features_daily": "date",
}


def _filter_rows_by_date(rows: list[dict], columns: list[str]) -> list[dict]:
    """Silently remove rows with any date column beyond _STOP_DATE."""
    if not _STOP_DATE or not rows:
        return rows

    # Find which columns look like dates
    date_cols = []
    for col in columns:
        if col in ("date", "transaction_date") or col.endswith("_date"):
            date_cols.append(col)

    if not date_cols:
        return rows

    filtered = []
    for row in rows:
        keep = True
        for col in date_cols:
            val = row.get(col)
            if isinstance(val, str) and len(val) >= 10 and val[:10] > _STOP_DATE:
                keep = False
                break
        if keep:
            filtered.append(row)

    return filtered


def _inject_date_filter(sql: str) -> str:
    """Wrap the query so results are filtered to dates <= _STOP_DATE.

    Uses a CTE approach: wraps the original query, then filters any date
    columns in the outer SELECT. This ensures LIMIT/ORDER BY work correctly
    within the date range.
    """
    if not _STOP_DATE:
        return sql

    # For each table with a known date column, inject a date filter
    # by creating views that are pre-filtered
    return sql


def execute_query(sql: str) -> dict:
    """Execute a read-only SQL query with date filtering."""
    stripped = sql.strip().upper()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        return {"error": "Only SELECT queries are allowed."}

    for keyword in ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "REPLACE"]:
        if keyword in stripped:
            return {"error": f"Query contains forbidden keyword: {keyword}"}

    try:
        conn = sqlite3.connect(str(MARKET_DB_PATH))
        conn.row_factory = sqlite3.Row

        # Build WHERE clauses for temp views
        # Tables with a symbol column that should be sector-filtered
        SYMBOL_TABLES = {"prices", "income", "balance", "cashflow", "earnings",
                         "insider_trades", "analyst_grades"}

        # Benchmark/sector ETFs — always queryable regardless of sector scope so the
        # agent can compare against SPY or its sector ETF. ETFs are absent from
        # universe_profiles, so the sector subquery would otherwise exclude them.
        BENCHMARK_ETFS = ("SPY", "XLK", "XLF", "XLE", "XLV")
        etf_list = ", ".join(f"'{s}'" for s in BENCHMARK_ETFS)

        for table, date_col in DATE_COLUMN_MAP.items():
            conditions = []
            if _STOP_DATE:
                conditions.append(f"{date_col} <= '{_STOP_DATE}'")
            if _SECTOR and table in SYMBOL_TABLES:
                conditions.append(
                    f"(symbol IN (SELECT symbol FROM main.universe_profiles WHERE sector = '{_SECTOR}') "
                    f"OR symbol IN ({etf_list}))"
                )
            if conditions:
                where = " AND ".join(conditions)
                conn.execute(f"CREATE TEMP VIEW IF NOT EXISTS {table} AS SELECT * FROM main.{table} WHERE {where}")

        # Filter universe_profiles by sector (but keep it queryable)
        if _SECTOR:
            conn.execute(f"""
                CREATE TEMP VIEW IF NOT EXISTS universe_profiles AS
                SELECT * FROM main.universe_profiles WHERE sector = '{_SECTOR}'
            """)

        cursor = conn.execute(sql)
        rows = cursor.fetchmany(500)

        if not rows:
            conn.close()
            return {"columns": [], "rows": [], "row_count": 0}

        columns = [desc[0] for desc in cursor.description]
        result_rows = [dict(r) for r in rows]
        conn.close()

        return {
            "columns": columns,
            "rows": result_rows,
            "row_count": len(result_rows),
        }

    except Exception as e:
        return {"error": str(e)}


def validate_portfolio(config: dict) -> dict:
    """
    Validate a full portfolio config against the Pydantic PortfolioConfig model.

    Returns:
        {"valid": True} or {"valid": False, "error": "description of what's wrong"}
    """
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "server"))
        from pydantic import ValidationError
        from models.portfolio import PortfolioConfig

        # Parse through Pydantic — catches all type/field/constraint errors
        PortfolioConfig.model_validate(config)

        # Cross-field check: regime_gate IDs must reference regime_definitions keys
        regime_defs = config.get("regime_definitions") or {}
        for i, sleeve in enumerate(config.get("sleeves", [])):
            label = sleeve.get("label", f"sleeve_{i}")
            for gate_id in sleeve.get("regime_gate", []):
                if gate_id != "*" and gate_id not in regime_defs:
                    return {
                        "valid": False,
                        "error": f"Sleeve '{label}': regime_gate references '{gate_id}' "
                                 f"but it is not defined in regime_definitions. "
                                 f"Add it to regime_definitions or use '*' for always-active.",
                    }

        # Cross-field check: weights must sum to ~1.0
        weights = [s.get("weight", 0) for s in config.get("sleeves", [])]
        total_weight = sum(weights)
        if abs(total_weight - 1.0) > 0.01:
            return {"valid": False, "error": f"Sleeve weights sum to {total_weight:.2f}, must sum to 1.0"}

        return {"valid": True}

    except ValidationError as e:
        # Return first error in a readable format
        first = e.errors()[0]
        loc = " -> ".join(str(x) for x in first["loc"])
        return {"valid": False, "error": f"{loc}: {first['msg']}"}

    except Exception as e:
        return {"valid": False, "error": f"Validation error: {str(e)}"}


# --- Claude Agent SDK tool registration ---

@tool(
    "query_market_data",
    "Run a read-only SQL SELECT query against the market database. "
    "Returns up to 500 rows. All results are automatically filtered to the allowed date range. "
    "Use this for all market data queries — prices, fundamentals, earnings, macro indicators, etc.",
    {"sql": str},
)
async def query_market_data_tool(args: dict[str, Any]) -> dict[str, Any]:
    result = execute_query(args.get("sql", ""))
    # Compact output for large result sets
    text = json.dumps(result, default=str)
    if len(text) > 50000:
        # Truncate rows to fit context
        result["rows"] = result["rows"][:100]
        result["row_count"] = len(result["rows"])
        result["truncated"] = True
        text = json.dumps(result, default=str)
    return {"content": [{"type": "text", "text": text}]}


@tool(
    "validate_portfolio",
    "Validate a portfolio configuration against the backtest engine schema. "
    "Call this with your complete portfolio config JSON BEFORE outputting your final <thesis>. "
    "Returns {valid: true} if correct, or {valid: false, error: '...'} with the exact issue to fix. "
    "Smoothing + rebalance defaults applied uniformly: each regime in "
    "regime_definitions accepts entry_persistence_days / exit_persistence_days "
    "(default 3 each) — consecutive days of confirming evidence required before "
    "activate/deactivate. Portfolio-level transition_days_to_defensive (default "
    "1, fast escape) / transition_days_to_offensive (default 3, patient redeployment). "
    "rebalance_threshold (default 0.05 = 5%) gates daily drift correction within "
    "an active profile — set to 0 for continuous daily rebalance, leave at default "
    "for institutional 5% drift tolerance. Regime flips and lerp days always "
    "rebalance regardless of threshold.",
    {"config": dict},
)
async def validate_portfolio_tool(args: dict[str, Any]) -> dict[str, Any]:
    result = validate_portfolio(args.get("config", {}))
    return {"content": [{"type": "text", "text": json.dumps(result)}]}


@tool(
    "evaluate_signal",
    "Scores a single entry signal as a long-only equal-weight factor "
    "portfolio (each signal-fire opens a unit-weight position held for "
    "target_horizon trading days; no costs, no capacity caps).\n\n"
    "UPPER BOUND: these are factor-portfolio metrics. The basket holds "
    "ALL firing names equal-weight with no ranking, no max_positions, "
    "no slippage, no exit rules beyond the time stop. A real backtest "
    "with ranking.by + max_positions + rebalance + costs typically "
    "realizes meaningfully less alpha (often 30-50% lower) at "
    "similar-or-lower Sharpe. Treat as the ceiling, not the forecast.\n\n"
    "Returns portfolio_metrics (Sharpe, alpha vs market & sector, "
    "drawdown, volatility, sortino), rolling IC at 63/252/504-day "
    "windows with CUSUM regime change-points and daily-step series, "
    "and coverage diagnostics (trigger_count, unique_stocks, "
    "yearly_breakdown, top/bottom_stocks).\n\n"
    "What it tells you: the upper-bound return profile of one specific "
    "signal in isolation; whether its predictive power is stable or "
    "broke at some point in history; which names contribute the alpha "
    "vs which are drags; how often it actually fires.\n\n"
    "Args: signal_config (entry condition config dict; same format as "
    "portfolio entry conditions, e.g. feature_threshold, "
    "feature_percentile, days_to_earnings, analyst_upgrades, "
    "momentum_rank, current_drop, rsi), target_horizon ('3m'/'6m'/'12m').",
    {"signal_config": dict, "target_horizon": str},
)
async def evaluate_signal_tool(args: dict[str, Any]) -> dict[str, Any]:
    from signal_ranker import evaluate_signal

    signal_config = args.get("signal_config", {})
    target_horizon = args.get("target_horizon", "6m")

    result = evaluate_signal(
        signal_config=signal_config,
        target_horizon=target_horizon,
        db_path=str(MARKET_DB_PATH),
        start=_START_DATE or "2015-01-01",
        end=_STOP_DATE or "2025-12-31",
        sector=_SECTOR,
    )

    text = json.dumps(result, default=str)
    if len(text) > 50000:
        # Trim coverage diagnostics; keep portfolio_metrics + ic intact.
        result["top_stocks"] = result.get("top_stocks", [])[:5]
        result["bottom_stocks"] = result.get("bottom_stocks", [])[:5]
        result["yearly_breakdown"] = result.get("yearly_breakdown", [])
        result["truncated"] = True
        text = json.dumps(result, default=str)
    return {"content": [{"type": "text", "text": text}]}


@tool(
    "rank_signals",
    "Scores 3-6 candidate entry signals as threshold-specific factor "
    "portfolios.\n\n"
    "UPPER BOUND: all metrics (per-candidate portfolio stats, "
    "correlation matrix, forward-selection Sharpes) are factor-"
    "portfolio metrics — ALL firing names equal-weight, no ranking, "
    "no max_positions, no slippage. A real backtest with ranking.by + "
    "max_positions typically realizes meaningfully less alpha (often "
    "30-50% lower) at similar-or-lower Sharpe. Treat as the ceiling, "
    "not the forecast.\n\n"
    "Returns per-candidate portfolio metrics (Sharpe, alpha vs "
    "sector/market, drawdown, volatility, trigger_count, unique_stocks), "
    "rolling IC at 63/252/504-day windows with CUSUM regime "
    "change-points, a pairwise Pearson correlation matrix of "
    "candidates' daily factor-portfolio returns, and a greedy AND-"
    "intersection forward-selection trace (per step: sharpe, "
    "sharpe_delta, correlation_with_running_combo, trigger_count, "
    "alpha_vs_sector_pct, max_drawdown_pct, verdict).\n\n"
    "What it tells you: the upper-bound return profile of these "
    "specific thresholds; whether each signal's edge is stable over "
    "time or has regime breaks; whether your candidates are saying "
    "the same thing once ANDed; which subset of them combines into "
    "the strongest joint signal before redundancy or trade-count "
    "starvation kicks in.\n\n"
    "Args: candidate_signals (list of entry condition config dicts, "
    "same format as evaluate_signal), target_horizon (hold period & "
    "IC horizon, e.g. '3m', '6m', '12m').",
    {"candidate_signals": list, "target_horizon": str},
)
async def rank_signals_tool(args: dict[str, Any]) -> dict[str, Any]:
    from signal_ranker import rank_signals

    candidates = args.get("candidate_signals", [])
    target_horizon = args.get("target_horizon", "6m")

    result = rank_signals(
        candidate_signals=candidates,
        target_horizon=target_horizon,
        db_path=str(MARKET_DB_PATH),
        start=_START_DATE or "2015-01-01",
        end=_STOP_DATE or "2025-12-31",
        sector=_SECTOR,
    )

    text = json.dumps(result, default=str)
    if len(text) > 50000:
        # Trim per-candidate coverage; keep portfolio_metrics, ic, and the
        # correlation matrix intact (those are the load-bearing outputs).
        for sig in result.get("individual_signals", []):
            sig.pop("top_stocks", None)
            sig.pop("bottom_stocks", None)
            sig.pop("yearly_breakdown", None)
        result["truncated"] = True
        text = json.dumps(result, default=str)
    return {"content": [{"type": "text", "text": text}]}


@tool(
    "analyze_factor_library",
    "Profiles all 35 registered features across a (universe, window). "
    "Returns per-factor statistics (IC at 4 horizons, sector + ln(mcap) "
    "neutralized IC, quintile spreads with monotonicity, top-bucket "
    "turnover) plus a cross-feature orthogonality block (35x35 rank "
    "correlation matrix, 35x35 factor-return correlation at 63d, "
    "hierarchical clusters, economic categories, top-K neighbors per "
    "feature).\n\n"
    "What it tells you: which factors carry statistically real "
    "predictive power in this universe/window; whether each factor's "
    "edge is genuine stock-picking or a hidden sector/size tilt; the "
    "natural horizon where each factor is strongest; which factors are "
    "redundant (same names, same timing) and which add diversification.\n\n"
    "Args (all optional, default to session context): sector, start, "
    "end, universe (symbol list overrides sector), features (subset of "
    "35). Cached on the arg tuple.",
    {},
)
async def analyze_factor_library_tool(args: dict[str, Any]) -> dict[str, Any]:
    from auto_trader.factor_library import analyze_factor_library as _afl

    sector = args.get("sector") or _SECTOR
    start = args.get("start") or _START_DATE or "2015-01-01"
    end = args.get("end") or _STOP_DATE or "2025-12-31"
    universe = args.get("universe")
    features = args.get("features")

    result = _afl(
        sector=sector,
        universe=universe if isinstance(universe, list) else None,
        start=start,
        end=end,
        features=features if isinstance(features, list) else None,
        use_cache=True,
    )
    return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}


@tool(
    "combine_factors",
    "Solve data-driven composite_score weights for a set of factors, instead of "
    "hand-setting bucket weights. Given factors + a target horizon, it computes "
    "each factor's rank-IC over the run's training window, decorrelates them via "
    "the factor covariance, and returns a ready-to-use composite_score block "
    "(one factor per bucket, with the solved weight and the IC-implied sign).\n\n"
    "CRITICAL — judge by the OUT-OF-SAMPLE number, not in-sample. The diagnostics "
    "include `combined_ic_oos` (purged k-fold, embargoed by the horizon so "
    "overlapping forward-return windows can't leak) and `equal_weight_ic_oos` as "
    "the baseline to beat. If `combined_ic_oos` <= 0 or < equal-weight, DO NOT use "
    "the solved weights — fall back to method='equal' or 'ic_weighted'. A large "
    "in-sample/OOS gap means overfitting (raise `shrinkage`).\n\n"
    "What it tells you: which factors actually carry OOS predictive power at this "
    "horizon (and their correct sign), how to weight them accounting for "
    "redundancy, and whether the combination generalizes at all. Window + universe "
    "default to the run's training period and sector (never reads past the data "
    "cutoff).\n\n"
    "Args: factors (list of feature names from the 35-factor library), horizon "
    "('63d'/'3m'/'6m'/'12m'; hold/IC horizon, default '63d'), method "
    "('ic_optimal' = Sigma^-1*IC decorrelated [default], 'ic_weighted' = "
    "IC-weighted, 'equal'), shrinkage (0..1 toward equal weight, default 0.3).",
    {"factors": list, "horizon": str, "method": str, "shrinkage": float},
)
async def combine_factors_tool(args: dict[str, Any]) -> dict[str, Any]:
    from alpha_combine import combine_factors

    result = combine_factors(
        factors=args.get("factors") or [],
        horizon=args.get("horizon") or "63d",
        method=args.get("method") or "ic_optimal",
        shrinkage=args.get("shrinkage") if args.get("shrinkage") is not None else 0.3,
        sector=_SECTOR,
        start=_START_DATE or "2015-01-01",
        end=_STOP_DATE,
        db_path=str(MARKET_DB_PATH),
    )
    return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}


@tool(
    "get_experiment_trades",
    "Drill into the trade log for a past experiment in this run. Call "
    "get_experiment_stats FIRST to identify the dimension worth drilling into "
    "(a reason, a month, a symbol, a tail), then use the filters here to pull "
    "only the relevant rows — never dump without a narrowing filter on multi-year "
    "backtests.\n\n"
    "The experiment_id is the hash shown in brackets in each past experiment's header "
    "(e.g., '### Experiment 4 [id: 50e63c54f604]'). Pass that hash as experiment_id.\n\n"
    "Returns BUYs and SELLs for the experiment's backtest, with each row tagged by "
    "sleeve_label. SELL rows carry round-trip fields: pnl, pnl_pct, entry_date, "
    "entry_price, days_held, reason. BUY rows have those fields as null.\n\n"
    "Filters (all optional, combined with AND):\n"
    "  sleeve_label:    only trades from one sleeve (matches by_sleeve keys)\n"
    "  window:          'training' (training-period trades) or an eval-window\n"
    "                   label like '2017-01-01_2019-01-01' (matches the keys in\n"
    "                   get_experiment_stats.available_windows). Omit to include\n"
    "                   all windows. Only meaningful for experiments with eval.\n"
    "  action:          'BUY' or 'SELL' only\n"
    "  winners_only:    SELL trades with pnl > 0\n"
    "  losers_only:     SELL trades with pnl <= 0\n"
    "  reason:          exit reason (matches by_exit_reason keys — e.g. 'stop_loss')\n"
    "  symbol:          single ticker (matches top_symbols_by_contribution)\n"
    "  start_date:      YYYY-MM-DD inclusive (matches monthly_pnl buckets)\n"
    "  end_date:        YYYY-MM-DD inclusive\n"
    "  min_abs_pnl:     only trades with |pnl| >= this number (tail inspection)\n"
    "  min_days_held:   only SELLs held >= this many days\n"
    "  max_days_held:   only SELLs held <= this many days\n\n"
    "Results are capped at 200 rows. If truncated=true, add a narrower filter.\n"
    "Scope: experiments from the current run only. Cross-run access returns empty.",
    {"experiment_id": str, "sleeve_label": str, "window": str, "action": str,
     "winners_only": bool, "losers_only": bool,
     "reason": str, "symbol": str,
     "start_date": str, "end_date": str,
     "min_abs_pnl": float,
     "min_days_held": int, "max_days_held": int},
)
async def get_experiment_trades_tool(args: dict[str, Any]) -> dict[str, Any]:
    from auto_trader.schema import get_db

    experiment_id = args.get("experiment_id", "").strip()
    if not experiment_id:
        return {"content": [{"type": "text", "text": json.dumps({"error": "experiment_id is required"})}]}

    # Build WHERE clause — scope enforced via run_id subquery
    where = ["source_type = 'experiment'", "source_id = ?"]
    params: list[Any] = [experiment_id]

    if _RUN_ID:
        where.append("source_id IN (SELECT id FROM experiments WHERE run_id = ?)")
        params.append(_RUN_ID)

    sleeve_label = args.get("sleeve_label")
    if sleeve_label:
        where.append("sleeve_label = ?")
        params.append(sleeve_label)

    # Eval-window filter. 'training' means window_label IS NULL.
    window_filter = args.get("window")
    if window_filter:
        if window_filter == "training":
            where.append("window_label IS NULL")
        else:
            where.append("window_label = ?")
            params.append(window_filter)

    action = args.get("action")
    if action in ("BUY", "SELL"):
        where.append("action = ?")
        params.append(action)

    if args.get("winners_only"):
        where.append("action = 'SELL' AND pnl > 0")
    elif args.get("losers_only"):
        where.append("action = 'SELL' AND pnl <= 0")

    reason = args.get("reason")
    if reason:
        where.append("reason = ?")
        params.append(reason)

    symbol = args.get("symbol")
    if symbol:
        where.append("symbol = ?")
        params.append(symbol)

    start_date = args.get("start_date")
    if start_date:
        where.append("date >= ?")
        params.append(start_date)

    end_date = args.get("end_date")
    if end_date:
        where.append("date <= ?")
        params.append(end_date)

    min_abs_pnl = args.get("min_abs_pnl")
    if min_abs_pnl is not None:
        # pnl is null on BUYs → ABS(NULL) is NULL → comparison is NULL → row excluded.
        # This implicitly restricts to SELLs with pnl, which is the intended tail filter.
        where.append("ABS(pnl) >= ?")
        params.append(float(min_abs_pnl))

    min_days_held = args.get("min_days_held")
    if min_days_held is not None:
        where.append("days_held >= ?")
        params.append(int(min_days_held))

    max_days_held = args.get("max_days_held")
    if max_days_held is not None:
        where.append("days_held <= ?")
        params.append(int(max_days_held))

    sql = f"""
        SELECT sleeve_label, date, action, symbol, shares, price, amount,
               reason, signal_detail, entry_date, entry_price,
               pnl, pnl_pct, days_held
        FROM trades
        WHERE {' AND '.join(where)}
        ORDER BY date, action, symbol
        LIMIT 201
    """

    conn = get_db()
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    truncated = len(rows) > 200
    rows = rows[:200]

    trades = []
    for r in rows:
        d = dict(r)
        if d.get("signal_detail") and isinstance(d["signal_detail"], str):
            try:
                d["signal_detail"] = json.loads(d["signal_detail"])
            except (json.JSONDecodeError, TypeError):
                pass
        trades.append(d)

    result = {
        "experiment_id": experiment_id,
        "trade_count": len(trades),
        "trades": trades,
        "truncated": truncated,
    }
    return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}


@tool(
    "get_experiment_stats",
    "Aggregate statistics for a past experiment's trade log. Returns NO trade rows — "
    "only population-level summaries so you can understand the shape of the trade "
    "distribution without pulling individual trades. Call this BEFORE get_experiment_trades "
    "to decide whether a drill-down is warranted and what to filter by.\n\n"
    "The experiment_id is the hash shown in brackets in the history header "
    "(e.g., '### Experiment 4 [id: 50e63c54f604]').\n\n"
    "Optional `window` filter scopes to one slice of the experiment:\n"
    "  unset:      all trades (training-period + every eval window combined)\n"
    "  'training': only the training-period backtest (window_label IS NULL)\n"
    "  'YYYY-MM-DD_YYYY-MM-DD': one eval window (see available_windows in the response)\n\n"
    "Returns:\n"
    "  available_windows: list of eval window labels present (empty if no eval)\n"
    "  filtered_by_window: echoes the window arg (or null = all)\n"
    "  totals: trade counts (buys, sells, closed), total_pnl, win_rate_pct, avg_pnl\n"
    "  pnl_distribution: min, p10, p25, p50 (median), p75, p90, max, stdev — across closed SELLs\n"
    "  by_exit_reason: {reason: {count, total_pnl, avg_pnl}}\n"
    "  by_sleeve: {sleeve_label: {count, total_pnl, avg_pnl, win_rate_pct}}\n"
    "  top_symbols_by_contribution: up to 10 symbols ranked by |total_pnl|\n"
    "  monthly_pnl: list of {month, pnl, trades} ordered by month\n"
    "  holding_days: {winners_avg, losers_avg, overall_avg}\n\n"
    "Use this tool first to spot where the experiment's P&L actually came from; only "
    "then pull specific rows via get_experiment_trades with targeted filters.",
    {"experiment_id": str, "window": str},
)
async def get_experiment_stats_tool(args: dict[str, Any]) -> dict[str, Any]:
    from auto_trader.schema import get_db
    import statistics as _stats

    experiment_id = args.get("experiment_id", "").strip()
    if not experiment_id:
        return {"content": [{"type": "text",
                             "text": json.dumps({"error": "experiment_id is required"})}]}

    # Scope predicate — same run_id guard as get_experiment_trades
    scope_where = ["source_type = 'experiment'", "source_id = ?"]
    scope_params: list[Any] = [experiment_id]
    if _RUN_ID:
        scope_where.append("source_id IN (SELECT id FROM experiments WHERE run_id = ?)")
        scope_params.append(_RUN_ID)

    # Walk-forward window filter.
    window_filter = args.get("window")
    if window_filter:
        if window_filter == "training":
            scope_where.append("window_label IS NULL")
        else:
            scope_where.append("window_label = ?")
            scope_params.append(window_filter)
    scope_sql = " AND ".join(scope_where)

    conn = get_db()

    # 0. Available eval-window labels for this experiment (so the agent
    # knows what values are valid for window=).
    avail = [r[0] for r in conn.execute(
        "SELECT DISTINCT window_label FROM trades "
        "WHERE source_type='experiment' AND source_id=? "
        "  AND window_label IS NOT NULL "
        "ORDER BY window_label",
        [experiment_id],
    ).fetchall()]

    # 1. Overall counts
    row = conn.execute(
        f"SELECT "
        f"  COUNT(*)                                          AS total, "
        f"  SUM(CASE WHEN action='BUY'  THEN 1 ELSE 0 END)    AS buys, "
        f"  SUM(CASE WHEN action='SELL' THEN 1 ELSE 0 END)    AS sells, "
        f"  SUM(CASE WHEN action='SELL' AND pnl IS NOT NULL THEN 1 ELSE 0 END) AS closed "
        f"FROM trades WHERE {scope_sql}",
        scope_params,
    ).fetchone()
    totals = {
        "total_trades": row["total"] or 0,
        "buys":         row["buys"]  or 0,
        "sells":        row["sells"] or 0,
        "closed_sells": row["closed"] or 0,
    }

    # 2. P&L distribution — across SELL rows with non-null pnl
    pnls = [r[0] for r in conn.execute(
        f"SELECT pnl FROM trades WHERE {scope_sql} AND action='SELL' AND pnl IS NOT NULL "
        f"ORDER BY pnl", scope_params).fetchall()]

    def _quantile(sorted_xs: list[float], q: float) -> float | None:
        if not sorted_xs:
            return None
        # Linear interpolation between positions — type-7 quantile (R default).
        n = len(sorted_xs)
        if n == 1:
            return sorted_xs[0]
        pos = (n - 1) * q
        lo = int(pos)
        hi = min(lo + 1, n - 1)
        frac = pos - lo
        return sorted_xs[lo] + (sorted_xs[hi] - sorted_xs[lo]) * frac

    if pnls:
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        total_pnl = sum(pnls)
        pnl_distribution = {
            "min":   round(pnls[0], 2),
            "p10":   round(_quantile(pnls, 0.10), 2),
            "p25":   round(_quantile(pnls, 0.25), 2),
            "p50":   round(_quantile(pnls, 0.50), 2),
            "p75":   round(_quantile(pnls, 0.75), 2),
            "p90":   round(_quantile(pnls, 0.90), 2),
            "max":   round(pnls[-1], 2),
            "stdev": round(_stats.stdev(pnls), 2) if len(pnls) > 1 else 0.0,
        }
        totals["total_pnl"]    = round(total_pnl, 2)
        totals["avg_pnl"]      = round(total_pnl / len(pnls), 2)
        totals["wins"]         = len(wins)
        totals["losses"]       = len(losses)
        totals["win_rate_pct"] = round(len(wins) / len(pnls) * 100, 2)
    else:
        pnl_distribution = None
        totals.update({"total_pnl": 0.0, "avg_pnl": None, "wins": 0, "losses": 0,
                       "win_rate_pct": None})

    # 3. By exit reason — SELLs only, grouped
    by_reason = {}
    for r in conn.execute(
        f"SELECT reason, COUNT(*) AS n, "
        f"       COALESCE(SUM(pnl), 0) AS total, "
        f"       COALESCE(AVG(pnl), 0) AS avg "
        f"FROM trades WHERE {scope_sql} AND action='SELL' AND pnl IS NOT NULL "
        f"GROUP BY reason ORDER BY n DESC", scope_params).fetchall():
        by_reason[r["reason"] or "unknown"] = {
            "count":     r["n"],
            "total_pnl": round(r["total"], 2),
            "avg_pnl":   round(r["avg"], 2),
        }

    # 4. By sleeve — include win rate
    by_sleeve = {}
    for r in conn.execute(
        f"SELECT sleeve_label, "
        f"       COUNT(*) AS n, "
        f"       COALESCE(SUM(pnl), 0)     AS total, "
        f"       COALESCE(AVG(pnl), 0)     AS avg, "
        f"       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins "
        f"FROM trades WHERE {scope_sql} AND action='SELL' AND pnl IS NOT NULL "
        f"GROUP BY sleeve_label ORDER BY n DESC", scope_params).fetchall():
        label = r["sleeve_label"] or "unknown"
        by_sleeve[label] = {
            "count":        r["n"],
            "total_pnl":    round(r["total"], 2),
            "avg_pnl":      round(r["avg"], 2),
            "win_rate_pct": round(r["wins"] / r["n"] * 100, 2) if r["n"] else None,
        }

    # 5. Top symbols by |total_pnl|
    top_symbols = []
    for r in conn.execute(
        f"SELECT symbol, COUNT(*) AS n, COALESCE(SUM(pnl), 0) AS total "
        f"FROM trades WHERE {scope_sql} AND action='SELL' AND pnl IS NOT NULL "
        f"GROUP BY symbol ORDER BY ABS(COALESCE(SUM(pnl),0)) DESC LIMIT 10",
        scope_params).fetchall():
        top_symbols.append({
            "symbol":    r["symbol"],
            "trades":    r["n"],
            "total_pnl": round(r["total"], 2),
        })

    # 6. Monthly P&L — bucketed by the SELL exit date
    monthly = []
    for r in conn.execute(
        f"SELECT substr(date, 1, 7) AS month, "
        f"       COUNT(*) AS n, "
        f"       COALESCE(SUM(pnl), 0) AS total "
        f"FROM trades WHERE {scope_sql} AND action='SELL' AND pnl IS NOT NULL "
        f"GROUP BY month ORDER BY month", scope_params).fetchall():
        monthly.append({
            "month":  r["month"],
            "trades": r["n"],
            "pnl":    round(r["total"], 2),
        })

    # 7. Holding days — winners vs losers averages
    hd = conn.execute(
        f"SELECT "
        f"  AVG(CASE WHEN pnl > 0  THEN days_held END) AS win_avg, "
        f"  AVG(CASE WHEN pnl <= 0 THEN days_held END) AS loss_avg, "
        f"  AVG(days_held) AS all_avg "
        f"FROM trades WHERE {scope_sql} AND action='SELL' AND pnl IS NOT NULL "
        f"  AND days_held IS NOT NULL", scope_params).fetchone()
    holding_days = {
        "winners_avg": round(hd["win_avg"], 1)  if hd["win_avg"]  is not None else None,
        "losers_avg":  round(hd["loss_avg"], 1) if hd["loss_avg"] is not None else None,
        "overall_avg": round(hd["all_avg"], 1)  if hd["all_avg"]  is not None else None,
    }

    conn.close()

    result = {
        "experiment_id":                experiment_id,
        "available_windows":            avail,
        "filtered_by_window":           window_filter,
        "totals":                       totals,
        "pnl_distribution":             pnl_distribution,
        "by_exit_reason":               by_reason,
        "by_sleeve":                    by_sleeve,
        "top_symbols_by_contribution":  top_symbols,
        "monthly_pnl":                  monthly,
        "holding_days":                 holding_days,
    }
    return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}


# ---------------------------------------------------------------------------
# Portfolio factor exposure analysis
# ---------------------------------------------------------------------------

def _market_db() -> sqlite3.Connection:
    """Open a read-only connection to market.db."""
    conn = sqlite3.connect(f"file:{MARKET_DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _resolve_trading_date(conn: sqlite3.Connection, as_of: str) -> str | None:
    """Return the most recent trading date ≤ as_of (uses features_daily as the
    calendar). None if no data on/before that date."""
    row = conn.execute(
        "SELECT MAX(date) FROM features_daily WHERE date <= ?", (as_of,)
    ).fetchone()
    return row[0] if row and row[0] else None


def _resolve_experiment_holdings(
    experiment_id: str,
    as_of_date: str | None,
) -> tuple[dict[str, float], str, str | None]:
    """Reconstruct open holdings for an experiment at `as_of_date`.

    Returns ({symbol: weight}, resolved_date, error_or_None).
    Weights are dollar-value share weights using close on resolved_date.
    """
    from auto_trader.schema import get_db

    app_conn = get_db()
    where = ["source_type = 'experiment'", "source_id = ?"]
    params: list[Any] = [experiment_id]
    if _RUN_ID:
        where.append("source_id IN (SELECT id FROM experiments WHERE run_id = ?)")
        params.append(_RUN_ID)

    # Fall back to experiment.backtest_end if as_of_date not given
    if not as_of_date:
        row = app_conn.execute(
            "SELECT backtest_end FROM experiments WHERE id = ?", (experiment_id,)
        ).fetchone()
        if not row or not row["backtest_end"]:
            app_conn.close()
            return {}, "", f"experiment {experiment_id} not found or has no backtest_end"
        as_of_date = row["backtest_end"]

    where.append("date <= ?")
    params.append(as_of_date)

    rows = app_conn.execute(
        f"""SELECT symbol,
                   SUM(CASE WHEN action='BUY' THEN shares ELSE -shares END) AS net_shares
            FROM trades
            WHERE {' AND '.join(where)}
            GROUP BY symbol
            HAVING net_shares > 1e-9""",
        params,
    ).fetchall()
    app_conn.close()

    if not rows:
        return {}, as_of_date, "no open holdings at as_of_date"

    symbols = [r["symbol"] for r in rows]
    net_shares = {r["symbol"]: float(r["net_shares"]) for r in rows}

    # Get closing prices on resolved trading date (≤ as_of_date)
    mkt = _market_db()
    trading_date = _resolve_trading_date(mkt, as_of_date)
    if not trading_date:
        mkt.close()
        return {}, as_of_date, f"no market data on/before {as_of_date}"

    placeholders = ",".join("?" * len(symbols))
    prows = mkt.execute(
        f"""SELECT symbol, close FROM prices
            WHERE symbol IN ({placeholders}) AND date = ?""",
        (*symbols, trading_date),
    ).fetchall()
    mkt.close()

    prices = {r["symbol"]: float(r["close"]) for r in prows if r["close"] is not None}
    values = {s: net_shares[s] * prices[s] for s in symbols if s in prices}
    if not values:
        return {}, trading_date, "no price data for holdings on as_of_date"

    total = sum(values.values())
    weights = {s: v / total for s, v in values.items()}
    return weights, trading_date, None


def _compute_zscores(
    conn: sqlite3.Connection,
    as_of_date: str,
    factors: list[str],
    universe_symbols: list[str] | None,
) -> tuple[dict[str, dict[str, float]], dict[str, tuple[float | None, float | None, int]]]:
    """Compute z-scores for `factors` at `as_of_date` over a universe.

    Universe = `universe_symbols` if provided, else all symbols in features_daily.

    Returns:
        per_symbol: {symbol: {factor: z_score_or_None}}
        stats:     {factor: (mean, std, n)}  — for diagnostics
    """
    factor_cols = ",".join(factors)
    if universe_symbols:
        placeholders = ",".join("?" * len(universe_symbols))
        rows = conn.execute(
            f"""SELECT symbol, {factor_cols} FROM features_daily
                WHERE date = ? AND symbol IN ({placeholders})""",
            (as_of_date, *universe_symbols),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""SELECT symbol, {factor_cols} FROM features_daily WHERE date = ?""",
            (as_of_date,),
        ).fetchall()

    raw: dict[str, dict[str, float]] = {}
    for r in rows:
        sym = r["symbol"]
        d: dict[str, float] = {}
        for f in factors:
            v = r[f]
            if v is not None:
                d[f] = float(v)
        raw[sym] = d

    # Per-factor mean/std over the universe (ignoring NULLs)
    import statistics
    stats: dict[str, tuple[float | None, float | None, int]] = {}
    for f in factors:
        vals = [raw[s][f] for s in raw if f in raw[s]]
        if len(vals) < 5:
            stats[f] = (None, None, len(vals))
            continue
        m = statistics.fmean(vals)
        sd = statistics.pstdev(vals)
        stats[f] = (m, sd if sd > 0 else None, len(vals))

    z_per_symbol: dict[str, dict[str, float]] = {}
    for sym, d in raw.items():
        zs: dict[str, float] = {}
        for f in factors:
            val = d.get(f)
            m, sd, _ = stats.get(f, (None, None, 0))
            if val is None or m is None or sd is None:
                continue
            zs[f] = (val - m) / sd
        z_per_symbol[sym] = zs

    return z_per_symbol, stats


def _aggregate_exposure(
    weights: dict[str, float],
    z_per_symbol: dict[str, dict[str, float]],
    factors: list[str],
) -> tuple[dict[str, float], dict[str, list[dict]]]:
    """Position-weighted aggregation across holdings.

    Returns:
        exposures: {factor: weighted_avg_z}
        contributors: {factor: [{symbol, weight, z, contribution_pct}, ...]} top 5
    """
    exposures: dict[str, float] = {}
    contributors: dict[str, list[dict]] = {}

    for f in factors:
        # Renormalize weights across symbols that actually have data for this factor.
        # Otherwise a NULL z drags the average toward zero.
        usable = [(s, w) for s, w in weights.items() if f in z_per_symbol.get(s, {})]
        if not usable:
            continue
        wsum = sum(w for _, w in usable)
        if wsum <= 0:
            continue
        exposure = sum((w / wsum) * z_per_symbol[s][f] for s, w in usable)
        exposures[f] = exposure

        # Top contributors by |signed contribution| (w/wsum × z)
        contribs = []
        for s, w in usable:
            z = z_per_symbol[s][f]
            contrib = (w / wsum) * z
            contribs.append((s, w, z, contrib))
        contribs.sort(key=lambda t: abs(t[3]), reverse=True)
        contributors[f] = [
            {
                "symbol": s,
                "weight": round(w, 4),
                "z": round(z, 3),
                "contribution_pct": round(100 * (contrib / exposure) if exposure else 0, 1),
            }
            for (s, w, z, contrib) in contribs[:5]
        ]

    return exposures, contributors


def _roll_up_bets(exposures: dict[str, float]) -> dict[str, float]:
    """Roll up per-factor z-scores into per-category 'bet' scores.

    Sign-flips factors with direction='lower' so + always means MORE of the
    category bet (e.g., -pe contributes positively to 'value' bet).
    """
    bets: dict[str, float] = {}
    for cat, factors in CANONICAL_FACTORS.items():
        signed: list[float] = []
        for col, direction in factors:
            if col not in exposures:
                continue
            sign = 1.0 if direction == "higher" else -1.0
            signed.append(sign * exposures[col])
        if signed:
            bets[cat] = sum(signed) / len(signed)
    return bets


def _concentration_stats(weights: dict[str, float]) -> dict[str, float]:
    """Herfindahl + effective N + max position."""
    if not weights:
        return {}
    ws = list(weights.values())
    herfindahl = sum(w * w for w in ws)
    return {
        "n_positions": len(ws),
        "max_single_position_pct": round(max(ws), 4),
        "herfindahl": round(herfindahl, 4),
        "effective_n_positions": round(1.0 / herfindahl, 2) if herfindahl > 0 else 0,
    }


@tool(
    "analyze_portfolio_exposures",
    "Decompose a portfolio's actual factor exposures — what bets the realized "
    "holdings express, not just what the screening rules intended.\n\n"
    "Unlike analyze_factor_library (which describes the universe), this tool "
    "introspects YOUR portfolio: position-weighted z-scores across momentum, "
    "value, quality, growth, yield, sentiment. Useful for:\n"
    "  - sanity-check that the screen produced the bets you intended\n"
    "  - detect hidden tilts (size, momentum, vol leakage from correlated screens)\n"
    "  - compare two sleeves to see if they overlap in factor space\n"
    "  - attribute eval-window losses to factor moves\n\n"
    "Inputs (provide EITHER experiment_id OR positions+as_of_date):\n"
    "  experiment_id:  hash from history header ([id: 50e63c54f604]). Holdings\n"
    "                  reconstructed from trade log; as_of_date defaults to\n"
    "                  experiment.backtest_end if not given.\n"
    "  positions:      {symbol: weight} dict, weights need not sum to 1 (renormalized)\n"
    "  as_of_date:     YYYY-MM-DD (required if using positions)\n\n"
    "Returns:\n"
    "  exposures: per-factor sector-relative AND full-universe z-scores\n"
    "  bet_summary: rolled-up per-category 'bet' (sign-corrected so + = more of bet)\n"
    "  top_contributors: which 1-5 names drive each factor tilt\n"
    "  concentration: herfindahl, effective N, max position\n\n"
    "Interpretation of z-scores:\n"
    "  |z| < 0.3   neutral, no real bet on this factor\n"
    "  0.3-1.0     mild tilt\n"
    "  1.0-2.0     strong, deliberate bet\n"
    "  > 2.0       aggressive concentration",
    {"experiment_id": str, "positions": dict, "as_of_date": str,
     "sector": str, "factors": list},
)
async def analyze_portfolio_exposures_tool(args: dict[str, Any]) -> dict[str, Any]:
    experiment_id = (args.get("experiment_id") or "").strip()
    positions = args.get("positions") or {}
    as_of_date = args.get("as_of_date")
    sector_arg = args.get("sector") or _SECTOR
    factor_arg = args.get("factors")

    factors = factor_arg if isinstance(factor_arg, list) and factor_arg else CANONICAL_FACTOR_COLUMNS

    # Resolve holdings
    if experiment_id:
        weights, resolved_date, err = _resolve_experiment_holdings(experiment_id, as_of_date)
        if err:
            return {"content": [{"type": "text", "text": json.dumps({"error": err})}]}
        as_of_date = resolved_date
    elif positions and as_of_date:
        if not isinstance(positions, dict):
            return {"content": [{"type": "text", "text": json.dumps({"error": "positions must be {symbol: weight}"})}]}
        total = sum(float(v) for v in positions.values())
        if total <= 0:
            return {"content": [{"type": "text", "text": json.dumps({"error": "positions weights sum to 0"})}]}
        weights = {s: float(v) / total for s, v in positions.items()}
        # Snap as_of_date to trading day
        mkt = _market_db()
        snapped = _resolve_trading_date(mkt, as_of_date)
        mkt.close()
        if not snapped:
            return {"content": [{"type": "text", "text": json.dumps({"error": f"no market data on/before {as_of_date}"})}]}
        as_of_date = snapped
    else:
        return {"content": [{"type": "text", "text": json.dumps({"error": "Provide either experiment_id, or positions + as_of_date"})}]}

    holding_symbols = sorted(weights.keys())

    # Resolve the reference universe for the "declared" z-score comparison.
    # Priority:
    #   1. explicit `sector` argument from the caller (escape hatch)
    #   2. strategy's declared eligible universe (single source of truth)
    #   3. modal sector of the holdings (legacy fallback)
    mkt = _market_db()
    sector: str | None = None
    sector_universe: list[str] | None = None
    declared_universe_label: str | None = None

    if sector_arg:
        sector = sector_arg
        urows = mkt.execute(
            "SELECT symbol FROM universe_profiles WHERE sector = ?", (sector,)
        ).fetchall()
        sector_universe = sorted({r["symbol"] for r in urows})
        declared_universe_label = sector
    elif experiment_id:
        from auto_trader.universe import resolve_experiment_universe
        uid, syms, _kinds = resolve_experiment_universe(experiment_id, mkt)
        if uid != "all" and syms:
            sector_universe = syms
            declared_universe_label = uid  # sector name or 'custom'
            sector = uid if uid != "custom" else None

    if not sector_universe:
        # Last-resort fallback: modal sector of holdings
        ph = ",".join("?" * len(holding_symbols))
        srows = mkt.execute(
            f"SELECT sector, COUNT(*) c FROM universe_profiles "
            f"WHERE symbol IN ({ph}) GROUP BY sector ORDER BY c DESC LIMIT 1",
            holding_symbols,
        ).fetchall()
        if srows:
            sector = srows[0]["sector"]
            urows = mkt.execute(
                "SELECT symbol FROM universe_profiles WHERE sector = ?", (sector,)
            ).fetchall()
            sector_universe = sorted({r["symbol"] for r in urows})
            declared_universe_label = f"inferred:{sector}"

    # Compute z-scores both ways
    z_sector, stats_sector = ({}, {})
    if sector_universe and len(sector_universe) >= 5:
        z_sector, stats_sector = _compute_zscores(mkt, as_of_date, factors, sector_universe)

    z_full, stats_full = _compute_zscores(mkt, as_of_date, factors, None)
    mkt.close()

    # Aggregate (twice — once per z-frame)
    exposures_sector, contributors_sector = _aggregate_exposure(weights, z_sector, factors) if z_sector else ({}, {})
    exposures_full, contributors_full = _aggregate_exposure(weights, z_full, factors)

    bet_summary_sector = _roll_up_bets(exposures_sector) if exposures_sector else {}
    bet_summary_full = _roll_up_bets(exposures_full)

    # Build factors block — pair sector + full z per factor
    factors_block: dict[str, dict] = {}
    for f in factors:
        cat, direction = FACTOR_META.get(f, ("custom", "higher"))
        entry: dict[str, Any] = {
            "category": cat,
            "direction": "higher = more of factor" if direction == "higher" else "lower = more of named bet (sign-flipped in bet_summary)",
        }
        if f in exposures_sector:
            entry["z_sector"] = round(exposures_sector[f], 3)
        if f in exposures_full:
            entry["z_universe"] = round(exposures_full[f], 3)
        if f in contributors_sector:
            entry["top_contributors_sector"] = contributors_sector[f]
        elif f in contributors_full:
            entry["top_contributors_universe"] = contributors_full[f]
        # Only include factors with at least one z computed
        if "z_sector" in entry or "z_universe" in entry:
            factors_block[f] = entry

    result = {
        "as_of_date": as_of_date,
        "sector": sector,
        "declared_universe": declared_universe_label,
        "n_positions": len(weights),
        "weights": {s: round(w, 4) for s, w in sorted(weights.items(), key=lambda kv: -kv[1])},
        "factors": factors_block,
        "bet_summary": {
            "sector_relative": {k: round(v, 3) for k, v in bet_summary_sector.items()},
            "universe": {k: round(v, 3) for k, v in bet_summary_full.items()},
        },
        "concentration": _concentration_stats(weights),
        "diagnostics": {
            "factor_coverage": {f: stats_full[f][2] for f in factors if f in stats_full},
            "declared_universe_size": len(sector_universe) if sector_universe else 0,
            "declared_universe_source": (
                "explicit_sector" if sector_arg
                else "strategy_config" if declared_universe_label and not declared_universe_label.startswith("inferred:")
                else "inferred_modal_sector"
            ),
        },
    }
    return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}


@tool(
    "recall_memo_items",
    "Search the analyst's library of distilled claims from prior experiments "
    "in this run. Returns forward-looking claims (predictions a future "
    "experiment could falsify) by default — backward-looking observations "
    "are excluded unless you set forward_looking_only=false.\n\n"
    "Filters compose with AND. Defaults: this run, all kinds, forward-only, "
    "non-falsified, ordered by promotion_count DESC.\n\n"
    "Use this BEFORE designing a new iteration to surface insights you'd "
    "otherwise rediscover the hard way (e.g. 'tight stops whipsaw on PAYC' "
    "or 'macro gates + tight entry filters starve the book').\n\n"
    "Args:\n"
    "  experiment_id: only items extracted from this experiment.\n"
    "  universe:      'global' or a sector slug. Auto-scopes to the session "
    "sector if not set.\n"
    "  kind:          one of {factor_observation, trade_pattern, "
    "risk_observation, thesis_validation, regime_observation, anomaly}.\n"
    "  scope_level:   'run' (default), 'universe', or 'global' — the "
    "promotion-ladder level.\n"
    "  forward_looking_only: default true.\n"
    "  include_falsified:    default false.\n"
    "  limit:                default 20, max 100.",
    {"experiment_id": str, "universe": str, "kind": str,
     "scope_level": str, "forward_looking_only": bool,
     "include_falsified": bool, "limit": int},
)
async def recall_memo_items_tool(args: dict[str, Any]) -> dict[str, Any]:
    from auto_trader.analyst import recall_memo_items
    items = recall_memo_items(
        run_id=_RUN_ID,
        experiment_id=args.get("experiment_id") or None,
        universe=args.get("universe") or _SECTOR,
        kind=args.get("kind") or None,
        scope_level=args.get("scope_level") or None,
        forward_looking_only=args.get("forward_looking_only", True),
        include_falsified=args.get("include_falsified", False),
        limit=min(int(args.get("limit") or 20), 100),
    )
    return {"content": [{"type": "text", "text": json.dumps(
        {"n_items": len(items), "items": items}, default=str)}]}


@tool(
    "read_memo",
    "Read the full markdown post-mortem memo for a specific experiment. "
    "Use when recall_memo_items surfaces an interesting claim and you want "
    "the full context — the narrative around the claim, the numbers, the "
    "thesis comparison.\n\n"
    "Args:\n"
    "  experiment_id: hash from a history header ([id: 50e63c54f604]) or "
    "from a recall_memo_items result.",
    {"experiment_id": str},
)
async def read_memo_tool(args: dict[str, Any]) -> dict[str, Any]:
    from auto_trader.analyst import read_memo
    eid = (args.get("experiment_id") or "").strip()
    if not eid:
        return {"content": [{"type": "text", "text": json.dumps(
            {"error": "experiment_id required"})}]}
    memo = read_memo(eid)
    return {"content": [{"type": "text", "text": json.dumps(memo, default=str)}]}


def create_auto_trader_tools(stop_date: str | None = None, sector: str | None = None,
                             start_date: str | None = None, run_id: str | None = None,
                             allowed_tool_names: list[str] | None = None):
    """Create the MCP server with auto-trader tools.

    Args:
        stop_date: If set, silently filters all query results to dates <= stop_date.
        sector: If set, silently filters stock data to only this sector.
        start_date: If set, used as the start date for signal evaluation/ranking.
        run_id: If set, scopes get_experiment_trades to this run's experiments only.
        allowed_tool_names: If set, only these tool names are registered on the
            server — the model's tool catalog for this run cannot include any
            forbidden tools. If None, all tools are registered (CLI convenience).
    """
    global _STOP_DATE, _SECTOR, _START_DATE, _RUN_ID
    _STOP_DATE = stop_date
    _START_DATE = start_date
    _SECTOR = sector
    _RUN_ID = run_id

    if allowed_tool_names is None:
        tools = ALL_TOOLS
    else:
        allow = set(allowed_tool_names)
        tools = [t for t in ALL_TOOLS if t.name in allow]

    return create_sdk_mcp_server(
        name=MCP_SERVER_NAME,
        version="1.0.0",
        tools=tools,
    )


ALL_TOOLS: list[SdkMcpTool] = [
    v for v in list(globals().values()) if isinstance(v, SdkMcpTool)
]
TOOL_NAMES: set[str] = {t.name for t in ALL_TOOLS}


def list_available_tools() -> list[dict]:
    """Return the catalog of user-configurable MCP tools as [{name, description}]."""
    return [{"name": t.name, "description": t.description} for t in ALL_TOOLS]


def mcp_tool_id(name: str) -> str:
    """Prefix a tool name with the MCP server namespace the SDK expects."""
    return f"mcp__{MCP_SERVER_NAME}__{name}"
