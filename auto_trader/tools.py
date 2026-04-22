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

        for table, date_col in DATE_COLUMN_MAP.items():
            conditions = []
            if _STOP_DATE:
                conditions.append(f"{date_col} <= '{_STOP_DATE}'")
            if _SECTOR and table in SYMBOL_TABLES:
                conditions.append(
                    f"symbol IN (SELECT symbol FROM main.universe_profiles WHERE sector = '{_SECTOR}')"
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
    "Returns {valid: true} if correct, or {valid: false, error: '...'} with the exact issue to fix.",
    {"config": dict},
)
async def validate_portfolio_tool(args: dict[str, Any]) -> dict[str, Any]:
    result = validate_portfolio(args.get("config", {}))
    return {"content": [{"type": "text", "text": json.dumps(result)}]}


@tool(
    "evaluate_signal",
    "Test how a single entry signal performed historically. "
    "Scans the full universe over the given period, finds every time the signal fired, "
    "and measures forward returns at the target horizon. "
    "Use this during research to investigate whether a signal pattern actually predicts returns. "
    "Returns trigger count, win rate, average return, Sharpe, and sample events (best/worst).\n\n"
    "signal_config: An entry condition config dict. Same format as portfolio entry conditions. "
    "Examples (prefer feature_threshold / feature_percentile / days_to_earnings / analyst_upgrades for valuation, growth, and catalyst signals):\n"
    '  {"type": "feature_percentile", "feature": "ev_ebitda", "max_percentile": 20, "scope": "sector", "min_value": 0, "max_value": 25}\n'
    '  {"type": "feature_threshold", "feature": "fcf_yield", "operator": ">=", "value": 5}\n'
    '  {"type": "feature_threshold", "feature": "eps_yoy", "operator": ">=", "value": 20}\n'
    '  {"type": "days_to_earnings", "min_days": 0, "max_days": 5}\n'
    '  {"type": "analyst_upgrades", "window_days": 90, "min_net_upgrades": 2}\n'
    '  {"type": "momentum_rank", "lookback": 63, "operator": ">=", "value": 80}\n'
    '  {"type": "current_drop", "threshold": -15, "window_days": 90}\n'
    '  {"type": "rsi", "period": 14, "operator": "<=", "value": 30}\n\n'
    "target_horizon: Forward return horizon. e.g. '3m', '6m', '12m'.",
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
        # Truncate sample events to fit context
        result["sample_events"] = result.get("sample_events", [])[:10]
        result["truncated"] = True
        text = json.dumps(result, default=str)
    return {"content": [{"type": "text", "text": text}]}


@tool(
    "rank_signals",
    "Rank multiple candidate entry signals and find the optimal combination. "
    "Tests each signal independently, then runs forward selection: starts with the best single signal, "
    "greedily adds the next best, stops when adding hurts Sharpe. "
    "Combination = intersection (trigger counts only when ALL signals agree on the same stock+date). "
    "Use this after investigating signals with evaluate_signal to decide the final signal set.\n\n"
    "candidate_signals: List of entry condition config dicts (same format as evaluate_signal). "
    "Provide 2-8 candidates for meaningful results.\n\n"
    "target_horizon: Forward return horizon. e.g. '3m', '6m', '12m'.",
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
        # Trim individual signal sample events
        for sig in result.get("individual_signals", []):
            sig.pop("sample_events", None)
        result["truncated"] = True
        text = json.dumps(result, default=str)
    return {"content": [{"type": "text", "text": text}]}


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
    {"experiment_id": str, "sleeve_label": str, "action": str,
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
    "Returns:\n"
    "  totals: trade counts (buys, sells, closed), total_pnl, win_rate_pct, avg_pnl\n"
    "  pnl_distribution: min, p10, p25, p50 (median), p75, p90, max, stdev — across closed SELLs\n"
    "  by_exit_reason: {reason: {count, total_pnl, avg_pnl}}\n"
    "  by_sleeve: {sleeve_label: {count, total_pnl, avg_pnl, win_rate_pct}}\n"
    "  top_symbols_by_contribution: up to 10 symbols ranked by |total_pnl|\n"
    "  monthly_pnl: list of {month, pnl, trades} ordered by month\n"
    "  holding_days: {winners_avg, losers_avg, overall_avg}\n\n"
    "Use this tool first to spot where the experiment's P&L actually came from; only "
    "then pull specific rows via get_experiment_trades with targeted filters.",
    {"experiment_id": str},
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
    scope_sql = " AND ".join(scope_where)

    conn = get_db()

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
        "totals":                       totals,
        "pnl_distribution":             pnl_distribution,
        "by_exit_reason":               by_reason,
        "by_sleeve":                    by_sleeve,
        "top_symbols_by_contribution":  top_symbols,
        "monthly_pnl":                  monthly,
        "holding_days":                 holding_days,
    }
    return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}


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
