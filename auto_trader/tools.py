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
    "Examples:\n"
    '  {"type": "momentum_rank", "lookback": 63, "operator": ">=", "value": 80}\n'
    '  {"type": "earnings_momentum", "lookback_quarters": 4, "min_beats": 3}\n'
    '  {"type": "pe_percentile", "max_percentile": 20}\n'
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
    "Fetch the full trade log for a past experiment in this run. "
    "Use this when a past experiment's summary suggests a pattern worth drilling into "
    "(e.g., one large loser dragged the hit rate, a sleeve ran hot in one regime, "
    "an exit reason dominates). "
    "The experiment_id is shown in brackets in each past experiment's header in the history "
    "(e.g., '### Experiment 4 [id: 50e63c54f604]'). Pass that hash as experiment_id.\n\n"
    "Returns all BUYs and SELLs for the experiment's backtest, with each row tagged by "
    "sleeve_label. SELL rows carry round-trip fields: pnl, pnl_pct, entry_date, "
    "entry_price, days_held, reason. BUY rows have those fields as null.\n\n"
    "Optional filters:\n"
    "  sleeve_label: narrow to one sleeve\n"
    "  action: 'BUY' or 'SELL' only\n"
    "  winners_only: SELL trades with pnl > 0 only\n"
    "  losers_only: SELL trades with pnl <= 0 only\n\n"
    "Scope: experiments from the current run only. Cross-run access returns empty.",
    {"experiment_id": str, "sleeve_label": str, "action": str,
     "winners_only": bool, "losers_only": bool},
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
