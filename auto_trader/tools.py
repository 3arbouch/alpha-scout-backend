"""
Custom tools for the auto-trader agent.

query_market_data — run SQL against market.db with date filtering (no future data leakage).
validate_portfolio — validate a portfolio config against the engine schema.

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

from claude_agent_sdk import tool, create_sdk_mcp_server

MARKET_DB_PATH = Path(os.environ.get("MARKET_DB_PATH",
    str(Path(__file__).parent.parent / "data" / "market.db")))

# Filters — set by create_auto_trader_tools() at runtime
_STOP_DATE: str | None = None
_SECTOR: str | None = None

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
    Validate a full portfolio config: portfolio-level params + each sleeve's strategy.

    Returns:
        {"valid": True} or {"valid": False, "error": "description of what's wrong"}
    """
    try:
        # Portfolio-level checks
        if not isinstance(config, dict):
            return {"valid": False, "error": "Config must be a dict"}

        if "sleeves" not in config and "strategies" not in config:
            return {"valid": False, "error": "Missing 'sleeves' field"}

        sleeves = config.get("sleeves", config.get("strategies", []))

        if not isinstance(sleeves, list) or len(sleeves) == 0:
            return {"valid": False, "error": "sleeves must be a non-empty list"}

        if len(sleeves) > 5:
            return {"valid": False, "error": f"Too many sleeves ({len(sleeves)}). Maximum is 5."}

        # Check weights sum to ~1.0
        weights = [s.get("weight", 0) for s in sleeves]
        total_weight = sum(weights)
        if abs(total_weight - 1.0) > 0.01:
            return {"valid": False, "error": f"Sleeve weights sum to {total_weight:.2f}, must sum to 1.0"}

        # Check capital_when_gated_off
        capital_flow = config.get("capital_when_gated_off", config.get("capital_flow", "to_cash"))
        if capital_flow not in ("to_cash", "redistribute"):
            return {"valid": False, "error": f"Invalid capital_when_gated_off: '{capital_flow}'. Must be 'to_cash' or 'redistribute'."}

        # Validate each sleeve
        from backtest_engine import validate_strategy

        for i, sleeve in enumerate(sleeves):
            label = sleeve.get("label", f"sleeve_{i}")

            # Check weight
            w = sleeve.get("weight")
            if w is None or not isinstance(w, (int, float)) or w <= 0 or w > 1:
                return {"valid": False, "error": f"Sleeve '{label}': weight must be between 0 and 1, got {w}"}

            # Check regime_gate format
            rg = sleeve.get("regime_gate", ["*"])
            if not isinstance(rg, list):
                return {"valid": False, "error": f"Sleeve '{label}': regime_gate must be a list, got {type(rg).__name__}"}
            for gate in rg:
                if not isinstance(gate, str):
                    return {"valid": False, "error": f"Sleeve '{label}': regime_gate entries must be strings (regime IDs), got {type(gate).__name__}: {gate}"}

            # Check strategy config exists
            sc = sleeve.get("strategy_config", sleeve.get("config"))
            if not sc:
                if not sleeve.get("strategy_id") and not sleeve.get("config_path"):
                    return {"valid": False, "error": f"Sleeve '{label}': must have strategy_config, strategy_id, or config_path"}
                continue  # strategy_id or config_path — can't validate without DB lookup

            if not isinstance(sc, dict):
                return {"valid": False, "error": f"Sleeve '{label}': strategy_config must be a dict"}

            # Ensure backtest block exists (portfolio engine injects it later, but validate_strategy requires it)
            if "backtest" not in sc:
                sc["backtest"] = {
                    "start": "2015-01-01", "end": "2024-12-31",
                    "entry_price": "next_close", "slippage_bps": 10,
                }

            # Validate the strategy config
            try:
                validate_strategy(sc)
            except ValueError as e:
                return {"valid": False, "error": f"Sleeve '{label}': {str(e)}"}

        return {"valid": True}

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


def create_auto_trader_tools(stop_date: str | None = None, sector: str | None = None):
    """Create the MCP server with auto-trader tools.

    Args:
        stop_date: If set, silently filters all query results to dates <= stop_date.
        sector: If set, silently filters stock data to only this sector.
    """
    global _STOP_DATE, _SECTOR
    _STOP_DATE = stop_date
    _SECTOR = sector

    return create_sdk_mcp_server(
        name="auto_trader",
        version="1.0.0",
        tools=[query_market_data_tool, validate_portfolio_tool],
    )
