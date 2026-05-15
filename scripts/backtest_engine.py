#!/usr/bin/env python3
"""
AlphaScout Backtest Engine
==========================
Reads a strategy JSON config and simulates it over historical price data.

Usage:
    python3 backtest_engine.py strategies/defence_mean_reversion.json
    python3 backtest_engine.py strategies/defence_mean_reversion.json --start 2020-01-01 --end 2024-12-31
    python3 backtest_engine.py strategies/defence_mean_reversion.json --allocation 500000
"""

import os
import sys
import json
import hashlib
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from signals import (compute_rsi, compute_momentum_rank, compute_ma_crossover,
                     compute_volume_capitulation)


def compute_strategy_id(config: dict) -> str:
    """Deterministic ID from the core strategy parameters (excludes backtest dates/name)."""
    core = {k: config[k] for k in sorted(config) if k not in ("backtest", "name", "strategy_id")}
    return hashlib.sha256(json.dumps(core, sort_keys=True).encode()).hexdigest()[:12]


def stamp_strategy_id(config: dict) -> dict:
    """Ensure config has a strategy_id. Adds one if missing."""
    if "strategy_id" not in config:
        config["strategy_id"] = compute_strategy_id(config)
    return config


def _persist_strategy_config(config: dict) -> Path | None:
    """DEPRECATED — strategies are persisted to DB only.
    Kept as a no-op for backward compatibility with callers."""
    return None

# Add scripts dir to path for signals import
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from signals import (
    get_prices, find_period_drops, find_current_drops, find_daily_drops, find_selloffs, get_connection,
    find_revenue_breakouts, find_revenue_acceleration, find_margin_expansion,
    find_margin_turnaround, find_relative_outperformance, find_volume_conviction,
    find_revenue_deceleration, find_margin_collapse,
)


# ---------------------------------------------------------------------------
# Earnings Data
# ---------------------------------------------------------------------------
def load_earnings_data(symbols: list[str], conn) -> dict:
    """
    Load earnings beat/miss data.

    Returns:
        {symbol: {date: {"eps_actual": float, "eps_estimated": float, "beat": bool}}}
    """
    cur = conn.cursor()
    placeholders = ",".join("?" * len(symbols))
    cur.execute(
        f"SELECT symbol, date, eps_actual, eps_estimated "
        f"FROM earnings WHERE symbol IN ({placeholders}) "
        f"AND eps_actual IS NOT NULL AND eps_estimated IS NOT NULL",
        symbols,
    )

    earnings = defaultdict(dict)
    for sym, date, actual, estimated in cur.fetchall():
        earnings[sym][date] = {
            "eps_actual": actual,
            "eps_estimated": estimated,
            "beat": actual > estimated,
        }
    return dict(earnings)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
from db_config import MARKET_DB_PATH as DB_PATH
WORKSPACE = Path(os.environ.get("WORKSPACE", "/app"))
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))

# ---------------------------------------------------------------------------
# Strategy Loader & Validator
# ---------------------------------------------------------------------------
REQUIRED_FIELDS = ["name", "universe", "entry", "sizing", "backtest"]

DEFAULTS = {
    "stop_loss": None,
    "take_profit": None,
    "time_stop": None,
    "rebalancing": {"frequency": "none", "rules": {}},
    "sizing": {
        "type": "equal_weight",
        "max_positions": 10,
        "initial_allocation": 1000000,
    },
    "backtest": {
        "start": "2015-01-01",
        "end": "2025-12-31",
        "entry_price": "next_close",
        "slippage_bps": 10,
    },
}

def _calendar_to_trading_days(calendar_days: int) -> int:
    """Convert calendar days to approximate trading days (5 trading days per 7 calendar days)."""
    return max(1, round(calendar_days * 5 / 7))

VALID_TRIGGER_TYPES = [
    "period_drop", "current_drop", "daily_drop", "selloff", "earnings_momentum", "pe_percentile",
    "revenue_growth_yoy", "revenue_accelerating", "margin_expanding",
    "margin_turnaround", "relative_performance", "volume_conviction",
    "rsi", "momentum_rank", "ma_crossover", "volume_capitulation",
    "always",
    # Feature-table conditions (read from market.db features_daily)
    "feature_threshold", "feature_percentile", "days_to_earnings", "analyst_upgrades",
]

# Features available in features_daily.
# Reads pre-computed point-in-time feature values from market.db.features_daily.
# All features have ≥90% coverage from 2015-01-02 onward.
FEATURE_COLUMNS = (
    # Valuation (5)
    "pe", "ps", "p_b", "ev_ebitda", "ev_sales",
    # Yield (2)
    "fcf_yield", "div_yield",
    # Growth (2)
    "eps_yoy", "rev_yoy",
    # Quality — current margins (3)
    "gross_margin", "op_margin", "net_margin",
    # Quality — margin trajectory (op + net only; gross_margin_yoy_delta not ingested)
    "op_margin_yoy_delta", "net_margin_yoy_delta",
    # Growth acceleration (2)
    "rev_yoy_accel", "eps_yoy_accel",
    # Balance-sheet quality (3)
    "roe", "roic", "debt_to_equity",
    # Returns / momentum (5)
    "ret_1m", "ret_3m", "ret_6m", "ret_12m", "ret_12_1m",
    # Analyst flow (2)
    "analyst_net_upgrades_30d", "analyst_net_upgrades_90d",
    # Calendar / event (3)
    "days_since_last_earnings", "days_to_next_earnings", "pre_earnings_window_5d",
)
VALID_STOP_TYPES = ["drawdown_from_entry", "fundamental"]
VALID_TP_TYPES = ["gain_from_entry", "above_peak", "target_price"]
VALID_SIZING_TYPES = ["equal_weight", "risk_parity", "fixed_amount"]
VALID_REBAL_FREQ = ["none", "quarterly", "monthly", "on_earnings"]
VALID_ENTRY_PRICE = ["next_close", "next_open"]
VALID_ENTRY_PRIORITY = ["worst_drawdown", "random"]
VALID_REBAL_MODES = ["trim", "equal_weight"]


def get_config_schema() -> dict:
    """
    Authoritative strategy config schema — generated from the Pydantic models.

    Usage:
        python3 -c "from backtest_engine import get_config_schema; import json; print(json.dumps(get_config_schema(), indent=2))"
    """
    sys.path.insert(0, str(Path(__file__).parent.parent / "server"))
    from models.strategy import StrategyConfig
    return StrategyConfig.model_json_schema()


def load_strategy(path: str) -> dict:
    """Load a strategy config from a JSON file and validate it."""
    with open(path) as f:
        config = json.load(f)
    return validate_strategy(config)


def validate_strategy(config: dict) -> dict:
    """Validate and normalize a strategy config dict via the domain model.

    Handles backward compat (tickers→symbols, trigger/confirm→conditions, days→max_days)
    through the Pydantic model_validators, then returns a normalized dict with defaults applied.
    """
    sys.path.insert(0, str(Path(__file__).parent.parent / "server"))
    from pydantic import ValidationError
    from models.strategy import StrategyConfig

    try:
        validated = StrategyConfig.model_validate(config)
        # Dump back to dict — engine works with dicts, not model instances.
        # Use exclude_none=False so defaults (stop_loss=None, etc.) are present.
        result = validated.model_dump(mode="json")
        # Preserve extra fields the engine may have added (e.g. strategy_id from caller)
        for k, v in config.items():
            if k not in result:
                result[k] = v
        return result
    except ValidationError as e:
        # Convert Pydantic error to ValueError for backward compat with callers
        first = e.errors()[0]
        loc = " -> ".join(str(x) for x in first["loc"])
        raise ValueError(f"{loc}: {first['msg']}")


# ---------------------------------------------------------------------------
# Universe Resolution
# ---------------------------------------------------------------------------
def resolve_universe(config: dict, conn) -> list[str]:
    """Resolve the universe of tickers from config.

    Types:
      symbols  — explicit list from `universe.symbols`. No PIT semantics.
      sector   — symbols whose CURRENT sector profile matches. NOT PIT-aware;
                 historical sector classifications are not tracked.
      index    — PIT-aware membership of a major index (sp500 / nasdaq /
                 dowjones). Returns the UNION of every symbol that was a
                 member at any point during the backtest window, so the engine
                 can precompute signals for all of them. The per-day entry
                 scan then filters by as-of membership (engines use
                 `pit_members_by_date()` for this).
      all      — every symbol in the prices table.
    """
    universe_cfg = config["universe"]
    utype = universe_cfg.get("type", "symbols")
    exclude = set(universe_cfg.get("exclude", []))

    if utype == "symbols":
        symbols = universe_cfg.get("symbols", [])
    elif utype == "sector":
        sector = universe_cfg["sector"]
        # Prefer the DB-backed universe_profiles table — includes every name
        # we have profile data for (today's universe + the PIT backfill set).
        # Falls back to profile JSONs only if the DB is empty (dev env without
        # backfill done).
        symbols = _get_sector_symbols_from_db(sector, conn) or _get_sector_symbols(sector)
        # Optional PIT anchor: when set, intersect with ever-members of the
        # named index over the backtest window. Gives PIT-aware sector
        # universes (modulo the limitation that GICS sector itself is treated
        # as time-invariant — we use today's sector classification because
        # historical sector tags aren't tracked).
        anchor = universe_cfg.get("anchor_index")
        if anchor:
            from universe_history import ever_members
            bt = config.get("backtest", {})
            start = universe_cfg.get("start") or bt.get("start") or "2015-01-01"
            end = universe_cfg.get("end") or bt.get("end") or "2099-12-31"
            symbols = list(set(symbols) & ever_members(conn, anchor, start, end))
    elif utype == "index":
        from universe_history import ever_members
        idx = universe_cfg.get("index", "sp500")
        # Window: prefer explicit override on universe block, else fall back
        # to the strategy's backtest window.
        bt = config.get("backtest", {})
        start = universe_cfg.get("start") or bt.get("start") or "2015-01-01"
        end = universe_cfg.get("end") or bt.get("end") or "2099-12-31"
        symbols = sorted(ever_members(conn, idx, start, end))
    elif utype == "all":
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT symbol FROM prices ORDER BY symbol")
        symbols = [row[0] for row in cur.fetchall()]
    else:
        raise ValueError(f"Unknown universe type: {utype}")

    return sorted([s for s in symbols if s not in exclude])


def pit_members_by_date(config: dict, conn, dates: list[str]) -> dict[str, frozenset[str]] | None:
    """Precompute as-of membership for each trading date in `dates`.

    Returns a dict {date: frozenset(members)}, or None when the config's
    universe doesn't have PIT semantics (symbols / sector / all). The engine
    daily loop should:
       members_on = pit_members_by_date(config, conn, trading_dates)
       if members_on and symbol not in members_on[date]: skip entry candidate

    PIT is currently only available for type='index'. Sector universes don't
    have historical classification data, so they stay survivor-biased; mention
    this limitation in any user-facing summary.
    """
    universe_cfg = config.get("universe", {})
    if universe_cfg.get("type") != "index":
        return None
    from universe_history import members_as_of
    idx = universe_cfg.get("index", "sp500")
    # Build {date: frozenset} by replaying once per unique date. Cheap: dates
    # are O(years × 252) and members_as_of is O(events_after_date).
    out: dict[str, frozenset[str]] = {}
    # Optimization: dates are in ascending order. Compute change-event points
    # and bucket dates by which member-set applies. For simplicity (and given
    # 3000-day backtests run in milliseconds here), just call per date.
    for d in dates:
        out[d] = frozenset(members_as_of(conn, idx, d))
    return out


def _get_sector_symbols(sector: str) -> list[str]:
    """Get tickers for a sector from profile JSONs (legacy fallback)."""
    profile_dir = DATA_DIR / "universe" / "profiles"
    symbols = []
    for f in profile_dir.glob("*.json"):
        try:
            content = json.loads(f.read_text())
            data = content.get("data", [])
            profile = data[0] if isinstance(data, list) and data else data
            if profile.get("sector", "").lower() == sector.lower():
                symbols.append(f.stem)
            elif profile.get("industry", "").lower() == sector.lower():
                symbols.append(f.stem)
        except (json.JSONDecodeError, KeyError):
            continue
    return symbols


def _get_sector_symbols_from_db(sector: str, conn) -> list[str]:
    """Get tickers for a sector from the universe_profiles table.

    Includes the PIT backfill set (delisted historical index members whose
    profile rows we wrote during ingest_index_history + backfill_pit_members).
    Returns [] if the table is empty so the caller can fall back to the JSON
    path in dev environments without a backfill run.
    """
    rows = conn.execute(
        "SELECT symbol FROM universe_profiles "
        "WHERE LOWER(sector) = LOWER(?) OR LOWER(industry) = LOWER(?)",
        (sector, sector),
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Signal Pre-computation Cache (per-iteration memoization)
# ---------------------------------------------------------------------------
# precompute_condition is a pure function over (condition_config, symbols,
# start, end) — same inputs always produce the same output. The agent's
# research tools (rank_signals, evaluate_signal) make many calls per
# iteration with overlapping signal configs. Memoizing here gives a 60-80%
# reduction in research-phase wall-time at zero accuracy cost.
#
# Scope: per-iteration. The cache is cleared at the start of each agent
# iteration via clear_precompute_cache(), invoked by auto_trader/runner.py.
# Bounded memory; no cross-iteration drift if market data changes.
#
# Returned dicts are NOT deep-copied. By convention in this codebase,
# callers (signal_ranker, sleeve simulation) treat the precompute output
# as read-only — they iterate, never mutate. Saves 5-15ms per call.
_PRECOMPUTE_CACHE: dict = {}
_PRECOMPUTE_STATS: dict = {"hits": 0, "misses": 0}


def clear_precompute_cache() -> dict:
    """Reset the per-iteration memoization cache.

    Returns the cache stats from the prior iteration (hits, misses, entries)
    so the runner can log them. Safe to call before any iteration / process
    boundary; idempotent.

    Mutates the existing dict objects in place rather than reassigning the
    module globals. This keeps any external references valid (e.g., tests
    or instrumentation that imported the dict object) and avoids subtle
    "stale reference" bugs.
    """
    stats = {
        "hits": _PRECOMPUTE_STATS["hits"],
        "misses": _PRECOMPUTE_STATS["misses"],
        "entries": len(_PRECOMPUTE_CACHE),
    }
    _PRECOMPUTE_CACHE.clear()
    _PRECOMPUTE_STATS["hits"] = 0
    _PRECOMPUTE_STATS["misses"] = 0
    return stats


def _precompute_cache_key(condition_config: dict, symbols, start, end) -> str:
    """Deterministic cache key for memoization.

    Canonicalizes the condition_config dict (sorted keys, JSON dump) and
    the symbol list (sorted tuple) so semantically-identical inputs yield
    the same key regardless of insertion order.
    """
    cond_canon = json.dumps(condition_config, sort_keys=True, default=str)
    syms_canon = ",".join(sorted(symbols)) if symbols else ""
    return f"{cond_canon}|{syms_canon}|{start}|{end}"


def precompute_condition(condition_config: dict, symbols: list[str], conn, start: str, end: str,
                         earnings_data: dict = None, price_index: dict = None) -> dict:
    """
    Pre-compute one entry condition for all tickers, with per-iteration cache.

    Args:
        condition_config: Single condition configuration dict
        symbols: List of ticker symbols
        conn: Database connection
        start: Backtest start date
        end: Backtest end date
        earnings_data: Pre-loaded earnings data (for earnings_momentum condition)

    Returns:
        {symbol: {signal_date: metadata}} where metadata varies by condition type:
        - Price conditions: drawdown_pct (float)
        - earnings_momentum: {"beats": int, "avg_surprise": float, "no_recent_miss": bool}
    """
    key = _precompute_cache_key(condition_config, symbols, start, end)
    cached = _PRECOMPUTE_CACHE.get(key)
    if cached is not None:
        _PRECOMPUTE_STATS["hits"] += 1
        return cached
    _PRECOMPUTE_STATS["misses"] += 1
    result = _precompute_condition_uncached(
        condition_config, symbols, conn, start, end, earnings_data, price_index
    )
    _PRECOMPUTE_CACHE[key] = result
    return result


def _precompute_condition_uncached(condition_config: dict, symbols: list[str], conn, start: str, end: str,
                                   earnings_data: dict = None, price_index: dict = None) -> dict:
    """Underlying compute path — no cache. Use precompute_condition() in callers."""
    ctype = condition_config["type"]

    if ctype in ("current_drop", "period_drop", "daily_drop", "selloff"):
        return _precompute_price_condition(condition_config, symbols, conn, start, end, price_index=price_index)
    elif ctype == "earnings_momentum":
        return _precompute_earnings_momentum(condition_config, symbols, earnings_data or {}, start, end)
    elif ctype == "pe_percentile":
        return _precompute_pe_percentile(condition_config, symbols, conn, start, end, price_index=price_index)
    elif ctype in ("revenue_growth_yoy", "revenue_accelerating", "margin_expanding",
                    "margin_turnaround", "relative_performance", "volume_conviction"):
        return _precompute_fundamental_condition(condition_config, symbols, conn, start, end)
    elif ctype in ("rsi", "momentum_rank", "ma_crossover", "volume_capitulation"):
        return _precompute_technical_condition(condition_config, symbols, conn, start, end, price_index=price_index)
    elif ctype == "always":
        return _precompute_always_condition(symbols, conn, start, end, price_index=price_index)
    elif ctype == "feature_threshold":
        return _precompute_feature_threshold(condition_config, symbols, conn, start, end,
                                             price_index=price_index)
    elif ctype == "feature_percentile":
        return _precompute_feature_percentile(condition_config, symbols, conn, start, end,
                                              price_index=price_index)
    elif ctype == "days_to_earnings":
        return _precompute_days_to_earnings(condition_config, symbols, conn, start, end, price_index=price_index)
    elif ctype == "analyst_upgrades":
        return _precompute_analyst_upgrades(condition_config, symbols, conn, start, end, price_index=price_index)
    else:
        raise ValueError(f"Unknown condition type: {ctype}")


def _precompute_price_condition(condition_config: dict, symbols: list[str], conn,
                                start: str, end: str, price_index: dict = None) -> dict:
    """Pre-compute a price-based condition (current_drop, period_drop, daily_drop, selloff).

    If price_index is provided, uses it instead of querying DB per symbol.
    """
    ttype = condition_config["type"]
    signals = {}

    for symbol in symbols:
        # Use pre-loaded prices if available, otherwise query DB
        if price_index and symbol in price_index:
            prices = sorted(price_index[symbol].items())  # [(date, close), ...]
        else:
            prices = get_prices(symbol, conn=conn)
        if len(prices) < 20:
            continue

        if ttype == "period_drop":
            window_calendar = condition_config.get("window_days", 90)
            window_trading = _calendar_to_trading_days(window_calendar)
            threshold = condition_config.get("threshold", -15)
            raw = find_period_drops(prices, period_days=window_trading, threshold=threshold)
            signal_data = {}
            for r in raw:
                if start and r["signal_date"] < start:
                    continue
                if end and r["signal_date"] > end:
                    continue
                signal_data[r["signal_date"]] = r["drawdown_pct"]
            signals[symbol] = signal_data

        elif ttype == "current_drop":
            window_calendar = condition_config.get("window_days", 90)
            window_trading = _calendar_to_trading_days(window_calendar)
            threshold = condition_config.get("threshold", -15)
            raw = find_current_drops(prices, period_days=window_trading, threshold=threshold)
            signal_data = {}
            for r in raw:
                if start and r["signal_date"] < start:
                    continue
                if end and r["signal_date"] > end:
                    continue
                signal_data[r["signal_date"]] = r["drawdown_pct"]
            signals[symbol] = signal_data

        elif ttype == "daily_drop":
            threshold = condition_config.get("threshold", -5)
            events = find_daily_drops(prices, threshold=threshold)
            signal_data = {}
            for e in events:
                if start and e["date"] < start:
                    continue
                if end and e["date"] > end:
                    continue
                signal_data[e["date"]] = e["change_pct"]
            signals[symbol] = signal_data

        elif ttype == "selloff":
            threshold = condition_config.get("threshold", -20)
            peak_window = condition_config.get("peak_window", "all_time")
            selloffs_list = find_selloffs(prices, drop_threshold=threshold, peak_window=peak_window)
            signal_data = {}
            for s in selloffs_list:
                trigger_date = s.get("trigger_date")
                end_date = s.get("current_date", end)
                dd = s.get("drawdown_pct", threshold)
                if s["status"] == "recovered":
                    end_date = s["current_date"]
                for d, c in prices:
                    if d < trigger_date:
                        continue
                    if d > end_date:
                        break
                    if start and d < start:
                        continue
                    if end and d > end:
                        break
                    signal_data[d] = dd
            signals[symbol] = signal_data

    return signals


def _load_pe_timeseries(symbols: list[str]) -> dict:
    """
    Load quarterly PE timeseries from key-metrics JSON files.

    Returns:
        {symbol: [(date, pe), ...]}  sorted by date ascending.
        Only includes positive PE (profitable quarters).
    """
    result = {}
    for symbol in symbols:
        fpath = DATA_DIR / "metrics" / "key-metrics" / f"{symbol}.json"
        if not fpath.exists():
            continue
        try:
            raw = json.loads(fpath.read_text())
            records = raw.get("data", raw) if isinstance(raw, dict) else raw
            if not isinstance(records, list):
                continue
            entries = []
            for r in records:
                ey = r.get("earningsYield")
                d = r.get("date", "")[:10]
                if not d or not ey or ey <= 0:
                    continue  # skip negative/zero earnings
                pe = 1.0 / ey
                if pe > 0:
                    entries.append((d, pe))
            entries.sort(key=lambda x: x[0])
            if entries:
                result[symbol] = entries
        except (json.JSONDecodeError, KeyError, ZeroDivisionError):
            continue
    return result


def _precompute_always_condition(symbols: list[str], conn, start: str, end: str,
                                 price_index: dict = None) -> dict:
    """
    'always' condition: every ticker in the universe qualifies on every trading day.
    Used for buy-and-hold / equal-weight strategies where entry is unconditional.

    Returns:
        {symbol: {date: 0}} — signal value 0 (no drawdown context).
    """
    if price_index:
        all_dates = set()
        for s in symbols:
            if s in price_index:
                all_dates.update(d for d in price_index[s] if start <= d <= end)
        trading_dates = sorted(all_dates)
    else:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT date FROM prices WHERE date >= ? AND date <= ? ORDER BY date", (start, end))
        trading_dates = [row[0] for row in cur.fetchall()]

    signals = {}
    for symbol in symbols:
        signals[symbol] = {d: 0 for d in trading_dates}

    return signals


# ---------------------------------------------------------------------------
# Feature-table conditions (read from market.db features_daily)
# ---------------------------------------------------------------------------
_OPERATORS = {
    ">":  lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<":  lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


def _get_market_conn(conn):
    """Return a connection to market.db.

    The engine's `conn` may be app.db (where trades/portfolios live) or market.db
    (where prices/fundamentals/features live) depending on caller. If the given
    conn doesn't have features_daily (or is None), open market.db directly.
    """
    if conn is not None:
        try:
            conn.cursor().execute("SELECT 1 FROM features_daily LIMIT 1")
            return conn, False  # same connection, caller owns close
        except Exception:
            pass
    import sqlite3
    import os
    from pathlib import Path
    mk = os.environ.get("MARKET_DB_PATH") or str(Path(__file__).parent.parent / "data" / "market.db")
    return sqlite3.connect(mk), True  # new connection, must close


def _load_feature_series(feature: str, symbols: list[str], start: str, end: str, conn,
                         price_index: dict | None = None) -> dict:
    """Return {symbol: [(date, value), ...]} for `feature` over [start, end].

    Registry-aware:
      - precomputed → bulk SELECT from features_daily.
      - on_the_fly  → call FeatureDef.compute_series(symbol, prices) per symbol.
        price_index must be provided (the engine builds it once per backtest).

    Range is widened 1y before `start` so bisect-as-of can find a value for
    early trading days when the first in-window row hasn't been written yet.
    """
    from datetime import datetime, timedelta
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).parent.parent))
    from server.factors import get as _get_feature, feature_names as _feature_names

    if feature not in _feature_names():
        raise ValueError(f"Unknown feature: {feature}")
    fd = _get_feature(feature)
    pad_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")

    if fd.materialization == "on_the_fly":
        # Compute per-symbol from price history. Use price_index if given;
        # else fall back to loading closes from the DB.
        out: dict[str, list] = {}
        for sym in symbols:
            if price_index and sym in price_index:
                # price_index[sym] is {date: close}; convert to ascending list.
                prices = sorted(price_index[sym].items())
            else:
                mconn, owned = _get_market_conn(conn)
                try:
                    rows = mconn.execute(
                        "SELECT date, close FROM prices WHERE symbol=? "
                        "AND close IS NOT NULL ORDER BY date ASC",
                        (sym,),
                    ).fetchall()
                    prices = [(d, float(c)) for d, c in rows]
                finally:
                    if owned:
                        mconn.close()
            if not prices:
                continue
            series_map = fd.compute_series(sym, prices)
            if not series_map:
                continue
            # Filter to the padded window and convert to (date, value) ascending.
            pts = sorted(
                (d, v) for d, v in series_map.items()
                if pad_start <= d <= end and v is not None
            )
            if pts:
                out[sym] = pts
        return out

    # precomputed path — read from features_daily.
    mconn, owned = _get_market_conn(conn)
    try:
        cur = mconn.cursor()
        placeholders = ",".join("?" * len(symbols))
        q = (f"SELECT symbol, date, {feature} FROM features_daily "
             f"WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ? AND {feature} IS NOT NULL "
             f"ORDER BY symbol, date")
        rows = cur.execute(q, (*symbols, pad_start, end)).fetchall()
    finally:
        if owned:
            mconn.close()

    out = {}
    for sym, d, v in rows:
        out.setdefault(sym, []).append((d, v))
    return out


def _trading_dates(price_index, symbols, conn, start, end):
    """Union of trading dates in range from price_index (or DB fallback)."""
    if price_index:
        all_dates = set()
        for s in symbols:
            if s in price_index:
                all_dates.update(d for d in price_index[s] if start <= d <= end)
        return sorted(all_dates)
    mconn, owned = _get_market_conn(conn)
    try:
        rows = mconn.execute(
            "SELECT DISTINCT date FROM prices WHERE date >= ? AND date <= ? ORDER BY date",
            (start, end),
        ).fetchall()
    finally:
        if owned:
            mconn.close()
    return [r[0] for r in rows]


def _precompute_feature_threshold(cond: dict, symbols: list[str], conn, start: str, end: str,
                                  price_index: dict | None = None) -> dict:
    """Signal fires on each trading day where the as-of feature value passes operator/value."""
    from bisect import bisect_right

    feature = cond["feature"]
    operator = cond.get("operator", ">=")
    target = cond["value"]
    op_fn = _OPERATORS.get(operator)
    if op_fn is None:
        raise ValueError(f"Unknown operator: {operator}")

    series = _load_feature_series(feature, symbols, start, end, conn, price_index=price_index)
    # Use each symbol's own feature dates as "trading days" — dense: one row per trading day.
    signals: dict = {}
    for sym, pts in series.items():
        dates = [p[0] for p in pts]
        values = [p[1] for p in pts]
        sig: dict = {}
        for d, v in zip(dates, values):
            if d < start or d > end:
                continue
            if op_fn(v, target):
                sig[d] = v
        if sig:
            signals[sym] = sig
    return signals


def _precompute_feature_percentile(cond: dict, symbols: list[str], conn, start: str, end: str,
                                   price_index: dict | None = None) -> dict:
    """For each trading day, rank symbols by `feature` and emit signal for the bottom N% (or top, if max_percentile > 50 feels natural as-is)."""
    from bisect import bisect_right
    from collections import defaultdict

    feature = cond["feature"]
    max_pct = float(cond.get("max_percentile", 30))
    scope = cond.get("scope", "universe")
    min_value = cond.get("min_value")
    max_value = cond.get("max_value")

    series = _load_feature_series(feature, symbols, start, end, conn, price_index=price_index)
    if not series:
        return {}

    # Sector lookup if scoped
    sector_of: dict[str, str] = {}
    if scope == "sector":
        mconn, owned = _get_market_conn(conn)
        try:
            cur = mconn.cursor()
            placeholders = ",".join("?" * len(symbols))
            try:
                rows = cur.execute(
                    f"SELECT symbol, sector FROM universe_profiles WHERE symbol IN ({placeholders})",
                    symbols,
                ).fetchall()
                sector_of = {s: sec or "UNKNOWN" for s, sec in rows}
            except Exception:
                sector_of = {}
        finally:
            if owned:
                mconn.close()

    # Build per-symbol date→value for bisect
    per_sym = {s: ([p[0] for p in pts], [p[1] for p in pts]) for s, pts in series.items()}

    trading_dates = _trading_dates(None, symbols, conn, start, end)

    signals: dict = defaultdict(dict)
    for d in trading_dates:
        # Collect latest-as-of value per symbol
        snap: dict[str, float] = {}
        for sym, (dates, values) in per_sym.items():
            idx = bisect_right(dates, d) - 1
            if idx < 0:
                continue
            v = values[idx]
            if min_value is not None and v < min_value:
                continue
            if max_value is not None and v > max_value:
                continue
            snap[sym] = v

        if len(snap) < 3:
            continue

        if scope == "sector":
            buckets: dict[str, list] = defaultdict(list)
            for sym, v in snap.items():
                buckets[sector_of.get(sym, "UNKNOWN")].append((sym, v))
            for sec, items in buckets.items():
                if len(items) < 3:
                    continue
                items.sort(key=lambda x: x[1])
                cutoff = max(1, int(len(items) * max_pct / 100))
                for sym, v in items[:cutoff]:
                    signals[sym][d] = v
        else:
            ranked = sorted(snap.items(), key=lambda x: x[1])
            cutoff = max(1, int(len(ranked) * max_pct / 100))
            for sym, v in ranked[:cutoff]:
                signals[sym][d] = v

    return dict(signals)


def _precompute_days_to_earnings(cond: dict, symbols: list[str], conn, start: str, end: str,
                                 price_index: dict = None) -> dict:
    """Signal on trading days where the next upcoming earnings event is [min_days, max_days] away."""
    from bisect import bisect_left
    from datetime import datetime

    min_days = int(cond.get("min_days", 0))
    max_days = int(cond.get("max_days", 7))

    mconn, owned = _get_market_conn(conn)
    try:
        cur = mconn.cursor()
        placeholders = ",".join("?" * len(symbols))
        # Pull ALL earnings events in window — at backtest time, any event with
        # event_date > today counts as upcoming, regardless of whether eps_actual
        # was later filled in. The live-only "IS NULL" filter would be wrong here.
        from datetime import datetime as _dt, timedelta as _td
        pad_end = (_dt.strptime(end, "%Y-%m-%d") + _td(days=max_days + 1)).strftime("%Y-%m-%d")
        rows = cur.execute(
            f"SELECT symbol, date FROM earnings WHERE symbol IN ({placeholders}) "
            f"AND date >= ? AND date <= ? ORDER BY symbol, date",
            (*symbols, start, pad_end),
        ).fetchall()
    finally:
        if owned:
            mconn.close()

    upcoming: dict[str, list[str]] = {}
    for sym, d in rows:
        upcoming.setdefault(sym, []).append(d)

    trading_dates = _trading_dates(price_index, symbols, conn, start, end)
    if not trading_dates:
        return {}

    # Cache datetime parses
    td_parsed = [datetime.strptime(d, "%Y-%m-%d") for d in trading_dates]

    signals: dict = {}
    for sym, event_dates in upcoming.items():
        if not event_dates:
            continue
        ev_parsed = [datetime.strptime(d, "%Y-%m-%d") for d in event_dates]
        sig: dict = {}
        for td, td_dt in zip(trading_dates, td_parsed):
            # Find the next event strictly after today (or ≥ today: include day-of)
            idx = bisect_left(event_dates, td)
            if idx >= len(event_dates):
                continue
            delta = (ev_parsed[idx] - td_dt).days
            if min_days <= delta <= max_days:
                sig[td] = delta
        if sig:
            signals[sym] = sig
    return signals


def _precompute_analyst_upgrades(cond: dict, symbols: list[str], conn, start: str, end: str,
                                 price_index: dict = None) -> dict:
    """Signal when net (upgrades - downgrades) in trailing window >= min_net_upgrades."""
    from datetime import datetime, timedelta

    window_days = int(cond.get("window_days", 90))
    min_net = int(cond.get("min_net_upgrades", 2))

    mconn, owned = _get_market_conn(conn)
    try:
        cur = mconn.cursor()
        # Widen lookback by window_days so the first trading day has full history
        from datetime import datetime as _dt
        pad_start = (_dt.strptime(start, "%Y-%m-%d") - timedelta(days=window_days)).strftime("%Y-%m-%d")
        placeholders = ",".join("?" * len(symbols))
        rows = cur.execute(
            f"SELECT symbol, date, action FROM analyst_grades WHERE symbol IN ({placeholders}) "
            f"AND action IN ('upgrade','downgrade') AND date >= ? AND date <= ? ORDER BY symbol, date",
            (*symbols, pad_start, end),
        ).fetchall()
    finally:
        if owned:
            mconn.close()

    # Bucket per symbol: sorted list of (date_dt, +1 or -1)
    events: dict[str, list] = {}
    for sym, d, action in rows:
        sign = 1 if action == "upgrade" else -1
        events.setdefault(sym, []).append((datetime.strptime(d, "%Y-%m-%d"), sign))

    trading_dates = _trading_dates(price_index, symbols, conn, start, end)
    if not trading_dates:
        return {}

    td_parsed = [datetime.strptime(d, "%Y-%m-%d") for d in trading_dates]

    signals: dict = {}
    for sym, ev in events.items():
        # Two-pointer sliding window
        sig: dict = {}
        lo, hi = 0, 0
        net = 0
        for td_str, td_dt in zip(trading_dates, td_parsed):
            window_start = td_dt - timedelta(days=window_days)
            # Advance hi to include all events with date <= td_dt
            while hi < len(ev) and ev[hi][0] <= td_dt:
                net += ev[hi][1]
                hi += 1
            # Advance lo to exclude events older than window_start
            while lo < hi and ev[lo][0] < window_start:
                net -= ev[lo][1]
                lo += 1
            if net >= min_net:
                sig[td_str] = net
        if sig:
            signals[sym] = sig
    return signals


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------
VALID_RANKING_METRICS = [
    "pe_percentile", "current_drop", "rsi", "momentum_rank",
    "revenue_growth_yoy", "margin_expanding",
    # features_daily columns — any of these is a valid ranking metric
    *FEATURE_COLUMNS,
]

DEFAULT_RANKING_METRIC = "pe_percentile"
DEFAULT_RANKING_ORDER = "asc"


def _compute_composite_score(
    symbols: list[str],
    conn,
    date: str,
    price_index: dict,
    composite_config: dict,
    preloaded_series: dict | None = None,
) -> dict:
    """Multi-factor continuous composite score.

    score(s) = Σ_b (weight_b / Σ_w) · mean_{f ∈ b}( sign_f · z(f, s, t) )

    where z is computed cross-sectionally across the given `symbols` (the
    post-entry-filter candidate set) at date `t`. Default standardization is
    rank-normalized; alternative is plain z (mean/stdev).

    NaN handling:
      - A stock missing a single factor in a bucket: bucket z is averaged
        over the factor(s) it does have.
      - A stock missing EVERY factor in a bucket: that bucket contributes 0.
      - A stock missing every factor in every bucket: returned with NaN
        score (sorted to the end by the caller).

    `preloaded_series` (optional): {factor_name: {symbol: [(date, value)...]}}
    pre-loaded ONCE at run_backtest startup. When provided, the per-day DB
    query per factor is skipped. The bisect-as-of lookup is unchanged, so
    the result is byte-identical to the un-cached path.

    Returns {symbol: score}. Higher = better when the agent's intended
    direction has been encoded in factor signs.
    """
    import numpy as np
    from bisect import bisect_right

    buckets = composite_config.get("buckets") or {}
    if not buckets:
        return {}
    standardization = composite_config.get("standardization", "rank")
    # Normalize bucket weights to sum to 1.
    raw_w = {name: float(b.get("weight", 1.0)) for name, b in buckets.items()}
    total_w = sum(raw_w.values()) or 1.0
    weights = {name: raw_w[name] / total_w for name in raw_w}

    # Collect the set of distinct factors so we load each only once.
    factor_set: set[str] = set()
    for b in buckets.values():
        for f in b.get("factors", []):
            factor_set.add(f["name"] if isinstance(f, dict) else f.name)
    if not factor_set:
        return {}

    # Load feature values for each factor, as-of bisect at `date`.
    # Result: {factor_name: {symbol: value}}
    values_by_factor: dict[str, dict[str, float]] = {}
    # 365-day staleness cap: matches the per-day _load_feature_series window
    # so the preload path produces byte-identical results. Without this, a
    # full-range preload would let stale (>365d-old) factor values bleed in
    # for query dates near bt_start, while the per-day fallback would reject
    # them. Enforcing the cap here keeps the two code paths in lockstep.
    from datetime import datetime as _dt, timedelta as _td
    _staleness_cutoff = (_dt.strptime(date, "%Y-%m-%d") - _td(days=365)).strftime("%Y-%m-%d")

    # Convert candidate `symbols` to a set for O(1) membership checks below.
    # The preloaded series is keyed by the FULL UNIVERSE; the per-day SQL
    # path is keyed by the candidate subset passed in. Restricting the
    # preload iteration to `symbols` keeps the cross-sectional
    # standardization base identical between paths.
    _symbols_set = set(symbols)

    for fname in factor_set:
        # Prefer the preloaded full-range series when available. Falls back
        # to a per-call _load_feature_series for direct callers (unit tests,
        # ad-hoc usage) that don't preload.
        if preloaded_series is not None and fname in preloaded_series:
            series = preloaded_series[fname]
            _use_staleness_cap = True
        else:
            try:
                series = _load_feature_series(fname, symbols, date, date, conn,
                                              price_index=price_index)
            except Exception:
                values_by_factor[fname] = {}
                continue
            # Per-call path already has the 365d window baked into its SQL.
            _use_staleness_cap = False

        per_sym: dict[str, float] = {}
        for sym, pts in series.items():
            # When iterating the preloaded series, skip symbols that
            # aren't in the candidate subset for this ranking call.
            if _use_staleness_cap and sym not in _symbols_set:
                continue
            if not pts:
                continue
            dates_only = [p[0] for p in pts]
            idx = bisect_right(dates_only, date) - 1
            if idx >= 0:
                v = pts[idx][1]
                # Drop values older than the 365d cap when using the preload
                # series, so it matches what per-day SQL would return.
                if _use_staleness_cap and dates_only[idx] < _staleness_cutoff:
                    continue
                if v is not None and np.isfinite(v):
                    per_sym[sym] = float(v)
        values_by_factor[fname] = per_sym

    # Cross-sectional standardization per factor (rank or z), over the
    # candidate set passed in. Stocks missing the factor are not in z_by_factor[f].
    z_by_factor: dict[str, dict[str, float]] = {}
    for fname, per_sym in values_by_factor.items():
        if len(per_sym) < 2:
            z_by_factor[fname] = {}
            continue
        syms = list(per_sym.keys())
        vals = np.array([per_sym[s] for s in syms], dtype=np.float64)
        if standardization == "rank":
            # Average-rank → percentile → z-equivalent (approximately N(0,1))
            order = np.argsort(vals, kind="mergesort")
            ranks = np.empty_like(order, dtype=np.float64)
            # Use average ranks for ties (same as scipy.stats.rankdata 'average')
            n = len(vals)
            i = 0
            sorted_vals = vals[order]
            while i < n:
                j = i + 1
                while j < n and sorted_vals[j] == sorted_vals[i]:
                    j += 1
                avg_rank = (i + j - 1) / 2.0 + 1.0  # 1-based
                ranks[order[i:j]] = avg_rank
                i = j
            # Percentile in (0, 1), then map to z ~ N(0, 1) via inverse normal.
            # Use simple (rank - 0.5) / n to avoid 0 or 1.
            pct = (ranks - 0.5) / n
            # Equivalent z via inverse normal — but we don't need scipy here.
            # Use sqrt(2) * erfinv(2*pct - 1). For audit purposes use a direct
            # rank-based standardization that's bit-exact reproducible: scale
            # ranks to [-1, +1] then to unit std. Skip the inv-normal mapping
            # since the relative ordering is what ultimately drives ranking.
            centered = ranks - (n + 1) / 2.0   # mean 0
            scale = np.std(centered, ddof=0)
            if scale == 0:
                z_vals = np.zeros_like(centered)
            else:
                z_vals = centered / scale
        else:  # 'z': plain mean/stdev
            mu = float(vals.mean())
            sd = float(vals.std(ddof=0))
            if sd == 0:
                z_vals = np.zeros_like(vals)
            else:
                z_vals = (vals - mu) / sd
        z_by_factor[fname] = {syms[k]: float(z_vals[k]) for k in range(len(syms))}

    # Compose: per-bucket mean over (sign · z); cross-bucket weighted sum.
    scores: dict[str, float] = {}
    for sym in symbols:
        composite = 0.0
        any_factor_seen = False
        for b_name, b in buckets.items():
            bucket_factors = b.get("factors", [])
            bucket_zs: list[float] = []
            for f in bucket_factors:
                if isinstance(f, dict):
                    fname = f["name"]
                    sign = f.get("sign", "+")
                else:
                    fname = f.name
                    sign = f.sign
                z = z_by_factor.get(fname, {}).get(sym)
                if z is None:
                    continue
                bucket_zs.append(-z if sign == "-" else z)
            if not bucket_zs:
                continue  # entire bucket missing → contributes 0
            any_factor_seen = True
            bucket_z = sum(bucket_zs) / len(bucket_zs)
            composite += weights[b_name] * bucket_z
        if any_factor_seen:
            scores[sym] = composite
    return scores


def _compute_ranking_scores(metric: str, symbols: list[str], conn, date: str,
                            price_index: dict, pe_series: dict = None,
                            rsi_cache: dict = None,
                            composite_config: dict | None = None,
                            composite_series: dict | None = None) -> dict:
    """
    Compute a single ranking score per symbol for a given date.

    Returns:
        {symbol: score} — lower score = better for asc, higher = better for desc.
    """
    from bisect import bisect_right

    scores = {}

    if metric == "composite_score":
        if composite_config is None:
            return {}
        return _compute_composite_score(symbols, conn, date, price_index, composite_config,
                                         preloaded_series=composite_series)

    if metric == "pe_percentile":
        if pe_series is None:
            pe_series = _load_pe_timeseries(symbols)
        for symbol in symbols:
            series = pe_series.get(symbol)
            if not series:
                continue
            dates_only = [s[0] for s in series]
            idx = bisect_right(dates_only, date) - 1
            if idx >= 0:
                _, pe = series[idx]
                if 0 < pe < 500:
                    scores[symbol] = pe
    
    elif metric == "current_drop":
        for symbol in symbols:
            prices = price_index.get(symbol, {})
            sorted_dates = sorted(d for d in prices if d <= date)
            if len(sorted_dates) < 20:
                continue
            lookback = sorted_dates[-63:]  # ~3 months
            peak = max(prices[d] for d in lookback)
            current = prices.get(date)
            if current and peak > 0:
                scores[symbol] = ((current - peak) / peak) * 100  # negative = more beaten down
    
    elif metric == "rsi":
        if rsi_cache:
            for symbol in symbols:
                rsi_val = rsi_cache.get(symbol, {}).get(date)
                if rsi_val is not None:
                    scores[symbol] = rsi_val
        else:
            for symbol in symbols:
                try:
                    rsi_series = compute_rsi(symbol, period=14, start=date, end=date, conn=conn)
                    if date in rsi_series:
                        scores[symbol] = rsi_series[date]
                except Exception:
                    continue
    
    elif metric == "momentum_rank":
        rank_data = compute_momentum_rank(symbols, lookback=63, start=date, end=date, conn=conn)
        for symbol in symbols:
            val = rank_data.get(symbol, {}).get(date)
            if val is not None:
                scores[symbol] = val
    
    elif metric == "revenue_growth_yoy":
        for symbol in symbols:
            fpath = DATA_DIR / "fundamentals" / "income-growth" / f"{symbol}.json"
            if not fpath.exists():
                continue
            try:
                raw = json.loads(fpath.read_text())
                records = raw if isinstance(raw, list) else raw.get("data", [])
                # Find most recent quarter before date
                for r in sorted(records, key=lambda x: x.get("date", ""), reverse=True):
                    rd = r.get("date", "")[:10]
                    if rd <= date:
                        val = r.get("growthRevenue")
                        if val is not None:
                            scores[symbol] = val
                        break
            except (json.JSONDecodeError, KeyError):
                continue
    
    elif metric == "margin_expanding":
        for symbol in symbols:
            fpath = DATA_DIR / "fundamentals" / "income" / f"{symbol}.json"
            if not fpath.exists():
                continue
            try:
                raw = json.loads(fpath.read_text())
                records = raw if isinstance(raw, list) else raw.get("data", [])
                quarters = sorted(
                    [r for r in records if r.get("period", "").startswith("Q") and r.get("date", "")[:10] <= date],
                    key=lambda x: x["date"],
                    reverse=True
                )
                if len(quarters) >= 2:
                    curr_margin = (quarters[0].get("netIncome", 0) / quarters[0].get("revenue", 1)) if quarters[0].get("revenue") else 0
                    prev_margin = (quarters[1].get("netIncome", 0) / quarters[1].get("revenue", 1)) if quarters[1].get("revenue") else 0
                    scores[symbol] = (curr_margin - prev_margin) * 100  # margin change in pct points
            except (json.JSONDecodeError, KeyError, ZeroDivisionError):
                continue

    elif metric in FEATURE_COLUMNS:
        # Any features_daily column is a valid ranker. As-of lookup per symbol.
        series = _load_feature_series(metric, symbols, date, date, conn)
        for symbol, pts in series.items():
            # _load_feature_series pads 1y before start; most recent <= date wins
            dates_only = [p[0] for p in pts]
            idx = bisect_right(dates_only, date) - 1
            if idx >= 0:
                scores[symbol] = pts[idx][1]

    return scores


def rank_candidates(candidates: list[tuple], config: dict, conn, date: str,
                    price_index: dict, pe_series: dict = None,
                    rsi_cache: dict = None,
                    composite_series: dict | None = None) -> list[tuple]:
    """
    Rank entry candidates by the configured ranking metric.

    Args:
        candidates: [(symbol, drawdown)] from signal matching
        config: Strategy config dict
        conn: DB connection
        date: Current trading date
        price_index: {symbol: {date: price}}
        pe_series: Pre-loaded PE timeseries (optional, for pe_percentile)
        rsi_cache: Pre-loaded RSI cache (optional)
        composite_series: Pre-loaded composite-factor timeseries (optional,
            for composite_score). Skips a per-day DB query per factor.

    Returns:
        Sorted candidates list.
    """
    ranking_config = config.get("ranking")
    symbols_in_play = [c[0] for c in candidates]

    if not ranking_config:
        # Default: pe_percentile asc when more candidates than slots
        metric = DEFAULT_RANKING_METRIC
        order = DEFAULT_RANKING_ORDER
    else:
        metric = ranking_config.get("by", DEFAULT_RANKING_METRIC)
        order = ranking_config.get("order", DEFAULT_RANKING_ORDER)

    composite_config = config.get("composite_score") if metric == "composite_score" else None
    scores = _compute_ranking_scores(metric, symbols_in_play, conn, date,
                                      price_index, pe_series, rsi_cache,
                                      composite_config=composite_config,
                                      composite_series=composite_series)
    
    if not scores:
        # Fallback to original ordering if ranking data unavailable
        return candidates
    
    # Sort: asc = lowest first, desc = highest first
    reverse = (order == "desc")
    
    # Candidates with scores get sorted; those without go to the end
    scored = [(sym, dd) for sym, dd in candidates if sym in scores]
    unscored = [(sym, dd) for sym, dd in candidates if sym not in scores]
    
    scored.sort(key=lambda x: scores[x[0]], reverse=reverse)
    
    return scored + unscored


def _precompute_pe_percentile(condition_config: dict, symbols: list[str], conn, start: str, end: str,
                              price_index: dict = None) -> dict:
    """
    Pre-compute PE percentile ranking condition.

    For each trading day, ranks all universe tickers by their most recent
    quarterly PE (from key-metrics earningsYield).  Tickers in the bottom
    ``max_percentile`` (cheapest) get a signal.

    Params (in condition_config):
        max_percentile (float, default 30): percentile cutoff (0-100).
            Bottom 30 = cheapest 30% of the sector.
        min_pe (float, default 0): minimum PE to consider (filters
            near-zero PE from one-off earnings spikes).
        max_pe (float, default 500): cap to exclude outliers.
    """
    from bisect import bisect_right

    max_percentile = condition_config.get("max_percentile", 30)
    min_pe = condition_config.get("min_pe", 0)
    max_pe = condition_config.get("max_pe", 500)

    # Load quarterly PE for all universe tickers
    pe_series = _load_pe_timeseries(symbols)
    if not pe_series:
        print("  WARNING: No PE data found for pe_percentile condition")
        return {}

    print(f"  PE data loaded for {len(pe_series)}/{len(symbols)} tickers")

    # Get trading dates from shared price index or DB
    if price_index:
        all_dates = set()
        for s in symbols:
            if s in price_index:
                all_dates.update(d for d in price_index[s] if start <= d <= end)
        trading_dates = sorted(all_dates)
    else:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT date FROM prices WHERE date >= ? AND date <= ? ORDER BY date", (start, end))
        trading_dates = [row[0] for row in cur.fetchall()]

    # For each trading day, find most recent PE for each ticker, rank, signal bottom N%
    signals = defaultdict(dict)

    for date in trading_dates:
        # Collect most recent PE for each ticker as of this date
        pe_snapshot = {}
        for symbol, series in pe_series.items():
            # Binary search for most recent quarter <= date
            dates_only = [s[0] for s in series]
            idx = bisect_right(dates_only, date) - 1
            if idx >= 0:
                _, pe = series[idx]
                if min_pe <= pe <= max_pe:
                    pe_snapshot[symbol] = pe

        if len(pe_snapshot) < 3:
            continue  # Need enough tickers to rank

        # Rank by PE ascending (lowest = cheapest)
        ranked = sorted(pe_snapshot.items(), key=lambda x: x[1])
        cutoff_idx = max(1, int(len(ranked) * max_percentile / 100))

        for symbol, pe in ranked[:cutoff_idx]:
            signals[symbol][date] = pe  # store PE as the signal value

    return dict(signals)


def _precompute_earnings_momentum(condition_config: dict, symbols: list[str], earnings_data: dict, start: str, end: str) -> dict:
    """
    Pre-compute earnings momentum condition.
    
    For each symbol, computes rolling earnings statistics after each earnings report
    and expands into date ranges where the condition passes.
    """
    from datetime import datetime, timedelta
    
    lookback_quarters = condition_config.get("lookback_quarters", 4)
    min_beats = condition_config.get("min_beats", 2)
    min_avg_surprise_pct = condition_config.get("min_avg_surprise_pct")
    no_recent_miss = condition_config.get("no_recent_miss", False)
    
    signals = {}
    
    for symbol in symbols:
        if symbol not in earnings_data:
            continue
            
        sym_earnings = earnings_data[symbol]
        if not sym_earnings:
            continue
            
        # Sort earnings by date
        sorted_earnings = sorted(sym_earnings.items())
        signal_data = {}
        
        for i, (earn_date, earn_data) in enumerate(sorted_earnings):
            # Look back at last N quarters (including current)
            start_idx = max(0, i - lookback_quarters + 1)
            lookback_earnings = sorted_earnings[start_idx:i+1]
            
            if len(lookback_earnings) < min(2, lookback_quarters):  # Need at least 2 quarters of data
                continue
                
            # Compute rolling stats
            beats = sum(1 for _, ed in lookback_earnings if ed["beat"])
            total_quarters = len(lookback_earnings)
            
            # Average surprise percentage
            surprises = []
            for _, ed in lookback_earnings:
                if ed["eps_estimated"] != 0:
                    surprise_pct = ((ed["eps_actual"] - ed["eps_estimated"]) / abs(ed["eps_estimated"])) * 100
                    surprises.append(surprise_pct)
            avg_surprise = sum(surprises) / len(surprises) if surprises else 0
            
            # Check no recent miss (most recent quarter must be a beat)
            most_recent_beat = lookback_earnings[-1][1]["beat"] if lookback_earnings else False
            
            # Apply filters
            if beats < min_beats:
                continue
            if min_avg_surprise_pct is not None and avg_surprise < min_avg_surprise_pct:
                continue
            if no_recent_miss and not most_recent_beat:
                continue
                
            # Signal is active from this earnings date until next earnings date (or end of backtest)
            signal_start = earn_date
            
            # Find next earnings date for this symbol
            next_earn_date = None
            if i + 1 < len(sorted_earnings):
                next_earn_date = sorted_earnings[i + 1][0]
            
            # Determine signal end date
            if next_earn_date:
                # Convert to datetime for date arithmetic
                signal_end_dt = datetime.strptime(next_earn_date, "%Y-%m-%d") - timedelta(days=1)
                signal_end = signal_end_dt.strftime("%Y-%m-%d")
            else:
                signal_end = end  # Use backtest end date
                
            # Generate all trading dates in this range where condition is active
            # We'll use a simple approach: generate daily dates and let the main loop filter to trading dates
            current_dt = datetime.strptime(signal_start, "%Y-%m-%d")
            end_dt = datetime.strptime(min(signal_end, end), "%Y-%m-%d")
            
            while current_dt <= end_dt:
                date_str = current_dt.strftime("%Y-%m-%d")
                if date_str >= start:  # Only include dates in backtest range
                    signal_data[date_str] = {
                        "beats": beats,
                        "avg_surprise": round(avg_surprise, 2),
                        "no_recent_miss": most_recent_beat,
                        "quarters_analyzed": total_quarters
                    }
                current_dt += timedelta(days=1)
        
        if signal_data:
            signals[symbol] = signal_data
    
    return signals


def _precompute_fundamental_condition(condition_config: dict, symbols: list[str], conn, start: str, end: str) -> dict:
    """
    Pre-compute fundamental (non-price) conditions for all tickers.
    
    Supports: revenue_growth_yoy, revenue_accelerating, margin_expanding,
              margin_turnaround, relative_performance, volume_conviction.
    
    Fundamental signals fire on filingDate (when market learns the data) and remain
    active until the next quarterly filing date (or end of backtest).
    """
    ctype = condition_config["type"]
    signals = {}
    
    for symbol in symbols:
        try:
            if ctype == "revenue_growth_yoy":
                raw = find_revenue_breakouts(
                    symbol,
                    threshold=condition_config.get("threshold", 50.0),
                    start=start, end=end,
                    conn=conn,
                )
            elif ctype == "revenue_accelerating":
                raw = find_revenue_acceleration(
                    symbol,
                    min_quarters=condition_config.get("min_quarters", 2),
                    start=start, end=end,
                    conn=conn,
                )
            elif ctype == "margin_expanding":
                raw = find_margin_expansion(
                    symbol,
                    metric=condition_config.get("metric", "net_margin"),
                    min_quarters=condition_config.get("min_quarters", 2),
                    start=start, end=end,
                    conn=conn,
                )
            elif ctype == "margin_turnaround":
                raw = find_margin_turnaround(
                    symbol,
                    metric=condition_config.get("metric", "net_margin"),
                    threshold_bps=condition_config.get("threshold_bps", 1000.0),
                    min_quarters=condition_config.get("min_quarters", 2),
                    start=start, end=end,
                    conn=conn,
                )
            elif ctype == "relative_performance":
                raw = find_relative_outperformance(
                    symbol,
                    benchmark_path=condition_config.get("benchmark_path", None),
                    threshold=condition_config.get("threshold", 20.0),
                    window_days=condition_config.get("window_days", 126),
                    start=start, end=end,
                    conn=conn,
                )
            elif ctype == "volume_conviction":
                raw = find_volume_conviction(
                    symbol,
                    short_window=condition_config.get("short_window", 60),
                    long_window=condition_config.get("long_window", 252),
                    ratio_threshold=condition_config.get("ratio", 0.8),
                    start=start, end=end,
                    conn=conn,
                )
            else:
                raw = []
        except Exception as e:
            raw = []
        
        if not raw:
            continue
        
        signal_data = {}
        
        if ctype in ("relative_performance", "volume_conviction"):
            # Daily signals — already have one entry per date
            for entry in raw:
                signal_data[entry["signal_date"]] = {
                    k: v for k, v in entry.items() if k != "signal_date"
                }
        else:
            # Quarterly signals — expand from filing date to next filing date
            for i, entry in enumerate(raw):
                sig_start = entry["signal_date"]
                # Find next signal's filing date (or +100 days as proxy for next quarter)
                if i + 1 < len(raw):
                    sig_end_dt = datetime.strptime(raw[i + 1]["signal_date"], "%Y-%m-%d") - timedelta(days=1)
                else:
                    sig_end_dt = datetime.strptime(sig_start, "%Y-%m-%d") + timedelta(days=100)
                
                sig_end = min(sig_end_dt.strftime("%Y-%m-%d"), end)
                
                metadata = {k: v for k, v in entry.items() if k != "signal_date"}
                
                current_dt = datetime.strptime(sig_start, "%Y-%m-%d")
                end_dt = datetime.strptime(sig_end, "%Y-%m-%d")
                while current_dt <= end_dt:
                    date_str = current_dt.strftime("%Y-%m-%d")
                    if date_str >= start:
                        signal_data[date_str] = metadata
                    current_dt += timedelta(days=1)
        
        if signal_data:
            signals[symbol] = signal_data
    
    return signals


def _precompute_technical_condition(condition_config: dict, symbols: list[str], conn, start: str, end: str,
                                    price_index: dict = None) -> dict:
    """
    Pre-compute technical indicator conditions: rsi, momentum_rank, ma_crossover, volume_capitulation.

    Config examples:
        {"type": "rsi", "period": 14, "operator": "<=", "value": 30}
        {"type": "momentum_rank", "lookback": 63, "operator": ">=", "value": 75}
        {"type": "ma_crossover", "fast": 50, "slow": 200, "operator": "==", "value": 1}
        {"type": "volume_capitulation", "window": 20, "multiplier": 3.0}

    Returns:
        {symbol: {date: metadata}} matching the standard signal format.
    """
    ctype = condition_config["type"]
    operator = condition_config.get("operator", "<=")
    threshold = condition_config.get("value", 0)
    signals = {}

    OPS = {
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
    }
    op_fn = OPS.get(operator)
    if op_fn is None:
        raise ValueError(f"Unknown operator: {operator}")

    if ctype == "rsi":
        period = condition_config.get("period", 14)
        for symbol in symbols:
            rsi_series = compute_rsi(symbol, period=period, start=start, end=end, conn=conn, price_index=price_index)
            signal_data = {}
            for date, rsi_val in rsi_series.items():
                if op_fn(rsi_val, threshold):
                    signal_data[date] = {"rsi": rsi_val}
            if signal_data:
                signals[symbol] = signal_data

    elif ctype == "momentum_rank":
        lookback = condition_config.get("lookback", 63)
        rank_data = compute_momentum_rank(symbols, lookback=lookback, start=start, end=end, conn=conn, price_index=price_index)
        for symbol in symbols:
            sym_ranks = rank_data.get(symbol, {})
            signal_data = {}
            for date, rank_val in sym_ranks.items():
                if op_fn(rank_val, threshold):
                    signal_data[date] = {"momentum_rank": rank_val}
            if signal_data:
                signals[symbol] = signal_data

    elif ctype == "ma_crossover":
        fast = condition_config.get("fast", 50)
        slow = condition_config.get("slow", 200)
        for symbol in symbols:
            ma_series = compute_ma_crossover(symbol, fast=fast, slow=slow, start=start, end=end, conn=conn, price_index=price_index)
            signal_data = {}
            for date, signal_val in ma_series.items():
                if op_fn(signal_val, threshold):
                    signal_data[date] = {"ma_signal": signal_val}
            if signal_data:
                signals[symbol] = signal_data

    elif ctype == "volume_capitulation":
        window = condition_config.get("window", 20)
        multiplier = condition_config.get("multiplier", 3.0)
        for symbol in symbols:
            cap_series = compute_volume_capitulation(symbol, window=window, multiplier=multiplier,
                                                     start=start, end=end, conn=conn)
            signal_data = {}
            for date, ratio in cap_series.items():
                # volume_capitulation always fires (ratio >= multiplier), no operator needed
                signal_data[date] = {"volume_ratio": ratio}
            if signal_data:
                signals[symbol] = signal_data

    return signals


def _precompute_exit_signals(config: dict, symbols: list[str], conn) -> dict:
    """
    Pre-compute fundamental exit signals for all tickers.
    
    Config section "exit_conditions" is a list of condition objects:
    [
      {"type": "revenue_deceleration", "min_quarters": 2, "require_margin_compression": true},
      {"type": "margin_collapse", "threshold_bps": -500, "min_quarters": 2}
    ]
    
    Logic is OR — any exit condition firing triggers exit.
    
    Returns:
        {symbol: {date: {"reason": "revenue_deceleration", ...metadata}}}
    """
    exit_conditions = config.get("exit_conditions", [])
    if not exit_conditions:
        return {}
    
    bt_start = config["backtest"]["start"]
    bt_end = config["backtest"]["end"]
    combined = {}  # {symbol: {date: metadata}}
    
    for cond in exit_conditions:
        ctype = cond["type"]
        for symbol in symbols:
            try:
                if ctype == "revenue_deceleration":
                    raw = find_revenue_deceleration(
                        symbol,
                        min_quarters=cond.get("min_quarters", 2),
                        require_margin_compression=cond.get("require_margin_compression", True),
                        margin_metric=cond.get("metric", "net_margin"),
                        start=bt_start, end=bt_end,
                        conn=conn,
                    )
                elif ctype == "margin_collapse":
                    raw = find_margin_collapse(
                        symbol,
                        metric=cond.get("metric", "net_margin"),
                        threshold_bps=cond.get("threshold_bps", -500),
                        min_quarters=cond.get("min_quarters", 2),
                        start=bt_start, end=bt_end,
                        conn=conn,
                    )
                else:
                    continue
            except Exception:
                continue
            
            if not raw:
                continue
            
            if symbol not in combined:
                combined[symbol] = {}
            
            for entry in raw:
                sig_date = entry["signal_date"]
                if sig_date not in combined[symbol]:
                    combined[symbol][sig_date] = {
                        "reason": ctype,
                        **{k: v for k, v in entry.items() if k != "signal_date"}
                    }
    
    return combined


def combine_signals(all_signals: list[dict], logic: str = "all") -> dict:
    """
    Combine multiple condition signals using AND or OR logic.
    
    Args:
        all_signals: List of {symbol: {date: metadata}} from each condition
        logic: "all" (AND - date must appear in ALL signals) or "any" (OR - date appears in ANY signal)
    
    Returns:
        {symbol: {date: combined_metadata}} where combined_metadata is a dict containing
        metadata from all contributing conditions
    """
    if not all_signals:
        return {}
    
    if len(all_signals) == 1:
        return all_signals[0]
    
    combined = {}
    
    # Get all symbols that appear in any signal set
    all_symbols = set()
    for signal_set in all_signals:
        all_symbols.update(signal_set.keys())
    
    for symbol in all_symbols:
        symbol_signals = {}
        
        if logic == "all":
            # Intersection: date must appear in ALL signal sets for this symbol
            # Start with dates from first signal set
            if symbol in all_signals[0]:
                candidate_dates = set(all_signals[0][symbol].keys())
                
                # Keep only dates that appear in all other signal sets
                for signal_set in all_signals[1:]:
                    if symbol not in signal_set:
                        candidate_dates = set()  # Symbol not in this signal set, no intersection
                        break
                    candidate_dates &= set(signal_set[symbol].keys())
                
                # Build combined metadata for intersecting dates
                for date in candidate_dates:
                    combined_meta = {}
                    for i, signal_set in enumerate(all_signals):
                        condition_meta = signal_set[symbol][date]
                        if isinstance(condition_meta, dict):
                            # Prefix keys with condition index to avoid conflicts
                            for k, v in condition_meta.items():
                                combined_meta[f"condition_{i}_{k}"] = v
                        else:
                            # Simple value (like drawdown_pct)
                            combined_meta[f"condition_{i}_value"] = condition_meta
                    symbol_signals[date] = combined_meta
                    
        elif logic == "any":
            # Union: date appears in ANY signal set for this symbol
            all_dates = set()
            for signal_set in all_signals:
                if symbol in signal_set:
                    all_dates.update(signal_set[symbol].keys())
            
            for date in all_dates:
                combined_meta = {}
                for i, signal_set in enumerate(all_signals):
                    if symbol in signal_set and date in signal_set[symbol]:
                        condition_meta = signal_set[symbol][date]
                        if isinstance(condition_meta, dict):
                            for k, v in condition_meta.items():
                                combined_meta[f"condition_{i}_{k}"] = v
                        else:
                            combined_meta[f"condition_{i}_value"] = condition_meta
                symbol_signals[date] = combined_meta
        
        if symbol_signals:
            combined[symbol] = symbol_signals
    
    return combined


def precompute_signals(config: dict, symbols: list[str], conn, price_index: dict = None) -> dict:
    """
    Pre-compute entry signals for all tickers using composable conditions.

    Supports both old format (entry.trigger) and new format (entry.conditions).
    If price_index is provided, passes it to price-based conditions to avoid re-querying.

    Returns:
        {symbol: {signal_date: metadata}} — dates where entry conditions are met
    """
    start = config["backtest"]["start"]
    end = config["backtest"]["end"]
    entry_config = config["entry"]
    
    # Handle backward compatibility: convert old format to new format internally
    if "conditions" in entry_config:
        # New format
        conditions = entry_config["conditions"]
        logic = entry_config.get("logic", "all")
    else:
        # Old format - convert trigger to conditions list
        conditions = [entry_config["trigger"]]
        if entry_config.get("confirm"):
            conditions.extend(entry_config["confirm"])
        logic = "all"
    
    # Pre-compute each condition independently
    all_signals = []
    earnings_data = None  # Load once if needed
    
    for condition in conditions:
        if condition["type"] == "earnings_momentum":
            if earnings_data is None:
                print("Loading earnings data for earnings_momentum condition...")
                earnings_data = load_earnings_data(symbols, conn)
        
        condition_signals = precompute_condition(condition, symbols, conn, start, end, earnings_data, price_index=price_index)
        all_signals.append(condition_signals)
    
    # Combine signals using the specified logic
    combined_signals = combine_signals(all_signals, logic)
    
    # For backward compatibility with priority ranking, we need to extract a single numeric value
    # from the combined metadata for each signal. We'll use the first numeric value we find.
    def extract_priority_value(metadata):
        if isinstance(metadata, (int, float)):
            return metadata
        if isinstance(metadata, dict):
            for value in metadata.values():
                if isinstance(value, (int, float)):
                    return value
        return -25  # Default drawdown for priority ranking
    
    # Convert metadata to priority values for existing code compatibility
    priority_signals = {}
    for symbol, dates in combined_signals.items():
        priority_signals[symbol] = {
            date: extract_priority_value(metadata) 
            for date, metadata in dates.items()
        }
    
    # Restructure combined_signals metadata into clean array format
    # From: {"condition_0_revenue_yoy": 56.65, "condition_1_beats": 3, ...}
    # To:   [{"type": "earnings_momentum", "threshold": ..., "values": {"revenue_yoy": 56.65}}, ...]
    structured_signals = {}
    for symbol, dates in combined_signals.items():
        structured_signals[symbol] = {}
        for date, metadata in dates.items():
            if not isinstance(metadata, dict):
                # Simple value — wrap into the canonical {type, config, observed} envelope.
                cond_config = conditions[0] if conditions else {}
                structured_signals[symbol][date] = [{
                    "type": cond_config.get("type", "unknown"),
                    "config": {k: v for k, v in cond_config.items() if k != "type"},
                    "observed": {"value": metadata},
                }]
                continue

            # Group keys by condition index
            by_condition = {}
            for key, val in metadata.items():
                parts = key.split("_", 2)  # condition_0_metric
                if len(parts) >= 3 and parts[0] == "condition" and parts[1].isdigit():
                    idx = int(parts[1])
                    metric = parts[2]
                    if idx not in by_condition:
                        by_condition[idx] = {}
                    by_condition[idx][metric] = val
                else:
                    # Ungrouped key — put in condition 0
                    if 0 not in by_condition:
                        by_condition[0] = {}
                    by_condition[0][key] = val

            result = []
            for idx in sorted(by_condition.keys()):
                cond_config = conditions[idx] if idx < len(conditions) else {}
                # Canonical signal-record envelope: {type, config, observed}.
                # See scripts/stop_pricing.py module docstring for the contract.
                entry = {
                    "type": cond_config.get("type", "unknown"),
                    "config": {k: v for k, v in cond_config.items() if k != "type"},
                    "observed": by_condition[idx] or {},
                }
                result.append(entry)

            structured_signals[symbol][date] = result

    return priority_signals, structured_signals


# ---------------------------------------------------------------------------
# Price Lookup
# ---------------------------------------------------------------------------
def build_price_index(symbols: list[str], conn) -> tuple[dict, dict, list]:
    """
    Build fast price lookups for both close and open:
      close_index = {symbol: {date: close}}
      open_index  = {symbol: {date: open}}
    Also returns sorted list of all trading dates.

    Uses a single bulk SQL query. open_index only includes (symbol, date)
    pairs where open is non-null in the source table; callers that fall back
    to close on missing-open dates should handle that themselves.

    open_index is consumed by entry_mode="next_open" to fill a signal from
    date D at open[D+1], avoiding the same-bar lookahead that would result
    from using close[D].
    """
    close_index = {s: {} for s in symbols}
    open_index = {s: {} for s in symbols}
    all_dates = set()

    cur = conn.cursor()
    placeholders = ",".join("?" * len(symbols))
    cur.execute(
        f"SELECT symbol, date, close, open FROM prices WHERE symbol IN ({placeholders}) ORDER BY symbol, date ASC",
        symbols,
    )
    for symbol, date, close, open_ in cur.fetchall():
        close_index[symbol][date] = close
        if open_ is not None:
            open_index[symbol][date] = open_
        all_dates.add(date)

    trading_dates = sorted(all_dates)
    return close_index, open_index, trading_dates


# ---------------------------------------------------------------------------
# Position & Portfolio
# ---------------------------------------------------------------------------
class Position:
    """A single open position."""

    def __init__(self, symbol: str, entry_date: str, entry_price: float,
                 shares: float, peak_price: float = None, signal_detail: dict = None,
                 stop_price: float | None = None, take_profit_price: float | None = None):
        self.symbol = symbol
        self.entry_date = entry_date
        self.entry_price = entry_price
        self.shares = shares
        self.peak_price = peak_price or entry_price  # pre-selloff peak for above_peak TP
        self.high_since_entry = entry_price
        self.signal_detail = signal_detail  # entry signal info, carried to SELL trades
        # Frozen exit prices for vol-adaptive modes (atr_multiple, realized_vol_multiple).
        # None for legacy modes — those use the dynamic pnl_pct check.
        self.stop_price = stop_price
        self.take_profit_price = take_profit_price

    def market_value(self, current_price: float) -> float:
        return self.shares * current_price

    def pnl_pct(self, current_price: float) -> float:
        if self.entry_price <= 0:
            return 0
        return ((current_price - self.entry_price) / self.entry_price) * 100

    def days_held(self, current_date: str) -> int:
        entry_dt = datetime.strptime(self.entry_date, "%Y-%m-%d")
        current_dt = datetime.strptime(current_date, "%Y-%m-%d")
        return (current_dt - entry_dt).days


class Portfolio:
    """Portfolio state tracker."""

    def __init__(self, initial_cash: float, strategy_config: dict | None = None,
                 ohlc_fetcher=None):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: dict[str, Position] = {}  # symbol -> Position
        self.trades: list[dict] = []
        self.nav_history: list[dict] = []  # [{date, nav, cash, positions_value}]
        self.closed_trades: list[dict] = []
        # For vol-adaptive stop modes: full strategy config (so open_position
        # knows the active mode/params) and a fetcher to pull pre-entry OHLC.
        # Both are None for tests/legacy callers — vol modes simply abort then.
        self.strategy_config = strategy_config or {}
        self.ohlc_fetcher = ohlc_fetcher

    def open_position(self, symbol: str, date: str, price: float,
                      amount: float, slippage_bps: float = 0,
                      peak_price: float = None, signal_detail: dict = None):
        """Open a new position. Skips silently if vol-adaptive stop pricing
        can't be computed (insufficient history)."""
        # Apply slippage
        exec_price = price * (1 + slippage_bps / 10000)

        # Vol-adaptive stop/TP: compute frozen exit prices once at entry.
        # Legacy modes get None for the prices (engine uses the dynamic check)
        # but still get a unified record in signal_detail for the FE.
        from stop_pricing import compute_stop_pricing
        pricing = compute_stop_pricing(
            self.strategy_config, symbol, date, exec_price, self.ohlc_fetcher,
        )
        if pricing["abort"]:
            # Insufficient history for the requested vol-adaptive mode — skip
            # the entry. Same effect as a missing price: no Position, no trade.
            print(f"  [vol-stops] skip {symbol} @ {date}: insufficient history")
            return

        # Build signal_detail. If no stop and no take_profit are configured,
        # preserve the original shape (a list of entry-condition records) so
        # legacy backtests are byte-identical. Otherwise normalize to a dict
        # with `entries` + `stop?` + `take_profit?` so the FE has one path
        # regardless of which exit modes are in use.
        if pricing["stop_record"] is None and pricing["tp_record"] is None:
            merged_signal = signal_detail
        else:
            merged_signal = {}
            if signal_detail is not None:
                merged_signal["entries"] = signal_detail
            if pricing["stop_record"]:
                merged_signal["stop"] = pricing["stop_record"]
            if pricing["tp_record"]:
                merged_signal["take_profit"] = pricing["tp_record"]

        shares = amount / exec_price
        if self.strategy_config.get("sizing", {}).get("shares") == "whole":
            import math
            shares = math.floor(shares)
            if shares <= 0:
                return
            amount = shares * exec_price

        if symbol in self.positions:
            # Add to existing position
            pos = self.positions[symbol]
            total_cost = (pos.shares * pos.entry_price) + (shares * exec_price)
            pos.shares += shares
            pos.entry_price = total_cost / pos.shares  # weighted avg
        else:
            self.positions[symbol] = Position(
                symbol=symbol,
                entry_date=date,
                entry_price=exec_price,
                shares=shares,
                peak_price=peak_price,
                signal_detail=merged_signal,
                stop_price=pricing["stop_price"],
                take_profit_price=pricing["take_profit_price"],
            )

        self.cash -= amount
        self.trades.append({
            "date": date,
            "symbol": symbol,
            "action": "BUY",
            "reason": "entry",
            "price": round(exec_price, 2),
            "shares": round(shares, 4),
            "amount": round(amount, 2),
            "signal_detail": merged_signal,
        })

    def close_position(self, symbol: str, date: str, price: float,
                       reason: str, slippage_bps: float = 0,
                       partial_pct: float = 100):
        """Close (or partially close) a position."""
        if symbol not in self.positions:
            return

        pos = self.positions[symbol]
        exec_price = price * (1 - slippage_bps / 10000)

        if partial_pct >= 100:
            # Full close
            shares_to_sell = pos.shares
            del self.positions[symbol]
        else:
            # Partial close
            shares_to_sell = pos.shares * (partial_pct / 100)
            pos.shares -= shares_to_sell

        proceeds = shares_to_sell * exec_price
        self.cash += proceeds

        cost_basis = shares_to_sell * pos.entry_price
        pnl = proceeds - cost_basis
        pnl_pct = ((exec_price - pos.entry_price) / pos.entry_price) * 100

        trade = {
            "date": date,
            "symbol": symbol,
            "action": "SELL",
            "reason": reason,
            "price": round(exec_price, 2),
            "shares": round(shares_to_sell, 4),
            "amount": round(proceeds, 2),
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
            "entry_date": pos.entry_date,
            "entry_price": round(pos.entry_price, 2),
            "days_held": pos.days_held(date),
            "signal_detail": pos.signal_detail,  # carry entry signal to SELL
        }
        self.trades.append(trade)
        self.closed_trades.append(trade)

    def nav(self, price_index: dict, date: str) -> float:
        """Calculate net asset value."""
        positions_value = 0
        for symbol, pos in self.positions.items():
            price = price_index.get(symbol, {}).get(date)
            if price:
                positions_value += pos.market_value(price)
                # Track high watermark per position
                if price > pos.high_since_entry:
                    pos.high_since_entry = price
        return self.cash + positions_value

    def record_nav(self, price_index: dict, date: str):
        """Record daily NAV snapshot with position-level breakdown."""
        positions_value = 0
        position_details = {}
        for symbol, pos in self.positions.items():
            price = price_index.get(symbol, {}).get(date)
            if price:
                mv = pos.market_value(price)
                positions_value += mv
                position_details[symbol] = {
                    "price": round(price, 2),
                    "shares": round(pos.shares, 4),
                    "market_value": round(mv, 2),
                    "pnl_pct": round(pos.pnl_pct(price), 2),
                    "entry_price": round(pos.entry_price, 2),
                    "entry_date": pos.entry_date,
                    "days_held": pos.days_held(date),
                }

        total_nav = self.cash + positions_value
        prev_nav = self.nav_history[-1]["nav"] if self.nav_history else self.initial_cash
        daily_pnl = total_nav - prev_nav
        daily_pnl_pct = (daily_pnl / prev_nav * 100) if prev_nav > 0 else 0

        self.nav_history.append({
            "date": date,
            "nav": round(total_nav, 2),
            "cash": round(self.cash, 2),
            "positions_value": round(positions_value, 2),
            "num_positions": len(self.positions),
            "daily_pnl": round(daily_pnl, 2),
            "daily_pnl_pct": round(daily_pnl_pct, 4),
            "positions": position_details,
        })

    def position_weight(self, symbol: str, price_index: dict, date: str) -> float:
        """Get position weight as % of NAV."""
        total_nav = self.nav(price_index, date)
        if total_nav <= 0 or symbol not in self.positions:
            return 0
        price = price_index.get(symbol, {}).get(date, 0)
        return (self.positions[symbol].market_value(price) / total_nav) * 100


# ---------------------------------------------------------------------------
# Exit Checks
# ---------------------------------------------------------------------------
def check_stop_loss(pos: Position, current_price: float, config: dict) -> bool:
    """Check if stop loss is triggered."""
    sl = config.get("stop_loss")
    if not sl:
        return False

    sl_type = sl.get("type")

    # Vol-adaptive modes: stop_price was frozen at entry. Trigger on price cross.
    if sl_type in ("atr_multiple", "realized_vol_multiple"):
        return pos.stop_price is not None and current_price <= pos.stop_price

    if sl_type == "drawdown_from_entry":
        sl_value = sl.get("value", -25)
        pnl = pos.pnl_pct(current_price)
        return pnl <= sl_value

    return False


def check_take_profit(pos: Position, current_price: float, config: dict) -> bool:
    """Check if take profit is triggered."""
    tp = config.get("take_profit")
    if not tp:
        return False

    tp_type = tp.get("type")

    # Vol-adaptive modes: tp_price was frozen at entry. Trigger on price cross.
    if tp_type in ("atr_multiple", "realized_vol_multiple"):
        return pos.take_profit_price is not None and current_price >= pos.take_profit_price

    tp_value = tp.get("value", 10)

    if tp_type == "gain_from_entry":
        pnl = pos.pnl_pct(current_price)
        return pnl >= tp_value

    elif tp_type == "above_peak":
        if pos.peak_price and pos.peak_price > 0:
            gain_from_peak = ((current_price - pos.peak_price) / pos.peak_price) * 100
            return gain_from_peak >= tp_value

    return False


def check_time_stop(pos: Position, current_date: str, config: dict) -> bool:
    """Check if time stop is triggered."""
    ts = config.get("time_stop")
    if not ts:
        return False

    max_days = ts.get("max_days", 365)
    return pos.days_held(current_date) >= max_days


# ---------------------------------------------------------------------------
# Rebalancing
# ---------------------------------------------------------------------------
def is_rebalance_date(date: str, last_rebal: str, frequency: str) -> bool:
    """Check if today is a rebalance date."""
    if frequency == "none":
        return False

    if not last_rebal:
        return False

    current = datetime.strptime(date, "%Y-%m-%d")
    last = datetime.strptime(last_rebal, "%Y-%m-%d")

    if frequency == "quarterly":
        return (current - last).days >= 90
    elif frequency == "monthly":
        return (current - last).days >= 30

    return False


# ---------------------------------------------------------------------------
# Main Backtest Loop
# ---------------------------------------------------------------------------
def run_backtest(config: dict, force_close_at_end: bool = True,
                 shared_price_index: tuple = None,
                 gate_dates: set = None) -> dict:
    """
    Execute the backtest.

    Args:
        config: Strategy configuration dict.
        force_close_at_end: Close all positions on last day (True for backtests, False for deployments).
        shared_price_index: Optional (price_index, trading_dates) tuple pre-built by the portfolio engine.
            When provided, skips building the price index and uses these instead.
        gate_dates: Optional set of date strings where new entries are allowed.
            When provided, the engine skips new entries on dates not in this set.
            Exits (stop-loss, take-profit, time-stop) still fire regardless.
            When None, all dates are allowed (no gating).

    Returns dict with:
        - trades: list of all trades
        - nav_history: daily NAV series
        - closed_trades: completed round-trips
        - metrics: summary statistics
    """
    stamp_strategy_id(config)
    conn = get_connection()

    # Resolve universe
    symbols = resolve_universe(config, conn)
    print(f"Universe: {len(symbols)} tickers — {', '.join(symbols)}")

    # Use shared price index if provided, otherwise build one.
    # shared_price_index is a 3-tuple: (close_index, open_index, trading_dates).
    if shared_price_index is not None:
        shared_pi, shared_oi, shared_td = shared_price_index
        # Filter to this sleeve's symbols (shared index may have more tickers)
        price_index = {s: shared_pi[s] for s in symbols if s in shared_pi}
        open_index = {s: shared_oi.get(s, {}) for s in symbols}
        all_dates_from_shared = set()
        for s in symbols:
            if s in shared_pi:
                all_dates_from_shared.update(shared_pi[s].keys())
        trading_dates = sorted(all_dates_from_shared)
        print(f"Using shared price index ({len(price_index)} tickers from shared)")
    else:
        price_index = None
        open_index = None
        trading_dates = None

    # Pre-compute signals (pass price_index to avoid re-querying)
    print("Pre-computing entry signals...")
    signals, signal_metadata = precompute_signals(config, symbols, conn, price_index=price_index)
    total_signals = sum(len(v) for v in signals.values())
    print(f"Found {total_signals} signal dates across {len(signals)} tickers")

    # Pre-compute exit signals (fundamental-based exits)
    exit_signals = {}
    if config.get("exit_conditions"):
        print("Pre-computing exit signals...")
        exit_signals = _precompute_exit_signals(config, symbols, conn)
        total_exit = sum(len(v) for v in exit_signals.values())
        print(f"Found {total_exit} exit signal dates across {len(exit_signals)} tickers")

    # Load earnings data for rebalancing
    print("Loading earnings data...")
    earnings_data = load_earnings_data(symbols, conn)
    print(f"Loaded earnings for {len(earnings_data)} tickers")

    # Build price index if not shared
    if price_index is None:
        print("Building price index...")
        price_index, open_index, trading_dates = build_price_index(symbols, conn)
    else:
        print("Price index ready (shared)")

    # Pre-load PE series for ranking (avoids reloading every day)
    _ranking_pe_series = _load_pe_timeseries(symbols)

    # Pre-load composite_score factor series for the full backtest window
    # so the daily ranker doesn't re-issue an N-symbol SELECT (with 365-day
    # padding) on every rank call. Mirrors the pe_series precedent above.
    # Same behavior — _compute_composite_score still bisects as-of per day —
    # but the heavy DB query happens ONCE per factor, not once per call.
    _composite_series_by_factor: dict | None = None
    _ranking_cfg = config.get("ranking") or {}
    if _ranking_cfg.get("by") == "composite_score":
        _composite_cfg = config.get("composite_score") or {}
        _factor_names: set[str] = set()
        for _b in (_composite_cfg.get("buckets") or {}).values():
            for _f in _b.get("factors", []):
                if isinstance(_f, dict):
                    _factor_names.add(_f["name"])
                else:
                    _factor_names.add(_f.name)
        if _factor_names:
            print(f"Pre-loading {len(_factor_names)} composite_score factor(s)...")
            _composite_series_by_factor = {}
            for _fname in _factor_names:
                _composite_series_by_factor[_fname] = _load_feature_series(
                    _fname, symbols, config["backtest"]["start"],
                    config["backtest"]["end"], conn, price_index=price_index,
                )

    conn.close()
    # Re-open connection for ranking queries during simulation
    conn = get_connection()

    # Filter trading dates to backtest range
    bt_start = config["backtest"]["start"]
    bt_end = config["backtest"]["end"]
    trading_dates = [d for d in trading_dates if bt_start <= d <= bt_end]

    # PIT membership precompute. For universe.type='index', this returns a
    # {date: frozenset(symbols-that-were-members-on-date)} map, which the
    # per-day entry scan uses to filter survivors-only candidates. None for
    # non-PIT universe types — engine behavior is unchanged in that case.
    _pit_members_on = pit_members_by_date(config, conn, trading_dates)
    if _pit_members_on is not None:
        print(f"PIT membership precomputed for {len(_pit_members_on)} trading dates.")
    print(f"Backtest period: {trading_dates[0]} to {trading_dates[-1]} ({len(trading_dates)} trading days)")

    # Initialize portfolio
    initial_cash = config["sizing"]["initial_allocation"]
    slippage = config["backtest"].get("slippage_bps", 10)
    max_positions = config["sizing"].get("max_positions", 10)
    entry_mode = config["backtest"].get("entry_price", "next_close")

    # Build a per-symbol OHLC fetcher for vol-adaptive stops. Cheap closure
    # over the simulation's market.db connection; only invoked at entry time.
    from stop_pricing import make_sqlite_ohlc_fetcher
    _ohlc_fetcher = make_sqlite_ohlc_fetcher(conn)
    portfolio = Portfolio(initial_cash, strategy_config=config, ohlc_fetcher=_ohlc_fetcher)

    import random

    # Track pending entries (for next_close execution)
    pending_entries = []  # [(symbol, peak_price)]
    last_rebal_date = None
    stop_loss_cooldowns = {}  # {symbol: last_stop_loss_date}
    cooldown_days = 0
    if config.get("stop_loss"):
        cooldown_calendar = config["stop_loss"].get("cooldown_days", 0)
        cooldown_days = _calendar_to_trading_days(cooldown_calendar) if cooldown_calendar > 0 else 0
    entry_priority = config.get("entry", {}).get("priority", "worst_drawdown")

    print(f"Running simulation with ${initial_cash:,.0f}...")
    print()

    _gated = gate_dates is not None  # regime gating active?

    for i, date in enumerate(trading_dates):
        _gate_on = (not _gated) or (date in gate_dates)

        # --- Execute pending entries from previous day (only if gated on) ---
        if not _gate_on:
            pending_entries = []

        # Pre-compute risk_parity weights once per execution day so position
        # sizes within this batch are properly inverse-vol weighted. Without
        # this, sizing each position independently would just produce equal-
        # weight (which is exactly the silent fallthrough we had before).
        sizing_type = config["sizing"]["type"]
        risk_parity_weights: dict[str, float] = {}
        if sizing_type == "risk_parity" and pending_entries:
            from stop_pricing import compute_realized_vol
            vol_window = int(config["sizing"].get("vol_window_days", 20))
            vol_source = config["sizing"].get("vol_source", "historical")
            sigmas: dict[str, float] = {}
            for sym, _, _ in pending_entries:
                # Reuse the price_index: closes up to (but not including) `date`.
                pm = price_index.get(sym, {})
                if not pm:
                    continue
                closes_dates = sorted(d for d in pm if d < date)
                tail = closes_dates[-(vol_window + 1):] if len(closes_dates) >= vol_window + 1 else []
                if not tail:
                    continue
                closes = [pm[d] for d in tail]
                sigma = compute_realized_vol(closes, vol_window, vol_source)
                if sigma is not None and sigma > 0:
                    sigmas[sym] = sigma
            if sigmas:
                # Inverse-vol weights normalized to sum to 1 across the batch.
                inv = {s: 1.0 / sigmas[s] for s in sigmas}
                total = sum(inv.values())
                risk_parity_weights = {s: v / total for s, v in inv.items()}

        # Select the fill-price index for this batch of pending entries.
        # next_close → close[D+1] (price_index). next_open → open[D+1] (open_index).
        fill_index = open_index if entry_mode == "next_open" else price_index

        for symbol, peak_price, sig_detail in pending_entries:
            if symbol in portfolio.positions:
                continue  # Already in portfolio
            if len(portfolio.positions) >= max_positions:
                break  # Full

            price = fill_index.get(symbol, {}).get(date)
            if not price:
                continue

            # Calculate position size
            current_nav = portfolio.nav(price_index, date)
            if current_nav <= 0:
                continue

            if sizing_type == "equal_weight":
                target_weight = 1.0 / max_positions
                amount = current_nav * target_weight
            elif sizing_type == "fixed_amount":
                amount = config["sizing"].get("amount_per_position", initial_cash / max_positions)
            elif sizing_type == "risk_parity":
                # Aggregate target for this batch matches equal_weight's:
                #   n_batch × (current_nav / max_positions).
                # Distribute that pool by inverse-vol weights computed above.
                n_batch = len(pending_entries)
                pool = (n_batch / max_positions) * current_nav
                w = risk_parity_weights.get(symbol)
                if w is not None:
                    amount = pool * w
                else:
                    # Insufficient vol history for this name → fall back to equal_weight slot.
                    amount = current_nav / max_positions
            else:
                # Unknown sizing type: safe fallback.
                amount = current_nav / max_positions

            # Don't exceed available cash
            amount = min(amount, portfolio.cash * 0.99)  # Keep 1% cash buffer
            if amount <= 0:
                continue

            # Check max position weight
            max_pct = config.get("rebalancing", {}).get("rules", {}).get("max_position_pct", 100)
            if (amount / current_nav) * 100 > max_pct:
                amount = current_nav * (max_pct / 100)

            portfolio.open_position(
                symbol=symbol, date=date, price=price,
                amount=amount, slippage_bps=slippage,
                peak_price=peak_price,
                signal_detail=sig_detail,
            )

        pending_entries = []

        # --- Check exits for open positions ---
        # Gated-off days suppress exits too. Rationale: when an allocation
        # profile sets the sleeve to 0%, the portfolio-level lerp has already
        # liquidated the sleeve's allocated capital. In a live deployment the
        # underlying broker positions are gone; only the sleeve's internal
        # bookkeeping still tracks them. Firing stop_loss / take_profit /
        # time_stop / fundamental_exit on those phantom positions would emit
        # SELL trades the broker can't execute, and create a dual-bookkeeping
        # mismatch where cumulative shares per symbol go negative in the
        # combined trade ledger (sleeve-internal SELL after a portfolio-level
        # rebalance SELL of the same lot).
        closed_today = []
        if _gate_on:
            for symbol, pos in list(portfolio.positions.items()):
                price = price_index.get(symbol, {}).get(date)
                if not price:
                    continue

                # Check stop loss
                if check_stop_loss(pos, price, config):
                    portfolio.close_position(symbol, date, price, "stop_loss", slippage)
                    closed_today.append(symbol)
                    if cooldown_days > 0:
                        stop_loss_cooldowns[symbol] = date
                    continue

                # Check take profit
                if check_take_profit(pos, price, config):
                    portfolio.close_position(symbol, date, price, "take_profit", slippage)
                    closed_today.append(symbol)
                    continue

                # Check time stop
                if check_time_stop(pos, date, config):
                    portfolio.close_position(symbol, date, price, "time_stop", slippage)
                    closed_today.append(symbol)
                    continue

                # Check fundamental exit conditions
                if exit_signals.get(symbol, {}).get(date):
                    reason = exit_signals[symbol][date].get("reason", "fundamental_exit")
                    portfolio.close_position(symbol, date, price, reason, slippage)
                    closed_today.append(symbol)
                    continue

        # --- Check rebalancing (only if regime gate is on) ---
        rebal_freq = config.get("rebalancing", {}).get("frequency", "none")
        if _gate_on and is_rebalance_date(date, last_rebal_date, rebal_freq):
            rebal_mode = config.get("rebalancing", {}).get("mode", "trim")
            if rebal_mode == "equal_weight":
                _do_equal_weight_rebalance(portfolio, price_index, date, config,
                                            slippage, symbols, signals, signal_metadata,
                                            conn, _ranking_pe_series,
                                            composite_series=_composite_series_by_factor)
            else:
                _do_rebalance(portfolio, price_index, date, config, slippage, earnings_data)
            last_rebal_date = date
        elif last_rebal_date is None and len(portfolio.positions) > 0:
            last_rebal_date = date

        # --- Check new entries (only if regime gate is on) ---
        # Collect all candidates with active signals today
        available_slots = max_positions - len(portfolio.positions) - len(pending_entries)
        if available_slots > 0 and _gate_on:
            candidates = []
            # Build trading day index for cooldown check
            date_idx = trading_dates.index(date) if date in trading_dates else -1

            # PIT filter: when an index-typed universe is in play, only
            # consider symbols that WERE members of the index on `date`. A
            # non-member can't be bought (out of mandate), but existing
            # positions stay open if they're later removed from the index —
            # matching how a real index-tracking PM would handle it.
            _pit_today = _pit_members_on[date] if _pit_members_on is not None else None

            for symbol in symbols:
                if symbol in portfolio.positions:
                    continue
                if _pit_today is not None and symbol not in _pit_today:
                    continue

                signal_data = signals.get(symbol, {})
                if date not in signal_data:
                    continue

                # Check stop-loss cooldown
                if cooldown_days > 0 and symbol in stop_loss_cooldowns:
                    sl_date = stop_loss_cooldowns[symbol]
                    sl_idx = trading_dates.index(sl_date) if sl_date in trading_dates else -1
                    if sl_idx >= 0 and (date_idx - sl_idx) < cooldown_days:
                        continue

                drawdown = signal_data[date]
                candidates.append((symbol, drawdown))

            # Rank candidates
            if len(candidates) > available_slots and config.get("ranking"):
                # Use ranking to select best candidates
                candidates = rank_candidates(candidates, config, conn, date,
                                              price_index, pe_series=_ranking_pe_series,
                                              rsi_cache=None,
                                              composite_series=_composite_series_by_factor)
            elif len(candidates) > available_slots:
                # More candidates than slots, no explicit ranking → default pe_percentile
                candidates = rank_candidates(candidates, config, conn, date,
                                              price_index, pe_series=_ranking_pe_series,
                                              rsi_cache=None,
                                              composite_series=_composite_series_by_factor)
            elif entry_priority == "worst_drawdown":
                candidates.sort(key=lambda x: x[1])  # most negative first
            elif entry_priority == "random":
                random.shuffle(candidates)

            # Apply top_n from ranking config if set
            ranking_top_n = config.get("ranking", {}).get("top_n") if config.get("ranking") else None
            if ranking_top_n and len(candidates) > ranking_top_n:
                candidates = candidates[:ranking_top_n]

            # Fill available slots — queue for next-day execution.
            # Both next_close and next_open queue here; the difference is the
            # fill-price index used on the next iteration (close vs open).
            for symbol, drawdown in candidates[:available_slots]:
                peak_price = _find_recent_peak(symbol, date, price_index, config)
                sig_detail = signal_metadata.get(symbol, {}).get(date)
                pending_entries.append((symbol, peak_price, sig_detail))

        # --- Record NAV ---
        portfolio.record_nav(price_index, date)

    # --- Close any remaining positions at end (backtests only, not live deployments) ---
    if force_close_at_end:
        last_date = trading_dates[-1]
        for symbol in list(portfolio.positions.keys()):
            price = price_index.get(symbol, {}).get(last_date)
            if price:
                portfolio.close_position(symbol, last_date, price, "backtest_end", slippage)

    # Close ranking DB connection
    conn.close()

    # --- Compute metrics ---
    metrics = compute_metrics(portfolio, initial_cash, trading_dates)

    # --- Compute benchmarks ---
    universe_sector = config.get("universe", {}).get("sector")
    ann_return = metrics.get("annualized_return_pct")
    period_total_return = metrics.get("total_return_pct")

    def _populate(bench: dict | None, prefix: str):
        """Always-honest period metrics; gated annualized alpha (None when
        either side lacks an annualized value — short window or missing data)."""
        if not bench:
            return
        bm = bench["metrics"]
        bench_total = bm.get("total_return_pct")
        bench_ann = bm.get("annualized_return_pct")

        metrics[f"{prefix}_benchmark_return_pct"] = bench_total
        if period_total_return is not None and bench_total is not None:
            metrics[f"period_excess_vs_{prefix}_pct"] = round(
                period_total_return - bench_total, 2)

        if bench_ann is not None and ann_return is not None:
            metrics[f"alpha_vs_{prefix}_pct"] = round(ann_return - bench_ann, 2)
            metrics[f"{prefix}_benchmark_ann_return_pct"] = bench_ann
        else:
            metrics[f"alpha_vs_{prefix}_pct"] = None
            metrics[f"{prefix}_benchmark_ann_return_pct"] = None

    # Market benchmark (SPY) — always compute
    print("Computing benchmark (S&P 500)...")
    market_benchmark = compute_benchmark(trading_dates, initial_cash, sector=None)
    _populate(market_benchmark, "market")
    if market_benchmark:
        # Backward-compat aliases for older readers (experiments table, etc.)
        metrics["benchmark_return_pct"] = market_benchmark["metrics"].get("total_return_pct")
        metrics["benchmark_ann_return_pct"] = market_benchmark["metrics"].get("annualized_return_pct")
        metrics["alpha_ann_pct"] = metrics.get("alpha_vs_market_pct")

    # Sector benchmark — compute if strategy has a sector universe
    benchmark = market_benchmark
    if universe_sector and universe_sector in SECTOR_ETF_MAP:
        print(f"Computing benchmark ({SECTOR_ETF_MAP[universe_sector]})...")
        sector_benchmark = compute_benchmark(trading_dates, initial_cash, sector=universe_sector)
        _populate(sector_benchmark, "sector")
        if sector_benchmark:
            benchmark = sector_benchmark

    from datetime import datetime, timezone
    # Build open positions list from portfolio
    last_date = trading_dates[-1]
    open_positions = []
    for symbol, pos in portfolio.positions.items():
        current_price = price_index.get(symbol, {}).get(last_date)
        if current_price:
            pnl_pct = ((current_price - pos.entry_price) / pos.entry_price) * 100
            open_positions.append({
                "symbol": symbol,
                "entry_date": pos.entry_date,
                "entry_price": round(pos.entry_price, 2),
                "current_price": round(current_price, 2),
                "shares": round(pos.shares, 4),
                "market_value": round(pos.shares * current_price, 2),
                "cost_basis": round(pos.shares * pos.entry_price, 2),
                "pnl": round(pos.shares * (current_price - pos.entry_price), 2),
                "pnl_pct": round(pnl_pct, 2),
                "days_held": (datetime.strptime(last_date, "%Y-%m-%d") - datetime.strptime(pos.entry_date, "%Y-%m-%d")).days,
            })

    return {
        "strategy": config["name"],
        "run_at": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "trades": portfolio.trades,
        "closed_trades": portfolio.closed_trades,
        "open_positions": open_positions,
        "nav_history": portfolio.nav_history,
        "metrics": metrics,
        "benchmark": benchmark,
    }


def _find_recent_peak(symbol: str, date: str, price_index: dict,
                      config: dict) -> float:
    """Find the pre-selloff peak price for a symbol."""
    prices = price_index.get(symbol, {})
    
    # Get window_days from the first price-based condition, or use default
    window_calendar = 90  # default
    entry_config = config["entry"]
    
    if "conditions" in entry_config:
        # New format - find first price condition with window_days
        for condition in entry_config["conditions"]:
            if condition["type"] in ("current_drop", "period_drop", "selloff"):
                window_calendar = condition.get("window_days", 90)
                break
    else:
        # Old format
        trigger = entry_config["trigger"]
        window_calendar = trigger.get("window_days", 90)
    
    window_trading = _calendar_to_trading_days(window_calendar)

    # Look back window * 2 trading days to find the peak before the selloff
    sorted_dates = sorted(d for d in prices.keys() if d <= date)
    lookback = sorted_dates[-(window_trading * 2):] if len(sorted_dates) > window_trading * 2 else sorted_dates

    if not lookback:
        return prices.get(date, 0)

    return max(prices[d] for d in lookback)


def _do_rebalance(portfolio: Portfolio, price_index: dict, date: str,
                  config: dict, slippage: float, earnings_data: dict = None):
    """Rebalance positions according to rules."""
    rules = config.get("rebalancing", {}).get("rules", {})
    max_pct = rules.get("max_position_pct", 100)

    current_nav = portfolio.nav(price_index, date)
    if current_nav <= 0:
        return

    # Trim positions that exceed max weight
    for symbol in list(portfolio.positions.keys()):
        weight = portfolio.position_weight(symbol, price_index, date)
        if weight > max_pct:
            trim_pct = ((weight - max_pct) / weight) * 100
            price = price_index.get(symbol, {}).get(date)
            if price:
                portfolio.close_position(
                    symbol, date, price, "rebalance_trim",
                    slippage, partial_pct=trim_pct,
                )

    # Earnings-beat add: if position is up > threshold AND recent earnings beat
    add_on_beat = rules.get("add_on_earnings_beat")
    if add_on_beat and earnings_data:
        gain_threshold = add_on_beat.get("min_gain_pct", 15)
        max_add_multiplier = add_on_beat.get("max_add_multiplier", 1.5)
        lookback_days = add_on_beat.get("lookback_days", 90)  # how recent the beat must be

        current_nav = portfolio.nav(price_index, date)
        current_dt = datetime.strptime(date, "%Y-%m-%d")

        for symbol, pos in list(portfolio.positions.items()):
            price = price_index.get(symbol, {}).get(date)
            if not price:
                continue

            # Check if position is up enough
            pnl = pos.pnl_pct(price)
            if pnl < gain_threshold:
                continue

            # Check for recent earnings beat
            sym_earnings = earnings_data.get(symbol, {})
            recent_beat = False
            for earn_date, earn_data in sym_earnings.items():
                earn_dt = datetime.strptime(earn_date, "%Y-%m-%d")
                days_ago = (current_dt - earn_dt).days
                if 0 <= days_ago <= lookback_days and earn_data["beat"]:
                    recent_beat = True
                    break

            if not recent_beat:
                continue

            # Add to position — up to max_add_multiplier of original size
            original_cost = pos.entry_price * pos.shares
            max_total = original_cost * max_add_multiplier
            current_value = pos.market_value(price)
            room_to_add = max_total - current_value

            if room_to_add <= 1000:
                continue

            amount = min(room_to_add, portfolio.cash * 0.25)  # Don't use more than 25% of cash
            if amount <= 0:
                continue

            # Check weight cap
            new_weight = ((current_value + amount) / current_nav) * 100
            if new_weight > max_pct:
                amount = (max_pct / 100 * current_nav) - current_value
                if amount <= 0:
                    continue

            portfolio.open_position(
                symbol=symbol, date=date, price=price,
                amount=amount, slippage_bps=slippage,
                peak_price=pos.peak_price,
            )


# ---------------------------------------------------------------------------
# Equal-Weight Rebalance (with optional re-ranking rotation)
# ---------------------------------------------------------------------------
def _do_equal_weight_rebalance(portfolio: Portfolio, price_index: dict, date: str,
                                config: dict, slippage: float, symbols: list[str],
                                signals: dict, signal_metadata: dict,
                                conn, pe_series: dict = None,
                                composite_series: dict | None = None):
    """
    Equal-weight rebalance: reset all positions to 1/N weight.
    
    If ranking is configured, also re-ranks the universe and rotates:
    - Sell positions that fell out of top N
    - Buy new positions that entered top N
    - Then equal-weight all holdings
    
    Without ranking, just reweights existing positions to equal weight.
    """
    current_nav = portfolio.nav(price_index, date)
    if current_nav <= 0:
        return

    max_positions = config["sizing"].get("max_positions", 10)
    ranking_config = config.get("ranking")
    
    # Determine target holdings
    if ranking_config:
        # Re-rank full universe and pick top N
        top_n = ranking_config.get("top_n", max_positions)
        
        # Build candidates from all universe tickers with active signals today
        candidates = []
        for symbol in symbols:
            if date in signals.get(symbol, {}):
                drawdown = signals[symbol][date]
                candidates.append((symbol, drawdown))
        
        if candidates:
            ranked = rank_candidates(candidates, config, conn, date,
                                      price_index, pe_series=pe_series,
                                      composite_series=composite_series)
            target_symbols = set(sym for sym, _ in ranked[:top_n])
        else:
            target_symbols = set(portfolio.positions.keys())
    else:
        # No ranking — keep current holdings
        target_symbols = set(portfolio.positions.keys())
    
    # Step 1: Sell positions that are no longer in target set
    for symbol in list(portfolio.positions.keys()):
        if symbol not in target_symbols:
            price = price_index.get(symbol, {}).get(date)
            if price:
                portfolio.close_position(symbol, date, price, "rebalance_rotation", slippage)
    
    # Step 2: Calculate target weight per position
    n_targets = len(target_symbols)
    if n_targets == 0:
        return
    
    # Recalculate NAV after sells
    current_nav = portfolio.nav(price_index, date)
    target_amount = current_nav / n_targets
    
    # Step 3: Reweight existing positions (trim or add)
    for symbol in list(portfolio.positions.keys()):
        if symbol not in target_symbols:
            continue
        price = price_index.get(symbol, {}).get(date)
        if not price:
            continue
        
        pos = portfolio.positions[symbol]
        current_value = pos.market_value(price)
        diff = target_amount - current_value
        
        if diff < -1000:
            # Overweight — trim
            trim_pct = (abs(diff) / current_value) * 100
            portfolio.close_position(symbol, date, price, "rebalance_trim", slippage, partial_pct=min(trim_pct, 99))
        elif diff > 1000 and portfolio.cash > 1000:
            # Underweight — add
            add_amount = min(diff, portfolio.cash * 0.95)
            if add_amount >= 1000:
                sig_detail = signal_metadata.get(symbol, {}).get(date)
                portfolio.open_position(symbol, date, price, add_amount, slippage,
                                         signal_detail=sig_detail)
    
    # Step 4: Buy new positions (rotation entries)
    # Recalculate NAV and target after reweighting
    current_nav = portfolio.nav(price_index, date)
    n_remaining = len(target_symbols)
    if n_remaining == 0:
        return
    target_amount = current_nav / n_remaining
    
    for symbol in target_symbols:
        if symbol in portfolio.positions:
            continue  # Already held
        price = price_index.get(symbol, {}).get(date)
        if not price:
            continue
        
        amount = min(target_amount, portfolio.cash * 0.95)
        if amount <= 0:
            continue

        sig_detail = signal_metadata.get(symbol, {}).get(date)
        portfolio.open_position(symbol, date, price, amount, slippage,
                                 signal_detail=sig_detail)


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------
SECTOR_ETF_MAP = {
    "Technology": "XLK",
    "Healthcare": "XLV",
    "Financial Services": "XLF",
    "Energy": "XLE",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Industrials": "XLI",
    "Basic Materials": "XLB",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
    "Utilities": "XLU",
}


# Minimum trading-day sample required to annualize a return without
# misleading the reader. 60d ≈ 3 calendar months — standard convention
# below which Sharpe/Sortino/ann return have CIs on the order of the
# point estimate itself. Below this we report only realized period metrics.
MIN_TRADING_DAYS_FOR_ANNUALIZATION = 60

# Minimum to compute *any* benchmark output (need ≥2 prices to compute a return).
MIN_TRADING_DAYS_FOR_BENCHMARK = 2


def compute_benchmark(trading_dates: list[str], initial_cash: float,
                      conn=None, sector: str = None) -> dict:
    """
    Compute buy-and-hold benchmark over the same period.

    If sector is provided, uses the sector ETF (e.g. XLK for Technology).
    Falls back to SPY / S&P 500 index for multi-sector or unknown sectors.

    Returns a dict with always-honest realized period metrics. Annualized
    metrics are only included when the sample is statistically meaningful
    (>= MIN_TRADING_DAYS_FOR_ANNUALIZATION); otherwise they are explicitly
    set to None so the UI can render "—" rather than a fabricated number.

    Returns None only when there isn't enough data to compute a return at all.
    """
    price_dict = {}
    bench_symbol = None

    # Build candidate list: sector ETF first, then broad market
    candidates = []
    if sector and sector in SECTOR_ETF_MAP:
        candidates.append(SECTOR_ETF_MAP[sector])
    candidates.extend(["SPY", "^GSPC", "GSPC"])

    # Try DB first
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    for sym in candidates:
        prices = get_prices(sym, start=trading_dates[0], end=trading_dates[-1], conn=conn)
        if prices and len(prices) >= MIN_TRADING_DAYS_FOR_BENCHMARK:
            bench_symbol = sym
            price_dict = {d: c for d, c in prices}
            break
    if own_conn:
        conn.close()

    # Fall back to index JSON (broad market only)
    if not bench_symbol:
        index_dir = DATA_DIR / "prices" / "indices"
        for fname, label in [("GSPC.json", "S&P 500"), ("DJI.json", "DJIA")]:
            fpath = index_dir / fname
            if fpath.exists():
                try:
                    data = json.loads(fpath.read_text())
                    records = data.get("data", data) if isinstance(data, dict) else data
                    if isinstance(records, list):
                        for r in records:
                            d = r.get("date", "")[:10]
                            c = r.get("close")
                            if d and c and trading_dates[0] <= d <= trading_dates[-1]:
                                price_dict[d] = c
                        if len(price_dict) >= MIN_TRADING_DAYS_FOR_BENCHMARK:
                            bench_symbol = label
                            break
                except (json.JSONDecodeError, KeyError):
                    continue

    if not bench_symbol or len(price_dict) < MIN_TRADING_DAYS_FOR_BENCHMARK:
        return None

    # Buy-and-hold: invest initial_cash on day 1
    first_price = None
    nav_history = []
    prev_nav = initial_cash

    for date in trading_dates:
        price = price_dict.get(date)
        if not price:
            continue
        if first_price is None:
            first_price = price

        nav = initial_cash * (price / first_price)
        daily_pnl = nav - prev_nav
        daily_pnl_pct = (daily_pnl / prev_nav * 100) if prev_nav > 0 else 0

        nav_history.append({
            "date": date,
            "nav": round(nav, 2),
            "daily_pnl_pct": round(daily_pnl_pct, 4),
        })
        prev_nav = nav

    if len(nav_history) < MIN_TRADING_DAYS_FOR_BENCHMARK:
        return None

    # ---- Realized period metrics (always honest, regardless of sample size) ----
    final_nav = nav_history[-1]["nav"]
    total_return = ((final_nav - initial_cash) / initial_cash) * 100

    # Max drawdown is a realized observation (worst peak-to-trough that
    # actually occurred in the window), not a statistical estimate, so it's
    # safe to report at any sample size — just label the window in the UI.
    peak = 0
    max_dd = 0
    for point in nav_history:
        if point["nav"] > peak:
            peak = point["nav"]
        dd = ((point["nav"] - peak) / peak) * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    n = len(nav_history)

    # ---- Statistical / projection metrics (gated by sample size) ----
    # Annualized return is a projection: it scales the period return up to a
    # full year. On short windows a single noisy day swings it wildly. Hide
    # it (return None) until the sample is large enough that the projection
    # is at least directionally meaningful.
    if n >= MIN_TRADING_DAYS_FOR_ANNUALIZATION:
        # Annualize using *trading* days, not calendar days, to stay
        # consistent with how Sharpe et al. are conventionally annualized
        # (sqrt(252)). Calendar-day annualization mixes incompatible units
        # with the Sharpe denominator.
        years = n / 252.0
        ann_return = ((final_nav / initial_cash) ** (1 / years) - 1) * 100
        ann_return_out = round(ann_return, 2)
    else:
        ann_return_out = None

    return {
        "symbol": bench_symbol,
        "nav_history": nav_history,
        "trading_days": n,
        "min_days_for_annualization": MIN_TRADING_DAYS_FOR_ANNUALIZATION,
        "partial": n < MIN_TRADING_DAYS_FOR_ANNUALIZATION,
        "metrics": {
            # Realized period metrics — always populated when benchmark exists.
            "total_return_pct": round(total_return, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "final_nav": round(final_nav, 2),
            # Statistical metrics — None when sample is too short to be honest.
            "annualized_return_pct": ann_return_out,
        },
    }


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
def compute_metrics(portfolio: Portfolio, initial_cash: float,
                    trading_dates: list[str]) -> dict:
    """Compute summary performance metrics."""
    nav_series = portfolio.nav_history
    if not nav_series:
        return {}

    # Defensive guard: zero-cash sleeve produces final_nav=0, initial_cash=0.
    # Returns are undefined; emit a safe-zero metric block so the portfolio
    # engine doesn't crash on multi-sleeve configs that include a dormant
    # sleeve with weight=0 and no allocation_profile activating it.
    if initial_cash <= 0:
        return {
            "total_return_pct": 0.0,
            "annualized_return_pct": None,
            "annualized_volatility_pct": None,
            "sharpe_ratio": None,
            "sharpe_ratio_annualized": None,
            "sharpe_ratio_period": None,
            "sharpe_basis": None,
            "sortino_ratio": None,
            "max_drawdown_pct": 0.0,
            "max_drawdown_date": "",
            "final_nav": 0.0,
            "trading_days": len(nav_series),
            "total_trades": 0,
            "win_rate_pct": None,
            "profit_factor": None,
            "total_entries": 0,
            "closed_trades": 0,
            "wins": 0,
            "losses": 0,
            "_note": "zero initial_cash — empty-portfolio safe metrics",
        }

    final_nav = nav_series[-1]["nav"]
    total_return = ((final_nav - initial_cash) / initial_cash) * 100

    # Annualized return — only when the sample is statistically meaningful.
    # Annualize using *trading* days (n/252) to stay consistent with the
    # sqrt(252) convention used by Sharpe/vol below.
    n_nav = len(nav_series)
    if n_nav >= MIN_TRADING_DAYS_FOR_ANNUALIZATION:
        years = n_nav / 252.0
        ann_return = ((final_nav / initial_cash) ** (1 / years) - 1) * 100
    else:
        ann_return = None

    # Max drawdown
    peak_nav = 0
    max_dd = 0
    max_dd_date = ""
    for point in nav_series:
        if point["nav"] > peak_nav:
            peak_nav = point["nav"]
        dd = ((point["nav"] - peak_nav) / peak_nav) * 100 if peak_nav > 0 else 0
        if dd < max_dd:
            max_dd = dd
            max_dd_date = point["date"]

    # Trade stats
    closed = portfolio.closed_trades
    # Exclude backtest_end trades for win rate calculation
    total_entries = len([t for t in portfolio.trades if t["action"] == "BUY"])
    real_trades = [t for t in closed if t["reason"] != "backtest_end"]
    wins = [t for t in real_trades if t["pnl"] > 0]
    losses = [t for t in real_trades if t["pnl"] <= 0]

    win_rate = (len(wins) / len(real_trades) * 100) if real_trades else 0
    all_wins = [t for t in closed if t["pnl"] > 0]
    win_rate_incl_open = (len(all_wins) / len(closed) * 100) if closed else 0
    avg_win = sum(t["pnl_pct"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnl_pct"] for t in losses) / len(losses) if losses else 0
    avg_days = sum(t["days_held"] for t in real_trades) / len(real_trades) if real_trades else 0

    # PnL by exit reason
    by_reason = defaultdict(lambda: {"count": 0, "pnl": 0})
    for t in closed:
        reason = t["reason"]
        by_reason[reason]["count"] += 1
        by_reason[reason]["pnl"] += t["pnl"]

    # Profit factor
    gross_wins = sum(t["pnl"] for t in wins)
    gross_losses = abs(sum(t["pnl"] for t in losses))
    profit_factor = min(gross_wins / gross_losses, 999.99) if gross_losses > 0 else 999.99

    # Volatility metrics (from daily NAV returns)
    daily_returns = []
    for i in range(1, len(nav_series)):
        prev_nav = nav_series[i - 1]["nav"]
        if prev_nav > 0:
            daily_returns.append((nav_series[i]["nav"] - prev_nav) / prev_nav)

    # Risk-free rate is used for Sharpe/Sortino. Loaded regardless so we can
    # report it even when downstream stats are gated off.
    from _nav_metrics import compute_nav_stats, load_risk_free_ann_pct

    risk_free_ann = load_risk_free_ann_pct(
        nav_series[0]["date"], nav_series[-1]["date"]
    )

    # Statistical risk metrics — gated by sample size for the same reason as
    # ann_return above. With <60 daily returns, std() and Sharpe have CIs on
    # the order of the point estimate; reporting them is misleading.
    _stats = compute_nav_stats(
        daily_returns=daily_returns,
        n_nav=n_nav,
        total_return_pct=total_return,
        ann_return_pct=ann_return,
        risk_free_ann_pct=risk_free_ann,
    )
    ann_vol = _stats["annualized_volatility_pct"]
    sharpe = _stats["sharpe_ratio"]
    sharpe_ann = _stats["sharpe_ratio_annualized"]
    sharpe_period = _stats["sharpe_ratio_period"]
    sharpe_basis = _stats["sharpe_basis"]
    sortino = _stats["sortino_ratio"]

    # Utilized capital metrics
    positions_values = [p["positions_value"] for p in nav_series]
    peak_utilized_capital = max(positions_values) if positions_values else 0
    avg_utilized_capital = sum(positions_values) / len(positions_values) if positions_values else 0
    total_pnl = final_nav - initial_cash
    return_on_utilized_capital_pct = (
        (total_pnl / avg_utilized_capital) * 100 if avg_utilized_capital > 0 else 0
    )
    utilization_pct = (avg_utilized_capital / initial_cash) * 100 if initial_cash > 0 else 0

    def _r(v, ndigits=2):
        return None if v is None else round(v, ndigits)

    return {
        # Realized period metrics — always populated when there's NAV data.
        "total_return_pct": round(total_return, 2),
        "max_drawdown_pct": round(max_dd, 2),
        "max_drawdown_date": max_dd_date,
        "final_nav": round(final_nav, 2),
        # Statistical metrics — None when sample is too short to be honest.
        "annualized_return_pct": _r(ann_return),
        "annualized_volatility_pct": _r(ann_vol),
        # `sharpe_ratio` is basis-aware: period sharpe for <252 trading days,
        # annualized otherwise. Side fields always populated.
        "sharpe_ratio": _r(sharpe),
        "sharpe_ratio_annualized": _r(sharpe_ann),
        "sharpe_ratio_period": _r(sharpe_period),
        "sharpe_basis": sharpe_basis,
        "sortino_ratio": _r(sortino),
        # Sample-size signal so consumers can render "—" with context.
        "trading_days": n_nav,
        "min_days_for_annualization": MIN_TRADING_DAYS_FOR_ANNUALIZATION,
        "stats_partial": n_nav < MIN_TRADING_DAYS_FOR_ANNUALIZATION,
        # Trade-based metrics — honest when there are trades.
        "total_entries": total_entries,
        "total_trades": len(real_trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(win_rate, 2),
        "win_rate_incl_open_pct": round(win_rate_incl_open, 2),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "avg_holding_days": round(avg_days, 1),
        "profit_factor": round(profit_factor, 2),
        # Capital utilization — period observation, always honest.
        "risk_free_rate_pct": round(risk_free_ann, 2),
        "peak_utilized_capital": round(peak_utilized_capital, 2),
        "avg_utilized_capital": round(avg_utilized_capital, 2),
        "utilization_pct": round(utilization_pct, 2),
        "return_on_utilized_capital_pct": round(return_on_utilized_capital_pct, 2),
        "by_exit_reason": dict(by_reason),
    }


# ---------------------------------------------------------------------------
# Report Printer
# ---------------------------------------------------------------------------
def save_results(result: dict, strategy_path: str, output_dir: str | None = None):
    """Auto-save backtest results to JSON.

    If output_dir is given, write clean-named files there (results.json,
    results_daily.json, config.json).  Otherwise fall back to the legacy
    timestamped naming under backtest/results/.
    """
    if output_dir:
        results_dir = Path(output_dir)
    else:
        results_dir = Path(os.environ.get("WORKSPACE", "/app")) / "backtest" / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    # --- build filenames ------------------------------------------------
    if output_dir:
        # Clean naming inside a dedicated run directory
        filename = "results.json"
        daily_filename = "results_daily.json"
    else:
        # Legacy: timestamped flat files
        name = result["strategy"].lower().replace(" ", "_")
        bt = result["config"]["backtest"]
        start = bt["start"].replace("-", "")
        end = bt["end"].replace("-", "")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{start}_{end}_{timestamp}.json"
        daily_filename = f"{name}_{start}_{end}_{timestamp}_daily.json"

    filepath = results_dir / filename

    # Create a clean output — strip position details from nav_history for file size
    nav_summary = []
    for point in result["nav_history"]:
        nav_summary.append({
            "date": point["date"],
            "nav": point["nav"],
            "cash": point["cash"],
            "positions_value": point["positions_value"],
            "num_positions": point["num_positions"],
            "daily_pnl": point["daily_pnl"],
            "daily_pnl_pct": point["daily_pnl_pct"],
        })

    output = {
        "strategy": result["strategy"],
        "config": result["config"],
        "metrics": result["metrics"],
        "trades": result["trades"],
        "closed_trades": result["closed_trades"],
        "open_positions": result.get("open_positions", []),
        "nav_history": nav_summary,
        "benchmark": {
            "symbol": result["benchmark"]["symbol"],
            "metrics": result["benchmark"]["metrics"],
        } if result.get("benchmark") else None,
    }

    with open(filepath, "w") as f:
        json.dump(output, f, indent=2)

    # Full daily detail (with per-position breakdown)
    daily_filepath = results_dir / daily_filename
    daily_output = {
        "strategy": result["strategy"],
        "nav_history": result["nav_history"],
        "benchmark_nav": result["benchmark"]["nav_history"] if result.get("benchmark") else None,
    }
    with open(daily_filepath, "w") as f:
        json.dump(daily_output, f, indent=2)

    # Copy strategy config into the output dir for provenance
    if output_dir:
        import shutil
        config_dst = results_dir / "config.json"
        shutil.copy2(strategy_path, config_dst)

    # Auto-index into backtest_runs table (skip deployment evals — they use output_dir)
    if not output_dir:
        try:
            from index_backtests import index_result, SCHEMA
            import sqlite3 as _sqlite3
            from db_config import APP_DB_PATH as _db
            _conn = _sqlite3.connect(str(_db))
            _conn.executescript(SCHEMA)
            index_result(_conn, filepath)
            _conn.commit()
            _conn.close()
        except Exception as e:
            print(f"  Warning: failed to index run into DB: {e}")

    return filepath, daily_filepath


def print_report(result: dict):
    """Print a formatted backtest report."""
    m = result["metrics"]
    config_name = result["strategy"]

    print()
    print("=" * 70)
    print(f"  BACKTEST REPORT: {config_name}")
    print("=" * 70)

    def _fmt(v, spec=">8.2f", suffix="%"):
        return f"{'—':>8}{suffix}" if v is None else f"{v:{spec}}{suffix}"

    print(f"\n  Performance")
    print(f"  {'Total Return:':<25} {_fmt(m.get('total_return_pct'))}")
    print(f"  {'Annualized Return:':<25} {_fmt(m.get('annualized_return_pct'))}")
    print(f"  {'Max Drawdown:':<25} {_fmt(m.get('max_drawdown_pct'))}  ({m.get('max_drawdown_date', '')})")
    print(f"  {'Final NAV:':<25} ${m['final_nav']:>12,.2f}")
    print(f"  {'Profit Factor:':<25} {m['profit_factor']:>8.2f}")

    # Benchmark comparison
    if m.get("benchmark_return_pct") is not None:
        print(f"\n  Benchmark (S&P 500)")
        print(f"  {'Benchmark Return:':<25} {_fmt(m.get('benchmark_return_pct'))}")
        print(f"  {'Benchmark Ann. Return:':<25} {_fmt(m.get('benchmark_ann_return_pct'))}")
        print(f"  {'Alpha (annualized):':<25} {_fmt(m.get('alpha_ann_pct'))}")

    print(f"\n  Trade Statistics")
    print(f"  {'Total Trades:':<25} {m['total_trades']:>8}")
    print(f"  {'Wins:':<25} {m['wins']:>8}")
    print(f"  {'Losses:':<25} {m['losses']:>8}")
    print(f"  {'Win Rate:':<25} {m['win_rate_pct']:>8.1f}%")
    print(f"  {'Avg Win:':<25} {m['avg_win_pct']:>8.2f}%")
    print(f"  {'Avg Loss:':<25} {m['avg_loss_pct']:>8.2f}%")
    print(f"  {'Avg Holding Period:':<25} {m['avg_holding_days']:>8.1f} days")

    print(f"\n  Exits by Reason")
    for reason, stats in m.get("by_exit_reason", {}).items():
        print(f"  {'  ' + reason + ':':<25} {stats['count']:>4} trades  ${stats['pnl']:>12,.2f}")

    # Print last 20 trades
    trades = result["closed_trades"]
    real_trades = [t for t in trades if t["reason"] != "backtest_end"]
    if real_trades:
        print(f"\n  Recent Trades (last 20)")
        print(f"  {'Date':<12} {'Symbol':<8} {'Entry $':>8} {'Exit $':>8} {'PnL%':>7} {'Days':>5} {'Reason':<15}")
        print("  " + "-" * 68)
        for t in real_trades[-20:]:
            print(f"  {t['date']:<12} {t['symbol']:<8} ${t['entry_price']:>7.2f} "
                  f"${t['price']:>7.2f} {t['pnl_pct']:>6.1f}% {t['days_held']:>5} {t['reason']:<15}")

    # NAV at key dates
    nav_hist = result["nav_history"]
    if nav_hist:
        print(f"\n  NAV Snapshots")
        step = max(1, len(nav_hist) // 10)
        print(f"  {'Date':<12} {'NAV':>14} {'Positions':>10}")
        print("  " + "-" * 40)
        for i in range(0, len(nav_hist), step):
            n = nav_hist[i]
            print(f"  {n['date']:<12} ${n['nav']:>13,.2f} {n['num_positions']:>10}")
        # Always print last
        n = nav_hist[-1]
        print(f"  {n['date']:<12} ${n['nav']:>13,.2f} {n['num_positions']:>10}")

    print()
    print("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="AlphaScout Backtest Engine")
    parser.add_argument("strategy", type=str, help="Path to strategy JSON config")
    parser.add_argument("--start", type=str, help="Override backtest start date")
    parser.add_argument("--end", type=str, help="Override backtest end date")
    parser.add_argument("--allocation", type=float, help="Override initial allocation")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument("--output-dir", type=str, help="Directory for results (clean naming: results.json, config.json, charts)")

    args = parser.parse_args()

    config = load_strategy(args.strategy)

    # Apply CLI overrides
    if args.start:
        config["backtest"]["start"] = args.start
    if args.end:
        config["backtest"]["end"] = args.end
    if args.allocation:
        config["sizing"]["initial_allocation"] = args.allocation

    result = run_backtest(config)

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print_report(result)

    # Auto-save results
    filepath, daily_filepath = save_results(result, args.strategy, output_dir=args.output_dir)
    print(f"\n  Results saved to: {filepath}")
    print(f"  Daily detail saved to: {daily_filepath}")

    # Persist trades to DB (single source of truth)
    try:
        from deploy_engine import persist_trades
        run_id = filepath.stem if hasattr(filepath, 'stem') else str(filepath).rsplit('/', 1)[-1].replace('.json', '')
        all_trades = result.get("trades", [])
        if all_trades:
            n = persist_trades("backtest", run_id, all_trades)
            print(f"  💾 {n} trade(s) persisted to DB")
    except Exception as e:
        print(f"  ⚠ Trade persist failed: {e}")


if __name__ == "__main__":
    main()
