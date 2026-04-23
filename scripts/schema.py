"""
AlphaScout App Database Schema — single source of truth.

Defines all tables, indexes, and foreign keys for the app database (app.db).
This file is the authoritative reference for the database structure.
Market data tables (prices, fundamentals, etc.) are in market.db and managed
by build_db.py / daily_update.py / macro_data.py — not here.

Usage:
    from schema import init_db

    conn = sqlite3.connect("app.db")
    init_db(conn)

Tables by domain:
    Core entities:      strategies, portfolios, regimes
    Backtests:          backtest_runs, portfolio_backtest_runs
    Deployments:        deployments, sleeves, deployed_strategies, portfolio_deployments
    Trades:             trades, trade_alerts, trade_executions
    Regime monitoring:  regime_deployments, regime_state_history, regime_alerts
    Auto-trader:        auto_trader_agents, auto_trader_templates, auto_trader_runs, experiments
    Reference:          universe_profiles
"""

import sqlite3


# ---------------------------------------------------------------------------
# Core entities — strategies, portfolios, regimes
# Config stored as JSON TEXT blobs, validated by Pydantic domain models.
# ---------------------------------------------------------------------------

STRATEGIES = """
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id     TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    config          TEXT NOT NULL,       -- JSON: full StrategyConfig
    created_at      TEXT,
    updated_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_strat_name ON strategies(name);
"""

PORTFOLIOS = """
CREATE TABLE IF NOT EXISTS portfolios (
    portfolio_id    TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    config          TEXT NOT NULL,       -- JSON: full PortfolioConfig
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

REGIMES = """
CREATE TABLE IF NOT EXISTS regimes (
    regime_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    config          TEXT NOT NULL,       -- JSON: RegimeConfig
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


# ---------------------------------------------------------------------------
# Backtest results
# ---------------------------------------------------------------------------

BACKTEST_RUNS = """
CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id                          TEXT PRIMARY KEY,
    type                            TEXT NOT NULL DEFAULT 'portfolio',
    name                            TEXT NOT NULL,
    config_json                     TEXT NOT NULL,
    start_date                      TEXT NOT NULL,
    end_date                        TEXT NOT NULL,
    initial_capital                 REAL NOT NULL,
    final_nav                       REAL,
    total_return_pct                REAL,
    annualized_return_pct           REAL,
    annualized_volatility_pct       REAL,
    max_drawdown_pct                REAL,
    max_drawdown_date               TEXT,
    sharpe_ratio                    REAL,
    sortino_ratio                   REAL,
    calmar_ratio                    REAL,
    profit_factor                   REAL,
    total_entries                   INTEGER,
    closed_trades                   INTEGER,
    wins                            INTEGER,
    losses                          INTEGER,
    win_rate_pct                    REAL,
    avg_holding_days                REAL,
    utilization_pct                 REAL,
    trading_days                    INTEGER,
    benchmark_return_pct            REAL,
    alpha_ann_pct                   REAL,
    num_sleeves                     INTEGER DEFAULT 1,
    per_sleeve_json                 TEXT,
    results_path                    TEXT,
    created_at                      TEXT NOT NULL,
    -- Legacy columns (from index_backtests.py, kept for backward compat)
    strategy_name                   TEXT,
    strategy_id                     TEXT,
    author_id                       TEXT,
    author_name                     TEXT,
    universe_type                   TEXT,
    universe_detail                 TEXT,
    entry_type                      TEXT,
    entry_threshold                 REAL,
    entry_window                    INTEGER,
    stop_loss                       REAL,
    take_profit                     REAL,
    time_stop                       INTEGER,
    max_positions                   INTEGER,
    capital                         REAL,
    rebalance_freq                  TEXT,
    slippage_bps                    REAL,
    total_return                    REAL,
    ann_return                      REAL,
    alpha                           REAL,
    max_drawdown                    REAL,
    sharpe                          REAL,
    sortino                         REAL,
    win_rate                        REAL,
    total_trades                    INTEGER,
    avg_win_pct                     REAL,
    avg_loss_pct                    REAL,
    final_nav_legacy                REAL,
    benchmark_return                REAL,
    peak_utilized_capital           REAL,
    avg_utilized_capital            REAL,
    return_on_utilized_capital_pct  REAL,
    has_report                      INTEGER DEFAULT 0,
    has_analysis                    INTEGER DEFAULT 0,
    has_charts                      INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_backtest_runs_type ON backtest_runs(type);
CREATE INDEX IF NOT EXISTS idx_backtest_runs_created ON backtest_runs(created_at);
CREATE INDEX IF NOT EXISTS idx_br_strategy ON backtest_runs(strategy_name);
CREATE INDEX IF NOT EXISTS idx_br_strategy_id ON backtest_runs(strategy_id);
CREATE INDEX IF NOT EXISTS idx_br_universe ON backtest_runs(universe_type, universe_detail);
CREATE INDEX IF NOT EXISTS idx_br_alpha ON backtest_runs(alpha);
CREATE INDEX IF NOT EXISTS idx_br_sharpe ON backtest_runs(sharpe);
CREATE INDEX IF NOT EXISTS idx_br_created ON backtest_runs(created_at);
"""

PORTFOLIO_BACKTEST_RUNS = """
CREATE TABLE IF NOT EXISTS portfolio_backtest_runs (
    run_id                      TEXT PRIMARY KEY,
    portfolio_id                TEXT NOT NULL,
    portfolio_name              TEXT NOT NULL,
    created_at                  TEXT NOT NULL,
    start_date                  TEXT NOT NULL,
    end_date                    TEXT NOT NULL,
    initial_capital             REAL NOT NULL,
    final_nav                   REAL,
    total_return_pct            REAL,
    annualized_return_pct       REAL,
    annualized_volatility_pct   REAL,
    max_drawdown_pct            REAL,
    max_drawdown_date           TEXT,
    sharpe_ratio                REAL,
    sortino_ratio               REAL,
    calmar_ratio                REAL,
    profit_factor               REAL,
    total_entries               INTEGER,
    closed_trades               INTEGER,
    wins                        INTEGER,
    losses                      INTEGER,
    win_rate_pct                REAL,
    avg_holding_days            REAL,
    utilization_pct             REAL,
    trading_days                INTEGER,
    benchmark_return_pct        REAL,
    alpha_ann_pct               REAL,
    regime_transitions          INTEGER,
    num_sleeves                 INTEGER,
    per_sleeve_json             TEXT,
    config_json                 TEXT,
    results_path                TEXT
);
CREATE INDEX IF NOT EXISTS idx_pbt_portfolio_id ON portfolio_backtest_runs(portfolio_id);
CREATE INDEX IF NOT EXISTS idx_pbt_created_at ON portfolio_backtest_runs(created_at);
"""


# ---------------------------------------------------------------------------
# Deployments — live paper-trading
# ---------------------------------------------------------------------------

DEPLOYMENTS = """
CREATE TABLE IF NOT EXISTS deployments (
    id                              TEXT PRIMARY KEY,
    type                            TEXT NOT NULL DEFAULT 'portfolio',
    name                            TEXT NOT NULL,
    portfolio_id                    TEXT,                       -- FK → portfolios.portfolio_id (lineage)
    config_json                     TEXT NOT NULL,              -- frozen snapshot at deploy time
    start_date                      TEXT NOT NULL,
    initial_capital                 REAL NOT NULL,
    status                          TEXT NOT NULL DEFAULT 'active',
    created_at                      TEXT NOT NULL,
    updated_at                      TEXT NOT NULL,
    -- Latest evaluation metrics
    last_evaluated                  TEXT,
    last_nav                        REAL,
    last_return_pct                 REAL,
    last_alpha_pct                  REAL,
    last_benchmark_return_pct       REAL,
    last_sharpe_ratio               REAL,
    -- basis-aware sharpe: `last_sharpe_ratio` holds the displayed value
    -- (period for <252 trading days, annualized otherwise). The side fields
    -- are always populated so the UI can show both if it wants.
    last_sharpe_basis               TEXT,
    last_sharpe_ratio_annualized    REAL,
    last_max_drawdown_pct           REAL,
    last_ann_volatility_pct         REAL,
    rolling_vol_30d_pct             REAL,
    total_trades                    INTEGER DEFAULT 0,
    open_positions                  INTEGER DEFAULT 0,
    current_utilization_pct         REAL,
    peak_utilized_capital           REAL,
    avg_utilized_capital            REAL,
    utilization_pct                 REAL,
    return_on_utilized_capital_pct  REAL,
    -- Dual benchmark
    alpha_vs_market_pct             REAL,
    alpha_vs_sector_pct             REAL,
    market_benchmark_return_pct     REAL,
    sector_benchmark_return_pct     REAL,
    -- Portfolio-specific
    active_regimes                  TEXT,
    sleeve_summary                  TEXT,
    num_sleeves                     INTEGER DEFAULT 1,
    -- Alerts
    alert_mode                      INTEGER DEFAULT 0,
    error                           TEXT
);
CREATE INDEX IF NOT EXISTS idx_deployments_status ON deployments(status);
CREATE INDEX IF NOT EXISTS idx_deployments_type ON deployments(type);
CREATE INDEX IF NOT EXISTS idx_deployments_portfolio ON deployments(portfolio_id);
"""

SLEEVES = """
CREATE TABLE IF NOT EXISTS sleeves (
    sleeve_id           TEXT PRIMARY KEY,
    portfolio_id        TEXT,
    deployment_id       TEXT,
    source_type         TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    label               TEXT NOT NULL,
    strategy_id         TEXT,
    config_json         TEXT,
    weight              REAL NOT NULL,
    regime_gate         TEXT,
    allocated_capital   REAL,
    is_active           INTEGER DEFAULT 1,
    last_nav            REAL,
    last_return_pct     REAL,
    sharpe              REAL,
    max_drawdown_pct    REAL,
    profit_factor       REAL,
    win_rate_pct        REAL,
    total_trades        INTEGER DEFAULT 0,
    closed_trades       INTEGER DEFAULT 0,
    wins                INTEGER DEFAULT 0,
    losses              INTEGER DEFAULT 0,
    active_days         INTEGER DEFAULT 0,
    gated_off_days      INTEGER DEFAULT 0,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sleeves_deployment ON sleeves(deployment_id);
CREATE INDEX IF NOT EXISTS idx_sleeves_portfolio ON sleeves(portfolio_id);
CREATE INDEX IF NOT EXISTS idx_sleeves_source ON sleeves(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_sleeves_strategy ON sleeves(strategy_id);
"""


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------

TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    id                  TEXT PRIMARY KEY,
    source_type         TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    deployment_type     TEXT,
    sleeve_label        TEXT,
    date                TEXT NOT NULL,
    action              TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    shares              REAL NOT NULL,
    price               REAL NOT NULL,
    amount              REAL,
    reason              TEXT,
    signal_detail       TEXT,
    entry_date          TEXT,
    entry_price         REAL,
    pnl                 REAL,
    pnl_pct             REAL,
    days_held           INTEGER,
    linked_trade_id     TEXT,
    created_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_source ON trades(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol, date);
CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(date);
CREATE INDEX IF NOT EXISTS idx_trades_deployment ON trades(source_id, date);
"""

TRADE_ALERTS = """
CREATE TABLE IF NOT EXISTS trade_alerts (
    id                  TEXT PRIMARY KEY,
    deployment_id       TEXT NOT NULL,
    date                TEXT NOT NULL,
    action              TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    shares              REAL NOT NULL,
    target_price        REAL NOT NULL,
    amount              REAL,
    reason              TEXT,
    signal_detail       TEXT,
    entry_date          TEXT,
    entry_price         REAL,
    pnl_pct             REAL,
    pnl                 REAL,
    days_held           INTEGER,
    sleeve_label        TEXT,
    created_at          TEXT NOT NULL,
    FOREIGN KEY (deployment_id) REFERENCES deployments(id)
);
CREATE INDEX IF NOT EXISTS idx_alerts_deploy_date ON trade_alerts(deployment_id, date);
CREATE INDEX IF NOT EXISTS idx_alerts_date ON trade_alerts(date);
"""

TRADE_EXECUTIONS = """
CREATE TABLE IF NOT EXISTS trade_executions (
    id                  TEXT PRIMARY KEY,
    alert_id            TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    fill_price          REAL,
    fill_time           TEXT,
    fill_shares         REAL,
    broker              TEXT DEFAULT 'manual',
    slippage_pct        REAL,
    notes               TEXT,
    updated_at          TEXT NOT NULL,
    FOREIGN KEY (alert_id) REFERENCES trade_alerts(id)
);
CREATE INDEX IF NOT EXISTS idx_executions_alert ON trade_executions(alert_id);
CREATE INDEX IF NOT EXISTS idx_executions_status ON trade_executions(status);
"""


# ---------------------------------------------------------------------------
# Regime monitoring
# ---------------------------------------------------------------------------

REGIME_DEPLOYMENTS = """
CREATE TABLE IF NOT EXISTS regime_deployments (
    id                      TEXT PRIMARY KEY,
    regime_id               TEXT NOT NULL,
    regime_name             TEXT NOT NULL,
    config_json             TEXT NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'active',
    alert_mode              INTEGER DEFAULT 0,
    is_active               INTEGER,
    last_evaluated          TEXT,
    last_detail             TEXT,
    total_evaluated_days    INTEGER DEFAULT 0,
    total_active_days       INTEGER DEFAULT 0,
    last_activated_date     TEXT,
    last_deactivated_date   TEXT,
    error                   TEXT,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS regime_state_history (
    deployment_id   TEXT NOT NULL,
    date            TEXT NOT NULL,
    is_active       INTEGER NOT NULL,
    PRIMARY KEY (deployment_id, date),
    FOREIGN KEY (deployment_id) REFERENCES regime_deployments(id)
);

CREATE TABLE IF NOT EXISTS regime_alerts (
    id              TEXT PRIMARY KEY,
    deployment_id   TEXT NOT NULL,
    date            TEXT NOT NULL,
    transition      TEXT NOT NULL,
    regime_name     TEXT NOT NULL,
    detail          TEXT,
    created_at      TEXT NOT NULL,
    FOREIGN KEY (deployment_id) REFERENCES regime_deployments(id)
);
CREATE INDEX IF NOT EXISTS idx_regime_alerts_deploy ON regime_alerts(deployment_id, date);
"""


# ---------------------------------------------------------------------------
# Auto-trader
# ---------------------------------------------------------------------------

AUTO_TRADER = """
CREATE TABLE IF NOT EXISTS auto_trader_templates (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,
    description     TEXT NOT NULL,
    prompt          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_att_category ON auto_trader_templates(category);

CREATE TABLE IF NOT EXISTS auto_trader_agents (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    prompt          TEXT NOT NULL,
    allowed_tools   TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS auto_trader_runs (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    agent_id            TEXT NOT NULL DEFAULT 'default',
    status              TEXT NOT NULL DEFAULT 'pending',
    config              TEXT NOT NULL,
    current_iteration   INTEGER DEFAULT 0,
    max_experiments     INTEGER NOT NULL,
    best_metric_value   REAL,
    best_experiment_id  TEXT,
    pid                 INTEGER,
    error               TEXT,
    started_at          TEXT,
    completed_at        TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    FOREIGN KEY (agent_id) REFERENCES auto_trader_agents(id)
);
CREATE INDEX IF NOT EXISTS idx_atr_status ON auto_trader_runs(status);
CREATE INDEX IF NOT EXISTS idx_atr_agent ON auto_trader_runs(agent_id);
"""

EXPERIMENTS = """
CREATE TABLE IF NOT EXISTS experiments (
    id                              TEXT PRIMARY KEY,
    run_id                          TEXT NOT NULL,
    iteration                       INTEGER NOT NULL,
    -- Agent output
    thesis                          TEXT,
    assumptions                     TEXT,
    lessons                         TEXT,              -- agent's reflection on prior experiments (UI-only; not fed back)
    portfolio_id                    TEXT,              -- FK → portfolios.portfolio_id (lineage)
    portfolio_config                TEXT,              -- legacy: will be phased out, use portfolio_id instead
    -- Optimization target
    target_metric                   TEXT,
    target_value                    REAL,
    conditions                      TEXT,
    conditions_met                  INTEGER,
    -- Backtest metrics
    total_return_pct                REAL,
    annualized_return_pct           REAL,
    -- `sharpe_ratio` is basis-aware (period if backtest spans <252 trading
    -- days, annualized otherwise). Side fields always populated.
    sharpe_ratio                    REAL,
    sharpe_basis                    TEXT,
    sharpe_ratio_annualized         REAL,
    sharpe_ratio_period             REAL,
    sortino_ratio                   REAL,
    max_drawdown_pct                REAL,
    annualized_volatility_pct       REAL,
    alpha_ann_pct                   REAL,
    alpha_vs_market_pct             REAL,
    alpha_vs_sector_pct             REAL,
    market_benchmark_return_pct     REAL,
    market_benchmark_ann_return_pct REAL,
    sector_benchmark_return_pct     REAL,
    sector_benchmark_ann_return_pct REAL,
    profit_factor                   REAL,
    win_rate_pct                    REAL,
    total_trades                    INTEGER,
    -- Decision
    decision                        TEXT NOT NULL,
    best_value_so_far               REAL,
    improvement_pct                 REAL,
    -- Backtest config
    backtest_start                  TEXT,
    backtest_end                    TEXT,
    initial_capital                 REAL,
    -- Meta
    model                           TEXT,
    session_id                      TEXT,
    tokens_used                     INTEGER,
    duration_seconds                REAL,
    error                           TEXT,
    created_at                      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_experiments_run ON experiments(run_id, iteration);
CREATE INDEX IF NOT EXISTS idx_experiments_decision ON experiments(run_id, decision);
CREATE INDEX IF NOT EXISTS idx_experiments_portfolio ON experiments(portfolio_id);
"""


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

UNIVERSE_PROFILES = """
CREATE TABLE IF NOT EXISTS universe_profiles (
    symbol              TEXT PRIMARY KEY,
    name                TEXT NOT NULL DEFAULT '',
    sector              TEXT NOT NULL DEFAULT '',
    industry            TEXT NOT NULL DEFAULT '',
    market_cap          REAL,
    exchange            TEXT NOT NULL DEFAULT '',
    country             TEXT NOT NULL DEFAULT '',
    beta                REAL,
    price               REAL,
    volume              INTEGER,
    avg_volume          INTEGER,
    is_actively_trading INTEGER DEFAULT 1,
    ipo_date            TEXT,
    is_etf              INTEGER DEFAULT 0,
    is_adr              INTEGER DEFAULT 0,
    cik                 TEXT,
    description         TEXT,
    synced_at           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_up_sector ON universe_profiles(sector);
CREATE INDEX IF NOT EXISTS idx_up_industry ON universe_profiles(industry);
CREATE INDEX IF NOT EXISTS idx_up_market_cap ON universe_profiles(market_cap);
CREATE INDEX IF NOT EXISTS idx_up_exchange ON universe_profiles(exchange);
CREATE INDEX IF NOT EXISTS idx_up_country ON universe_profiles(country);
CREATE INDEX IF NOT EXISTS idx_up_sector_mcap ON universe_profiles(sector, market_cap);
"""


# ---------------------------------------------------------------------------
# Legacy tables (kept for backward compat, not used by new code)
# ---------------------------------------------------------------------------

LEGACY = """
CREATE TABLE IF NOT EXISTS deployed_strategies (
    id                              TEXT PRIMARY KEY,
    strategy_name                   TEXT NOT NULL,
    config_json                     TEXT NOT NULL,
    start_date                      TEXT NOT NULL,
    initial_capital                 REAL NOT NULL,
    status                          TEXT NOT NULL DEFAULT 'active',
    created_at                      TEXT NOT NULL,
    updated_at                      TEXT NOT NULL,
    last_evaluated                  TEXT,
    last_nav                        REAL,
    last_return_pct                 REAL,
    total_trades                    INTEGER DEFAULT 0,
    open_positions                  INTEGER DEFAULT 0,
    error                           TEXT,
    strategy_id                     TEXT,
    peak_utilized_capital           REAL,
    avg_utilized_capital            REAL,
    utilization_pct                 REAL,
    return_on_utilized_capital_pct  REAL,
    last_alpha_pct                  REAL,
    last_benchmark_return_pct       REAL,
    last_sharpe_ratio               REAL,
    last_ann_volatility_pct         REAL,
    current_utilization_pct         REAL,
    rolling_vol_30d_pct             REAL,
    alert_mode                      INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ds_strategy_id ON deployed_strategies(strategy_id);

CREATE TABLE IF NOT EXISTS portfolio_deployments (
    id                          TEXT PRIMARY KEY,
    portfolio_id                TEXT NOT NULL,
    portfolio_name              TEXT NOT NULL,
    config_json                 TEXT NOT NULL,
    start_date                  TEXT NOT NULL,
    initial_capital             REAL NOT NULL,
    status                      TEXT NOT NULL DEFAULT 'active',
    last_evaluated              TEXT,
    last_nav                    REAL,
    last_return_pct             REAL,
    last_alpha_pct              REAL,
    last_benchmark_return_pct   REAL,
    last_sharpe_ratio           REAL,
    last_max_drawdown_pct       REAL,
    active_regimes              TEXT,
    sleeve_summary              TEXT,
    error                       TEXT,
    created_at                  TEXT NOT NULL,
    updated_at                  TEXT NOT NULL,
    alert_mode                  INTEGER DEFAULT 0
);
"""


# ---------------------------------------------------------------------------
# All schemas combined
# ---------------------------------------------------------------------------

ALL_SCHEMAS = [
    STRATEGIES,
    PORTFOLIOS,
    REGIMES,
    BACKTEST_RUNS,
    PORTFOLIO_BACKTEST_RUNS,
    DEPLOYMENTS,
    SLEEVES,
    TRADES,
    TRADE_ALERTS,
    TRADE_EXECUTIONS,
    REGIME_DEPLOYMENTS,
    AUTO_TRADER,
    EXPERIMENTS,
    LEGACY,
    # Note: universe_profiles is in market.db, not app.db — managed by server/api.py
]


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, coltype: str):
    """Add a column to an existing table if it doesn't already exist."""
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")


def _apply_migrations(conn: sqlite3.Connection):
    """Add columns to existing tables that don't have them yet.
    Run BEFORE schema CREATEs so subsequent index creation on those columns works."""
    # Check each table exists first (may not if DB is brand new)
    existing_tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "experiments" in existing_tables:
        _add_column_if_missing(conn, "experiments", "portfolio_id", "TEXT")
        _add_column_if_missing(conn, "experiments", "lessons", "TEXT")
        _add_column_if_missing(conn, "experiments", "sharpe_basis", "TEXT")
        _add_column_if_missing(conn, "experiments", "sharpe_ratio_annualized", "REAL")
        _add_column_if_missing(conn, "experiments", "sharpe_ratio_period", "REAL")
    if "deployments" in existing_tables:
        _add_column_if_missing(conn, "deployments", "portfolio_id", "TEXT")
        _add_column_if_missing(conn, "deployments", "last_sharpe_basis", "TEXT")
        _add_column_if_missing(conn, "deployments", "last_sharpe_ratio_annualized", "REAL")
    if "auto_trader_agents" in existing_tables:
        _add_column_if_missing(conn, "auto_trader_agents", "allowed_tools", "TEXT")


def init_db(conn: sqlite3.Connection):
    """Create all tables and indexes. Idempotent (IF NOT EXISTS + ALTER)."""
    # Run migrations first so subsequent indexes on new columns work
    _apply_migrations(conn)

    for schema in ALL_SCHEMAS:
        conn.executescript(schema)
    conn.commit()
