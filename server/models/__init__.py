"""
AlphaScout domain models — single source of truth.

Import from here:
    from server.models import StrategyConfig, PortfolioConfig, BacktestResult
"""

# Strategy
from .strategy import (
    # Entry conditions
    CurrentDropCondition,
    PeriodDropCondition,
    DailyDropCondition,
    SelloffCondition,
    EarningsMomentumCondition,
    PePercentileCondition,
    RevenueGrowthCondition,
    RevenueAcceleratingCondition,
    MarginExpandingCondition,
    MarginTurnaroundCondition,
    RelativePerformanceCondition,
    VolumeConvictionCondition,
    RsiCondition,
    MomentumRankCondition,
    MaCrossoverCondition,
    VolumCapitulationCondition,
    AlwaysCondition,
    EntryCondition,
    # Exit conditions
    RevenueDecelerationExit,
    MarginCollapseExit,
    ExitCondition,
    # Sub-configs
    UniverseConfig,
    EntryConfig,
    StopLossConfig,
    TakeProfitConfig,
    TimeStopConfig,
    RankingConfig,
    RebalancingRules,
    RebalancingConfig,
    SizingConfig,
    BacktestParams,
    # Full config
    StrategyConfig,
)

# Regime
from .regime import RegimeCondition, RegimeConfig

# Portfolio
from .portfolio import (
    SleeveConfig,
    AllocationProfile,
    InlineRegimeDefinition,
    PortfolioConfig,
)

# Backtest results
from .backtest import (
    TradeRecord,
    PositionSnapshot,
    DailySnapshot,
    BacktestMetrics,
    BenchmarkResult,
    BacktestResult,
    SleeveResult,
    PortfolioBacktestResult,
)

# Deployment
from .deployment import (
    Deployment,
    DeploymentSleeve,
    TradeAlert,
    TradeExecution,
)

# Market data
from .market_data import Price, CompanyProfile, MacroDataPoint
