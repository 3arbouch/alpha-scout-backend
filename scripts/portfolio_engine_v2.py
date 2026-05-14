"""
PortfolioEngineV2 — unified-position-book executor.

Phase 2 of the live-trading plan. Replaces v1's per-sleeve mini-simulations
+ portfolio-level lerp with a single executor that:

  1. Maintains ONE PositionBook for the whole portfolio (provenance-tagged).
  2. Reads directives from sleeve_signals (entry candidates, exits,
     rebalance trims/adds).
  3. Emits ONE unified trade ledger that represents what the broker would
     actually execute.

NO dual bookkeeping. NO phantom trades. NO post-hoc reconciliation.

Backward compatibility: v1's run_portfolio_backtest in scripts/portfolio_engine.py
is untouched. v2 is opt-in via `engine_version: "v2"` in the portfolio config.

STATUS (Phase 2 Step 3a): single-sleeve, no-regime, fixed-weight portfolios
only. Multi-sleeve and regime/allocation_profile support added in 3b-3e.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))

from backtest_engine import (
    _calendar_to_trading_days,
    _find_recent_peak as _v1_find_recent_peak,
    _load_feature_series,
    _load_pe_timeseries,
    build_price_index,
    compute_benchmark,
    compute_metrics,
    get_connection,
    is_rebalance_date,
    load_earnings_data,
    precompute_signals,
    resolve_universe,
    stamp_strategy_id,
    validate_strategy,
    _precompute_exit_signals,
    SECTOR_ETF_MAP,
)
from position_book import PositionBook
from sleeve_signals import (
    SleeveRuntimeState,
    get_entry_candidates,
    get_exit_recommendations,
    get_rebalance_directives,
)
from stop_pricing import compute_realized_vol, compute_stop_pricing


# ---------------------------------------------------------------------------
# Per-sleeve precomputed state used during the daily loop.
# ---------------------------------------------------------------------------
class _SleeveContext:
    """Bundles everything the executor needs about one sleeve.

    Built once at startup; reused inside the daily loop. Holds precomputed
    signals, exit signals, factor preloads (for composite_score), and
    runtime state (cooldowns, last rebal date).
    """

    def __init__(
        self,
        sleeve_def: dict,           # {label, weight, regime_gate, strategy_config}
        signals: dict,              # precompute_signals output
        signal_metadata: dict,
        exit_signals: dict,         # _precompute_exit_signals output
        pe_series: dict | None,
        composite_series: dict | None,
        symbols: list[str],
    ):
        self.label = sleeve_def["label"]
        self.weight = float(sleeve_def.get("weight", 1.0))
        self.regime_gate = list(sleeve_def.get("regime_gate", []))
        self.config = sleeve_def["strategy_config"]
        self.symbols = symbols
        self.signals = signals
        self.signal_metadata = signal_metadata
        self.exit_signals = exit_signals
        self.pe_series = pe_series
        self.composite_series = composite_series
        self.state = SleeveRuntimeState(sleeve_label=self.label)
        # pending entries from yesterday's signals (next_close/next_open mode)
        self.pending_entries: list = []  # list of EntryDirective
        # Allocated capital (in fixed-weight mode = weight × initial_capital)
        self.allocated_capital: float = 0.0
        # Bookkeeping
        self.entry_mode: str = self.config["backtest"].get("entry_price", "next_close")
        self.slippage_bps: float = self.config["backtest"].get("slippage_bps", 10)


# ---------------------------------------------------------------------------
# Sizing helpers
# ---------------------------------------------------------------------------
def _compute_risk_parity_weights(
    sleeve: _SleeveContext,
    pending: list,
    price_index: dict,
    fill_date: str,
) -> dict[str, float]:
    """Inverse-vol weights for a risk_parity batch. Mirrors v1's
    backtest_engine.py:2347-2370 exactly. Empty dict if insufficient history.
    """
    cfg = sleeve.config
    if cfg["sizing"]["type"] != "risk_parity" or not pending:
        return {}
    vol_window = int(cfg["sizing"].get("vol_window_days", 20))
    vol_source = cfg["sizing"].get("vol_source", "historical")
    sigmas: dict[str, float] = {}
    for d in pending:
        pm = price_index.get(d.symbol, {})
        if not pm:
            continue
        closes_dates = sorted(date for date in pm if date < fill_date)
        tail = closes_dates[-(vol_window + 1):] if len(closes_dates) >= vol_window + 1 else []
        if not tail:
            continue
        closes = [pm[date] for date in tail]
        sigma = compute_realized_vol(closes, vol_window, vol_source)
        if sigma is not None and sigma > 0:
            sigmas[d.symbol] = sigma
    if not sigmas:
        return {}
    inv = {s: 1.0 / sig for s, sig in sigmas.items()}
    total = sum(inv.values())
    return {s: v / total for s, v in inv.items()}


# Use v1's _find_recent_peak directly (window-aware, calendar→trading-days
# conversion, includes today's price in the lookback) so the peak_price
# reference is byte-identical between engines.
_find_recent_peak = _v1_find_recent_peak


# ---------------------------------------------------------------------------
# Pending-entry execution: fills BUY orders from yesterday's signal directives.
# ---------------------------------------------------------------------------
def _execute_pending_entries(
    sleeve: _SleeveContext,
    book: PositionBook,
    price_index: dict,
    open_index: dict,
    date: str,
) -> list[dict]:
    """Process a sleeve's pending entries from yesterday, filling at today's
    close (next_close) or today's open (next_open).

    Replicates v1's per-symbol fill loop exactly:
      - current_nav recomputed EACH ITERATION (slippage drag shrinks pool)
      - amount = current_nav / max_positions (equal_weight) — fresh per iter
      - risk_parity: pool = (n_batch / max_positions) × current_nav per iter
      - amount = min(amount, portfolio.cash * 0.99) — cash buffer cap
      - min position size $1000 (matches v1)

    Returns the list of BUY trade records executed.
    """
    if not sleeve.pending_entries:
        return []

    pending = sleeve.pending_entries
    sleeve.pending_entries = []

    cfg = sleeve.config
    sizing_type = cfg["sizing"]["type"]
    max_positions = cfg["sizing"].get("max_positions", 10)
    initial_alloc = cfg["sizing"].get("initial_allocation", sleeve.allocated_capital)
    fill_index = open_index if sleeve.entry_mode == "next_open" else price_index

    # Drop entries already held / cap to slot count
    held = book.symbols_held_by_sleeve(sleeve.label)
    pending = [d for d in pending if d.symbol not in held]
    available = max_positions - len(held)
    if available <= 0:
        return []
    pending = pending[:available]

    # Risk-parity weights — computed ONCE per batch (matches v1)
    rp_weights = (_compute_risk_parity_weights(sleeve, pending, price_index, date)
                  if sizing_type == "risk_parity" else {})

    # max_position_pct from rebalance rules (applied as a per-position cap)
    rules = (cfg.get("rebalancing") or {}).get("rules") or {}
    max_pct = float(rules.get("max_position_pct", 100))

    n_batch = len(pending)
    trades: list[dict] = []
    for d in pending:
        price = fill_index.get(d.symbol, {}).get(date)
        if price is None or price <= 0:
            continue

        # Stop position count from exceeding max_positions (rebalance may have
        # opened in same iteration). Defensive: v1 also breaks here.
        if book.num_positions(sleeve.label) >= max_positions:
            break

        # ---- v1-equivalent per-iteration NAV-based sizing ------------------
        current_nav = book.nav(price_index, date)
        if current_nav <= 0:
            continue

        if sizing_type == "equal_weight":
            amount = current_nav / max_positions
        elif sizing_type == "fixed_amount":
            amount = cfg["sizing"].get("amount_per_position", initial_alloc / max_positions)
        elif sizing_type == "risk_parity":
            pool = (n_batch / max_positions) * current_nav
            w = rp_weights.get(d.symbol)
            if w is not None:
                amount = pool * w
            else:
                # Insufficient vol history → equal-weight slot fallback
                amount = current_nav / max_positions
        else:
            amount = current_nav / max_positions

        # max_position_pct cap
        if max_pct < 100 and (amount / current_nav) * 100 > max_pct:
            amount = current_nav * (max_pct / 100.0)

        # Cash buffer cap (matches v1's `amount = min(amount, portfolio.cash * 0.99)`)
        # NOTE: PositionBook.open ALSO applies this cap internally; we replicate
        # it here so the min-amount guard below sees the same value v1 sees.
        amount = min(amount, book.cash * 0.99)
        if amount < 1000:   # v1 minimum position size
            continue

        # Peak-price reference for above_peak TP (uses v1's window logic)
        peak_price = _find_recent_peak(d.symbol, date, price_index, cfg)

        # Vol-adaptive stop/TP pricing — frozen levels at entry
        pricing = compute_stop_pricing(
            sleeve.config, d.symbol, date, price, _make_ohlc_fetcher(price_index),
        )
        if pricing["abort"]:
            continue

        # signal_detail composition (matches v1 Portfolio.open_position)
        if pricing["stop_record"] is None and pricing["tp_record"] is None:
            merged_sig = d.signal_detail
        else:
            merged_sig = {}
            if d.signal_detail is not None:
                merged_sig["entries"] = d.signal_detail
            if pricing["stop_record"]:
                merged_sig["stop"] = pricing["stop_record"]
            if pricing["tp_record"]:
                merged_sig["take_profit"] = pricing["tp_record"]

        trade = book.open(
            sleeve_label=sleeve.label, symbol=d.symbol, date=date,
            amount=amount, exec_price=price,
            peak_price=peak_price,
            signal_detail=merged_sig,
            stop_price=pricing["stop_price"],
            take_profit_price=pricing["take_profit_price"],
            slippage_bps=sleeve.slippage_bps,
            reason="entry",
        )
        if trade is not None:
            trades.append(trade)
    return trades


def _make_ohlc_fetcher(price_index: dict):
    """Stub OHLC fetcher used by compute_stop_pricing for vol-adaptive modes.

    Returns (high, low, close) bars by computing high/low = close (degraded
    case). Matches v1's behavior when prices are only close-only.
    """
    def fetch(symbol: str, end_date: str, n: int):
        pm = price_index.get(symbol, {})
        if not pm:
            return []
        closes_dates = sorted(d for d in pm if d <= end_date)
        tail = closes_dates[-n:]
        if len(tail) < n:
            return []
        return [(pm[d], pm[d], pm[d]) for d in tail]
    return fetch


# ---------------------------------------------------------------------------
# Daily loop — single sleeve, no regime (Step 3a)
# ---------------------------------------------------------------------------
def _run_daily_loop(
    book: PositionBook,
    sleeves: list[_SleeveContext],
    price_index: dict,
    open_index: dict,
    trading_dates: list[str],
    conn,
    earnings_data: dict,
    force_close_at_end: bool,
    initial_capital: float,
) -> dict:
    """Daily loop: iterate trading days, apply per-sleeve directives,
    execute trades against the unified PositionBook, emit unified ledger.

    Step 3a: single sleeve, no regime. All sleeves are always gated on.
    """
    all_trades: list[dict] = []
    last_date = trading_dates[-1] if trading_dates else None

    for date in trading_dates:
        # -------------------------------------------------------------------
        # 1. Execute pending entries from yesterday (next_close / next_open)
        # -------------------------------------------------------------------
        for sleeve in sleeves:
            trades = _execute_pending_entries(
                sleeve, book, price_index, open_index, date,
            )
            all_trades.extend(trades)

        # -------------------------------------------------------------------
        # 2. Apply exits
        # -------------------------------------------------------------------
        for sleeve in sleeves:
            exits = get_exit_recommendations(
                sleeve_label=sleeve.label, sleeve_config=sleeve.config, date=date,
                sleeve_positions=book.positions_for_sleeve(sleeve.label),
                price_index=price_index, exit_signals=sleeve.exit_signals,
            )
            for d in exits:
                price = price_index.get(d.symbol, {}).get(date)
                if price is None:
                    continue
                t = book.sell(
                    sleeve_label=d.sleeve_label, symbol=d.symbol, date=date,
                    exec_price=price, reason=d.reason, shares=d.shares,
                    slippage_bps=sleeve.slippage_bps,
                )
                if t is not None:
                    all_trades.append(t)
                    if d.reason == "stop_loss":
                        sleeve.state.stop_loss_cooldowns[d.symbol] = date

        # -------------------------------------------------------------------
        # 3. Apply rebalance directives
        # -------------------------------------------------------------------
        for sleeve in sleeves:
            sleeve_nav = sum(
                p.market_value(price_index.get(p.symbol, {}).get(date, p.high_since_entry))
                for p in book.positions_for_sleeve(sleeve.label).values()
            )
            # plus this sleeve's cash share (single-sleeve: all cash; multi-sleeve handled in 3b)
            sleeve_nav += book.cash * (1.0 if len(sleeves) == 1 else sleeve.weight)

            rds = get_rebalance_directives(
                sleeve_label=sleeve.label, sleeve_config=sleeve.config, date=date,
                sleeve_positions=book.positions_for_sleeve(sleeve.label),
                price_index=price_index, state=sleeve.state,
                sleeve_nav=sleeve_nav, available_cash=book.cash,
                earnings_data=earnings_data,
            )
            for d in rds:
                price = price_index.get(d.symbol, {}).get(date)
                if price is None:
                    continue
                if d.action == "SELL":
                    t = book.sell(
                        sleeve_label=d.sleeve_label, symbol=d.symbol, date=date,
                        exec_price=price, reason=d.reason, shares=d.shares,
                        slippage_bps=sleeve.slippage_bps,
                    )
                    if t is not None:
                        all_trades.append(t)
                else:   # BUY add
                    amount = (d.amount if d.amount is not None
                              else (d.shares * price if d.shares else 0))
                    if amount > 0:
                        t = book.open(
                            sleeve_label=d.sleeve_label, symbol=d.symbol,
                            date=date, amount=amount, exec_price=price,
                            slippage_bps=sleeve.slippage_bps,
                            reason=d.reason,
                            signal_detail=d.detail,
                        )
                        if t is not None:
                            all_trades.append(t)
            # Advance the rebalance anchor regardless of whether directives
            # fired today. Matches v1's loop: last_rebal_date is set on every
            # is_rebalance_date day (even if _do_rebalance emits no trades),
            # so the next rebalance is ~90/30 days from THIS anchor, not from
            # the prior one. Without this advance, v2 keeps firing the
            # rebalance check daily after the first cadence hit, which can
            # generate unintended earnings-beat adds.
            freq = (sleeve.config.get("rebalancing") or {}).get("frequency", "none")
            if freq != "none" and is_rebalance_date(
                date, sleeve.state.last_rebal_date, freq
            ):
                sleeve.state.last_rebal_date = date
            elif sleeve.state.last_rebal_date is None and book.num_positions(sleeve.label) > 0:
                # Seed: first time we have positions, treat as anchor
                sleeve.state.last_rebal_date = date

        # -------------------------------------------------------------------
        # 4. Collect new entry candidates → queue for tomorrow
        # -------------------------------------------------------------------
        for sleeve in sleeves:
            held_now = book.symbols_held_by_sleeve(sleeve.label)
            available_slots = sleeve.config["sizing"].get("max_positions", 10) - len(held_now)
            if available_slots <= 0:
                sleeve.pending_entries = []
                continue
            cands = get_entry_candidates(
                sleeve_label=sleeve.label, sleeve_config=sleeve.config, date=date,
                trading_dates=trading_dates,
                signals=sleeve.signals, signal_metadata=sleeve.signal_metadata,
                held_symbols_in_sleeve=held_now, available_slots=available_slots,
                state=sleeve.state, conn=conn, price_index=price_index,
                pe_series=sleeve.pe_series, composite_series=sleeve.composite_series,
            )
            sleeve.pending_entries = cands

        # -------------------------------------------------------------------
        # 5. Record NAV
        # -------------------------------------------------------------------
        book.record_nav(price_index, date)

    # -------------------------------------------------------------------
    # Force-close at end (backtest mode)
    # -------------------------------------------------------------------
    if force_close_at_end and last_date is not None:
        # Snapshot keys first; close_position mutates the dict
        for (sleeve_label, symbol) in list(book.positions.keys()):
            price = price_index.get(symbol, {}).get(last_date)
            if price is None:
                continue
            # Find the sleeve's slippage_bps
            slip = next((s.slippage_bps for s in sleeves
                          if s.label == sleeve_label), 0)
            t = book.sell(
                sleeve_label=sleeve_label, symbol=symbol, date=last_date,
                exec_price=price, reason="backtest_end", shares=None,
                slippage_bps=slip,
            )
            if t is not None:
                all_trades.append(t)

    return {"trades": all_trades, "nav_history": book.nav_history}


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------
def run_portfolio_backtest(
    portfolio_config: dict,
    force_close_at_end: bool = True,
) -> dict:
    """V2 portfolio backtest with unified position book.

    Same input/output schema as v1's run_portfolio_backtest, but the engine
    underneath is the unified-book executor. Trade ledger represents the
    actual broker-executable trades.

    STEP 3a SCOPE: single-sleeve, no regime_filter, fixed-weight=1.0 only.
    Multi-sleeve and regime support added in later steps.
    """
    name = portfolio_config.get("name", "Portfolio")
    initial_capital = float(portfolio_config["backtest"]["initial_capital"])
    bt_start = portfolio_config["backtest"]["start"]
    bt_end = portfolio_config["backtest"]["end"]

    print(f"\n{'=' * 70}")
    print(f"PORTFOLIO BACKTEST (V2): {name}")
    print(f"Capital: ${initial_capital:,.0f} | Period: {bt_start} to {bt_end}")
    print(f"{'=' * 70}")

    # --- Step 3a guardrails: only the simplest case is supported here ----
    sleeves_def = portfolio_config["sleeves"]
    if len(sleeves_def) != 1:
        raise NotImplementedError(
            "engine_version=v2 currently supports single-sleeve portfolios only. "
            "Multi-sleeve + regime support is in progress (Phase 2 Steps 3b-3e). "
            f"Got {len(sleeves_def)} sleeves."
        )
    if portfolio_config.get("regime_filter"):
        raise NotImplementedError(
            "engine_version=v2 currently does not support regime_filter. "
            "In progress (Phase 2 Step 3c-3e). Use engine_version=v1 for now."
        )

    sleeve_def = sleeves_def[0]
    sleeve_config = validate_strategy(sleeve_def["strategy_config"])
    sleeve_config["sizing"]["initial_allocation"] = initial_capital * float(
        sleeve_def.get("weight", 1.0)
    )

    # --- Resolve universe + load shared price index ---
    conn = get_connection()
    symbols = resolve_universe(sleeve_config, conn)
    print(f"Universe: {len(symbols)} tickers")
    price_index, open_index, all_trading_dates = build_price_index(symbols, conn)
    trading_dates = [d for d in all_trading_dates if bt_start <= d <= bt_end]
    if not trading_dates:
        conn.close()
        raise ValueError(f"No trading dates in range {bt_start} to {bt_end}")

    # --- Precompute signals + exit signals + earnings + pe + composite ---
    signals, signal_metadata = precompute_signals(
        sleeve_config, symbols, conn, price_index=price_index,
    )
    print(f"Pre-computed {sum(len(v) for v in signals.values())} signal dates")
    exit_signals = {}
    if sleeve_config.get("exit_conditions"):
        exit_signals = _precompute_exit_signals(sleeve_config, symbols, conn)
    earnings_data = load_earnings_data(symbols, conn)

    pe_series = _load_pe_timeseries(symbols)

    composite_series: dict | None = None
    ranking_cfg = sleeve_config.get("ranking") or {}
    if ranking_cfg.get("by") == "composite_score":
        composite_cfg = sleeve_config.get("composite_score") or {}
        factor_names: set[str] = set()
        for b in (composite_cfg.get("buckets") or {}).values():
            for f in b.get("factors", []):
                factor_names.add(f["name"] if isinstance(f, dict) else f.name)
        if factor_names:
            composite_series = {}
            for fname in factor_names:
                composite_series[fname] = _load_feature_series(
                    fname, symbols, bt_start, bt_end, conn, price_index=price_index,
                )

    sleeve_ctx = _SleeveContext(
        sleeve_def={
            **sleeve_def,
            "strategy_config": sleeve_config,
        },
        signals=signals, signal_metadata=signal_metadata,
        exit_signals=exit_signals,
        pe_series=pe_series, composite_series=composite_series,
        symbols=symbols,
    )
    sleeve_ctx.allocated_capital = initial_capital * sleeve_ctx.weight

    # --- Run the unified daily loop ---
    book = PositionBook(initial_capital)
    print(f"Running V2 simulation with ${initial_capital:,.0f}...")
    loop_result = _run_daily_loop(
        book=book, sleeves=[sleeve_ctx],
        price_index=price_index, open_index=open_index,
        trading_dates=trading_dates, conn=conn,
        earnings_data=earnings_data,
        force_close_at_end=force_close_at_end,
        initial_capital=initial_capital,
    )
    conn.close()

    # --- Build metrics ---
    # Use v1's compute_metrics by passing book in place of v1's Portfolio
    # (the function only reads .nav_history, .trades, .closed_trades).
    book.trades = loop_result["trades"]    # for compute_metrics' total_entries count
    metrics = compute_metrics(book, initial_capital, trading_dates)

    # --- Benchmarks (same as v1) ---
    market_bench = compute_benchmark(trading_dates, initial_capital, sector=None)
    if market_bench:
        metrics["market_benchmark_return_pct"] = market_bench["metrics"].get("total_return_pct")
        metrics["alpha_vs_market_pct"] = (
            metrics.get("annualized_return_pct", 0) - market_bench["metrics"].get("annualized_return_pct", 0)
            if metrics.get("annualized_return_pct") is not None
            and market_bench["metrics"].get("annualized_return_pct") is not None
            else None
        )

    print(f"\n  Total Return: {metrics.get('total_return_pct', 0):+.2f}%")
    print(f"  Final NAV:    ${metrics.get('final_nav', initial_capital):,.2f}")
    print(f"  Total trades: {len(loop_result['trades'])}")
    print(f"  Sharpe:       {metrics.get('sharpe_ratio')}")

    return {
        "portfolio": name,
        "engine_version": "v2",
        "metrics": metrics,
        "trades": loop_result["trades"],
        "nav_history": loop_result["nav_history"],
        "combined_nav_history": loop_result["nav_history"],   # alias for compat
        "sleeve_results": [{
            "label": sleeve_ctx.label,
            "trades": loop_result["trades"],
            "nav_history": loop_result["nav_history"],
            "metrics": metrics,
        }],
        "per_sleeve": [{
            "label": sleeve_ctx.label,
            "weight": sleeve_ctx.weight,
            "allocated_capital": sleeve_ctx.allocated_capital,
            "regime_gate": sleeve_ctx.regime_gate,
        }],
        "config": portfolio_config,
        "backtest": {"start": bt_start, "end": bt_end,
                     "initial_capital": initial_capital},
    }
