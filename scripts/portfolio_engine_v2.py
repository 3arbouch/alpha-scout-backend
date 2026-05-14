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
# v1's regime resolver — same code, same regimes, same dates
from portfolio_engine import _load_regime_configs
from db_config import APP_DB_PATH
from regime import evaluate_regime_series_with_stats

# Match v1's persistence defaults (defined as local vars inside v1's
# run_portfolio_backtest at lines 272-273)
_DEFAULT_ENTRY_PERSIST = 3
_DEFAULT_EXIT_PERSIST = 3
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
        # ---- Gate state ----
        # Combined gate (intersection of layer 1 + layer 2). None means
        # always-on; a set means the sleeve is on ONLY on those dates.
        self.gate_dates_on: set[str] | None = None
        # Allocation-profile layer ONLY (None when no allocation_profile is
        # configured for this sleeve). Used to detect "liquidate this sleeve
        # today" events — regime_gate transitions just pause, they don't
        # liquidate. Allocation_profile is what actually moves capital.
        self.alloc_dates_on: set[str] | None = None

    def is_gated_on(self, date: str) -> bool:
        return self.gate_dates_on is None or date in self.gate_dates_on

    def alloc_active_today(self, date: str) -> bool:
        """Whether the allocation_profile gives this sleeve weight > 0 today.
        True when no allocation_profile is configured (alloc layer not in play)."""
        return self.alloc_dates_on is None or date in self.alloc_dates_on


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
        # Per-sleeve NAV (matches v1's standalone sleeve simulation).
        sleeve_nav_now = book.sleeve_nav(sleeve.label, price_index, date)
        if sleeve_nav_now <= 0:
            continue

        if sizing_type == "equal_weight":
            amount = sleeve_nav_now / max_positions
        elif sizing_type == "fixed_amount":
            amount = cfg["sizing"].get("amount_per_position", initial_alloc / max_positions)
        elif sizing_type == "risk_parity":
            pool = (n_batch / max_positions) * sleeve_nav_now
            w = rp_weights.get(d.symbol)
            if w is not None:
                amount = pool * w
            else:
                amount = sleeve_nav_now / max_positions
        else:
            amount = sleeve_nav_now / max_positions

        # max_position_pct cap (against sleeve nav)
        if max_pct < 100 and (amount / sleeve_nav_now) * 100 > max_pct:
            amount = sleeve_nav_now * (max_pct / 100.0)

        # Cash buffer cap — uses this sleeve's cash pool, matching v1's
        # per-sleeve standalone simulation where each sleeve had its own
        # portfolio.cash. min(amount, sleeve_cash * 0.99).
        amount = min(amount, book.sleeve_cash(sleeve.label) * 0.99)
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
    profile_name_by_date: dict[str, str] | None = None,
) -> dict:
    """Daily loop: iterate trading days, apply per-sleeve directives,
    execute trades against the unified PositionBook, emit unified ledger.

    On any day a sleeve transitions from gated-on to gated-off (typically a
    regime/allocation_profile flip), all sleeve-tagged positions are
    liquidated cleanly with reason=`rebalance_to_<profile>`. This is the
    actual broker action when the portfolio reallocates a sleeve to 0%.
    """
    all_trades: list[dict] = []
    last_date = trading_dates[-1] if trading_dates else None
    profile_name_by_date = profile_name_by_date or {}

    # Track each sleeve's ALLOCATION-PROFILE gate state from the prior day so
    # we can detect the day allocation_profile drops it to 0% and emit clean
    # liquidation trades. regime_gate transitions only PAUSE the sleeve;
    # positions stay intact (matches v1 behavior). Only allocation_profile
    # is the "actual capital is moving" signal.
    prev_alloc_active: dict[str, bool] = {ctx.label: False for ctx in sleeves}
    if trading_dates:
        d0 = trading_dates[0]
        for sleeve in sleeves:
            prev_alloc_active[sleeve.label] = sleeve.alloc_active_today(d0)

    for date in trading_dates:
        # -------------------------------------------------------------------
        # 0. Allocation-profile transition: sleeve had weight > 0 yesterday,
        #    weight == 0 today → liquidate all sleeve-tagged positions cleanly.
        # -------------------------------------------------------------------
        for sleeve in sleeves:
            alloc_today = sleeve.alloc_active_today(date)
            if prev_alloc_active[sleeve.label] and not alloc_today:
                # Emit clean liquidation trades for this sleeve's holdings.
                # reason carries the target profile name so trade-ledger
                # readers can attribute the move.
                profile_name = profile_name_by_date.get(date) or "gated_off"
                reason = f"rebalance_to_{profile_name}"
                for symbol in list(book.symbols_held_by_sleeve(sleeve.label)):
                    price = price_index.get(symbol, {}).get(date)
                    if price is None:
                        continue
                    t = book.sell(
                        sleeve_label=sleeve.label, symbol=symbol, date=date,
                        exec_price=price, reason=reason, shares=None,
                        slippage_bps=sleeve.slippage_bps,
                    )
                    if t is not None:
                        all_trades.append(t)
            prev_alloc_active[sleeve.label] = alloc_today

        # -------------------------------------------------------------------
        # 1. Execute pending entries from yesterday (next_close / next_open)
        # -------------------------------------------------------------------
        # Per-sleeve gating: when a sleeve is gated off today, v1 clears its
        # pending_entries (no fills) — match that here. Otherwise process
        # pending fills normally.
        for sleeve in sleeves:
            if not sleeve.is_gated_on(date):
                sleeve.pending_entries = []
                continue
            trades = _execute_pending_entries(
                sleeve, book, price_index, open_index, date,
            )
            all_trades.extend(trades)

        # -------------------------------------------------------------------
        # 2. Apply exits — gated off when sleeve is gated. v1 fix 9d0fead
        # suppresses exits on gated-off days; v2 mirrors that.
        # -------------------------------------------------------------------
        for sleeve in sleeves:
            if not sleeve.is_gated_on(date):
                continue
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
        # 3. Apply rebalance directives — gated-off days skip entirely
        # -------------------------------------------------------------------
        for sleeve in sleeves:
            if not sleeve.is_gated_on(date):
                continue
            sleeve_nav = book.sleeve_nav(sleeve.label, price_index, date)
            rds = get_rebalance_directives(
                sleeve_label=sleeve.label, sleeve_config=sleeve.config, date=date,
                sleeve_positions=book.positions_for_sleeve(sleeve.label),
                price_index=price_index, state=sleeve.state,
                sleeve_nav=sleeve_nav,
                available_cash=book.sleeve_cash(sleeve.label),
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
            # fired today (matches v1 line 2380-2390 in run_backtest).
            freq = (sleeve.config.get("rebalancing") or {}).get("frequency", "none")
            if freq != "none" and is_rebalance_date(
                date, sleeve.state.last_rebal_date, freq
            ):
                sleeve.state.last_rebal_date = date
            elif sleeve.state.last_rebal_date is None and book.num_positions(sleeve.label) > 0:
                sleeve.state.last_rebal_date = date

        # -------------------------------------------------------------------
        # 4. Collect new entry candidates → queue for tomorrow.
        #    Gated-off sleeves emit nothing (v1 line 2256-2257).
        # -------------------------------------------------------------------
        for sleeve in sleeves:
            if not sleeve.is_gated_on(date):
                sleeve.pending_entries = []
                continue
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

    sleeves_def = portfolio_config["sleeves"]
    allocation_profiles = portfolio_config.get("allocation_profiles") or None
    profile_priority = portfolio_config.get("profile_priority") or []

    # --- Resolve universe (union across sleeves) + load shared price index ---
    conn = get_connection()
    sleeve_configs: list[dict] = []
    sleeve_symbols: list[list[str]] = []
    all_sleeve_symbols: set[str] = set()
    for sd in sleeves_def:
        scfg = validate_strategy(sd["strategy_config"])
        scfg["sizing"]["initial_allocation"] = initial_capital * float(sd.get("weight", 1.0))
        syms = resolve_universe(scfg, conn)
        sleeve_configs.append(scfg)
        sleeve_symbols.append(syms)
        all_sleeve_symbols.update(syms)
    print(f"Universe (across sleeves): {len(all_sleeve_symbols)} tickers")
    price_index, open_index, all_trading_dates = build_price_index(
        list(all_sleeve_symbols), conn,
    )
    trading_dates = [d for d in all_trading_dates if bt_start <= d <= bt_end]
    if not trading_dates:
        conn.close()
        raise ValueError(f"No trading dates in range {bt_start} to {bt_end}")

    # --- Build per-sleeve precomputed context ---
    earnings_data = load_earnings_data(list(all_sleeve_symbols), conn)
    sleeve_ctxs: list[_SleeveContext] = []
    for sd, scfg, syms in zip(sleeves_def, sleeve_configs, sleeve_symbols):
        signals, signal_metadata = precompute_signals(
            scfg, syms, conn, price_index=price_index,
        )
        exit_signals = (_precompute_exit_signals(scfg, syms, conn)
                        if scfg.get("exit_conditions") else {})
        pe_series = _load_pe_timeseries(syms)
        composite_series: dict | None = None
        ranking_cfg = scfg.get("ranking") or {}
        if ranking_cfg.get("by") == "composite_score":
            composite_cfg = scfg.get("composite_score") or {}
            factor_names: set[str] = set()
            for b in (composite_cfg.get("buckets") or {}).values():
                for f in b.get("factors", []):
                    factor_names.add(f["name"] if isinstance(f, dict) else f.name)
            if factor_names:
                composite_series = {}
                for fname in factor_names:
                    composite_series[fname] = _load_feature_series(
                        fname, syms, bt_start, bt_end, conn, price_index=price_index,
                    )
        ctx = _SleeveContext(
            sleeve_def={**sd, "strategy_config": scfg},
            signals=signals, signal_metadata=signal_metadata,
            exit_signals=exit_signals,
            pe_series=pe_series, composite_series=composite_series,
            symbols=syms,
        )
        ctx.allocated_capital = initial_capital * ctx.weight
        sleeve_ctxs.append(ctx)
        print(f"  Sleeve '{ctx.label}' (weight {ctx.weight:.0%}): "
              f"{len(syms)} tickers, "
              f"{sum(len(v) for v in signals.values())} signal dates")

    # --- Resolve regimes (Step 3c+) and per-sleeve gate dates -----------------
    regime_enabled = bool(portfolio_config.get("regime_filter"))
    regime_id_to_name: dict[str, str] = {}
    regime_series: dict[str, list[str]] = {}

    if regime_enabled:
        all_regime_ids: set[str] = set()
        for sd in sleeves_def:
            for rid in sd.get("regime_gate", []):
                if rid != "*":
                    all_regime_ids.add(rid)
        # Also collect regime IDs referenced by allocation_profile triggers
        if allocation_profiles:
            for pname, pdef in allocation_profiles.items():
                if pname == "default":
                    continue
                if isinstance(pdef, dict):
                    for rid in pdef.get("trigger", []):
                        all_regime_ids.add(rid)
        if all_regime_ids:
            print(f"\n  Loading {len(all_regime_ids)} regime definitions...")
            inline_defs = portfolio_config.get("regime_definitions") or {}
            regime_configs = []
            db_ids: list[str] = []
            for rid in all_regime_ids:
                if rid in inline_defs:
                    defn = inline_defs[rid]
                    rc = {"name": rid}
                    rc.update(defn if isinstance(defn, dict) else defn.model_dump())
                    regime_configs.append(rc)
                    regime_id_to_name[rid] = rid
                    print(f"    {rid}: resolved from inline regime_definitions")
                else:
                    db_ids.append(rid)
            if db_ids:
                import sqlite3 as _sqlite3
                _app_conn = _sqlite3.connect(str(APP_DB_PATH))
                _app_conn.row_factory = _sqlite3.Row
                db_configs, db_id_to_name = _load_regime_configs(db_ids, _app_conn)
                _app_conn.close()
                regime_configs.extend(db_configs)
                regime_id_to_name.update(db_id_to_name)
            # Stamp version-aware persistence defaults (match v1)
            for rc in regime_configs:
                rc.setdefault("entry_persistence_days", _DEFAULT_ENTRY_PERSIST)
                rc.setdefault("exit_persistence_days", _DEFAULT_EXIT_PERSIST)
            print(f"  Computing regime series {bt_start} to {bt_end}...")
            regime_series, _persistence_stats = evaluate_regime_series_with_stats(
                bt_start, bt_end, regime_configs,
            )
            from collections import Counter
            counts: Counter = Counter()
            for _date, active in regime_series.items():
                for r in active:
                    counts[r] += 1
            for rname, cnt in counts.most_common():
                pct = cnt / max(len(regime_series), 1) * 100
                print(f"    {rname}: {cnt} days ({pct:.1f}%)")

    # --- Allocation_profile resolution (Step 3d) -----------------------------
    # For each trading day, compute the target weight per sleeve from the
    # active profile. profile_priority is walked top-to-bottom; the first
    # profile whose triggers are all in today's active regime set wins.
    # "default" matches unconditionally. Result: profile_weights_by_date.
    profile_weights_by_date: dict[str, dict[str, float]] = {}
    profile_name_by_date: dict[str, str] = {}

    def _resolve_today_profile(active_regime_names: set[str]):
        if not allocation_profiles or not profile_priority:
            return None, None
        for pname in profile_priority:
            if pname == "default":
                w = allocation_profiles.get("default", {}).get("weights", {})
                return "default", w
            pdef = allocation_profiles.get(pname, {})
            triggers = pdef.get("trigger", [])
            if not triggers:
                continue
            trigger_names = {regime_id_to_name.get(rid, rid) for rid in triggers}
            if trigger_names and trigger_names.issubset(active_regime_names):
                return pname, pdef.get("weights", {})
        if "default" in allocation_profiles:
            return "default", allocation_profiles["default"].get("weights", {})
        return None, None

    if regime_enabled and allocation_profiles:
        for d in trading_dates:
            active = set(regime_series.get(d, []))
            pname, weights = _resolve_today_profile(active)
            if pname is not None:
                profile_name_by_date[d] = pname
                profile_weights_by_date[d] = weights or {}

    # --- Per-sleeve gate dates: intersection of regime_gate AND profile>0 ---
    for ctx in sleeve_ctxs:
        # Layer 1: regime_gate dates
        gate = ctx.regime_gate
        if not regime_enabled or gate == ["*"] or not gate:
            regime_dates_on = None
        else:
            gated_names = {regime_id_to_name.get(rid, rid) for rid in gate}
            regime_dates_on = {
                d for d, active in regime_series.items()
                if gated_names & set(active)
            }

        # Layer 2: allocation_profile non-zero-weight dates
        if profile_weights_by_date:
            alloc_dates_on = {
                d for d, w in profile_weights_by_date.items()
                if float(w.get(ctx.label, 0) or 0) > 0
            }
        else:
            alloc_dates_on = None
        ctx.alloc_dates_on = alloc_dates_on   # stored separately for liquidation detection

        # Intersect — sleeve is on iff BOTH layers allow it.
        if regime_dates_on is None and alloc_dates_on is None:
            ctx.gate_dates_on = None
        elif regime_dates_on is None:
            ctx.gate_dates_on = alloc_dates_on
        elif alloc_dates_on is None:
            ctx.gate_dates_on = regime_dates_on
        else:
            ctx.gate_dates_on = regime_dates_on & alloc_dates_on

        if ctx.gate_dates_on is not None and regime_series:
            total = len(regime_series)
            on = len(ctx.gate_dates_on)
            layers = []
            if regime_dates_on is not None:
                layers.append("regime_gate")
            if alloc_dates_on is not None:
                layers.append("allocation_profiles")
            src = "+".join(layers) if layers else "none"
            print(f"  Sleeve '{ctx.label}' gate ({src}): {on}/{total} days active ({on/total*100:.1f}%)")

    # --- Initialize PositionBook with per-sleeve cash pools ---
    initial_cash_by_sleeve = {ctx.label: ctx.allocated_capital for ctx in sleeve_ctxs}
    # If sleeve weights don't sum to 1.0, the leftover stays unallocated.
    leftover = initial_capital - sum(initial_cash_by_sleeve.values())
    if abs(leftover) > 0.01:
        # Park leftover in the wildcard pool so it doesn't get lost.
        initial_cash_by_sleeve["*"] = leftover
    book = PositionBook(initial_cash_by_sleeve)
    print(f"Running V2 simulation with ${initial_capital:,.0f}...")
    loop_result = _run_daily_loop(
        book=book, sleeves=sleeve_ctxs,
        price_index=price_index, open_index=open_index,
        trading_dates=trading_dates, conn=conn,
        earnings_data=earnings_data,
        force_close_at_end=force_close_at_end,
        initial_capital=initial_capital,
        profile_name_by_date=profile_name_by_date,
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

    # --- Per-sleeve trade attribution ---
    trades_by_sleeve: dict[str, list[dict]] = {ctx.label: [] for ctx in sleeve_ctxs}
    for t in loop_result["trades"]:
        lbl = t.get("sleeve_label")
        if lbl in trades_by_sleeve:
            trades_by_sleeve[lbl].append(t)

    sleeve_results = [{
        "label": ctx.label,
        "trades": trades_by_sleeve.get(ctx.label, []),
        "nav_history": loop_result["nav_history"],   # combined book nav for now
    } for ctx in sleeve_ctxs]

    per_sleeve = [{
        "label": ctx.label,
        "weight": ctx.weight,
        "allocated_capital": ctx.allocated_capital,
        "regime_gate": ctx.regime_gate,
    } for ctx in sleeve_ctxs]

    return {
        "portfolio": name,
        "engine_version": "v2",
        "metrics": metrics,
        "trades": loop_result["trades"],
        "nav_history": loop_result["nav_history"],
        "combined_nav_history": loop_result["nav_history"],   # alias for compat
        "sleeve_results": sleeve_results,
        "per_sleeve": per_sleeve,
        "config": portfolio_config,
        "backtest": {"start": bt_start, "end": bt_end,
                     "initial_capital": initial_capital},
    }
