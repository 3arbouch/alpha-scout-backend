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
from collections import namedtuple
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))

from backtest_engine import (
    pit_members_by_date,
    rank_candidates,
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


# Acronyms / domain terms that should stay uppercase or get a specific casing
# when we humanize snake_case macro series codes for the frontend. Anything not
# in this map falls through to plain title-case.
_HUMANIZE_TOKEN_OVERRIDES = {
    "hy":    "HY",       "ig":  "IG",     "spx":  "SPX",    "vix": "VIX",
    "cpi":   "CPI",      "pce": "PCE",    "ppi":  "PPI",    "ism": "ISM",
    "yoy":   "YoY",      "mom": "MoM",    "qoq":  "QoQ",    "fy":  "FY",
    "ytd":   "YTD",      "dma": "DMA",    "ema":  "EMA",    "sma": "SMA",
    "ma":    "MA",       "wti": "WTI",    "fx":   "FX",     "ust": "UST",
    "10y":   "10Y",      "2y":  "2Y",     "30y":  "30Y",    "zscore": "Z-Score",
    "pct":   "(%)",      "vs":  "vs",     "of":   "of",     "and": "and",
    "the":   "the",      "to":  "to",     "natgas": "NatGas",
}

# Tokens that mix digits and letters in the source (e.g. "200dma", "50dma") get
# split at the digit→letter boundary so each side can be humanized independently.
import re as _re
_DIGIT_LETTER_SPLIT = _re.compile(r"(\d+)([a-zA-Z]+)")


def _humanize_token(tok: str) -> str:
    """Single-token humanizer: lowercase token → presentation form."""
    lo = tok.lower()
    if lo in _HUMANIZE_TOKEN_OVERRIDES:
        return _HUMANIZE_TOKEN_OVERRIDES[lo]
    m = _DIGIT_LETTER_SPLIT.fullmatch(tok)
    if m:
        # "200dma" → "200-DMA"; "50ema" → "50-EMA"
        return f"{m.group(1)}-{_humanize_token(m.group(2))}"
    return tok.capitalize()


def _humanize_series_code(code: str) -> str:
    """Turn a macro series code like ``hy_spread_zscore`` into a display label
    like ``HY Spread Z-Score``. Used to stamp ``series_label`` on regime
    conditions so the frontend can render readable trigger strings without
    maintaining a parallel code→label table."""
    return " ".join(_humanize_token(t) for t in code.split("_") if t)


def _humanize_regime_id(rid: str) -> str:
    """Same idea but for regime IDs (``macro_defensive`` → ``Macro Defensive``)."""
    return " ".join(_humanize_token(t) for t in rid.split("_") if t)
from position_book import PositionBook
from sleeve_signals import (
    SleeveRuntimeState,
    get_entry_candidates,
    get_exit_recommendations,
    get_rebalance_directives,
    pop_ranking_event,
)
from stop_pricing import compute_realized_vol, compute_stop_pricing, make_sqlite_ohlc_fetcher


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
        # PIT membership: {date: frozenset(members)} when universe.type='index',
        # None otherwise. Precomputed in the sleeve setup loop.
        self.pit_members_on: dict[str, frozenset[str]] | None = None
        # pending entries from yesterday's signals (next_close/next_open mode)
        self.pending_entries: list = []  # list of EntryDirective
        # True when this sleeve's book was seeded with carried-in opening
        # positions (real broker holdings). The strategy then runs forward
        # exactly as designed from that opening book — seeding only changes the
        # starting state, not the entry/exit logic.
        self.seeded: bool = False
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
    ohlc_fetcher=None,
) -> list[dict]:
    """Process a sleeve's pending entries from yesterday, filling at today's
    close (next_close) or today's open (next_open).

    Replicates v1's per-symbol fill loop exactly:
      - current_nav recomputed EACH ITERATION (slippage drag shrinks pool)
      - amount = current_nav / max_positions (equal_weight) — fresh per iter
      - risk_parity: pool = (n_batch / max_positions) × current_nav per iter
      - amount = min(amount, portfolio.cash * 0.99) — cash buffer cap
      - no minimum position size (only guard: amount > 0)

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
        if amount <= 0:
            continue

        # Peak-price reference for above_peak TP (uses v1's window logic)
        peak_price = _find_recent_peak(d.symbol, date, price_index, cfg)

        # Vol-adaptive stop/TP pricing — frozen levels at entry. v1 passes the
        # SLIPPAGE-ADJUSTED entry price (backtest_engine.py:1948-1955), so the
        # frozen stop/TP levels reflect what the broker actually paid; v2 must
        # match to keep stop-fire dates byte-identical.
        exec_entry_price = price * (1 + sleeve.slippage_bps / 10000)
        # Use DB-backed OHLC fetcher when provided (matches v1's
        # make_sqlite_ohlc_fetcher: real H/L for ATR, strict-before-entry to
        # avoid lookahead). Falls back to the close-only stub only if the
        # caller didn't pass a fetcher.
        fetcher = ohlc_fetcher if ohlc_fetcher is not None else _make_ohlc_fetcher(price_index)
        pricing = compute_stop_pricing(
            sleeve.config, d.symbol, date, exec_entry_price, fetcher,
        )
        if pricing["abort"]:
            continue

        # signal_detail composition (matches v1 Portfolio.open_position). The
        # directive carries `signal_detail` as the raw entries list and an
        # optional `ranking` block (per-pick score / factor reduction). We
        # wrap them into the unified trade record so every BUY is self-
        # describing.
        has_ranking = d.ranking is not None
        if (pricing["stop_record"] is None and pricing["tp_record"] is None
                and not has_ranking):
            merged_sig = d.signal_detail
        else:
            merged_sig = {}
            if d.signal_detail is not None:
                merged_sig["entries"] = d.signal_detail
            if pricing["stop_record"]:
                merged_sig["stop"] = pricing["stop_record"]
            if pricing["tp_record"]:
                merged_sig["take_profit"] = pricing["tp_record"]
            if has_ranking:
                merged_sig["ranking"] = d.ranking

        trade = book.open(
            sleeve_label=sleeve.label, symbol=d.symbol, date=date,
            amount=amount, exec_price=price,
            peak_price=peak_price,
            signal_detail=merged_sig,
            stop_price=pricing["stop_price"],
            take_profit_price=pricing["take_profit_price"],
            shares_mode=cfg.get("sizing", {}).get("shares"),
            slippage_bps=sleeve.slippage_bps,
            reason="entry",
        )
        if trade is not None:
            trades.append(trade)
    return trades


# ---------------------------------------------------------------------------
def _apply_equal_weight_rebalance(
    sleeve: "_SleeveContext",
    book: PositionBook,
    conn,
    price_index: dict,
    date: str,
) -> list[dict]:
    """Equal-weight rebalance (mirrors v1 _do_equal_weight_rebalance).

    Step 1: re-rank universe → pick top_n target set (or keep current
            holdings if no ranking config).
    Step 2: sell positions that fell out of target set (rebalance_rotation).
    Step 3: reweight each surviving position toward target_amount = NAV/n
            (trim → rebalance_trim, add → entry).
    Step 4: buy new positions in target set not yet held (entry).

    NAV is recomputed between phases (matches v1).
    """
    cfg = sleeve.config
    sizing_cfg = cfg.get("sizing", {})
    max_positions = sizing_cfg.get("max_positions", 10)
    ranking_cfg = cfg.get("ranking")
    slippage = sleeve.slippage_bps

    trades: list[dict] = []
    sleeve_label = sleeve.label

    sleeve_nav = book.sleeve_nav(sleeve_label, price_index, date)
    if sleeve_nav <= 0:
        return trades

    # ---- Determine target holdings -------------------------------------
    if ranking_cfg:
        top_n = ranking_cfg.get("top_n", max_positions)
        candidates: list[tuple[str, float]] = []
        for symbol in sorted(sleeve.signals.keys()):
            sig_data = sleeve.signals.get(symbol, {})
            if date not in sig_data:
                continue
            candidates.append((symbol, sig_data[date]))
        if candidates:
            ranked = rank_candidates(
                candidates, cfg, conn, date, price_index,
                pe_series=sleeve.pe_series,
                composite_series=sleeve.composite_series,
            )
            target_symbols = set(sym for sym, _ in ranked[:top_n])
        else:
            target_symbols = set(book.symbols_held_by_sleeve(sleeve_label))
    else:
        target_symbols = set(book.symbols_held_by_sleeve(sleeve_label))

    # ---- Step 1: rotation sells ----------------------------------------
    for symbol in list(book.symbols_held_by_sleeve(sleeve_label)):
        if symbol in target_symbols:
            continue
        price = price_index.get(symbol, {}).get(date)
        if not price:
            continue
        t = book.sell(
            sleeve_label=sleeve_label, symbol=symbol, date=date,
            exec_price=price, reason="rebalance_rotation",
            slippage_bps=slippage,
        )
        if t is not None:
            trades.append(t)

    # ---- Step 2: reweight survivors ------------------------------------
    n_targets = len(target_symbols)
    if n_targets == 0:
        return trades

    sleeve_nav = book.sleeve_nav(sleeve_label, price_index, date)
    target_amount = sleeve_nav / n_targets

    for symbol in list(book.symbols_held_by_sleeve(sleeve_label)):
        if symbol not in target_symbols:
            continue
        price = price_index.get(symbol, {}).get(date)
        if not price:
            continue
        pos = book.get(sleeve_label, symbol)
        if pos is None:
            continue
        current_value = pos.market_value(price)
        diff = target_amount - current_value
        if diff < -1000:
            # Overweight — trim
            trim_pct = (abs(diff) / current_value) * 100
            trim_pct = min(trim_pct, 99)
            trim_shares = pos.shares * (trim_pct / 100.0)
            if trim_shares > 0:
                t = book.sell(
                    sleeve_label=sleeve_label, symbol=symbol, date=date,
                    exec_price=price, reason="rebalance_trim",
                    shares=trim_shares, slippage_bps=slippage,
                )
                if t is not None:
                    trades.append(t)
        elif diff > 1000 and book.sleeve_cash(sleeve_label) > 1000:
            # Underweight — add (v1 emits this with no explicit reason →
            # `entry` is the default)
            add_amount = min(diff, book.sleeve_cash(sleeve_label) * 0.95)
            if add_amount >= 1000:
                t = book.open(
                    sleeve_label=sleeve_label, symbol=symbol, date=date,
                    amount=add_amount, exec_price=price,
                    slippage_bps=slippage, reason="entry",
                    shares_mode=cfg.get("sizing", {}).get("shares"),
                )
                if t is not None:
                    trades.append(t)

    # ---- Step 3: buy new top-N entrants --------------------------------
    sleeve_nav = book.sleeve_nav(sleeve_label, price_index, date)
    if n_targets == 0:
        return trades
    target_amount = sleeve_nav / n_targets

    for symbol in target_symbols:
        if book.has(sleeve_label, symbol):
            continue
        price = price_index.get(symbol, {}).get(date)
        if not price:
            continue
        amount = min(target_amount, book.sleeve_cash(sleeve_label) * 0.95)
        if amount <= 0:
            continue
        t = book.open(
            sleeve_label=sleeve_label, symbol=symbol, date=date,
            amount=amount, exec_price=price,
            slippage_bps=slippage, reason="entry",
            shares_mode=cfg.get("sizing", {}).get("shares"),
        )
        if t is not None:
            trades.append(t)

    return trades


_HeldName = namedtuple("_HeldName", ["symbol"])


def _apply_target_weight_rebalance(
    sleeve: "_SleeveContext",
    book: PositionBook,
    price_index: dict,
    date: str,
) -> list[dict]:
    """Two-sided rebalance of held positions toward their sizing-model targets.

    Targets come from the sleeve's sizing model (risk_parity → inverse-vol,
    recomputed as of `date`; otherwise equal weight) over the CURRENTLY HELD,
    priced names, normalized to sum to 1. Each position is trimmed down / added
    up to its target share of the current invested value, so the pass is
    cash-neutral (trim proceeds fund the adds). Names whose weight is within
    ±rebalance_band_pct of target are left untouched (no-trade band).

    Unlike equal_weight mode, this does NOT re-rank the universe, rotate names,
    or open new positions — it is pure drift-control on existing holdings.
    Trims are emitted before adds so freed cash funds the buys.
    """
    cfg = sleeve.config
    sleeve_label = sleeve.label
    slippage = sleeve.slippage_bps

    positions = book.positions_for_sleeve(sleeve_label)
    if not positions:
        return []

    rules = (cfg.get("rebalancing") or {}).get("rules") or {}
    band = float(rules.get("rebalance_band_pct", 0)) / 100.0

    # Priced, currently-held names + invested value.
    priced: dict[str, tuple] = {}  # sym -> (pos, price, market_value)
    invested = 0.0
    for sym, pos in positions.items():
        price = price_index.get(sym, {}).get(date)
        if not price or price <= 0:
            continue
        mv = pos.market_value(price)
        priced[sym] = (pos, price, mv)
        invested += mv
    if invested <= 0 or not priced:
        return []

    # Target weights from the sizing model (sum to 1 over priced names).
    sizing_type = cfg.get("sizing", {}).get("type", "equal_weight")
    targets: dict[str, float] = {}
    if sizing_type == "risk_parity":
        shim = [_HeldName(sym) for sym in priced]
        targets = _compute_risk_parity_weights(sleeve, shim, price_index, date)
    if not targets:
        # equal_weight / fixed_amount, or risk_parity with no vol history.
        w = 1.0 / len(priced)
        targets = {sym: w for sym in priced}
    else:
        # Names with no vol estimate get the average weight; then renormalize
        # so the targets sum to 1 across exactly the priced names.
        missing = [s for s in priced if s not in targets]
        if missing:
            avg = sum(targets.values()) / len(targets)
            for s in missing:
                targets[s] = avg
        tot = sum(targets[s] for s in priced)
        if tot <= 0:
            return []
        targets = {s: targets[s] / tot for s in priced}

    MIN_TRADE = 1.0  # ignore sub-dollar dust
    sells: list[tuple] = []
    buys: list[tuple] = []
    for sym, (pos, price, mv) in priced.items():
        cur_w = mv / invested
        tgt_w = targets.get(sym, 0.0)
        if abs(cur_w - tgt_w) <= band:
            continue
        diff = (tgt_w * invested) - mv  # >0 underweight (buy), <0 overweight (trim)
        if diff < -MIN_TRADE:
            trim_shares = pos.shares * min(abs(diff) / mv, 1.0)
            if trim_shares > 0:
                sells.append((sym, price, trim_shares))
        elif diff > MIN_TRADE:
            buys.append((sym, price, diff))

    trades: list[dict] = []
    for sym, price, trim_shares in sells:
        t = book.sell(
            sleeve_label=sleeve_label, symbol=sym, date=date,
            exec_price=price, reason="rebalance_trim",
            shares=trim_shares, slippage_bps=slippage,
        )
        if t is not None:
            trades.append(t)
    for sym, price, add_amount in buys:
        amount = min(add_amount, book.sleeve_cash(sleeve_label) * 0.99)
        if amount <= MIN_TRADE:
            continue
        t = book.open(
            sleeve_label=sleeve_label, symbol=sym, date=date,
            amount=amount, exec_price=price, slippage_bps=slippage,
            reason="entry",
            shares_mode=cfg.get("sizing", {}).get("shares"),
        )
        if t is not None:
            trades.append(t)
    return trades


def _band_dest_amount(mv: float, sleeve_nav: float, target_weight: float, band: float):
    """Dollar destination for a survivor under a no-trade band with trade-to-edge.

    Returns None when the position needs no trade — either it sits within the
    no-trade band (|weight − target| ≤ band) or the sleeve NAV is non-positive.
    Otherwise returns the dollar amount to size the position to:
      - band > 0: the NEAR BAND EDGE (target ± band) — drift up to the edge was
        already acceptable, so we stop there rather than overshooting to target
        (the no-trade-region result; minimises turnover).
      - band == 0: full correction to the target weight (legacy behaviour).
    """
    if sleeve_nav <= 0:
        return None
    weight = mv / sleeve_nav
    if band > 0:
        if abs(weight - target_weight) <= band:
            return None  # within no-trade band
        edge_weight = target_weight + band if weight > target_weight else target_weight - band
        return sleeve_nav * max(edge_weight, 0.0)
    return sleeve_nav * target_weight


def _rank_buffer_target_weights(
    symbols: list[str],
    sleeve: "_SleeveContext",
    price_index: dict,
    date: str,
    n_targets: int,
) -> dict[str, float]:
    """Per-name target weights for the rank_buffer reweight, per sizing.type.

    - equal_weight / fixed_amount → uniform 1/n_targets (byte-identical to the
      legacy reweight, which always targeted equal weight).
    - risk_parity → inverse-vol shares normalized across the held book, so risk
      parity is MAINTAINED on every rebalance instead of decaying to equal
      weight (the rank_buffer reweight previously ignored sizing.type). Names
      whose vol can't be estimated fall back to the book's median vol so they
      stay invested rather than dropping to a zero target.
    """
    uniform = (1.0 / n_targets) if n_targets else 0.0
    cfg = sleeve.config
    if cfg.get("sizing", {}).get("type") != "risk_parity" or not symbols:
        return {s: uniform for s in symbols}
    import statistics
    vol_window = int(cfg["sizing"].get("vol_window_days", 20))
    vol_source = cfg["sizing"].get("vol_source", "historical")
    sigmas: dict[str, float] = {}
    for sym in symbols:
        pm = price_index.get(sym, {})
        if not pm:
            continue
        closes_dates = sorted(d for d in pm if d < date)
        tail = closes_dates[-(vol_window + 1):] if len(closes_dates) >= vol_window + 1 else []
        if not tail:
            continue
        sigma = compute_realized_vol([pm[d] for d in tail], vol_window, vol_source)
        if sigma is not None and sigma > 0:
            sigmas[sym] = sigma
    if not sigmas:
        return {s: uniform for s in symbols}
    median_sigma = statistics.median(sigmas.values())
    inv = {s: 1.0 / sigmas.get(s, median_sigma) for s in symbols}
    total = sum(inv.values())
    return {s: v / total for s, v in inv.items()}


def _apply_rank_buffer_rebalance(
    sleeve: "_SleeveContext",
    book: PositionBook,
    conn,
    price_index: dict,
    date: str,
) -> list[dict]:
    """Rank-buffer (hysteresis) rotation — the low-turnover ranking rebalance.

    Re-ranks the universe by composite score, then rotates the book with an
    ASYMMETRIC entry/exit-rank buffer so names oscillating around the top_n
    boundary are not churned:
      - SELL a held name only if its rank falls past ``exit_rank`` (or it drops
        out of the ranked universe).            → reason=rebalance_rotation
      - KEEP every held name still inside exit_rank (the buffer zone).
      - BUY non-held names ranked within ``entry_rank``, best first, up to the
        max_positions cap.                       → reason=entry
      - Reweight survivors toward the equal-weight target, skipping any whose
        weight is within ``rebalance_band_pct`` of target (no-trade band).

    entry_rank/exit_rank default to ranking.top_n; with entry_rank == exit_rank
    == top_n and band == 0 this matches equal_weight's rotation membership. (It
    is not byte-identical to equal_weight: weights are spread over the actually
    actionable book — survivors + priced entrants — rather than the nominal
    top_n, so capital is not left idle on an unpriceable target slot.) Requires
    a ranking config — without one there is nothing to rotate on (no-op).
    """
    cfg = sleeve.config
    sizing_cfg = cfg.get("sizing", {})
    max_positions = sizing_cfg.get("max_positions", 10)
    ranking_cfg = cfg.get("ranking")
    slippage = sleeve.slippage_bps
    sleeve_label = sleeve.label
    trades: list[dict] = []

    if not ranking_cfg:
        return trades

    sleeve_nav = book.sleeve_nav(sleeve_label, price_index, date)
    if sleeve_nav <= 0:
        return trades

    rules = (cfg.get("rebalancing") or {}).get("rules") or {}
    top_n = ranking_cfg.get("top_n", max_positions)
    entry_rank = int(rules.get("entry_rank") or top_n)
    exit_rank = int(rules.get("exit_rank") or top_n)
    band = float(rules.get("rebalance_band_pct", 0)) / 100.0

    # ---- Re-rank the universe as of `date` -----------------------------
    candidates: list[tuple[str, float]] = []
    for symbol in sorted(sleeve.signals.keys()):
        sig_data = sleeve.signals.get(symbol, {})
        if date in sig_data:
            candidates.append((symbol, sig_data[date]))
    if not candidates:
        return trades
    ranked = rank_candidates(
        candidates, cfg, conn, date, price_index,
        pe_series=sleeve.pe_series,
        composite_series=sleeve.composite_series,
    )
    rank_of = {sym: i + 1 for i, (sym, _) in enumerate(ranked)}  # 1-based

    # ---- Step 1: rotation sells (only past the exit-rank buffer) -------
    survivors: list[str] = []
    for symbol in list(book.symbols_held_by_sleeve(sleeve_label)):
        r = rank_of.get(symbol)
        if r is not None and r <= exit_rank:
            survivors.append(symbol)
            continue
        price = price_index.get(symbol, {}).get(date)
        if not price:
            survivors.append(symbol)  # can't price → can't sell, keep
            continue
        t = book.sell(
            sleeve_label=sleeve_label, symbol=symbol, date=date,
            exec_price=price, reason="rebalance_rotation",
            slippage_bps=slippage,
        )
        if t is not None:
            trades.append(t)
        else:
            survivors.append(symbol)

    # ---- Step 2: pick new entrants (within entry_rank, fill to cap) ----
    slots = max_positions - len(survivors)
    survivor_set = set(survivors)
    entrants: list[str] = []
    if slots > 0:
        for sym, _ in ranked:
            if len(entrants) >= slots or rank_of[sym] > entry_rank:
                break  # ranked is best-first → nothing eligible remains
            if sym in survivor_set:
                continue
            if price_index.get(sym, {}).get(date):
                entrants.append(sym)

    n_targets = len(survivors) + len(entrants)
    if n_targets == 0:
        return trades

    # ---- Step 3: reweight survivors toward equal target (with band) ----
    # No-trade band with trade-to-edge: a survivor whose weight is within
    # `band` of the equal target is left alone. When it breaches, we correct it
    # back only to the NEAR BAND EDGE (target ± band), not all the way to
    # target — the no-trade-region result (Markowitz–van Dijk / Leland). Drift
    # up to the edge was already deemed acceptable, so trading past the edge is
    # wasted turnover. With band == 0 this degrades to full correction to target.
    sleeve_nav = book.sleeve_nav(sleeve_label, price_index, date)
    # Per-name target weights from sizing.type (equal_weight → uniform;
    # risk_parity → inverse-vol). Computed once over the full intended book
    # (survivors + entrants) and reused by Step 4.
    target_weights = _rank_buffer_target_weights(
        survivors + entrants, sleeve, price_index, date, n_targets)
    uniform_w = 1.0 / n_targets
    for symbol in survivors:
        price = price_index.get(symbol, {}).get(date)
        if not price:
            continue
        pos = book.get(sleeve_label, symbol)
        if pos is None:
            continue
        mv = pos.market_value(price)
        dest_amount = _band_dest_amount(mv, sleeve_nav, target_weights.get(symbol, uniform_w), band)
        if dest_amount is None:
            continue  # within no-trade band
        diff = dest_amount - mv
        if diff < -1000:
            trim_pct = min((abs(diff) / mv) * 100, 99)
            trim_shares = pos.shares * (trim_pct / 100.0)
            if trim_shares > 0:
                t = book.sell(
                    sleeve_label=sleeve_label, symbol=symbol, date=date,
                    exec_price=price, reason="rebalance_trim",
                    shares=trim_shares, slippage_bps=slippage,
                )
                if t is not None:
                    trades.append(t)
        elif diff > 1000 and book.sleeve_cash(sleeve_label) > 1000:
            add_amount = min(diff, book.sleeve_cash(sleeve_label) * 0.95)
            if add_amount >= 1000:
                t = book.open(
                    sleeve_label=sleeve_label, symbol=symbol, date=date,
                    amount=add_amount, exec_price=price,
                    slippage_bps=slippage, reason="entry",
                    shares_mode=cfg.get("sizing", {}).get("shares"),
                )
                if t is not None:
                    trades.append(t)

    # ---- Step 4: buy new entrants (at their sizing-derived target) ------
    sleeve_nav = book.sleeve_nav(sleeve_label, price_index, date)
    for symbol in entrants:
        price = price_index.get(symbol, {}).get(date)
        if not price:
            continue
        target_amount = sleeve_nav * target_weights.get(symbol, uniform_w)
        amount = min(target_amount, book.sleeve_cash(sleeve_label) * 0.95)
        if amount <= 0:
            continue
        t = book.open(
            sleeve_label=sleeve_label, symbol=symbol, date=date,
            amount=amount, exec_price=price,
            slippage_bps=slippage, reason="entry",
            shares_mode=cfg.get("sizing", {}).get("shares"),
        )
        if t is not None:
            trades.append(t)

    return trades


def _make_ohlc_fetcher(price_index: dict):
    """Stub OHLC fetcher used by compute_stop_pricing for vol-adaptive modes.

    Returns (high, low, close) bars STRICTLY before `end_date` (no lookahead),
    with high/low = close (degraded; matches v1 when prices are close-only).
    The strict-before-entry semantics mirror v1's make_sqlite_ohlc_fetcher
    contract ("strictly BEFORE entry_date"); otherwise the entry bar leaks
    into the realized-vol window and stop levels diverge from v1.
    """
    def fetch(symbol: str, end_date: str, n: int):
        pm = price_index.get(symbol, {})
        if not pm:
            return []
        closes_dates = sorted(d for d in pm if d < end_date)
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
    seed_trades: list[dict] | None = None,
) -> dict:
    """Daily loop: iterate trading days, apply per-sleeve directives,
    execute trades against the unified PositionBook, emit unified ledger.

    On any day a sleeve transitions from gated-on to gated-off (typically a
    regime/allocation_profile flip), all sleeve-tagged positions are
    liquidated cleanly with reason=`rebalance_to_<profile>`. This is the
    actual broker action when the portfolio reallocates a sleeve to 0%.
    """
    all_trades: list[dict] = list(seed_trades or [])
    ranking_history: list[dict] = []   # one entry per (date, sleeve) ranking event
    last_date = trading_dates[-1] if trading_dates else None
    profile_name_by_date = profile_name_by_date or {}
    # DB-backed OHLC fetcher for vol-adaptive stops (matches v1). Built once,
    # threaded into _execute_pending_entries so every entry's stop/TP uses
    # real high/low data — critical for ATR-mode stops which collapse if
    # high==low==close.
    ohlc_fetcher = make_sqlite_ohlc_fetcher(conn)

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
                ohlc_fetcher=ohlc_fetcher,
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
                    elif d.detail and d.detail.get("action") == "trim_gain":
                        # Ratchet: re-anchor the TP reference to today's price so
                        # the surviving shares need another +value% to trim again.
                        rp = book.get(d.sleeve_label, d.symbol)
                        if rp is not None:
                            rp.tp_reference_price = d.detail.get("new_reference", price)
                    elif d.detail and d.detail.get("action") == "trailing_peak":
                        # Reset the trailing peak to today's price so a fresh new
                        # high + another drop_pct pullback is needed to re-arm.
                        rp = book.get(d.sleeve_label, d.symbol)
                        if rp is not None:
                            rp.trail_high = d.detail.get("reset_high", price)

        # -------------------------------------------------------------------
        # 3. Apply rebalance directives — gated-off days skip entirely
        # -------------------------------------------------------------------
        for sleeve in sleeves:
            if not sleeve.is_gated_on(date):
                continue
            rb_cfg = sleeve.config.get("rebalancing") or {}
            rb_mode = rb_cfg.get("mode", "trim")
            rb_freq = rb_cfg.get("frequency", "none")

            # equal_weight rebalance mode (v1's _do_equal_weight_rebalance):
            # re-rank universe on rebalance day, rotate out positions that
            # fell out of top-N, reweight survivors, buy new entrants.
            # Same rebalance-date gating as the trim-mode path.
            if (rb_mode == "equal_weight" and rb_freq != "none"
                    and is_rebalance_date(date, sleeve.state.last_rebal_date, rb_freq)):
                ew_trades = _apply_equal_weight_rebalance(
                    sleeve=sleeve, book=book, conn=conn,
                    price_index=price_index, date=date,
                )
                all_trades.extend(ew_trades)
                sleeve.state.last_rebal_date = date
                continue

            # target_weight rebalance mode: two-sided drift-control on held
            # positions toward the sizing model's (recomputed) target weights,
            # with a no-trade band. Does NOT rotate the universe.
            if (rb_mode == "target_weight" and rb_freq != "none"
                    and is_rebalance_date(date, sleeve.state.last_rebal_date, rb_freq)):
                tw_trades = _apply_target_weight_rebalance(
                    sleeve=sleeve, book=book, price_index=price_index, date=date,
                )
                all_trades.extend(tw_trades)
                sleeve.state.last_rebal_date = date
                continue

            # rank_buffer rebalance mode: hysteresis rotation — re-rank, sell
            # only names past exit_rank, buy only names within entry_rank, keep
            # survivors in the buffer zone (low-turnover ranking model).
            if (rb_mode == "rank_buffer" and rb_freq != "none"
                    and is_rebalance_date(date, sleeve.state.last_rebal_date, rb_freq)):
                rbf_trades = _apply_rank_buffer_rebalance(
                    sleeve=sleeve, book=book, conn=conn,
                    price_index=price_index, date=date,
                )
                all_trades.extend(rbf_trades)
                sleeve.state.last_rebal_date = date
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
                            shares_mode=sleeve.config.get("sizing", {}).get("shares"),
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
            _pit_today = (sleeve.pit_members_on[date]
                          if sleeve.pit_members_on is not None else None)
            cands = get_entry_candidates(
                sleeve_label=sleeve.label, sleeve_config=sleeve.config, date=date,
                trading_dates=trading_dates,
                signals=sleeve.signals, signal_metadata=sleeve.signal_metadata,
                held_symbols_in_sleeve=held_now, available_slots=available_slots,
                state=sleeve.state, conn=conn, price_index=price_index,
                pe_series=sleeve.pe_series, composite_series=sleeve.composite_series,
                pit_members_today=_pit_today,
            )
            sleeve.pending_entries = cands
            # Capture the ranking event (if any) for the ranking-explorer endpoint.
            # `pop_ranking_event` returns None when fewer candidates than slots
            # made ranking unnecessary, OR when ranking is signal-order only.
            ev = pop_ranking_event(cands)
            if ev is not None:
                ranking_history.append(ev)

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

    return {
        "trades":          all_trades,
        "nav_history":     book.nav_history,
        "ranking_history": ranking_history,
    }


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------
def _seed_opening_positions(
    portfolio_config: dict,
    book: PositionBook,
    sleeve_ctxs: list[_SleeveContext],
    seed_date: str,
) -> list[dict]:
    """Carry pre-existing holdings into the book at deploy time.

    `portfolio_config["opening_positions"]` is a list of
    {symbol, shares, entry_price, entry_date?, sleeve_label?}. Each is seeded
    as an open lot at its REAL fill price (cash debited by the exact cost
    basis). Sleeves that receive a seed are marked `seeded` and have their
    rebalance anchor set to the seed date. The strategy then runs forward from
    this opening book exactly as designed — seeding changes only the starting
    state, not the entry/exit logic (e.g. a full book leaves no open slot, so
    no day-1 top-up; a partial book backfills toward max_positions as usual).
    Returns the BUY trade records for the carried-in lots (prepended to the
    ledger).
    """
    seeds = portfolio_config.get("opening_positions") or []
    if not seeds:
        return []
    by_label = {c.label: c for c in sleeve_ctxs}
    default_label = sleeve_ctxs[0].label if len(sleeve_ctxs) == 1 else None
    trades: list[dict] = []
    for s in seeds:
        label = s.get("sleeve_label") or default_label
        if label is None or label not in by_label:
            raise ValueError(
                f"opening_positions: sleeve_label required and must match a "
                f"sleeve; got {s.get('sleeve_label')!r} for {s.get('symbol')!r}"
            )
        t = book.seed_position(
            sleeve_label=label,
            symbol=s["symbol"],
            entry_date=s.get("entry_date") or seed_date,
            entry_price=float(s["entry_price"]),
            shares=float(s["shares"]),
        )
        if t is not None:
            trades.append(t)
            by_label[label].seeded = True
    # Anchor the rebalance cadence at the seed date for seeded sleeves so the
    # next scheduled rebalance is measured from the carry-in, and the loop
    # suppresses day-1 entry top-ups.
    for c in sleeve_ctxs:
        if c.seeded:
            c.state.last_rebal_date = seed_date
    if any(c.seeded for c in sleeve_ctxs):
        total = sum(t["amount"] for t in trades)
        print(f"Seeded {len(trades)} opening position(s) @ cost ${total:,.2f} "
              f"(cash remaining ${book.cash:,.2f})")
    return trades


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
        # Accept legacy `config` field (older deployments) — mirrors v1's
        # _resolve_strategy_config (portfolio_engine.py:151).
        raw = sd.get("strategy_config") or sd.get("config")
        scfg = validate_strategy(raw)
        # Match v1's portfolio_engine.py:340-345: override the strategy's
        # backtest range with the PORTFOLIO's range. The strategy's own
        # window in the config is the historical training window; the
        # portfolio window is what we actually backtest/deploy.
        scfg["backtest"] = {
            "start": bt_start,
            "end": bt_end,
            "slippage_bps": scfg.get("backtest", {}).get("slippage_bps", 10),
            "entry_price": scfg.get("backtest", {}).get("entry_price", "next_close"),
        }
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
        # Precompute PIT membership for the sleeve (None when not index-typed).
        ctx.pit_members_on = pit_members_by_date(scfg, conn, trading_dates)
        if ctx.pit_members_on is not None:
            print(f"  Sleeve '{ctx.label}' PIT membership precomputed.")
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
    # Carry in pre-existing holdings (real broker fills) when provided. Must
    # run before the daily loop so day 1 sees the seeded book.
    seed_trades = _seed_opening_positions(
        portfolio_config, book, sleeve_ctxs, seed_date=trading_dates[0],
    )
    print(f"Running V2 simulation with ${initial_capital:,.0f}...")
    loop_result = _run_daily_loop(
        book=book, sleeves=sleeve_ctxs,
        price_index=price_index, open_index=open_index,
        trading_dates=trading_dates, conn=conn,
        earnings_data=earnings_data,
        force_close_at_end=force_close_at_end,
        initial_capital=initial_capital,
        profile_name_by_date=profile_name_by_date,
        seed_trades=seed_trades,
    )
    conn.close()

    # --- Build metrics ---
    # Use v1's compute_metrics by passing book in place of v1's Portfolio
    # (the function only reads .nav_history, .trades, .closed_trades).
    book.trades = loop_result["trades"]    # for compute_metrics' total_entries count
    metrics = compute_metrics(book, initial_capital, trading_dates)

    # --- Benchmarks (same as v1) ---
    # Both `market_bench` and `sector_bench` (when available) contain
    # nav_history arrays aligned to `trading_dates`, plus per-period
    # metrics. We expose the full dicts via the result so the API can serve
    # benchmark NAV curves to the frontend (for the equity overlay chart).
    market_bench = compute_benchmark(trading_dates, initial_capital, sector=None)
    sector_bench: dict | None = None
    if market_bench:
        market_total = market_bench["metrics"].get("total_return_pct")
        market_ann = market_bench["metrics"].get("annualized_return_pct")
        metrics["market_benchmark_return_pct"] = market_total
        metrics["market_benchmark_ann_return_pct"] = market_ann
        # v1 also writes a generic `benchmark_return_pct` alias (defaults to
        # the market benchmark). deploy_engine.py reads that key when it
        # updates `last_benchmark_return_pct` on the deployments row, which
        # the frontend renders as "SPX return". V2 was missing this alias,
        # so the frontend column came up empty under v2.
        metrics["benchmark_return_pct"] = market_total
        # Annualized alpha — gated by min_days_for_annualization in
        # compute_metrics. Period alpha (always populatable) lives in
        # alpha_vs_market_pct_period; consumers that want a day-1 number
        # use that instead.
        metrics["alpha_vs_market_pct"] = (
            metrics["annualized_return_pct"] - market_ann
            if metrics.get("annualized_return_pct") is not None and market_ann is not None
            else None
        )
        # Period alpha: strategy total return - benchmark total return.
        # Always populated when both totals exist. Matches the "period sharpe"
        # pattern in _nav_metrics where we expose a basis-aware figure.
        strat_total = metrics.get("total_return_pct")
        if strat_total is not None and market_total is not None:
            metrics["alpha_vs_market_pct_period"] = round(strat_total - market_total, 2)
            metrics["alpha_ann_pct"] = metrics.get("alpha_vs_market_pct")

    # Sector benchmarks — one buy-and-hold ETF per sector represented in the
    # universe. Multi-sector portfolios (e.g. Tech + Comm Services) get
    # multiple overlays so the frontend can show each sector ETF as its own
    # line. The singular `benchmark_sector` (primary = most-represented
    # sector) is preserved for back-compat with existing FE consumers.
    from portfolio_engine import _infer_sleeve_sectors_with_counts
    sector_counts: dict[str, int] = {}
    for ctx in sleeve_ctxs:
        for sec, n in _infer_sleeve_sectors_with_counts(ctx.config).items():
            sector_counts[sec] = sector_counts.get(sec, 0) + n
    # Most-represented first; drop sectors we have no ETF mapping for.
    ordered_sectors = sorted(
        (s for s in sector_counts if s in SECTOR_ETF_MAP),
        key=lambda s: -sector_counts[s],
    )
    benchmark_sectors: list[dict] = []
    for sec in ordered_sectors:
        b = compute_benchmark(trading_dates, initial_capital, sector=sec)
        if b:
            b["sector"] = sec  # tag so the FE can label each line
            benchmark_sectors.append(b)
    sector_bench = benchmark_sectors[0] if benchmark_sectors else None

    if sector_bench:
        sector_total = sector_bench["metrics"].get("total_return_pct")
        sector_ann = sector_bench["metrics"].get("annualized_return_pct")
        metrics["sector_benchmark_return_pct"] = sector_total
        metrics["sector_benchmark_ann_return_pct"] = sector_ann
        metrics["alpha_vs_sector_pct"] = (
            metrics["annualized_return_pct"] - sector_ann
            if metrics.get("annualized_return_pct") is not None and sector_ann is not None
            else None
        )
        strat_total = metrics.get("total_return_pct")
        if strat_total is not None and sector_total is not None:
            metrics["alpha_vs_sector_pct_period"] = round(strat_total - sector_total, 2)
            metrics["period_excess_vs_sector_pct"] = metrics["alpha_vs_sector_pct_period"]

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

    # closed_trades and open_positions per sleeve — needed by the API's
    # /deployments/{id}/positions endpoint (calls _build_position_book which
    # reads sleeve["open_positions"] and sleeve["closed_trades"]). V2 was
    # only emitting `trades`; the positions table on the frontend came up
    # empty as a result.
    closed_by_sleeve: dict[str, list[dict]] = {ctx.label: [] for ctx in sleeve_ctxs}
    for ct in getattr(book, "closed_trades", []):
        lbl = ct.get("sleeve_label")
        if lbl in closed_by_sleeve:
            closed_by_sleeve[lbl].append(ct)

    from datetime import datetime as _dt
    last_date = trading_dates[-1] if trading_dates else None

    def _open_positions_for_sleeve(sleeve_label: str) -> list[dict]:
        """Build the {symbol, entry_*, current_*, pnl, ...} list of currently-
        held positions for a sleeve. Same shape v1 emits in
        backtest_engine.py:2724 so _build_position_book is engine-agnostic."""
        out: list[dict] = []
        if last_date is None:
            return out
        for (lbl, sym), pos in book.positions.items():
            if lbl != sleeve_label:
                continue
            cur_px = price_index.get(sym, {}).get(last_date)
            if cur_px is None:
                cur_px = pos.high_since_entry
            pnl_pct = ((cur_px - pos.entry_price) / pos.entry_price * 100
                       if pos.entry_price else 0.0)
            days_held = 0
            if pos.entry_date and last_date:
                try:
                    days_held = (_dt.strptime(last_date, "%Y-%m-%d")
                                 - _dt.strptime(pos.entry_date, "%Y-%m-%d")).days
                except ValueError:
                    days_held = 0
            out.append({
                "symbol": sym,
                "entry_date": pos.entry_date,
                "entry_price": round(pos.entry_price, 2),
                "current_price": round(cur_px, 2),
                "shares": round(pos.shares, 4),
                "market_value": round(pos.shares * cur_px, 2),
                "cost_basis": round(pos.shares * pos.entry_price, 2),
                "pnl": round(pos.shares * (cur_px - pos.entry_price), 2),
                "pnl_pct": round(pnl_pct, 2),
                "days_held": days_held,
            })
        return out

    sleeve_results = [{
        "label": ctx.label,
        "trades": trades_by_sleeve.get(ctx.label, []),
        "closed_trades": closed_by_sleeve.get(ctx.label, []),
        "open_positions": _open_positions_for_sleeve(ctx.label),
        "nav_history": loop_result["nav_history"],   # combined book nav for now
    } for ctx in sleeve_ctxs]

    per_sleeve = [{
        "label": ctx.label,
        "weight": ctx.weight,
        "allocated_capital": ctx.allocated_capital,
        "regime_gate": ctx.regime_gate,
    } for ctx in sleeve_ctxs]

    # --- Dense regime + allocation timelines for frontend rendering ---------
    # Both series are aligned to `trading_dates` so the frontend can zip them
    # against `nav_history` without interpolation (same pattern as benchmark_nav).
    #
    # regime_history[i].active_regimes is empty when no regime is firing — that
    # is information ("calm"), not absence. Always emit one row per trading day
    # so the frontend gets a guaranteed-aligned series.
    sleeve_labels_set = {ctx.label for ctx in sleeve_ctxs}
    regime_history: list[dict] = []
    allocation_history: list[dict] = []
    for d in trading_dates:
        active = list(regime_series.get(d, []))
        regime_history.append({"date": d, "active_regimes": active})

        if profile_weights_by_date:
            pname = profile_name_by_date.get(d)
            raw_w = profile_weights_by_date.get(d, {}) or {}
            # Normalize: surface every sleeve label explicitly, drop unknown
            # keys (except Cash), compute Cash = 1 - sum(sleeve_weights) so the
            # row always sums to 1.0 and the frontend never has to guess.
            tw: dict[str, float] = {}
            for lbl in sleeve_labels_set:
                tw[lbl] = float(raw_w.get(lbl, 0.0) or 0.0)
            sleeve_sum = sum(tw.values())
            tw["Cash"] = max(0.0, round(1.0 - sleeve_sum, 6))
            allocation_history.append({
                "date": d,
                "profile_name": pname,
                "target_weights": tw,
            })

    # Derived sparse view (one row per profile *change*) — back-compat with v1's
    # `allocation_profile_history` consumers. Empty when no allocation_profiles
    # are configured.
    allocation_profile_history: list[dict] = []
    prev_profile = None
    prev_weights: dict | None = None
    for row in allocation_history:
        pname = row["profile_name"]
        tw = row["target_weights"]
        if pname != prev_profile:
            entry = {"date": row["date"], "profile_name": pname, "weights": tw}
            if prev_weights is not None:
                entry["from_weights"] = prev_weights
                entry["transition"] = "instant"  # v2 doesn't smooth yet
            else:
                entry["transition"] = "instant (initial)"
            allocation_profile_history.append(entry)
            prev_profile = pname
            prev_weights = tw

    # --- Annotate regime_definitions with display labels --------------------
    # The raw config carries series codes (e.g. "hy_spread_zscore"). We stamp
    # human-readable labels next to them so the frontend can render the trigger
    # string ("HY Spread Z-Score > 2.0 OR ...") without maintaining a parallel
    # code→label mapping. Done on a shallow copy so the caller's config dict
    # isn't mutated.
    config_out = dict(portfolio_config)
    raw_defs = portfolio_config.get("regime_definitions") or {}
    if isinstance(raw_defs, dict) and raw_defs:
        annotated: dict = {}
        for rid, defn in raw_defs.items():
            if not isinstance(defn, dict):
                annotated[rid] = defn
                continue
            d2 = dict(defn)
            d2.setdefault("label", _humanize_regime_id(rid))
            conds_out = []
            for c in defn.get("conditions", []) or []:
                if isinstance(c, dict):
                    c2 = dict(c)
                    series_code = c2.get("series")
                    if series_code and "series_label" not in c2:
                        c2["series_label"] = _humanize_series_code(series_code)
                    conds_out.append(c2)
                else:
                    conds_out.append(c)
            d2["conditions"] = conds_out
            annotated[rid] = d2
        config_out["regime_definitions"] = annotated

    # --- Stamp ranking_model on every sleeve --------------------------------
    # Presentation-ready projection of `ranking` + `composite_score` so the
    # frontend renders a "Scoring Model" card without normalizing weights or
    # resolving factor labels client-side. See scripts/ranking_model.py.
    from ranking_model import build_ranking_model
    raw_sleeves = portfolio_config.get("sleeves") or []
    if isinstance(raw_sleeves, list) and raw_sleeves:
        annotated_sleeves: list = []
        for s in raw_sleeves:
            if isinstance(s, dict):
                s2 = dict(s)
                scfg = s2.get("strategy_config") or {}
                rm = build_ranking_model(scfg)
                if rm is not None:
                    s2["ranking_model"] = rm
                annotated_sleeves.append(s2)
            else:
                annotated_sleeves.append(s)
        config_out["sleeves"] = annotated_sleeves

    return {
        "portfolio": name,
        "engine_version": "v2",
        "metrics": metrics,
        "trades": loop_result["trades"],
        "nav_history": loop_result["nav_history"],
        "combined_nav_history": loop_result["nav_history"],   # alias for compat
        "sleeve_results": sleeve_results,
        "per_sleeve": per_sleeve,
        "config": config_out,
        # Dense daily series (one row per trading_date) — frontend zips these
        # against `nav_history` to render the regime + allocation overlay.
        "regime_history":              regime_history,
        "allocation_history":          allocation_history,
        # Sparse view (one row per profile transition) — back-compat with v1.
        "allocation_profile_history":  allocation_profile_history,
        # One entry per (date, sleeve) ranking event. Each entry carries the
        # full leaderboard (picked + not-picked candidates) so the frontend's
        # ranking-explorer view can render alternatives next to the picks.
        "ranking_history":             loop_result.get("ranking_history", []),
        "backtest": {"start": bt_start, "end": bt_end,
                     "initial_capital": initial_capital},
        # Benchmarks: same keys v1 emits. Each is the full compute_benchmark()
        # dict (with `symbol`, `nav_history`, `metrics`). `benchmark` is the
        # primary (sector if available, else market) for legacy callers.
        # deploy_engine reads benchmark_market / benchmark_sector when
        # writing results.json, and the API's GET /deployments/{id} surfaces
        # them as-is. The flat `benchmark_nav` shape the frontend equity
        # chart consumes is derived in the API layer.
        "benchmark":         sector_bench or market_bench,
        "benchmark_market":  market_bench,
        "benchmark_sector":  sector_bench,
        # Per-sector list — one entry per sector touched by the universe (each
        # with its own ETF buy-and-hold curve). Single-sector portfolios get
        # a one-element list mirroring `benchmark_sector`; multi-sector
        # portfolios get N entries the FE can render as separate overlay lines.
        "benchmark_sectors": benchmark_sectors,
    }
