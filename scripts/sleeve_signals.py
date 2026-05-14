"""
SleeveSignals — pure directive generators for the v2 portfolio engine.

Phase 2 of the live-trading plan separates "what a sleeve recommends" from
"what the portfolio executes." In v1, sleeves were independent simulators
that managed their own cash + positions; the portfolio-level lerp ran on
top, producing the dual-bookkeeping bug.

In v2, sleeves are PURE SIGNAL GENERATORS:
  - get_entry_candidates(date, ...) → list of EntryDirective
  - get_exit_recommendations(date, ...) → list of ExitDirective
  - get_rebalance_directives(date, ...) → list of RebalanceDirective

Each directive is a recommendation. The PortfolioExecutor (Step 3) reads
directives across all sleeves, reconciles them with the unified
PositionBook, and emits one daily trade list to the broker.

These functions are PURE — they don't mutate state, they don't keep
between-day memory. They re-use v1's pure utility functions
(check_stop_loss / check_take_profit / check_time_stop / is_rebalance_date /
rank_candidates) which are battle-tested by the audit suite. The directives
they emit go through the same logical conditions v1 used, just expressed as
recommendations instead of side-effects.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))

from backtest_engine import (
    check_stop_loss as _check_stop_loss,
    check_take_profit as _check_take_profit,
    check_time_stop as _check_time_stop,
    is_rebalance_date as _is_rebalance_date,
    rank_candidates as _rank_candidates,
    _calendar_to_trading_days,
)
from position_book import Position


# ---------------------------------------------------------------------------
# Directive types — pure data, no logic. The executor reads these.
# ---------------------------------------------------------------------------
@dataclass
class EntryDirective:
    """A sleeve recommends opening this position."""
    sleeve_label: str
    symbol: str
    signal_value: float                  # the raw signal value (drawdown, feature value, etc.)
    rank_score: Optional[float] = None   # post-ranking score; None means signal-order
    signal_detail: Optional[dict] = None # rich metadata for the trade record
    peak_price: Optional[float] = None   # pre-entry peak (for above_peak TP)


@dataclass
class ExitDirective:
    """A sleeve recommends closing (or partially closing) one of its positions.

    `shares=None` means full close. Otherwise partial.
    """
    sleeve_label: str
    symbol: str
    reason: str                          # stop_loss, take_profit, time_stop, fundamental_exit
    shares: Optional[float] = None
    detail: Optional[dict] = None        # e.g. {"reason": "revenue_deceleration"} for fundamental_exit


@dataclass
class RebalanceDirective:
    """A sleeve recommends a rebalance trim or add-on on an existing position.

    `action="SELL"` = trim; `action="BUY"` = add.
    Either `shares` or `amount` is set, not both.
    """
    sleeve_label: str
    symbol: str
    action: str                          # "BUY" | "SELL"
    reason: str                          # rebalance_trim, rebalance_rotation, entry (for adds)
    shares: Optional[float] = None
    amount: Optional[float] = None
    detail: Optional[dict] = None


# ---------------------------------------------------------------------------
# Cooldown state — per-sleeve, owned by the executor and threaded in.
# ---------------------------------------------------------------------------
@dataclass
class SleeveRuntimeState:
    """Per-sleeve mutable state the executor maintains across days.

    Kept separate from PositionBook (which is portfolio-wide) and from the
    directive functions (which are pure). The executor passes this in to
    cooldown / rebalance-frequency checks.
    """
    sleeve_label: str
    last_rebal_date: Optional[str] = None
    stop_loss_cooldowns: dict[str, str] = field(default_factory=dict)  # {symbol: date_of_stop}


# ---------------------------------------------------------------------------
# Entry candidates
# ---------------------------------------------------------------------------
def get_entry_candidates(
    sleeve_label: str,
    sleeve_config: dict,
    date: str,
    trading_dates: list[str],
    signals: dict[str, dict],            # {sym: {date: signal_value}}
    signal_metadata: dict[str, dict],    # {sym: {date: signal_detail}}
    held_symbols_in_sleeve: set[str],    # don't re-enter what this sleeve already holds
    available_slots: int,
    state: SleeveRuntimeState,
    conn,
    price_index: dict,
    pe_series: dict | None = None,
    composite_series: dict | None = None,
) -> list[EntryDirective]:
    """Pure directive generator: which symbols should this sleeve open today.

    Mirrors v1's entry-collection block in run_backtest, but emits directives
    instead of mutating a portfolio.

    Steps (matching v1 logic):
      1. Filter symbols where today's signal fires.
      2. Exclude symbols this sleeve already holds (different sleeves can
         hold the same symbol — that's a portfolio-level decision; sleeve-
         level deduplication is correct).
      3. Apply stop-loss cooldown filter (if a stop fired within last
         cooldown_days for this sleeve, don't re-enter).
      4. Rank candidates via v1's rank_candidates (using preloaded pe/composite
         series for performance).
      5. Take top-N where N = min(available_slots, ranking.top_n or unlimited).

    Returns a list of EntryDirective, ordered by rank (best first).
    """
    if available_slots <= 0:
        return []

    cfg = sleeve_config
    universe = list(signals.keys())   # only symbols that have ANY signal in the precomputed dict

    # Cooldown setup (per-sleeve via state)
    cooldown_calendar_days = 0
    if cfg.get("stop_loss"):
        cooldown_calendar_days = cfg["stop_loss"].get("cooldown_days", 0)
    cooldown_td = _calendar_to_trading_days(cooldown_calendar_days) if cooldown_calendar_days > 0 else 0

    date_idx = trading_dates.index(date) if date in trading_dates else -1

    candidates: list[tuple[str, float]] = []
    for symbol in universe:
        if symbol in held_symbols_in_sleeve:
            continue
        sig_data = signals.get(symbol, {})
        if date not in sig_data:
            continue
        # Cooldown
        if cooldown_td > 0 and symbol in state.stop_loss_cooldowns:
            sl_date = state.stop_loss_cooldowns[symbol]
            sl_idx = trading_dates.index(sl_date) if sl_date in trading_dates else -1
            if sl_idx >= 0 and (date_idx - sl_idx) < cooldown_td:
                continue
        signal_value = sig_data[date]
        candidates.append((symbol, signal_value))

    if not candidates:
        return []

    # Rank (delegates to v1's rank_candidates which already supports composite_series)
    ranking_cfg = cfg.get("ranking") or {}
    if ranking_cfg and len(candidates) > available_slots:
        ranked = _rank_candidates(
            candidates, cfg, conn, date, price_index,
            pe_series=pe_series, rsi_cache=None,
            composite_series=composite_series,
        )
        top_n = ranking_cfg.get("top_n")
        if top_n is not None and top_n < available_slots:
            ranked = ranked[:top_n]
        else:
            ranked = ranked[:available_slots]
    else:
        # Fewer candidates than slots, or no ranking: keep all up to slot count.
        # When v1 has no ranking + more candidates than slots, it falls back
        # to pe_percentile asc via rank_candidates' internal default.
        if len(candidates) > available_slots:
            ranked = _rank_candidates(
                candidates, cfg, conn, date, price_index,
                pe_series=pe_series, rsi_cache=None,
                composite_series=composite_series,
            )[:available_slots]
        else:
            # priority handling for under-capacity batches
            entry_priority = (cfg.get("entry") or {}).get("priority", "worst_drawdown")
            if entry_priority == "worst_drawdown":
                ranked = sorted(candidates, key=lambda x: x[1])  # most negative first
            else:
                ranked = candidates

    # Convert to EntryDirective
    out: list[EntryDirective] = []
    for symbol, signal_value in ranked:
        sig_detail = signal_metadata.get(symbol, {}).get(date)
        out.append(EntryDirective(
            sleeve_label=sleeve_label,
            symbol=symbol,
            signal_value=signal_value,
            signal_detail=sig_detail,
            peak_price=None,  # computed by the executor at fill time (needs price_index lookup)
        ))
    return out


# ---------------------------------------------------------------------------
# Exit recommendations
# ---------------------------------------------------------------------------
def get_exit_recommendations(
    sleeve_label: str,
    sleeve_config: dict,
    date: str,
    sleeve_positions: dict[str, Position],
    price_index: dict,
    exit_signals: dict | None = None,    # {sym: {date: {"reason": ...}}}
) -> list[ExitDirective]:
    """Pure directive generator: which of this sleeve's positions should exit today.

    Mirrors v1's exit check order:
      1. stop_loss   (sets cooldown — executor handles state update)
      2. take_profit
      3. time_stop
      4. fundamental_exit (from precomputed exit_signals)

    The cooldown is recorded via the returned ExitDirective.reason ==
    "stop_loss"; the executor updates SleeveRuntimeState.stop_loss_cooldowns
    when it processes a stop_loss directive.

    Only one exit can fire per position per day (matching v1 — first match
    wins, others skipped).
    """
    out: list[ExitDirective] = []

    for symbol, pos in sleeve_positions.items():
        price = price_index.get(symbol, {}).get(date)
        if price is None:
            continue  # no fresh price → no exit can be checked today

        # Update trailing high BEFORE running exit checks (matches v1 sequence
        # in Portfolio.nav, which observed prices before record_nav).
        pos.observe_price(price)

        # Check stop_loss first
        if _check_stop_loss(pos, price, sleeve_config):
            out.append(ExitDirective(
                sleeve_label=sleeve_label, symbol=symbol,
                reason="stop_loss", shares=None,
            ))
            continue

        # Take profit
        if _check_take_profit(pos, price, sleeve_config):
            out.append(ExitDirective(
                sleeve_label=sleeve_label, symbol=symbol,
                reason="take_profit", shares=None,
            ))
            continue

        # Time stop
        if _check_time_stop(pos, date, sleeve_config):
            out.append(ExitDirective(
                sleeve_label=sleeve_label, symbol=symbol,
                reason="time_stop", shares=None,
            ))
            continue

        # Fundamental exit (from precomputed exit_signals)
        if exit_signals is not None:
            ex_today = exit_signals.get(symbol, {}).get(date)
            if ex_today:
                reason = ex_today.get("reason", "fundamental_exit")
                out.append(ExitDirective(
                    sleeve_label=sleeve_label, symbol=symbol,
                    reason=reason, shares=None,
                    detail=ex_today,
                ))
                continue

    return out


# ---------------------------------------------------------------------------
# Rebalance directives
# ---------------------------------------------------------------------------
def get_rebalance_directives(
    sleeve_label: str,
    sleeve_config: dict,
    date: str,
    sleeve_positions: dict[str, Position],
    price_index: dict,
    state: SleeveRuntimeState,
    sleeve_nav: float,                 # this sleeve's total NAV (positions + its share of cash)
    earnings_data: dict | None = None,
) -> list[RebalanceDirective]:
    """Pure directive generator: trim / add directives for this sleeve.

    Currently supports the v1 "trim" mode rebalance:
      - on_earnings_beat: "add" — add to positions that beat earnings in
        lookback_days by >= min_gain_pct, capped at max_add_multiplier
      - on_earnings_miss: "trim" — reduce positions that missed by trim_pct
      - max_position_pct: trim positions exceeding the cap

    The equal_weight rebalance mode (rotation across the universe) is a
    bigger reshape — Step 3 (the executor) will handle that via a separate
    code path that uses get_entry_candidates + sells everything not picked.

    Returns an empty list if today is not a rebalance date for this sleeve.
    """
    rb = sleeve_config.get("rebalancing") or {}
    freq = rb.get("frequency", "none")
    if freq == "none":
        return []

    # Special case: on_earnings frequency fires on earnings dates, not calendar
    if freq == "on_earnings":
        return _earnings_driven_rebalance(
            sleeve_label, sleeve_config, date, sleeve_positions,
            price_index, earnings_data,
        )

    # Calendar-frequency: only fire on rebalance dates
    if not _is_rebalance_date(date, state.last_rebal_date, freq):
        return []

    # Calendar rebalance — apply trim rules + on_earnings_beat/miss within
    # the lookback window of the rebalance date.
    out = _calendar_rebalance(
        sleeve_label, sleeve_config, date, sleeve_positions,
        price_index, sleeve_nav, earnings_data,
    )
    return out


def _calendar_rebalance(
    sleeve_label: str,
    sleeve_config: dict,
    date: str,
    sleeve_positions: dict[str, Position],
    price_index: dict,
    sleeve_nav: float,
    earnings_data: dict | None,
) -> list[RebalanceDirective]:
    """Calendar (quarterly/monthly) rebalance: trim oversized + earnings adjustments."""
    rules = (sleeve_config.get("rebalancing") or {}).get("rules", {}) or {}
    max_pct = float(rules.get("max_position_pct", 100))
    trim_pct = float(rules.get("trim_pct", 50)) / 100.0   # fraction
    on_beat = rules.get("on_earnings_beat")
    on_miss = rules.get("on_earnings_miss")
    add_cfg = rules.get("add_on_earnings_beat") or {}
    add_lookback_days = int(add_cfg.get("lookback_days", 90))
    add_min_gain_pct = float(add_cfg.get("min_gain_pct", 0))
    add_max_mult = float(add_cfg.get("max_add_multiplier", 1.0))

    out: list[RebalanceDirective] = []

    for symbol, pos in sleeve_positions.items():
        price = price_index.get(symbol, {}).get(date)
        if price is None:
            continue
        mv = pos.market_value(price)
        sleeve_pct = (mv / sleeve_nav * 100.0) if sleeve_nav > 0 else 0.0

        # Earnings event lookup
        ep = _earnings_within_lookback(earnings_data, symbol, date, add_lookback_days)

        # on_earnings_miss → trim
        if on_miss == "trim" and ep and ep.get("type") == "miss":
            trim_shares = pos.shares * trim_pct
            if trim_shares > 0:
                out.append(RebalanceDirective(
                    sleeve_label=sleeve_label, symbol=symbol,
                    action="SELL", reason="rebalance_trim",
                    shares=trim_shares,
                    detail={"trigger": "earnings_miss"},
                ))
            continue

        # on_earnings_beat → add
        if on_beat == "add" and ep and ep.get("type") == "beat":
            gain_pct = float(ep.get("gain_pct", 0))
            if gain_pct >= add_min_gain_pct:
                # Add shares until total ≤ max_add_multiplier × current
                target_shares = pos.shares * add_max_mult
                add_shares = max(0.0, target_shares - pos.shares)
                if add_shares > 0:
                    out.append(RebalanceDirective(
                        sleeve_label=sleeve_label, symbol=symbol,
                        action="BUY", reason="entry",
                        shares=add_shares,
                        detail={"trigger": "earnings_beat",
                                "gain_pct": gain_pct},
                    ))
            continue

        # max_position_pct trim (universal)
        if sleeve_pct > max_pct and max_pct > 0:
            target_mv = sleeve_nav * (max_pct / 100.0)
            trim_mv = mv - target_mv
            trim_shares = trim_mv / price if price > 0 else 0
            if trim_shares > 0:
                out.append(RebalanceDirective(
                    sleeve_label=sleeve_label, symbol=symbol,
                    action="SELL", reason="rebalance_trim",
                    shares=trim_shares,
                    detail={"trigger": "max_position_pct"},
                ))

    return out


def _earnings_driven_rebalance(
    sleeve_label: str,
    sleeve_config: dict,
    date: str,
    sleeve_positions: dict[str, Position],
    price_index: dict,
    earnings_data: dict | None,
) -> list[RebalanceDirective]:
    """frequency='on_earnings': fire trim/add on the earnings event date itself."""
    rules = (sleeve_config.get("rebalancing") or {}).get("rules", {}) or {}
    trim_pct = float(rules.get("trim_pct", 50)) / 100.0
    on_beat = rules.get("on_earnings_beat")
    on_miss = rules.get("on_earnings_miss")
    add_cfg = rules.get("add_on_earnings_beat") or {}
    add_max_mult = float(add_cfg.get("max_add_multiplier", 1.0))
    add_min_gain_pct = float(add_cfg.get("min_gain_pct", 0))

    out: list[RebalanceDirective] = []
    for symbol, pos in sleeve_positions.items():
        # Earnings event AT THIS DATE for this symbol
        ep = _earnings_on_date(earnings_data, symbol, date)
        if not ep:
            continue
        if on_miss == "trim" and ep.get("type") == "miss":
            out.append(RebalanceDirective(
                sleeve_label=sleeve_label, symbol=symbol,
                action="SELL", reason="rebalance_trim",
                shares=pos.shares * trim_pct,
                detail={"trigger": "earnings_miss"},
            ))
        elif on_beat == "add" and ep.get("type") == "beat":
            gain_pct = float(ep.get("gain_pct", 0))
            if gain_pct >= add_min_gain_pct:
                target_shares = pos.shares * add_max_mult
                add_shares = max(0.0, target_shares - pos.shares)
                if add_shares > 0:
                    out.append(RebalanceDirective(
                        sleeve_label=sleeve_label, symbol=symbol,
                        action="BUY", reason="entry",
                        shares=add_shares,
                        detail={"trigger": "earnings_beat",
                                "gain_pct": gain_pct},
                    ))
    return out


# ---------------------------------------------------------------------------
# Earnings helpers (extracted from v1's _do_rebalance for reuse here)
# ---------------------------------------------------------------------------
def _earnings_within_lookback(
    earnings_data: dict | None,
    symbol: str,
    date: str,
    lookback_days: int,
) -> dict | None:
    """Return the most recent earnings event for symbol within `lookback_days`
    before `date`, or None. The event dict carries {date, type=beat|miss,
    gain_pct, ...} — exact shape matches v1's load_earnings_data output.
    """
    if not earnings_data or symbol not in earnings_data:
        return None
    from datetime import datetime, timedelta
    d = datetime.strptime(date, "%Y-%m-%d")
    cutoff = (d - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    for ev in reversed(earnings_data.get(symbol, [])):
        ev_date = ev.get("date")
        if ev_date is None:
            continue
        if cutoff <= ev_date <= date:
            return ev
        if ev_date < cutoff:
            break
    return None


def _earnings_on_date(
    earnings_data: dict | None,
    symbol: str,
    date: str,
) -> dict | None:
    """Return the earnings event for symbol whose date == `date`, or None."""
    if not earnings_data or symbol not in earnings_data:
        return None
    for ev in earnings_data.get(symbol, []):
        if ev.get("date") == date:
            return ev
    return None
