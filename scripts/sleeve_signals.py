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
    pit_members_today: frozenset[str] | None = None,
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
    # Iterate in alphabetical order to match v1's resolve_universe(sorted=True).
    # Stable-sort ties downstream then depend on this ordering, so misaligning
    # it causes shares-per-fill drift on multi-symbol days with tied signals.
    universe = sorted(signals.keys())

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
        # PIT membership gate: only as-of members of the configured index
        # can be NEW entries. Existing holdings carry through index removals.
        # None => no PIT filter (universe.type != 'index').
        if pit_members_today is not None and symbol not in pit_members_today:
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
    available_cash: float,             # cash available for adds (v1's portfolio.cash)
    earnings_data: dict | None = None,
) -> list[RebalanceDirective]:
    """Pure directive generator: trim / add directives for this sleeve.

    Replicates v1's _do_rebalance (trim mode):
      - max_position_pct trim: positions exceeding cap trimmed proportionally
      - add_on_earnings_beat: positions up > min_gain_pct AND with a recent
        beat get an add (capped at max_add_multiplier × original_cost,
        25% of cash, and max_pct of NAV).

    The equal_weight rebalance mode is handled by a separate executor path.

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

    return _calendar_rebalance(
        sleeve_label, sleeve_config, date, sleeve_positions,
        price_index, sleeve_nav, available_cash, earnings_data,
    )


def _calendar_rebalance(
    sleeve_label: str,
    sleeve_config: dict,
    date: str,
    sleeve_positions: dict[str, Position],
    price_index: dict,
    sleeve_nav: float,
    available_cash: float,
    earnings_data: dict | None,
) -> list[RebalanceDirective]:
    """Calendar (quarterly/monthly) rebalance: trim oversized + earnings-beat adds.

    Mirrors v1's backtest_engine._do_rebalance step-by-step:
      1. For each held position: if current weight > max_position_pct, emit
         a SELL trim directive with partial_pct = ((weight - max_pct) / weight) * 100.
         The reason is `rebalance_trim`.
      2. For each held position: if pnl_pct >= add_on_earnings_beat.min_gain_pct
         AND there was a beat within lookback_days, compute room_to_add =
         original_cost × max_add_multiplier - current_value. Add up to
         min(room_to_add, available_cash × 0.25, post-cap room) with reason
         `entry` (same as v1, which calls open_position with the default reason).

    earnings_data shape: {symbol: {date: {eps_actual, eps_estimated, beat}}}.
    """
    from datetime import datetime as _dt
    rules = (sleeve_config.get("rebalancing") or {}).get("rules", {}) or {}
    max_pct = float(rules.get("max_position_pct", 100))
    add_cfg = rules.get("add_on_earnings_beat")

    out: list[RebalanceDirective] = []

    # ---- Step 1: max_pct trim ------------------------------------------------
    for symbol, pos in sleeve_positions.items():
        price = price_index.get(symbol, {}).get(date)
        if price is None:
            continue
        mv = pos.market_value(price)
        weight = (mv / sleeve_nav * 100.0) if sleeve_nav > 0 else 0.0
        if weight > max_pct and max_pct > 0:
            # Match v1 exactly: trim_pct = ((weight - max_pct) / weight) * 100
            # → fraction = (weight - max_pct) / weight
            fraction = (weight - max_pct) / weight
            trim_shares = pos.shares * fraction
            if trim_shares > 0:
                out.append(RebalanceDirective(
                    sleeve_label=sleeve_label, symbol=symbol,
                    action="SELL", reason="rebalance_trim",
                    shares=trim_shares,
                    detail={"trigger": "max_position_pct"},
                ))

    # ---- Step 2: earnings-beat add ------------------------------------------
    if not add_cfg or not earnings_data:
        return out
    gain_threshold = float(add_cfg.get("min_gain_pct", 15))
    max_add_multiplier = float(add_cfg.get("max_add_multiplier", 1.5))
    lookback_days = int(add_cfg.get("lookback_days", 90))
    current_dt = _dt.strptime(date, "%Y-%m-%d")

    for symbol, pos in sleeve_positions.items():
        price = price_index.get(symbol, {}).get(date)
        if price is None:
            continue
        # Has the position gained enough?
        if pos.pnl_pct(price) < gain_threshold:
            continue
        # Recent earnings beat?
        sym_earnings = earnings_data.get(symbol, {})
        recent_beat = False
        for earn_date, earn_data in sym_earnings.items():
            earn_dt = _dt.strptime(earn_date, "%Y-%m-%d")
            days_ago = (current_dt - earn_dt).days
            if 0 <= days_ago <= lookback_days and earn_data.get("beat"):
                recent_beat = True
                break
        if not recent_beat:
            continue
        original_cost = pos.entry_price * pos.shares
        max_total = original_cost * max_add_multiplier
        current_value = pos.market_value(price)
        room_to_add = max_total - current_value
        if room_to_add <= 1000:
            continue
        amount = min(room_to_add, available_cash * 0.25)
        if amount < 1000:
            continue
        # Weight cap on the post-add value
        new_weight = ((current_value + amount) / sleeve_nav) * 100 if sleeve_nav > 0 else 0
        if new_weight > max_pct and max_pct > 0:
            amount = (max_pct / 100 * sleeve_nav) - current_value
            if amount < 1000:
                continue
        out.append(RebalanceDirective(
            sleeve_label=sleeve_label, symbol=symbol,
            action="BUY", reason="entry",   # v1 uses default "entry" reason
            amount=amount,
            detail={"trigger": "earnings_beat"},
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
    """frequency='on_earnings': fire only on the earnings event date itself.

    earnings_data shape: {symbol: {date: {eps_actual, eps_estimated, beat}}}.
    """
    rules = (sleeve_config.get("rebalancing") or {}).get("rules", {}) or {}
    trim_pct = float(rules.get("trim_pct", 50)) / 100.0
    on_beat = rules.get("on_earnings_beat")
    on_miss = rules.get("on_earnings_miss")
    add_cfg = rules.get("add_on_earnings_beat") or {}
    add_max_mult = float(add_cfg.get("max_add_multiplier", 1.0))

    out: list[RebalanceDirective] = []
    if not earnings_data:
        return out
    for symbol, pos in sleeve_positions.items():
        sym_earnings = earnings_data.get(symbol, {})
        ev = sym_earnings.get(date)
        if not ev:
            continue
        beat = bool(ev.get("beat"))
        if on_miss == "trim" and not beat:
            out.append(RebalanceDirective(
                sleeve_label=sleeve_label, symbol=symbol,
                action="SELL", reason="rebalance_trim",
                shares=pos.shares * trim_pct,
                detail={"trigger": "earnings_miss"},
            ))
        elif on_beat == "add" and beat:
            target_shares = pos.shares * add_max_mult
            add_shares = max(0.0, target_shares - pos.shares)
            if add_shares > 0:
                out.append(RebalanceDirective(
                    sleeve_label=sleeve_label, symbol=symbol,
                    action="BUY", reason="entry",
                    shares=add_shares,
                    detail={"trigger": "earnings_beat"},
                ))
    return out
