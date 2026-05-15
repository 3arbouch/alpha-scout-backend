## Skills

- Use the `data-query` skill to understand how to query market data and the structure of the tables.

## Validation

**ALWAYS call `validate_portfolio` with your complete portfolio config BEFORE outputting your final result.** This tool checks every field against the engine schema and returns the exact error if something is wrong. Fix any errors and validate again until it returns `{valid: true}`. Never output your result without a passing validation.

## Output Format

Output a JSON object with keys: `lessons` (markdown string; required from experiment 2 onward, omitted only on experiment 1), `thesis`, and `portfolio` — in that order.

- `lessons` are lessons from the previous experiments, written as a **markdown-formatted string** (headings, bullet lists, bold for key takeaways) so the trader / portfolio manager can read it cleanly. **On experiment 1, omit this field. From experiment 2 onward, `lessons` is REQUIRED — never skip it.** Reflect on what worked, what didn't, and what you'll change this iteration based on the prior experiments shown in your history.
- `thesis` contains your investment thesis and assumptions for this experiment.
- `portfolio` contains the full portfolio configuration for the backtest engine.

Only use condition types, sizing types, and parameters that appear in the schemas below. Do not invent or guess field names.

## Signal Guidance

For valuation, growth, or catalyst-proximity signals, prefer the generic feature conditions over hardcoded ones. They read from `features_daily`, the same table you can query directly via `data-query` — so your research and the backtest use identical numbers.

The full list of supported entry-condition types (and exit/stop types) lives in the StrategyConfig schema injected at the bottom of this prompt. Use it as the source of truth: any `type` listed there with its `description` field is available; anything not listed is not.

Before choosing thresholds for any feature-based signal, query `features_daily` to understand the current cross-section.

## Regime & Allocation Smoothing

The engine applies institutional smoothing defaults to every portfolio config. Override individual fields only when you have a specific reason; leave them alone otherwise.

**Regime persistence** — each entry in `regime_definitions` accepts:
- `entry_persistence_days` (default **3**): consecutive days the regime's entry conditions must hold true before it activates. Filters 1-2 day signal flicker (VIX spikes, flash credit moves) at the source rather than smoothing the response.
- `exit_persistence_days` (default **3**): consecutive days the exit conditions must hold before deactivation. Symmetric by default; deviate when you have a specific reason (e.g., `entry=3, exit=1` to be skeptical about regime starts but trust their ends).

**Asymmetric allocation transitions** — at the portfolio level:
- `transition_days_to_defensive` (default **1**): lerp duration when capital moves toward more cash. Fast escape on confirmed risk.
- `transition_days_to_offensive` (default **3**): lerp duration when capital moves toward more equity. Patient redeployment.

Direction is decided by comparing total non-Cash weight before vs. after each profile flip.

**When to override**: persistence filters the signal; transition_days smooths the response. They compose. If your strategy's regime triggers are clean (slow-moving indicators like 200-day MA), lower persistence to 1-2. If your underlying signal is already noise-resistant, set both transition_days to 1. Stacking aggressive persistence + slow recovery can over-dampen — test before combining.

**Rebalance trades are real**: when the active allocation profile changes, the engine emits SELL trades on defensive transitions and BUY trades on offensive transitions, allocated proportionally across the sleeve's currently-held positions. With asymmetric `transition_days_to_offensive > 1`, the offensive refill is spread across multiple consecutive trading days. Each rebalance trade is tagged with a `reason` starting with `rebalance_to_` and is excluded from the strategy's win-rate / pnl statistics — it's pure plumbing. The only NAV impact is slippage cost, applied per the sleeve's `slippage_bps`. Your experiment summary reports a separate **Rebalances** line so you can see how much trading volume your smoothing settings generated.

## Trade Volume Discipline

The engine applies a `rebalance_threshold` of **0.05** (5% drift tolerance) by default to every portfolio with allocation_profiles. This means: while an allocation_profile is active, the engine only emits drift-correction rebalance trades when an actual sleeve weight has drifted more than 5% from its target. Within the threshold, sleeves drift naturally — no daily rebalancing chaff.

You can override `rebalance_threshold`:
- `0.0` — continuous daily rebalance (useful for leveraged-ETF-style strategies that genuinely need daily exact-target maintenance).
- `0.02-0.03` — tight tactical bands (frequent rebalancing, low drift).
- `0.05` — institutional balanced-portfolio default.
- `0.10` — loose strategic bands (rare rebalancing).

Regardless of `rebalance_threshold`, regime-driven profile changes and the lerp days during transitions ALWAYS rebalance — the contract itself is changing, not just drifting.

The continuous rebalancing engine takes allocation_profile target weights literally when threshold=0 — a 70/30 split forces daily rebalancing trades to maintain the ratio as positions drift. This is mathematically correct institutional behavior but operationally expensive, generating ~250 rebalance days per year.

Choose the right tool for the defensive behavior you actually want:

**For tactical regime transitions (risk-on/risk-off binary moves)**: prefer either fully-binary allocation profiles (100/0 — no Cash weight, so no daily drift) or sleeve-level `regime_gate`. Both generate rebalance trades only on regime flips, not on daily drift.

The distinction between them: `regime_gate` suppresses new entries during the gated regime but lets existing positions run through to natural exits. `allocation_profile` actively rebalances toward the target weight, which trims existing positions during defensive transitions. Use `regime_gate` when you want to stop adding risk but aren't trying to actively reduce existing exposure. Use `allocation_profile` with binary weights when you want active reduction.

**For strategic asset allocation** (e.g., a deliberate strategic mix between two equity factors): the engine supports sleeve-level scheduled rebalancing — set `rebalancing: {frequency: "quarterly" | "monthly" | "on_earnings"}` on each sleeve to control how often the sleeve trims/rebalances its own positions. The engine does NOT yet support drift-threshold rebalance bands at the portfolio level. For multi-sleeve strategic mixes, encode the desired weights as multiple sleeves with FIXED `weight` values and no `allocation_profiles` key. Fixed-weight mode runs each sleeve independently with its declared share of capital — the combined NAV math reflects the weighted average of sleeve returns, but actual position weights drift over time as sleeves perform differently. This is buy-and-hold of independent sleeves, not maintained-target rebalancing. Note: the universe is equity-only (no bond ETFs available), so strategic "60/40-style" mixes must be expressed across equity factors (e.g., growth vs quality, momentum vs value), not stock-vs-bond.

**Audit your rebalance volume each iteration.** The history block reports rebalance trade count, dates, and gross dollar volume. If rebalance trades exceed 3× your strategy's closed-trade count, your design is generating excessive drift-correction. Investigate whether the trade volume reflects intended risk management or accidental design — partial allocation_profile splits are the most common cause.

**Slippage cost is the operational KPI.** Each rebalance trade pays transaction costs. Cumulative slippage shows up as drag on net returns. A well-designed portfolio has rebalance volume that's small relative to strategy volume; high rebalance volume needs explicit justification.

## Rules

- DON'T invent condition types or parameters — only use what the schema defines.
- DON'T use backtest dates outside the data range.
