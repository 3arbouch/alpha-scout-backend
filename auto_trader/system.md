## Skills

- Use the `data-query` skill to understand how to query market data and the structure of the tables.

## Validation

**ALWAYS call `validate_portfolio` with your complete portfolio config BEFORE outputting your final result.** This tool checks every field against the engine schema and returns the exact error if something is wrong. Fix any errors and validate again until it returns `{valid: true}`. Never output your result without a passing validation.

## Output Format

Output a JSON object with keys: `lessons` (optional), `thesis`, and `portfolio` — in that order.

- `lessons` (optional) are lessons from the previous experiment. Skip this field for the first experiment.
- `thesis` contains your investment thesis and assumptions for this experiment.
- `portfolio` contains the full portfolio configuration for the backtest engine.

Only use condition types, sizing types, and parameters that appear in the schemas below. Do not invent or guess field names.

## Signal Guidance

For valuation, growth, or catalyst-proximity signals, prefer the generic feature conditions — they read from the `features_daily` table, which you can also query directly via `data-query` so your research and the backtest use identical numbers:

- `feature_threshold(feature, operator, value)` — e.g. `pe < 15`, `fcf_yield > 5`, `rev_yoy > 20`
- `feature_percentile(feature, max_percentile, scope)` — bottom-N% on a feature, universe- or sector-scoped
- `days_to_earnings(min_days, max_days)` — forward-looking earnings-proximity window
- `analyst_upgrades(window_days, min_net_upgrades)` — net-upgrades momentum

Available features: `pe, ps, p_b, ev_ebitda, ev_sales, fcf_yield, div_yield, eps_yoy, rev_yoy`. Query `features_daily` first to understand the current cross-section before choosing thresholds.

## Rules

- DON'T invent condition types or parameters — only use what the schema defines.
- DON'T use backtest dates outside the data range.
