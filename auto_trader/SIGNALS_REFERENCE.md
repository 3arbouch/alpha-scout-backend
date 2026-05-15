# AlphaScout Signal Reference

Single source of truth for every entry condition, exit condition, and factor the strategy engine accepts. Use these names and shapes verbatim — anything not listed here is not supported.

---

## 1. Entry conditions — 21 types

Used in `entry.conditions: [...]`. Combine with `entry.logic: "all" | "any"`.

### 1.1 Generic factor conditions (registry-driven, prefer for new work)

#### `feature_threshold`
Point-in-time as-of value passes operator.
```json
{
  "type": "feature_threshold",
  "feature": "<factor name from §3>",
  "operator": ">=",            // ">", ">=", "<", "<=", "==", "!="
  "value": 15
}
```
Fires on (symbol, day) where the factor's as-of value satisfies `value OP threshold`.

#### `feature_percentile`
Cross-sectional bottom-quintile rank.
```json
{
  "type": "feature_percentile",
  "feature": "<factor name from §3>",
  "max_percentile": 30,        // 0..100, default 30
  "scope": "universe",         // "universe" | "sector"
  "min_value": null,           // optional pre-filter
  "max_value": null            // optional pre-filter
}
```
Ranks all active symbols ascending by the factor each trading day; fires when rank ≤ `max_percentile` of cross-section. **Bottom-quintile only** — to express top-quintile, use `feature_threshold` with an absolute cut OR use the strategy-level `ranking.by` block instead.

`min_value` / `max_value` exclude outliers before ranking (e.g. `min_value: 0` on `pe` to drop loss-makers from a cheap-PE screen).

### 1.2 Earnings / catalyst

#### `earnings_momentum`
N consecutive earnings beats with positive surprise.
```json
{ "type": "earnings_momentum", "min_quarters": 2, "min_avg_surprise_pct": 5.0 }
```

#### `days_to_earnings`
Within `[min_days, max_days]` calendar days of next reported earnings event.
```json
{ "type": "days_to_earnings", "min_days": 0, "max_days": 7 }
```
Use to enter pre-earnings momentum (e.g. 0..5) or to suppress entries in a blackout window.

#### `analyst_upgrades`
Net (upgrades − downgrades) in trailing window ≥ threshold.
```json
{ "type": "analyst_upgrades", "window_days": 90, "min_net_upgrades": 2 }
```

### 1.3 Price / momentum / volume (hard-coded)

| `type` | Parameters | Semantics |
|---|---|---|
| `current_drop` | `pct: float` (negative) | Price ≥ X% below recent peak. |
| `period_drop` | `pct`, `lookback_days` | Cumulative drop over rolling window. |
| `daily_drop` | `pct` | Single-day decline ≥ X%. |
| `selloff` | `pct`, `consecutive_days` | N consecutive down days totaling ≥ X%. |
| `rsi` | `value`, `comparison: "below"\|"above"` | RSI(14) crosses threshold. |
| `momentum_rank` | `top_pct`, `lookback_days` | Cross-sectional momentum rank in top X%. |
| `ma_crossover` | `short_window`, `long_window`, `direction` | Short MA crosses long MA. |
| `relative_performance` | `vs: "SPY"`, `lookback_days`, `min_outperformance_pct` | Stock vs benchmark over window. |
| `volume_conviction` | `min_volume_multiplier`, `lookback_days` | Volume ≥ k× rolling average. |
| `volume_capitulation` | `pct`, `volume_multiplier` | Volume spike + drop combo. |
| `always` | — | Always true. Stub for unconditional sleeves. |

### 1.4 Legacy fundamentals (kept for back-compat; prefer `feature_threshold`)

| `type` | Equivalent in §3 |
|---|---|
| `pe_percentile` | `feature_percentile(pe)` |
| `revenue_growth_yoy` | `feature_threshold(rev_yoy, ...)` |
| `revenue_accelerating` | `feature_threshold(rev_yoy_accel, ">", 0)` |
| `margin_expanding` | `feature_threshold(*_margin_yoy_delta, ">", ...)` |
| `margin_turnaround` | (multi-quarter sign flip — no direct factor equivalent) |

---

## 2. Exit logic — 4 orthogonal blocks

All blocks are additive within a strategy; any one firing closes the position.

### 2.1 `stop_loss` — 3 types

```json
// drawdown_from_entry: fixed % loss from entry
{ "type": "drawdown_from_entry", "value": -15, "cooldown_days": 90 }

// atr_multiple: stop = entry − k·ATR(window), frozen at entry
{ "type": "atr_multiple", "k": 2.5, "window_days": 20, "cooldown_days": 90 }

// realized_vol_multiple: stop = entry · (1 − k·σ_daily), frozen at entry
{ "type": "realized_vol_multiple", "k": 3.0, "window_days": 20,
  "sigma_source": "historical", "cooldown_days": 90 }
```
`cooldown_days` blocks re-entry of the same ticker after a stop fires. `sigma_source`: `"historical"` or `"ewma"`.

### 2.2 `take_profit` — 4 types

```json
// gain_from_entry: fixed % gain from entry
{ "type": "gain_from_entry", "value": 30 }

// above_peak: trailing — exit when current vs peak ≤ −X%
{ "type": "above_peak", "value": 15 }

// atr_multiple: tp = entry + k·ATR(window)
{ "type": "atr_multiple", "k": 4.0, "window_days": 20 }

// realized_vol_multiple: tp = entry · (1 + k·σ_daily)
{ "type": "realized_vol_multiple", "k": 4.0, "window_days": 20,
  "sigma_source": "historical" }
```

### 2.3 `time_stop` — single config

```json
{ "max_days": 90 }
```
Force-exit after N calendar days regardless of P&L.

### 2.4 `exit.conditions[]` — fundamental deterioration (2 types)

```json
// revenue_deceleration
{ "type": "revenue_deceleration",
  "min_quarters": 2,
  "require_margin_compression": true,
  "metric": "net_margin" }     // "net_margin" | "op_margin"

// margin_collapse
{ "type": "margin_collapse",
  "metric": "net_margin",       // "net_margin" | "op_margin"
  "threshold_bps": -500,        // negative; -500 = 5pp YoY contraction
  "min_quarters": 2 }
```

---

## 3. Factor catalog (35 names) — `feature` field of `feature_threshold` / `feature_percentile`

Categories track the `category` field in the registry.

### Valuation (5) — `category: value`
| name | unit | definition |
|---|---|---|
| `pe` | ratio | `market_cap / TTM net_income`. NULL when TTM net_income ≤ 0. |
| `ps` | ratio | `market_cap / TTM revenue`. |
| `p_b` | ratio | `market_cap / total_equity`. |
| `ev_ebitda` | ratio | `(market_cap + net_debt) / TTM ebitda`. |
| `ev_sales` | ratio | `(market_cap + net_debt) / TTM revenue`. |

### Yield (2) — `category: yield`
| name | unit | definition |
|---|---|---|
| `fcf_yield` | percent | `TTM free_cash_flow / market_cap × 100`. |
| `div_yield` | percent | `TTM abs(dividends_paid) / market_cap × 100`. |

### Growth (2) — `category: growth`
| name | unit | definition |
|---|---|---|
| `eps_yoy` | percent | Latest-Q `eps_diluted` vs same-Q prior year. |
| `rev_yoy` | percent | Latest-Q `revenue` vs same-Q prior year. |

### Growth acceleration (2) — `category: growth`
| name | unit | definition |
|---|---|---|
| `rev_yoy_accel` | pp | `rev_yoy(latest Q) − rev_yoy(prior Q)`. |
| `eps_yoy_accel` | pp | `eps_yoy(latest Q) − eps_yoy(prior Q)`. |

### Margin trajectory (2) — `category: growth`
| name | unit | definition |
|---|---|---|
| `op_margin_yoy_delta` | pp | `op_margin(latest Q) − op_margin(same-Q prior year)`. |
| `net_margin_yoy_delta` | pp | `net_margin(latest Q) − net_margin(same-Q prior year)`. |

### Quality — current margins (3) — `category: quality`
| name | unit | definition |
|---|---|---|
| `gross_margin` | percent | `TTM gross_profit / TTM revenue × 100`. |
| `op_margin` | percent | `TTM operating_income / TTM revenue × 100`. |
| `net_margin` | percent | `TTM net_income / TTM revenue × 100`. |

### Quality — balance sheet (3) — `category: quality`
| name | unit | definition |
|---|---|---|
| `roe` | percent | `TTM net_income / total_equity × 100`. |
| `roic` | percent | `TTM operating_income / (total_equity + total_debt) × 100`. **Proxy** — no tax adjustment. |
| `debt_to_equity` | ratio | `total_debt / total_equity`. |

### Returns / momentum — precomputed (5) — `category: momentum`
| name | unit | definition |
|---|---|---|
| `ret_1m` | percent | 21-trading-day total return. |
| `ret_3m` | percent | 63-trading-day total return. |
| `ret_6m` | percent | 126-trading-day total return. |
| `ret_12m` | percent | 252-trading-day total return. |
| `ret_12_1m` | percent | 12-month return excluding the most recent month — Asness momentum. |

### Returns / momentum — on-the-fly (4) — `category: momentum`
*Computed at backtest time, not stored in `features_daily` — not queryable via the data-query skill.*
| name | unit | definition |
|---|---|---|
| `rsi_14` | 0..100 | Wilder RSI on 14-day window. |
| `drawdown_60d` | percent | Drawdown vs 60-day rolling peak (negative). |
| `drawdown_252d` | percent | Drawdown vs 252-day rolling peak. |
| `drawdown_alltime` | percent | Drawdown vs all-time peak. |

### Volume — on-the-fly (2) — `category: volume`
*Not stored in `features_daily`.*
| name | unit | definition |
|---|---|---|
| `vol_z_20` | z-score | `(volume − μ_20) / σ_20`. |
| `dollar_vol_20` | dollars | 20-day average of `close × volume`. |

### Analyst sentiment (2) — `category: sentiment`
| name | unit | definition |
|---|---|---|
| `analyst_net_upgrades_30d` | count | `(upgrades − downgrades)` in trailing 30 calendar days. |
| `analyst_net_upgrades_90d` | count | Same in trailing 90 calendar days. |

### Calendar / event (3) — `category: calendar`
| name | unit | definition |
|---|---|---|
| `days_since_last_earnings` | days | Calendar days since most recent earnings event. |
| `days_to_next_earnings` | days | Calendar days to next scheduled earnings event. |
| `pre_earnings_window_5d` | 0/1 | `1` iff `days_to_next_earnings ≤ 5`. |

---

## 4. Point-in-time correctness

Every fundamentals factor reflects only data the market actually had on the trading day. Each quarter is bound to the earliest matching `earnings`-table date within 60 days of period-end, falling back to period-end + 45 days. At trading day T you never see a quarter that ended before T but wasn't reported until after T.

The 29 precomputed factors are stored in `market.db.features_daily`, one row per `(symbol, date)`. The 6 on-the-fly factors are computed during backtests from price history; they are not in `features_daily` and cannot be screened via the data-query skill — only used in `feature_threshold` / `feature_percentile` at backtest evaluation time.
