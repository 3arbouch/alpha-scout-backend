---
name: data-query
description: >
  Query AlphaScout market data — stock prices, fundamentals, earnings, insider trades,
  analyst grades, macro indicators, and derived series. 10 tables in SQLite with data
  from 2015 to present for ~530 tickers (S&P 500 + sector ETFs).
  Use when you need to explore market data, test hypotheses, or analyze stocks/sectors.
---

# Data Query

## How to Query

Use the `query_market_data` tool with a SQL SELECT query:

```
query_market_data(sql="SELECT date, close FROM prices WHERE symbol='AAPL' ORDER BY date DESC LIMIT 5")
```

The tool returns `{"columns": [...], "rows": [...], "row_count": N}`.

- Only SELECT queries are allowed
- Results are automatically filtered to the allowed date range
- Maximum 500 rows per query — use LIMIT and WHERE to scope your queries
- Run one query per tool call (no multi-statement queries)

---

## Table: prices (~1.4M rows)

Daily OHLCV for ~530 tickers. From 2015-01-02 to present.

```
symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INT, change_pct REAL, vwap REAL
```

Primary key: (symbol, date). `change_pct` = daily return in percent.

**Sector ETFs available:** SPY (broad), XLK (Tech), XLF (Financials), XLE (Energy), XLV (Healthcare), XLP (Consumer Def), XLY (Consumer Cyc), XLI (Industrials), XLB (Materials), XLRE (Real Estate), XLC (Comms), XLU (Utilities)

```sql
-- Monthly closes for 2024
SELECT date, close FROM prices WHERE symbol='AAPL' AND date >= '2024-01-01' AND date LIKE '%-01' ORDER BY date;

-- Sector ETF comparison
SELECT symbol, MIN(close) as low, MAX(close) as high,
       ROUND((MAX(close) - MIN(close)) * 100.0 / MIN(close), 1) as range_pct
FROM prices WHERE symbol IN ('XLK','XLE','XLF','XLV') AND date >= '2024-01-01'
GROUP BY symbol;

-- Biggest daily drops
SELECT symbol, date, change_pct FROM prices
WHERE date >= '2024-01-01' ORDER BY change_pct LIMIT 20;

-- Stock return over a period
SELECT symbol,
  (SELECT close FROM prices WHERE symbol=p.symbol ORDER BY date DESC LIMIT 1) /
  (SELECT close FROM prices WHERE symbol=p.symbol AND date >= '2024-01-01' ORDER BY date LIMIT 1) - 1 as return_pct
FROM (SELECT DISTINCT symbol FROM prices WHERE symbol IN ('AAPL','MSFT','NVDA')) p;
```

## Table: income (~24K rows)

Quarterly income statements.

```
symbol TEXT, date TEXT, fiscal_year TEXT, period TEXT, revenue REAL, gross_profit REAL, operating_income REAL, net_income REAL, ebitda REAL, eps REAL, eps_diluted REAL, shares_diluted REAL
```

`period` = Q1/Q2/Q3/Q4. `date` = quarter-end filing date.

```sql
-- Revenue growth YoY
SELECT a.symbol, a.date, a.period, a.revenue,
  ROUND((a.revenue - b.revenue) * 100.0 / b.revenue, 1) as yoy_pct
FROM income a
JOIN income b ON a.symbol=b.symbol AND a.period=b.period
  AND CAST(a.fiscal_year AS INT) = CAST(b.fiscal_year AS INT) + 1
WHERE a.symbol='AAPL' ORDER BY a.date DESC LIMIT 8;

-- Margin trends
SELECT date, period,
  ROUND(gross_profit * 100.0 / revenue, 1) as gross_margin,
  ROUND(net_income * 100.0 / revenue, 1) as net_margin
FROM income WHERE symbol='AAPL' ORDER BY date DESC LIMIT 8;
```

## Table: balance (~24K rows)

Quarterly balance sheets.

```
symbol TEXT, date TEXT, fiscal_year TEXT, period TEXT, cash REAL, inventory REAL, total_current_assets REAL, total_assets REAL, total_current_liabilities REAL, long_term_debt REAL, total_debt REAL, total_liabilities REAL, total_equity REAL, net_debt REAL
```

`net_debt` = total_debt - cash (precomputed).

## Table: cashflow (~24K rows)

Quarterly cash flow statements.

```
symbol TEXT, date TEXT, fiscal_year TEXT, period TEXT, operating_cf REAL, capex REAL, free_cash_flow REAL, dividends_paid REAL, stock_repurchased REAL
```

`free_cash_flow` = operating_cf + capex. capex/dividends/buybacks are negative values.

## Table: earnings (~25K rows)

EPS actuals vs estimates (earnings beats/misses).

```
symbol TEXT, date TEXT, eps_actual REAL, eps_estimated REAL, revenue_actual REAL, revenue_estimated REAL
```

`eps_actual` is NULL for future dates.

```sql
-- Consistent earnings beaters
SELECT symbol, COUNT(*) as quarters,
       SUM(CASE WHEN eps_actual > eps_estimated THEN 1 ELSE 0 END) as beats
FROM earnings WHERE date >= '2023-01-01' AND eps_actual IS NOT NULL
GROUP BY symbol HAVING quarters >= 4 ORDER BY beats DESC LIMIT 20;

-- Revenue surprise
SELECT symbol, date, revenue_actual, revenue_estimated,
  ROUND((revenue_actual - revenue_estimated) * 100.0 / revenue_estimated, 1) as surprise_pct
FROM earnings WHERE symbol='NVDA' AND revenue_actual IS NOT NULL ORDER BY date DESC LIMIT 8;
```

## Table: analyst_grades (~274K rows)

Analyst rating changes.

```
symbol TEXT, date TEXT, grading_company TEXT, previous_grade TEXT, new_grade TEXT, action TEXT
```

`action`: upgrade, downgrade, maintain, init, reiterated.

## Table: insider_trades (~83K rows)

Insider buy/sell transactions.

```
symbol TEXT, transaction_date TEXT, reporting_name TEXT, type_of_owner TEXT, transaction_type TEXT, shares REAL, price REAL, value REAL, securities_owned REAL
```

`transaction_type`: P-Purchase, S-Sale, A-Award. `value` = shares x price.

```sql
-- Insider buying activity (last 90 days)
SELECT symbol, COUNT(*) as buys, SUM(CAST(value AS INT)) as total_value
FROM insider_trades WHERE transaction_type='P-Purchase'
  AND transaction_date > date('now', '-90 days') AND price > 0
GROUP BY symbol ORDER BY total_value DESC LIMIT 20;
```

## Table: universe_profiles (521 rows)

Company metadata.

```
symbol TEXT, name TEXT, sector TEXT, industry TEXT, market_cap REAL, exchange TEXT, beta REAL, is_etf INT
```

**Sectors:** Technology (91), Industrials (79), Financial Services (70), Healthcare (62), Consumer Cyclical (53), Consumer Defensive (37), Utilities (32), Real Estate (31), Communication Services (23), Energy (23), Basic Materials (20)

```sql
-- All energy stocks
SELECT symbol, name, market_cap FROM universe_profiles WHERE sector='Energy' ORDER BY market_cap DESC;

-- Sector breakdown
SELECT sector, COUNT(*) as n FROM universe_profiles WHERE is_etf=0 GROUP BY sector ORDER BY n DESC;
```

## Table: macro_indicators (~102K rows)

Macro economic indicators from FRED. Daily/weekly/monthly frequency.

```
date TEXT, series TEXT, value REAL, source TEXT
```

**Daily series:** vix, brent, wti, natgas, dxy, spx, nasdaq, fed_funds, treasury_2y, treasury_5y, treasury_10y, treasury_30y, spread_10y2y, spread_10y3m, hy_spread, hy_yield, bbb_spread, breakeven_5y, breakeven_10y, tips_real_10y, eurusd, gbpusd, jpyusd, financial_stress, nfci, vix_st_futures, gas_regular, gas_diesel

**Monthly series:** cpi, core_cpi, pce, core_pce, cpi_energy, ppi_commodities, nonfarm_payrolls, unemployment, retail_sales, industrial_prod, housing_starts, building_permits, consumer_sentiment, gdp, copper, aluminum, jolts_openings

```sql
-- Current macro snapshot
SELECT series, date, value FROM macro_indicators
WHERE series IN ('vix','brent','fed_funds','treasury_10y','hy_spread','spx')
  AND date = (SELECT MAX(date) FROM macro_indicators WHERE series='vix');

-- VIX above 25 periods
SELECT date, value FROM macro_indicators WHERE series='vix' AND value > 25 AND date >= '2022-01-01' ORDER BY date;

-- Yield curve inversion
SELECT date, value FROM macro_indicators WHERE series='spread_10y2y' AND value < 0 AND date >= '2020-01-01' ORDER BY date;

-- Oil vs equity (monthly)
SELECT strftime('%Y-%m', m.date) as month, ROUND(AVG(m.value),1) as brent, ROUND(AVG(s.value),0) as spx
FROM macro_indicators m
JOIN macro_indicators s ON strftime('%Y-%m',m.date) = strftime('%Y-%m',s.date) AND s.series='spx'
WHERE m.series='brent' AND m.date >= '2023-01-01' GROUP BY month ORDER BY month;
```

## Table: macro_derived (~28K rows)

Computed macro series (moving averages, z-scores, YoY changes).

```
date TEXT, series TEXT, value REAL
```

**Available series:**

| Key | Description |
|---|---|
| brent_50dma, brent_200dma | Brent moving averages |
| brent_vs_50dma_pct | Brent % above/below 50DMA (>10% = breakout) |
| spx_50dma, spx_200dma | S&P 500 moving averages |
| spx_vs_200dma_pct | SPX % above/below 200DMA (negative = bear trend) |
| natgas_50dma | Natural gas 50DMA |
| vix_term_spread | VIX spot minus futures (positive = backwardation = fear) |
| hy_spread_zscore | HY spread z-score vs 252-day window (>1 = stress) |
| cpi_yoy, core_cpi_yoy, core_pce_yoy | Inflation YoY rates |
| cpi_mom | CPI month-over-month change |
| real_fed_funds | Fed funds minus Core CPI YoY (monetary tightness) |

```sql
-- Market regime: SPX above or below 200DMA
SELECT date, ROUND(value,1) as pct, CASE WHEN value > 0 THEN 'BULL' ELSE 'BEAR' END as regime
FROM macro_derived WHERE series='spx_vs_200dma_pct' ORDER BY date DESC LIMIT 20;

-- Credit stress signal
SELECT m.date, ROUND(m.value,1) as vix, ROUND(d.value,2) as hy_zscore
FROM macro_indicators m
JOIN macro_derived d ON m.date=d.date AND d.series='hy_spread_zscore'
WHERE m.series='vix' AND m.value > 25 AND d.value > 1.0 AND m.date >= '2020-01-01'
ORDER BY m.date;
```

---

## Tips

- Always use `LIMIT` for large result sets
- Use `strftime('%Y-%m', date)` for monthly aggregation
- NULL in earnings = not yet reported — filter with `IS NOT NULL`
- capex, dividends_paid, stock_repurchased are negative values
- For sector analysis, JOIN prices with universe_profiles on symbol
- Date format is always YYYY-MM-DD, use SQLite date functions: `date('now')`, `date('now', '-30 days')`
