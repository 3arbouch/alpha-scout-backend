#!/usr/bin/env python3
"""
AlphaScout Backtest Charts
==========================
Reads backtest results JSON and generates Bloomberg-style charts.

Usage:
    python3 backtest_charts.py <results_json>
    python3 backtest_charts.py <results_json> --charts equity,drawdown,annual
    python3 backtest_charts.py <results_json> --output-dir /path/to/output

Generates:
    - equity_curve.png    Strategy NAV vs S&P 500 benchmark
    - drawdown.png        Underwater plot (peak-to-trough drawdowns)
    - annual_returns.png  Year-by-year strategy vs benchmark bars
"""

import json
import argparse
import os
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates

# ---------------------------------------------------------------------------
# Bloomberg-inspired dark theme
# ---------------------------------------------------------------------------
COLORS = {
    "bg": "#1a1a2e",
    "panel": "#16213e",
    "grid": "#2a2a4a",
    "text": "#e0e0e0",
    "text_dim": "#808080",
    "strategy": "#00d4ff",
    "benchmark": "#ff6b35",
    "positive": "#00c853",
    "negative": "#ff1744",
    "drawdown": "#ff1744",
    "watermark": "#2a2a4a",
}

def apply_theme(fig, ax):
    """Apply Bloomberg-dark theme to figure and axes."""
    fig.patch.set_facecolor(COLORS["bg"])
    ax.set_facecolor(COLORS["panel"])
    ax.tick_params(colors=COLORS["text"], labelsize=9)
    ax.xaxis.label.set_color(COLORS["text"])
    ax.yaxis.label.set_color(COLORS["text"])
    ax.title.set_color(COLORS["text"])
    ax.grid(True, color=COLORS["grid"], alpha=0.5, linewidth=0.5)
    for spine in ax.spines.values():
        spine.set_color(COLORS["grid"])


# ---------------------------------------------------------------------------
# Chart: Equity Curve
# ---------------------------------------------------------------------------
def chart_equity_curve(results: dict, output_path: str):
    """Strategy NAV vs benchmark over time."""
    nav = results["nav_history"]
    bench_nav = results.get("benchmark", {}).get("nav_history", [])
    config = results.get("config", {})
    name = results.get("strategy", "Strategy")

    dates = [datetime.strptime(p["date"], "%Y-%m-%d") for p in nav]
    navs = [p["nav"] for p in nav]

    fig, ax = plt.subplots(figsize=(14, 6))
    apply_theme(fig, ax)

    # Strategy line
    ax.plot(dates, navs, color=COLORS["strategy"], linewidth=1.5,
            label=name, zorder=3)

    # Benchmark line
    if bench_nav:
        bench_dates = [datetime.strptime(p["date"], "%Y-%m-%d") for p in bench_nav]
        bench_vals = [p["nav"] for p in bench_nav]
        ax.plot(bench_dates, bench_vals, color=COLORS["benchmark"],
                linewidth=1.2, label="S&P 500", linestyle="--", zorder=2)

    # Fill between strategy and benchmark
    if bench_nav:
        # Align dates
        bench_dict = {p["date"]: p["nav"] for p in bench_nav}
        for i, point in enumerate(nav):
            b = bench_dict.get(point["date"])
            if b and i > 0:
                prev_b = bench_dict.get(nav[i-1]["date"])
                if prev_b:
                    color = COLORS["positive"] if navs[i] >= b else COLORS["negative"]
                    ax.fill_between(
                        [dates[i-1], dates[i]],
                        [navs[i-1], navs[i]],
                        [prev_b, b],
                        alpha=0.08, color=color, zorder=1
                    )

    # Formatting
    ax.set_title(f"{name} — Equity Curve", fontsize=14, fontweight="bold",
                 pad=15)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, p: f"${x:,.0f}"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_major_locator(mdates.YearLocator())

    legend = ax.legend(loc="upper left", facecolor=COLORS["panel"],
                       edgecolor=COLORS["grid"], fontsize=10)
    for text in legend.get_texts():
        text.set_color(COLORS["text"])

    # Stats annotation
    metrics = results.get("metrics", {})
    stats_text = (
        f"Total: {metrics.get('total_return_pct', 0):+.1f}%  |  "
        f"Ann: {metrics.get('annualized_return_pct', 0):+.1f}%  |  "
        f"Alpha: {metrics.get('alpha_ann_pct', 0):+.1f}%  |  "
        f"Max DD: {metrics.get('max_drawdown_pct', 0):.1f}%"
    )
    ax.text(0.5, -0.12, stats_text, transform=ax.transAxes,
            ha="center", fontsize=10, color=COLORS["text_dim"])

    fig.tight_layout(rect=[0, 0.05, 1, 1])
    fig.savefig(output_path, dpi=150, facecolor=fig.get_facecolor(),
                bbox_inches="tight")
    plt.close(fig)
    print(f"  ✓ Equity curve → {output_path}")


# ---------------------------------------------------------------------------
# Chart: Drawdown
# ---------------------------------------------------------------------------
def chart_drawdown(results: dict, output_path: str):
    """Underwater plot showing drawdown from peak NAV."""
    nav = results["nav_history"]
    name = results.get("strategy", "Strategy")

    dates = [datetime.strptime(p["date"], "%Y-%m-%d") for p in nav]
    navs = [p["nav"] for p in nav]

    # Calculate drawdown series
    peak = 0
    drawdowns = []
    for n in navs:
        if n > peak:
            peak = n
        dd = ((n - peak) / peak) * 100 if peak > 0 else 0
        drawdowns.append(dd)

    fig, ax = plt.subplots(figsize=(14, 4))
    apply_theme(fig, ax)

    ax.fill_between(dates, drawdowns, 0, color=COLORS["drawdown"],
                    alpha=0.4, zorder=2)
    ax.plot(dates, drawdowns, color=COLORS["drawdown"], linewidth=0.8,
            zorder=3)

    # Zero line
    ax.axhline(y=0, color=COLORS["text_dim"], linewidth=0.5, zorder=1)

    # Mark max drawdown
    min_dd = min(drawdowns)
    min_idx = drawdowns.index(min_dd)
    ax.annotate(
        f"{min_dd:.1f}%",
        xy=(dates[min_idx], min_dd),
        xytext=(dates[min_idx], min_dd - 3),
        fontsize=9, color=COLORS["negative"], fontweight="bold",
        ha="center",
        arrowprops=dict(arrowstyle="-", color=COLORS["negative"], lw=0.8),
    )

    ax.set_title(f"{name} — Drawdown from Peak", fontsize=14,
                 fontweight="bold", pad=15)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, p: f"{x:.0f}%"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_major_locator(mdates.YearLocator())

    fig.tight_layout()
    fig.savefig(output_path, dpi=150, facecolor=fig.get_facecolor(),
                bbox_inches="tight")
    plt.close(fig)
    print(f"  ✓ Drawdown → {output_path}")


# ---------------------------------------------------------------------------
# Chart: Annual Returns
# ---------------------------------------------------------------------------
def chart_annual_returns(results: dict, output_path: str):
    """Year-by-year bar chart, strategy vs benchmark."""
    nav = results["nav_history"]
    bench_nav = results.get("benchmark", {}).get("nav_history", [])
    name = results.get("strategy", "Strategy")

    # Group NAV by year — first and last entry per year
    def yearly_returns(series):
        by_year = defaultdict(list)
        for p in series:
            year = p["date"][:4]
            by_year[year].append(p["nav"])
        returns = {}
        for year, vals in sorted(by_year.items()):
            if len(vals) >= 2:
                returns[year] = ((vals[-1] - vals[0]) / vals[0]) * 100
        return returns

    strat_returns = yearly_returns(nav)
    bench_returns = yearly_returns(bench_nav) if bench_nav else {}

    years = sorted(strat_returns.keys())
    if not years:
        return

    x = range(len(years))
    strat_vals = [strat_returns.get(y, 0) for y in years]
    bench_vals = [bench_returns.get(y, 0) for y in years]

    fig, ax = plt.subplots(figsize=(14, 5))
    apply_theme(fig, ax)

    bar_width = 0.35
    bars1 = ax.bar([i - bar_width/2 for i in x], strat_vals, bar_width,
                   label=name, zorder=3,
                   color=[COLORS["positive"] if v >= 0 else COLORS["negative"]
                          for v in strat_vals],
                   edgecolor="none", alpha=0.85)

    if bench_returns:
        ax.bar([i + bar_width/2 for i in x], bench_vals, bar_width,
               label="S&P 500", color=COLORS["benchmark"],
               edgecolor="none", alpha=0.5, zorder=2)

    # Value labels on strategy bars
    for bar, val in zip(bars1, strat_vals):
        y_pos = bar.get_height()
        offset = 1 if val >= 0 else -2.5
        ax.text(bar.get_x() + bar.get_width()/2, y_pos + offset,
                f"{val:+.1f}%", ha="center", fontsize=7.5,
                color=COLORS["text"], fontweight="bold")

    ax.axhline(y=0, color=COLORS["text_dim"], linewidth=0.5, zorder=1)
    ax.set_xticks(list(x))
    ax.set_xticklabels(years, fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, p: f"{x:+.0f}%"))

    ax.set_title(f"{name} — Annual Returns", fontsize=14,
                 fontweight="bold", pad=15)

    legend = ax.legend(loc="upper left", facecolor=COLORS["panel"],
                       edgecolor=COLORS["grid"], fontsize=10)
    for text in legend.get_texts():
        text.set_color(COLORS["text"])

    fig.tight_layout()
    fig.savefig(output_path, dpi=150, facecolor=fig.get_facecolor(),
                bbox_inches="tight")
    plt.close(fig)
    print(f"  ✓ Annual returns → {output_path}")


# ---------------------------------------------------------------------------
# Chart: Win/Loss Distribution
# ---------------------------------------------------------------------------
def chart_trade_distribution(results: dict, output_path: str):
    """Histogram of trade PnL percentages."""
    closed = results.get("closed_trades", [])
    real_trades = [t for t in closed if t.get("reason") != "backtest_end"]
    name = results.get("strategy", "Strategy")

    if not real_trades:
        return

    pnls = [t["pnl_pct"] for t in real_trades]

    fig, ax = plt.subplots(figsize=(14, 4))
    apply_theme(fig, ax)

    # Separate wins and losses for coloring
    bins = 40
    n, bin_edges, patches = ax.hist(pnls, bins=bins, edgecolor=COLORS["panel"],
                                     linewidth=0.5, zorder=3)
    for patch, edge in zip(patches, bin_edges):
        if edge >= 0:
            patch.set_facecolor(COLORS["positive"])
            patch.set_alpha(0.7)
        else:
            patch.set_facecolor(COLORS["negative"])
            patch.set_alpha(0.7)

    # Vertical line at zero
    ax.axvline(x=0, color=COLORS["text_dim"], linewidth=1, linestyle="--",
               zorder=2)

    # Stats
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    win_rate = len(wins) / len(pnls) * 100 if pnls else 0
    stats_text = (
        f"{len(real_trades)} trades  |  "
        f"Win rate: {win_rate:.1f}%  |  "
        f"Avg win: +{sum(wins)/len(wins):.1f}%  |  "
        f"Avg loss: {sum(losses)/len(losses):.1f}%"
    ) if wins and losses else f"{len(real_trades)} trades"

    ax.text(0.5, -0.18, stats_text, transform=ax.transAxes,
            ha="center", fontsize=10, color=COLORS["text_dim"])

    ax.set_title(f"{name} — Trade PnL Distribution", fontsize=14,
                 fontweight="bold", pad=15)
    ax.set_xlabel("Trade PnL %", fontsize=10)
    ax.set_ylabel("Count", fontsize=10)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, p: f"{x:+.0f}%"))

    fig.tight_layout(rect=[0, 0.05, 1, 1])
    fig.savefig(output_path, dpi=150, facecolor=fig.get_facecolor(),
                bbox_inches="tight")
    plt.close(fig)
    print(f"  ✓ Trade distribution → {output_path}")


# ---------------------------------------------------------------------------
# Chart: Monthly Returns Heatmap
# ---------------------------------------------------------------------------
def chart_monthly_heatmap(results: dict, output_path: str):
    """Monthly returns heatmap — rows = years, columns = months."""
    nav = results["nav_history"]
    name = results.get("strategy", "Strategy")

    # Build monthly returns
    by_month = defaultdict(list)
    for p in nav:
        key = p["date"][:7]  # YYYY-MM
        by_month[key].append(p["nav"])

    monthly_returns = {}
    prev_end = None
    for month in sorted(by_month.keys()):
        vals = by_month[month]
        start = prev_end if prev_end else vals[0]
        end = vals[-1]
        monthly_returns[month] = ((end - start) / start) * 100 if start > 0 else 0
        prev_end = end

    if not monthly_returns:
        return

    # Build grid
    years = sorted(set(k[:4] for k in monthly_returns.keys()))
    months = list(range(1, 13))
    month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    grid = []
    for year in years:
        row = []
        for month in months:
            key = f"{year}-{month:02d}"
            row.append(monthly_returns.get(key, None))
        grid.append(row)

    fig, ax = plt.subplots(figsize=(14, max(3, len(years) * 0.45 + 1)))
    apply_theme(fig, ax)

    # Draw cells
    for i, year in enumerate(years):
        for j, val in enumerate(grid[i]):
            if val is None:
                color = COLORS["panel"]
                text_color = COLORS["text_dim"]
                label = "—"
            else:
                # Color intensity based on magnitude
                intensity = min(abs(val) / 10, 1.0)  # cap at ±10%
                if val >= 0:
                    r, g, b = 0, 0.78, 0.33  # green base
                else:
                    r, g, b = 1.0, 0.09, 0.27  # red base
                # Blend with panel background
                bg_r, bg_g, bg_b = 0.086, 0.129, 0.243  # panel color
                r = bg_r + (r - bg_r) * intensity * 0.7
                g = bg_g + (g - bg_g) * intensity * 0.7
                b = bg_b + (b - bg_b) * intensity * 0.7
                color = (r, g, b)
                text_color = COLORS["text"]
                label = f"{val:+.1f}"

            rect = plt.Rectangle((j, len(years) - 1 - i), 1, 1,
                                  facecolor=color, edgecolor=COLORS["grid"],
                                  linewidth=0.5)
            ax.add_patch(rect)
            ax.text(j + 0.5, len(years) - 1 - i + 0.5, label,
                    ha="center", va="center", fontsize=8,
                    color=text_color, fontweight="bold")

    ax.set_xlim(0, 12)
    ax.set_ylim(0, len(years))
    ax.set_xticks([i + 0.5 for i in range(12)])
    ax.set_xticklabels(month_labels, fontsize=9)
    ax.set_yticks([i + 0.5 for i in range(len(years))])
    ax.set_yticklabels(list(reversed(years)), fontsize=9)
    ax.tick_params(length=0)

    ax.set_title(f"{name} — Monthly Returns (%)", fontsize=14,
                 fontweight="bold", pad=15)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150, facecolor=fig.get_facecolor(),
                bbox_inches="tight")
    plt.close(fig)
    print(f"  ✓ Monthly heatmap → {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
AVAILABLE_CHARTS = {
    "equity": chart_equity_curve,
    "drawdown": chart_drawdown,
    "annual": chart_annual_returns,
    "distribution": chart_trade_distribution,
    "monthly": chart_monthly_heatmap,
}

def main():
    parser = argparse.ArgumentParser(description="AlphaScout Backtest Charts")
    parser.add_argument("results", type=str, help="Path to backtest results JSON")
    parser.add_argument("--charts", type=str, default="all",
                        help="Comma-separated chart names: equity,drawdown,annual,distribution,monthly (default: all)")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Output directory (default: same as results file)")
    args = parser.parse_args()

    # Load results
    results_path = Path(args.results)
    if not results_path.exists():
        print(f"Error: {results_path} not found")
        return

    with open(results_path) as f:
        results = json.load(f)

    # Also load daily file if it exists (has full nav_history with positions)
    daily_path = results_path.with_name(
        results_path.stem.replace(".json", "") + "_daily.json"
    )
    if not daily_path.exists():
        # Try the naming convention: ..._timestamp.json → ..._timestamp_daily.json
        daily_path = Path(str(results_path).replace(".json", "_daily.json"))
    if daily_path.exists():
        with open(daily_path) as f:
            daily = json.load(f)
        # Use daily nav_history if main one lacks detail
        if daily.get("nav_history") and len(daily["nav_history"]) > len(results.get("nav_history", [])):
            results["nav_history"] = daily["nav_history"]
        if daily.get("benchmark_nav") and not results.get("benchmark", {}).get("nav_history"):
            if "benchmark" not in results:
                results["benchmark"] = {}
            results["benchmark"]["nav_history"] = daily["benchmark_nav"]

    # Output directory
    output_dir = Path(args.output_dir) if args.output_dir else results_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Base name for output files
    base = results_path.stem
    # Strip _daily suffix if present
    if base.endswith("_daily"):
        base = base[:-6]

    # Select charts
    if args.charts == "all":
        selected = list(AVAILABLE_CHARTS.keys())
    else:
        selected = [c.strip() for c in args.charts.split(",")]
        for c in selected:
            if c not in AVAILABLE_CHARTS:
                print(f"Warning: unknown chart '{c}', skipping. Available: {list(AVAILABLE_CHARTS.keys())}")

    print(f"Generating charts for: {results.get('strategy', 'Unknown')}")
    print(f"Output: {output_dir}/")
    print()

    for chart_name in selected:
        if chart_name not in AVAILABLE_CHARTS:
            continue
        output_path = output_dir / f"{base}_{chart_name}.png"
        try:
            AVAILABLE_CHARTS[chart_name](results, str(output_path))
        except Exception as e:
            print(f"  ✗ {chart_name} failed: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
