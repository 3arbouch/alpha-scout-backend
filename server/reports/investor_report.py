"""Investor PDF report for a deployment.

matplotlib (charts) + Jinja2 (HTML/CSS) -> WeasyPrint (PDF). The data is gathered
from the same engine functions the API serves, so the report never diverges from
the live numbers. Entry point: ``build_report_pdf(deploy_id) -> bytes | None``.
"""
import os
# matplotlib needs a writable cache dir; the container runs as a non-root user
# without a home, so point it at /tmp before importing pyplot.
os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

import io
import sys
import base64
from pathlib import Path
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from jinja2 import Environment, BaseLoader, select_autoescape
from weasyprint import HTML

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from deploy_engine import (  # noqa: E402
    get_deployment, daily_pnl_series, build_position_book, get_db,
)

PORTFOLIO_COLOR = "#1a73e8"
MARKET_COLOR = "#9aa0a6"
SECTOR_COLOR = "#f29900"


# --------------------------------------------------------------------------- #
# Data gathering
# --------------------------------------------------------------------------- #
def _recent_trades(deploy_id: str, limit: int = 25) -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT date, action, symbol, shares, price, amount, reason,
                      entry_price, pnl, pnl_pct, days_held
               FROM trades WHERE source_id = ?
               ORDER BY date DESC, id DESC LIMIT ?""",
            (deploy_id, limit),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def _gather(deploy_id: str) -> dict | None:
    d = get_deployment(deploy_id)
    if not d:
        return None

    initial_capital = d.get("initial_capital") or 0
    metrics = d.get("metrics") or {}
    book = build_position_book(d.get("sleeves") or [], initial_capital)
    daily = daily_pnl_series(deploy_id) or {}

    nav_history = d.get("nav_history") or []
    last_nav = book.get("portfolio_value") or d.get("last_nav") or initial_capital

    bench_sector = d.get("benchmark_sector") or {}
    sector_nav = bench_sector.get("nav_history") if isinstance(bench_sector, dict) else None

    return {
        "deployment": d,
        "metrics": metrics,
        "book": book,
        "initial_capital": initial_capital,
        "last_nav": last_nav,
        "nav_history": nav_history,
        "benchmark_nav": d.get("benchmark_nav") or [],
        "sector_nav": sector_nav or [],
        "sector_symbol": bench_sector.get("symbol") if isinstance(bench_sector, dict) else None,
        "market_symbol": daily.get("benchmark_symbol"),
        "trades": _recent_trades(deploy_id),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


# --------------------------------------------------------------------------- #
# Charts
# --------------------------------------------------------------------------- #
def _cum_returns(points: list[dict], nav_key: str = "nav"):
    pts = [(p["date"], p[nav_key]) for p in points if p.get(nav_key) is not None]
    if len(pts) < 2:
        return [], []
    base = pts[0][1]
    if not base:
        return [], []
    dates = [datetime.strptime(d, "%Y-%m-%d") for d, _ in pts]
    vals = [(v / base - 1.0) * 100.0 for _, v in pts]
    return dates, vals


def _fig_to_data_uri(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return "data:image/png;base64," + base64.b64encode(buf.read()).decode("ascii")


def _performance_chart(data: dict) -> str | None:
    pd, pv = _cum_returns(data["nav_history"])
    if not pd:
        return None
    fig, ax = plt.subplots(figsize=(8.2, 3.2))
    ax.plot(pd, pv, color=PORTFOLIO_COLOR, linewidth=2.0, label="Portfolio", zorder=3)

    md, mv = _cum_returns(data["benchmark_nav"])
    if md:
        ax.plot(md, mv, color=MARKET_COLOR, linewidth=1.4,
                label=f"Market ({data.get('market_symbol') or 'benchmark'})")
    sd, sv = _cum_returns(data["sector_nav"])
    if sd:
        ax.plot(sd, sv, color=SECTOR_COLOR, linewidth=1.4,
                label=f"Sector ({data.get('sector_symbol') or 'benchmark'})")

    ax.axhline(0, color="#cccccc", linewidth=0.8, zorder=1)
    ax.set_ylabel("Cumulative return (%)", fontsize=9)
    ax.grid(True, axis="y", color="#eeeeee", linewidth=0.8)
    ax.legend(loc="upper left", fontsize=8, frameon=False)
    ax.tick_params(labelsize=8)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    fig.autofmt_xdate()
    return _fig_to_data_uri(fig)


def _drawdown_chart(data: dict) -> str | None:
    pts = [(p["date"], p["nav"]) for p in data["nav_history"] if p.get("nav") is not None]
    if len(pts) < 2:
        return None
    dates = [datetime.strptime(d, "%Y-%m-%d") for d, _ in pts]
    peak = float("-inf")
    dd = []
    for _, v in pts:
        peak = max(peak, v)
        dd.append((v / peak - 1.0) * 100.0 if peak else 0.0)
    fig, ax = plt.subplots(figsize=(8.2, 1.9))
    ax.fill_between(dates, dd, 0, color="#d93025", alpha=0.18)
    ax.plot(dates, dd, color="#d93025", linewidth=1.2)
    ax.set_ylabel("Drawdown (%)", fontsize=9)
    ax.grid(True, axis="y", color="#eeeeee", linewidth=0.8)
    ax.tick_params(labelsize=8)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    fig.autofmt_xdate()
    return _fig_to_data_uri(fig)


def _monthly_returns(points: list[dict], nav_key: str = "nav") -> dict:
    """Per-calendar-month % return from a date-ascending daily NAV series."""
    pts = [(p["date"], p[nav_key]) for p in points if p.get(nav_key) is not None]
    if len(pts) < 2:
        return {}
    month_end: dict[str, float] = {}
    for d, v in pts:
        month_end[d[:7]] = v  # last value within each YYYY-MM (pts ascending)
    out, prev = {}, pts[0][1]
    for ym, v in month_end.items():
        out[ym] = (v / prev - 1.0) * 100.0 if prev else 0.0
        prev = v
    return out


def _month_label(ym: str) -> str:
    return datetime.strptime(ym, "%Y-%m").strftime("%b %y")


def _monthly_bar_chart(data: dict) -> str | None:
    from matplotlib.ticker import FuncFormatter

    port = _monthly_returns(data["nav_history"])
    if not port:
        return None
    mkt = _monthly_returns(data["benchmark_nav"])
    sec = _monthly_returns(data["sector_nav"])
    months = list(port.keys())
    # Drop the current, still-in-progress calendar month: its bar would only be a
    # partial month-to-date figure (portfolio and benchmark), not a real monthly
    # return — misleading next to completed months.
    current_ym = datetime.now(timezone.utc).strftime("%Y-%m")
    if months and months[-1] == current_ym:
        months = months[:-1]
    if not months:
        return None
    xs = list(range(len(months)))

    series = [
        ("Portfolio", [port.get(m, 0) for m in months], PORTFOLIO_COLOR),
        (f"S&P 500 ({data.get('market_symbol') or 'SPY'})",
         [mkt.get(m, 0) for m in months], MARKET_COLOR),
        (f"Sector ({data.get('sector_symbol') or 'benchmark'})",
         [sec.get(m, 0) for m in months], SECTOR_COLOR),
    ]
    n = len(series)
    group_w = 0.7
    bar_w = group_w / n

    fig, ax = plt.subplots(figsize=(8.2, 2.7))
    for i, (label, vals, color) in enumerate(series):
        offsets = [x - group_w / 2 + bar_w * (i + 0.5) for x in xs]
        ax.bar(offsets, vals, bar_w * 0.86, color=color, label=label,
               edgecolor="white", linewidth=0.5, zorder=3)

    ax.axhline(0, color="#9aa0a6", linewidth=0.9, zorder=2)
    ax.set_xticks(xs)
    ax.set_xticklabels([_month_label(m) for m in months], fontsize=8, color="#3c4043")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.locator_params(axis="y", nbins=5)
    ax.tick_params(length=0, labelsize=8, colors="#5f6368")
    ax.grid(True, axis="y", color="#eef0f2", linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color("#dadce0")

    allv = [v for _, vals, _ in series for v in vals]
    lo, hi = min(allv + [0]), max(allv + [0])
    pad = max(1.5, (hi - lo) * 0.16)
    ax.set_ylim(lo - pad, hi + pad)

    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=n,
              fontsize=8, frameon=False, handlelength=1.1, columnspacing=2.0)
    fig.tight_layout()
    return _fig_to_data_uri(fig)


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #
def _money(v, decimals=0):
    if v is None:
        return "—"
    return f"${v:,.{decimals}f}"


def _pct(v, decimals=2, signed=False):
    if v is None:
        return "—"
    s = f"{v:+.{decimals}f}" if signed else f"{v:.{decimals}f}"
    return f"{s}%"


def _num(v, decimals=2):
    if v is None:
        return "—"
    return f"{v:,.{decimals}f}"


def _sign_class(v):
    if v is None:
        return "neutral"
    return "pos" if v >= 0 else "neg"


def _arrow(v):
    if v is None:
        return ""
    return "▲" if v >= 0 else "▼"


def _build_env() -> Environment:
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    env.filters["money"] = _money
    env.filters["pct"] = _pct
    env.filters["num"] = _num
    env.filters["sign_class"] = _sign_class
    env.filters["arrow"] = _arrow
    return env


def build_report_pdf(deploy_id: str) -> bytes | None:
    """Build the investor PDF for a deployment. Returns None if it doesn't exist."""
    data = _gather(deploy_id)
    if data is None:
        return None

    data["perf_chart"] = _performance_chart(data)
    data["dd_chart"] = _drawdown_chart(data)
    data["monthly_chart"] = _monthly_bar_chart(data)

    env = _build_env()
    html = env.from_string(_TEMPLATE).render(**data)
    return HTML(string=html, base_url=str(Path(__file__).parent)).write_pdf()


_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<style>
  @page {
    size: A4;
    margin: 18mm 14mm 16mm 14mm;
    @bottom-center {
      content: "AlphaScout — confidential investor report";
      font-size: 7.5pt; color: #9aa0a6;
    }
    @bottom-right { content: "Page " counter(page) " / " counter(pages);
                    font-size: 7.5pt; color: #9aa0a6; }
  }
  * { box-sizing: border-box; }
  body { font-family: "DejaVu Sans", "Helvetica Neue", Arial, sans-serif;
         color: #202124; font-size: 9pt; line-height: 1.4; margin: 0; }
  h1 { font-size: 20pt; margin: 0 0 2pt 0; }
  h2 { font-size: 12pt; margin: 18pt 0 8pt 0; padding-bottom: 4pt;
       border-bottom: 2px solid #1a73e8; color: #1a73e8; }
  .sub { color: #5f6368; font-size: 9pt; }
  .badges { margin-top: 8pt; }
  .badge { display: inline-block; padding: 2pt 8pt; border-radius: 10pt;
           font-size: 7.5pt; font-weight: 600; margin-right: 4pt;
           background: #e8f0fe; color: #1a73e8; }
  .badge.live { background: #e6f4ea; color: #137333; }
  .badge.muted { background: #f1f3f4; color: #5f6368; }

  .header { border-bottom: 1px solid #e0e0e0; padding-bottom: 10pt; }
  .header .meta { margin-top: 6pt; color: #5f6368; font-size: 8.5pt; }
  .brand { display: flex; align-items: center; gap: 6pt; margin-bottom: 10pt; }
  .brand .mark { width: 14pt; height: 14pt; background: #1a73e8;
                 border-radius: 3pt; transform: rotate(45deg); }
  .brand .word { font-size: 12pt; font-weight: 700; letter-spacing: 2pt;
                 color: #1a73e8; text-transform: uppercase; }
  .brand .word .cap { color: #5f6368; font-weight: 600; }

  .cards { display: flex; flex-wrap: wrap; gap: 8pt; margin-top: 10pt; }
  .card { flex: 1 1 22%; min-width: 95pt; border: 1px solid #e8eaed;
          border-radius: 6pt; padding: 8pt 10pt; background: #fbfcff; }
  .card .label { font-size: 7.5pt; color: #5f6368; text-transform: uppercase;
                 letter-spacing: .3pt; }
  .card .value { font-size: 14pt; font-weight: 600; margin-top: 3pt; }
  .pos { color: #137333; }
  .neg { color: #c5221f; }
  .neutral { color: #202124; }

  .panels { display: flex; gap: 10pt; margin-top: 10pt; }
  .panel { flex: 1; border: 1px solid #e8eaed; border-radius: 6pt; overflow: hidden; }
  .panel-title { background: #f1f3f4; color: #5f6368; font-size: 8pt; font-weight: 700;
                 text-transform: uppercase; letter-spacing: .5pt; padding: 5pt 10pt; }
  table.kv { width: 100%; margin: 0; }
  table.kv td { padding: 5pt 10pt; border-bottom: 1px solid #f5f5f5; }
  table.kv tr:last-child td { border-bottom: none; }
  table.kv td.k { text-align: left; color: #5f6368; font-weight: 400; background: none; }
  table.kv td.v { text-align: right; font-weight: 600; font-size: 10.5pt; }
  tr.total td { border-top: 1.5px solid #d0d0d0; font-weight: 700; background: #f8f9fa; }

  .chart { width: 100%; margin-top: 6pt; }
  .chart img { width: 100%; }

  table { width: 100%; border-collapse: collapse; margin-top: 6pt; font-size: 8pt; }
  th { text-align: right; padding: 5pt 6pt; background: #f1f3f4; color: #5f6368;
       font-weight: 600; border-bottom: 1px solid #e0e0e0; }
  th.l, td.l { text-align: left; }
  td { padding: 4pt 6pt; text-align: right; border-bottom: 1px solid #f1f3f4; }
  tr:nth-child(even) td { background: #fafbfc; }
  .muted-text { color: #9aa0a6; }
  .note { color: #9aa0a6; font-size: 8pt; margin-top: 4pt; }
  .avoid-break { page-break-inside: avoid; }
</style>
</head>
<body>

<div class="header avoid-break">
  <div class="brand">
    <span class="mark"></span>
    <span class="word">AlphaScout <span class="cap">Capital</span></span>
  </div>
  <h1>{{ deployment.name or deployment.id }}</h1>
  <div class="sub">{{ deployment.id }}</div>
  <div class="badges">
    <span class="badge">{{ (deployment.type or 'strategy')|capitalize }}</span>
    {% if deployment.live_capital %}<span class="badge live">Live capital</span>{% endif %}
    <span class="badge muted">{{ deployment.status|capitalize }}</span>
    {% if deployment.num_sleeves and deployment.num_sleeves > 1 %}
      <span class="badge muted">{{ deployment.num_sleeves }} sleeves</span>{% endif %}
  </div>
  <div class="meta">
    Period {{ deployment.start_date }} → {{ deployment.last_evaluated or '—' }}
    &nbsp;•&nbsp; Initial capital {{ initial_capital|money }}
    &nbsp;•&nbsp; Generated {{ generated_at }}
  </div>
</div>

{% set ret = (book.total_pnl / initial_capital * 100) if initial_capital else None %}
<div class="panels avoid-break">
  <div class="panel">
    <div class="panel-title">Account</div>
    <table class="kv">
      <tr><td class="k">Net Liquidation</td><td class="v">{{ book.portfolio_value|money }}</td></tr>
      <tr><td class="k">Cash</td><td class="v">{{ book.cash|money }}</td></tr>
      <tr><td class="k">Securities Value</td><td class="v">{{ book.positions_value|money }}</td></tr>
      <tr><td class="k">Initial Capital</td><td class="v">{{ initial_capital|money }}</td></tr>
    </table>
  </div>
  <div class="panel">
    <div class="panel-title">Profit &amp; Loss</div>
    <table class="kv">
      <tr><td class="k">Realized P&amp;L</td>
          <td class="v {{ book.total_realized_pnl|sign_class }}">{{ book.total_realized_pnl|arrow }} {{ book.total_realized_pnl|money }}</td></tr>
      <tr><td class="k">Unrealized P&amp;L</td>
          <td class="v {{ book.total_unrealized_pnl|sign_class }}">{{ book.total_unrealized_pnl|arrow }} {{ book.total_unrealized_pnl|money }}</td></tr>
      <tr><td class="k">Total P&amp;L</td>
          <td class="v {{ book.total_pnl|sign_class }}">{{ book.total_pnl|arrow }} {{ book.total_pnl|money }}</td></tr>
      <tr><td class="k">Return</td>
          <td class="v {{ ret|sign_class }}">{{ ret|arrow }} {{ ret|pct(2) }}</td></tr>
    </table>
  </div>
</div>

<h2>Performance</h2>
{% if perf_chart %}<div class="chart avoid-break"><img src="{{ perf_chart }}"></div>
{% else %}<div class="note">No NAV history available yet.</div>{% endif %}
{% if dd_chart %}<div class="chart avoid-break"><img src="{{ dd_chart }}"></div>{% endif %}
{% if monthly_chart %}<div class="chart avoid-break"><img src="{{ monthly_chart }}"></div>{% endif %}

<table class="avoid-break">
  <tr><th class="l">Metric</th><th>Value</th><th class="l">Metric</th><th>Value</th></tr>
  <tr><td class="l">Annualized return</td><td class="{{ metrics.get('annualized_return_pct')|sign_class }}">{{ metrics.get('annualized_return_pct')|pct(2, true) }}</td>
      <td class="l">Annualized volatility</td><td>{{ metrics.get('annualized_volatility_pct')|pct }}</td></tr>
  <tr><td class="l">Sharpe (annualized)</td><td>{{ metrics.get('sharpe_ratio_annualized')|num }}</td>
      <td class="l">Sortino</td><td>{{ metrics.get('sortino_ratio')|num }}</td></tr>
  <tr><td class="l">Max drawdown</td><td class="neg">{{ metrics.get('max_drawdown_pct')|pct }}</td>
      <td class="l">Max drawdown date</td><td>{{ metrics.get('max_drawdown_date') or '—' }}</td></tr>
  <tr><td class="l">Alpha vs market</td><td class="{{ metrics.get('alpha_vs_market_pct_period')|sign_class }}">{{ metrics.get('alpha_vs_market_pct_period')|pct(2, true) }}</td>
      <td class="l">Market return</td><td>{{ metrics.get('market_benchmark_return_pct')|pct(2, true) }}</td></tr>
  <tr><td class="l">Alpha vs sector</td><td class="{{ metrics.get('alpha_vs_sector_pct_period')|sign_class }}">{{ metrics.get('alpha_vs_sector_pct_period')|pct(2, true) }}</td>
      <td class="l">Sector return</td><td>{{ metrics.get('sector_benchmark_return_pct')|pct(2, true) }}</td></tr>
  <tr><td class="l">Win rate</td><td>{{ metrics.get('win_rate_pct')|pct }}</td>
      <td class="l">Profit factor</td><td>{{ metrics.get('profit_factor')|num }}</td></tr>
  <tr><td class="l">Closed trades</td><td>{{ metrics.get('total_trades') or 0 }}</td>
      <td class="l">Avg holding (days)</td><td>{{ metrics.get('avg_holding_days')|num(1) }}</td></tr>
</table>

<h2>Current positions</h2>
{% set open_pos = book.positions | selectattr('status', 'equalto', 'open') | list %}
{% if open_pos %}
<table>
  <tr><th class="l">Symbol</th><th class="l">Sleeve</th><th>Shares</th><th>Avg entry</th>
      <th>Price</th><th>Market value</th><th>Weight</th><th>Unrealized P&amp;L</th><th>Total P&amp;L</th></tr>
  {% for p in open_pos %}
  <tr><td class="l"><b>{{ p.symbol }}</b></td>
      <td class="l muted-text">{{ p.sleeves|join(', ') if p.sleeves else '—' }}</td>
      <td>{{ p.shares_held|num(0) }}</td>
      <td>{{ p.avg_entry|money(2) }}</td>
      <td>{{ p.current_price|money(2) }}</td>
      <td>{{ p.market_value|money(0) }}</td>
      <td>{{ p.weight_pct|pct(1) }}</td>
      <td class="{{ p.unrealized_pnl|sign_class }}">{{ p.unrealized_pnl|money(0) }}</td>
      <td class="{{ p.total_pnl_pct|sign_class }}">{{ p.total_pnl_pct|pct(1, true) }}</td></tr>
  {% endfor %}
  <tr class="total"><td class="l">Total</td><td></td><td></td><td></td><td></td>
      <td>{{ book.positions_value|money(0) }}</td>
      <td>{{ (open_pos|sum(attribute='weight_pct'))|pct(1) }}</td>
      <td class="{{ book.total_unrealized_pnl|sign_class }}">{{ book.total_unrealized_pnl|money(0) }}</td>
      <td></td></tr>
</table>
<div class="note">{{ open_pos|length }} open positions • {{ book.closed_count }} closed.</div>
{% else %}<div class="note">No open positions.</div>{% endif %}

<h2>Recent trades</h2>
{% if trades %}
<table>
  <tr><th class="l">Date</th><th class="l">Action</th><th class="l">Symbol</th><th>Shares</th>
      <th>Price</th><th>P&amp;L</th><th>Return</th><th>Days</th><th class="l">Reason</th></tr>
  {% for t in trades %}
  <tr><td class="l">{{ t.date }}</td>
      <td class="l">{{ t.action }}</td>
      <td class="l"><b>{{ t.symbol }}</b></td>
      <td>{{ t.shares|num(0) }}</td>
      <td>{{ t.price|money(2) }}</td>
      <td class="{{ t.pnl|sign_class if t.pnl is not none else 'neutral' }}">
        {{ t.pnl|money(0) if t.pnl is not none else '—' }}</td>
      <td class="{{ t.pnl_pct|sign_class if t.pnl_pct is not none else 'neutral' }}">
        {{ t.pnl_pct|pct(1, true) if t.pnl_pct is not none else '—' }}</td>
      <td>{{ t.days_held if t.days_held is not none else '—' }}</td>
      <td class="l muted-text">{{ (t.reason or '')[:42] }}</td></tr>
  {% endfor %}
</table>
{% else %}<div class="note">No trades recorded.</div>{% endif %}

</body>
</html>
"""
