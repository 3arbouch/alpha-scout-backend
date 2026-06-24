"""Fund tear sheet + per-investor statement PDFs (WeasyPrint + matplotlib).

Reuses the rendering helpers from investor_report. Entry points:
    build_fund_report_pdf(fund_id) -> bytes | None
    build_investor_statement_pdf(fund_id, investor_id) -> bytes | None
"""
import os
os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from weasyprint import HTML

from datetime import datetime, timezone

from reports.investor_report import (
    _money, _pct, _num, _build_env, _fig_to_data_uri,
    PORTFOLIO_COLOR, MARKET_COLOR, SECTOR_COLOR,
    _monthly_returns, _month_label,
)
from reports import fund_commentary

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import funds as _funds  # noqa: E402
from deploy_engine import get_deployment, build_position_book  # noqa: E402


def _rebased(series, start_date: str, base: float):
    """Rebase a [{date, nav}] series to `base` at the first date >= start_date,
    so a deployment benchmark overlays comparably on the fund's NAV/unit line."""
    pts = [(p["date"], p["nav"]) for p in (series or [])
           if p.get("nav") is not None and p["date"] >= start_date]
    if len(pts) < 2 or not pts[0][1]:
        return [], []
    b0 = pts[0][1]
    dates = [datetime.strptime(d, "%Y-%m-%d") for d, _ in pts]
    vals = [base * v / b0 for _, v in pts]
    return dates, vals


def _nav_unit_chart(weekly: list[dict], base: float, market=None, sector=None,
                    market_label: str | None = None, sector_label: str | None = None) -> str | None:
    pts = [(p["date"], p["nav_per_unit"]) for p in weekly if p.get("nav_per_unit") is not None]
    if len(pts) < 2:
        return None
    start_date = pts[0][0]
    dates = [datetime.strptime(d, "%Y-%m-%d") for d, _ in pts]
    vals = [v for _, v in pts]
    fig, ax = plt.subplots(figsize=(8.2, 3.0))
    ax.plot(dates, vals, color=PORTFOLIO_COLOR, linewidth=2.0, label="Fund (NAV/unit)", zorder=3)

    md, mv = _rebased(market, start_date, base)
    if md:
        ax.plot(md, mv, color=MARKET_COLOR, linewidth=1.4,
                label=f"Market ({market_label or 'SPY'})", zorder=2)
    sd, sv = _rebased(sector, start_date, base)
    if sd:
        ax.plot(sd, sv, color=SECTOR_COLOR, linewidth=1.4,
                label=f"Sector ({sector_label or 'benchmark'})", zorder=2)

    ax.axhline(base, color="#cccccc", linewidth=0.8, zorder=1)
    ax.set_ylabel("NAV per unit ($)", fontsize=9, color="#5f6368")
    ax.grid(True, axis="y", color="#eef0f2", linewidth=0.8)
    ax.tick_params(length=0, labelsize=8, colors="#5f6368")
    if md or sd:
        ax.legend(loc="upper left", fontsize=8, frameon=False)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color("#dadce0")
    fig.autofmt_xdate()
    return _fig_to_data_uri(fig)


def _latest_month(daily: list[dict]) -> tuple[str, float] | None:
    """(month label, % return) for the most recent COMPLETED calendar month."""
    mr = _monthly_returns(daily, "nav_per_unit")
    if not mr:
        return None
    current_ym = datetime.now(timezone.utc).strftime("%Y-%m")
    completed = [(ym, r) for ym, r in mr.items() if ym != current_ym]
    if not completed:
        return None
    ym, ret = completed[-1]
    return _month_label(ym), ret


def _deployment_extras(fund: dict) -> dict:
    """Performance metrics, top contributors/detractors and sector exposure from
    the fund's underlying deployment. Empty dict if the deployment can't load."""
    try:
        d = get_deployment(fund["deployment_id"])
    except Exception:
        d = None
    if not d:
        return {}

    book = build_position_book(d.get("sleeves") or [], d.get("initial_capital") or 0)
    positions = [p for p in (book.get("positions") or []) if p.get("total_pnl") is not None]
    ranked = sorted(positions, key=lambda p: p["total_pnl"], reverse=True)
    contributors = [p for p in ranked if p["total_pnl"] > 0][:5]
    detractors = sorted((p for p in positions if p["total_pnl"] < 0),
                        key=lambda p: p["total_pnl"])[:5]

    open_pos = [p for p in positions if p.get("status") == "open"]
    sec_map = _funds.symbol_sectors([p["symbol"] for p in open_pos])
    agg: dict[str, float] = {}
    for p in open_pos:
        sec = sec_map.get(p["symbol"]) or "Unclassified"
        agg[sec] = agg.get(sec, 0.0) + (p.get("weight_pct") or 0.0)
    sector_exposure = sorted(
        ({"sector": k, "weight_pct": v} for k, v in agg.items()),
        key=lambda x: x["weight_pct"], reverse=True,
    )
    bench_sector = d.get("benchmark_sector") or {}
    return {
        "metrics": d.get("metrics") or {},
        "contributors": contributors,
        "detractors": detractors,
        "sector_exposure": sector_exposure,
        "benchmark_nav": d.get("benchmark_nav") or [],
        "sector_nav": (bench_sector.get("nav_history") if isinstance(bench_sector, dict) else None) or [],
        "market_symbol": (d.get("benchmark_market") or {}).get("symbol"),
        "sector_symbol": bench_sector.get("symbol") if isinstance(bench_sector, dict) else None,
    }


def _commentary_facts(fund, nav_now, since_incept, as_of, period_label,
                      latest_month, extras, aum) -> dict:
    def r(v, n=2):
        return round(v, n) if isinstance(v, (int, float)) else v

    m = extras.get("metrics", {})
    return {
        "fund_name": fund["name"],
        "period": period_label,
        "as_of": as_of,
        "currency": fund["currency"],
        "nav_per_unit": r(nav_now, 4),
        "aum": r(aum, 0),
        "since_inception_return_pct": r(since_incept),
        "latest_completed_month": (
            {"month": latest_month[0], "return_pct": r(latest_month[1])}
            if latest_month else None),
        "annualized_return_pct": r(m.get("annualized_return_pct")),
        "annualized_volatility_pct": r(m.get("annualized_volatility_pct")),
        "sharpe_annualized": r(m.get("sharpe_ratio_annualized")),
        "sortino": r(m.get("sortino_ratio")),
        "max_drawdown_pct": r(m.get("max_drawdown_pct")),
        "alpha_vs_market_pct": r(m.get("alpha_vs_market_pct_period")),
        "market_return_pct": r(m.get("market_benchmark_return_pct")),
        "alpha_vs_sector_pct": r(m.get("alpha_vs_sector_pct_period")),
        "sector_return_pct": r(m.get("sector_benchmark_return_pct")),
        "top_contributors": [
            {"symbol": p["symbol"], "pnl": r(p["total_pnl"], 0),
             "return_pct": r(p.get("total_pnl_pct"))}
            for p in extras.get("contributors", [])],
        "top_detractors": [
            {"symbol": p["symbol"], "pnl": r(p["total_pnl"], 0),
             "return_pct": r(p.get("total_pnl_pct"))}
            for p in extras.get("detractors", [])],
        "sector_exposure": [
            {"sector": s["sector"], "weight_pct": r(s["weight_pct"], 1)}
            for s in extras.get("sector_exposure", [])],
    }


def build_fund_report_pdf(fund_id: str, include_commentary: bool = True) -> bytes | None:
    fund = _funds.get_fund(fund_id)
    if not fund:
        return None
    weekly = _funds.nav_per_unit_series(fund_id, weekly=True)
    daily = _funds.nav_per_unit_series(fund_id, weekly=False)
    nav_now = weekly[-1]["nav_per_unit"] if weekly else fund["base_nav_per_unit"]
    since_incept = (nav_now / fund["base_nav_per_unit"] - 1.0) * 100.0
    as_of = weekly[-1]["date"] if weekly else fund["inception_date"]
    book = _funds.fund_investors(fund_id)

    period_label = datetime.strptime(as_of, "%Y-%m-%d").strftime("%B %Y")
    latest_month = _latest_month(daily)
    extras = _deployment_extras(fund)

    commentary = None
    if include_commentary:
        facts = _commentary_facts(fund, nav_now, since_incept, as_of, period_label,
                                  latest_month, extras, book.get("aum"))
        commentary = fund_commentary.get_commentary(fund_id, as_of, facts)

    data = {
        "fund": fund,
        "nav_now": nav_now,
        "since_inception_pct": since_incept,
        "as_of": as_of,
        "period_label": period_label,
        "latest_month": latest_month,
        "book": book,
        "chart": _nav_unit_chart(
            weekly, fund["base_nav_per_unit"],
            market=extras.get("benchmark_nav"), sector=extras.get("sector_nav"),
            market_label=extras.get("market_symbol"), sector_label=extras.get("sector_symbol")),
        "weekly_points": len(weekly),
        "metrics": extras.get("metrics", {}),
        "contributors": extras.get("contributors", []),
        "detractors": extras.get("detractors", []),
        "sector_exposure": extras.get("sector_exposure", []),
        "commentary": commentary,
    }
    html = _build_env().from_string(_FUND_TEMPLATE).render(**data)
    return HTML(string=html, base_url=str(Path(__file__).parent)).write_pdf()


def build_investor_statement_pdf(fund_id: str, investor_id: str) -> bytes | None:
    try:
        st = _funds.investor_statement(fund_id, investor_id)
    except ValueError:
        return None
    html = _build_env().from_string(_STATEMENT_TEMPLATE).render(s=st)
    return HTML(string=html, base_url=str(Path(__file__).parent)).write_pdf()


_HEAD = r"""
<style>
  @page { size: A4; margin: 18mm 14mm 16mm 14mm;
    @bottom-center { content: "AlphaScout Capital — confidential"; font-size: 7.5pt; color: #9aa0a6; }
    @bottom-right { content: "Page " counter(page) " / " counter(pages); font-size: 7.5pt; color: #9aa0a6; } }
  * { box-sizing: border-box; }
  body { font-family: "DejaVu Sans", Arial, sans-serif; color: #202124; font-size: 9pt; line-height: 1.4; margin: 0; }
  h1 { font-size: 19pt; margin: 0 0 2pt 0; }
  h2 { font-size: 12pt; margin: 16pt 0 8pt 0; padding-bottom: 4pt; border-bottom: 2px solid #1a73e8; color: #1a73e8; }
  .sub { color: #5f6368; font-size: 9pt; }
  .brand { display: flex; align-items: center; gap: 6pt; margin-bottom: 10pt; }
  .brand .mark { width: 14pt; height: 14pt; background: #1a73e8; border-radius: 3pt; transform: rotate(45deg); }
  .brand .word { font-size: 12pt; font-weight: 700; letter-spacing: 2pt; color: #1a73e8; text-transform: uppercase; }
  .brand .word .cap { color: #5f6368; font-weight: 600; }
  .meta { margin-top: 6pt; color: #5f6368; font-size: 8.5pt; }
  .cards { display: flex; flex-wrap: wrap; gap: 8pt; margin-top: 10pt; }
  .card { flex: 1 1 22%; min-width: 95pt; border: 1px solid #e8eaed; border-radius: 6pt; padding: 8pt 10pt; background: #fbfcff; }
  .card .label { font-size: 7.5pt; color: #5f6368; text-transform: uppercase; letter-spacing: .3pt; }
  .card .value { font-size: 14pt; font-weight: 600; margin-top: 3pt; }
  .pos { color: #137333; } .neg { color: #c5221f; }
  .chart img { width: 100%; margin-top: 6pt; }
  table { width: 100%; border-collapse: collapse; margin-top: 6pt; font-size: 8pt; }
  th { text-align: right; padding: 5pt 6pt; background: #f1f3f4; color: #5f6368; font-weight: 600; border-bottom: 1px solid #e0e0e0; }
  th.l, td.l { text-align: left; }
  td { padding: 4pt 6pt; text-align: right; border-bottom: 1px solid #f1f3f4; }
  tr:nth-child(even) td { background: #fafbfc; }
  .panels { display: flex; gap: 10pt; margin-top: 10pt; }
  .panel { flex: 1; border: 1px solid #e8eaed; border-radius: 6pt; overflow: hidden; }
  .panel-title { background: #f1f3f4; color: #5f6368; font-size: 8pt; font-weight: 700; text-transform: uppercase; letter-spacing: .5pt; padding: 5pt 10pt; }
  table.kv { width: 100%; margin: 0; }
  table.kv td { padding: 5pt 10pt; border-bottom: 1px solid #f5f5f5; }
  table.kv td.k { text-align: left; color: #5f6368; background: none; }
  table.kv td.v { text-align: right; font-weight: 600; font-size: 10.5pt; }
  .note { color: #9aa0a6; font-size: 8pt; margin-top: 4pt; }
  .prose { font-size: 9pt; line-height: 1.55; color: #3c4043; margin-top: 6pt; white-space: pre-wrap; }
  .two-col { display: flex; gap: 10pt; margin-top: 6pt; }
  .two-col > div { flex: 1; }
  .col-title { font-size: 8pt; font-weight: 700; color: #5f6368; text-transform: uppercase; letter-spacing: .4pt; }
  .avoid-break { page-break-inside: avoid; }
  .disclosures { color: #9aa0a6; font-size: 7.5pt; margin-top: 16pt; border-top: 1px solid #e8eaed; padding-top: 6pt; }
</style>
<div class="brand"><span class="mark"></span><span class="word">AlphaScout <span class="cap">Capital</span></span></div>
"""

_FUND_TEMPLATE = r"""<!DOCTYPE html><html><head><meta charset="utf-8">""" + _HEAD + r"""
</head><body>
<h1>{{ fund.name }}</h1>
<div class="sub">Monthly Report — {{ period_label }}</div>
<div class="meta">Fund · {{ fund.id }} · Inception {{ fund.inception_date }}
  · Base NAV/unit {{ fund.base_nav_per_unit|money(2) }} · {{ fund.currency }}
  · {{ fund.dealing_frequency|capitalize }} dealing · as of {{ as_of }}</div>

<div class="cards">
  <div class="card"><div class="label">NAV / unit</div><div class="value">{{ nav_now|money(4) }}</div></div>
  <div class="card"><div class="label">Since inception</div>
    <div class="value {{ 'pos' if since_inception_pct >= 0 else 'neg' }}">{{ since_inception_pct|pct(2, true) }}</div></div>
  {% if latest_month %}
  <div class="card"><div class="label">{{ latest_month[0] }}</div>
    <div class="value {{ 'pos' if latest_month[1] >= 0 else 'neg' }}">{{ latest_month[1]|pct(2, true) }}</div></div>
  {% endif %}
  <div class="card"><div class="label">AUM</div><div class="value">{{ book.aum|money(0) }}</div></div>
  <div class="card"><div class="label">Investors</div><div class="value">{{ book.investor_count }}</div></div>
</div>

<h2>Performance Summary</h2>
{% if metrics %}
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
</table>
{% else %}<div class="note">Performance metrics not available yet.</div>{% endif %}

<h2>Performance Chart — NAV per unit (weekly, base = {{ fund.base_nav_per_unit|money(0) }})</h2>
{% if chart %}<div class="chart"><img src="{{ chart }}"></div>
{% else %}<div class="note">Not enough history to chart yet.</div>{% endif %}

{% if commentary and commentary.commentary %}
<h2>Monthly Commentary</h2>
<div class="prose">{{ commentary.commentary }}</div>
{% endif %}

{% if contributors or detractors %}
<h2>Top Contributors &amp; Detractors</h2>
<div class="two-col avoid-break">
  <div>
    <div class="col-title">Top Contributors</div>
    {% if contributors %}
    <table>
      <tr><th class="l">Symbol</th><th>P&amp;L</th><th>Return</th></tr>
      {% for p in contributors %}
      <tr><td class="l"><b>{{ p.symbol }}</b></td>
          <td class="pos">{{ p.total_pnl|money(0) }}</td>
          <td class="{{ p.total_pnl_pct|sign_class }}">{{ p.total_pnl_pct|pct(1, true) }}</td></tr>
      {% endfor %}
    </table>
    {% else %}<div class="note">None.</div>{% endif %}
  </div>
  <div>
    <div class="col-title">Top Detractors</div>
    {% if detractors %}
    <table>
      <tr><th class="l">Symbol</th><th>P&amp;L</th><th>Return</th></tr>
      {% for p in detractors %}
      <tr><td class="l"><b>{{ p.symbol }}</b></td>
          <td class="neg">{{ p.total_pnl|money(0) }}</td>
          <td class="{{ p.total_pnl_pct|sign_class }}">{{ p.total_pnl_pct|pct(1, true) }}</td></tr>
      {% endfor %}
    </table>
    {% else %}<div class="note">None.</div>{% endif %}
  </div>
</div>
<div class="note">Contribution measured as total P&amp;L (realized + unrealized) since inception.</div>
{% endif %}

{% if sector_exposure %}
<h2>Portfolio Positioning — Sector Exposure</h2>
<table class="avoid-break">
  <tr><th class="l">Sector</th><th>Weight of NAV</th></tr>
  {% for s in sector_exposure %}
  <tr><td class="l">{{ s.sector }}</td><td>{{ s.weight_pct|pct(1) }}</td></tr>
  {% endfor %}
</table>
<div class="note">Weights are of current open positions as a share of net liquidation value.</div>
{% endif %}

{% if commentary and commentary.outlook %}
<h2>Outlook</h2>
<div class="prose">{{ commentary.outlook }}</div>
{% endif %}

<h2>Investors</h2>
{% if book.positions %}
<table>
  <tr><th class="l">Investor</th><th>Units</th><th>Net invested</th><th>Current value</th><th>Gain</th></tr>
  {% for p in book.positions %}
  <tr><td class="l">{{ p.investor_name or p.investor_id }}</td>
      <td>{{ p.units|num(4) }}</td>
      <td>{{ p.net_invested|money(0) }}</td>
      <td>{{ p.current_value|money(0) }}</td>
      <td class="{{ 'pos' if p.gain >= 0 else 'neg' }}">{{ p.gain|money(0) }}</td></tr>
  {% endfor %}
  <tr><td class="l"><b>Total</b></td><td><b>{{ book.units_outstanding|num(4) }}</b></td><td></td>
      <td><b>{{ book.aum|money(0) }}</b></td><td></td></tr>
</table>
{% else %}<div class="note">No investors yet.</div>{% endif %}

<div class="disclosures">Past performance is not indicative of future results. This document is
  for informational purposes only and does not constitute an offer to sell or a solicitation of an
  offer to buy any security or interest. Any commentary and outlook reflect conditions as of the
  date shown and are subject to change without notice.</div>
</body></html>"""

_STATEMENT_TEMPLATE = r"""<!DOCTYPE html><html><head><meta charset="utf-8">""" + _HEAD + r"""
</head><body>
<h1>Investor Statement</h1>
<div class="sub">{{ s.investor_name }} · {{ s.fund_name }}</div>
<div class="meta">As of {{ s.as_of }} · Entry {{ s.entry_date }} at NAV/unit {{ s.entry_nav_per_unit|money(4) }}
  · NAV/unit now {{ s.nav_per_unit_now|money(4) }}</div>

<div class="panels">
  <div class="panel"><div class="panel-title">Your Capital</div>
    <table class="kv">
      <tr><td class="k">Units held</td><td class="v">{{ s.units_held|num(4) }}</td></tr>
      <tr><td class="k">Total contributed</td><td class="v">{{ s.contributions|money(0) }}</td></tr>
      <tr><td class="k">Net invested</td><td class="v">{{ s.net_invested|money(0) }}</td></tr>
      <tr><td class="k">Current value</td><td class="v">{{ s.current_value|money(0) }}</td></tr>
    </table>
  </div>
  <div class="panel"><div class="panel-title">Your Return</div>
    <table class="kv">
      <tr><td class="k">Total gain</td>
          <td class="v {{ 'pos' if s.gain >= 0 else 'neg' }}">{{ s.gain|money(0) }}</td></tr>
      <tr><td class="k">Return on capital</td>
          <td class="v {{ 'pos' if (s.return_on_capital_pct or 0) >= 0 else 'neg' }}">{{ s.return_on_capital_pct|pct(2, true) }}</td></tr>
      <tr><td class="k">Money-weighted IRR (ann.)</td>
          <td class="v {{ 'pos' if (s.money_weighted_irr_pct or 0) >= 0 else 'neg' }}">{{ s.money_weighted_irr_pct|pct(2, true) }}</td></tr>
      <tr><td class="k">Fund return (since entry)</td>
          <td class="v {{ 'pos' if (s.fund_return_pct or 0) >= 0 else 'neg' }}">{{ s.fund_return_pct|pct(2, true) }}</td></tr>
    </table>
  </div>
</div>

<h2>Transactions</h2>
<table>
  <tr><th class="l">Date</th><th class="l">Type</th><th>Amount</th><th>NAV/unit</th><th>Units</th></tr>
  {% for t in s.transactions %}
  <tr><td class="l">{{ t.date }}</td><td class="l">{{ t.type|capitalize }}</td>
      <td>{{ t.amount|money(0) }}</td><td>{{ t.nav_per_unit|money(4) }}</td><td>{{ t.units|num(4) }}</td></tr>
  {% endfor %}
</table>
<div class="note">Return on capital is your simple money-in vs money-out return; IRR annualizes it for the timing of your flows;
  fund return is the strategy's time-weighted return since your entry.</div>
</body></html>"""
