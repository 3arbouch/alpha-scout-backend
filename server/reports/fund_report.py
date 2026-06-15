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

from reports.investor_report import (
    _money, _pct, _num, _build_env, _fig_to_data_uri, PORTFOLIO_COLOR,
)

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import funds as _funds  # noqa: E402


def _nav_unit_chart(weekly: list[dict], base: float) -> str | None:
    pts = [(p["date"], p["nav_per_unit"]) for p in weekly if p.get("nav_per_unit") is not None]
    if len(pts) < 2:
        return None
    from datetime import datetime
    dates = [datetime.strptime(d, "%Y-%m-%d") for d, _ in pts]
    vals = [v for _, v in pts]
    fig, ax = plt.subplots(figsize=(8.2, 3.0))
    ax.plot(dates, vals, color=PORTFOLIO_COLOR, linewidth=2.0)
    ax.axhline(base, color="#cccccc", linewidth=0.8)
    ax.set_ylabel("NAV per unit ($)", fontsize=9, color="#5f6368")
    ax.grid(True, axis="y", color="#eef0f2", linewidth=0.8)
    ax.tick_params(length=0, labelsize=8, colors="#5f6368")
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color("#dadce0")
    fig.autofmt_xdate()
    return _fig_to_data_uri(fig)


def build_fund_report_pdf(fund_id: str) -> bytes | None:
    fund = _funds.get_fund(fund_id)
    if not fund:
        return None
    weekly = _funds.nav_per_unit_series(fund_id, weekly=True)
    nav_now = weekly[-1]["nav_per_unit"] if weekly else fund["base_nav_per_unit"]
    since_incept = (nav_now / fund["base_nav_per_unit"] - 1.0) * 100.0
    book = _funds.fund_investors(fund_id)

    data = {
        "fund": fund,
        "nav_now": nav_now,
        "since_inception_pct": since_incept,
        "as_of": weekly[-1]["date"] if weekly else fund["inception_date"],
        "book": book,
        "chart": _nav_unit_chart(weekly, fund["base_nav_per_unit"]),
        "weekly_points": len(weekly),
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
</style>
<div class="brand"><span class="mark"></span><span class="word">AlphaScout <span class="cap">Capital</span></span></div>
"""

_FUND_TEMPLATE = r"""<!DOCTYPE html><html><head><meta charset="utf-8">""" + _HEAD + r"""
</head><body>
<h1>{{ fund.name }}</h1>
<div class="sub">Fund · {{ fund.id }}</div>
<div class="meta">Inception {{ fund.inception_date }} · Base NAV/unit {{ fund.base_nav_per_unit|money(2) }}
  · {{ fund.currency }} · {{ fund.dealing_frequency|capitalize }} dealing · as of {{ as_of }}</div>

<div class="cards">
  <div class="card"><div class="label">NAV / unit</div><div class="value">{{ nav_now|money(4) }}</div></div>
  <div class="card"><div class="label">Since inception</div>
    <div class="value {{ 'pos' if since_inception_pct >= 0 else 'neg' }}">{{ since_inception_pct|pct(2, true) }}</div></div>
  <div class="card"><div class="label">AUM</div><div class="value">{{ book.aum|money(0) }}</div></div>
  <div class="card"><div class="label">Investors</div><div class="value">{{ book.investor_count }}</div></div>
</div>

<h2>NAV per unit (weekly, base = {{ fund.base_nav_per_unit|money(0) }})</h2>
{% if chart %}<div class="chart"><img src="{{ chart }}"></div>
{% else %}<div class="note">Not enough history to chart yet.</div>{% endif %}

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
