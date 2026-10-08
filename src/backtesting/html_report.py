"""A self-contained HTML report of a saved backtest, for sharing.

One file with no external requests: styles inline, charts as inline SVG drawn
here, and a small script that adds hover readouts (every value is also in a
table, so the page reads fully without it). Drill-down sections use
``<details>`` so a reader opens a holding, a year, or a run in place.

Light and dark colours are both chosen: the categorical series colours are the
workstation's (validated against each surface), gains are indigo and losses
vermilion, and every number also carries its sign.
"""

from __future__ import annotations

import html
import json
import math
from datetime import datetime, timezone
from statistics import mean, median
from typing import Any, Iterable

from src.backtesting.detail import build_single_detail
from src.backtesting.zip_export import build_summary

SERIES = 6
WIDTH = 760

# ---------------------------------------------------------------------------
# formatting
# ---------------------------------------------------------------------------


def _num(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _pct(value: Any, digits: int = 1, signed: bool = True) -> str:
    number = _num(value)
    if number is None:
        return "—"
    text = f"{abs(number) * 100:,.{digits}f}%"
    if signed:
        return ("+" if number > 0 else "−" if number < 0 else "") + text
    return ("−" if number < 0 else "") + text


def _dec(value: Any, digits: int = 2) -> str:
    number = _num(value)
    return "—" if number is None else f"{number:,.{digits}f}".replace("-", "−")


def _money(value: Any) -> str:
    number = _num(value)
    if number is None:
        return "—"
    digits = 0 if abs(number) >= 1000 else 2
    return f"{number:,.{digits}f}".replace("-", "−")


def _tone(value: Any) -> str:
    number = _num(value)
    return "" if number is None or number == 0 else (" up" if number > 0 else " down")


def _short(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _e(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _heat(value: float | None, scale: float) -> str:
    """Background for a diverging cell: indigo for gains, vermilion for losses."""
    if value is None or scale <= 0:
        return ""
    strength = min(abs(value) / scale, 1.0)
    variable = "--s1" if value > 0 else "--s2"
    return f' style="background:color-mix(in srgb, var({variable}) {round(6 + strength * 26)}%, transparent)"'


# ---------------------------------------------------------------------------
# charts (inline SVG)
# ---------------------------------------------------------------------------


def _nice_ticks(low: float, high: float, count: int = 5) -> list[float]:
    if not math.isfinite(low) or not math.isfinite(high):
        return [0.0]
    if high == low:
        high, low = high + 0.01, low - 0.01
    span = high - low
    if low < 0 < high and -low < span * 0.04:
        low = 0.0  # a hair below the baseline does not earn its own tick
    if high > 0 > low and high < span * 0.04:
        high = 0.0
    raw = (high - low) / max(count - 1, 1)
    power = 10 ** math.floor(math.log10(raw))
    step = min((m * power for m in (1, 2, 2.5, 5, 10) if m * power >= raw), default=raw)
    start = math.floor(low / step) * step
    ticks = []
    value = start
    while value <= high + step * 0.5 and len(ticks) < 12:
        ticks.append(round(value, 10))
        value += step
    return ticks


def _date_ticks(labels: list[str], wanted: int = 6) -> list[int]:
    if not labels:
        return []
    if len(labels) <= wanted:
        return list(range(len(labels)))
    step = (len(labels) - 1) / (wanted - 1)
    return sorted({round(i * step) for i in range(wanted)})


class _Series:
    def __init__(self, name: str, values: list[float | None], slot: int | None = None, *, muted: bool = False, area: bool = False):
        self.name = name
        self.values = values
        self.slot = slot
        self.muted = muted
        self.area = area

    @property
    def stroke(self) -> str:
        return "var(--muted-series)" if self.muted or self.slot is None else f"var(--s{self.slot % SERIES + 1})"


def _legend(series: list[_Series], kind: str = "line") -> str:
    if len(series) < 2:
        return ""
    items = "".join(
        f'<li><span class="key key--{kind}" style="--c:{item.stroke}"></span>{_e(item.name)}</li>' for item in series
    )
    return f'<ul class="legend">{items}</ul>'


_chart_counter = 0


def line_chart(title: str, labels: list[str], series: list[_Series], *, value_format: str = "pct", zero_line: bool = True, height: int = 240, note: str = "") -> str:
    """A line chart on one axis, with a crosshair readout (values also appear in tables)."""
    global _chart_counter
    _chart_counter += 1
    values = [v for item in series for v in item.values if v is not None]
    if not labels or not values:
        return ""
    low, high = min(values + ([0.0] if zero_line else [])), max(values + ([0.0] if zero_line else []))
    ticks = _nice_ticks(low, high)
    low, high = min(ticks[0], low), max(ticks[-1], high)
    left, right, top, bottom = 8, 64, 12, 26
    plot_w, plot_h = WIDTH - left - right, height - top - bottom
    n = len(labels)

    def x(i: int) -> float:
        return left + (plot_w * i / (n - 1) if n > 1 else plot_w / 2)

    def y(v: float) -> float:
        return top + plot_h * (1 - (v - low) / (high - low))

    fmt = (lambda v: _pct(v, 0, False)) if value_format == "pct" else (lambda v: _dec(v, 2))
    parts = [f'<svg viewBox="0 0 {WIDTH} {height}" role="img" aria-label="{_e(title)}" class="chart">']
    for tick in ticks:
        parts.append(f'<line class="grid" x1="{left}" x2="{left + plot_w}" y1="{y(tick):.1f}" y2="{y(tick):.1f}"/>')
        parts.append(f'<text class="tick" x="{left + plot_w + 6}" y="{y(tick) + 3.5:.1f}">{_e(fmt(tick))}</text>')
    if zero_line and low < 0 < high:
        parts.append(f'<line class="axis" x1="{left}" x2="{left + plot_w}" y1="{y(0):.1f}" y2="{y(0):.1f}"/>')
    for index in _date_ticks(labels):
        anchor = "start" if index == 0 else "end" if index == n - 1 else "middle"
        parts.append(f'<text class="tick" text-anchor="{anchor}" x="{x(index):.1f}" y="{height - 8}">{_e(labels[index][:7])}</text>')
    for item in series:
        segments: list[list[tuple[float, float]]] = [[]]
        for i, value in enumerate(item.values):
            if value is None:
                if segments[-1]:
                    segments.append([])
                continue
            segments[-1].append((x(i), y(value)))
        for points in (segment for segment in segments if segment):
            path = " ".join(f"{'M' if j == 0 else 'L'}{px:.1f},{py:.1f}" for j, (px, py) in enumerate(points))
            if item.area:
                base = y(0 if low <= 0 <= high else low)
                parts.append(f'<path class="area" style="--c:{item.stroke}" d="{path} L{points[-1][0]:.1f},{base:.1f} L{points[0][0]:.1f},{base:.1f} Z"/>')
            parts.append(f'<path class="line{" line--muted" if item.muted else ""}" style="--c:{item.stroke}" d="{path}"/>')
        last = next(((i, v) for i, v in reversed(list(enumerate(item.values))) if v is not None), None)
        if last and not item.muted:
            parts.append(f'<circle class="dot" style="--c:{item.stroke}" cx="{x(last[0]):.1f}" cy="{y(last[1]):.1f}" r="4"/>')
    parts.append(f'<line class="crosshair" x1="0" x2="0" y1="{top}" y2="{top + plot_h}" visibility="hidden"/>')
    parts.append("</svg>")
    payload = {
        "labels": labels, "left": left, "width": plot_w, "total": WIDTH, "format": value_format,
        "series": [{"name": item.name, "color": item.stroke, "values": item.values} for item in series],
    }
    data = json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")
    caption = f'<figcaption>{_e(title)}{f"<small>{_e(note)}</small>" if note else ""}</figcaption>'
    return (
        f'<figure class="figure" data-chart="line">{caption}<div class="plot">{"".join(parts)}<div class="readout" hidden></div></div>'
        f'{_legend(series)}<script type="application/json">{data}</script></figure>'
    )


def _bar_path(x0: float, width: float, base: float, end: float, radius: float = 4) -> str:
    """A column rounded at its data end and square at the baseline."""
    height = abs(end - base)
    r = min(radius, width / 2, height)
    if end <= base:  # grows up
        return (f"M{x0:.1f},{base:.1f} L{x0:.1f},{end + r:.1f} Q{x0:.1f},{end:.1f} {x0 + r:.1f},{end:.1f} "
                f"L{x0 + width - r:.1f},{end:.1f} Q{x0 + width:.1f},{end:.1f} {x0 + width:.1f},{end + r:.1f} L{x0 + width:.1f},{base:.1f} Z")
    return (f"M{x0:.1f},{base:.1f} L{x0:.1f},{end - r:.1f} Q{x0:.1f},{end:.1f} {x0 + r:.1f},{end:.1f} "
            f"L{x0 + width - r:.1f},{end:.1f} Q{x0 + width:.1f},{end:.1f} {x0 + width:.1f},{end - r:.1f} L{x0 + width:.1f},{base:.1f} Z")


def column_chart(title: str, categories: list[str], series: list[_Series], *, height: int = 220, label_values: bool = True, note: str = "") -> str:
    """Grouped columns from a zero baseline; each column names its value on hover."""
    values = [v for item in series for v in item.values if v is not None]
    if not categories or not values:
        return ""
    ticks = _nice_ticks(min(values + [0.0]), max(values + [0.0]))
    low, high = ticks[0], ticks[-1]
    left, right, top, bottom = 8, 64, 16, 26
    plot_w, plot_h = WIDTH - left - right, height - top - bottom
    band = plot_w / len(categories)
    bar = min(24.0, (band * 0.7 - 2 * (len(series) - 1)) / max(len(series), 1))

    def y(v: float) -> float:
        return top + plot_h * (1 - (v - low) / (high - low))

    parts = [f'<svg viewBox="0 0 {WIDTH} {height}" role="img" aria-label="{_e(title)}" class="chart">']
    for tick in ticks:
        parts.append(f'<line class="grid" x1="{left}" x2="{left + plot_w}" y1="{y(tick):.1f}" y2="{y(tick):.1f}"/>')
        parts.append(f'<text class="tick" x="{left + plot_w + 6}" y="{y(tick) + 3.5:.1f}">{_e(_pct(tick, 0, False))}</text>')
    parts.append(f'<line class="axis" x1="{left}" x2="{left + plot_w}" y1="{y(0):.1f}" y2="{y(0):.1f}"/>')
    group_w = len(series) * bar + 2 * (len(series) - 1)
    step = max(1, math.ceil(len(categories) / 14))
    for c, category in enumerate(categories):
        start = left + band * c + (band - group_w) / 2
        if c % step == 0:
            parts.append(f'<text class="tick" text-anchor="middle" x="{left + band * (c + 0.5):.1f}" y="{height - 8}">{_e(category)}</text>')
        for s, item in enumerate(series):
            value = item.values[c] if c < len(item.values) else None
            if value is None:
                continue
            x0 = start + s * (bar + 2)
            parts.append(f'<path class="bar" tabindex="0" style="--c:{item.stroke}" d="{_bar_path(x0, bar, y(0), y(value))}"><title>{_e(category)} · {_e(item.name)}: {_e(_pct(value))}</title></path>')
            if label_values and len(categories) <= 12 and len(series) == 1:
                ty = y(value) - 5 if value >= 0 else y(value) + 13
                parts.append(f'<text class="value" text-anchor="middle" x="{x0 + bar / 2:.1f}" y="{ty:.1f}">{_e(_pct(value))}</text>')
    parts.append("</svg>")
    caption = f'<figcaption>{_e(title)}{f"<small>{_e(note)}</small>" if note else ""}</figcaption>'
    return f'<figure class="figure">{caption}<div class="plot">{"".join(parts)}</div>{_legend(series, "bar")}</figure>'


def diverging_bars(title: str, rows: list[tuple[str, float | None]], *, note: str = "") -> str:
    """Horizontal bars from zero: gains right in indigo, losses left in vermilion."""
    rows = [(name, value) for name, value in rows if value is not None]
    if not rows:
        return ""
    row_h, label_w, value_w = 22, 230, 70
    height = row_h * len(rows) + 8
    plot_w = WIDTH - label_w - value_w
    extent = max(abs(value) for _, value in rows) or 1.0
    has_negative = any(value < 0 for _, value in rows)
    zero = label_w + (plot_w / 2 if has_negative else 0)
    scale = (plot_w / 2 if has_negative else plot_w) / extent
    parts = [f'<svg viewBox="0 0 {WIDTH} {height}" role="img" aria-label="{_e(title)}" class="chart">']
    parts.append(f'<line class="axis" x1="{zero:.1f}" x2="{zero:.1f}" y1="0" y2="{height}"/>')
    for i, (name, value) in enumerate(rows):
        cy = 4 + i * row_h + row_h / 2
        length = abs(value) * scale
        x0 = zero if value >= 0 else zero - length
        variable = "--gain" if value >= 0 else "--loss"
        r = min(4, length / 2, 6)
        if value >= 0:
            d = f"M{x0:.1f},{cy - 6:.1f} L{x0 + length - r:.1f},{cy - 6:.1f} Q{x0 + length:.1f},{cy - 6:.1f} {x0 + length:.1f},{cy - 6 + r:.1f} L{x0 + length:.1f},{cy + 6 - r:.1f} Q{x0 + length:.1f},{cy + 6:.1f} {x0 + length - r:.1f},{cy + 6:.1f} L{x0:.1f},{cy + 6:.1f} Z"
        else:
            d = f"M{zero:.1f},{cy - 6:.1f} L{x0 + r:.1f},{cy - 6:.1f} Q{x0:.1f},{cy - 6:.1f} {x0:.1f},{cy - 6 + r:.1f} L{x0:.1f},{cy + 6 - r:.1f} Q{x0:.1f},{cy + 6:.1f} {x0 + r:.1f},{cy + 6:.1f} L{zero:.1f},{cy + 6:.1f} Z"
        parts.append(f'<text class="label" x="{label_w - 8}" y="{cy + 4:.1f}" text-anchor="end">{_e(name)}</text>')
        parts.append(f'<path class="bar" style="--c:var({variable})" d="{d}"><title>{_e(name)}: {_e(_pct(value, 2))}</title></path>')
        parts.append(f'<text class="value" x="{WIDTH - 4}" y="{cy + 4:.1f}" text-anchor="end">{_e(_pct(value, 2))}</text>')
    parts.append("</svg>")
    caption = f'<figcaption>{_e(title)}{f"<small>{_e(note)}</small>" if note else ""}</figcaption>'
    return f'<figure class="figure">{caption}<div class="plot plot--bars">{"".join(parts)}</div></figure>'


def stacked_area(title: str, labels: list[str], series: list[_Series], *, note: str = "") -> str:
    """Shares that add to 100%, stacked bottom-up with a surface gap between bands."""
    if not labels or not series:
        return ""
    height, left, right, top, bottom = 220, 8, 64, 10, 26
    plot_w, plot_h = WIDTH - left - right, height - top - bottom
    n = len(labels)

    def x(i: int) -> float:
        return left + (plot_w * i / (n - 1) if n > 1 else 0)

    def y(v: float) -> float:
        return top + plot_h * (1 - max(0.0, min(1.0, v)))

    parts = [f'<svg viewBox="0 0 {WIDTH} {height}" role="img" aria-label="{_e(title)}" class="chart">']
    for tick in (0, 0.25, 0.5, 0.75, 1.0):
        parts.append(f'<line class="grid" x1="{left}" x2="{left + plot_w}" y1="{y(tick):.1f}" y2="{y(tick):.1f}"/>')
        parts.append(f'<text class="tick" x="{left + plot_w + 6}" y="{y(tick) + 3.5:.1f}">{int(tick * 100)}%</text>')
    for index in _date_ticks(labels):
        anchor = "start" if index == 0 else "end" if index == n - 1 else "middle"
        parts.append(f'<text class="tick" text-anchor="{anchor}" x="{x(index):.1f}" y="{height - 8}">{_e(labels[index][:7])}</text>')
    base = [0.0] * n
    for item in series:
        top_line = [base[i] + (item.values[i] or 0.0) for i in range(n)]
        upper = " ".join(f"{'M' if i == 0 else 'L'}{x(i):.1f},{y(top_line[i]):.1f}" for i in range(n))
        lower = " ".join(f"L{x(i):.1f},{y(base[i]):.1f}" for i in reversed(range(n)))
        parts.append(f'<path class="band" style="--c:{item.stroke}" d="{upper} {lower} Z"><title>{_e(item.name)}</title></path>')
        base = top_line
    parts.append("</svg>")
    caption = f'<figcaption>{_e(title)}{f"<small>{_e(note)}</small>" if note else ""}</figcaption>'
    return f'<figure class="figure">{caption}<div class="plot">{"".join(parts)}</div>{_legend(series, "bar")}</figure>'


# ---------------------------------------------------------------------------
# page shell
# ---------------------------------------------------------------------------

_CSS = """
:root{--paper:#f3f0e8;--surface:#f7f5ef;--ink:#1c1b19;--text2:#4a4740;--muted:#6b675f;--rule:#ddd7ca;--grid:#e4dfd3;
--gain:#2b3a55;--loss:#b0301f;--muted-series:#8a857b;--s1:#3460A8;--s2:#C4462C;--s3:#13917A;--s4:#BE8410;--s5:#A9568F;--s6:#55891F;
color-scheme:light}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--paper:#181715;--surface:#1f1e1b;--ink:#ece8de;--text2:#c9c4b8;--muted:#9a958a;
--rule:#34322d;--grid:#2c2a26;--gain:#7f9cd6;--loss:#e07a62;--muted-series:#8a857b;--s1:#5B86D6;--s2:#DD6A4C;--s3:#1E9A80;--s4:#B98618;--s5:#C06FA6;--s6:#6FA436;color-scheme:dark}}
:root[data-theme="dark"]{--paper:#181715;--surface:#1f1e1b;--ink:#ece8de;--text2:#c9c4b8;--muted:#9a958a;--rule:#34322d;--grid:#2c2a26;
--gain:#7f9cd6;--loss:#e07a62;--s1:#5B86D6;--s2:#DD6A4C;--s3:#1E9A80;--s4:#B98618;--s5:#C06FA6;--s6:#6FA436;color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--ink);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI","Hiragino Sans","Yu Gothic",sans-serif}
main{max-width:1080px;margin:0 auto;padding:28px 16px 64px}
header.page{border-bottom:1px solid var(--rule);padding-bottom:16px;margin-bottom:20px}
.eyebrow{margin:0;color:var(--muted);font-size:11px;letter-spacing:.14em;text-transform:uppercase}
h1{margin:6px 0 4px;font:500 28px/1.2 Georgia,"Hiragino Mincho ProN","Yu Mincho",serif}
h2{margin:32px 0 10px;font:500 19px/1.3 Georgia,"Hiragino Mincho ProN","Yu Mincho",serif;border-bottom:1px solid var(--rule);padding-bottom:6px}
h3{margin:18px 0 8px;font-size:14px}
p{margin:6px 0}.muted{color:var(--muted)}.sub{color:var(--text2)}
.kpis{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));border-top:1px solid var(--rule);border-left:1px solid var(--rule);margin:16px 0}
.kpi{background:var(--surface);padding:10px 12px;border-right:1px solid var(--rule);border-bottom:1px solid var(--rule)}.kpi span{display:block;color:var(--muted);font-size:11px}
.kpi strong{display:block;font-size:20px;font-weight:600;margin-top:2px}.kpi small{color:var(--muted);font-size:11px}
.up{color:var(--gain)}.down{color:var(--loss)}
.figure{margin:14px 0;background:var(--surface);border:1px solid var(--rule);padding:12px 12px 8px}
figcaption{font-size:12px;font-weight:600;margin-bottom:6px}figcaption small{display:block;font-weight:400;color:var(--muted)}
.plot{position:relative;overflow-x:auto}.plot svg{display:block;width:100%;min-width:560px;height:auto}
.plot--bars svg{min-width:480px}
.chart text{font:11px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;fill:var(--muted)}
.chart text.label{font-family:ui-sans-serif,system-ui,sans-serif;fill:var(--ink)}.chart text.value{fill:var(--text2)}
.grid{stroke:var(--grid);stroke-width:1}.axis{stroke:var(--muted);stroke-width:1;opacity:.6}
.line{fill:none;stroke:var(--c);stroke-width:2;stroke-linejoin:round;stroke-linecap:round}.line--muted{stroke-width:1.5}
.area{fill:var(--c);opacity:.1;stroke:none}.dot{fill:var(--c);stroke:var(--surface);stroke-width:2}
.bar{fill:var(--c)}.bar:hover,.bar:focus{opacity:.8;outline:none}.band{fill:var(--c);stroke:var(--surface);stroke-width:2;opacity:.9}
.crosshair{stroke:var(--muted);stroke-width:1}
.readout{position:absolute;top:4px;pointer-events:none;background:var(--paper);border:1px solid var(--rule);padding:6px 8px;font-size:12px;min-width:150px;box-shadow:0 6px 18px rgb(0 0 0/12%)}
.readout b{display:block;font-size:11px;color:var(--muted);font-weight:500;margin-bottom:2px}
.readout div{display:flex;align-items:center;gap:6px;justify-content:space-between}.readout i{width:12px;height:2px;background:var(--c);display:inline-block}
.readout strong{font-variant-numeric:tabular-nums}
.legend{display:flex;flex-wrap:wrap;gap:4px 14px;list-style:none;padding:0;margin:6px 0 0;font-size:12px;color:var(--text2)}
.legend li{display:flex;align-items:center;gap:6px}.key{display:inline-block;background:var(--c)}.key--line{width:14px;height:2px}.key--bar{width:10px;height:10px;border-radius:2px}
.table-wrap{overflow-x:auto;border:1px solid var(--rule);background:var(--surface);margin:10px 0}
table{border-collapse:collapse;width:100%;font-size:12.5px}
th,td{padding:5px 8px;border-bottom:1px solid var(--rule);text-align:left;white-space:nowrap}
th{font-weight:600;color:var(--text2);background:var(--paper);position:sticky;top:0}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
tbody tr:last-child td{border-bottom:0}
details{border:1px solid var(--rule);background:var(--surface);margin:8px 0}
details>summary{cursor:pointer;padding:8px 12px;display:flex;flex-wrap:wrap;gap:4px 14px;align-items:baseline;list-style:none}
details>summary::-webkit-details-marker{display:none}
details>summary::before{content:"▸";color:var(--muted);width:10px}details[open]>summary::before{content:"▾"}
details>summary .title{font-weight:600;min-width:120px}details>summary .fact{font-size:12px;color:var(--text2);font-variant-numeric:tabular-nums}
details .inner{padding:0 12px 12px;border-top:1px solid var(--rule)}
.notes{border-left:2px solid var(--muted);padding:6px 12px;background:var(--surface);color:var(--text2);font-size:12.5px}
.notes ul{margin:4px 0;padding-left:18px}
.method dt{font-weight:600;margin-top:8px}.method dd{margin:2px 0 0;color:var(--text2)}
footer{margin-top:40px;color:var(--muted);font-size:11px;border-top:1px solid var(--rule);padding-top:10px}
.toggle{float:right;font:inherit;font-size:12px;border:1px solid var(--rule);background:var(--surface);color:var(--ink);padding:4px 10px;cursor:pointer}
@media print{.toggle{display:none}details{break-inside:avoid}body{background:#fff}}
"""

_SCRIPT = """
(function(){
  var root=document.documentElement;
  var btn=document.querySelector('.toggle');
  if(btn){btn.addEventListener('click',function(){var dark=root.getAttribute('data-theme')==='dark'||(!root.getAttribute('data-theme')&&matchMedia('(prefers-color-scheme: dark)').matches);root.setAttribute('data-theme',dark?'light':'dark');});}
  function fmt(v,kind){if(v===null||v===undefined||!isFinite(v))return '—';if(kind==='pct'){var s=(Math.abs(v)*100).toFixed(1)+'%';return (v>0?'+':v<0?'−':'')+s;}return v.toFixed(2);}
  document.querySelectorAll('figure[data-chart="line"]').forEach(function(fig){
    var data=JSON.parse(fig.querySelector('script[type="application/json"]').textContent);
    var svg=fig.querySelector('svg'),line=svg.querySelector('.crosshair'),out=fig.querySelector('.readout');
    function show(evt){
      var box=svg.getBoundingClientRect();var sx=(evt.clientX-box.left)/box.width*data.total;
      var n=data.labels.length;var i=Math.round((sx-data.left)/data.width*(n-1));i=Math.max(0,Math.min(n-1,i));
      var px=data.left+(n>1?data.width*i/(n-1):data.width/2);
      line.setAttribute('x1',px);line.setAttribute('x2',px);line.setAttribute('visibility','visible');
      out.textContent='';var head=document.createElement('b');head.textContent=data.labels[i];out.appendChild(head);
      data.series.forEach(function(s){var row=document.createElement('div');var key=document.createElement('span');var mark=document.createElement('i');mark.style.setProperty('--c',s.color);key.appendChild(mark);key.appendChild(document.createTextNode(' '+s.name));var val=document.createElement('strong');val.textContent=fmt(s.values[i],data.format);row.appendChild(key);row.appendChild(val);out.appendChild(row);});
      out.hidden=false;var left=px/data.total*box.width+12;if(left+out.offsetWidth>box.width)left=px/data.total*box.width-out.offsetWidth-12;out.style.left=Math.max(0,left)+'px';
    }
    function hide(){line.setAttribute('visibility','hidden');out.hidden=true;}
    svg.addEventListener('pointermove',show);svg.addEventListener('pointerleave',hide);
  });
})();
"""


def _page(title: str, eyebrow: str, subtitle: str, body: str) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{_e(title)}</title><style>{_CSS}</style></head><body><main>"
        f'<header class="page"><button type="button" class="toggle">Light / dark</button><p class="eyebrow">{_e(eyebrow)}</p>'
        f'<h1>{_e(title)}</h1><p class="sub">{_e(subtitle)}</p></header>'
        f"{body}"
        f'<footer>Shade Research backtest report · generated {generated}. Past performance in a backtest does not predict future results.</footer>'
        f"</main><script>{_SCRIPT}</script></body></html>"
    )


def _kpis(items: Iterable[tuple[str, str, str, str]]) -> str:
    cells = "".join(
        f'<div class="kpi"><span>{_e(label)}</span><strong class="{tone.strip()}">{_e(value)}</strong>{f"<small>{_e(detail)}</small>" if detail else ""}</div>'
        for label, value, tone, detail in items
    )
    return f'<div class="kpis">{cells}</div>'


def _table(headers: list[tuple[str, bool]], rows: list[list[str]], *, caption: str = "") -> str:
    head = "".join(f'<th class="{"num" if numeric else ""}" scope="col">{_e(label)}</th>' for label, numeric in headers)
    body = "".join("<tr>" + "".join(rows_cell for rows_cell in row) + "</tr>" for row in rows)
    cap = f"<caption class=\"muted\" style=\"caption-side:bottom;text-align:left;padding:6px 8px\">{_e(caption)}</caption>" if caption else ""
    return f'<div class="table-wrap"><table>{cap}<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def _td(text: str, *, num: bool = True, tone: str = "", extra: str = "") -> str:
    classes = " ".join(part for part in ("num" if num else "", tone.strip()) if part)
    return f'<td class="{classes}"{extra}>{text}</td>'


def _notes(items: list[str], title: str = "Notes") -> str:
    items = [item for item in dict.fromkeys(items) if item]
    if not items:
        return ""
    return f'<div class="notes"><strong>{_e(title)}</strong><ul>{"".join(f"<li>{_e(item)}</li>" for item in items[:40])}</ul></div>'


_METHOD = (
    ("Holdings", "Bought on the first trading day of the period at the closing price (plus any commission, slippage, and spread set for the run) and held to the end, without rebalancing."),
    ("Prices", "Daily closes, adjusted for stock splits so a split is not a gain or loss. Missing days carry the last close forward; a holding that stops trading keeps its last price."),
    ("Dividends", "Each annual dividend is split into its interim and final payments, credited on their record dates while the holding is owned, and kept as cash (not reinvested). Amounts are put on the split-adjusted share basis of the prices; the as-paid amount is shown alongside."),
    ("Benchmark", "Held the same way: one purchase on the first day, dividends kept as cash. Comparisons use only the days both series have."),
    ("Currencies", "With a base currency set, prices and dividends are converted at each day's exchange rate; native prices are shown next to holdings."),
    ("Measures", "Annualized return compounds from the purchase day. Volatility and tracking error annualize daily returns by √252; the Sharpe ratio is the annualized return less the risk-free rate over volatility. Yearly figures run from the previous year's close."),
)


def _method() -> str:
    items = "".join(f"<dt>{_e(term)}</dt><dd>{_e(text)}</dd>" for term, text in _METHOD)
    return f'<details><summary><span class="title">How these numbers are calculated</span></summary><div class="inner"><dl class="method">{items}</dl></div></details>'


# ---------------------------------------------------------------------------
# single backtest
# ---------------------------------------------------------------------------

_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _monthly_table(points: list[dict]) -> str:
    month_end: dict[str, float] = {}
    for point in points:
        value = _num(point.get("portfolio"))
        if value is not None:
            month_end[str(point.get("date", ""))[:7]] = 1 + value
    keys = sorted(month_end)
    if not keys:
        return ""
    by_year: dict[str, list[float | None]] = {}
    previous = 1.0
    year_start: dict[str, float] = {}
    for key in keys:
        year, month = key[:4], int(key[5:7])
        year_start.setdefault(year, previous)
        by_year.setdefault(year, [None] * 12)[month - 1] = month_end[key] / previous - 1
        previous = month_end[key]
    rows = []
    for year in sorted(by_year):
        cells = [f'<th scope="row">{year}</th>']
        for value in by_year[year]:
            cells.append(_td("" if value is None else _e(_pct(value)), extra=_heat(value, 0.1)))
        last_key = max(key for key in keys if key.startswith(year))
        total = month_end[last_key] / year_start[year] - 1
        cells.append(_td(f"<b>{_e(_pct(total))}</b>", extra=_heat(total, 0.4)))
        rows.append(cells)
    return _table([("Year", False), *[(month, True) for month in _MONTHS], ("Year", True)], rows)


def _holding_label(holding: dict) -> str:
    name = holding.get("name") or ""
    return f"{holding['ticker']} {name.title() if name.isupper() else name}".strip()


def _holding_section(holding: dict, detail: dict, slot: int) -> str:
    portfolio_growth = [None if value is None else 1 + value for value in detail["portfolio"]]
    chart = line_chart(
        f"{_holding_label(holding)}: growth, dividends as cash, vs the portfolio",
        detail["dates"],
        [_Series(holding["ticker"], [None if v is None else v - 1 for v in holding["growth"]], slot),
         _Series("Portfolio", [None if v is None else v - 1 for v in portfolio_growth], muted=True)],
    )
    year_rows = [[
        f'<th scope="row">{item["year"]}</th>',
        _td(_e(f'{item.get("start_date") or "—"} → {item.get("end_date") or "—"}'), num=False),
        _td(_e(_dec(item["start_price"]))), _td(_e(_dec(item["end_price"]))),
        _td(_e(_pct(item["price_return"])), tone=_tone(item["price_return"])),
        _td(_e(_pct(item["dividend_return"]))), _td(_e(_pct(item["total_return"])), tone=_tone(item["total_return"]), extra=_heat(item["total_return"], 0.4)),
        _td(_e(_pct(item.get("start_weight"), 1, False))),
        _td(_e(_pct(item["contribution"], 2)), tone=_tone(item["contribution"])),
        _td(_e(_dec(item["dividend_per_share"]))),
    ] for item in holding["years"]]
    years = _table([("Year", False), ("From → to", False), ("Start", True), ("End", True), ("Price", True), ("Dividend", True), ("Total", True), ("Weight at start", True), ("Contribution", True), ("Dividend / share", True)], year_rows, caption="Prices in the holding's own currency; returns in the base currency.") if year_rows else ""
    dividend_rows = [[
        _td(_e(item.get("record_date")), num=False), _td(_e(item.get("payment") or "—"), num=False),
        _td(_e(_dec(item.get("reported_per_share")))), _td(_e(_dec(item.get("split_factor"), 3))),
        _td(_e(_dec(item.get("per_share"), 3))), _td(_e(_dec(item.get("shares"), 1))), _td(_e(_money(item.get("cash")))),
    ] for item in holding["dividends"]]
    dividends = _table([("Record date", False), ("Payment", False), ("As paid", True), ("Split factor", True), ("Per adjusted share", True), ("Shares", True), ("Cash", True)], dividend_rows, caption="As paid × split factor = per adjusted share; shares × that = cash credited.") if dividend_rows else '<p class="muted">No dividends were credited while this holding was owned.</p>'
    summary = (
        f'<span class="title">{_e(_holding_label(holding))}</span>'
        f'<span class="fact">weight {_e(_pct(holding["weight"], 1, False))}</span>'
        f'<span class="fact{_tone(holding["total_return"])}">total {_e(_pct(holding["total_return"]))}</span>'
        f'<span class="fact">price {_e(_pct(holding["price_return"]))} · dividends {_e(_pct(holding["dividend_return"]))}</span>'
        f'<span class="fact{_tone(holding["contribution"])}">contribution {_e(_pct(holding["contribution"], 2))}</span>'
    )
    return f'<details><summary>{summary}</summary><div class="inner">{chart}<h3>By year</h3>{years}<h3>Dividends credited</h3>{dividends}</div></details>'


def render_single_report(result: dict, *, title: str, subtitle: str, backtest_id: str = "") -> str:
    summary = build_summary(result)
    metrics = result.get("metrics") or {}
    charts = result.get("chart_data") or {}
    detail = build_single_detail(result)
    cumulative = charts.get("cumulative") or []
    has_bench = _num(summary.get("benchmark_total_return")) is not None
    body: list[str] = []
    period = f"{metrics.get('start_date', '—')} → {metrics.get('end_date', '—')}"
    body.append(f'<p class="muted">{_e(period)} · base currency {_e(detail["base_currency"] or "—")} · initial capital {_e(_money(detail["initial_capital"]))}{f" · id {_e(backtest_id)}" if backtest_id else ""}</p>')
    body.append(_kpis([
        ("Total return", _pct(summary.get("total_return")), _tone(summary.get("total_return")), f"price {_pct(summary.get('price_return'))} · dividends {_pct(summary.get('dividend_return'))}"),
        ("Annualized", _pct(summary.get("annualized_return"), 2), _tone(summary.get("annualized_return")), ""),
        ("Benchmark annualized", _pct(summary.get("benchmark_annualized_return"), 2), "", f"total {_pct(summary.get('benchmark_total_return'))}" if has_bench else "no benchmark"),
        ("Excess annualized", _pct(summary.get("excess_annualized_return"), 2), _tone(summary.get("excess_annualized_return")), f"total {_pct(summary.get('excess_return'))}" if has_bench else ""),
        ("Volatility", _pct(summary.get("volatility"), 1, False), "", f"benchmark {_pct(summary.get('benchmark_volatility'), 1, False)}" if has_bench else ""),
        ("Sharpe", _dec(summary.get("sharpe_ratio")), "", f"benchmark {_dec(summary.get('benchmark_sharpe_ratio'))}" if has_bench else ""),
        ("Max drawdown", _pct(summary.get("max_drawdown"), 1, False), " down", f"benchmark {_pct(summary.get('benchmark_max_drawdown'), 1, False)}" if has_bench else ""),
        ("Tracking error", _pct(summary.get("tracking_error"), 1, False), "", ""),
        ("Information ratio", _dec(summary.get("information_ratio")), _tone(summary.get("information_ratio")), ""),
    ]))
    body.append(_notes(list(result.get("warnings") or [])))
    body.append(_method())

    body.append("<h2>Portfolio</h2>")
    sampled = cumulative[:: max(1, len(cumulative) // 400)] + ([cumulative[-1]] if cumulative else [])
    labels = [str(point.get("date", "")) for point in sampled]
    series = [_Series("Portfolio", [_num(point.get("portfolio")) for point in sampled], 0)]
    if has_bench:
        series.append(_Series("Benchmark", [_num(point.get("benchmark")) for point in sampled], muted=True))
    body.append(line_chart("Cumulative return" + (" vs the benchmark" if has_bench else ""), labels, series))
    drawdown = (charts.get("drawdown") or [])[:: max(1, len(charts.get("drawdown") or []) // 400)]
    body.append(line_chart(
        "Drawdown from the previous peak", [str(point.get("date", "")) for point in drawdown],
        [_Series("Portfolio", [_num(point.get("portfolio")) for point in drawdown], 1, area=True),
         *([_Series("Benchmark", [_num(point.get("benchmark")) for point in drawdown], muted=True)] if has_bench else [])],
    ))
    yearly = result.get("yearly_returns") or []
    bench_years = _benchmark_years(cumulative)
    if yearly:
        categories = [str(row.get("Year")) for row in yearly]
        year_series = [_Series("Portfolio", [_num(row.get("Total Return")) for row in yearly], 0)]
        if has_bench:
            year_series.append(_Series("Benchmark", [bench_years.get(category) for category in categories], muted=True))
        body.append(column_chart("Calendar-year total return", categories, year_series))
        rows = [[
            f'<th scope="row">{_e(row.get("Year"))}</th>',
            _td(_e(_pct(row.get("Price Return"))), tone=_tone(row.get("Price Return"))),
            _td(_e(_pct(row.get("Dividend Return")))),
            _td(_e(_pct(row.get("Total Return"))), tone=_tone(row.get("Total Return")), extra=_heat(_num(row.get("Total Return")), 0.4)),
            *([_td(_e(_pct(bench_years.get(str(row.get("Year"))))))] if has_bench else []),
        ] for row in yearly]
        body.append(_table([("Year", False), ("Price", True), ("Dividends", True), ("Total", True), *([("Benchmark", True)] if has_bench else [])], rows))
    monthly = _monthly_table(cumulative)
    if monthly:
        body.append("<h3>Monthly returns</h3>" + monthly)

    holdings = detail["holdings"]
    if holdings:
        body.append("<h2>Holdings</h2>")
        body.append(diverging_bars("Contribution to the total return, by holding", [(_short(_holding_label(item), 32), item["contribution"]) for item in holdings],
                                   note="Each holding's weight × its total return; they add up to the portfolio's return before costs."))
        rows = [[
            f'<th scope="row">{_e(_holding_label(item))} <span class="muted">{_e(item["currency"])}</span></th>',
            _td(_e(_pct(item["weight"], 1, False))), _td(_e(f'{_dec(item["start_price"])} → {_dec(item["end_price"])}')),
            _td(_e(_pct(item["price_return"])), tone=_tone(item["price_return"])), _td(_e(_pct(item["dividend_return"]))),
            _td(_e(_pct(item["total_return"])), tone=_tone(item["total_return"])),
            _td(_e(_pct(item["contribution"], 2)), tone=_tone(item["contribution"])),
            _td(_e(_money(item["dividends_received"]))), _td(_e(_money(item["market_value"]))),
        ] for item in holdings]
        body.append(_table([("Holding", False), ("Weight", True), ("Start → end", True), ("Price", True), ("Dividends", True), ("Total", True), ("Contribution", True), ("Dividends received", True), ("End value", True)], rows,
                           caption="Weights at purchase; values in the base currency."))
        top = holdings[:SERIES - 1]
        rest = holdings[SERIES - 1:]
        alloc_series = [_Series(item["ticker"], detail["allocation"]["holdings"].get(item["ticker"], []), index) for index, item in enumerate(top)]
        if rest:
            other = [sum((detail["allocation"]["holdings"].get(item["ticker"], [None] * len(detail["dates"]))[i] or 0.0) for item in rest) for i in range(len(detail["dates"]))]
            alloc_series.append(_Series(f"Other {len(rest)}", other, muted=True))
        alloc_series.append(_Series("Cash (dividends)", detail["allocation"]["cash"], muted=True))
        body.append(stacked_area("Allocation over time", detail["dates"], alloc_series, note="Buy-and-hold weights drift with prices; dividends build up as cash."))
        contribution_series = [_Series(item["ticker"], detail["contribution"].get(item["ticker"], []), index) for index, item in enumerate(top)]
        if rest:
            contribution_series.append(_Series(f"Other {len(rest)}", [sum((detail["contribution"].get(item["ticker"], [None] * len(detail["dates"]))[i] or 0.0) for item in rest) for i in range(len(detail["dates"]))], muted=True))
        body.append(line_chart("What each holding has added to the portfolio's return so far", detail["dates"], contribution_series, note="Gain or loss including dividends, as a share of the initial capital."))
        if detail["contribution_by_year"]:
            tickers = [item["ticker"] for item in holdings]
            year_rows = []
            for row in detail["contribution_by_year"]:
                total = sum(value or 0.0 for key, value in row.items() if key != "year")
                year_rows.append([f'<th scope="row">{row["year"]}</th>', *[_td(_e(_pct(row.get(ticker), 2)) if row.get(ticker) is not None else "", extra=_heat(row.get(ticker), 0.05)) for ticker in tickers],
                                  _td(f"<b>{_e(_pct(total, 2))}</b>", tone=_tone(total))])
            body.append("<h3>Contribution by year</h3>")
            body.append(_table([("Year", False), *[(ticker, True) for ticker in tickers], ("Portfolio", True)], year_rows,
                               caption="Each holding's start-of-year weight × its return for the year; the row adds up to the year's return."))
        body.append("<h3>Each holding</h3><p class=\"muted\">Open a holding for its growth, its years, and every dividend it was paid.</p>")
        body.extend(_holding_section(item, detail, index) for index, item in enumerate(holdings))
    if detail["dividend_payments"]:
        rows = [[_td(_e(item.get("record_date")), num=False), _td(_e(item.get("ticker")), num=False), _td(_e(item.get("payment") or "—"), num=False),
                 _td(_e(_dec(item.get("reported_per_share")))), _td(_e(_dec(item.get("split_factor"), 3))), _td(_e(_dec(item.get("per_share"), 3))),
                 _td(_e(_money(item.get("cash"))))] for item in detail["dividend_payments"]]
        body.append("<h2>Dividend ledger</h2>")
        body.append(_table([("Record date", False), ("Holding", False), ("Payment", False), ("As paid", True), ("Split factor", True), ("Per adjusted share", True), ("Cash", True)], rows))
    return _page(title, "Backtest report", subtitle, "".join(body))


def _benchmark_years(points: list[dict]) -> dict[str, float]:
    """Calendar-year returns of the benchmark line, each from the previous year's last level."""
    years: dict[str, float] = {}
    current: str | None = None
    start_level = last_level = 1.0
    for point in points:
        value = _num(point.get("benchmark"))
        if value is None:
            continue
        year = str(point.get("date", ""))[:4]
        if current is not None and year != current:
            start_level = last_level
        current = year
        last_level = 1 + value
        years[year] = last_level / start_level - 1
    return years


# ---------------------------------------------------------------------------
# sets: rolling screens and CSV sets
# ---------------------------------------------------------------------------


def _weighting_label(value: str) -> str:
    return {"market_cap": "Market cap", "equal": "Equal weight", "csv": "CSV weights"}.get(value, value)


def _stats(rows: list[dict]) -> dict[str, float | None]:
    ok = [row for row in rows if row.get("status") == "ok"]
    annual = [v for v in (_num(row.get("annualized_return")) for row in ok) if v is not None]
    excess = [v for v in (_num(row.get("excess_annualized_return")) for row in ok) if v is not None]
    beat = [row.get("beat_benchmark") for row in ok if row.get("beat_benchmark") is not None]
    sharpe = [v for v in (_num(row.get("sharpe_ratio")) for row in ok) if v is not None]
    drawdown = [v for v in (_num(row.get("max_drawdown")) for row in ok) if v is not None]
    bench = [v for v in (_num(row.get("benchmark_annualized_return")) for row in ok) if v is not None]
    return {
        "count": len(ok), "mean": mean(annual) if annual else None, "median": median(annual) if annual else None,
        "best": max(annual) if annual else None, "worst": min(annual) if annual else None,
        "positive": sum(v > 0 for v in annual) / len(annual) if annual else None,
        "win": sum(bool(v) for v in beat) / len(beat) if beat else None,
        "excess": mean(excess) if excess else None, "sharpe": mean(sharpe) if sharpe else None,
        "drawdown": mean(drawdown) if drawdown else None, "bench": mean(bench) if bench else None,
    }


def _histogram(values: list[float], bench: list[float]) -> tuple[list[str], list[float | None], list[float | None]]:
    if not values:
        return [], [], []
    spread = max(values) - min(values)
    width = 0.1 if spread > 1 else 0.05 if spread > 0.4 else 0.02 if spread > 0.15 else 0.01
    low = math.floor(min(values + bench) / width) * width
    high = math.ceil(max(values + bench) / width) * width
    edges = [low + i * width for i in range(int(round((high - low) / width)) + 1)]
    labels, own, other = [], [], []
    for start in edges[:-1] or edges:
        end = start + width
        labels.append(f"{start * 100:.0f}…{end * 100:.0f}")
        own.append(float(sum(start <= v < end or (end >= high and v == high) for v in values)))
        other.append(float(sum(start <= v < end or (end >= high and v == high) for v in bench)))
    return labels, own, other


def _count_chart(title: str, labels: list[str], own: list[float | None], other: list[float | None]) -> str:
    """A histogram of run counts (counts, not percentages, on the axis)."""
    if not labels:
        return ""
    total = max([v or 0 for v in own + other] + [1])
    height, left, right, top, bottom = 200, 8, 48, 14, 26
    plot_w, plot_h = WIDTH - left - right, height - top - bottom
    band = plot_w / len(labels)
    has_other = any(other)
    bar = min(24.0, (band * 0.8 - (2 if has_other else 0)) / (2 if has_other else 1))
    ticks = _nice_ticks(0, total, 4)

    def y(v: float) -> float:
        return top + plot_h * (1 - v / ticks[-1])

    parts = [f'<svg viewBox="0 0 {WIDTH} {height}" role="img" aria-label="{_e(title)}" class="chart">']
    for tick in ticks:
        parts.append(f'<line class="grid" x1="{left}" x2="{left + plot_w}" y1="{y(tick):.1f}" y2="{y(tick):.1f}"/>')
        parts.append(f'<text class="tick" x="{left + plot_w + 6}" y="{y(tick) + 3.5:.1f}">{int(tick)}</text>')
    step = max(1, math.ceil(len(labels) / 12))
    for i, label in enumerate(labels):
        start = left + band * i + (band - (bar * (2 if has_other else 1) + (2 if has_other else 0))) / 2
        if i % step == 0:
            parts.append(f'<text class="tick" text-anchor="middle" x="{left + band * (i + 0.5):.1f}" y="{height - 8}">{_e(label)}%</text>')
        for j, (values, cls, name) in enumerate(((own, "var(--s1)", "Strategy"), (other, "var(--muted-series)", "Benchmark, same windows"))):
            if j == 1 and not has_other:
                continue
            value = values[i] or 0
            if value <= 0:
                continue
            x0 = start + j * (bar + 2)
            parts.append(f'<path class="bar" style="--c:{cls}" d="{_bar_path(x0, bar, y(0), y(value))}"><title>{_e(label)}%: {int(value)} runs ({_e(name)})</title></path>')
    parts.append("</svg>")
    legend = _legend([_Series("Strategy", [], 0), _Series("Benchmark, same windows", [], muted=True)], "bar") if has_other else ""
    return f'<figure class="figure"><figcaption>{_e(title)}</figcaption><div class="plot">{"".join(parts)}</div>{legend}</figure>'


def _run_heatmap(rows: list[dict]) -> str:
    cells = {str(row.get("period", ""))[:7]: row for row in rows}
    years = sorted({key[:4] for key in cells})
    values = [_num(row.get("annualized_return")) for row in rows if row.get("status") in ("ok", "truncated")]
    scale = max([0.05] + [abs(v) for v in values if v is not None]) * 0.8
    out = []
    for year in years:
        line = [f'<th scope="row">{year}</th>']
        for month in range(1, 13):
            row = cells.get(f"{year}-{month:02d}")
            value = _num(row.get("annualized_return")) if row and row.get("status") in ("ok", "truncated") else None
            text = "" if row is None else "·" if value is None else _pct(value)
            line.append(_td(_e(text), extra=_heat(value, scale)))
        out.append(line)
    return _table([("Start", False), *[(month, True) for month in _MONTHS]], out, caption="Annualized return of the run that started in each month.")


def _most_held(rows: list[dict], run_holdings: dict[str, list[dict]]) -> str:
    tally: dict[str, dict[str, Any]] = {}
    ok = [row for row in rows if row.get("status") == "ok"]
    for row in ok:
        for holding in run_holdings.get(f"{row['period']}|{row['weighting']}|{row['duration']}", []):
            entry = tally.setdefault(holding["ticker"], {"runs": 0, "returns": [], "contributions": []})
            entry["runs"] += 1
            if holding.get("total_return") is not None:
                entry["returns"].append(holding["total_return"])
            if holding.get("weighted_total") is not None:
                entry["contributions"].append(holding["weighted_total"])
    if not tally:
        return '<p class="muted">Per-run holdings were not kept for this result (it was saved before they were); run it again to see them.</p>'
    ranked = sorted(tally.items(), key=lambda item: (-item[1]["runs"], -(sum(item[1]["contributions"]) if item[1]["contributions"] else 0)))[:40]
    table_rows = [[
        f'<th scope="row">{_e(ticker)}</th>', _td(f'{entry["runs"]} of {len(ok)}'),
        _td(_e(_pct(mean(entry["returns"]) if entry["returns"] else None)), tone=_tone(mean(entry["returns"]) if entry["returns"] else None)),
        _td(_e(_pct(median(entry["returns"]) if entry["returns"] else None))),
        _td(_e(_pct(sum(v > 0 for v in entry["returns"]) / len(entry["returns"]) if entry["returns"] else None, 0, False))),
        _td(_e(_pct(mean(entry["contributions"]) if entry["contributions"] else None, 2)), tone=_tone(mean(entry["contributions"]) if entry["contributions"] else None)),
    ] for ticker, entry in ranked]
    return _table([("Company", False), ("Held in", True), ("Mean return", True), ("Median", True), ("Positive", True), ("Mean contribution", True)], table_rows,
                  caption="Over complete runs of this holding period and weighting; return over each run's holding period.")


def _run_details(rows: list[dict], run_holdings: dict[str, list[dict]], limit: int = 240) -> str:
    out = []
    for row in rows[:limit]:
        key = f"{row['period']}|{row['weighting']}|{row['duration']}"
        holdings = run_holdings.get(key, [])
        status = {"ok": "complete", "truncated": "truncated", "no_data": "no prices", "failed": "failed"}.get(str(row.get("status")), str(row.get("status")))
        summary = (
            f'<span class="title">{_e(str(row.get("period", ""))[:7])}</span><span class="fact">{_e(status)}</span>'
            f'<span class="fact{_tone(row.get("annualized_return"))}">ann. {_e(_pct(row.get("annualized_return")))}</span>'
            f'<span class="fact">bench {_e(_pct(row.get("benchmark_annualized_return")))}</span>'
            f'<span class="fact{_tone(row.get("excess_annualized_return"))}">excess {_e(_pct(row.get("excess_annualized_return")))}</span>'
            f'<span class="fact">max DD {_e(_pct(row.get("max_drawdown"), 1, False))}</span><span class="fact">{_e(row.get("companies") or "—")} held</span>'
        )
        if holdings:
            table_rows = [[f'<th scope="row">{_e(item["ticker"])}</th>', _td(_e(_pct(item.get("weight"), 1, False))),
                           _td(_e(_pct(item.get("price_return"))), tone=_tone(item.get("price_return"))), _td(_e(_pct(item.get("dividend_return")))),
                           _td(_e(_pct(item.get("total_return"))), tone=_tone(item.get("total_return"))),
                           _td(_e(_pct(item.get("weighted_total"), 2)), tone=_tone(item.get("weighted_total")))] for item in holdings]
            inner = _table([("Holding", False), ("Weight", True), ("Price", True), ("Dividends", True), ("Total", True), ("Contribution", True)], table_rows)
        else:
            inner = '<p class="muted">No holding breakdown was kept for this run.</p>'
        warnings = _notes(list(row.get("warnings") or []), "Warnings")
        out.append(f'<details><summary>{summary}</summary><div class="inner">{warnings}{inner}</div></details>')
    if len(rows) > limit:
        out.append(f'<p class="muted">{len(rows) - limit} more runs are in the download.</p>')
    return "".join(out)


def render_set_report(result: dict, *, title: str, subtitle: str, backtest_id: str = "", kind: str = "rolling") -> str:
    runs: list[dict] = list(result.get("runs") or [])
    config = result.get("config") or {}
    run_holdings = result.get("run_holdings") or {}
    durations = sorted({str(row.get("duration")) for row in runs}, key=lambda value: float(value.rstrip("yr") or 0))
    weightings = list(dict.fromkeys(str(row.get("weighting")) for row in runs))
    overall = _stats(runs)
    counts = {status: sum(row.get("status") == status for row in runs) for status in ("ok", "truncated", "no_data", "failed")}
    body: list[str] = []
    meta = []
    if kind == "rolling":
        meta.append(f"top {config.get('max_companies', '—')} · ranking {config.get('ranking_algorithm', 'none')} · {config.get('cadence', '')}")
    meta.append("vs own portfolio" if config.get("benchmark_mode") == "portfolio" else f"vs {config.get('benchmark_ticker')}" if config.get("benchmark_ticker") else "no benchmark")
    if runs:
        meta.append(f"starts {str(runs[0].get('period'))[:7]} → {str(runs[-1].get('period'))[:7]}")
    if backtest_id:
        meta.append(f"id {backtest_id}")
    body.append(f'<p class="muted">{_e(" · ".join(meta))}</p>')
    body.append(_kpis([
        ("Complete runs", str(counts["ok"]), "", f"{counts['truncated']} truncated · {counts['no_data'] + counts['failed']} empty"),
        ("Mean annualized", _pct(overall["mean"]), _tone(overall["mean"]), "all holding periods"),
        ("Median", _pct(overall["median"]), _tone(overall["median"]), ""),
        ("Best · worst", f"{_pct(overall['best'], 0)} · {_pct(overall['worst'], 0)}", "", ""),
        ("Positive", _pct(overall["positive"], 0, False), "", ""),
        ("Beat the benchmark", _pct(overall["win"], 0, False), "", ""),
        ("Mean excess", _pct(overall["excess"]), _tone(overall["excess"]), "annualized"),
        ("Mean Sharpe", _dec(overall["sharpe"]), "", ""),
        ("Mean max drawdown", _pct(overall["drawdown"], 1, False), " down", ""),
    ]))
    notes = []
    if kind == "rolling" and str(config.get("ranking_algorithm", "none")) == "none":
        notes.append("This screen has no ranking: where more companies matched than were held, which ones were held was arbitrary.")
    if counts["truncated"]:
        notes.append(f"{counts['truncated']} runs end before their holding period does (prices run out) and are left out of the statistics.")
    body.append(_notes(notes))
    body.append(_method())

    body.append("<h2>Every holding period and weighting</h2>")
    matrix_rows = []
    for weighting in weightings:
        for duration in durations:
            group = [row for row in runs if row.get("weighting") == weighting and row.get("duration") == duration]
            stats = _stats(group)
            matrix_rows.append([
                f'<th scope="row">{_e(_weighting_label(weighting))}</th>', _td(_e(duration), num=False), _td(str(stats["count"])),
                _td(f"<b>{_e(_pct(stats['mean']))}</b>", tone=_tone(stats["mean"])), _td(_e(_pct(stats["median"])), tone=_tone(stats["median"])),
                _td(_e(_pct(stats["best"]))), _td(_e(_pct(stats["worst"]))), _td(_e(_pct(stats["positive"], 0, False))),
                _td(_e(_pct(stats["win"], 0, False))), _td(_e(_pct(stats["excess"])), tone=_tone(stats["excess"])),
                _td(_e(_pct(stats["bench"]))), _td(_e(_dec(stats["sharpe"]))), _td(_e(_pct(stats["drawdown"], 1, False))),
            ])
    body.append(_table([("Weighting", False), ("Hold", False), ("Runs", True), ("Mean ann.", True), ("Median", True), ("Best", True), ("Worst", True), ("Positive", True),
                        ("Beat bench", True), ("Mean excess", True), ("Bench ann.", True), ("Sharpe", True), ("Max DD", True)], matrix_rows,
                       caption="Annualized returns of complete runs."))

    periods = sorted({str(row.get("period")) for row in runs})
    by_key = {(str(row.get("period")), str(row.get("weighting")), str(row.get("duration"))): row for row in runs}
    for weighting in weightings:
        series = [
            _Series(duration, [
                _num(by_key[(period, weighting, duration)].get("annualized_return")) if (period, weighting, duration) in by_key and by_key[(period, weighting, duration)].get("status") in ("ok", "truncated") else None
                for period in periods
            ], index)
            for index, duration in enumerate(durations[:SERIES])
        ]
        if durations:
            first = durations[0]
            series.append(_Series(f"Benchmark {first}", [_num(by_key[(period, weighting, first)].get("benchmark_annualized_return")) if (period, weighting, first) in by_key else None for period in periods], muted=True))
        body.append(line_chart(f"Annualized return by start month · {_weighting_label(weighting)}", periods, series))

    for weighting in weightings:
        for index, duration in enumerate(durations):
            group = [row for row in runs if row.get("weighting") == weighting and row.get("duration") == duration]
            if not group:
                continue
            stats = _stats(group)
            ok = [row for row in group if row.get("status") == "ok"]
            values = [v for v in (_num(row.get("annualized_return")) for row in ok) if v is not None]
            bench = [v for v in (_num(row.get("benchmark_annualized_return")) for row in ok) if v is not None]
            labels, own, other = _histogram(values, bench)
            summary = (
                f'<span class="title">{_e(_weighting_label(weighting))} · {_e(duration)}</span>'
                f'<span class="fact">{stats["count"]} complete runs</span>'
                f'<span class="fact{_tone(stats["mean"])}">mean {_e(_pct(stats["mean"]))}</span>'
                f'<span class="fact">beat the benchmark {_e(_pct(stats["win"], 0, False))}</span>'
            )
            inner = (
                _count_chart(f"Annualized return of complete runs, % ({len(values)} runs)", labels, own, other)
                + "<h3>By start month</h3>" + _run_heatmap(group)
                + "<h3>Companies held most often</h3>" + _most_held(group, run_holdings)
                + "<h3>Each run</h3><p class=\"muted\">Open a run for its holdings and what each contributed.</p>"
                + _run_details(sorted(group, key=lambda row: str(row.get("period"))), run_holdings)
            )
            open_attr = " open" if index == 0 and weighting == weightings[0] else ""
            body.append(f"<details{open_attr}><summary>{summary}</summary><div class=\"inner\">{inner}</div></details>")
    if not runs:
        body.append('<p class="muted">This result has only summary statistics (it was saved before per-run results were kept).</p>')
    return _page(title, "Backtest report", subtitle, "".join(body))


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def render_report(stored: dict, *, meta: dict | None = None, backtest_id: str = "") -> str:
    """Render a saved ``result.json`` payload as one self-contained HTML page."""
    meta = meta or {}
    if "metrics" in stored:
        tickers = [str(row.get("Ticker")) for row in stored.get("per_company") or []]
        title = meta.get("title") or ("Backtest · " + ", ".join(tickers[:4]) + (f" +{len(tickers) - 4}" if len(tickers) > 4 else ""))
        return render_single_report(stored, title=title, subtitle=meta.get("subtitle") or "", backtest_id=backtest_id)
    kind = meta.get("kind") or ("rolling" if "config" in stored else "csv")
    title = meta.get("title") or ("Rolling screen backtest" if kind == "rolling" else "CSV portfolio backtest")
    return render_set_report(stored, title=title, subtitle=meta.get("subtitle") or "", backtest_id=backtest_id, kind=kind)
