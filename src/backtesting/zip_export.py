"""ZIP export for backtest results.

Generates .zip archives containing reports, CSVs, and chart PNGs for both
single and rolling backtests.  Results are saved server-side and served as
static files.
"""

from __future__ import annotations

import csv
import io
import logging
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO
from uuid import uuid4

logger = logging.getLogger(__name__)


class ExportSizeLimitExceeded(ValueError):
    """Raised before a generated archive can exceed its disk budget."""

    def __init__(self, limit_bytes: int, attempted_bytes: int) -> None:
        self.limit_bytes = limit_bytes
        self.attempted_bytes = attempted_bytes
        super().__init__(
            f"Generated archive exceeds the {limit_bytes}-byte limit"
        )


class _SizeLimitedWriter:
    """Seekable file wrapper that rejects writes beyond a fixed size."""

    def __init__(self, stream: BinaryIO, max_bytes: int) -> None:
        self._stream = stream
        self._max_bytes = max_bytes

    def write(self, data: bytes) -> int:
        attempted_end = self._stream.tell() + len(data)
        if attempted_end > self._max_bytes:
            raise ExportSizeLimitExceeded(self._max_bytes, attempted_end)
        return self._stream.write(data)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._stream.seek(offset, whence)

    def tell(self) -> int:
        return self._stream.tell()

    def flush(self) -> None:
        self._stream.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._stream, name)

# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def build_single_backtest_zip(result: dict[str, Any]) -> bytes:
    """Build a ZIP archive for a single backtest result.

    Contains ``report.txt``, ``metrics.csv``, ``capital_allocation.csv``,
    ``per_company_per_year.csv``, ``per_company_per_day.csv``, and chart
    PNGs regenerated from chart data.
    """
    buf = io.BytesIO()
    metrics = result.get("metrics", {})
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # Report
        report = _build_report_text(
            metrics,
            result.get("per_company"),
            result.get("yearly_returns"),
            result.get("dividends_by_year"),
            result.get("warnings"),
            per_company_per_year=result.get("per_company_per_year"),
        )
        zf.writestr("report.txt", report)

        # CSVs
        if metrics:
            _write_csv(zf, "metrics.csv", [_flat_metrics(metrics)])

        per_company = result.get("per_company")
        if per_company:
            _write_csv(zf, "capital_allocation.csv", per_company)

        pyp = result.get("per_company_per_year")
        if pyp:
            _write_csv(zf, "per_company_per_year.csv", pyp)

        # Daily data is too large for the ZIP; saved separately as
        # per_company_per_day.csv on disk.

        # Charts
        chart_data = result.get("chart_data", {})
        for chart_type in ["cumulative", "drawdown", "decomposition"]:
            png = _chart_png_from_json(chart_data, chart_type)
            if png is not None:
                filename = {
                    "cumulative": "cumulative_returns.png",
                    "drawdown": "drawdown.png",
                    "decomposition": "decomposition.png",
                }[chart_type]
                zf.writestr(filename, png)

    return buf.getvalue()


def build_rolling_zip(rolling_result: dict[str, Any]) -> bytes:
    """Build a ZIP archive from a rolling backtest result."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        _write_rolling_zip(zf, rolling_result)

    return buf.getvalue()


def _write_rolling_zip(
    archive: zipfile.ZipFile,
    rolling_result: dict[str, Any],
) -> None:
    config = rolling_result.get("config", {})
    aggregate = rolling_result.get("aggregate", {})
    results = rolling_result.get("results", [])
    _add_summary_txt(archive, config, aggregate)
    _add_summary_csv(archive, aggregate)
    _add_heatmap_csv(archive, aggregate)
    _add_backtest_files(archive, results)


def save_rolling_backtest_zip(
    rolling_result: dict[str, Any],
    base_dir: str,
    max_bytes: int,
) -> str:
    """Build a bounded rolling archive directly on disk and return its directory."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    root = Path(base_dir)
    root.mkdir(parents=True, exist_ok=True)
    identifier = f"{datetime.now():%Y%m%d_%H%M%S}_{uuid4().hex[:8]}"
    out_dir = root / identifier
    out_dir.mkdir(exist_ok=False)
    partial_path = out_dir / "backtest.zip.partial"
    zip_path = out_dir / "backtest.zip"
    try:
        with partial_path.open("w+b") as stream:
            limited_stream = _SizeLimitedWriter(stream, max_bytes)
            with zipfile.ZipFile(
                limited_stream,
                "w",
                zipfile.ZIP_DEFLATED,
            ) as archive:
                _write_rolling_zip(archive, rolling_result)
        partial_path.replace(zip_path)
    except Exception:
        shutil.rmtree(out_dir, ignore_errors=True)
        raise
    logger.info("Backtest saved to %s (%d bytes)", zip_path, zip_path.stat().st_size)
    return str(out_dir)


def save_backtest_zip(
    zip_bytes: bytes, base_dir: str = "data/Backtests"
) -> str:
    """Save ZIP bytes to a timestamped directory, return the relative path."""
    ts = f"{datetime.now():%Y%m%d_%H%M%S}_{uuid4().hex[:8]}"
    out_dir = Path(base_dir) / ts
    out_dir.mkdir(parents=True, exist_ok=False)
    zip_path = out_dir / "backtest.zip"
    zip_path.write_bytes(zip_bytes)

    logger.info("Backtest saved to %s", zip_path)
    return str(out_dir)


def build_summary(result: dict[str, Any]) -> dict[str, Any]:
    """Extract a lightweight summary from a backtest result dict."""
    m = result.get("metrics", {})
    return {
        "total_return": m.get("total_return"),
        "annualized_return": m.get("annualized_return"),
        "price_return": m.get("portfolio_price_return"),
        "dividend_return": m.get("portfolio_dividend_return"),
        "volatility": m.get("volatility"),
        "sharpe_ratio": m.get("sharpe_ratio"),
        "max_drawdown": m.get("max_drawdown"),
        "start_date": m.get("start_date"),
        "end_date": m.get("end_date"),
        "initial_capital": m.get("initial_capital"),
        "benchmark_total_return": m.get("benchmark_total_return"),
        "excess_return": m.get("excess_return"),
        "tickers": [r.get("Ticker") for r in result.get("per_company", [])],
        "warnings": result.get("warnings", []),
    }


# ---------------------------------------------------------------------------
# Summary files
# ---------------------------------------------------------------------------


def _add_summary_txt(
    zf: zipfile.ZipFile, config: dict, agg: dict,
) -> None:
    """Write ``summary.txt`` — config info and aggregate statistics."""
    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("  ROLLING SCREENING BACKTEST — SUMMARY")
    lines.append("=" * 60)
    lines.append("")

    # ── Config ────────────────────────────────────────────────────────
    lines.append("--- Configuration ---")
    config_rows = [
        ("Cadence", config.get("cadence", "")),
        ("Durations", _join_list(config.get("durations"))),
        ("Weighting modes", _join_list(config.get("weighting_modes"))),
        ("Max companies", str(config.get("max_companies", ""))),
        ("Benchmark ticker", config.get("benchmark_ticker", "(none)")),
        ("Benchmark mode", config.get("benchmark_mode", "ticker")),
        ("Base currency", config.get("base_currency") or "(native)"),
        ("Start period", config.get("start_period") or "(earliest)"),
        ("End period", config.get("end_period") or "(latest)"),
    ]
    max_label = max(len(l) for l, _ in config_rows)
    for label, value in config_rows:
        lines.append(f"  {label:<{max_label}}  {value}")

    # Criteria summary
    criteria = config.get("criteria")
    if criteria and isinstance(criteria, list) and len(criteria) > 0:
        lines.append("")
        lines.append("  Screening criteria:")
        for c in criteria:
            if isinstance(c, dict):
                lines.append(f"    {_fmt_criterion(c)}")

    lines.append("")

    # ── Aggregate stats ───────────────────────────────────────────────
    lines.append("--- Aggregate Statistics ---")
    lines.append(f"  Total backtests:   {agg.get('total_runs', 0)}")
    lines.append(f"  Successful:         {agg.get('successful', 0)}")
    lines.append(f"  Failed:             {agg.get('failed', 0)}")
    lines.append(f"  Periods screened:   {agg.get('periods', 0)}")

    date_range = agg.get("date_range", {})
    if date_range:
        lines.append(
            f"  Date range:         {date_range.get('first', '?')} → "
            f"{date_range.get('last', '?')}"
        )

    # Overall stats
    stats: dict = agg.get("stats", {})
    for stat_key, stat_label in [
        ("total_return", "Total Return"),
        ("sharpe_ratio", "Sharpe Ratio"),
        ("max_drawdown", "Max Drawdown"),
    ]:
        s = stats.get(stat_key, {})
        if s:
            lines.append("")
            lines.append(f"  {stat_label}:")
            lines.append(f"    Mean:   {_fmt_pct(s.get('mean'))}")
            lines.append(f"    Median: {_fmt_pct(s.get('median'))}")
            lines.append(f"    Min:    {_fmt_pct(s.get('min'))}")
            lines.append(f"    Max:    {_fmt_pct(s.get('max'))}")
            lines.append(f"    Std:    {_fmt_pct(s.get('std'))}")

    lines.append("")

    # ── By-weighting × duration table ─────────────────────────────────
    by_weighting: dict = agg.get("by_weighting", {})
    if by_weighting:
        lines.append("--- Returns by Weighting & Duration ---")
        header = (
            f"  {'Weighting':<12} {'Dur':>5}  {'Count':>6}  "
            f"{'Mean Ret':>10}  {'Med Ret':>10}  {'Mean Sharpe':>12}"
        )
        lines.append(header)
        lines.append("  " + "-" * (len(header) - 2))
        for wm, dur_data in sorted(by_weighting.items()):
            for dur, d in sorted(dur_data.items()):
                lines.append(
                    f"  {wm:<12} {dur:>5}  {d.get('count', 0):>6}  "
                    f"{_fmt_pct(d.get('mean_return')):>10}  "
                    f"{_fmt_pct(d.get('median_return')):>10}  "
                    f"{d.get('mean_sharpe', 0):>12.3f}"
                )

    lines.append("")

    # ── Benchmark comparison ──────────────────────────────────────────
    bc = agg.get("benchmark_comparison")
    if bc:
        lines.append("--- Benchmark Comparison ---")
        lines.append(f"  Outperformed:  {bc.get('outperformed', 0)}")
        lines.append(f"  Underperformed: {bc.get('underperformed', 0)}")
        wr = bc.get("win_rate", 0)
        lines.append(f"  Win rate:      {wr * 100:.1f}%")

        by_dur = bc.get("by_duration")
        if by_dur:
            lines.append("")
            lines.append("  By duration:")
            header = f"    {'Dur':>5}  {'Wins':>5} {'Loss':>5} {'Win %':>7}  {'Bench Ret':>10}"
            lines.append(header)
            lines.append("    " + "-" * (len(header) - 4))
            for dur, d in sorted(by_dur.items()):
                wins = d.get("out", 0)
                total = d.get("total", 0)
                losses = total - wins
                wr = d.get("win_rate", 0)
                bench_ret = _fmt_pct(d.get("bench_mean_return"))
                lines.append(
                    f"    {dur:>5}  {wins:>5} {losses:>5} {wr * 100:>6.1f}%  "
                    f"{bench_ret:>10}"
                )

    lines.append("")
    lines.append("=" * 60)
    lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 60)

    zf.writestr("summary.txt", "\n".join(lines))


def _add_summary_csv(zf: zipfile.ZipFile, agg: dict) -> None:
    """Write ``summary.csv`` — one row per weighting × duration combo."""
    by_weighting: dict = agg.get("by_weighting", {})
    if not by_weighting:
        return

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "weighting", "duration", "count",
        "mean_return", "median_return", "mean_sharpe",
    ])
    for wm, dur_data in sorted(by_weighting.items()):
        for dur, d in sorted(dur_data.items()):
            writer.writerow([
                wm, dur, d.get("count", 0),
                d.get("mean_return"), d.get("median_return"),
                d.get("mean_sharpe"),
            ])

    zf.writestr("summary.csv", buf.getvalue())


def _add_heatmap_csv(zf: zipfile.ZipFile, agg: dict) -> None:
    """Write ``heatmap.csv`` — period × duration return matrix.

    Structure of ``agg.heatmap``::

        {
            "equal": {
                "1yr":  [{"period": "2020-01", "return": 0.15}, ...],
                "2yr":  [...],
            },
            "market_cap": {...},
            "excess": {
                "1yr":  [{"period": "2020-01", "return": 0.03}, ...],
            },
        }
    """
    heatmap: dict = agg.get("heatmap", {})
    if not heatmap:
        return

    # Collect all unique periods (preserve order)
    periods: list[str] = []
    seen: set[str] = set()
    for wm_data in heatmap.values():
        for entries in wm_data.values():
            for entry in entries:
                p = entry.get("period", "")
                if p not in seen:
                    seen.add(p)
                    periods.append(p)

    # Determine all (weighting, duration) columns
    weightings = sorted(
        k for k in heatmap if k not in ("excess",)
    )
    if not weightings:
        # Fallback: collect from all keys
        weightings = sorted(heatmap.keys())

    # Collect all durations across weightings
    all_durations: list[str] = []
    dur_seen: set[str] = set()
    for wm in weightings:
        for dur in heatmap.get(wm, {}):
            if dur not in dur_seen:
                dur_seen.add(dur)
                all_durations.append(dur)

    # Build header
    header = ["period"]
    for wm in weightings:
        for dur in all_durations:
            header.append(f"{wm}_{dur}")
    # Excess columns
    excess_data: dict = heatmap.get("excess", {})
    for dur in all_durations:
        if dur in excess_data:
            header.append(f"excess_{dur}")

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)

    # Build lookup: (wm, dur, period) → return
    lookup: dict[tuple[str, str, str], float | None] = {}
    for wm in weightings:
        for dur, entries in heatmap.get(wm, {}).items():
            for entry in entries:
                lookup[(wm, dur, entry.get("period", ""))] = entry.get("return")

    exc_lookup: dict[tuple[str, str], float | None] = {}
    for dur, entries in excess_data.items():
        for entry in entries:
            exc_lookup[(dur, entry.get("period", ""))] = entry.get("return")

    for period in periods:
        row = [period]
        for wm in weightings:
            for dur in all_durations:
                val = lookup.get((wm, dur, period))
                row.append(val)
        for dur in all_durations:
            if dur in excess_data:
                val = exc_lookup.get((dur, period))
                row.append(val)
        writer.writerow(row)

    zf.writestr("heatmap.csv", buf.getvalue())


# ---------------------------------------------------------------------------
# Per‑backtest files
# ---------------------------------------------------------------------------


def _add_backtest_files(
    zf: zipfile.ZipFile, results: list[dict],
) -> None:
    """Iterate all backtests and write one subdirectory per combination."""
    for period_result in results:
        period = period_result.get("period", "unknown")
        backtests: dict = period_result.get("backtests", {})

        for wm, dur_data in sorted(backtests.items()):
            for dur, bt in sorted(dur_data.items()):

                folder = _sanitize_path(f"backtests/{period}_{wm}_{dur}")
                metrics = bt.get("metrics") if isinstance(bt, dict) else None

                if metrics is None:
                    # Failed backtest
                    warnings = bt.get("warnings", []) if isinstance(bt, dict) else []
                    msg = (
                        f"Backtest failed for period={period}, "
                        f"weighting={wm}, duration={dur}\n"
                    )
                    if warnings:
                        msg += "\nWarnings:\n" + "\n".join(
                            f"  - {w}" for w in warnings
                        )
                    zf.writestr(f"{folder}/_failed.txt", msg)
                    continue

                # ── Report ──────────────────────────────────────────
                report = _build_report_text(
                    metrics,
                    bt.get("per_company"),
                    bt.get("yearly_returns"),
                    bt.get("dividends_by_year"),
                    bt.get("warnings"),
                    period=period,
                    weighting=wm,
                    duration=dur,
                    per_company_per_year=bt.get("per_company_per_year"),
                )
                zf.writestr(f"{folder}/report.txt", report)

                # ── Chart PNGs ──────────────────────────────────────
                chart_data = bt.get("chart_data", {})
                for chart_type in ["cumulative", "drawdown", "decomposition"]:
                    png = _chart_png_from_json(chart_data, chart_type)
                    if png is not None:
                        filename = {
                            "cumulative": "cumulative_returns.png",
                            "drawdown": "drawdown.png",
                            "decomposition": "decomposition.png",
                        }[chart_type]
                        zf.writestr(f"{folder}/{filename}", png)

                # ── CSVs ────────────────────────────────────────────
                # Performance metrics
                if metrics:
                    _write_csv(zf, f"{folder}/metrics.csv", [_flat_metrics(metrics)])

                # Capital allocation / per-company summary
                per_company = bt.get("per_company")
                if per_company and isinstance(per_company, list) and len(per_company) > 0:
                    _write_csv(zf, f"{folder}/capital_allocation.csv", per_company)

                # Per-company per-year (aggregate of daily)
                per_company_per_year = bt.get("per_company_per_year")
                if per_company_per_year and isinstance(per_company_per_year, list) and len(per_company_per_year) > 0:
                    _write_csv(zf, f"{folder}/per_company_per_year.csv", per_company_per_year)

                # Per-company per-day (source of truth)
                daily_data = bt.get("daily")
                if daily_data and isinstance(daily_data, list) and len(daily_data) > 0:
                    _write_csv(zf, f"{folder}/per_company_per_day.csv", daily_data)


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

_PER_COMPANY_COLUMNS = [
    "Ticker", "total_return", "price_return", "dividend_return",
    "weight", "weighted_price", "weighted_dividend", "weighted_total",
    "capital_invested", "shares_purchased",
    "dividends_received", "market_value", "start_price", "end_price",
]


def _build_report_text(
    metrics: dict,
    per_company: list[dict] | None,
    yearly_returns: list[dict] | None,
    dividends_by_year: list[dict] | None,
    warnings: list[str] | None,
    *,
    period: str = "",
    weighting: str = "",
    duration: str = "",
    per_company_per_year: list[dict] | None = None,
) -> str:
    """Build a human‑readable text report for a single backtest."""
    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("  BACKTEST REPORT")
    lines.append("=" * 60)
    lines.append(f"  Period:      {period}")
    lines.append(f"  Weighting:   {weighting}")
    lines.append(f"  Duration:    {duration}")
    lines.append(f"  Dates:       {metrics.get('start_date', '?')} → "
                  f"{metrics.get('end_date', '?')}")
    bc = metrics.get("base_currency", "")
    if bc:
        lines.append(f"  Base Currency: {bc}")
    lines.append("")

    # ── Core metrics ─────────────────────────────────────────────────
    lines.append("--- Performance Metrics ---")
    metric_rows = [
        ("Total Return", _fmt_pct(metrics.get("total_return"))),
        ("Annualized Return", _fmt_pct(metrics.get("annualized_return"))),
        ("Price Return", _fmt_pct(metrics.get("portfolio_price_return"))),
        ("Dividend Return", _fmt_pct(metrics.get("portfolio_dividend_return"))),
        ("Volatility (ann.)", _fmt_pct(metrics.get("volatility"))),
        ("Sharpe Ratio", f"{metrics.get('sharpe_ratio', 0):.4f}"
                         f"  (rf={metrics.get('risk_free_rate', 0):.2%})"),
        ("Max Drawdown", _fmt_pct(metrics.get("max_drawdown"))),
    ]
    max_label = max(len(l) for l, _ in metric_rows)
    for label, value in metric_rows:
        lines.append(f"  {label:<{max_label}}  {value}")

    # Benchmark
    bench_total = metrics.get("benchmark_total_return")
    if bench_total is not None:
        lines.append("")
        lines.append("--- Benchmark ---")
        bench_rows = [
            ("Benchmark Total Return", _fmt_pct(bench_total)),
            ("Benchmark Ann. Return",
             _fmt_pct(metrics.get("benchmark_annualized_return"))),
            ("Benchmark Volatility",
             _fmt_pct(metrics.get("benchmark_volatility"))),
            ("Benchmark Max Drawdown",
             _fmt_pct(metrics.get("benchmark_max_drawdown"))),
            ("Excess Return", _fmt_pct(metrics.get("excess_return"))),
        ]
        info_ratio = metrics.get("information_ratio")
        if info_ratio is not None:
            bench_rows.append(("Information Ratio", f"{info_ratio:.4f}"))
        bench_sharpe = metrics.get("benchmark_sharpe_ratio")
        if bench_sharpe is not None:
            bench_rows.append(("Benchmark Sharpe", f"{bench_sharpe:.4f}"))
        max_bl = max(len(l) for l, _ in bench_rows)
        for label, value in bench_rows:
            lines.append(f"  {label:<{max_bl}}  {value}")

    # Initial capital & VAMI
    initial_capital = metrics.get("initial_capital", 0.0)
    if initial_capital > 0:
        lines.append("")
        lines.append("--- Capital Allocation ---")
        lines.append(f"  Initial Capital: {initial_capital:,.0f}")
        # VAMI = final value of the virtual portfolio
        total_ret = metrics.get("total_return", 0.0) or 0.0
        final_vami = initial_capital * (1.0 + total_ret)
        lines.append(f"  Final VAMI:      {final_vami:,.0f}  "
                     f"(change: {total_ret:+.2%})")
        if per_company and len(per_company) > 0:
            lines.append("")
            alloc_header = (
                f"  {'Ticker':<10} {'Weight':>8} {'Currency':>10} "
                f"{'Start Price':>12} {'Capital':>14} "
                f"{'Shares':>12} {'Cost Basis':>14}"
            )
            lines.append(alloc_header)
            lines.append("  " + "-" * (len(alloc_header) - 2))
            total_capital = 0.0
            for row in per_company:
                weight = row.get("weight", 0) or 0
                start_px = row.get("start_price", 0) or 0
                capital = row.get("capital_invested") or (weight * initial_capital)
                shares_val = row.get("shares_purchased") or (capital / start_px if start_px else 0)
                cost_basis = shares_val * start_px
                total_capital += cost_basis
                currency = row.get("Currency", "")
                lines.append(
                    f"  {str(row.get('Ticker', '')):<10} "
                    f"{weight:>7.1%} "
                    f"{currency:<10} "
                    f"{start_px:>12,.2f} "
                    f"{capital:>14,.0f} "
                    f"{shares_val:>11,.2f} "
                    f"{cost_basis:>14,.0f}"
                )
            lines.append("  " + "-" * (len(alloc_header) - 2))
            lines.append(
                f"  {'TOTAL':<10} "
                f"{sum(row.get('weight', 0) or 0 for row in per_company):>7.1%} "
                f"{'':<10} "
                f"{'':>12} "
                f"{total_capital:>14,.0f} "
                f"{'':>12} "
                f"{total_capital:>14,.0f}"
            )

    # ── Per‑company per‑year breakdown (PRIMARY) ──────────────────────
    if per_company_per_year and len(per_company_per_year) > 0:
        lines.append("")
        lines.append("--- Per Company Per Year Breakdown ---")
        # Determine column set
        pyp_cols_all = list(per_company_per_year[0].keys())
        # Preferred order
        pyp_col_order = [
            "Year", "Ticker",
            "Starting_Shares", "Dividend_Per_Share",
            "Start_Price", "End_Price",
            "Total_Dividends_Received", "Dividend_Currency",
            "Ending_Shares",
            "Starting_Market_Value", "Ending_Market_Value",
            "Price_Return_Pct", "Dividend_Return_Pct", "Total_Return_Pct",
            "Weighted_Value_Start", "Weighted_Value_End", "Weighted_Return",
        ]
        pyp_cols = [c for c in pyp_col_order if c in pyp_cols_all]
        # Add any extra columns not in the preferred list
        for c in pyp_cols_all:
            if c not in pyp_cols:
                pyp_cols.append(c)

        # Group by year
        years_seen = sorted({r.get("Year") for r in per_company_per_year if r.get("Year") is not None})
        for yr in years_seen:
            yr_rows = [r for r in per_company_per_year if r.get("Year") == yr]
            lines.append("")
            yr_header = f"  --- {yr} ---"
            lines.append(yr_header)
            col_widths = _column_widths(yr_rows, pyp_cols)
            header = "  " + "  ".join(
                f"{c:<{col_widths[c]}}" for c in pyp_cols
            )
            lines.append(header)
            lines.append("  " + "-" * (len(header) - 2))
            for row in yr_rows:
                line = "  " + "  ".join(
                    _fmt_cell(row.get(c), col_widths[c]) for c in pyp_cols
                )
                lines.append(line)

    # ── Per‑company per‑day summary (from daily tracker) ───────────
    # Full daily data is available in per_company_per_day.csv
    lines.append("")
    lines.append("--- Per Company Per Day ---")
    lines.append("  (Full daily breakdown in per_company_per_day.csv)")
    if per_company_per_year and len(per_company_per_year) > 0:
        # Show annual start/end snapshot per ticker from the yearly aggregate
        tickers_seen = set()
        for row in per_company_per_year:
            tk = row.get("Ticker", "")
            if tk and tk != row.get("Dividend_Currency", ""):
                tickers_seen.add(tk)
        lines.append(f"  Tickers tracked daily: {', '.join(sorted(tickers_seen))}")

    # ── Warnings ─────────────────────────────────────────────────────
    if warnings and len(warnings) > 0:
        lines.append("")
        lines.append("--- Warnings ---")
        for w in warnings:
            lines.append(f"  ⚠ {w}")

    lines.append("")
    lines.append("=" * 60)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Chart generation
# ---------------------------------------------------------------------------

_CHART_FILENAME_MAP = {
    "cumulative": "cumulative_returns.png",
    "drawdown": "drawdown.png",
    "decomposition": "decomposition.png",
}

# Match the styling of ``generate_backtest_charts()`` in the orchestrator.
_COLORS = {
    "portfolio_total": "#2196F3",
    "portfolio_price": "#64B5F6",
    "benchmark_total": "#FF9800",
    "benchmark_price": "#FFB74D",
    "drawdown_fill": "#EF5350",
    "drawdown_line": "#C62828",
    "benchmark_drawdown": "#FF9800",
    "price_fill": "#42A5F5",
    "dividend_fill": "#66BB6A",
    "total_line": "#1B5E20",
}


def _chart_png_from_json(
    chart_data: dict, chart_type: str,
) -> bytes | None:
    """Build a PNG chart from JSON chart data arrays.

    Args:
        chart_data: The ``chart_data`` dict from a ``BacktestResult``,
            with keys ``cumulative``, ``drawdown``, ``decomposition``.
        chart_type: One of ``"cumulative"``, ``"drawdown"``, ``"decomposition"``.

    Returns:
        PNG bytes, or ``None`` if the corresponding data is empty/missing.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("matplotlib not installed — skipping chart generation.")
        return None

    data = chart_data.get(chart_type)
    if not data or len(data) == 0:
        return None

    # Parse date strings into datetime objects
    dates = []
    for entry in data:
        try:
            dates.append(datetime.strptime(entry["date"], "%Y-%m-%d"))
        except (ValueError, KeyError):
            dates.append(None)
    # Filter out bad dates
    valid = [(d, e) for d, e in zip(dates, data) if d is not None]
    if not valid:
        return None
    dates, data_entries = zip(*valid)

    if chart_type == "cumulative":
        png = _plot_cumulative(dates, data_entries)
    elif chart_type == "drawdown":
        png = _plot_drawdown(dates, data_entries)
    elif chart_type == "decomposition":
        png = _plot_decomposition(dates, data_entries)
    else:
        return None

    plt.close("all")
    return png


def _plot_cumulative(
    dates: tuple, entries: tuple,
) -> bytes | None:
    """Plot cumulative returns: portfolio vs benchmark, with VAMI on right axis."""
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker

    portfolio_vals = [e.get("portfolio", 0) * 100 for e in entries]
    benchmark_vals = [e.get("benchmark") for e in entries]
    vami_vals = [e.get("vami") for e in entries]
    has_benchmark = any(b is not None for b in benchmark_vals)
    has_vami = any(v is not None for v in vami_vals)

    if all(v == 0 for v in portfolio_vals) and not has_benchmark:
        return None

    fig, ax = plt.subplots(figsize=(12, 6))

    # Portfolio return (left axis, %)
    ax.plot(dates, portfolio_vals, label="Portfolio (Total)",
            linewidth=2, color=_COLORS["portfolio_total"])

    # VAMI (right axis, absolute value)
    if has_vami:
        ax_vami = ax.twinx()
        ax_vami.plot(dates, vami_vals, label="VAMI",
                     linewidth=1.5, linestyle="--", color="#2ea043")
        ax_vami.set_ylabel("VAMI (value of initial capital)", color="#2ea043")
        ax_vami.tick_params(axis="y", labelcolor="#2ea043")
        # Format large numbers compactly
        ax_vami.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1e3:.0f}K")
        )

    if has_benchmark:
        b_vals = [b * 100 if b is not None else float("nan")
                  for b in benchmark_vals]
        ax.plot(dates, b_vals, label="Benchmark (Total)",
                linewidth=2, color=_COLORS["benchmark_total"])

    # Combine legends from both axes
    lines = ax.get_lines() + (ax_vami.get_lines() if has_vami else [])
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc="best")

    ax.set_title("Cumulative Returns", fontsize=14)
    ax.set_ylabel("Return (%)")
    ax.set_xlabel("Date")
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color="grey", linewidth=0.8, linestyle="-")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    buf.seek(0)
    result = buf.read()
    plt.close(fig)
    return result


def _plot_drawdown(
    dates: tuple, entries: tuple,
) -> bytes | None:
    """Plot drawdown: filled area + line for portfolio, optional benchmark."""
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    portfolio_vals = [e.get("portfolio", 0) * 100 for e in entries]
    benchmark_vals = [e.get("benchmark") for e in entries]
    has_benchmark = any(b is not None for b in benchmark_vals)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.fill_between(dates, portfolio_vals, 0,
                    color=_COLORS["drawdown_fill"], alpha=0.4,
                    label="Drawdown")
    ax.plot(dates, portfolio_vals, color=_COLORS["drawdown_line"],
            linewidth=1)

    if has_benchmark:
        b_vals = [b * 100 if b is not None else float("nan")
                  for b in benchmark_vals]
        ax.plot(dates, b_vals, color=_COLORS["benchmark_drawdown"],
                linewidth=1, linestyle="--", label="Benchmark Drawdown")

    ax.set_title("Drawdown", fontsize=14)
    ax.set_ylabel("Drawdown (%)")
    ax.set_xlabel("Date")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    buf.seek(0)
    result = buf.read()
    plt.close(fig)
    return result


def _plot_decomposition(
    dates: tuple, entries: tuple,
) -> bytes | None:
    """Plot return decomposition: stacked area of price + dividend."""
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    price_vals = [e.get("price_only", 0) * 100 for e in entries]
    div_vals = [e.get("dividend_only", 0) * 100 for e in entries]
    total_vals = [e.get("total", 0) * 100 for e in entries]

    if all(v == 0 for v in price_vals) and all(v == 0 for v in div_vals):
        return None

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.fill_between(dates, 0, price_vals,
                    alpha=0.5, color=_COLORS["price_fill"],
                    label="Price Return")
    ax.fill_between(dates, price_vals,
                    [p + d for p, d in zip(price_vals, div_vals)],
                    alpha=0.5, color=_COLORS["dividend_fill"],
                    label="Dividend Return")
    ax.plot(dates, total_vals,
            color=_COLORS["total_line"], linewidth=1.5,
            label="Total Return")

    ax.set_title("Portfolio Return Decomposition", fontsize=14)
    ax.set_ylabel("Cumulative Return (%)")
    ax.set_xlabel("Date")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color="grey", linewidth=0.8, linestyle="-")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    buf.seek(0)
    result = buf.read()
    plt.close(fig)
    return result


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _write_csv(
    zf: zipfile.ZipFile, path: str, rows: list[dict],
) -> None:
    """Write a list of dicts as a CSV file inside the ZIP."""
    if not rows:
        return
    # Collect all keys preserving order of first appearance
    keys: list[str] = []
    seen_keys: set[str] = set()
    for row in rows:
        for k in row:
            if k not in seen_keys:
                seen_keys.add(k)
                keys.append(k)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=keys, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    zf.writestr(path, buf.getvalue())


def _sanitize_path(name: str, max_len: int = 100) -> str:
    """Replace characters unsafe for ZIP paths with underscores."""
    unsafe = set(r'/\:*?"<>|')
    result = "".join("_" if c in unsafe else c for c in name)
    if len(result) > max_len:
        result = result[:max_len]
    return result


def _fmt_pct(value: Any) -> str:
    """Format a value as a percentage string, or ``"N/A"`` if None."""
    if value is None:
        return "N/A"
    try:
        return f"{float(value):+.2%}"
    except (ValueError, TypeError):
        return str(value)


def _fmt_cell(value: Any, width: int) -> str:
    """Format a single table cell with left‑alignment to *width*."""
    if value is None:
        s = "N/A"
    elif isinstance(value, float):
        # Use 2 decimal places for floats
        s = f"{value:.2f}"
    else:
        s = str(value)
    return f"{s:<{width}}"


def _column_widths(rows: list[dict], columns: list[str]) -> dict[str, int]:
    """Compute minimum column widths from data and headers."""
    widths: dict[str, int] = {}
    for c in columns:
        widths[c] = len(c)
    for row in rows:
        for c in columns:
            val = row.get(c)
            cell = _fmt_cell(val, 0).strip()
            widths[c] = max(widths[c], len(cell))
    return widths


def _flat_metrics(metrics: dict) -> dict:
    """Flatten metrics dict to a single-level dict for CSV export."""
    flat: dict[str, Any] = {}
    for k, v in metrics.items():
        if isinstance(v, dict):
            for sub_k, sub_v in v.items():
                flat[f"{k}_{sub_k}"] = sub_v
        elif isinstance(v, list):
            flat[k] = ", ".join(str(x) for x in v)
        else:
            flat[k] = v
    return flat


def _join_list(value: Any) -> str:
    """Join a list with commas, or return the string as‑is."""
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    return str(value) if value else ""


def _fmt_criterion(c: dict) -> str:
    """Format a single screening criterion for display in the summary.

    Handles the various comparison modes used by the screening engine.
    """
    mode = c.get("comparison_mode", "fixed")
    col = c.get("column", "") or ""
    tbl = c.get("table", "") or ""
    op = c.get("operator", "") or ""
    val = c.get("value")
    field_type = c.get("field_type", "num")

    # Build a qualified column name
    if tbl and col:
        qualified = f"{tbl}.{col}"
    elif col:
        qualified = col
    else:
        qualified = "?"

    if mode == "full_expression":
        left = c.get("left_expression") or c.get("left_side")
        if left:
            if isinstance(left, list):
                left = " ".join(
                    t.get("value", t.get("column", "?"))
                    for t in left if isinstance(t, dict)
                )
            return f"{left}"
        return f"expression: {qualified}"

    if mode == "expression":
        right = c.get("right_side")
        if right and isinstance(right, list):
            right_str = " ".join(
                t.get("value", t.get("column", "?"))
                for t in right if isinstance(t, dict)
            )
            return f"{qualified} {op} ({right_str})"
        return f"{qualified} {op} {val}"

    if mode == "column":
        cmp_tbl = c.get("compare_table", "") or ""
        cmp_col = c.get("compare_column", "") or ""
        offset = c.get("offset")
        cmp_qualified = f"{cmp_tbl}.{cmp_col}" if cmp_tbl else cmp_col
        s = f"{qualified} {op} {cmp_qualified}"
        if offset:
            s += f" {'+' if offset > 0 else ''}{offset}"
        return s

    if mode == "between":
        v2 = c.get("value2")
        return f"{qualified} BETWEEN {val} AND {v2}"

    if mode == "in":
        vals = c.get("values")
        if isinstance(vals, list):
            return f"{qualified} IN ({', '.join(str(v) for v in vals)})"
        return f"{qualified} IN ({val})"

    if mode == "like":
        return f"{qualified} LIKE '{val}'"

    if mode == "stock_price":
        left_expr = c.get("left_expression", "") or ""
        return f"{left_expr or qualified} {op} Stock_Prices.Price"

    # Default: fixed / simple value comparison
    if field_type == "percent" and isinstance(val, (int, float)):
        val = f"{float(val) * 100:.1f}%"
    return f"{qualified} {op} {val}"
