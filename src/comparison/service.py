"""Calculations used by the company-comparison workspace."""

from __future__ import annotations

import re
from typing import Any

# The one description of each standard metric, in display order. ``group``
# and ``label`` drive both the comparison matrix and the analysis snapshot;
# ``direction`` says which way is favourable when companies are compared (size
# metrics and payout have none); ``format`` is ``money`` or ``percent`` (stored
# as a fraction), and money is in the ``price`` or ``reporting`` currency;
# ``description`` says how the value is calculated, for tooltips.
_PRICE_MONEY = {"format": "money", "currency": "price"}
_REPORTED_MONEY = {"format": "money", "currency": "reporting"}
_PERCENT = {"format": "percent"}

METRIC_DEFINITIONS: dict[str, dict[str, str]] = {
    "LatestPrice": {
        "label": "Price", "group": "Market", **_PRICE_MONEY,
        "description": "Latest stored closing price, adjusted for share splits.",
    },
    "MarketCap": {
        "label": "Market cap", "group": "Market", **_PRICE_MONEY,
        "description": "Latest price × shares issued as of the latest annual filing date.",
    },
    "PERatio": {
        "label": "P/E", "group": "Valuation", "direction": "lower",
        "description": "Price-to-earnings: latest price ÷ basic earnings per share from the latest annual filing.",
    },
    "PriceToBook": {
        "label": "P/B", "group": "Valuation", "direction": "lower",
        "description": "Price-to-book: latest price ÷ net assets per share from the latest annual filing.",
    },
    "PriceToSales": {
        "label": "P/S", "group": "Valuation", "direction": "lower",
        "description": "Price-to-sales: latest price ÷ sales per share from the latest annual filing.",
    },
    "EnterpriseValueToSales": {
        "label": "EV/Sales", "group": "Valuation", "direction": "lower",
        "description": "Enterprise value ÷ annual revenue, from the stored valuation snapshot.",
    },
    "DividendsYield": {
        "label": "Dividend yield", "group": "Valuation", "direction": "higher", **_PERCENT,
        "description": "Annual dividend paid per share ÷ latest price.",
    },
    "PayoutRatio": {
        "label": "Payout ratio", "group": "Valuation", **_PERCENT,
        "description": "Share of earnings paid out as dividends: dividend per share ÷ basic earnings per share.",
    },
    "ReturnOnEquity": {
        "label": "ROE", "group": "Quality", "direction": "higher", **_PERCENT,
        "description": "Return on equity: net income ÷ shareholders' equity, averaged over the last three fiscal years.",
    },
    "ReturnOnAssets": {
        "label": "ROA", "group": "Quality", "direction": "higher", **_PERCENT,
        "description": "Return on assets: net income ÷ total assets, averaged over the last three fiscal years.",
    },
    "DebtToEquity": {
        "label": "Debt/equity", "group": "Quality", "direction": "lower",
        "description": "Leverage: total liabilities ÷ shareholders' equity. Below 1 means equity funds more than creditors.",
    },
    "CurrentRatio": {
        "label": "Current ratio", "group": "Quality", "direction": "higher",
        "description": "Liquidity: current assets ÷ current liabilities. Above 1 means short-term assets cover short-term obligations.",
    },
    "GrossMargin": {
        "label": "Gross margin", "group": "Quality", "direction": "higher", **_PERCENT,
        "description": "(Revenue − cost of sales) ÷ revenue.",
    },
    "OperatingMargin": {
        "label": "Operating margin", "group": "Quality", "direction": "higher", **_PERCENT,
        "description": "Operating income ÷ revenue: profit from the core business before interest and taxes.",
    },
    "NetMargin": {
        "label": "Net margin", "group": "Quality", "direction": "higher", **_PERCENT,
        "description": "Net income ÷ revenue.",
    },
    "Revenue": {
        "label": "Revenue", "group": "Income", **_REPORTED_MONEY,
        "description": "Net sales (or operating revenue) for the latest fiscal year.",
    },
    "OperatingIncome": {
        "label": "Operating income", "group": "Income", **_REPORTED_MONEY,
        "description": "Operating profit for the latest fiscal year.",
    },
    "NetIncome": {
        "label": "Net income", "group": "Income", **_REPORTED_MONEY,
        "description": "Profit for the latest fiscal year.",
    },
    "TotalAssets": {
        "label": "Total assets", "group": "Balance sheet", **_REPORTED_MONEY,
        "description": "Total assets at the latest fiscal year end.",
    },
    "TotalEquity": {
        "label": "Shareholders' equity", "group": "Balance sheet", **_REPORTED_MONEY,
        "description": "Shareholders' equity at the latest fiscal year end.",
    },
    "SharesOutstanding": {
        "label": "Shares outstanding", "group": "Balance sheet",
        "description": "Shares issued as of the latest annual filing date, adjusted for share splits.",
    },
}

DEFAULT_METRICS = list(METRIC_DEFINITIONS)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first(value: Any, fallback: Any) -> Any:
    return value if value is not None else fallback


def _normalise_label(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _statement_rows(history: dict[str, Any], names: tuple[str, ...]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name in names:
        candidate = history.get(name)
        if isinstance(candidate, list):
            rows.extend(row for row in candidate if isinstance(row, dict))
    return rows


def _latest_statement_value(
    rows: list[dict[str, Any]],
    periods: list[str],
    labels: tuple[str, ...],
    largest: bool = False,
) -> tuple[float | None, str | None]:
    if largest:
        series = _revenue_series(rows, len(periods), labels)
        index = next((index for index in range(len(series) - 1, -1, -1) if series[index] is not None), None)
        return (series[index], periods[index]) if index is not None else (None, None)
    wanted = {_normalise_label(label) for label in labels}
    for row in rows:
        row_label = _normalise_label(row.get("field") or row.get("record_field") or row.get("metric"))
        if row_label not in wanted:
            continue
        values = row.get("values")
        if not isinstance(values, list):
            continue
        for index in range(min(len(values), len(periods)) - 1, -1, -1):
            value = _number(values[index])
            if value is not None:
                return value, periods[index]
    return None, None


# Statement lines read for each fundamental, by source table and label, shared by
# the snapshot (latest value) and the trend charts (every year).
# Revenue is the largest of its lines: a parent-only holding company files a
# small net sales line inside a larger operating revenue, a railway files
# operating revenue alone. Gross margin stays on net sales where there is
# one, as its cost of sales goes with them.
_STATEMENT_LABELS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "Revenue": (
        ("IncomeStatement", "income_statement"),
        ("Net sales", "Net sales (revenue)", "Total revenue", "Operating Revenue - Operating revenue", "Operating revenue"),
    ),
    "NetSales": (("IncomeStatement", "income_statement"), ("Net sales", "Net sales (revenue)")),
    "CostOfSales": (("IncomeStatement", "income_statement"), ("Cost of sales",)),
    "OperatingIncome": (("IncomeStatement", "income_statement"), ("Operating income - Operating profit (loss)", "Operating income")),
    "NetIncome": (("IncomeStatement", "income_statement"), ("Profit (loss)", "Net income (loss)", "Net income")),
    "TotalAssets": (("BalanceSheet", "balance_sheet"), ("Assets", "Total assets")),
    "TotalEquity": (("BalanceSheet", "balance_sheet"), ("Shareholders' equity", "Shareholders equity", "Net assets")),
    "TotalLiabilities": (("BalanceSheet", "balance_sheet"), ("Liabilities", "Total liabilities")),
    "CurrentAssets": (("BalanceSheet", "balance_sheet"), ("Current assets",)),
    "CurrentLiabilities": (("BalanceSheet", "balance_sheet"), ("Current liabilities",)),
}


_LARGEST_LINES = {"Revenue"}
# A bank's or insurer's revenue is its ordinary revenue (経常収益); the line
# counts as revenue only beside ordinary expenses, as elsewhere the same label
# can hold ordinary profit.
_ORDINARY_REVENUE = ("Ordinary Income - Ordinary income",)
_ORDINARY_EXPENSES = ("Ordinary Expenses - Operating expenses", "Ordinary expenses")


def _revenue_series(rows: list[dict[str, Any]], count: int, labels: tuple[str, ...]) -> list[float | None]:
    """Each year's largest revenue line, ordinary revenue included where it is one."""
    revenue = _row_series(rows, count, labels, largest=True)
    ordinary = _row_series(rows, count, _ORDINARY_REVENUE)
    expenses = _row_series(rows, count, _ORDINARY_EXPENSES)
    series: list[float | None] = []
    for line, banking, costs in zip(revenue, ordinary, expenses, strict=True):
        candidates = [value for value in (line, banking if costs is not None else None) if value is not None]
        series.append(max(candidates) if candidates else None)
    return series


def extract_latest_statement_metrics(history: dict[str, Any]) -> tuple[dict[str, float | None], str | None]:
    """Extract comparison fundamentals from the latest populated history values."""
    periods = [str(period) for period in history.get("periods", [])]
    lines = {
        **_STATEMENT_LABELS,
        "SharesOutstanding": (
            ("ShareMetrics", "share_metrics"),
            (
                "Number of issued shares as of filing date",
                "Total number of issued shares",
                "Number of issued shares as of fiscal year end",
            ),
        ),
    }
    metrics: dict[str, float | None] = {}
    metric_periods: list[str] = []
    for metric, (sources, labels) in lines.items():
        value, period = _latest_statement_value(_statement_rows(history, sources), periods, labels, largest=metric in _LARGEST_LINES)
        metrics[metric] = value
        if period:
            metric_periods.append(period)
    return metrics, max(metric_periods) if metric_periods else None


# Year-by-year series for comparison charts. Keys match the snapshot metrics
# where the meaning is the same; ROE here is each year's own ratio.
TREND_METRICS: dict[str, dict[str, str]] = {
    "Revenue": {**METRIC_DEFINITIONS["Revenue"], "description": "Net sales (or operating revenue) for each fiscal year."},
    "OperatingIncome": {**METRIC_DEFINITIONS["OperatingIncome"], "description": "Operating profit for each fiscal year."},
    "NetIncome": {**METRIC_DEFINITIONS["NetIncome"], "description": "Profit for each fiscal year."},
    "GrossMargin": METRIC_DEFINITIONS["GrossMargin"],
    "OperatingMargin": METRIC_DEFINITIONS["OperatingMargin"],
    "NetMargin": METRIC_DEFINITIONS["NetMargin"],
    "ReturnOnEquity": {
        **METRIC_DEFINITIONS["ReturnOnEquity"],
        "description": "Net income ÷ shareholders' equity at each fiscal year end (the snapshot averages three years).",
    },
    "DebtToEquity": METRIC_DEFINITIONS["DebtToEquity"],
    "CurrentRatio": METRIC_DEFINITIONS["CurrentRatio"],
    "TotalAssets": {**METRIC_DEFINITIONS["TotalAssets"], "description": "Total assets at each fiscal year end."},
    "TotalEquity": {**METRIC_DEFINITIONS["TotalEquity"], "description": "Shareholders' equity at each fiscal year end."},
}


def _row_series(rows: list[dict[str, Any]], count: int, labels: tuple[str, ...], largest: bool = False) -> list[float | None]:
    """Per-period values of the first matching row that has one (or the largest), as the snapshot reads the latest."""
    wanted = {_normalise_label(label) for label in labels}
    matching = [
        row["values"] for row in rows
        if _normalise_label(row.get("field") or row.get("record_field") or row.get("metric")) in wanted
        and isinstance(row.get("values"), list)
    ]
    series: list[float | None] = []
    for index in range(count):
        if largest:
            present = [value for values in matching if index < len(values) and (value := _number(values[index])) is not None]
            series.append(max(present) if present else None)
            continue
        series.append(next(
            (value for values in matching if index < len(values) and (value := _number(values[index])) is not None),
            None,
        ))
    return series


def _ratio(numerator: float | None, denominator: float | None, positive: bool = False) -> float | None:
    if numerator is None or denominator is None or denominator == 0 or (positive and denominator < 0):
        return None
    return numerator / denominator


def statement_series(history: dict[str, Any], metric_refs: list[str] | None = None) -> dict[str, Any]:
    """Year-by-year values of the trend metrics and any ``Table.Column`` refs."""
    periods = [str(period) for period in history.get("periods", [])]
    count = len(periods)
    raw = {
        key: _revenue_series(_statement_rows(history, sources), count, labels) if key in _LARGEST_LINES
        else _row_series(_statement_rows(history, sources), count, labels)
        for key, (sources, labels) in _STATEMENT_LABELS.items()
    }

    def each(fn) -> list[float | None]:
        return [fn(index) for index in range(count)]

    revenue, net_income, equity = raw["Revenue"], raw["NetIncome"], raw["TotalEquity"]
    sales = [net if net is not None else total for net, total in zip(raw["NetSales"], revenue, strict=True)]
    series: dict[str, list[float | None]] = {
        "Revenue": revenue,
        "OperatingIncome": raw["OperatingIncome"],
        "NetIncome": net_income,
        "GrossMargin": each(lambda i: _ratio(
            sales[i] - raw["CostOfSales"][i] if sales[i] is not None and raw["CostOfSales"][i] is not None else None,
            sales[i],
        )),
        "OperatingMargin": each(lambda i: _ratio(raw["OperatingIncome"][i], revenue[i])),
        "NetMargin": each(lambda i: _ratio(net_income[i], revenue[i])),
        "ReturnOnEquity": each(lambda i: _ratio(net_income[i], equity[i], positive=True)),
        "DebtToEquity": each(lambda i: _ratio(raw["TotalLiabilities"][i], equity[i], positive=True)),
        "CurrentRatio": each(lambda i: _ratio(raw["CurrentAssets"][i], raw["CurrentLiabilities"][i], positive=True)),
        "TotalAssets": raw["TotalAssets"],
        "TotalEquity": equity,
    }
    for metric_ref in metric_refs or []:
        table, separator, column = metric_ref.partition(".")
        if separator and table and column:
            series[metric_ref] = _row_series(_statement_rows(history, (table,)), count, (column,))
    return {"periods": periods, "series": series}


def extract_latest_table_metrics(
    history: dict[str, Any],
    metric_refs: list[str],
) -> tuple[dict[str, float | None], str | None]:
    """Extract latest populated values for arbitrary ``Table.Column`` refs."""
    periods = [str(period) for period in history.get("periods", [])]
    values: dict[str, float | None] = {}
    metric_periods: list[str] = []
    for metric_ref in metric_refs:
        table, separator, column = metric_ref.partition(".")
        if not separator or not table or not column:
            values[metric_ref] = None
            continue
        rows = _statement_rows(history, (table,))
        value, period = _latest_statement_value(rows, periods, (column,))
        values[metric_ref] = value
        if period:
            metric_periods.append(period)
    return values, max(metric_periods) if metric_periods else None


def flatten_overview(overview: dict[str, Any]) -> dict[str, float | None]:
    """Map the nested analysis response to the comparison metric vocabulary."""
    analysis = overview.get("metrics") or {}
    fundamentals = overview.get("fundamentals_latest", {})
    valuation = overview.get("valuation_latest", {})
    quality = overview.get("quality_latest", {})
    market = overview.get("market", {})
    share = overview.get("per_share_latest", {})
    def value(key: str, *fallbacks: Any) -> Any:
        if analysis.get(key) is not None:
            return analysis.get(key)
        return next((fallback for fallback in fallbacks if fallback is not None), None)

    metrics = {
        "LatestPrice": _number(value("LatestPrice", market.get("latest_price"))),
        "MarketCap": _number(value("MarketCap", valuation.get("MarketCap"))),
        "PERatio": _number(value("PERatio", valuation.get("PERatio"))),
        "PriceToBook": _number(value("PriceToBook", valuation.get("PriceToBook"))),
        "PriceToSales": _number(value("PriceToSales", valuation.get("PriceToSales"))),
        "EnterpriseValueToSales": _number(value("EnterpriseValueToSales", valuation.get("EnterpriseValueToSales"))),
        "DividendsYield": _number(value("DividendsYield", valuation.get("DividendsYield"))),
        "ReturnOnEquity": _number(value("ReturnOnEquity", quality.get("ReturnOnEquity"), valuation.get("ReturnOnEquity"))),
        "DebtToEquity": _number(value("DebtToEquity", quality.get("DebtToEquity"), valuation.get("DebtToEquity"))),
        "CurrentRatio": _number(value("CurrentRatio", quality.get("CurrentRatio"), valuation.get("CurrentRatio"))),
        "GrossMargin": _number(value("GrossMargin", quality.get("GrossMargin"), valuation.get("GrossMargin"))),
        "OperatingMargin": _number(value("OperatingMargin", valuation.get("OperatingMargin"))),
        "NetMargin": _number(value("NetMargin", valuation.get("NetProfitMargin"))),
        "PayoutRatio": _number(value("PayoutRatio")),
        "ReturnOnAssets": _number(value("ReturnOnAssets")),
        "Revenue": _number(value("Revenue", fundamentals.get("Revenue"))),
        "OperatingIncome": _number(value("OperatingIncome", fundamentals.get("OperatingIncome"))),
        "NetIncome": _number(value("NetIncome", fundamentals.get("NetIncome"))),
        "TotalAssets": _number(value("TotalAssets", fundamentals.get("TotalAssets"))),
        "TotalEquity": _number(value("TotalEquity", fundamentals.get("ShareholdersEquity"))),
        "SharesOutstanding": _number(value("SharesOutstanding", fundamentals.get("SharesOutstanding"))),
    }
    eps = _number(share.get("EPS"))
    dividends = _number(share.get("Dividends"))
    if (
        metrics["PayoutRatio"] is None
        and eps is not None
        and eps != 0
        and dividends is not None
    ):
        metrics["PayoutRatio"] = dividends / eps
    return metrics


def common_size_income(metrics: dict[str, float | None]) -> dict[str, float | None]:
    """Scale income-statement metrics by revenue where available."""
    revenue = metrics.get("Revenue")
    if not revenue:
        return {key: None for key in ("Revenue", "OperatingIncome", "NetIncome")}
    result: dict[str, float | None] = {}
    for key in ("Revenue", "OperatingIncome", "NetIncome"):
        value = metrics.get(key)
        result[key] = value / revenue if value is not None else None
    return result


def common_size_balance(metrics: dict[str, float | None]) -> dict[str, float | None]:
    """Scale balance-sheet metrics by total assets where available."""
    assets = metrics.get("TotalAssets")
    if not assets:
        return {key: None for key in ("TotalAssets", "TotalEquity")}
    result: dict[str, float | None] = {}
    for key in ("TotalAssets", "TotalEquity"):
        value = metrics.get(key)
        result[key] = value / assets if value is not None else None
    return result


def growth_rate(current: float | None, previous: float | None) -> float | None:
    """Year-over-year growth rate; None when either value is missing or zero."""
    if current is None or previous is None or previous == 0:
        return None
    return (current - previous) / previous


def growth_matrix(
    current_metrics: dict[str, dict[str, float | None]],
    previous_metrics: dict[str, dict[str, float | None]],
) -> dict[str, dict[str, float | None]]:
    """Compute growth rates per company per metric."""
    result: dict[str, dict[str, float | None]] = {}
    all_codes = set(current_metrics.keys()) | set(previous_metrics.keys())
    for code in all_codes:
        current = current_metrics.get(code, {})
        previous = previous_metrics.get(code, {})
        metric_names = set(current.keys()) | set(previous.keys())
        result[code] = {metric: growth_rate(current.get(metric), previous.get(metric)) for metric in metric_names}
    return result


def peer_percentile(
    company_code: str,
    metric: str,
    metrics_map: dict[str, dict[str, float | None]],
) -> float | None:
    """Compute a midpoint-tie percentile rank within the selected companies."""
    values = []
    company_value = None
    for code, company_metrics in metrics_map.items():
        value = company_metrics.get(metric)
        if value is not None:
            values.append(value)
            if code == company_code:
                company_value = value
    if company_value is None or len(values) < 2:
        return None
    sorted_values = sorted(values)
    below = sum(1 for value in sorted_values if value < company_value)
    ties = sum(1 for value in sorted_values if value == company_value) - 1
    return (below + 0.5 * ties) / len(sorted_values)


def normalize_companies(
    company_codes: list[str],
    overviews: dict[str, dict[str, Any]],
    metrics: list[str] | None = None,
    metric_values: dict[str, dict[str, float | None]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Produce metric, common-size, and peer-rank data for each company."""
    selected = [
        metric
        for metric in (metrics or DEFAULT_METRICS)
        if metric in METRIC_DEFINITIONS or "." in metric
    ]
    metrics_map = {
        code: {
            key: (
                flatten_overview(overviews.get(code, {})).get(key)
                if key in METRIC_DEFINITIONS
                else (metric_values or {}).get(code, {}).get(key)
            )
            for key in selected
        }
        for code in company_codes
    }
    result: dict[str, dict[str, Any]] = {}
    for code in company_codes:
        company_metrics = metrics_map[code]
        result[code] = {
            "metrics": company_metrics,
            "common_size_income": common_size_income(flatten_overview(overviews.get(code, {}))),
            "common_size_balance": common_size_balance(flatten_overview(overviews.get(code, {}))),
            "percentiles": {metric: peer_percentile(code, metric, metrics_map) for metric in selected},
            "company_name": overviews.get(code, {}).get("company", {}).get("company_name"),
        }
    return result
