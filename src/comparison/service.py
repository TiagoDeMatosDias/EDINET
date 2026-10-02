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
) -> tuple[float | None, str | None]:
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


def extract_latest_statement_metrics(history: dict[str, Any]) -> tuple[dict[str, float | None], str | None]:
    """Extract comparison fundamentals from the latest populated history values."""
    periods = [str(period) for period in history.get("periods", [])]
    income = _statement_rows(history, ("IncomeStatement", "income_statement"))
    balance = _statement_rows(history, ("BalanceSheet", "balance_sheet"))
    shares = _statement_rows(history, ("ShareMetrics", "share_metrics"))
    definitions = {
        "Revenue": (income, ("Net sales", "Net sales (revenue)", "Total revenue")),
        "CostOfSales": (income, ("Cost of sales",)),
        "OperatingIncome": (income, ("Operating income - Operating profit (loss)", "Operating income")),
        "NetIncome": (income, ("Profit (loss)", "Net income (loss)", "Net income")),
        "TotalAssets": (balance, ("Assets", "Total assets")),
        "TotalEquity": (balance, ("Shareholders' equity", "Shareholders equity", "Net assets")),
        "TotalLiabilities": (balance, ("Liabilities", "Total liabilities")),
        "CurrentAssets": (balance, ("Current assets",)),
        "CurrentLiabilities": (balance, ("Current liabilities",)),
        "SharesOutstanding": (
            shares,
            (
                "Number of issued shares as of filing date",
                "Total number of issued shares",
                "Number of issued shares as of fiscal year end",
            ),
        ),
    }
    metrics: dict[str, float | None] = {}
    metric_periods: list[str] = []
    for metric, (rows, labels) in definitions.items():
        value, period = _latest_statement_value(rows, periods, labels)
        metrics[metric] = value
        if period:
            metric_periods.append(period)
    return metrics, max(metric_periods) if metric_periods else None


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
