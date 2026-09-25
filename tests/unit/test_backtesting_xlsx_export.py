"""The rolling backtest XLSX export lays out fixed input data predictably."""

from __future__ import annotations

import io

import pytest
from fastapi.testclient import TestClient

from src.web_app.server import app

openpyxl = pytest.importorskip("openpyxl")

_COMPANY_KEYS = (
    "total_return", "price_return", "dividend_return", "weight",
    "weighted_price", "weighted_dividend", "weighted_total",
    "start_price", "end_price",
)
_CAPITAL_KEYS = ("capital_invested", "shares_purchased", "dividends_received", "market_value")


def _company(ticker: str, base: float, *, capital: bool) -> dict:
    row = {"Ticker": ticker}
    # Multiples of 1/8 survive the XLSX float round trip exactly.
    row.update({key: base + index / 8 for index, key in enumerate(_COMPANY_KEYS)})
    if capital:
        row.update({key: 1000 + index for index, key in enumerate(_CAPITAL_KEYS)})
    return row


def _payload(*, capital: bool) -> dict:
    return {
        "rolling_result": {
            "aggregate": {"total_runs": 1, "successful": 1, "failed": 0, "periods": 1},
            "config": {"cadence": "yearly", "durations": ["1y"]},
            "results": [
                {
                    "period": "2024-01-31",
                    "ticker_count": 2,
                    "tickers": ["AAA", "BBB"],
                    "backtests": {
                        "equal": {
                            "1y": {
                                "metrics": {
                                    "total_return": 0.1,
                                    "annualized_return": 0.1,
                                    "sharpe_ratio": 1.2,
                                    "max_drawdown": -0.05,
                                    "volatility": 0.15,
                                    "start_date": "2024-01-31",
                                    "end_date": "2025-01-31",
                                },
                                "per_company": [
                                    _company("AAA", 0.5, capital=capital),
                                    _company("BBB", 0.25, capital=capital),
                                ],
                            }
                        }
                    },
                }
            ],
        }
    }


@pytest.mark.parametrize("capital", [False, True])
def test_per_company_breakdown_columns(capital):
    response = TestClient(app).post(
        "/api/backtesting/export-rolling-xlsx", json=_payload(capital=capital)
    )
    assert response.status_code == 200
    sheet = openpyxl.load_workbook(io.BytesIO(response.content))["20240131"]

    # Rows 4-6 hold the weighting block; the per-company table follows.
    assert sheet.cell(row=8, column=1).value == "1y — Per-Company Breakdown"
    header = [cell.value for cell in sheet[9] if cell.value is not None]
    assert header[0] == "Ticker"
    assert len(header) == (14 if capital else 10)

    keys = _COMPANY_KEYS + (_CAPITAL_KEYS if capital else ())
    for offset, (ticker, base) in enumerate((("AAA", 0.5), ("BBB", 0.25))):
        row = 10 + offset
        values = [sheet.cell(row=row, column=col).value for col in range(1, len(keys) + 2)]
        expected = _company(ticker, base, capital=capital)
        assert values == [ticker] + [expected[key] for key in keys]
        assert sheet.cell(row=row, column=len(keys) + 1).border.left.style == "thin"
        assert sheet.cell(row=row, column=2).number_format == "0.00%"
