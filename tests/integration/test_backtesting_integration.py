"""End-to-end backtesting API tests against the generated market database."""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import src.backtesting.api as backtesting_api
from src.web_app.server import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def isolated_backtest_artifacts(monkeypatch, tmp_path: Path) -> Path:
    root = tmp_path / "backtests"
    monkeypatch.setattr(backtesting_api, "_BACKTEST_ROOT", root)
    return root


def _portfolio() -> dict[str, dict[str, float | str]]:
    return {
        "AAA": {"mode": "weight", "value": 0.6},
        "BBB": {"mode": "weight", "value": 0.4},
    }


def _load_saved_result(root: Path, response: dict) -> dict:
    result_path = root / response["id"] / "result.json"
    assert result_path.is_file()
    return json.loads(result_path.read_text(encoding="utf-8"))


def _assert_finite(value, path: str = "result") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            _assert_finite(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_finite(item, f"{path}[{index}]")
    elif isinstance(value, float):
        assert math.isfinite(value), f"Non-finite number at {path}: {value}"


def test_database_metadata_endpoints_use_generated_database() -> None:
    database = client.get("/api/backtesting/db-path")
    tickers = client.get("/api/backtesting/available-tickers")
    currencies = client.get("/api/backtesting/base-currencies")

    assert database.status_code == 200
    assert database.json() == {"db_path": "default"}
    assert tickers.status_code == 200
    assert tickers.json()["tickers"] == ["AAA", "BBB", "BENCH", "SPIN"]
    assert currencies.status_code == 200
    assert {item["code"] for item in currencies.json()["currencies"]} == {
        "EUR",
        "JPY",
        "USD",
    }


def test_manual_backtest_returns_and_persists_complete_result(
    isolated_backtest_artifacts: Path,
) -> None:
    response = client.post(
        "/api/backtesting/run",
        json={
            "portfolio": _portfolio(),
            "start_date": "2020-01-01",
            "end_date": "2023-01-01",
            "initial_capital": 50_000,
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["path"] == body["id"]
    assert body["summary"]["initial_capital"] == 50_000
    assert body["summary"]["warnings"] == []
    assert body["chart_data"]["cumulative"]
    assert body["chart_data"]["drawdown"]
    assert {row["Ticker"] for row in body["per_company"]} == {"AAA", "BBB"}

    saved = _load_saved_result(isolated_backtest_artifacts, body)
    assert saved["metrics"]["initial_capital"] == 50_000
    assert saved["yearly_returns"]
    assert saved["dividends_by_year"]
    _assert_finite(saved)


def test_benchmark_and_currency_conversion_are_applied(
    isolated_backtest_artifacts: Path,
) -> None:
    response = client.post(
        "/api/backtesting/run",
        json={
            "portfolio": _portfolio(),
            "start_date": "2020-01-01",
            "end_date": "2023-01-01",
            "benchmark_ticker": "BENCH",
            "base_currency": "USD",
            "risk_free_rate": 0.02,
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    saved = _load_saved_result(isolated_backtest_artifacts, body)
    metrics = saved["metrics"]
    assert metrics["base_currency"] == "USD"
    assert metrics["benchmark_total_return"] is not None
    assert metrics["benchmark_annualized_return"] is not None
    assert metrics["information_ratio"] is not None
    assert all(
        point["benchmark"] is not None
        for point in body["chart_data"]["cumulative"]
    )


@pytest.mark.parametrize(
    ("payload", "expected_status"),
    [
        ({"portfolio": {}}, 422),
        (
            {
                "portfolio": {},
                "start_date": "2020-01-01",
                "end_date": "2021-01-01",
            },
            400,
        ),
        (
            {
                "portfolio": {"AAA": {"mode": "weight", "value": 1}},
                "start_date": "2023-01-01",
                "end_date": "2020-01-01",
            },
            400,
        ),
        (
            {
                "portfolio": {"AAA": {"mode": "weight", "value": 1}},
                "start_date": "2020-01-01",
                "end_date": "2021-01-01",
                "base_currency": "INVALID",
            },
            400,
        ),
        (
            {
                "portfolio": {"AAA": {"mode": "weight", "value": 1}},
                "start_date": "2020-01-01",
                "end_date": "2021-01-01",
                "benchmark_mode": "invalid",
            },
            422,
        ),
    ],
)
def test_manual_backtest_validation(payload: dict, expected_status: int) -> None:
    response = client.post("/api/backtesting/run", json=payload)
    assert response.status_code == expected_status


def test_csv_backtest_runs_each_requested_duration(
    isolated_backtest_artifacts: Path,
) -> None:
    csv_content = "\n".join(
        [
            "# Benchmark: BENCH",
            "Year,Tickers,Type,Amount",
            "2020,AAA,weight,0.6",
            "2020,BBB,weight,0.4",
        ]
    )
    response = client.post(
        "/api/backtesting/run-from-csv",
        json={"csv_content": csv_content, "durations": ["1yr", "2yr"]},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["aggregate"]["total_runs"] == 2
    assert body["aggregate"]["successful"] == 2
    assert body["aggregate"]["failed"] == 0
    saved = _load_saved_result(isolated_backtest_artifacts, body)
    assert [(row["year"], row["duration"]) for row in saved["results"]] == [
        ("2020", "1yr"),
        ("2020", "2yr"),
    ]
    assert all(
        row["metrics"]["benchmark_total_return"] is not None
        for row in saved["results"]
    )


@pytest.mark.parametrize(
    ("payload", "expected_status"),
    [
        ({}, 422),
        ({"csv_content": ""}, 400),
        ({"csv_content": "Year,Amount\n2020,1"}, 400),
    ],
)
def test_csv_backtest_validation(payload: dict, expected_status: int) -> None:
    response = client.post("/api/backtesting/run-from-csv", json=payload)
    assert response.status_code == expected_status


def test_csv_header_without_rows_returns_empty_result(
    isolated_backtest_artifacts: Path,
) -> None:
    response = client.post(
        "/api/backtesting/run-from-csv",
        json={"csv_content": "Year,Tickers,Type,Amount"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["aggregate"]["total_runs"] == 0
    assert _load_saved_result(isolated_backtest_artifacts, body)["results"] == []


def test_unknown_ticker_returns_warning_without_non_finite_values(
    isolated_backtest_artifacts: Path,
) -> None:
    response = client.post(
        "/api/backtesting/run",
        json={
            "portfolio": {
                "AAA": {"mode": "weight", "value": 0.5},
                "MISSING": {"mode": "weight", "value": 0.5},
            },
            "start_date": "2020-01-01",
            "end_date": "2021-01-01",
        },
    )

    assert response.status_code == 200
    saved = _load_saved_result(isolated_backtest_artifacts, response.json())
    assert any("MISSING" in warning for warning in saved["warnings"])
    assert all(isinstance(warning, str) for warning in saved["warnings"])
    _assert_finite(saved)
