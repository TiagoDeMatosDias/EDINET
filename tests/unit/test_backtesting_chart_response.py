from fastapi.testclient import TestClient

from src.backtesting import api as backtesting_api
from src.web_app.server import app


def test_run_response_includes_chart_and_breakdown_data(monkeypatch, tmp_path) -> None:
    result = {
        "metrics": {"total_return": 0.2, "annualized_return": 0.1},
        "chart_data": {
            "cumulative": [{"date": "2025-01-01", "portfolio": 0.2}],
            "drawdown": [{"date": "2025-01-01", "portfolio": -0.1}],
            "decomposition": [{"date": "2025-01-01", "price_only": 0.15}],
        },
        "per_company": [{"Ticker": "TEST", "total_return": 0.2}],
        "yearly_returns": [{"year": 2025, "return": 0.2}],
        "dividends_by_year": [{"year": 2025, "TEST": 10.0}],
        "daily": [],
        "warnings": [],
    }
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(backtesting_api, "_resolve_db", lambda _path="": "test.db")
    monkeypatch.setattr(backtesting_api._bt, "run_backtest_web", lambda **_kwargs: result)
    monkeypatch.setattr(backtesting_api, "build_single_backtest_zip", lambda _result: b"zip")

    response = TestClient(app).post("/api/backtesting/run", json={
        "portfolio": {"TEST": {"mode": "weight", "value": 100}},
        "start_date": "2024-01-01",
        "end_date": "2025-01-01",
    })

    assert response.status_code == 200
    payload = response.json()
    assert payload["chart_data"] == result["chart_data"]
    assert payload["per_company"] == result["per_company"]
    assert payload["yearly_returns"] == result["yearly_returns"]
    assert payload["dividends_by_year"] == result["dividends_by_year"]
