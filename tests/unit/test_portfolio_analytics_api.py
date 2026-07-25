"""Authenticated portfolio analytical preview route tests."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth.api import router as auth_router
from src.portfolio.api import router as portfolio_router
from src.web_app.security import AppSettings, install_security


def _client(tmp_path):
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(portfolio_router)
    install_security(app, AppSettings(auth_mode="accounts", auth_db_path=tmp_path / "auth.db"))
    client = TestClient(app)
    password = "correct horse battery staple"
    client.post("/api/auth/register", json={"username": "analytics-user", "password": password})
    login = client.post("/api/auth/login", json={"login": "analytics-user", "password": password})
    client.headers["Authorization"] = f"Bearer {login.json()['access_token']}"
    return client


def test_portfolio_analytics_previews_require_auth_and_return_assumptions(tmp_path):
    client = _client(tmp_path)
    lots = client.post(
        "/api/portfolio/tax-lots",
        json={
            "method": "fifo",
            "events": [
                {"action": "buy", "ticker": "AAA", "date": "2024-01-01", "shares": 2, "price": 10},
                {"action": "sell", "ticker": "AAA", "date": "2024-02-01", "shares": 1, "price": 12},
            ],
        },
    )
    assert lots.status_code == 200
    assert lots.json()["realized"][0]["gain"] == 2

    scenario = client.post(
        "/api/portfolio/scenarios/evaluate",
        json={
            "holdings": {"AAA": 2},
            "prices": {"AAA": 10},
            "currencies": {"AAA": "JPY"},
            "price_shocks": {"AAA": -0.1},
        },
    )
    assert scenario.status_code == 200
    assert scenario.json()["total_shocked"] == 18
    assert "assumptions" in scenario.json()

    greeks = client.post(
        "/api/portfolio/greeks",
        json={
            "positions": [{
                "option_type": "call", "spot": 100, "strike": 100,
                "years": 1, "rate": 0.02, "volatility": 0.2,
                "quantity": 2, "multiplier": 100,
            }],
        },
    )
    assert greeks.status_code == 200
    assert greeks.json()["totals"]["delta"] > 0


def test_portfolio_analytics_previews_reject_missing_auth(tmp_path):
    app = FastAPI()
    app.include_router(portfolio_router)
    install_security(app, AppSettings(auth_mode="accounts", auth_db_path=tmp_path / "auth.db"))
    response = TestClient(app).post("/api/portfolio/scenarios/evaluate", json={"holdings": {}, "prices": {}})
    assert response.status_code == 401
