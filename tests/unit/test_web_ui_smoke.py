"""Smoke tests for the frontend-v2 SPA routes."""

from bs4 import BeautifulSoup
from fastapi.testclient import TestClient

from src.web_app.server import app


client = TestClient(app)


def _soup_for(path: str) -> BeautifulSoup:
    response = client.get(path)
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    return BeautifulSoup(response.text, "html.parser")


def test_root_serves_spa() -> None:
    soup = _soup_for("/")
    assert soup.select_one("div#root") is not None
    assert "Shade Research" in soup.text


def test_screen_serves_spa() -> None:
    soup = _soup_for("/screen")
    assert soup.select_one("div#root") is not None


def test_analyze_serves_spa() -> None:
    soup = _soup_for("/analyze/test-code")
    assert soup.select_one("div#root") is not None


def test_backtest_serves_spa() -> None:
    soup = _soup_for("/backtest")
    assert soup.select_one("div#root") is not None


def test_pipeline_serves_spa() -> None:
    soup = _soup_for("/pipeline")
    assert soup.select_one("div#root") is not None


def test_portfolio_serves_spa() -> None:
    soup = _soup_for("/portfolio")
    assert soup.select_one("div#root") is not None


def test_favicon_served() -> None:
    response = client.get("/favicon.ico")
    assert response.status_code == 200


def test_unknown_page_404() -> None:
    response = client.get("/nonexistent-page")
    assert response.status_code == 404


def test_unknown_api_404() -> None:
    response = client.get("/api/does-not-exist")
    assert response.status_code == 404
