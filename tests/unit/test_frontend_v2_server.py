from bs4 import BeautifulSoup
from fastapi.testclient import TestClient

from src.web_app.server import app


client = TestClient(app)


def test_workspace_routes_serve_the_react_entrypoint() -> None:
    for path in (
        "/",
        "/screen",
        "/analyze",
        "/analyze/E02144",
        "/backtest",
        "/portfolio",
        "/pipeline",
    ):
        response = client.get(path)
        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")
        assert "Shade Research" in response.text
        assert '<div id="root"></div>' in response.text


def test_workspace_assets_are_served_from_isolated_mount() -> None:
    response = client.get("/")
    soup = BeautifulSoup(response.text, "html.parser")
    script = soup.select_one("script[type='module']")
    assert script is not None
    source = script.get("src", "")
    assert source.startswith("/app-assets/")

    asset = client.get(source)
    assert asset.status_code == 200
    assert "javascript" in asset.headers.get("content-type", "")


def test_screen_route_serves_frontend() -> None:
    response = client.get("/screen")
    assert response.status_code == 200
    assert "Shade Research" in response.text

