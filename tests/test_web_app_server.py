from fastapi.testclient import TestClient

from src.web_app.server import app


client = TestClient(app)


def test_web_root_serves_html() -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    assert "Shade Research" in response.text


def test_page_routes_serve_html() -> None:
    for route in ("/orchestrator", "/screening", "/security"):
        response = client.get(route)
        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")


def test_unknown_api_route_returns_404() -> None:
    response = client.get("/api/does-not-exist")
    assert response.status_code == 404


def test_health_and_steps_endpoints_available() -> None:
    health = client.get("/health")
    assert health.status_code == 200
    payload = health.json()
    assert payload.get("status") == "healthy"

    steps = client.get("/api/steps")
    assert steps.status_code == 200
    body = steps.json()
    assert isinstance(body, dict)
    assert isinstance(body.get("steps"), list)
