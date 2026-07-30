from fastapi.testclient import TestClient

from src.web_app.server import app

client = TestClient(app)


def test_health_and_steps_endpoints_available() -> None:
    health = client.get("/health")
    assert health.status_code == 200
    payload = health.json()
    assert payload.get("status") == "healthy"
    assert isinstance(payload["jobs"]["queue_depth"], int)
    assert isinstance(payload["jobs"]["active"], int)
    assert isinstance(payload["jobs"]["counts_by_status"], dict)

    steps = client.get("/api/steps")
    assert steps.status_code == 200
    body = steps.json()
    assert isinstance(body, dict)
    assert isinstance(body.get("steps"), list)


def test_each_method_path_pair_is_registered_once() -> None:
    seen: set[tuple[str, str]] = set()
    duplicates: set[tuple[str, str]] = set()
    for route in app.router.routes:
        path = getattr(route, "path", None)
        for method in getattr(route, "methods", None) or ():
            key = (method, path)
            if key in seen:
                duplicates.add(key)
            seen.add(key)
    assert duplicates == set()
