from fastapi.testclient import TestClient

from src.web_app.server import app

client = TestClient(app)


def test_health_and_steps_endpoints_available() -> None:
    health = client.get("/health")
    assert health.status_code == 200
    payload = health.json()
    assert payload.get("status") == "healthy"
    # /health is public, so queue state is only reported to operators.
    assert "jobs" not in payload

    status = client.get("/api/system/status")
    assert status.status_code == 200
    jobs = status.json()["jobs"]
    assert isinstance(jobs["queue_depth"], int)
    assert isinstance(jobs["active"], int)
    assert isinstance(jobs["counts_by_status"], dict)

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
