"""Compatibility contract for the frontend-facing HTTP surface."""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.version import __version__
from src.web_app.server import app

EXPECTED_OPERATIONS = {
    ("GET", "/health"),
    ("GET", "/api/config"),
    ("GET", "/api/steps"),
    ("GET", "/api/steps/{step_name}"),
    ("POST", "/api/pipeline/run"),
    ("GET", "/api/jobs"),
    ("GET", "/api/jobs/{job_id}"),
    ("POST", "/api/jobs/{job_id}/cancel"),
    ("GET", "/api/jobs/{job_id}/output"),
    ("POST", "/api/screening/run"),
    ("POST", "/api/screening/save"),
    ("GET", "/api/screening/saved"),
    ("GET", "/api/screening/saved/{name}"),
    ("DELETE", "/api/screening/saved/{name}"),
    ("POST", "/api/screening/export"),
    ("GET", "/api/security/search"),
    ("GET", "/api/security/overview"),
    ("GET", "/api/security/history"),
    ("POST", "/api/backtesting/run"),
    ("POST", "/api/backtesting/run-from-csv"),
    ("POST", "/api/backtesting/run-rolling"),
    ("GET", "/api/backtesting/list"),
    ("GET", "/api/backtesting/download/{backtest_id}"),
    ("POST", "/api/portfolio/upload"),
    ("POST", "/api/portfolio/rebuild"),
    ("GET", "/api/portfolio/transactions"),
    ("DELETE", "/api/portfolio/transactions/{source_file}"),
    ("GET", "/api/portfolio/holdings"),
    ("GET", "/api/portfolio/performance"),
    ("GET", "/api/tags"),
    ("GET", "/api/tags/{company_code}"),
    ("POST", "/api/tags/{company_code}/{tag}"),
    ("DELETE", "/api/tags/{company_code}/{tag}"),
}


def _api_operations(schema: dict) -> set[tuple[str, str]]:
    return {
        (method.upper(), path)
        for path, path_item in schema["paths"].items()
        if path == "/health" or path.startswith("/api/")
        for method in path_item
        if method in {"get", "post", "put", "patch", "delete"}
    }


def test_required_frontend_operations_remain_in_openapi():
    schema = app.openapi()
    missing = EXPECTED_OPERATIONS - _api_operations(schema)
    assert not missing


def test_openapi_operation_ids_are_unique():
    schema = app.openapi()
    operation_ids = [
        operation["operationId"]
        for path_item in schema["paths"].values()
        for method, operation in path_item.items()
        if method in {"get", "post", "put", "patch", "delete"}
    ]
    assert len(operation_ids) == len(set(operation_ids))


def test_public_config_is_versioned_and_contains_no_host_path():
    response = TestClient(app).get("/api/config")
    assert response.status_code == 200
    assert response.json()["version"] == __version__
    assert response.json()["workspace_id"] == "default"
    assert (
        response.json()["max_backtest_artifact_bytes"]
        > response.json()["max_export_bytes"]
    )
    assert "repo_root" not in response.json()
