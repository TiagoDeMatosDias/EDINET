"""The per-screen API documentation: coverage, examples, and the access it promises and enforces."""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path
from urllib.parse import urlencode

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from src.api.models import PipelineConfig
from src.api_docs import api as docs_api
from src.api_docs.api import install_openapi_security, openapi_for_role
from src.api_docs.catalogue import PYTHON_SETUP, SECTIONS, SECTIONS_BY_ID
from src.api_docs.endpoints import ENDPOINTS
from src.api_docs.reference import (
    _specificity,
    documented_operations,
    endpoint_reference,
    guide_level,
    guide_paths,
    owner,
    section_reference,
    section_visible,
)
from src.auth.dependencies import require_admin, require_operator
from src.auth.route_access import access_level, read_only_token_allowed, role_allows
from src.jobs.user_api import install_job_error_handlers
from src.web_app.security import PUBLIC_AUTH_PATHS, AppSettings, install_security
from src.web_app.server import app as server_app

PASSWORD = "correct horse battery staple"
APP_TSX = Path(__file__).resolve().parents[2] / "frontend-v2" / "src" / "App.tsx"
OPERATIONS = documented_operations(server_app)


def _routes() -> dict[tuple[str, str], APIRoute]:
    return {
        (method, route.path_format): route
        for route in server_app.routes
        if isinstance(route, APIRoute)
        for method in route.methods
    }


ROUTES = _routes()


def _template_regex(path: str) -> re.Pattern[str]:
    return re.compile("^" + re.sub(r"\\\{[^}]+\\\}", "[^/]+", re.escape(path)) + "$")


ROUTE_PATTERNS = [(method, _template_regex(path)) for method, path in OPERATIONS]


def _filled(method: str, path: str) -> str:
    """The URL with the documented example values (or placeholders) filled in."""
    reference = endpoint_reference(server_app, method, path)
    values = {item["name"]: item["example"] for item in reference["parameters"] if item["in"] == "path"}
    url = re.sub(r"\{([^}]+)\}", lambda match: str(values.get(match.group(1)) or "X"), path)
    query = {item["name"]: item["example"] for item in reference["parameters"] if item["in"] == "query" and item["example"] not in (None, "")}
    return f"{url}?{urlencode(query, doseq=True)}" if query else url


def _docs_app(tmp_path) -> FastAPI:
    """The real routes behind accounts-mode security, as a deployed server runs them."""
    app = FastAPI()
    app.router.routes.extend(route for route in server_app.routes if isinstance(route, APIRoute))
    install_job_error_handlers(app)
    install_security(
        app,
        AppSettings(auth_mode="accounts", registration_mode="open", auth_db_path=tmp_path / "auth.db"),
    )
    install_openapi_security(app)
    return app


@pytest.fixture
def accounts(tmp_path):
    """An admin (the first account), an operator, a member, and personal tokens for each."""
    app = _docs_app(tmp_path)
    client = TestClient(app)
    for username in ("root-admin", "plain-member", "data-operator"):
        assert client.post("/api/auth/register", json={"username": username, "password": PASSWORD}).status_code == 201
    service = app.state.auth_service
    users = {user["username"]: user for user in service.store.list_users()}
    admin = users["root-admin"]["user_id"]
    service.update_user_role(users["data-operator"]["user_id"], "operator", requested_by=admin)
    users = {user["username"]: user for user in service.store.list_users()}
    assert [users[name]["role"] for name in ("root-admin", "data-operator", "plain-member")] == ["admin", "operator", "member"]
    member = users["plain-member"]["user_id"]
    tokens = {
        "member": service.create_api_token(member, "full", ["*"]),
        "member_read": service.create_api_token(member, "reader", ["read"]),
        "operator": service.create_api_token(users["data-operator"]["user_id"], "operator", ["*"]),
        "admin": service.create_api_token(admin, "admin", ["*"]),
    }
    return app, client, {name: {"Authorization": f"Bearer {token}"} for name, token in tokens.items()}


# -- coverage ----------------------------------------------------------------


def _owners(method: str, path: str) -> list[tuple[str, str]]:
    """Every (section, group) whose most specific pattern names the operation equally well."""
    scored = [
        (score, section.id, group.title)
        for section in SECTIONS
        for group in section.groups
        for pattern in group.patterns
        if (score := _specificity(pattern, method, path)) is not None
    ]
    if not scored:
        return []
    best = max(score for score, _section, _group in scored)
    return sorted({(section, group) for score, section, group in scored if score == best})


def test_every_operation_is_documented_once_with_a_summary():
    assert len(OPERATIONS) > 250
    assert not any(path.startswith("/api/worker/") for _method, path in OPERATIONS)
    problems = [f"{method} {path}: {_owners(method, path)}" for method, path in OPERATIONS if len(_owners(method, path)) != 1]
    assert problems == []
    shown = []
    for section in SECTIONS:
        for group in section_reference(server_app, section)["groups"]:
            for endpoint in group["endpoints"]:
                shown.append((endpoint["method"], endpoint["path"]))
                assert endpoint["summary"], f"{endpoint['method']} {endpoint['path']} has no summary"
                assert endpoint["access"] == access_level(endpoint["method"], endpoint["path"])
    assert sorted(shown) == sorted(OPERATIONS)


def test_patterns_related_entries_and_endpoint_notes_name_real_routes():
    for section in SECTIONS:
        for group in section.groups:
            for pattern in group.patterns:
                assert any(_specificity(pattern, method, path) is not None for method, path in OPERATIONS), f"{section.id}: {pattern}"
        for entry in section.related:
            method, _, path = entry.partition(" ")
            assert (method, path) in OPERATIONS, f"{section.id} related: {entry}"
            assert owner(method, path)[0].id != section.id, f"{section.id} lists its own {entry} as related"
    for key, doc in ENDPOINTS.items():
        method, _, path = key.partition(" ")
        assert (method, path) in ROUTES, f"ENDPOINTS[{key!r}] names no route"
        route = ROUTES[(method, path)]
        names = {param.alias for param in route.dependant.query_params}
        assert set(doc.query or {}) <= names, f"{key}: unknown query parameters {set(doc.query or {}) - names}"
        names = {param.alias for param in route.dependant.path_params}
        assert set(doc.path or {}) <= names, f"{key}: unknown path parameters {set(doc.path or {}) - names}"


def test_sections_point_at_real_workspace_screens():
    app_routes = set(re.findall(r'<Route path="([^"]+)"', APP_TSX.read_text(encoding="utf-8")))
    assert {"/api-docs", "/api-docs/:sectionId"} <= app_routes
    for section in SECTIONS:
        for screen in section.screen_paths:
            assert screen in app_routes, f"{section.id}: {screen} is not a workspace route"
    assert SECTIONS[0].id == "start"
    assert len(SECTIONS_BY_ID) == len(SECTIONS)


#: Routes that read their JSON body themselves (they also take multipart uploads).
_SELF_PARSED_BODIES = {("POST", "/api/pipeline/run"): PipelineConfig}


def test_example_bodies_validate_against_the_routes():
    checked = 0
    for method, path in OPERATIONS:
        body = endpoint_reference(server_app, method, path)["body"]
        if not body or body["form"] is not None or body["example"] is None:
            continue
        if (method, path) in _SELF_PARSED_BODIES:
            _SELF_PARSED_BODIES[(method, path)].model_validate(body["example"])
            checked += 1
            continue
        route = ROUTES[(method, path)]
        assert route.body_field is not None, f"{method} {path} documents a body it does not take"
        _value, errors = route.body_field.validate(body["example"], {}, loc=("body",))
        assert not errors, f"{method} {path}: {errors}"
        checked += 1
    assert checked > 60


def test_query_examples_and_upload_forms_use_real_parameters():
    for method, path in OPERATIONS:
        reference = endpoint_reference(server_app, method, path)
        route = ROUTES[(method, path)]
        for parameter in reference["parameters"]:
            if parameter["in"] == "query" and parameter["example"] not in (None, ""):
                field = next(param for param in route.dependant.query_params if param.alias == parameter["name"])
                example = parameter["example"]
                if parameter["type"].startswith("array") and not isinstance(example, list):
                    example = [example]
                _value, errors = field.validate(example, {}, loc=("query", parameter["name"]))
                assert not errors, f"{method} {path} ?{parameter['name']}={parameter['example']!r}: {errors}"
        if reference["body"] and reference["body"]["form"]:
            names = {param.alias for param in route.dependant.body_params}
            assert set(reference["body"]["form"]) <= names, f"{method} {path}: form fields {set(reference['body']['form']) - names}"


def test_guides_compile_and_call_real_routes():
    guides = [(section.id, guide) for section in SECTIONS for guide in section.guides]
    assert len([guide for _section, guide in guides if guide.curl and guide.python]) > 20
    compile(PYTHON_SETUP, "python_setup", "exec")
    for section_id, guide in guides:
        if guide.python:
            compile(PYTHON_SETUP + "\n" + guide.python, f"{section_id}: {guide.title}", "exec")
        for path in guide_paths(guide.curl + "\n" + guide.python):
            assert any(pattern.match(path) for _method, pattern in ROUTE_PATTERNS), f"{section_id}: {guide.title} calls {path}"


# -- what the docs promise about access ----------------------------------------


def test_every_non_public_endpoint_needs_a_token(accounts):
    _app, client, _headers = accounts
    for method, path in OPERATIONS:
        if access_level(method, path) == "public":
            continue
        response = client.request(method, _filled(method, path))
        assert response.status_code == 401, f"{method} {path}: {response.status_code}"
        assert response.json()["code"] == "unauthorized"


def test_public_endpoints_answer_without_a_token(accounts):
    _app, client, _headers = accounts
    public = [(method, path) for method, path in OPERATIONS if access_level(method, path) == "public"]
    assert ("GET", "/health") in public and ("POST", "/api/auth/login") in public
    assert {path for _method, path in public} <= PUBLIC_AUTH_PATHS | {"/health"}
    for method, path in public:
        response = client.request(method, path)
        assert response.status_code < 500, f"{method} {path}: {response.status_code}"
        if response.status_code == 401:
            assert response.json().get("code") != "unauthorized", f"{method} {path} asked for a token"


def test_read_only_tokens_can_only_read(accounts):
    _app, client, headers = accounts
    for method, path in OPERATIONS:
        if read_only_token_allowed(method, path):
            assert method == "GET" or access_level(method, path) == "public"
            continue
        response = client.request(method, _filled(method, path), headers=headers["member_read"])
        assert response.status_code == 403, f"{method} {path}: {response.status_code}"
        assert response.json()["code"] == "insufficient_scope"
    assert client.get("/api/docs", headers=headers["member_read"]).status_code == 200


@pytest.mark.parametrize(("role", "refused"), [("member", {"operator", "admin"}), ("operator", {"admin"})])
def test_roles_are_refused_endpoints_above_them(accounts, role, refused):
    _app, client, headers = accounts
    guarded = [(method, path) for method, path in OPERATIONS if access_level(method, path) in refused]
    assert len(guarded) > 25
    for method, path in guarded:
        body = endpoint_reference(server_app, method, path)["body"]
        json_body = body["example"] if body and body["form"] is None else None
        response = client.request(method, _filled(method, path), headers=headers[role], json=json_body)
        assert response.status_code == 403, f"{role} {method} {path}: {response.status_code} {response.text[:200]}"
        assert response.json()["code"] == "forbidden"


_RANK = {"public": 0, "member": 1, "operator": 2, "admin": 3}


def _own_check(route: APIRoute) -> str:
    """The role a route's own code demands: its ``require_*`` dependencies or helper calls."""
    levels = {"public"}

    def walk(dependant) -> None:
        for dependency in dependant.dependencies:
            if dependency.call is require_admin:
                levels.add("admin")
            elif dependency.call is require_operator:
                levels.add("operator")
            walk(dependency)

    walk(route.dependant)
    source = inspect.getsource(route.endpoint)
    if re.search(r"\b_?require_admin\(", source):
        levels.add("admin")
    if re.search(r"\b_?require_operator\(", source):
        levels.add("operator")
    return max(levels, key=_RANK.__getitem__)


def test_the_route_table_names_every_route_that_checks_a_role():
    """A route that demands operator or admin must say so in ``route_access``, or the docs show it to members."""
    checked = 0
    for (method, path), route in ROUTES.items():
        if path.startswith("/api/worker/") or not path.startswith("/api/"):
            continue
        own = _own_check(route)
        checked += own != "public"
        assert _RANK[access_level(method, path)] >= _RANK[own], f"{method} {path} checks {own} but route_access says {access_level(method, path)}"
    assert checked > 25


def test_the_role_check_covers_routes_without_their_own(tmp_path):
    """The middleware refuses by the route table, so a route that forgets its check is still guarded."""
    app = FastAPI()

    @app.get("/api/jobs")
    def unguarded_operator_route() -> dict[str, bool]:
        return {"ran": True}

    @app.post("/api/admin/unguarded")
    def unguarded_admin_route() -> dict[str, bool]:
        return {"ran": True}

    @app.get("/api/notes")
    def member_route() -> dict[str, bool]:
        return {"ran": True}

    @app.api_route("/{path:path}", methods=["GET", "POST"])
    def page_fallback(path: str) -> None:  # like the server's single-page fallback
        raise HTTPException(status_code=404)

    install_security(app, AppSettings(auth_mode="accounts", registration_mode="open", auth_db_path=tmp_path / "auth.db"))
    client = TestClient(app)
    service = app.state.auth_service
    for username in ("root-admin", "plain-member", "data-operator"):
        service.register(username, PASSWORD)
    users = {user["username"]: user["user_id"] for user in service.store.list_users()}
    service.update_user_role(users["data-operator"], "operator", requested_by=users["root-admin"])
    bearer = {name: {"Authorization": f"Bearer {service.create_api_token(users[name], 't', ['*'])}"} for name in users}
    expected = {
        "plain-member": {"/api/notes": 200, "/api/jobs": 403, "/api/admin/unguarded": 403, "/api/admin/missing": 403},
        "data-operator": {"/api/notes": 200, "/api/jobs": 200, "/api/admin/unguarded": 403, "/api/admin/missing": 403},
        "root-admin": {"/api/notes": 200, "/api/jobs": 200, "/api/admin/unguarded": 200, "/api/admin/missing": 404},
    }
    for name, answers in expected.items():
        for path, status in answers.items():
            method = "POST" if path.startswith("/api/admin/") else "GET"
            response = client.request(method, path, headers=bearer[name])
            assert response.status_code == status, f"{name} {method} {path}: {response.status_code}"
            if status == 403:
                assert response.json()["code"] == "forbidden"


def test_api_tokens_cannot_mint_more_tokens(accounts):
    _app, client, headers = accounts
    refused = client.post("/api/auth/tokens", headers=headers["member"], json={"name": "another"})
    assert refused.status_code == 403
    assert "signed-in session" in refused.json()["detail"]
    session = client.post("/api/auth/login", json={"login": "plain-member", "password": PASSWORD}).json()["access_token"]
    created = client.post(
        "/api/auth/tokens",
        headers={"Authorization": f"Bearer {session}"},
        json={"name": "nightly", "scopes": ["read"], "expires_at": "2099-01-01T00:00:00Z"},
    )
    assert created.status_code == 200
    token = created.json()["token"]
    assert token.startswith("ed_pat_")
    listed = client.get("/api/auth/tokens", headers={"Authorization": f"Bearer {token}"}).json()
    assert any(item["name"] == "nightly" and item["scopes_json"] == '["read"]' for item in listed)


# -- the docs endpoints ----------------------------------------------------------


def test_docs_index_and_sections(accounts):
    _app, client, headers = accounts
    assert client.get("/api/docs").status_code == 401
    index = client.get("/api/docs", headers=headers["member"]).json()
    assert index["auth_mode"] == "accounts"
    member_sections = [section["id"] for section in index["sections"]]
    assert member_sections == [section.id for section in SECTIONS if section.id not in {"pipeline", "admin"}]
    assert "def call(" in index["python_setup"]
    assert index["openapi_url"] == "/api/docs/openapi.json" and index["explorer_url"] is None
    operator_sections = [section["id"] for section in client.get("/api/docs", headers=headers["operator"]).json()["sections"]]
    assert operator_sections == [section.id for section in SECTIONS if section.id != "admin"]
    admin_sections = [section["id"] for section in client.get("/api/docs", headers=headers["admin"]).json()["sections"]]
    assert admin_sections == [section.id for section in SECTIONS]
    for hidden in ("pipeline", "admin"):
        assert client.get(f"/api/docs/{hidden}", headers=headers["member"]).status_code == 404
    assert client.get("/api/docs/admin", headers=headers["operator"]).status_code == 404
    screening = client.get("/api/docs/screening", headers=headers["member"]).json()
    run = next(endpoint for group in screening["groups"] for endpoint in group["endpoints"] if endpoint["id"] == "post-api-screening-run")
    assert run["access"] == "member" and run["read_only_token"] is False and run["try_it"] is True
    assert run["body"]["example"]["criteria"]
    assert screening["guides"] and screening["screen_paths"] == ["/screen"]
    assert client.get("/api/docs/nope", headers=headers["member"]).status_code == 404


def test_certificate_download(accounts, tmp_path, monkeypatch):
    _app, client, headers = accounts
    pem = tmp_path / "cert.pem"
    pem.write_text("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")
    monkeypatch.setattr(docs_api, "server_certificate_path", lambda: pem)
    response = client.get("/api/docs/certificate", headers=headers["member_read"])
    assert response.status_code == 200
    assert response.text.startswith("-----BEGIN CERTIFICATE-----")
    assert 'filename="shade.pem"' in response.headers["content-disposition"]
    monkeypatch.setattr(docs_api, "server_certificate_path", lambda: None)
    assert client.get("/api/docs/certificate", headers=headers["member"]).status_code == 404


def test_openapi_schema_declares_bearer_tokens(accounts):
    app, _client, _headers = accounts
    schema = app.openapi()
    assert schema["components"]["securitySchemes"]["bearerAuth"] == {
        "type": "http",
        "scheme": "bearer",
        "description": schema["components"]["securitySchemes"]["bearerAuth"]["description"],
    }
    assert schema["security"] == [{"bearerAuth": []}]
    login = schema["paths"]["/api/auth/login"]["post"]
    assert login["security"] == [] and login["x-required-role"] == "public"
    assert schema["paths"]["/api/pipeline/run"]["post"]["x-required-role"] == "operator"
    assert schema["paths"]["/api/screening/run"]["post"]["x-required-role"] == "member"


def _hidden_paths(role: str) -> list[str]:
    """Route templates the role may not call with any method."""
    allowed = {path for method, path in OPERATIONS if role_allows(role, access_level(method, path))}
    return sorted({path for _method, path in OPERATIONS} - allowed)


def _mentions(text: str, path: str) -> bool:
    return re.search(re.escape(path) + r"(?![\w/{-])", text) is not None


@pytest.mark.parametrize("role", ["member", "operator"])
def test_docs_show_no_trace_of_routes_above_the_role(accounts, role):
    """Nothing a role may not call appears in its index, sections, or OpenAPI schema."""
    _app, client, headers = accounts
    hidden = _hidden_paths(role)
    assert any(path.startswith("/api/admin/") for path in hidden)
    index = client.get("/api/docs", headers=headers[role]).json()
    texts = {"index": json.dumps(index)}
    for section in index["sections"]:
        response = client.get(f"/api/docs/{section['id']}", headers=headers[role])
        assert response.status_code == 200
        body = response.json()
        texts[section["id"]] = json.dumps(body)
        endpoints = [endpoint for group in body["groups"] for endpoint in group["endpoints"]] + body["related"]
        assert all(role_allows(role, endpoint["access"]) for endpoint in endpoints)
        assert section["endpoint_count"] == sum(len(group["endpoints"]) for group in body["groups"])
        assert all(group["endpoints"] for group in body["groups"])
    schema = client.get("/api/docs/openapi.json", headers=headers[role]).json()
    texts["openapi"] = json.dumps(schema)
    for name, text in texts.items():
        leaked = [path for path in hidden if _mentions(text, path)]
        assert not leaked, f"{role} sees {leaked} in {name}"
    if role == "member":
        assert "PUT /api/splits/{split_id}" not in json.dumps(texts)
        assert "put" not in schema["paths"]["/api/splits/{split_id}"]
    operations = {(method.upper(), path) for path, item in schema["paths"].items() for method in item if method != "parameters"}
    assert operations == {(m, p) for m, p in OPERATIONS if role_allows(role, access_level(m, p))}
    admin_only = set(openapi_for_role(server_app, "admin")["components"]["schemas"]) - set(schema["components"]["schemas"])
    assert {"AdminUserResponse", "PipelineScheduleRequest"} <= admin_only


def test_admins_see_every_section_endpoint_and_guide(accounts):
    _app, client, headers = accounts
    index = client.get("/api/docs", headers=headers["admin"]).json()
    assert sum(section["endpoint_count"] for section in index["sections"]) == len(OPERATIONS)
    for section in SECTIONS:
        body = client.get(f"/api/docs/{section.id}", headers=headers["admin"]).json()
        assert [guide["title"] for guide in body["guides"]] == [guide.title for guide in section.guides]
    schema = client.get("/api/docs/openapi.json", headers=headers["admin"]).json()
    assert sum(1 for item in schema["paths"].values() for method in item if method != "parameters") == len(OPERATIONS)
    assert not any(path.startswith("/api/worker/") for path in schema["paths"])


def test_guides_and_notes_follow_the_role(accounts):
    _app, client, headers = accounts
    levels = {(section.id, guide.title): guide_level(server_app, guide) for section in SECTIONS for guide in section.guides}
    assert levels[("pipeline", "Run pipeline steps")] == "operator"
    assert {level for (section, _title), level in levels.items() if section == "admin"} == {"admin"}
    assert {level for (section, _title), level in levels.items() if section not in {"pipeline", "admin"}} == {"member"}
    pipeline = client.get("/api/docs/pipeline", headers=headers["operator"]).json()
    assert [group["title"] for group in pipeline["groups"]].count("Saved setups") == 0
    assert pipeline["guides"]
    assert "Saved setups" in [group["title"] for group in client.get("/api/docs/pipeline", headers=headers["admin"]).json()["groups"]]
    assert not section_visible(server_app, SECTIONS_BY_ID["admin"], "operator")

    def details(role: str, section: str, endpoint_id: str) -> str:
        body = client.get(f"/api/docs/{section}", headers=headers[role]).json()
        endpoint = next(item for group in body["groups"] for item in group["endpoints"] if item["id"] == endpoint_id)
        return " ".join([endpoint["summary"], *endpoint["details"]])

    assert "pipeline and system jobs" not in details("member", "jobs", "get-api-my-jobs")
    assert "pipeline and system jobs" in details("operator", "jobs", "get-api-my-jobs")
    assert "force" not in details("member", "jobs", "post-api-my-jobs-job-id-cancel")
    assert "force" in details("operator", "jobs", "post-api-my-jobs-job-id-cancel")
    chat_delete = "delete-api-chat-messages-message-id"
    assert "administrator" not in details("operator", "chat", chat_delete)
    assert "administrator" in details("admin", "chat", chat_delete)


def test_full_schema_and_explorers_are_closed_with_accounts(accounts, tmp_path):
    app, client, headers = accounts
    for path in (app.openapi_url, app.docs_url, app.redoc_url, app.swagger_ui_oauth2_redirect_url):
        for header in ({}, headers["admin"]):
            assert client.get(path, headers=header).status_code == 404, path
    open_app = FastAPI()
    open_app.router.routes.extend(route for route in server_app.routes if isinstance(route, APIRoute))
    install_security(open_app, AppSettings(auth_mode="disabled", auth_db_path=tmp_path / "open.db"))
    install_openapi_security(open_app)
    open_client = TestClient(open_app)
    assert open_client.get("/openapi.json").status_code == 200
    assert open_client.get("/docs").status_code == 200
    index = open_client.get("/api/docs").json()
    assert index["explorer_url"] == "/docs" and [section["id"] for section in index["sections"]] == [section.id for section in SECTIONS]
