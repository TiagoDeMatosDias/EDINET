"""``/api/docs``: the per-screen API documentation the workspace's docs page shows.

Like every ``/api`` route these need a token; the index tells the page whether
the server runs with accounts (tokens required) or without sign-in. Each caller
sees only what their role may call: sections, endpoints, guides, notes, and the
OpenAPI schema (``/api/docs/openapi.json``) leave out everything else, and a
section with nothing for them answers 404 like one that does not exist.
"""

from __future__ import annotations

import copy
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import Response

from src.auth.route_access import WORKER_PREFIX, access_level, role_allows
from src.web_app.tls import server_certificate_path

from .catalogue import PYTHON_SETUP, SECTIONS, SECTIONS_BY_ID
from .reference import (
    HTTP_METHODS,
    documented_operations,
    section_reference,
    section_summary,
    section_visible,
)

router = APIRouter(prefix="/api/docs", tags=["api-docs"])


def _auth_mode(request: Request) -> str:
    settings = getattr(request.app.state, "settings", None)
    return str(getattr(settings, "auth_mode", "accounts"))


def _role(request: Request) -> str:
    """The caller's role; the security middleware has authenticated every ``/api/docs`` request."""
    return str(getattr(getattr(request.state, "user", None), "role", ""))


@router.get("")
def docs_index(request: Request) -> dict[str, Any]:
    app, role = request.app, _role(request)
    accounts = _auth_mode(request) != "disabled"
    return {
        "auth_mode": _auth_mode(request),
        "sections": [section_summary(app, section, role) for section in SECTIONS if section_visible(app, section, role)],
        "python_setup": PYTHON_SETUP,
        "openapi_url": "/api/docs/openapi.json",
        # Without accounts there are no roles, so the full explorer stays available.
        "explorer_url": None if accounts else app.docs_url,
    }


# Registered before ``/{section_id}`` so the path is not read as a section.
@router.get("/certificate")
def docs_certificate() -> Response:
    path = server_certificate_path()
    if path is None:
        raise HTTPException(status_code=404, detail="The server has no certificate file")
    return Response(
        path.read_bytes(),
        media_type="application/x-pem-file",
        headers={"Content-Disposition": 'attachment; filename="shade.pem"'},
    )


@router.get("/openapi.json")
def docs_openapi(request: Request) -> dict[str, Any]:
    """The OpenAPI schema of the routes you may call, for Postman, Insomnia, or client generators."""
    return openapi_for_role(request.app, _role(request))


@router.get("/{section_id}")
def docs_section(request: Request, section_id: str) -> dict[str, Any]:
    section = SECTIONS_BY_ID.get(section_id)
    role = _role(request)
    if section is None or not section_visible(request.app, section, role):
        raise HTTPException(status_code=404, detail="No such documentation section")
    return {
        **section_reference(request.app, section, role),
        "auth_mode": _auth_mode(request),
        "python_setup": PYTHON_SETUP,
    }


_BEARER_DESCRIPTION = (
    "A personal API token from Account → Personal API tokens (`ed_pat_…`), or a signed-in session's access token. "
    "Read-only tokens may only send GET requests."
)


def install_openapi_security(app: FastAPI) -> None:
    """Declare bearer authentication in the OpenAPI schema, so clients generated from it send the token.

    Every operation requires the token except the public sign-in endpoints;
    ``x-required-role`` states the role each operation needs (``public``,
    ``member``, ``operator`` or ``admin``). With accounts, callers only ever
    receive their role's part of it (``openapi_for_role``); the full schema and
    the ``/docs`` explorer are served only when sign-in is off.
    """
    build = app.openapi

    def openapi() -> dict[str, Any]:
        if app.openapi_schema is not None:
            return app.openapi_schema
        schema = build()
        schemes = schema.setdefault("components", {}).setdefault("securitySchemes", {})
        schemes["bearerAuth"] = {"type": "http", "scheme": "bearer", "description": _BEARER_DESCRIPTION}
        schema["security"] = [{"bearerAuth": []}]
        for path, item in schema.get("paths", {}).items():
            for method, operation in item.items():
                if method not in HTTP_METHODS or path.startswith(WORKER_PREFIX):
                    continue
                level = access_level(method.upper(), path)
                operation["x-required-role"] = level
                if level == "public":
                    operation["security"] = []
        app.openapi_schema = schema
        return schema

    app.openapi = openapi  # type: ignore[method-assign]


_REF = "#/components/schemas/"


def _refs(value: Any, found: set[str]) -> None:
    if isinstance(value, dict):
        ref = value.get("$ref")
        if isinstance(ref, str) and ref.startswith(_REF):
            found.add(ref[len(_REF) :])
        for item in value.values():
            _refs(item, found)
    elif isinstance(value, list):
        for item in value:
            _refs(item, found)


def openapi_for_role(app: FastAPI, role: str) -> dict[str, Any]:
    """The documented operations ``role`` may call, with only the models they use.

    Routes the role may not call, the worker protocol, and the web pages are
    left out, and so is every model only those routes use.
    """
    full = app.openapi()
    allowed = {
        (method.lower(), path)
        for method, path in documented_operations(app)
        if role_allows(role, access_level(method, path))
    }
    paths: dict[str, Any] = {}
    for path, item in full.get("paths", {}).items():
        kept = {key: value for key, value in item.items() if key not in HTTP_METHODS or (key, path) in allowed}
        if any(key in HTTP_METHODS for key in kept):
            paths[path] = kept
    schemas = full.get("components", {}).get("schemas", {})
    used: set[str] = set()
    _refs(paths, used)
    pending = list(used)
    while pending:
        nested: set[str] = set()
        _refs(schemas.get(pending.pop(), {}), nested)
        pending.extend(nested - used)
        used |= nested
    document = copy.deepcopy({key: value for key, value in full.items() if key not in {"paths", "components"}})
    document["paths"] = copy.deepcopy(paths)
    document["components"] = copy.deepcopy(
        {**full.get("components", {}), "schemas": {name: schema for name, schema in schemas.items() if name in used}}
    )
    return document
