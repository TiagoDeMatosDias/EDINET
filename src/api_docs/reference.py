"""Build the per-screen endpoint reference from the running app's OpenAPI schema.

The schema supplies parameters, body fields, and response models; ``catalogue``
says which screen and group each endpoint belongs to; ``endpoints`` adds
descriptions and examples where the code's docstring is missing or too terse.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import FastAPI

from src.auth.route_access import (
    ACCESS_LABELS,
    WORKER_PREFIX,
    access_level,
    read_only_token_allowed,
    role_allows,
)

from .catalogue import SECTIONS, Guide, Section
from .endpoints import ENDPOINTS, UNSET, EndpointDoc

HTTP_METHODS = ("get", "post", "put", "patch", "delete")
_METHOD_ORDER = {method.upper(): index for index, method in enumerate(HTTP_METHODS)}
_MAX_MODEL_DEPTH = 4

# Example values by parameter or field name, used wherever an endpoint names
# no example of its own. Opaque ids come from earlier calls, so they stay
# recognisable placeholders.
SAMPLE_VALUES: dict[str, Any] = {
    "company_code": "E02144",
    "edinet_code": "E02144",
    "company_codes": ["E02144", "E01777"],
    "codes": "E02144,E01777",
    "ticker": "72030",
    "tickers": ["72030", "67580"],
    "symbol": "7203",
    "doc_id": "S100Y8NY",
    "q": "toyota",
    "query": "dividend",
    "username": "alice",
    "login": "alice",
    "password": "correct horse battery staple",
    "current_password": "old passphrase",
    "new_password": "new passphrase",
    "name": "My list",
    "title": "Margin check",
    "body": "Operating margin recovered after the yen moved.",
    "text": "Results look strong this quarter.",
    "date": "2025-03-31",
    "start_date": "2020-01-01",
    "end_date": "2024-12-31",
    "as_of": "2025-03-31",
    "base_currency": "JPY",
    "display_currency": "JPY",
    "currency": "JPY",
    "period": "yearly",
    "periods": 10,
    "limit": 20,
    "offset": 0,
    "statement_family": "BS",
    "metric": "PER",
    "operator": ">",
    "threshold": 15,
    "tags": ["value", "watch"],
    "tag": "value",
    "tag_name": "value",
    "role": "operator",
    "kind": "company",
    "channel_id": "company:E02144",
    "seq": 1,
    "tokens": "$7203",
    "texts": ["連結財務諸表"],
    "confirmation": "confirmed",
    "value": "",
}
_PLACEHOLDER_IDS = re.compile(r"(^|_)(id|identifier)$")


def _placeholder(name: str) -> str:
    return name.upper()


#: Values for string patterns the models use, so generated examples validate.
_PATTERN_SAMPLES: dict[str, str] = {
    r"^[0-9a-fA-F-]{36}$": "0b7c6f4e-2d1a-4c3b-9e8f-5a6b7c8d9e0f",
    r"^[0-9a-f]{64}$": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
    r"^\d{4}-\d{2}-\d{2}$": "2025-03-31",
    r"^[A-Za-z]{3}$": "JPY",
}
_PATTERN_CHOICES = re.compile(r"^\^\(([\w.|-]+)\)\$$")
_BASE64 = r"^[A-Za-z0-9+/]+={0,2}$"


def _fits(value: Any, schema: dict[str, Any]) -> bool:
    """Whether a shared sample value suits this field (its string pattern, if any)."""
    pattern = schema.get("pattern")
    return not (pattern and isinstance(value, str) and re.fullmatch(pattern, value) is None)


def _string_sample(name: str, schema: dict[str, Any]) -> str:
    pattern = schema.get("pattern") or ""
    if pattern in _PATTERN_SAMPLES:
        return _PATTERN_SAMPLES[pattern]
    choices = _PATTERN_CHOICES.match(pattern)
    if choices:
        return choices.group(1).split("|")[0]
    value = _placeholder(name)
    if pattern == _BASE64:
        # Keys, IVs and ciphertext travel as base64: ``WrappedPrivateKeyAAAA…``.
        value = re.sub(r"[^A-Za-z0-9]", "", name.title())
    minimum = int(schema.get("minLength") or 0)
    # Encoded blobs (keys, ciphertext) have minimum lengths; pad the placeholder to fit.
    if len(value) >= minimum:
        return value
    return (value + "A" * minimum)[:minimum] if pattern == _BASE64 else (value + "_" + "X" * minimum)[:minimum]


def sample_value(name: str, schema: dict[str, Any] | None = None) -> Any:
    """An example value for a parameter or field called ``name``."""
    schema = schema or {}
    if name in SAMPLE_VALUES and _fits(SAMPLE_VALUES[name], schema):
        return SAMPLE_VALUES[name]
    if "default" in schema and schema["default"] not in (None, "", [], {}):
        return schema["default"]
    if schema.get("enum"):
        return schema["enum"][0]
    kind = schema.get("type")
    if kind == "integer":
        return max(1, int(schema.get("minimum", 1)))
    if kind == "number":
        return 1.0
    if kind == "boolean":
        return False
    if schema.get("pattern") or schema.get("minLength"):
        return _string_sample(name, schema)
    return _placeholder(name)


class _Schema:
    """Readers over one OpenAPI document's component schemas."""

    def __init__(self, document: dict[str, Any]) -> None:
        self.components: dict[str, Any] = document.get("components", {}).get("schemas", {})

    @staticmethod
    def ref_name(schema: dict[str, Any]) -> str | None:
        ref = schema.get("$ref")
        return ref.rsplit("/", 1)[-1] if isinstance(ref, str) else None

    def resolve(self, schema: dict[str, Any]) -> dict[str, Any]:
        name = self.ref_name(schema)
        return self.components.get(name, {}) if name else schema

    @staticmethod
    def _variants(schema: dict[str, Any]) -> list[dict[str, Any]]:
        return list(schema.get("anyOf") or schema.get("oneOf") or [])

    def non_null(self, schema: dict[str, Any]) -> dict[str, Any]:
        """The schema without an ``anyOf: [X, null]`` wrapper."""
        variants = [item for item in self._variants(schema) if item.get("type") != "null"]
        return variants[0] if len(variants) == 1 else schema

    def label(self, schema: dict[str, Any]) -> str:
        """A short type name such as ``string``, ``array of Criterion`` or ``file``."""
        name = self.ref_name(schema)
        if name:
            return _display_name(name)
        variants = self._variants(schema)
        if variants:
            labels = [self.label(item) for item in variants if item.get("type") != "null"]
            text = " or ".join(dict.fromkeys(labels)) or "any"
            return f"{text} or null" if any(item.get("type") == "null" for item in variants) else text
        if "const" in schema:
            return repr(schema["const"])
        kind = schema.get("type")
        if kind == "array":
            return f"array of {self.label(schema.get('items') or {})}"
        if kind == "object":
            extra = schema.get("additionalProperties")
            return f"object of {self.label(extra)}" if isinstance(extra, dict) and extra else "object"
        if kind == "string" and schema.get("format") == "binary":
            return "file"
        if kind == "string" and schema.get("format") in {"date", "date-time"}:
            return f"string ({schema['format']})"
        return str(kind or "any")

    def fields(self, schema: dict[str, Any]) -> list[dict[str, Any]]:
        resolved = self.resolve(schema)
        required = set(resolved.get("required") or [])
        rows = []
        for name, prop in (resolved.get("properties") or {}).items():
            plain = self.non_null(prop)
            rows.append(
                {
                    "name": name,
                    "type": self.label(prop),
                    "required": name in required,
                    "default": prop.get("default"),
                    "description": _text(prop.get("description") or plain.get("description") or ""),
                    "enum": (plain.get("enum") or (plain.get("items") or {}).get("enum")),
                }
            )
        return rows

    def referenced_models(self, schema: dict[str, Any], depth: int = 0, seen: set[str] | None = None) -> list[str]:
        """Component names reachable from ``schema`` (outermost first)."""
        seen = set() if seen is None else seen
        names: list[str] = []
        if depth > _MAX_MODEL_DEPTH:
            return names
        name = self.ref_name(schema)
        if name:
            if name in seen:
                return names
            seen.add(name)
            names.append(name)
            schema = self.components.get(name, {})
        for child in self._children(schema):
            names.extend(self.referenced_models(child, depth + 1, seen))
        return names

    def _children(self, schema: dict[str, Any]) -> list[dict[str, Any]]:
        children = list(self._variants(schema))
        children.extend((schema.get("properties") or {}).values())
        if isinstance(schema.get("items"), dict):
            children.append(schema["items"])
        if isinstance(schema.get("additionalProperties"), dict):
            children.append(schema["additionalProperties"])
        return children

    def example(self, schema: dict[str, Any], name: str = "", depth: int = 0) -> Any:
        """A minimal valid value: required fields only, filled with sample values."""
        if depth > _MAX_MODEL_DEPTH:
            return None
        plain = self.non_null(schema)
        resolved = self.resolve(plain)
        if name in SAMPLE_VALUES and not resolved.get("properties") and _fits(SAMPLE_VALUES[name], resolved):
            return SAMPLE_VALUES[name]
        kind = resolved.get("type")
        if resolved.get("properties") or kind == "object":
            required = resolved.get("required") or []
            properties = resolved.get("properties") or {}
            return {key: self.example(properties[key], key, depth + 1) for key in required if key in properties}
        if kind == "array":
            item = self.example(resolved.get("items") or {}, name, depth + 1)
            minimum = int(resolved.get("minItems") or 1)
            return [item] * minimum if item is not None else []
        return sample_value(name, resolved)


def _display_name(component: str) -> str:
    """``src__auth__api__RegisterRequest`` -> ``RegisterRequest``; FastAPI's ``Body_*`` -> ``Request body``."""
    if component.startswith("Body_"):
        return "Request body"
    return component.rsplit("__", 1)[-1]


_DOUBLE_TICKS = re.compile(r"``([^`]+)``")
_STARRED = re.compile(r"(?<![\w*])\*([A-Za-z_][\w]*)\*(?![\w*])")


def _text(value: str) -> str:
    """Docstring markup to the docs' Markdown subset (``x``/*x* -> `x`)."""
    value = _DOUBLE_TICKS.sub(r"`\1`", value)
    return _STARRED.sub(r"`\1`", value).strip()


def _paragraphs(docstring: str) -> list[str]:
    """Docstring paragraphs with hard-wrapped lines joined."""
    blocks = re.split(r"\n\s*\n", docstring.strip())
    return [" ".join(line.strip() for line in block.splitlines() if line.strip()) for block in blocks if block.strip()]


def documented_operations(app: FastAPI) -> list[tuple[str, str]]:
    """Every (METHOD, path) the docs cover: ``/api/*`` except the worker protocol, and ``/health``."""
    operations = []
    for path, item in app.openapi()["paths"].items():
        if not (path == "/health" or path.startswith("/api/")) or path.startswith(WORKER_PREFIX):
            continue
        operations.extend((method.upper(), path) for method in item if method in HTTP_METHODS)
    return operations


def _specificity(pattern: str, method: str, path: str) -> tuple[int, int] | None:
    """How closely ``pattern`` names the operation, or ``None`` when it does not."""
    pattern_method, _, pattern_path = pattern.rpartition(" ")
    if pattern_method and pattern_method != method:
        return None
    if pattern_path.endswith("/*"):
        prefix = pattern_path[:-2]
        return (1, len(prefix)) if path == prefix or path.startswith(prefix + "/") else None
    if pattern_path != path:
        return None
    return (3 if pattern_method else 2, len(pattern_path))


def owner(method: str, path: str) -> tuple[Section, int] | None:
    """The section and group index that document an operation (the most specific pattern wins)."""
    best: tuple[tuple[int, int], Section, int] | None = None
    for section in SECTIONS:
        for index, group in enumerate(section.groups):
            for pattern in group.patterns:
                score = _specificity(pattern, method, path)
                if score is not None and (best is None or score > best[0]):
                    best = (score, section, index)
    return (best[1], best[2]) if best else None


def _pattern_rank(section: Section, group_index: int, method: str, path: str) -> int:
    for index, pattern in enumerate(section.groups[group_index].patterns):
        if _specificity(pattern, method, path) is not None:
            return index
    return len(section.groups[group_index].patterns)


def _parameters(reader: _Schema, operation: dict[str, Any], doc: EndpointDoc) -> list[dict[str, Any]]:
    rows = []
    examples = {**(doc.path or {}), **(doc.query or {})}
    for parameter in operation.get("parameters") or []:
        name = parameter["name"]
        schema = parameter.get("schema") or {}
        plain = reader.non_null(schema)
        rows.append(
            {
                "name": name,
                "in": parameter["in"],
                "required": bool(parameter.get("required")),
                "type": reader.label(schema),
                "default": schema.get("default"),
                "description": _text(parameter.get("description") or schema.get("description") or ""),
                "enum": plain.get("enum"),
                "example": examples.get(name, sample_value(name, plain) if parameter.get("required") or parameter["in"] == "path" else None),
            }
        )
    return rows


def _request_body(reader: _Schema, operation: dict[str, Any], doc: EndpointDoc) -> dict[str, Any] | None:
    content = (operation.get("requestBody") or {}).get("content") or {}
    if doc.form is not None:
        return {"content_type": "multipart/form-data", "fields": [], "models": [], "example": None, "form": dict(doc.form)}
    if not content and doc.body is UNSET:
        return None
    content_type, media = next(iter(content.items())) if content else ("application/json", {})
    schema = media.get("schema") or {}
    if content_type.startswith("multipart/"):
        fields = reader.fields(schema)
        return {
            "content_type": content_type,
            "fields": fields,
            "models": [],
            "example": None,
            "form": {field["name"]: f"@{field['name']}" if field["type"] == "file" else str(sample_value(field["name"])) for field in fields if field["required"]},
        }
    models = reader.referenced_models(schema)
    top = models[0] if models and reader.ref_name(reader.non_null(schema)) == models[0] else None
    example = doc.body if doc.body is not UNSET else reader.example(schema)
    return {
        "content_type": content_type,
        "required": bool((operation.get("requestBody") or {}).get("required")),
        "model": _display_name(top) if top else None,
        "fields": reader.fields(schema) if top else [],
        "models": [
            {"name": _display_name(name), "fields": reader.fields({"$ref": f"#/components/schemas/{name}"})}
            for name in models
            if name != top
        ],
        "example": example,
        "form": None,
    }


def _response(reader: _Schema, operation: dict[str, Any], doc: EndpointDoc) -> dict[str, Any]:
    responses = operation.get("responses") or {}
    status = next((code for code in responses if code.startswith("2")), "200")
    media = (responses.get(status) or {}).get("content") or {}
    content_type = next(iter(media), None)
    schema = (media.get(content_type) or {}).get("schema") or {} if content_type else {}
    models = reader.referenced_models(schema)
    top = reader.ref_name(reader.non_null(schema)) or reader.ref_name((reader.non_null(schema).get("items") or {}))
    return {
        "status": int(status) if status.isdigit() else 200,
        "content_type": doc.content_type or content_type,
        "type": reader.label(schema) if schema else None,
        "fields": reader.fields({"$ref": f"#/components/schemas/{top}"}) if top else [],
        "models": [
            {"name": _display_name(name), "fields": reader.fields({"$ref": f"#/components/schemas/{name}"})}
            for name in models
            if name != top
        ],
        "description": _text(doc.returns or ""),
    }


def endpoint_reference(app: FastAPI, method: str, path: str, role: str = "admin") -> dict[str, Any]:
    """Everything the docs page shows ``role`` for one operation."""
    document = app.openapi()
    reader = _Schema(document)
    operation = document["paths"][path][method.lower()]
    doc = ENDPOINTS.get(f"{method} {path}", EndpointDoc())
    paragraphs = _paragraphs(operation.get("description") or "") if doc.use_docstring else []
    summary = _text(doc.summary or (paragraphs[0] if paragraphs else ""))
    details = [_text(item) for item in (paragraphs if doc.summary else paragraphs[1:])]
    if doc.notes:
        details.append(_text(doc.notes))
    details.extend(_text(note) for level, note in (doc.role_notes or {}).items() if role_allows(role, level))
    access = access_level(method, path)
    return {
        "id": f"{method.lower()}-{re.sub(r'[^a-z0-9]+', '-', path.lower()).strip('-')}",
        "method": method,
        "path": path,
        "summary": summary,
        "details": details,
        "access": access,
        "access_label": ACCESS_LABELS[access],
        "read_only_token": read_only_token_allowed(method, path),
        "parameters": _parameters(reader, operation, doc),
        "body": _request_body(reader, operation, doc),
        "response": _response(reader, operation, doc),
        "try_it": method == "GET" if doc.try_it is None else doc.try_it,
        "streams": doc.streams,
    }


def _sorted(operations: list[tuple[str, str]], section: Section, group: int) -> list[tuple[str, str]]:
    return sorted(
        operations,
        key=lambda item: (_pattern_rank(section, group, *item), item[1], _METHOD_ORDER.get(item[0], 9)),
    )


def _index(app: FastAPI) -> dict[str, dict[int, list[tuple[str, str]]]]:
    """Operations by section id and group index, rebuilt only when the schema changes."""
    document = app.openapi()
    cached = getattr(app.state, "api_docs_index", None)
    if cached is not None and cached[0] is document:
        return cached[1]
    index: dict[str, dict[int, list[tuple[str, str]]]] = {}
    for method, path in documented_operations(app):
        found = owner(method, path)
        if found:
            index.setdefault(found[0].id, {}).setdefault(found[1], []).append((method, path))
    app.state.api_docs_index = (document, index)
    return index


_GUIDE_PATH = re.compile(r"/api/[A-Za-z0-9_\-/{}$:.]*")
_LEVEL_ORDER = {"public": 0, "member": 1, "operator": 2, "admin": 3}


def guide_paths(code: str) -> list[str]:
    """The ``/api`` paths a guide's code calls, with variables and placeholders as ``X``."""
    return [re.sub(r"\{[^}]*\}|\$\{?[A-Za-z_]+\}?", "X", match).rstrip("/.") for match in _GUIDE_PATH.findall(code)]


def _templates(app: FastAPI) -> list[tuple[re.Pattern[str], str, str]]:
    document = app.openapi()
    cached = getattr(app.state, "api_docs_templates", None)
    if cached is not None and cached[0] is document:
        return cached[1]
    templates = [
        (re.compile("^" + re.sub(r"\\\{[^}]+\\\}", "[^/]+", re.escape(path)) + "$"), method, path)
        for method, path in documented_operations(app)
    ]
    app.state.api_docs_templates = (document, templates)
    return templates


def guide_level(app: FastAPI, guide: Guide) -> str:
    """The highest role any route a guide names needs; every method of a named path counts."""
    levels = {"member"}
    for path in guide_paths(guide.curl + "\n" + guide.python):
        levels.update(access_level(method, template) for pattern, method, template in _templates(app) if pattern.match(path))
    return max(levels, key=lambda level: _LEVEL_ORDER[level])



def _allowed(role: str, operations: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return [(method, path) for method, path in operations if role_allows(role, access_level(method, path))]


def section_reference(app: FastAPI, section: Section, role: str = "admin") -> dict[str, Any]:
    """A screen's guides and endpoints that ``role`` may use, grouped as the catalogue lists them."""
    grouped = _index(app).get(section.id, {})
    groups = []
    for index, group in enumerate(section.groups):
        operations = _allowed(role, _sorted(grouped.get(index, []), section, index))
        if operations:
            groups.append(
                {
                    "title": group.title,
                    "intro": group.intro,
                    "endpoints": [endpoint_reference(app, method, path, role) for method, path in operations],
                }
            )
    related = []
    available = {operation for groups in _index(app).values() for operations in groups.values() for operation in operations}
    for entry in section.related:
        method, _, path = entry.partition(" ")
        found = owner(method, path) if (method, path) in available else None
        if found and role_allows(role, access_level(method, path)):
            reference = endpoint_reference(app, method, path, role)
            related.append({**reference, "section": found[0].id, "section_title": found[0].title})
    return {
        **section_summary(app, section, role),
        "intro": section.intro,
        "guides": [
            {"title": guide.title, "text": guide.text, "curl": guide.curl, "python": guide.python, "response": guide.response}
            for guide in section.guides
            if role_allows(role, guide_level(app, guide))
        ],
        "groups": groups,
        "related": related,
    }


def section_summary(app: FastAPI, section: Section, role: str = "admin") -> dict[str, Any]:
    count = sum(len(_allowed(role, operations)) for operations in _index(app).get(section.id, {}).values())
    return {
        "id": section.id,
        "title": section.title,
        "screen_paths": list(section.screen_paths),
        "summary": section.summary,
        "endpoint_count": count,
    }


def section_visible(app: FastAPI, section: Section, role: str) -> bool:
    """Whether ``role`` may use anything the section documents; hidden sections are not listed or served."""
    if section_summary(app, section, role)["endpoint_count"]:
        return True
    return any(role_allows(role, guide_level(app, guide)) for guide in section.guides)
