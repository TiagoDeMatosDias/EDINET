"""Defused XML parsing for XBRL facts and filing narrative sections."""

from __future__ import annotations

import hashlib
import math
import re
import uuid
from dataclasses import dataclass
from html import unescape
from pathlib import PurePosixPath
from typing import Any
from xml.etree.ElementTree import Element

from bs4 import BeautifulSoup
from defusedxml import ElementTree

_CONTEXT_SUFFIX = "context"
_UNIT_SUFFIX = "unit"
_FACT_SKIP = {"context", "unit", "schemaRef", "linkbaseRef", "roleRef", "arcroleRef"}


@dataclass(frozen=True)
class XbrlFact:
    fact_id: str
    concept: str
    namespace_uri: str | None
    context_id: str | None
    unit_id: str | None
    value_text: str | None
    numeric_value: float | None
    decimals: str | None
    is_nil: int


@dataclass(frozen=True)
class ParsedXbrl:
    contexts: list[dict[str, Any]]
    units: list[dict[str, Any]]
    facts: list[XbrlFact]


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _namespace(tag: str) -> str | None:
    return tag[1:].split("}", 1)[0] if tag.startswith("{") else None


def _child(parent: Element, local_name: str) -> Element | None:
    return next((item for item in parent if _local(item.tag) == local_name), None)


def _text(element: Element | None) -> str | None:
    if element is None:
        return None
    value = "".join(element.itertext()).strip()
    return value or None


def _number(value: str | None) -> float | None:
    if not value:
        return None
    normalized = value.replace(",", "").strip()
    try:
        result = float(normalized)
    except ValueError:
        return None
    return result if math.isfinite(result) else None


class XbrlParser:
    """Parse a single XBRL instance into bounded relational values."""

    def parse(self, content: bytes, doc_id: str, artifact_id: str) -> ParsedXbrl:
        root = ElementTree.fromstring(content)
        elements = list(root.iter())
        contexts = [self._context(element, doc_id) for element in elements if _local(element.tag) == _CONTEXT_SUFFIX]
        units = [self._unit(element, doc_id) for element in elements if _local(element.tag) == _UNIT_SUFFIX]
        facts = []
        for element in elements:
            local = _local(element.tag)
            inline = local in {"nonFraction", "nonNumeric"}
            if local in _FACT_SKIP or local.startswith("footnote") or (not inline and not element.attrib.get("contextRef")):
                continue
            context_id = element.attrib.get("contextRef")
            value = _text(element)
            concept = element.attrib.get("name", local).rsplit(":", 1)[-1]
            numeric_value = _number(value)
            if inline and numeric_value is not None:
                numeric_value *= 10 ** int(element.attrib.get("scale", "0"))
                if element.attrib.get("sign") == "-":
                    numeric_value *= -1
            facts.append(
                XbrlFact(
                    fact_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}:{artifact_id}:{concept}:{context_id}:{value}")),
                    concept=concept,
                    namespace_uri=_namespace(element.tag),
                    context_id=context_id,
                    unit_id=element.attrib.get("unitRef"),
                    value_text=value,
                    numeric_value=numeric_value,
                    decimals=element.attrib.get("decimals"),
                    is_nil=1 if element.attrib.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true" else 0,
                )
            )
        return ParsedXbrl(contexts, units, facts)

    @staticmethod
    def _context(element: Element, doc_id: str) -> dict[str, Any]:
        period = _child(element, "period")
        entity = _child(element, "entity")
        identifier = _child(entity, "identifier") if entity is not None else None
        start = _text(_child(period, "startDate")) if period is not None else None
        end = _text(_child(period, "endDate")) if period is not None else None
        instant = _text(_child(period, "instant")) if period is not None else None
        return {
            "context_id": element.attrib.get("id", ""),
            "doc_id": doc_id,
            "entity_identifier": _text(identifier),
            "period_start": start,
            "period_end": end,
            "instant": instant,
        }

    @staticmethod
    def _unit(element: Element, doc_id: str) -> dict[str, Any]:
        measure = next((item for item in element.iter() if _local(item.tag) == "measure"), None)
        return {
            "unit_id": element.attrib.get("id", ""),
            "doc_id": doc_id,
            "measure": _text(measure),
        }


def parse_narrative(content: bytes, doc_id: str, artifact_id: str) -> list[dict[str, Any]]:
    """Extract plain-text, heading-delimited sections from submitted HTML."""
    soup = BeautifulSoup(content, "html.parser")
    for element in soup(["script", "style", "iframe", "object", "embed", "form"]):
        element.decompose()
    sections: list[dict[str, Any]] = []
    current_title = "Document"
    current_parts: list[str] = []
    ordinal = 0
    for element in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "td"]):
        text = re.sub(r"\s+", " ", unescape(element.get_text(" ", strip=True))).strip()
        if not text:
            continue
        if element.name.startswith("h") and current_parts:
            sections.append(_section(doc_id, artifact_id, ordinal, current_title, current_parts))
            ordinal += 1
            current_parts = []
        if element.name.startswith("h"):
            current_title = text[:300]
        else:
            current_parts.append(text)
    if current_parts:
        sections.append(_section(doc_id, artifact_id, ordinal, current_title, current_parts))
    return sections


def _section(doc_id: str, artifact_id: str, ordinal: int, title: str, parts: list[str]) -> dict[str, Any]:
    text = "\n".join(parts)[:200_000]
    return {
        "section_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}:{artifact_id}:section:{ordinal}")),
        "doc_id": doc_id,
        "artifact_id": artifact_id,
        "ordinal": ordinal,
        "title": title,
        "text": text,
    }


def artifact_kind(member_path: str) -> tuple[str, str]:
    suffix = PurePosixPath(member_path).suffix.casefold()
    if suffix in {".xbrl", ".xml"}:
        return "xbrl", "application/xml"
    if suffix in {".htm", ".html", ".xhtml"}:
        return "narrative", "text/html"
    if suffix == ".xsd":
        return "taxonomy", "application/xml"
    if suffix == ".csv":
        return "csv", "text/csv"
    return "other", "application/octet-stream"


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
