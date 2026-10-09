"""Statement tables built from a filing's own XBRL linkbases.

Every structural decision comes from the filing rather than from lists kept in
code:

* the presentation linkbase supplies each statement (role), its line items,
  their order and nesting, and preferred labels (totals, period start/end);
* the definition linkbase supplies, per statement, the dimensions it uses, the
  members valid on each, and each dimension's default member. A fact belongs
  to a statement only when its context satisfies those rules, which is how the
  consolidated and non-consolidated balance sheets separate, and why segment
  or equity-component facts become member rows of the note that declares them;
* the instance supplies each context's explicit dimension members;
* labels come from the filing's English label linkbase (extension concepts),
  then from the caller's taxonomy lookup (standard concepts), and only then
  from the element name.

Filings without a presentation linkbase fall back to one flat table of their
undimensioned facts.
"""

from __future__ import annotations

import io
import re
import zipfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any
from xml.etree import ElementTree

from .archive import DEFAULT_ARCHIVE_POLICY, ArchivePolicy, validate_zip_in_memory

_XLINK = "{http://www.w3.org/1999/xlink}"
_LINK = "{http://www.xbrl.org/2003/linkbase}"
_XBRLDT = "{http://xbrl.org/2005/xbrldt}"
_DIM_ARCROLE = "http://xbrl.org/int/dim/arcrole/"
_STANDARD_LABEL = "http://www.xbrl.org/2003/role/label"
_MAX_LINKBASE_BYTES = 32 * 1024 * 1024

FALLBACK_STATEMENT_NAME = "Reported facts"

TaxonomyLabels = Callable[[Iterable[str]], Mapping[str, str]]


@dataclass
class FilingLinkbases:
    presentation: list[bytes] = field(default_factory=list)
    definition: list[bytes] = field(default_factory=list)
    labels_en: list[bytes] = field(default_factory=list)
    instances: list[bytes] = field(default_factory=list)


@dataclass
class _Node:
    concept: str
    qname: str
    preferred_label: str
    children: list[_Node] = field(default_factory=list)


@dataclass
class _Role:
    uri: str
    roots: list[_Node]


@dataclass
class _Dimensions:
    defaults: dict[str, str] = field(default_factory=dict)
    by_role: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    hypercubes: set[str] = field(default_factory=set)
    axes: set[str] = field(default_factory=set)
    # Line-item containers joined to a hypercube by ``all`` arcs.
    primary_roots: set[str] = field(default_factory=set)


def _local(reference: str) -> str:
    """Local name of a QName (``p:Name``) or linkbase fragment (``p_Name``)."""
    reference = reference.split("#", 1)[-1]
    if ":" in reference:
        return reference.rsplit(":", 1)[1]
    return reference.rsplit("_", 1)[-1]


def _qname(fragment: str) -> str:
    """``jppfs_cor_Assets`` -> ``jppfs_cor:Assets`` (EDINET element ids)."""
    fragment = fragment.split("#", 1)[-1]
    prefix, _, local = fragment.rpartition("_")
    return f"{prefix}:{local}" if prefix else local


def read_linkbases(archive: bytes, policy: ArchivePolicy = DEFAULT_ARCHIVE_POLICY) -> FilingLinkbases:
    """Collect the main document's linkbases and instances from a filing ZIP."""
    infos = [info for info in validate_zip_in_memory(archive, policy) if not info.is_dir()]
    public = [info for info in infos if "publicdoc/" in info.filename.replace("\\", "/").casefold()]
    members = public or infos
    found = FilingLinkbases()
    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        for info in members:
            name = info.filename.casefold()
            if info.file_size > _MAX_LINKBASE_BYTES:
                continue
            if name.endswith("_pre.xml"):
                found.presentation.append(zipped.read(info))
            elif name.endswith("_def.xml"):
                found.definition.append(zipped.read(info))
            elif name.endswith("_lab-en.xml"):
                found.labels_en.append(zipped.read(info))
            elif name.endswith(".xbrl"):
                found.instances.append(zipped.read(info))
    return found


def _links(content: bytes, tag: str) -> Iterable[tuple[str, dict[str, str], list[ElementTree.Element], list[ElementTree.Element]]]:
    root = ElementTree.fromstring(content)
    for link in root.iter(f"{_LINK}{tag}"):
        locators = {
            loc.get(f"{_XLINK}label", ""): loc.get(f"{_XLINK}href", "").split("#", 1)[-1]
            for loc in link.findall(f"{_LINK}loc")
        }
        arcs = [child for child in link if child.tag.endswith("Arc")]
        resources = [child for child in link if child.tag == f"{_LINK}label"]
        yield link.get(f"{_XLINK}role", ""), locators, arcs, resources


def _tree(
    label: str,
    preferred: str,
    locators: Mapping[str, str],
    children: Mapping[str, list[tuple[float, str, str]]],
    path: frozenset[str],
) -> _Node:
    fragment = locators[label]
    node = _Node(_local(fragment), _qname(fragment), preferred)
    for _, child, child_preferred in sorted(children.get(label, []), key=lambda item: item[0]):
        if child not in path:  # guard against cyclic arcs
            node.children.append(_tree(child, child_preferred, locators, children, path | {child}))
    return node


def parse_presentation(contents: Iterable[bytes]) -> list[_Role]:
    """Statement trees in document order, one per presentation role."""
    roles: dict[str, _Role] = {}
    for content in contents:
        for role_uri, locators, arcs, _ in _links(content, "presentationLink"):
            children: dict[str, list[tuple[float, str, str]]] = {}
            targets: set[str] = set()
            for arc in arcs:
                source, target = arc.get(f"{_XLINK}from", ""), arc.get(f"{_XLINK}to", "")
                if source not in locators or target not in locators:
                    continue
                order = float(arc.get("order") or 0)
                children.setdefault(source, []).append((order, target, arc.get("preferredLabel") or ""))
                targets.add(target)

            roots = [
                _tree(label, "", locators, children, frozenset({label}))
                for label in dict.fromkeys(list(children) + list(locators))
                if label in children and label not in targets
            ]
            if role_uri in roles:
                roles[role_uri].roots.extend(roots)
            elif roots:
                roles[role_uri] = _Role(role_uri, roots)
    return list(roles.values())


def parse_dimensions(contents: Iterable[bytes]) -> _Dimensions:
    """Per-role dimensions with their usable members, plus dimension defaults."""
    model = _Dimensions()
    for content in contents:
        for role_uri, locators, arcs, _ in _links(content, "definitionLink"):
            edges: dict[str, list[tuple[str, bool]]] = {}
            role_axes: dict[str, list[str]] = {}
            for arc in arcs:
                arcrole = arc.get(f"{_XLINK}arcrole", "")
                source = locators.get(arc.get(f"{_XLINK}from", ""))
                target = locators.get(arc.get(f"{_XLINK}to", ""))
                if not arcrole.startswith(_DIM_ARCROLE) or source is None or target is None:
                    continue
                kind = arcrole[len(_DIM_ARCROLE):]
                usable = arc.get(f"{_XBRLDT}usable", "true") != "false"
                if kind == "all":
                    model.primary_roots.add(_local(source))
                    model.hypercubes.add(_local(target))
                elif kind == "dimension-default":
                    model.defaults[_local(source)] = _local(target)
                    model.axes.add(_local(source))
                elif kind == "hypercube-dimension":
                    model.hypercubes.add(_local(source))
                    model.axes.add(_local(target))
                    role_axes.setdefault(_local(target), [])
                elif kind in {"dimension-domain", "domain-member"}:
                    edges.setdefault(_local(source), []).append((_local(target), usable))
            for axis in role_axes:
                members: list[str] = []
                pending = list(edges.get(axis, []))
                seen: set[str] = set()
                while pending:
                    member, usable = pending.pop(0)
                    if member in seen:
                        continue
                    seen.add(member)
                    if usable:
                        members.append(member)
                    pending.extend(edges.get(member, []))
                role_axes[axis] = members
            if role_axes:
                model.by_role.setdefault(role_uri, {}).update(role_axes)
    return model


def parse_context_dimensions(contents: Iterable[bytes]) -> dict[str, dict[str, str]]:
    """Explicit (and typed) dimension members for every context id."""
    contexts: dict[str, dict[str, str]] = {}
    for content in contents:
        for _, element in ElementTree.iterparse(io.BytesIO(content)):
            if not element.tag.endswith("}context"):
                continue
            members: dict[str, str] = {}
            for member in element.iter():
                if member.tag.endswith("}explicitMember"):
                    members[_local(member.get("dimension", ""))] = _local((member.text or "").strip())
                elif member.tag.endswith("}typedMember"):
                    members[_local(member.get("dimension", ""))] = "".join(member.itertext()).strip()
            contexts.setdefault(element.get("id", ""), members)
            element.clear()
    return contexts


def parse_labels(contents: Iterable[bytes]) -> dict[tuple[str, str], str]:
    """``(local name, label role) -> text`` from label linkbases."""
    labels: dict[tuple[str, str], str] = {}
    for content in contents:
        for _, locators, arcs, resources in _links(content, "labelLink"):
            by_resource: dict[str, list[tuple[str, str]]] = {}
            for resource in resources:
                text = " ".join("".join(resource.itertext()).split())
                if text:
                    by_resource.setdefault(resource.get(f"{_XLINK}label", ""), []).append(
                        (resource.get(f"{_XLINK}role", _STANDARD_LABEL), text)
                    )
            for arc in arcs:
                concept = locators.get(arc.get(f"{_XLINK}from", ""))
                if concept is None:
                    continue
                for role, text in by_resource.get(arc.get(f"{_XLINK}to", ""), []):
                    labels.setdefault((_local(concept), role), text)
    return labels


def humanize(name: str) -> str:
    """Readable text from an element or role name, e.g. ``NetSalesIFRS`` -> ``Net sales IFRS``."""
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", name))
    words = spaced.replace("_", " ").replace("-", " ").split()
    words = [word if word.isupper() and len(word) > 1 else word.lower() for word in words]
    text = " ".join(words)
    return text[:1].upper() + text[1:]


def statement_name(role_uri: str) -> str:
    """Name a role from its URI, e.g. ``.../rol_ConsolidatedStatementOfCashFlows-indirect``."""
    identifier = role_uri.rstrip("/").rsplit("/", 1)[-1].removeprefix("rol_")
    base, _, qualifier = identifier.partition("-")
    name = humanize(base)
    if qualifier:
        name += f" ({int(qualifier)})" if qualifier.isdigit() else f" ({humanize(qualifier).lower()})"
    return name


# -- periods -----------------------------------------------------------------


@dataclass(frozen=True)
class _Period:
    start: str | None
    end: str
    instant: bool

    @property
    def key(self) -> str:
        return f"I:{self.end}" if self.instant else f"D:{self.start or ''}:{self.end}"


def _fact_period(fact: Mapping[str, Any]) -> _Period | None:
    if fact.get("instant"):
        return _Period(None, str(fact["instant"]), True)
    if fact.get("period_end"):
        return _Period(fact.get("period_start") or None, str(fact["period_end"]), False)
    return None


def _day_after(value: str) -> str | None:
    try:
        return (date.fromisoformat(value) + timedelta(days=1)).isoformat()
    except ValueError:
        return None


def _months(period: _Period) -> int | None:
    if period.instant or not period.start:
        return None
    try:
        days = (date.fromisoformat(period.end) - date.fromisoformat(period.start)).days + 1
    except ValueError:
        return None
    return round(days / 30.44) if days > 0 else None


def _column_for(period: _Period, preferred_label: str, durations: list[_Period]) -> _Period | None:
    """Place balances in the flow column they open or close.

    A period-start balance is the instant the day before a duration starts; a
    period-end or ordinary balance is the instant on which it ends. An opening
    or closing balance that matches no flow column belongs to the other row
    (the same instant closes one period and opens the next), so it is dropped;
    other balances keep their own column when no single duration matches.
    """
    if not period.instant or not durations:
        return period
    opening = preferred_label.endswith("periodStartLabel")
    if opening:
        start = _day_after(period.end)
        matches = [duration for duration in durations if duration.start == start]
    else:
        matches = [duration for duration in durations if duration.end == period.end]
    if len(matches) == 1:
        return matches[0]
    if opening or preferred_label.endswith("periodEndLabel"):
        return None
    return period


def _period_payload(period: _Period, filled: int) -> dict[str, Any]:
    months = _months(period)
    return {
        "key": period.key,
        "start": period.start,
        "end": period.end,
        "label": period.end,
        "detail": "As of" if period.instant else f"{months} months" if months else "",
        "filled": filled,
    }


# -- statement assembly ------------------------------------------------------


@dataclass
class _Labeler:
    extension: dict[tuple[str, str], str]
    taxonomy: Mapping[str, str]

    def concept(self, node: _Node) -> str:
        preferred = node.preferred_label or _STANDARD_LABEL
        label = (
            self.extension.get((node.concept, preferred))
            or self.extension.get((node.concept, _STANDARD_LABEL))
            or self.taxonomy.get(node.qname)
            or humanize(node.concept)
        )
        if (node.concept, preferred) in self.extension:
            return label
        if preferred.endswith("periodStartLabel"):
            return f"{label}, beginning of period"
        if preferred.endswith("periodEndLabel"):
            return f"{label}, end of period"
        return label

    def heading(self, node: _Node) -> str:
        label = (
            self.extension.get((node.concept, _STANDARD_LABEL))
            or self.taxonomy.get(node.qname)
        )
        if label:
            return label
        # Abstract elements are conventionally named ``...Abstract``/``...Heading``.
        return humanize(re.sub(r"(Abstract|Heading)$", "", node.concept) or node.concept)

    def member(self, name: str) -> str:
        return self.extension.get((name, _STANDARD_LABEL)) or humanize(name.removesuffix("Member"))


def _role_axes(role: _Role, dimensions: _Dimensions, contexts: Mapping[str, Mapping[str, str]]) -> dict[str, list[str]]:
    """The role's dimensions and valid members (definition linkbase first)."""
    declared = dimensions.by_role.get(role.uri)
    if declared is not None:
        return declared
    # No definition linkbase for this role: read axes and members from the
    # presentation tree instead.
    used = {axis for members in contexts.values() for axis in members}
    axes: dict[str, list[str]] = {}

    def visit(node: _Node) -> None:
        if node.concept in dimensions.axes or node.concept in used:
            members: list[str] = []
            pending = list(node.children)
            while pending:
                child = pending.pop(0)
                members.append(child.concept)
                pending.extend(child.children)
            axes[node.concept] = members
            return
        for child in node.children:
            visit(child)

    for root in role.roots:
        visit(root)
    return axes


def _members_in_role(
    context_members: Mapping[str, str],
    axes: Mapping[str, list[str]],
    defaults: Mapping[str, str],
) -> tuple[str, ...] | None:
    """Members a fact takes on the role's axes, or None if it is not valid there."""
    if any(axis not in axes for axis in context_members):
        return None
    chosen: list[str] = []
    for axis, members in axes.items():
        member = context_members.get(axis, defaults.get(axis))
        if member is None or (members and member not in members):
            return None
        chosen.append(member)
    return tuple(chosen)


def _structural(node: _Node, axes: set[str], dimensions: _Dimensions) -> str | None:
    """``axis``, ``table``, or ``line-items`` for hypercube scaffolding, else None."""
    if node.concept in axes:
        return "axis"
    if node.concept in dimensions.hypercubes or any(child.concept in axes for child in node.children):
        return "table"
    if node.concept in dimensions.primary_roots:
        return "line-items"
    return None


def _line_items(role: _Role, dimensions: _Dimensions, axes: set[str], has_facts: Callable[[str], bool]) -> list[tuple[_Node, int, int]]:
    """Line items in order as ``(node, depth, end)``; ``end`` bounds its descendants.

    Hypercube scaffolding (tables, their line-item containers, and axes with
    their members) is not shown, and a lone abstract root is the statement's
    own title.
    """
    items: list[tuple[_Node, int, int]] = []

    def opens_hypercube(node: _Node, kind: str | None) -> bool:
        # A table's children, and the abstract siblings of a table (the
        # line-item container), are hypercube scaffolding.
        return kind == "table" or any(_structural(child, axes, dimensions) == "table" for child in node.children)

    def walk(node: _Node, depth: int, in_hypercube: bool) -> None:
        kind = _structural(node, axes, dimensions)
        if kind == "axis":
            return
        transparent = kind in {"table", "line-items"} or (in_hypercube and not has_facts(node.concept))
        position = len(items)
        if not transparent:
            items.append((node, depth, position + 1))
        for child in node.children:
            walk(child, depth if transparent else depth + 1, opens_hypercube(node, kind))
        if not transparent:
            items[position] = (node, depth, len(items))

    title_only = len(role.roots) == 1 and not has_facts(role.roots[0].concept)
    for root in role.roots:
        if title_only:
            root_kind = _structural(root, axes, dimensions)
            for child in root.children:
                walk(child, 0, opens_hypercube(root, root_kind))
        else:
            walk(root, 0, False)
    return items


def _build_role(
    role: _Role,
    dimensions: _Dimensions,
    contexts: Mapping[str, Mapping[str, str]],
    facts_by_concept: Mapping[str, list[Mapping[str, Any]]],
    labeler: _Labeler,
) -> dict[str, Any] | None:
    axes = _role_axes(role, dimensions, contexts)
    known_axes = dimensions.axes | set(axes) | {axis for members in contexts.values() for axis in members}
    defaults = dict(dimensions.defaults)
    for axis, members in axes.items():
        if axis not in defaults:
            # Without a declared default, the one listed member that no context
            # names explicitly is the implied one.
            explicit = {values[axis] for values in contexts.values() if axis in values}
            implied = [member for member in members if member not in explicit]
            if len(implied) == 1:
                defaults[axis] = implied[0]

    items = _line_items(role, dimensions, known_axes, lambda concept: bool(facts_by_concept.get(concept)))

    # Facts valid in this role, with the members they take on its axes.
    placed: list[tuple[int, tuple[str, ...], _Period, float, str | None]] = []
    durations: dict[str, _Period] = {}
    for index, (node, _, _) in enumerate(items):
        for fact in facts_by_concept.get(node.concept, []):
            chosen = _members_in_role(contexts.get(str(fact.get("context_id") or ""), {}), axes, defaults)
            period = _fact_period(fact)
            if chosen is None or period is None:
                continue
            value = float(fact["numeric_value"])
            if "negated" in node.preferred_label.casefold():
                value = -value
            placed.append((index, chosen, period, value, fact.get("unit_id")))
            if not period.instant:
                durations[period.key] = period
    if not placed:
        return None

    # Only axes whose members actually differ between facts become row splits.
    axis_names = list(axes)
    varying = [
        position for position in range(len(axis_names))
        if len({chosen[position] for _, chosen, _, _, _ in placed}) > 1
    ]
    columns: dict[str, _Period] = {}
    values: dict[tuple[int, tuple[str, ...]], dict[str, float]] = {}
    units: dict[tuple[int, tuple[str, ...]], str | None] = {}
    for index, chosen, period, value, unit in placed:
        combo = tuple(chosen[position] for position in varying)
        column = _column_for(period, items[index][0].preferred_label, list(durations.values()))
        if column is None:
            continue
        columns[column.key] = column
        values.setdefault((index, combo), {}).setdefault(column.key, value)
        units.setdefault((index, combo), unit)

    combos_by_item: dict[int, list[tuple[str, ...]]] = {}
    for index, combo in values:
        combos_by_item.setdefault(index, []).append(combo)
    member_order = {member: position for members in axes.values() for position, member in enumerate(members)}
    rows: list[dict[str, Any]] = []
    for index, (node, depth, end) in enumerate(items):
        combos = combos_by_item.get(index)
        if not combos:
            if any(later in combos_by_item for later in range(index + 1, end)):
                rows.append({"kind": "heading", "label": labeler.heading(node), "concept": node.concept, "depth": depth})
            continue
        kind = "total" if "total" in node.preferred_label.casefold() else "item"
        for combo in sorted(combos, key=lambda combo: [member_order.get(member, 0) for member in combo]):
            rows.append({
                "kind": kind,
                "label": labeler.concept(node),
                "concept": node.concept,
                "depth": depth,
                "member": " · ".join(labeler.member(member) for member in combo) or None,
                "unit": units[(index, combo)],
                "values": values[(index, combo)],
            })

    return {
        "id": role.uri.rstrip("/").rsplit("/", 1)[-1],
        "name": statement_name(role.uri),
        "member_axes": [labeler.member(axis_names[position]) for position in varying],
        "periods": _ordered_periods(columns, values.values()),
        "rows": rows,
    }


def _ordered_periods(columns: Mapping[str, _Period], rows: Iterable[Mapping[str, float]]) -> list[dict[str, Any]]:
    """Most recent first; longer durations before shorter ones ending together."""
    row_list = list(rows)
    ordered = sorted(columns.values(), key=lambda period: (period.end, _months(period) or 0, period.key), reverse=True)
    return [_period_payload(period, sum(1 for row in row_list if period.key in row)) for period in ordered]


def _flat_statement(
    facts: Iterable[Mapping[str, Any]],
    contexts: Mapping[str, Mapping[str, str]],
    labeler: _Labeler,
) -> dict[str, Any] | None:
    """One table of undimensioned facts for filings without a presentation linkbase."""
    rows: dict[str, dict[str, Any]] = {}
    columns: dict[str, _Period] = {}
    for fact in facts:
        period = _fact_period(fact)
        if period is None or contexts.get(str(fact.get("context_id") or "")):
            continue
        concept = str(fact["concept"])
        row = rows.setdefault(concept, {
            "kind": "item",
            "label": labeler.concept(_Node(concept, concept, "")),
            "concept": concept,
            "depth": 0,
            "member": None,
            "unit": fact.get("unit_id"),
            "values": {},
        })
        row["values"].setdefault(period.key, float(fact["numeric_value"]))
        columns[period.key] = period
    if not rows:
        return None
    return {
        "id": "facts",
        "name": FALLBACK_STATEMENT_NAME,
        "member_axes": [],
        "periods": _ordered_periods(columns, (row["values"] for row in rows.values())),
        "rows": sorted(rows.values(), key=lambda row: row["label"].casefold()),
    }


def build_statement_tables(
    linkbases: FilingLinkbases,
    facts: Iterable[Mapping[str, Any]],
    taxonomy_labels: TaxonomyLabels | None = None,
) -> dict[str, Any]:
    """Statement tables for one filing; see the module docstring."""
    fact_list = [fact for fact in facts if fact.get("numeric_value") is not None]
    roles = parse_presentation(linkbases.presentation)
    extension_labels = parse_labels(linkbases.labels_en)
    qnames = {
        node.qname
        for role in roles
        for node in _iter_nodes(role.roots)
    } or {str(fact["concept"]) for fact in fact_list}
    labeler = _Labeler(extension_labels, dict(taxonomy_labels(sorted(qnames))) if taxonomy_labels else {})

    contexts = parse_context_dimensions(linkbases.instances)
    if not roles:
        flat = _flat_statement(fact_list, contexts, labeler)
        return {"source": "facts", "fact_count": len(fact_list), "statements": [flat] if flat else []}

    dimensions = parse_dimensions(linkbases.definition)
    facts_by_concept: dict[str, list[Mapping[str, Any]]] = {}
    for fact in fact_list:
        facts_by_concept.setdefault(str(fact["concept"]), []).append(fact)
    statements = [
        statement
        for role in roles
        if (statement := _build_role(role, dimensions, contexts, facts_by_concept, labeler)) is not None
    ]
    return {"source": "linkbase", "fact_count": len(fact_list), "statements": statements}


def _iter_nodes(nodes: Iterable[_Node]) -> Iterable[_Node]:
    for node in nodes:
        yield node
        yield from _iter_nodes(node.children)
