"""Statement tables built from a filing's own linkbases (src/filings/statements.py).

The synthetic filing uses neutral context ids (C1, C2, ...) so the tests prove
that periods and dimensions come from the instance and linkbases, never from
EDINET context-id naming.
"""

from __future__ import annotations

import io
import zipfile

from fastapi.testclient import TestClient

from src.filings.catalog import FilingCatalog
from src.filings.ingest import ingest_content
from src.filings.statements import build_statement_tables, read_linkbases, statement_name

ROLE = "http://disclosure.edinet-fsa.go.jp/role/jppfs/"
PARENT_CHILD = "http://www.xbrl.org/2003/arcrole/parent-child"
DIM = "http://xbrl.org/int/dim/arcrole/"
TOTAL = "http://www.xbrl.org/2003/role/totalLabel"
PERIOD_START = "http://www.xbrl.org/2003/role/periodStartLabel"
PERIOD_END = "http://www.xbrl.org/2003/role/periodEndLabel"
LINKBASE = (
    '<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" '
    'xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xbrldt="http://xbrl.org/2005/xbrldt">{}</link:linkbase>'
)


def _fragment(label: str) -> str:
    concept = label.split("#", 1)[0]
    prefix = "jpcrp030000-asr_E00001-000" if concept.startswith("Seg") or concept == "SpecialGain" else "jppfs_cor"
    return f"{prefix}_{concept}"


def _link(tag: str, role: str, arcs: list[tuple[str, str, str, dict[str, str]]]) -> str:
    labels = dict.fromkeys(label for arc in arcs for label in arc[:2])
    locs = "".join(
        f'<link:loc xlink:type="locator" xlink:href="x.xsd#{_fragment(label)}" xlink:label="{label}"/>'
        for label in labels
    )
    arc_tag = tag.replace("Link", "Arc")
    rendered = "".join(
        f'<link:{arc_tag} xlink:type="arc" xlink:arcrole="{arcrole}" xlink:from="{source}" xlink:to="{target}" '
        f'order="{order}" '
        + " ".join(f'{key}="{value}"' for key, value in extra.items())
        + "/>"
        for order, (source, target, arcrole, extra) in enumerate(arcs, start=1)
    )
    return f'<link:{tag} xlink:type="extended" xlink:role="{ROLE}{role}">{locs}{rendered}</link:{tag}>'


def _presentation(role: str, edges: list[tuple[str, str] | tuple[str, str, str]]) -> str:
    return _link("presentationLink", role, [
        (edge[0], edge[1], PARENT_CHILD, {"preferredLabel": edge[2]} if len(edge) == 3 else {})
        for edge in edges
    ])


def _definition(role: str, line_items: str, table: str, axis: str, domain: list[str]) -> str:
    arcs = [
        (line_items, table, f"{DIM}all", {"xbrldt:contextElement": "scenario", "xbrldt:closed": "true"}),
        (table, axis, f"{DIM}hypercube-dimension", {}),
        (axis, domain[0], f"{DIM}dimension-domain", {}),
        *((domain[0], member, f"{DIM}domain-member", {}) for member in domain[1:]),
    ]
    return _link("definitionLink", role, arcs)


def _balance_sheet(role: str, member: str) -> tuple[str, str]:
    presentation = _presentation(role, [
        ("BalanceSheetHeading", "BalanceSheetTable"),
        ("BalanceSheetTable", "ConsolidatedOrNonConsolidatedAxis"),
        ("ConsolidatedOrNonConsolidatedAxis", member),
        ("BalanceSheetHeading", "BalanceSheetLineItems"),
        ("BalanceSheetLineItems", "AssetsAbstract"),
        ("AssetsAbstract", "CurrentAssetsAbstract"),
        ("CurrentAssetsAbstract", "CashAndDeposits"),
        ("CurrentAssetsAbstract", "CurrentAssets", TOTAL),
        ("AssetsAbstract", "Assets", TOTAL),
    ])
    definition = _definition(role, "BalanceSheetLineItems", "BalanceSheetTable", "ConsolidatedOrNonConsolidatedAxis", [member])
    return presentation, definition


def _filing() -> bytes:
    consolidated = _balance_sheet("rol_ConsolidatedBalanceSheet", "ConsolidatedMember")
    separate = _balance_sheet("rol_BalanceSheet", "NonConsolidatedMember")
    cash_flow = _presentation("rol_ConsolidatedStatementOfCashFlows-indirect", [
        ("CashFlowHeading", "CashFlowLineItems"),
        ("CashFlowLineItems", "NetIncreaseDecreaseInCashAndCashEquivalents"),
        ("CashFlowLineItems", "CashAndCashEquivalents#start", PERIOD_START),
        ("CashFlowLineItems", "CashAndCashEquivalents#end", PERIOD_END),
    ])
    segments = _presentation("rol_NotesSegmentInformation-01", [
        ("SegmentHeading", "SegmentTable"),
        ("SegmentTable", "OperatingSegmentsAxis"),
        ("OperatingSegmentsAxis", "EntityTotalMember"),
        ("EntityTotalMember", "SegAutoMember"),
        ("EntityTotalMember", "SegFoodMember"),
        ("SegmentHeading", "SegmentLineItems"),
        ("SegmentLineItems", "NetSales"),
        ("SegmentLineItems", "SpecialGain"),
    ])
    presentation = LINKBASE.format(consolidated[0] + separate[0] + cash_flow + segments)
    definition = LINKBASE.format(
        consolidated[1]
        + separate[1]
        + _definition("rol_NotesSegmentInformation-01", "SegmentLineItems", "SegmentTable", "OperatingSegmentsAxis", ["EntityTotalMember", "SegAutoMember", "SegFoodMember"])
        + _link("definitionLink", "rol_Defaults", [
            ("ConsolidatedOrNonConsolidatedAxis", "ConsolidatedMember", f"{DIM}dimension-default", {}),
            ("OperatingSegmentsAxis", "EntityTotalMember", f"{DIM}dimension-default", {}),
        ])
    )
    labels = LINKBASE.format(
        '<link:labelLink xlink:type="extended" xlink:role="http://www.xbrl.org/2003/role/link">'
        '<link:loc xlink:type="locator" xlink:href="x.xsd#jpcrp030000-asr_E00001-000_SegAutoMember" xlink:label="auto"/>'
        '<link:label xlink:type="resource" xlink:label="auto_label" xlink:role="http://www.xbrl.org/2003/role/label" xml:lang="en">Automotive</link:label>'
        '<link:labelArc xlink:type="arc" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label" xlink:from="auto" xlink:to="auto_label"/>'
        '<link:loc xlink:type="locator" xlink:href="x.xsd#jpcrp030000-asr_E00001-000_SpecialGain" xlink:label="gain"/>'
        '<link:label xlink:type="resource" xlink:label="gain_label" xlink:role="http://www.xbrl.org/2003/role/label" xml:lang="en">Special gain on land</link:label>'
        '<link:labelArc xlink:type="arc" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label" xlink:from="gain" xlink:to="gain_label"/>'
        "</link:labelLink>"
    )

    def context(context_id: str, period: str, member: tuple[str, str] | None = None) -> str:
        scenario = (
            f'<xbrli:scenario><xbrldi:explicitMember dimension="{member[0]}">{member[1]}</xbrldi:explicitMember></xbrli:scenario>'
            if member else ""
        )
        return (
            f'<xbrli:context id="{context_id}"><xbrli:entity><xbrli:identifier scheme="s">E00001</xbrli:identifier>'
            f"</xbrli:entity><xbrli:period>{period}</xbrli:period>{scenario}</xbrli:context>"
        )

    current = "<xbrli:startDate>2025-04-01</xbrli:startDate><xbrli:endDate>2026-03-31</xbrli:endDate>"
    prior = "<xbrli:startDate>2024-04-01</xbrli:startDate><xbrli:endDate>2025-03-31</xbrli:endDate>"
    separate_member = ("jppfs_cor:ConsolidatedOrNonConsolidatedAxis", "jppfs_cor:NonConsolidatedMember")
    contexts = "".join((
        context("C1", "<xbrli:instant>2026-03-31</xbrli:instant>"),
        context("C2", "<xbrli:instant>2025-03-31</xbrli:instant>"),
        context("C3", "<xbrli:instant>2024-03-31</xbrli:instant>"),
        context("C4", current),
        context("C5", prior),
        context("C6", "<xbrli:instant>2026-03-31</xbrli:instant>", separate_member),
        context("C7", current, ("jpcrp_cor:OperatingSegmentsAxis", "jpcrp030000-asr_E00001-000:SegAutoMember")),
        context("C8", current, ("jpcrp_cor:OperatingSegmentsAxis", "jpcrp030000-asr_E00001-000:SegFoodMember")),
    ))
    facts = "".join(
        f'<{concept} contextRef="{context_id}" unitRef="JPY" decimals="0">{value}</{concept}>'
        for concept, context_id, value in (
            ("jppfs_cor:CashAndDeposits", "C1", 100),
            ("jppfs_cor:CurrentAssets", "C1", 300),
            ("jppfs_cor:Assets", "C1", 900),
            ("jppfs_cor:Assets", "C2", 800),
            ("jppfs_cor:Assets", "C6", 500),
            ("jppfs_cor:NetIncreaseDecreaseInCashAndCashEquivalents", "C4", 40),
            ("jppfs_cor:NetIncreaseDecreaseInCashAndCashEquivalents", "C5", 30),
            ("jppfs_cor:CashAndCashEquivalents", "C1", 170),
            ("jppfs_cor:CashAndCashEquivalents", "C2", 130),
            ("jppfs_cor:CashAndCashEquivalents", "C3", 100),
            ("jppfs_cor:NetSales", "C4", 1200),
            ("jppfs_cor:NetSales", "C7", 700),
            ("jppfs_cor:NetSales", "C8", 500),
            ("ext:SpecialGain", "C4", 7),
        )
    )
    instance = (
        '<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:xbrldi="http://xbrl.org/2006/xbrldi" '
        'xmlns:jppfs_cor="urn:jppfs" xmlns:jpcrp_cor="urn:jpcrp" xmlns:ext="urn:ext" '
        'xmlns:jpcrp030000-asr_E00001-000="urn:ext">'
        f'{contexts}<xbrli:unit id="JPY"><xbrli:measure>iso4217:JPY</xbrli:measure></xbrli:unit>{facts}</xbrli:xbrl>'
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("XBRL/PublicDoc/report.xbrl", instance)
        archive.writestr("XBRL/PublicDoc/report_pre.xml", presentation)
        archive.writestr("XBRL/PublicDoc/report_def.xml", definition)
        archive.writestr("XBRL/PublicDoc/report_lab-en.xml", labels)
    return output.getvalue()


TAXONOMY_LABELS = {
    "jppfs_cor:CashAndDeposits": "Cash and deposits",
    "jppfs_cor:CurrentAssets": "Total current assets",
    "jppfs_cor:Assets": "Total assets",
    "jppfs_cor:CashAndCashEquivalents": "Cash and cash equivalents",
}


def _tables(tmp_path, content: bytes | None = None) -> dict:
    content = content or _filing()
    catalog = FilingCatalog(tmp_path / "Filings.db")
    ingest_content(content, "S100STMT", catalog)
    facts = [dict(row) for row in catalog.statement_facts("S100STMT")]
    return build_statement_tables(read_linkbases(content), facts, lambda names: {n: TAXONOMY_LABELS[n] for n in names if n in TAXONOMY_LABELS})


def _statement(tables: dict, name: str) -> dict:
    return next(statement for statement in tables["statements"] if statement["name"] == name)


def _row(statement: dict, label: str, member: str | None = None) -> dict:
    return next(row for row in statement["rows"] if row["label"] == label and row.get("member") == member)


def test_statements_follow_the_filings_presentation_roles(tmp_path):
    tables = _tables(tmp_path)

    assert tables["source"] == "linkbase"
    assert [statement["name"] for statement in tables["statements"]] == [
        "Consolidated balance sheet",
        "Balance sheet",
        "Consolidated statement of cash flows (indirect)",
        "Notes segment information (1)",
    ]
    balance = _statement(tables, "Consolidated balance sheet")
    assert [(row["kind"], row["label"], row["depth"]) for row in balance["rows"]] == [
        ("heading", "Assets", 0),
        ("heading", "Current assets", 1),
        ("item", "Cash and deposits", 2),
        ("total", "Total current assets", 2),
        ("total", "Total assets", 1),
    ]
    assert [period["label"] for period in balance["periods"]] == ["2026-03-31", "2025-03-31"]
    assert _row(balance, "Total assets")["values"] == {"I:2026-03-31": 900, "I:2025-03-31": 800}
    assert balance["member_axes"] == []


def test_dimension_rules_separate_consolidated_and_non_consolidated_values(tmp_path):
    tables = _tables(tmp_path)

    separate = _statement(tables, "Balance sheet")

    assert [row["label"] for row in separate["rows"] if row["kind"] != "heading"] == ["Total assets"]
    assert _row(separate, "Total assets")["values"] == {"I:2026-03-31": 500}


def test_opening_and_closing_balances_land_in_their_flow_columns(tmp_path):
    cash_flow = _statement(_tables(tmp_path), "Consolidated statement of cash flows (indirect)")

    assert [(period["label"], period["detail"]) for period in cash_flow["periods"]] == [
        ("2026-03-31", "12 months"),
        ("2025-03-31", "12 months"),
    ]
    current, prior = (period["key"] for period in cash_flow["periods"])
    opening = _row(cash_flow, "Cash and cash equivalents, beginning of period")
    closing = _row(cash_flow, "Cash and cash equivalents, end of period")
    assert opening["values"] == {current: 130, prior: 100}
    assert closing["values"] == {current: 170, prior: 130}


def test_members_on_a_declared_axis_become_labelled_rows(tmp_path):
    segments = _statement(_tables(tmp_path), "Notes segment information (1)")

    assert segments["member_axes"] == ["Operating segments axis"]
    assert [(row["label"], row["member"]) for row in segments["rows"]] == [
        ("Net sales", "Entity total"),
        ("Net sales", "Automotive"),
        ("Net sales", "Seg food"),
        ("Special gain on land", "Entity total"),
    ]
    assert _row(segments, "Net sales", "Automotive")["values"] == {"D:2025-04-01:2026-03-31": 700}


def test_filing_without_presentation_linkbase_lists_undimensioned_facts(tmp_path):
    original = zipfile.ZipFile(io.BytesIO(_filing()))
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for name in original.namelist():
            if not name.endswith(("_pre.xml", "_def.xml")):
                archive.writestr(name, original.read(name))

    tables = _tables(tmp_path, output.getvalue())

    assert tables["source"] == "facts"
    (flat,) = tables["statements"]
    assert _row(flat, "Assets")["values"] == {"I:2026-03-31": 900, "I:2025-03-31": 800}
    assert all(row["member"] is None for row in flat["rows"])
    assert not any(row["label"] == "Net sales" and row["values"].get("D:2025-04-01:2026-03-31") == 700 for row in flat["rows"])


def test_role_names_come_from_role_uris():
    assert statement_name(f"{ROLE}rol_ConsolidatedStatementOfCashFlows-indirect") == "Consolidated statement of cash flows (indirect)"
    assert statement_name(f"{ROLE}rol_ConsolidatedStatementOfFinancialPositionIFRS") == "Consolidated statement of financial position IFRS"
    assert statement_name(f"{ROLE}rol_MajorShareholders-01") == "Major shareholders (1)"


def test_statement_tables_endpoint(tmp_path):
    from src.filings.runtime import catalog
    from src.web_app.server import app

    ingest_content(_filing(), "S100STAPI", catalog)
    client = TestClient(app)

    response = client.get("/api/filings/S100STAPI/statement-tables")

    assert response.status_code == 200
    assert response.json()["statements"][0]["name"] == "Consolidated balance sheet"
    assert client.get("/api/filings/S100MISSING/statement-tables").status_code == 404
