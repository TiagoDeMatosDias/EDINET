from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import os
import re
import sqlite3
import time
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DEFAULT_TAXONOMY_PAGE_URL = "https://disclosure2.edinet-fsa.go.jp/weee0020.aspx"
TAXONOMY_DOWNLOAD_EVENT = "'DODOWNLOAD'"

_XSD_NS = "{http://www.w3.org/2001/XMLSchema}"
_XLINK_NS = "{http://www.w3.org/1999/xlink}"
_LINK_NS = "{http://www.xbrl.org/2003/linkbase}"
_XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"
_XBRLI_NS = "{http://www.xbrl.org/2003/instance}"


class _HiddenInputValueParser(HTMLParser):
    """Extract a hidden input value from an HTML document."""

    def __init__(self, target_id: str):
        super().__init__()
        self.target_id = target_id
        self.value: str | None = None

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "input" or self.value is not None:
            return
        attr_map = {key.lower(): value for key, value in attrs}
        if attr_map.get("id") == self.target_id or attr_map.get("name") == self.target_id:
            self.value = attr_map.get("value")


@dataclass(frozen=True)
class TaxonomyListingEntry:
    archive_name: str
    namespace_prefix: str
    taxonomy_date: str | None
    release_label: str | None
    release_year: int | None
    source_page_url: str


def _extract_hidden_input_value(html_text: str, input_id: str) -> str:
    parser = _HiddenInputValueParser(input_id)
    parser.feed(html_text)
    if parser.value is None:
        raise ValueError(f"Hidden input '{input_id}' not found in EDINET taxonomy page.")
    return parser.value


def _extract_download_bytes(download_response: dict) -> bytes:
    gx_props = download_response.get("gxProps") or []
    if not gx_props:
        raise ValueError("Taxonomy download response is missing gxProps.")

    script_caption = (gx_props[0].get("TXTSCRIPT") or {}).get("Caption") or ""
    match = re.search(r"base64,([^\"']+)", script_caption)
    if not match:
        raise ValueError("Unable to find the taxonomy ZIP payload in the response.")

    return base64.b64decode(match.group(1))


def _build_download_headers(gx_state: dict, page_url: str) -> dict[str, str]:
    origin_parts = urlsplit(page_url)
    origin = f"{origin_parts.scheme}://{origin_parts.netloc}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "GXAjaxRequest": "1",
        "Origin": origin,
        "Referer": page_url,
    }

    ajax_security_token = gx_state.get("AJAX_SECURITY_TOKEN")
    if ajax_security_token:
        headers["AJAX_SECURITY_TOKEN"] = ajax_security_token

    auth_token = gx_state.get(f"GX_AUTH_{gx_state.get('vPGMNAME', '')}")
    if auth_token:
        headers["X-GXAuth-Token"] = auth_token

    return headers


def _build_download_payload(gx_state: dict, archive_name: str) -> dict:
    required_keys = (
        "vPGMNAME",
        "vPGMDESC",
        "gxhash_vPGMNAME",
        "gxhash_vPGMDESC",
    )
    missing = [key for key in required_keys if not gx_state.get(key)]
    if missing:
        raise ValueError(
            "Missing GeneXus state required for taxonomy download: " + ", ".join(missing)
        )

    return {
        "MPage": False,
        "cmpCtx": "",
        "parms": [gx_state["vPGMNAME"], gx_state["vPGMDESC"], archive_name, []],
        "hsh": [
            {"hsh": gx_state["gxhash_vPGMNAME"], "row": ""},
            {"hsh": gx_state["gxhash_vPGMDESC"], "row": ""},
        ],
        "objClass": "weee0020",
        "pkgName": "GeneXus.Programs",
        "events": [TAXONOMY_DOWNLOAD_EVENT],
        "grids": {},
    }


def _normalise_namespace_prefix(value: str | None) -> str | None:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    mapping = {
        "jppfs": "jppfs_cor",
        "jppfs_cor": "jppfs_cor",
        "jpcrp": "jpcrp_cor",
        "jpcrp_cor": "jpcrp_cor",
        "all": "all",
    }
    return mapping.get(raw, raw)


def _namespace_prefix_for_archive(archive_name: str) -> str | None:
    name = str(archive_name or "").upper()
    if name.startswith("JPPFS_"):
        return "jppfs_cor"
    if name.startswith("JPCRP_"):
        return "jpcrp_cor"
    if name.startswith("ALL_"):
        return "all"
    return None


def _parse_taxonomy_date(archive_name: str) -> str | None:
    match = re.search(r"_(\d{8})\.zip$", str(archive_name or ""), re.IGNORECASE)
    if not match:
        return None
    token = match.group(1)
    return f"{token[0:4]}-{token[4:6]}-{token[6:8]}"


def _classify_statement_family(text: str | None, role_uri: str | None = None, source_file: str | None = None, namespace_prefix: str | None = None) -> str:
    haystack = " ".join(
        part for part in (str(text or ""), str(role_uri or ""), str(source_file or "")) if part
    ).lower()
    compact_haystack = re.sub(r"[^a-z0-9]+", "", haystack)

    if _normalise_namespace_prefix(namespace_prefix) == "jpcrp_cor":
        return "Disclosure"

    if (
        "balance sheet" in haystack
        or "balancesheet" in compact_haystack
        or re.search(r"(^|[_\-])bs([_\-.]|$)", haystack)
    ):
        return "BalanceSheet"
    if any(token in haystack for token in ("profit and loss", "statement of income", "income statement", "comprehensive income")):
        return "IncomeStatement"
    if any(
        token in compact_haystack
        for token in (
            "profitandloss",
            "statementofincome",
            "incomestatement",
            "statementofprofitandloss",
            "statementofcomprehensiveincome",
            "comprehensiveincome",
        )
    ):
        return "IncomeStatement"
    if re.search(r"(^|[_\-])pl([_\-.]|$)", haystack) or re.search(r"(^|[_\-])ss([_\-.]|$)", haystack):
        return "IncomeStatement"
    if (
        "cash flow" in haystack
        or "cashflow" in compact_haystack
        or "statementofcashflows" in compact_haystack
        or re.search(r"(^|[_\-])cf([_\-.]|$)", haystack)
    ):
        return "CashflowStatement"
    return "Other"


def _is_standard_statement_role(role_uri: str | None, statement_family: str | None) -> bool:
    if statement_family not in {"BalanceSheet", "IncomeStatement", "CashflowStatement"}:
        return False

    normalised_role = str(role_uri or "").strip().lower()
    if not normalised_role:
        return False

    if "disclosure.edinet-fsa.go.jp/role/" not in normalised_role:
        return True

    return re.search(r"/rol_std(?:_|$)", normalised_role) is not None


def _normalise_role_name(role_uri: str | None) -> str:
    role_name = str(role_uri or "").strip().rstrip("/")
    if not role_name:
        return ""
    return re.sub(r"[^0-9A-Za-z]+", "", role_name.rsplit("/", 1)[-1]).lower()


def _preferred_statement_role_names(statement_family: str | None) -> tuple[str, ...]:
    return {
        "BalanceSheet": (
            "rolstdbalancesheet",
            "stdbalancesheet",
            "balancesheet",
        ),
        "IncomeStatement": (
            "rolstdstatementofincome",
            "rolstdstatementofprofitandloss",
            "rolstdstatementofcomprehensiveincome",
            "stdstatementofincome",
            "stdstatementofprofitandloss",
            "stdstatementofcomprehensiveincome",
            "statementofincome",
            "statementofprofitandloss",
            "statementofcomprehensiveincome",
            "profitandloss",
            "comprehensiveincome",
            "incomestatement",
        ),
        "CashflowStatement": (
            "rolstdstatementofcashflows",
            "stdstatementofcashflows",
            "statementofcashflows",
            "cashflows",
            "cashflowstatement",
        ),
    }.get(statement_family, ())


def _statement_role_match_rank(role_name: str, preferred_names: tuple[str, ...]) -> int:
    for index, preferred_name in enumerate(preferred_names):
        if role_name == preferred_name:
            return index

    offset = len(preferred_names)
    for index, preferred_name in enumerate(preferred_names):
        if preferred_name and preferred_name in role_name:
            return offset + index

    return offset * 2


def _select_primary_statement_role_candidate(candidates: list[dict]) -> dict | None:
    if not candidates:
        return None

    def sort_key(candidate: dict) -> tuple[int, int, int, str, str]:
        statement_family = candidate.get("statement_family")
        preferred_names = _preferred_statement_role_names(statement_family)
        role_name = _normalise_role_name(candidate.get("role_uri"))
        concept_count = len(candidate.get("concept_qnames") or ())
        arc_count = len(candidate.get("arcs") or ())
        return (
            _statement_role_match_rank(role_name, preferred_names),
            -concept_count,
            -arc_count,
            role_name,
            str(candidate.get("role_uri") or ""),
        )

    return min(candidates, key=sort_key)


def _href_to_concept_qname(href: str | None) -> str | None:
    if not href or "#" not in href:
        return None
    fragment = href.split("#", 1)[1].strip()
    if not fragment:
        return None
    if ":" in fragment:
        return fragment
    match = re.match(r"^([A-Za-z0-9\-]+_[A-Za-z0-9\-]+)_(.+)$", fragment)
    if match:
        return f"{match.group(1)}:{match.group(2)}"
    return fragment


def _pick_primary_label(candidates: list[dict], language: str) -> str | None:
    filtered = [item for item in candidates if item.get("language") == language and item.get("label_text")]
    if not filtered:
        return None
    filtered.sort(
        key=lambda item: (
            0 if str(item.get("label_role") or "") == "http://www.xbrl.org/2003/role/label" else 1 if str(item.get("label_role") or "").endswith("/label") else 2,
            str(item.get("label_role") or ""),
            str(item.get("source_file") or ""),
        )
    )
    return filtered[0].get("label_text")


def _load_xml_from_zip(archive: zipfile.ZipFile, member_name: str) -> ET.Element:
    with archive.open(member_name) as handle:
        return ET.parse(handle).getroot()


_TAXONOMY_TABLE_NAME = "Taxonomy"
_TAXONOMY_COLUMNS = (
    "release_id",
    "statement_family",
    "value_type",
    "level",
    "concept_qname",
    "parent_concept_qname",
    "primary_label_en",
)
_ROLE_DERIVED_STATEMENT_FAMILIES = (
    "BalanceSheet",
    "IncomeStatement",
    "CashflowStatement",
)
_SUPPORTED_STATEMENT_FAMILIES = _ROLE_DERIVED_STATEMENT_FAMILIES + ("ShareMetrics",)
_SHARE_METRICS_CONCEPT_QNAMES = (
    "jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults",
    "jpcrp_cor:NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc",
    "jpcrp_cor:NumberOfIssuedSharesAsOfFilingDateIssuedSharesTotalNumberOfSharesEtc",
    "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults",
    "jpcrp_cor:TotalShareholderReturn",
    "jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults",
    "jpcrp_cor:DilutedEarningsPerShareSummaryOfBusinessResults",
    "jpcrp_cor:NetAssetsPerShareSummaryOfBusinessResults",
)
_NUMERIC_DATA_TYPE_TOKENS = (
    "monetaryitemtype",
    "sharesitemtype",
    "pershareitemtype",
    "decimalitemtype",
    "integeritemtype",
    "nonnegativeintegeritemtype",
    "positiveintegeritemtype",
    "doubleitemtype",
    "floatitemtype",
    "percentitemtype",
    "pureitemtype",
    "numericitemtype",
)
_STRING_DATA_TYPE_TOKENS = (
    "stringitemtype",
    "textblockitemtype",
)
_SEMANTIC_TRAILING_QUALIFIER_TOKENS = {
    "ca",
    "bnk",
    "cmd",
    "cna",
    "edu",
    "ele",
    "fnd",
    "gas",
    "hwy",
    "ins",
    "inv",
    "ivt",
    "lea",
    "liq",
    "med",
    "rw",
    "rwy",
    "sec",
    "spf",
    "wat",
}
_SEMANTIC_TRAILING_BUCKET_TOKENS = {
    "assets",
    "liabilities",
}
_CURRENT_ASSET_HINT_PHRASES = (
    ("agency", "accounts", "receivable"),
    ("allowance", "for", "doubtful", "accounts"),
    ("allowance", "for", "loan", "losses"),
    ("bills", "bought"),
    ("call", "loans"),
    ("cash",),
    ("deposits",),
    ("due", "from", "banks"),
    ("receivable",),
)
_NONCURRENT_ASSET_BLOCKLIST_TOKENS = {
    "deferred",
    "goodwill",
    "intangible",
    "investment",
    "investments",
    "long",
    "noncurrent",
    "property",
}
_CANONICAL_LABEL_CONTEXT_TOKENS = {
    "affiliates",
    "agency",
    "agents",
    "and",
    "associates",
    "building",
    "business",
    "commodity",
    "completed",
    "construction",
    "contracts",
    "customer",
    "customers",
    "directors",
    "employees",
    "for",
    "form",
    "from",
    "futures",
    "highway",
    "in",
    "management",
    "net",
    "of",
    "officers",
    "on",
    "or",
    "progress",
    "reinsurance",
    "road",
    "shareholders",
    "subsidiaries",
    "to",
    "transaction",
    "uncompleted",
}
_LEGACY_TAXONOMY_TABLES = (
    "taxonomy_releases",
    "taxonomy_files",
    "taxonomy_concepts",
    "taxonomy_labels",
    "taxonomy_roles",
    "taxonomy_presentation_arcs",
    "taxonomy_levels",
)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND lower(name) = lower(?) LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _release_id_for_listing_entry(listing_entry: TaxonomyListingEntry) -> str:
    return str(listing_entry.taxonomy_date or listing_entry.archive_name)


def _taxonomy_schema_sql() -> str:
    supported_families = "', '".join(_SUPPORTED_STATEMENT_FAMILIES)
    return f"""
        CREATE TABLE IF NOT EXISTS \"{_TAXONOMY_TABLE_NAME}\" (
            release_id TEXT NOT NULL,
            statement_family TEXT NOT NULL,
            value_type TEXT NOT NULL,
            level INTEGER NOT NULL,
            concept_qname TEXT NOT NULL,
            parent_concept_qname TEXT,
            primary_label_en TEXT NOT NULL,
            PRIMARY KEY (release_id, concept_qname),
            CHECK (statement_family IN ('{supported_families}')),
            CHECK (value_type IN ('number', 'string')),
            CHECK (level >= 0),
            CHECK (
                (level = 0 AND parent_concept_qname IS NULL)
                OR (level > 0 AND parent_concept_qname IS NOT NULL)
            )
        );

        CREATE INDEX IF NOT EXISTS idx_taxonomy_release_family_level
            ON \"{_TAXONOMY_TABLE_NAME}\"(release_id, statement_family, level);
        CREATE INDEX IF NOT EXISTS idx_taxonomy_release_parent
            ON \"{_TAXONOMY_TABLE_NAME}\"(release_id, parent_concept_qname);
    """


def _ensure_taxonomy_schema(conn: sqlite3.Connection) -> None:
    for table_name in _LEGACY_TAXONOMY_TABLES:
        conn.execute(f"DROP VIEW IF EXISTS {table_name}")
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    existing_columns = [
        str(row[1])
        for row in conn.execute(f'PRAGMA table_info("{_TAXONOMY_TABLE_NAME}")').fetchall()
    ]
    if existing_columns and existing_columns != list(_TAXONOMY_COLUMNS):
        conn.execute(f'DROP TABLE IF EXISTS "{_TAXONOMY_TABLE_NAME}"')

    conn.executescript(_taxonomy_schema_sql())


def _delete_release_namespace_rows(conn: sqlite3.Connection, release_id: str, namespace_prefix: str | None) -> None:
    if namespace_prefix:
        conn.execute(
            f'DELETE FROM "{_TAXONOMY_TABLE_NAME}" WHERE release_id = ? AND concept_qname LIKE ?',
            (str(release_id), f"{namespace_prefix}:%"),
        )
        return

    conn.execute(
        f'DELETE FROM "{_TAXONOMY_TABLE_NAME}" WHERE release_id = ?',
        (str(release_id),),
    )


def _taxonomy_rows_exist(conn: sqlite3.Connection, release_id: str, namespace_prefix: str | None) -> bool:
    if not _table_exists(conn, _TAXONOMY_TABLE_NAME):
        return False
    if namespace_prefix:
        row = conn.execute(
            f'SELECT 1 FROM "{_TAXONOMY_TABLE_NAME}" WHERE release_id = ? AND concept_qname LIKE ? LIMIT 1',
            (str(release_id), f"{namespace_prefix}:%"),
        ).fetchone()
        return row is not None
    row = conn.execute(
        f'SELECT 1 FROM "{_TAXONOMY_TABLE_NAME}" WHERE release_id = ? LIMIT 1',
        (str(release_id),),
    ).fetchone()
    return row is not None


def scrape_taxonomy_listing(page_url: str = DEFAULT_TAXONOMY_PAGE_URL) -> list[TaxonomyListingEntry]:
    response = requests.get(page_url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    entries: list[TaxonomyListingEntry] = []
    for row in soup.find_all("tr"):
        link = row.find("a", onclick=re.compile(r"onDownload\(", re.IGNORECASE))
        if link is None:
            continue
        onclick = link.get("onclick") or ""
        match = re.search(r"onDownload\('([^']+)'\)", onclick)
        if not match:
            continue

        archive_name = match.group(1)
        namespace_prefix = _namespace_prefix_for_archive(archive_name)
        if not namespace_prefix:
            continue

        cells = row.find_all("td")
        label_text = cells[0].get_text(" ", strip=True) if cells else ""
        year_match = re.search(r"EDINET Taxonomy\s+(\d{4})", label_text)
        entries.append(
            TaxonomyListingEntry(
                archive_name=archive_name,
                namespace_prefix=namespace_prefix,
                taxonomy_date=_parse_taxonomy_date(archive_name),
                release_label=label_text or None,
                release_year=int(year_match.group(1)) if year_match else None,
                source_page_url=page_url,
            )
        )

    release_meta_by_date = {
        entry.taxonomy_date: (entry.release_label, entry.release_year)
        for entry in entries
        if entry.namespace_prefix == "all" and entry.taxonomy_date
    }

    normalized: list[TaxonomyListingEntry] = []
    for entry in entries:
        release_label, release_year = entry.release_label, entry.release_year
        if entry.taxonomy_date in release_meta_by_date:
            mapped_label, mapped_year = release_meta_by_date[entry.taxonomy_date]
            release_label = mapped_label or release_label
            release_year = mapped_year or release_year
        normalized.append(
            TaxonomyListingEntry(
                archive_name=entry.archive_name,
                namespace_prefix=entry.namespace_prefix,
                taxonomy_date=entry.taxonomy_date,
                release_label=release_label,
                release_year=release_year,
                source_page_url=entry.source_page_url,
            )
        )

    return normalized


def download_taxonomy_archive_bytes(
    archive_name: str,
    page_url: str = DEFAULT_TAXONOMY_PAGE_URL,
    session: requests.Session | None = None,
) -> bytes:
    managed_session = session is None
    session = session or requests.Session()
    try:
        page_response = session.get(page_url, timeout=30)
        page_response.raise_for_status()

        gx_state_raw = _extract_hidden_input_value(page_response.text, "GXState")
        gx_state = json.loads(gx_state_raw)
        ajax_iv = gx_state.get("GX_AJAX_IV")
        if not ajax_iv:
            raise ValueError("Taxonomy download page is missing the GX_AJAX_IV token.")

        payload = _build_download_payload(gx_state, archive_name)
        headers = _build_download_headers(gx_state, page_url)
        post_url = f"{page_url}?{str(ajax_iv).lower()},gx-no-cache={int(time.time() * 1000)}"
        download_response = session.post(
            post_url,
            data=json.dumps(payload, separators=(",", ":")),
            headers=headers,
            timeout=60,
        )
        download_response.raise_for_status()
        return _extract_download_bytes(download_response.json())
    finally:
        if managed_session:
            session.close()


def _parse_concepts(
    archive: zipfile.ZipFile,
    release_id: int,
    namespace_prefix: str,
    concept_xsd_paths: list[str],
) -> dict[str, dict]:
    concepts: dict[str, dict] = {}
    for member_name in concept_xsd_paths:
        root = _load_xml_from_zip(archive, member_name)
        namespace_uri = root.get("targetNamespace")
        for element in root.findall(f"{_XSD_NS}element"):
            concept_name = element.get("name")
            element_id = element.get("id")
            if element_id and re.match(r"^[A-Za-z0-9\-]+_[A-Za-z0-9\-]+_.+$", element_id):
                concept_qname = _href_to_concept_qname(f"#{element_id}")
            else:
                concept_qname = f"{namespace_prefix}:{concept_name}" if concept_name else None
            if not concept_qname or not concept_name:
                continue

            concepts[concept_qname] = {
                "release_id": release_id,
                "namespace_prefix": namespace_prefix,
                "namespace_uri": namespace_uri,
                "concept_qname": concept_qname,
                "concept_name": concept_name,
                "element_id": element_id,
                "period_type": element.get(f"{_XBRLI_NS}periodType"),
                "balance": element.get(f"{_XBRLI_NS}balance"),
                "is_abstract": 1 if str(element.get("abstract", "false")).lower() == "true" else 0,
                "data_type": element.get("type"),
                "substitution_group": element.get("substitutionGroup"),
                "statement_family_default": None,
                "primary_role_uri": None,
                "primary_parent_concept_qname": None,
                "primary_line_order": None,
                "primary_line_depth": None,
                "primary_label": None,
                "primary_label_en": None,
            }
    return concepts


def _parse_roles(
    archive: zipfile.ZipFile,
    release_id: int,
    namespace_prefix: str,
    role_xsd_paths: list[str],
) -> dict[str, dict]:
    roles: dict[str, dict] = {}
    for member_name in role_xsd_paths:
        root = _load_xml_from_zip(archive, member_name)
        for role_type in root.findall(f".//{_LINK_NS}roleType"):
            role_uri = role_type.get("roleURI")
            if not role_uri:
                continue
            definition_node = role_type.find(f"{_LINK_NS}definition")
            definition = "".join(definition_node.itertext()).strip() if definition_node is not None else ""
            roles[role_uri] = {
                "release_id": release_id,
                "namespace_prefix": namespace_prefix,
                "role_uri": role_uri,
                "role_label": definition or role_type.get("id") or role_uri,
                "definition": definition or None,
                "statement_family": _classify_statement_family(definition, role_uri=role_uri, source_file=member_name, namespace_prefix=namespace_prefix),
                "source_file": member_name,
            }
    return roles


def _parse_labels(
    archive: zipfile.ZipFile,
    release_id: int,
    namespace_prefix: str,
    label_paths: list[str],
) -> dict[str, list[dict]]:
    label_rows_by_concept: dict[str, list[dict]] = {}
    for member_name in label_paths:
        root = _load_xml_from_zip(archive, member_name)
        loc_map: dict[str, str] = {}
        label_resources: dict[str, dict] = {}

        for loc in root.findall(f".//{_LINK_NS}loc"):
            loc_label = loc.get(f"{_XLINK_NS}label")
            concept_qname = _href_to_concept_qname(loc.get(f"{_XLINK_NS}href"))
            if loc_label and concept_qname:
                loc_map[loc_label] = concept_qname

        for label in root.findall(f".//{_LINK_NS}label"):
            resource_label = label.get(f"{_XLINK_NS}label")
            if not resource_label:
                continue
            label_resources[resource_label] = {
                "label_role": label.get(f"{_XLINK_NS}role"),
                "language": label.get(_XML_LANG),
                "label_text": "".join(label.itertext()).strip(),
            }

        for arc in root.findall(f".//{_LINK_NS}labelArc"):
            from_label = arc.get(f"{_XLINK_NS}from")
            to_label = arc.get(f"{_XLINK_NS}to")
            concept_qname = loc_map.get(from_label)
            label_resource = label_resources.get(to_label)
            if not concept_qname or not label_resource:
                continue
            if concept_qname.split(":", 1)[0] != namespace_prefix:
                continue
            label_rows_by_concept.setdefault(concept_qname, []).append(
                {
                    "release_id": release_id,
                    "namespace_prefix": namespace_prefix,
                    "concept_qname": concept_qname,
                    "label_role": label_resource.get("label_role"),
                    "language": label_resource.get("language"),
                    "label_text": label_resource.get("label_text"),
                    "source_file": member_name,
                }
            )

    return label_rows_by_concept


def _compute_arc_depths(arcs: list[dict]) -> list[dict]:
    children = {arc["child_concept_qname"] for arc in arcs if arc.get("child_concept_qname")}
    parents = {arc["parent_concept_qname"] for arc in arcs if arc.get("parent_concept_qname")}
    roots = sorted(parent for parent in parents if parent not in children)
    adjacency: dict[str | None, list[dict]] = {}
    for arc in arcs:
        adjacency.setdefault(arc.get("parent_concept_qname"), []).append(arc)
    for arc_list in adjacency.values():
        arc_list.sort(key=lambda item: (item.get("order_value") or 0.0, item.get("child_concept_qname") or ""))

    depths: dict[tuple[str | None, str], int] = {}

    def walk(parent: str | None, depth: int) -> None:
        for arc in adjacency.get(parent, []):
            child = arc.get("child_concept_qname")
            if not child:
                continue
            key = (parent, child)
            if key in depths and depths[key] <= depth + 1:
                continue
            depths[key] = depth + 1
            walk(child, depth + 1)

    if roots:
        for root in roots:
            walk(root, 0)
    else:
        walk(None, 0)

    enriched = []
    for arc in arcs:
        key = (arc.get("parent_concept_qname"), arc.get("child_concept_qname"))
        arc["line_depth"] = depths.get(key, 1)
        enriched.append(arc)
    return enriched


def _primary_candidate_sort_key(candidate: dict) -> tuple[int, float, str]:
    return (
        0 if candidate.get("primary_parent_concept_qname") else 1,
        candidate.get("primary_line_depth") if candidate.get("primary_line_depth") is not None else 10**9,
        candidate.get("primary_line_order") if candidate.get("primary_line_order") is not None else 10**9,
        str(candidate.get("primary_role_uri") or ""),
    )


def _register_primary_candidate(primary_metadata: dict[str, dict], concept_qname: str | None, candidate: dict) -> None:
    if not concept_qname:
        return
    existing = primary_metadata.get(concept_qname)
    if existing is None or _primary_candidate_sort_key(candidate) < _primary_candidate_sort_key(existing):
        primary_metadata[concept_qname] = candidate


def _humanise_concept_name(concept_name: str | None) -> str | None:
    text = str(concept_name or "").strip()
    if not text:
        return None
    text = re.sub(r"(Abstract(?:[A-Z]+)?|LineItems|Heading|Table)$", "", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", text)
    return text.replace("_", " ").strip() or None


def _primary_label_en_for_concept(concept: dict) -> str:
    label = (
        concept.get("primary_label_en")
        or concept.get("primary_label")
        or _humanise_concept_name(concept.get("concept_name"))
        or str(concept.get("concept_qname") or "").split(":", 1)[-1]
    )
    return str(label).strip()


def _value_type_for_concept(concept: dict) -> str:
    is_abstract = concept.get("is_abstract")
    if isinstance(is_abstract, str):
        is_abstract = is_abstract.strip().lower() in {"1", "true", "yes"}
    if is_abstract:
        return "string"

    normalized_type = re.sub(r"[^a-z0-9]+", "", str(concept.get("data_type") or "").lower())
    if any(token in normalized_type for token in _NUMERIC_DATA_TYPE_TOKENS):
        return "number"
    if any(token in normalized_type for token in _STRING_DATA_TYPE_TOKENS):
        return "string"
    if concept.get("balance") or str(concept.get("period_type") or "").strip().lower() in {"instant", "duration"}:
        return "number"
    return "string"


def _validate_taxonomy_rows(rows: list[dict]) -> None:
    rows_by_key: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (str(row.get("release_id") or ""), str(row.get("concept_qname") or ""))
        if key in rows_by_key:
            raise ValueError(f"Duplicate taxonomy concept row detected for {key[0]} / {key[1]}")
        rows_by_key[key] = row

    for row in rows:
        release_id = str(row.get("release_id") or "")
        concept_qname = str(row.get("concept_qname") or "")
        level = int(row.get("level") or 0)
        parent_concept_qname = row.get("parent_concept_qname")

        if level == 0:
            if parent_concept_qname:
                raise ValueError(
                    f"Taxonomy concept {concept_qname} in release {release_id} has level 0 but a parent."
                )
            continue

        if not parent_concept_qname:
            raise ValueError(
                f"Taxonomy concept {concept_qname} in release {release_id} has level {level} without a parent."
            )

        parent_key = (release_id, str(parent_concept_qname))
        parent_row = rows_by_key.get(parent_key)
        if parent_row is None:
            raise ValueError(
                f"Taxonomy concept {concept_qname} in release {release_id} references missing parent {parent_concept_qname}."
            )
        if parent_row.get("statement_family") != row.get("statement_family"):
            raise ValueError(
                f"Taxonomy concept {concept_qname} in release {release_id} crosses statement families via parent {parent_concept_qname}."
            )
        if int(parent_row.get("level") or 0) != level - 1:
            raise ValueError(
                f"Taxonomy concept {concept_qname} in release {release_id} is not exactly one level below its parent {parent_concept_qname}."
            )


def _is_statement_root_wrapper(concept_qname: str | None, statement_family: str | None) -> bool:
    if not concept_qname or not statement_family:
        return False

    local_name = re.sub(r"[^0-9A-Za-z]+", "", str(concept_qname).split(":", 1)[-1]).lower()
    if not local_name:
        return False

    family_tokens = {
        "BalanceSheet": ("balancesheet",),
        "IncomeStatement": (
            "statementofincome",
            "statementofprofitandloss",
            "statementofcomprehensiveincome",
            "profitandloss",
            "comprehensiveincome",
            "incomestatement",
        ),
        "CashflowStatement": ("statementofcashflows", "cashflows"),
    }.get(statement_family, ())
    if not family_tokens:
        return False

    if not (
        local_name.endswith("abstract")
        or local_name.endswith("lineitems")
        or local_name.endswith("heading")
        or local_name.endswith("table")
    ):
        return False

    return any(token in local_name for token in family_tokens)


def _build_taxonomy_level_rows(
    release_id: int,
    namespace_prefix: str,
    concepts: dict[str, dict],
) -> list[dict]:
    del namespace_prefix
    path_cache: dict[str, list[str]] = {}
    label_key_cache: dict[str, str] = {}
    local_name_tokens_cache: dict[str, tuple[str, ...]] = {}
    local_name_core_cache: dict[str, tuple[tuple[str, ...], int, int]] = {}
    label_tokens_cache: dict[str, tuple[str, ...]] = {}
    normalized_label_cache: dict[str, str] = {}
    abstract_replacement_cache: dict[str, str | None] = {}
    canonical_concept_cache: dict[str, str] = {}
    visible_parent_cache: dict[str, str | None] = {}
    standardized_parent_cache: dict[str, str | None] = {}
    semantic_anchor_cache: dict[str, str] = {}
    visible_path_cache: dict[str, list[str]] = {}

    def build_path(concept_qname: str, visiting: set[str] | None = None) -> list[str]:
        cached = path_cache.get(concept_qname)
        if cached is not None:
            return cached

        if visiting is None:
            visiting = set()
        if concept_qname in visiting:
            return [concept_qname]

        concept = concepts.get(concept_qname) or {}
        parent_qname = concept.get("primary_parent_concept_qname")
        path: list[str] = []
        if parent_qname and parent_qname in concepts:
            path.extend(build_path(parent_qname, visiting | {concept_qname}))
        if not path or path[-1] != concept_qname:
            path.append(concept_qname)
        path_cache[concept_qname] = path
        return path

    def is_abstract_concept(concept_qname: str | None) -> bool:
        concept = concepts.get(concept_qname or "") or {}
        value = concept.get("is_abstract")
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes"}
        if value is None:
            local_name = str(concept_qname or "").split(":", 1)[-1].lower()
            return local_name.endswith("abstract")
        return bool(value)

    def is_excluded_concept(concept_qname: str | None) -> bool:
        if not concept_qname:
            return True
        concept = concepts.get(concept_qname) or {}
        statement_family = concept.get("statement_family_default")
        if statement_family not in _SUPPORTED_STATEMENT_FAMILIES:
            return True
        substitution_group = str(concept.get("substitution_group") or "").lower()
        local_name = str(concept_qname).split(":", 1)[-1].lower()
        data_type = str(concept.get("data_type") or "").lower()
        if "dimensionitem" in substitution_group or "hypercubeitem" in substitution_group:
            return True
        if "domainitemtype" in data_type or local_name.endswith("member") or local_name.endswith("axis"):
            return True
        return _is_statement_root_wrapper(concept_qname, statement_family)

    def direct_named_paired_abstract_qname(concept_qname: str | None) -> str | None:
        if not concept_qname:
            return None
        concept = concepts.get(concept_qname)
        if concept is None:
            return None
        local_name = str(concept_qname).split(":", 1)[-1]
        if local_name.lower().endswith("abstract"):
            return None
        if ":" in str(concept_qname):
            prefix, _ = str(concept_qname).split(":", 1)
            candidate_qname = f"{prefix}:{local_name}Abstract"
        else:
            candidate_qname = f"{local_name}Abstract"
        candidate = concepts.get(candidate_qname)
        if not candidate:
            return None
        if candidate.get("statement_family_default") != concept.get("statement_family_default"):
            return None
        if is_excluded_concept(candidate_qname):
            return None
        return candidate_qname

    def normalized_label_key(value: str | None) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())

    def concept_label_key(concept_qname: str) -> str:
        cached = label_key_cache.get(concept_qname)
        if cached is not None:
            return cached
        concept = concepts.get(concept_qname) or {}
        key = normalized_label_key(
            _primary_label_en_for_concept(
                {
                    **concept,
                    "concept_qname": concept_qname,
                }
            )
        )
        label_key_cache[concept_qname] = key
        return key

    def semantic_tokens(value: str | None) -> tuple[str, ...]:
        text = str(value or "")
        if not text:
            return ()
        text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
        text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", text)
        text = re.sub(r"([A-Za-z])([0-9])", r"\1 \2", text)
        text = re.sub(r"([0-9])([A-Za-z])", r"\1 \2", text)
        return tuple(token for token in re.findall(r"[A-Za-z0-9]+", text.lower()) if token)

    def concept_local_name_tokens(concept_qname: str) -> tuple[str, ...]:
        cached = local_name_tokens_cache.get(concept_qname)
        if cached is not None:
            return cached
        local_name = str(concept_qname).split(":", 1)[-1]
        tokens = semantic_tokens(local_name)
        local_name_tokens_cache[concept_qname] = tokens
        return tokens

    def concept_local_name_core(concept_qname: str) -> tuple[tuple[str, ...], int, int]:
        cached = local_name_core_cache.get(concept_qname)
        if cached is not None:
            return cached

        tokens = list(concept_local_name_tokens(concept_qname))
        bucket_tokens_removed = 0
        qualifier_tokens_removed = 0
        while tokens:
            tail = tokens[-1]
            if tail in _SEMANTIC_TRAILING_BUCKET_TOKENS:
                tokens.pop()
                bucket_tokens_removed += 1
                continue
            if tail in _SEMANTIC_TRAILING_QUALIFIER_TOKENS or tail.isdigit():
                tokens.pop()
                qualifier_tokens_removed += 1
                continue
            break

        result = (tuple(tokens), bucket_tokens_removed, qualifier_tokens_removed)
        local_name_core_cache[concept_qname] = result
        return result

    def concept_primary_label_tokens(concept_qname: str) -> tuple[str, ...]:
        cached = label_tokens_cache.get(concept_qname)
        if cached is not None:
            return cached
        concept = concepts.get(concept_qname) or {}
        label = _primary_label_en_for_concept(
            {
                **concept,
                "concept_qname": concept_qname,
            }
        )
        tokens = semantic_tokens(label)
        label_tokens_cache[concept_qname] = tokens
        return tokens

    def contains_phrase(tokens: tuple[str, ...], phrase: tuple[str, ...]) -> bool:
        if not phrase or len(tokens) < len(phrase):
            return False
        if len(phrase) == 1:
            return phrase[0] in tokens
        return any(tokens[index : index + len(phrase)] == phrase for index in range(len(tokens) - len(phrase) + 1))

    def canonical_label_match_score(
        label_tokens: tuple[str, ...],
        anchor_tokens: tuple[str, ...],
    ) -> tuple[int, int] | None:
        if not label_tokens or not anchor_tokens or len(anchor_tokens) > len(label_tokens):
            return None

        for index in range(len(label_tokens) - len(anchor_tokens) + 1):
            if label_tokens[index : index + len(anchor_tokens)] != anchor_tokens:
                continue
            extra_tokens = label_tokens[:index] + label_tokens[index + len(anchor_tokens) :]
            if not extra_tokens:
                return (0, index)
            if extra_tokens == ("other",):
                continue
            if all(token in _CANONICAL_LABEL_CONTEXT_TOKENS for token in extra_tokens):
                return (len(extra_tokens), index)
        return None

    def is_clear_generic_anchor_candidate(concept_qname: str) -> bool:
        local_tokens, _bucket_tokens_removed, _qualifier_tokens_removed = concept_local_name_core(concept_qname)
        label_tokens = concept_primary_label_tokens(concept_qname)
        if not local_tokens or not label_tokens:
            return False
        if local_tokens == label_tokens:
            return True
        if len(local_tokens) > len(label_tokens) and local_tokens[: len(label_tokens)] == label_tokens:
            return all(token.isdigit() for token in local_tokens[len(label_tokens) :])
        return False

    def should_standardize_to_current_assets(concept_qname: str) -> bool:
        concept = concepts.get(concept_qname) or {}
        if concept.get("statement_family_default") != "BalanceSheet":
            return False
        if is_abstract_concept(concept_qname):
            return False
        if visible_parent_qname(concept_qname) != "jppfs_cor:Assets":
            return False

        label_tokens = concept_primary_label_tokens(concept_qname)
        if any(token in _NONCURRENT_ASSET_BLOCKLIST_TOKENS for token in label_tokens):
            return False
        return any(contains_phrase(label_tokens, phrase) for phrase in _CURRENT_ASSET_HINT_PHRASES)

    def normalized_primary_label_en(concept_qname: str) -> str:
        cached = normalized_label_cache.get(concept_qname)
        if cached is not None:
            return cached

        concept = concepts.get(concept_qname) or {}
        label = _primary_label_en_for_concept(
            {
                **concept,
                "concept_qname": concept_qname,
            }
        )
        if concept_qname not in visible_concept_qnames or is_abstract_concept(concept_qname):
            normalized_label_cache[concept_qname] = label
            return label

        family = concept.get("statement_family_default")
        parent_qname = standardized_parent_qname(concept_qname)
        current_core = concept_local_name_core(concept_qname)[0]
        current_label_tokens = concept_primary_label_tokens(concept_qname)
        best_score = None
        best_label = label

        for anchor_qname in clear_anchor_candidates_by_parent.get((family, parent_qname), []):
            if anchor_qname == concept_qname:
                continue

            anchor = concepts.get(anchor_qname) or {}
            anchor_label = _primary_label_en_for_concept(
                {
                    **anchor,
                    "concept_qname": anchor_qname,
                }
            )
            anchor_core, anchor_bucket_tokens_removed, anchor_qualifier_tokens_removed = concept_local_name_core(anchor_qname)
            if not anchor_label or not anchor_core:
                continue

            if current_core and anchor_core == current_core:
                score = (
                    0,
                    0,
                    0,
                    -len(anchor_core),
                    anchor_bucket_tokens_removed + anchor_qualifier_tokens_removed,
                    anchor_qname,
                )
            else:
                match_score = canonical_label_match_score(current_label_tokens, concept_primary_label_tokens(anchor_qname))
                if match_score is None:
                    continue
                extra_count, index = match_score
                score = (
                    1,
                    extra_count,
                    index,
                    -len(anchor_core),
                    anchor_bucket_tokens_removed + anchor_qualifier_tokens_removed,
                    anchor_qname,
                )

            if best_score is None or score < best_score:
                best_score = score
                best_label = anchor_label

        normalized_label_cache[concept_qname] = best_label
        return best_label

    non_abstract_candidates: dict[tuple[str | None, str], list[str]] = {}
    for concept_qname, concept in concepts.items():
        if is_excluded_concept(concept_qname) or is_abstract_concept(concept_qname):
            continue
        key = (concept.get("statement_family_default"), concept_label_key(concept_qname))
        non_abstract_candidates.setdefault(key, []).append(concept_qname)

    def abstract_replacement_qname(concept_qname: str | None) -> str | None:
        if not concept_qname:
            return None
        if concept_qname in abstract_replacement_cache:
            return abstract_replacement_cache[concept_qname]
        if is_excluded_concept(concept_qname) or not is_abstract_concept(concept_qname):
            abstract_replacement_cache[concept_qname] = None
            return None

        concept = concepts.get(concept_qname) or {}
        candidate_key = (concept.get("statement_family_default"), concept_label_key(concept_qname))
        candidates = non_abstract_candidates.get(candidate_key, [])
        best_qname = None
        best_score = None
        for candidate_qname in candidates:
            candidate = concepts.get(candidate_qname) or {}
            raw_parent_qname = candidate.get("primary_parent_concept_qname")
            concept_parent_qname = concept.get("primary_parent_concept_qname")
            candidate_path = build_path(candidate_qname)
            score = (
                0 if direct_named_paired_abstract_qname(candidate_qname) == concept_qname else 1,
                0 if raw_parent_qname == concept_qname else 1,
                0 if raw_parent_qname and concept_parent_qname and raw_parent_qname == concept_parent_qname else 1,
                0 if concept_qname in candidate_path[:-1] else 1,
                len(candidate_path),
                candidate_qname,
            )
            if score[:4] == (1, 1, 1, 1):
                continue
            if best_score is None or score < best_score:
                best_score = score
                best_qname = candidate_qname

        abstract_replacement_cache[concept_qname] = best_qname
        return best_qname

    def canonical_concept_qname(concept_qname: str | None) -> str | None:
        if not concept_qname:
            return None
        cached = canonical_concept_cache.get(concept_qname)
        if cached is not None:
            return cached
        replacement_qname = abstract_replacement_qname(concept_qname)
        canonical_qname = replacement_qname or concept_qname
        canonical_concept_cache[concept_qname] = canonical_qname
        return canonical_qname

    def build_share_metrics_rows() -> list[dict]:
        rows: list[dict] = []
        for concept_qname in _SHARE_METRICS_CONCEPT_QNAMES:
            concept = concepts.get(concept_qname)
            if not concept:
                continue
            if _value_type_for_concept(concept) != "number":
                continue

            rows.append(
                {
                    "release_id": str(release_id),
                    "statement_family": "ShareMetrics",
                    "value_type": "number",
                    "concept_qname": concept_qname,
                    "primary_label_en": normalized_primary_label_en(concept_qname),
                    "parent_concept_qname": None,
                    "level": 0,
                }
            )

        return rows

    visible_concept_qnames = {
        concept_qname
        for concept_qname, concept in concepts.items()
        if concept.get("statement_family_default") in _ROLE_DERIVED_STATEMENT_FAMILIES
        and not is_excluded_concept(concept_qname)
        and canonical_concept_qname(concept_qname) == concept_qname
    }

    visible_non_abstract_by_label: dict[tuple[str | None, str], list[str]] = {}
    for concept_qname in visible_concept_qnames:
        if is_abstract_concept(concept_qname):
            continue
        concept = concepts.get(concept_qname) or {}
        key = (concept.get("statement_family_default"), concept_label_key(concept_qname))
        visible_non_abstract_by_label.setdefault(key, []).append(concept_qname)

    generic_anchor_by_group: dict[tuple[str | None, str], str] = {}
    for key, qnames in visible_non_abstract_by_label.items():
        if len(qnames) < 2:
            continue

        generic_candidates = [
            concept_qname
            for concept_qname in qnames
            if is_clear_generic_anchor_candidate(concept_qname)
        ]
        if not generic_candidates:
            continue

        generic_candidates.sort(
            key=lambda concept_qname: (
                concept_local_name_core(concept_qname)[1],
                concept_local_name_core(concept_qname)[2],
                len(concept_local_name_core(concept_qname)[0]),
                concept_local_name_core(concept_qname)[0],
                concept_qname,
            )
        )
        generic_anchor_by_group[key] = generic_candidates[0]

    def semantic_anchor_qname(concept_qname: str) -> str:
        cached = semantic_anchor_cache.get(concept_qname)
        if cached is not None:
            return cached
        if concept_qname not in visible_concept_qnames or is_abstract_concept(concept_qname):
            semantic_anchor_cache[concept_qname] = concept_qname
            return concept_qname
        concept = concepts.get(concept_qname) or {}
        key = (concept.get("statement_family_default"), concept_label_key(concept_qname))
        anchor_qname = generic_anchor_by_group.get(key, concept_qname)
        semantic_anchor_cache[concept_qname] = anchor_qname
        return anchor_qname

    def visible_parent_qname(concept_qname: str) -> str | None:
        cached = visible_parent_cache.get(concept_qname)
        if concept_qname in visible_parent_cache:
            return cached

        parent_qname = None
        concept_label = concept_label_key(concept_qname)
        for ancestor_qname in reversed(build_path(concept_qname)[:-1]):
            canonical_qname = canonical_concept_qname(ancestor_qname)
            if not canonical_qname or canonical_qname == concept_qname:
                continue
            if canonical_qname in visible_concept_qnames:
                if concept_label and concept_label_key(canonical_qname) == concept_label:
                    continue
                parent_qname = canonical_qname
                break

        visible_parent_cache[concept_qname] = parent_qname
        return parent_qname

    def standardized_parent_qname(concept_qname: str) -> str | None:
        cached = standardized_parent_cache.get(concept_qname)
        if concept_qname in standardized_parent_cache:
            return cached

        anchor_qname = semantic_anchor_qname(concept_qname)
        if anchor_qname != concept_qname:
            parent_qname = standardized_parent_qname(anchor_qname)
        else:
            parent_qname = visible_parent_qname(concept_qname)
            if parent_qname == "jppfs_cor:Assets" and should_standardize_to_current_assets(concept_qname):
                parent_qname = "jppfs_cor:CurrentAssets"

        standardized_parent_cache[concept_qname] = parent_qname
        return parent_qname

    clear_anchor_candidates_by_parent: dict[tuple[str | None, str | None], list[str]] = {}
    for concept_qname in visible_concept_qnames:
        if is_abstract_concept(concept_qname) or not is_clear_generic_anchor_candidate(concept_qname):
            continue
        concept = concepts.get(concept_qname) or {}
        key = (concept.get("statement_family_default"), standardized_parent_qname(concept_qname))
        clear_anchor_candidates_by_parent.setdefault(key, []).append(concept_qname)

    def visible_path(concept_qname: str, visiting: set[str] | None = None) -> list[str]:
        cached = visible_path_cache.get(concept_qname)
        if cached is not None:
            return cached

        if visiting is None:
            visiting = set()
        if concept_qname in visiting:
            return [concept_qname]

        path: list[str] = []
        parent_qname = standardized_parent_qname(concept_qname)
        if parent_qname:
            path.extend(visible_path(parent_qname, visiting | {concept_qname}))
        if not path or path[-1] != concept_qname:
            path.append(concept_qname)
        visible_path_cache[concept_qname] = path
        return path

    rows: list[dict] = []
    for concept_qname, concept in concepts.items():
        statement_family = concept.get("statement_family_default")
        if not statement_family or concept_qname not in visible_concept_qnames:
            continue
        parent_concept_qname = standardized_parent_qname(concept_qname)
        level = max(len(visible_path(concept_qname)) - 1, 0)

        rows.append(
            {
                "release_id": str(release_id),
                "statement_family": statement_family,
                "value_type": _value_type_for_concept(concept),
                "concept_qname": concept_qname,
                "primary_label_en": normalized_primary_label_en(concept_qname),
                "parent_concept_qname": parent_concept_qname,
                "level": level,
            }
        )

    rows_by_qname = {row["concept_qname"]: row for row in rows}
    while rows_by_qname:
        child_counts = {concept_qname: 0 for concept_qname in rows_by_qname}
        for row in rows_by_qname.values():
            parent_qname = row.get("parent_concept_qname")
            if parent_qname in child_counts:
                child_counts[parent_qname] += 1

        orphan_string_qnames = [
            concept_qname
            for concept_qname, row in rows_by_qname.items()
            if row.get("value_type") == "string" and child_counts.get(concept_qname, 0) == 0
        ]
        if not orphan_string_qnames:
            break
        for concept_qname in orphan_string_qnames:
            rows_by_qname.pop(concept_qname, None)

    rows = list(rows_by_qname.values())
    existing_concepts = {row["concept_qname"] for row in rows}
    for row in build_share_metrics_rows():
        if row["concept_qname"] in existing_concepts:
            continue
        rows.append(row)
        existing_concepts.add(row["concept_qname"])

    rows.sort(
        key=lambda row: (
            row.get("statement_family") or "",
            int(row.get("level") or 0),
            row.get("parent_concept_qname") or "",
            row.get("concept_qname") or "",
        )
    )
    _validate_taxonomy_rows(rows)
    return rows


def _parse_presentation_arcs(
    archive: zipfile.ZipFile,
    release_id: int,
    namespace_prefix: str,
    pre_paths: list[str],
    roles: dict[str, dict],
) -> tuple[list[dict], dict[str, dict]]:
    arc_rows: list[dict] = []
    primary_metadata: dict[str, dict] = {}
    primary_role_candidates: dict[str, dict[str, dict]] = {}

    for member_name in pre_paths:
        root = _load_xml_from_zip(archive, member_name)
        for presentation_link in root.findall(f".//{_LINK_NS}presentationLink"):
            role_uri = presentation_link.get(f"{_XLINK_NS}role")
            loc_map: dict[str, str] = {}
            for loc in presentation_link.findall(f"{_LINK_NS}loc"):
                loc_label = loc.get(f"{_XLINK_NS}label")
                concept_qname = _href_to_concept_qname(loc.get(f"{_XLINK_NS}href"))
                if loc_label and concept_qname:
                    loc_map[loc_label] = concept_qname

            raw_arcs: list[dict] = []
            for arc in presentation_link.findall(f"{_LINK_NS}presentationArc"):
                parent_qname = loc_map.get(arc.get(f"{_XLINK_NS}from"))
                child_qname = loc_map.get(arc.get(f"{_XLINK_NS}to"))
                if not child_qname:
                    continue
                if child_qname.split(":", 1)[0] != namespace_prefix and (parent_qname or "").split(":", 1)[0] != namespace_prefix:
                    continue
                try:
                    order_value = float(arc.get("order") or 0.0)
                except ValueError:
                    order_value = 0.0
                raw_arcs.append(
                    {
                        "release_id": release_id,
                        "namespace_prefix": namespace_prefix,
                        "role_uri": role_uri,
                        "parent_concept_qname": parent_qname,
                        "child_concept_qname": child_qname,
                        "arcrole": arc.get(f"{_XLINK_NS}arcrole"),
                        "order_value": order_value,
                        "preferred_label_role": arc.get("preferredLabel"),
                        "source_file": member_name,
                    }
                )

            statement_family = (roles.get(role_uri) or {}).get("statement_family") or _classify_statement_family(
                role_uri,
                role_uri=role_uri,
                source_file=member_name,
                namespace_prefix=namespace_prefix,
            )
            use_for_primary = _is_standard_statement_role(role_uri, statement_family)
            enriched_arcs = _compute_arc_depths(raw_arcs)
            if use_for_primary:
                role_family_candidates = primary_role_candidates.setdefault(statement_family, {})
                candidate = role_family_candidates.setdefault(
                    str(role_uri or ""),
                    {
                        "statement_family": statement_family,
                        "role_uri": role_uri,
                        "root_qnames": set(),
                        "concept_qnames": set(),
                        "arcs": [],
                    },
                )
                candidate["root_qnames"].update(
                    {
                        arc.get("parent_concept_qname")
                        for arc in raw_arcs
                        if arc.get("parent_concept_qname")
                        and arc.get("parent_concept_qname").split(":", 1)[0] == namespace_prefix
                    }
                    - {
                        arc.get("child_concept_qname")
                        for arc in raw_arcs
                        if arc.get("child_concept_qname")
                    }
                )
                candidate["concept_qnames"].update(
                    qname
                    for arc in raw_arcs
                    for qname in (arc.get("parent_concept_qname"), arc.get("child_concept_qname"))
                    if qname and qname.split(":", 1)[0] == namespace_prefix
                )
                candidate["arcs"].extend(enriched_arcs)

            for arc in enriched_arcs:
                arc_rows.append(arc)

    for statement_family, candidates_by_role in primary_role_candidates.items():
        candidate = _select_primary_statement_role_candidate(list(candidates_by_role.values()))
        if not candidate:
            continue
        role_uri = candidate.get("role_uri")
        for root_qname in sorted(candidate.get("root_qnames") or ()):
            _register_primary_candidate(
                primary_metadata,
                root_qname,
                {
                    "statement_family_default": statement_family,
                    "primary_role_uri": role_uri,
                    "primary_parent_concept_qname": None,
                    "primary_line_order": 0.0,
                    "primary_line_depth": 0,
                },
            )

        for arc in candidate.get("arcs") or ():
            child_qname = arc.get("child_concept_qname")
            if not child_qname or child_qname.split(":", 1)[0] != namespace_prefix:
                continue
            _register_primary_candidate(
                primary_metadata,
                child_qname,
                {
                    "statement_family_default": statement_family,
                    "primary_role_uri": role_uri,
                    "primary_parent_concept_qname": arc.get("parent_concept_qname"),
                    "primary_line_order": arc.get("order_value"),
                    "primary_line_depth": arc.get("line_depth"),
                },
            )

    return arc_rows, primary_metadata


def _persist_taxonomy_package(
    conn: sqlite3.Connection,
    release_id: str,
    namespace_prefix: str,
    archive_name: str,
    archive_path: str,
    archive_bytes: bytes,
    downloaded_at: str,
) -> dict[str, int]:
    del archive_name, archive_path, downloaded_at

    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        names = archive.namelist()
        prefix_root = namespace_prefix.split("_", 1)[0]
        concept_xsd_paths = [
            name
            for name in names
            if re.search(rf"taxonomy/{prefix_root}/[^/]+/{prefix_root}_cor_.*\.xsd$", name)
            and "/deprecated/" not in name
        ]
        role_xsd_paths = [
            name
            for name in names
            if re.search(rf"taxonomy/{prefix_root}/[^/]+/{prefix_root}_rt_.*\.xsd$", name)
        ]
        label_paths = [
            name
            for name in names
            if f"taxonomy/{prefix_root}/" in name and "/label/" in name and name.endswith(".xml")
        ]
        pre_paths = [
            name
            for name in names
            if f"taxonomy/{prefix_root}/" in name and "/r/" in name and re.search(r"_pre[^/]*\.xml$", name)
        ]

        concepts = _parse_concepts(archive, release_id, namespace_prefix, concept_xsd_paths)
        roles = _parse_roles(archive, release_id, namespace_prefix, role_xsd_paths)
        labels_by_concept = _parse_labels(archive, release_id, namespace_prefix, label_paths)
        arcs, primary_metadata = _parse_presentation_arcs(archive, release_id, namespace_prefix, pre_paths, roles)

    for concept_qname, concept in concepts.items():
        labels = labels_by_concept.get(concept_qname, [])
        concept["primary_label"] = _pick_primary_label(labels, "ja")
        concept["primary_label_en"] = _pick_primary_label(labels, "en")
        concept.update(primary_metadata.get(concept_qname, {}))

    label_rows = [item for items in labels_by_concept.values() for item in items]
    taxonomy_rows = _build_taxonomy_level_rows(release_id, namespace_prefix, concepts)
    _delete_release_namespace_rows(conn, release_id, namespace_prefix)

    if taxonomy_rows:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO \"{_TAXONOMY_TABLE_NAME}\" (
                release_id,
                statement_family,
                value_type,
                level,
                concept_qname,
                parent_concept_qname,
                primary_label_en
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["release_id"],
                    row.get("statement_family"),
                    row.get("value_type"),
                    row.get("level"),
                    row["concept_qname"],
                    row.get("parent_concept_qname"),
                    row.get("primary_label_en"),
                )
                for row in taxonomy_rows
            ],
        )

    return {
        "concepts": len(concepts),
        "labels": len(label_rows),
        "roles": len(roles),
        "presentation_arcs": len(arcs),
        "taxonomy_rows": len(taxonomy_rows),
        "taxonomy_levels": len(taxonomy_rows),
    }


def _select_listing_entries(
    listing: list[TaxonomyListingEntry],
    release_selection: str = "all",
    release_years: list[int] | None = None,
    namespaces: list[str] | None = None,
) -> list[TaxonomyListingEntry]:
    wanted_namespaces = {
        _normalise_namespace_prefix(value)
        for value in (namespaces or ["jppfs_cor", "jpcrp_cor"])
        if _normalise_namespace_prefix(value)
    }
    filtered = [entry for entry in listing if entry.namespace_prefix in wanted_namespaces]
    if not filtered:
        return []

    selection = str(release_selection or "all").strip().lower()
    requested_years = {int(value) for value in (release_years or []) if str(value).strip()}

    if requested_years:
        return [entry for entry in filtered if entry.release_year in requested_years]

    if selection == "all":
        return filtered

    if selection == "latest":
        dated_entries = [entry for entry in filtered if entry.release_year is not None]
        if dated_entries:
            latest_year = max(entry.release_year for entry in dated_entries if entry.release_year is not None)
            return [entry for entry in filtered if entry.release_year == latest_year]
        latest_date = max(entry.taxonomy_date or "" for entry in filtered)
        return [entry for entry in filtered if entry.taxonomy_date == latest_date]

    return filtered


def sync_taxonomy_releases(
    target_database: str,
    release_selection: str = "all",
    release_years: list[int] | None = None,
    namespaces: list[str] | None = None,
    download_dir: str = "assets/taxonomy",
    force_download: bool = False,
    force_reparse: bool = False,
    page_url: str = DEFAULT_TAXONOMY_PAGE_URL,
) -> dict[str, int]:
    if not target_database:
        raise ValueError("target_database is required for taxonomy sync.")

    listing = scrape_taxonomy_listing(page_url=page_url)
    selected = _select_listing_entries(
        listing,
        release_selection=release_selection,
        release_years=release_years,
        namespaces=namespaces,
    )
    if not selected:
        logger.warning("Taxonomy sync found no matching releases for selection=%s years=%s namespaces=%s", release_selection, release_years, namespaces)
        return {
            "releases_processed": 0,
            "archives_processed": 0,
            "concepts": 0,
            "labels": 0,
            "roles": 0,
            "presentation_arcs": 0,
            "taxonomy_rows": 0,
            "taxonomy_levels": 0,
        }

    os.makedirs(download_dir, exist_ok=True)
    conn = sqlite3.connect(target_database)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        _ensure_taxonomy_schema(conn)

        stats = {
            "releases_processed": 0,
            "archives_processed": 0,
            "concepts": 0,
            "labels": 0,
            "roles": 0,
            "presentation_arcs": 0,
            "taxonomy_rows": 0,
            "taxonomy_levels": 0,
        }

        session = requests.Session()
        try:
            for entry in selected:
                release_id = _release_id_for_listing_entry(entry)
                archive_path = os.path.join(download_dir, entry.archive_name)
                downloaded_at = datetime.utcnow().isoformat(timespec="seconds")

                if os.path.exists(archive_path) and not force_download:
                    with open(archive_path, "rb") as handle:
                        archive_bytes = handle.read()
                else:
                    archive_bytes = download_taxonomy_archive_bytes(
                        entry.archive_name,
                        page_url=page_url,
                        session=session,
                    )
                    with open(archive_path, "wb") as handle:
                        handle.write(archive_bytes)

                if _taxonomy_rows_exist(conn, release_id, entry.namespace_prefix) and not force_reparse:
                    logger.info(
                        "Taxonomy archive %s already parsed for release %s; skipping reparse.",
                        entry.archive_name,
                        release_id,
                    )
                    stats["releases_processed"] += 1
                    continue

                package_stats = _persist_taxonomy_package(
                    conn,
                    release_id=release_id,
                    namespace_prefix=entry.namespace_prefix,
                    archive_name=entry.archive_name,
                    archive_path=archive_path,
                    archive_bytes=archive_bytes,
                    downloaded_at=downloaded_at,
                )
                for key, value in package_stats.items():
                    stats[key] += value
                stats["releases_processed"] += 1
                stats["archives_processed"] += 1
                conn.commit()
        finally:
            session.close()

        conn.commit()
        return stats
    finally:
        conn.close()


def import_local_taxonomy_xsd(
    target_database: str,
    xsd_file: str,
    namespace_prefix: str | None = None,
    release_label: str | None = None,
    release_year: int | None = None,
    taxonomy_date: str | None = None,
) -> dict[str, int]:
    if not target_database:
        raise ValueError("target_database is required for local taxonomy import.")
    if not xsd_file or not os.path.exists(xsd_file):
        raise FileNotFoundError(f"Taxonomy XSD file not found: {xsd_file}")

    inferred_prefix = _normalise_namespace_prefix(namespace_prefix)
    if not inferred_prefix:
        basename = os.path.basename(xsd_file).lower()
        if basename.startswith("jppfs"):
            inferred_prefix = "jppfs_cor"
        elif basename.startswith("jpcrp"):
            inferred_prefix = "jpcrp_cor"
        else:
            inferred_prefix = "unknown"

    del release_label, release_year

    release_id = str(taxonomy_date or date.today().isoformat())

    conn = sqlite3.connect(target_database)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        _ensure_taxonomy_schema(conn)

        with open(xsd_file, "rb") as handle:
            xsd_bytes = handle.read()
        root = ET.fromstring(xsd_bytes)
        namespace_uri = root.get("targetNamespace")

        concepts = {}
        for element in root.findall(f"{_XSD_NS}element"):
            concept_name = element.get("name")
            element_id = element.get("id")
            if element_id and re.match(r"^[A-Za-z0-9\-]+_[A-Za-z0-9\-]+_.+$", element_id):
                concept_qname = _href_to_concept_qname(f"#{element_id}")
            else:
                concept_qname = f"{inferred_prefix}:{concept_name}" if concept_name else None
            if not concept_qname or not concept_name:
                continue
            concepts[concept_qname] = (
                release_id,
                inferred_prefix,
                namespace_uri,
                concept_qname,
                concept_name,
                element_id,
                element.get(f"{_XBRLI_NS}periodType"),
                element.get(f"{_XBRLI_NS}balance"),
                1 if str(element.get("abstract", "false")).lower() == "true" else 0,
                element.get("type"),
                element.get("substitutionGroup"),
                _classify_statement_family(
                    concept_name,
                    source_file=os.path.basename(xsd_file),
                    namespace_prefix=inferred_prefix,
                ),
                None,
                None,
                None,
                None,
                None,
                None,
            )

        concept_rows = {
            concept_qname: {
                "concept_qname": concept_qname,
                "concept_name": values[4],
                "statement_family_default": values[11],
                "primary_role_uri": values[12],
                "primary_parent_concept_qname": values[13],
                "primary_line_order": values[14],
                "primary_line_depth": values[15],
                "primary_label": values[16],
                "primary_label_en": values[17],
                "is_abstract": values[8],
                "data_type": values[9],
            }
            for concept_qname, values in concepts.items()
        }
        taxonomy_rows = _build_taxonomy_level_rows(release_id, inferred_prefix, concept_rows)
        _delete_release_namespace_rows(conn, release_id, inferred_prefix)
        if taxonomy_rows:
            conn.executemany(
                f"""
                INSERT OR REPLACE INTO \"{_TAXONOMY_TABLE_NAME}\" (
                    release_id,
                    statement_family,
                    value_type,
                    level,
                    concept_qname,
                    parent_concept_qname,
                    primary_label_en
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["release_id"],
                        row.get("statement_family"),
                        row.get("value_type"),
                        row.get("level"),
                        row["concept_qname"],
                        row.get("parent_concept_qname"),
                        row.get("primary_label_en"),
                    )
                    for row in taxonomy_rows
                ],
            )
        conn.commit()
        return {
            "releases_processed": 1,
            "archives_processed": 1,
            "concepts": len(concepts),
            "labels": 0,
            "roles": 0,
            "presentation_arcs": 0,
            "taxonomy_rows": len(taxonomy_rows),
            "taxonomy_levels": len(taxonomy_rows),
        }
    finally:
        conn.close()


def load_release_rows(conn: sqlite3.Connection) -> list[dict]:
    if _table_exists(conn, _TAXONOMY_TABLE_NAME):
        release_ids = [
            str(row[0])
            for row in conn.execute(
                f'SELECT DISTINCT release_id FROM "{_TAXONOMY_TABLE_NAME}" ORDER BY release_id'
            ).fetchall()
        ]
        rows: list[dict] = []
        for release_id in release_ids:
            taxonomy_date = None
            release_year = None
            try:
                parsed_date = date.fromisoformat(release_id[0:10])
            except ValueError:
                parsed_date = None
            if parsed_date is not None:
                taxonomy_date = parsed_date.isoformat()
                release_year = parsed_date.year
            rows.append(
                {
                    "release_id": release_id,
                    "release_key": release_id,
                    "release_label": release_id,
                    "release_year": release_year,
                    "taxonomy_date": taxonomy_date,
                    "valid_from": taxonomy_date,
                    "valid_to": None,
                }
            )

        dated_indexes = [
            index
            for index, row in enumerate(rows)
            if row.get("taxonomy_date")
        ]
        for position, row_index in enumerate(dated_indexes[:-1]):
            next_index = dated_indexes[position + 1]
            next_date = date.fromisoformat(rows[next_index]["taxonomy_date"])
            rows[row_index]["valid_to"] = (next_date - timedelta(days=1)).isoformat()
        return rows

    if not _table_exists(conn, "taxonomy_releases"):
        return []

    rows = conn.execute(
        """
        SELECT release_id, release_key, release_label, release_year, taxonomy_date, valid_from, valid_to
        FROM taxonomy_releases
        ORDER BY taxonomy_date
        """
    ).fetchall()
    return [
        {
            "release_id": row[0],
            "release_key": row[1],
            "release_label": row[2],
            "release_year": row[3],
            "taxonomy_date": row[4],
            "valid_from": row[5],
            "valid_to": row[6],
        }
        for row in rows
    ]


def resolve_release_for_reference_date(releases: list[dict], reference_date: str | None) -> tuple[str | int | None, str | None, str | None]:
    if not reference_date:
        return None, None, None

    token = str(reference_date).strip()[0:10]
    if not token:
        return None, None, None

    try:
        ref_date = date.fromisoformat(token)
    except ValueError:
        return None, None, None

    best = None
    for release in releases:
        valid_from = release.get("valid_from")
        if not valid_from:
            continue
        try:
            start = date.fromisoformat(valid_from)
        except ValueError:
            continue
        valid_to = release.get("valid_to")
        end = None
        if valid_to:
            try:
                end = date.fromisoformat(valid_to)
            except ValueError:
                end = None

        if start <= ref_date and (end is None or ref_date <= end):
            best = release
    if best is None:
        historical = []
        for release in releases:
            valid_from = release.get("valid_from")
            try:
                start = date.fromisoformat(valid_from) if valid_from else None
            except ValueError:
                start = None
            if start and start <= ref_date:
                historical.append((start, release))
        if historical:
            historical.sort(key=lambda item: item[0])
            best = historical[-1][1]
            return best.get("release_id"), "date_fallback", f"Resolved using latest release on or before {token}"
        return None, None, None

    return best.get("release_id"), "date_window", None