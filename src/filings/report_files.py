"""Readable names for the HTML documents inside an EDINET filing package.

A filing ships its report as numbered inline-XBRL files (``0101010_honbun_…``,
``0105020_honbun_…``) plus auditor's reports (``jpaud-…``). The number says
little to a reader, but each file opens with the 【bracketed】 headings of the
statutory form it implements, so the first headings name the file. Standard
annual-report headings get an English label; anything else keeps its Japanese
heading.
"""

from __future__ import annotations

import html
import io
import re
import zipfile
from typing import Any

# How much of each member is read to find its opening headings.
_HEAD_BYTES = 160_000
_HEADING = re.compile(r"【([^】]{1,40})】")
_TAG = re.compile(r"<[^>]+>")
_CODE = re.compile(r"^(\d{7})_")

# Most specific first: "連結損益計算書" must win over "損益計算書". ``exact`` keys
# only match a whole heading, so "その他" does not claim "その他の参考情報".
_HEADING_LABELS: tuple[tuple[str, str, bool], ...] = (
    ("表紙", "Cover page", True),
    ("企業の概況", "Company overview", False),
    ("経営者による財政状態", "Management's discussion and analysis", False),
    ("経営方針、経営環境及び対処すべき課題等", "Business overview", False),
    ("事業の状況", "Business overview", True),
    ("重要な契約等", "Material contracts", False),
    ("研究開発活動", "Research and development", False),
    ("設備の状況", "Facilities", False),
    ("提出会社の株式事務の概要", "Share administration", False),
    ("提出会社の参考情報", "Reference information", False),
    ("提出会社の状況", "Corporate information", False),
    ("保証会社等の情報", "Guarantor information", False),
    ("経理の状況", "Financial information", False),
    ("連結財政状態計算書", "Consolidated statement of financial position", False),
    ("連結貸借対照表", "Consolidated balance sheet", False),
    ("連結損益及び包括利益計算書", "Consolidated statement of income and comprehensive income", False),
    ("連結損益計算書及び連結包括利益計算書", "Consolidated income statement", False),
    ("連結損益計算書", "Consolidated income statement", False),
    ("連結包括利益計算書", "Consolidated statement of comprehensive income", False),
    ("連結持分変動計算書", "Consolidated statement of changes in equity", False),
    ("連結株主資本等変動計算書", "Consolidated statement of changes in equity", False),
    ("連結キャッシュ・フロー計算書", "Consolidated cash flow statement", False),
    ("連結財務諸表注記", "Notes to the consolidated financial statements", False),
    ("連結附属明細表", "Consolidated supplementary schedules", False),
    ("連結財務諸表等", "Consolidated financial statements", False),
    ("貸借対照表", "Balance sheet (parent company)", False),
    ("損益計算書", "Income statement (parent company)", False),
    ("株主資本等変動計算書", "Statement of changes in equity (parent company)", False),
    ("キャッシュ・フロー計算書", "Cash flow statement (parent company)", False),
    ("注記事項", "Notes (parent company)", True),
    ("附属明細表", "Supplementary schedules (parent company)", False),
    ("主な資産及び負債の内容", "Major assets and liabilities", False),
    ("財務諸表等", "Financial statements (parent company)", False),
    ("その他", "Other", True),
)
# Headings that only introduce a more specific one in the same file.
_CONTAINERS = {
    "Financial information",
    "Consolidated financial statements",
    "Financial statements (parent company)",
}


def _label_for(heading: str) -> str | None:
    for key, label, exact in _HEADING_LABELS:
        if (heading == key) if exact else (key in heading):
            return label
    return None


def _opening_headings(archive: zipfile.ZipFile, member_path: str) -> list[str]:
    try:
        with archive.open(member_path) as handle:
            head = handle.read(_HEAD_BYTES)
    except (KeyError, zipfile.BadZipFile, RuntimeError, OSError):
        return []
    text = html.unescape(_TAG.sub(" ", head.decode("utf-8", errors="ignore")))
    headings: list[str] = []
    for match in _HEADING.finditer(text):
        heading = re.sub(r"\s+", "", match.group(1))
        if heading and heading != "企業情報" and heading not in headings:
            headings.append(heading)
        if len(headings) == 3:
            break
    return headings


def _group(filename: str, code: str | None) -> str:
    """The part of the statutory form a file belongs to; files sort by section number within it."""
    if filename.startswith("jpaud"):
        return "audit"
    if "_header_" in filename or code == "0000000":
        return "cover"
    if code and code.startswith("0105"):
        return "financials"
    if code and code[:4] in {"0106", "0107"}:
        return "shareholders"
    if code and code.startswith("01"):
        return "business"
    if code and code.startswith("02"):
        return "guarantor"
    return "other"


def _audit_label(filename: str) -> str:
    scope = "non-consolidated" if "-cn-" in filename else "consolidated" if "-cc-" in filename else ""
    return f"Auditor's report ({scope})" if scope else "Auditor's report"


def describe_report_files(archive_content: bytes | None, members: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Label, group, and order a filing's HTML members.

    ``members`` are artifact rows with ``artifact_id``, ``member_path`` and
    ``size_bytes``. Each result keeps those and adds ``label`` (English when the
    heading is a standard one), ``heading`` (the Japanese heading it came from),
    ``code`` (the form's section number), and ``group`` (cover, business,
    financials, shareholders, guarantor, audit, or other).
    """
    archive: zipfile.ZipFile | None = None
    if archive_content:
        try:
            archive = zipfile.ZipFile(io.BytesIO(archive_content))
        except zipfile.BadZipFile:
            archive = None
    described = []
    try:
        for member in members:
            path = str(member["member_path"])
            filename = path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            code_match = _CODE.match(filename)
            code = code_match.group(1) if code_match else None
            group = _group(filename, code)
            headings = _opening_headings(archive, path) if archive is not None else []
            labelled: list[tuple[str, str]] = []
            for heading in headings:
                label = _label_for(heading)
                if label and label not in (known for known, _ in labelled):
                    labelled.append((label, heading))
            specific = [pair for pair in labelled if pair[0] not in _CONTAINERS] or labelled
            source_heading = specific[0][1] if specific else headings[0] if headings else None
            if group == "audit":
                label = _audit_label(filename)
            elif group == "cover":
                label = "Cover page"
            elif specific:
                label = " · ".join(known for known, _ in specific[:2])
            elif headings:
                label = headings[0]
            else:
                label = f"Section {code}" if code else filename
            described.append({
                "artifact_id": member["artifact_id"],
                "member_path": path,
                "filename": filename,
                "size_bytes": member["size_bytes"],
                "label": label,
                "heading": source_heading,
                "code": code,
                "group": group,
            })
    finally:
        if archive is not None:
            archive.close()
    # Statutory order is the section number; auditor's reports close the package.
    return sorted(described, key=lambda item: (item["group"] == "audit", item["code"] is None, item["code"] or "", item["filename"]))
