"""Major shareholders and officers read from an annual securities report.

Every annual report (有価証券報告書) has two sections this module reads:

* 大株主の状況, the ten largest shareholders at the record date. Each one is
  tagged in inline XBRL with a name, an address, the shares held and the
  share of the company (``jpcrp_cor:NameMajorShareholders`` and its siblings,
  one context per rank). A company with several classes of shares adds a
  second list by voting rights.
* 役員の状況, the directors, auditors and executive officers at the filing
  date. Since the 2019 taxonomy each person is tagged with a title, a date of
  birth, a career summary, a term of office and the shares held, in a context
  named after the person; the filer's label files give the name in English.
  Reports filed before the shareholders' meeting add the slate proposed to it.
  Older reports, and those of some unlisted companies, carry the same table
  as plain HTML, which is read by its column headings instead.

Ratios are decimals (0.128 for 12.80%) and share counts are in shares, never
in the table's thousands.
"""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import asdict, dataclass, field
from html import unescape
from typing import Any

from bs4 import BeautifulSoup

from src.bonds.parsing import clean, compact, numeric, parse_date, table_grid

PARSER_VERSION = 1

_IX_TAG = re.compile(r"<(/?)ix:(nonNumeric|nonFraction)\b([^>]*?)(/?)>", re.IGNORECASE | re.DOTALL)
_ATTRIBUTE = re.compile(r'([\w:.-]+)\s*=\s*"([^"]*)"')
_CONTEXT = re.compile(r'<xbrli:context\b[^>]*\bid="([^"]+)"[^>]*>(.*?)</xbrli:context>', re.IGNORECASE | re.DOTALL)
_INSTANT = re.compile(r"<xbrli:instant>\s*([0-9-]{10})", re.IGNORECASE)
_MEMBER = re.compile(r"<xbrldi:explicitMember\b[^>]*>\s*([^<\s]+)\s*<", re.IGNORECASE)
_TAGS = re.compile(r"<[^>]+>")
_LINE_BREAKS = re.compile(r"<br\s*/?>|</tr>|</p>|</div>|</li>", re.IGNORECASE)
_RANK = re.compile(r"No(\d+)MajorShareholders(VotingRights)?Member")
_NUMBER = re.compile(r"\d+(?:\.\d+)?")
_LOC = re.compile(r'<link:loc\b[^>]*?xlink:href="[^"#]*#([^"]+)"[^>]*?xlink:label="([^"]+)"', re.IGNORECASE)
_LOC_REVERSED = re.compile(r'<link:loc\b[^>]*?xlink:label="([^"]+)"[^>]*?xlink:href="[^"#]*#([^"]+)"', re.IGNORECASE)
_LABEL_ARC = re.compile(r"<link:labelArc\b([^>]*)>", re.IGNORECASE)
_LABEL = re.compile(r"<link:label\b([^>]*)>([^<]*)</link:label>", re.IGNORECASE)
_STANDARD_LABEL = "http://www.xbrl.org/2003/role/label"
_JAPANESE = re.compile(r"[\u3040-\u30ff\u3400-\u9fff]")
_LATIN = re.compile(r"[A-Za-z]")
_KANJI = re.compile(r"[\u3400-\u9fff]")
_HIRAGANA_ONLY = re.compile(r"^[\u3040-\u309f]+$")
_NAME_BRACKET = re.compile(r"\([^()]*\)")
_NAME_NOTE = re.compile(r"\(\s*注\s*\d*\s*\)\s*\d*(?:\s*[、,.]\s*\d+)*|注\s*\d+(?:[、,.]\d+)*|[※*]\s*\d*")
# A date of birth written in the name cell: "(昭和24年8月25日生)", "1955年3月3日生", "(昭和27.11.23)".
_NAME_BIRTH = re.compile(r"\(?\s*(?:令和|平成|昭和|大正)?\s*(?:\d{1,4}|元)\s*[年.]\s*\d{1,2}\s*[月.]\s*\d{1,2}\s*日?\s*生?\s*\)?")
_GENDER_COUNT = re.compile(r"男性\s*(\d+)\s*名.{0,12}?女性\s*(\d+|[-‐－―ー─])\s*名")

_SHAREHOLDER_FIELDS = {
    "NameMajorShareholders": ("shares", "name"),
    "AddressMajorShareholders": ("shares", "address"),
    "NumberOfSharesHeld": ("shares", "shares"),
    "ShareholdingRatio": ("shares", "ratio"),
    "NameMajorShareholdersVotingRights": ("voting", "name"),
    "AddressMajorShareholdersVotingRights": ("voting", "address"),
    "NumberOfVotingRightsHeld": ("voting", "shares"),
    "RatioOfVotingRightsHeld": ("voting", "ratio"),
}
_OFFICER_SUFFIX = "InformationAboutDirectorsAndCorporateAuditors"
_OFFICER_FIELDS = {
    f"OfficialTitleOrPosition{_OFFICER_SUFFIX}": "title",
    f"Name{_OFFICER_SUFFIX}": "name",
    f"DateOfBirth{_OFFICER_SUFFIX}": "birth",
    f"CareerSummary{_OFFICER_SUFFIX}TextBlock": "career",
    f"CareerSummaryContinued{_OFFICER_SUFFIX}TextBlock": "career_more",
    f"TermOfOffice{_OFFICER_SUFFIX}": "term",
    f"NumberOfSharesHeldOrdinaryShares{_OFFICER_SUFFIX}": "shares",
    f"NumberOfSharesHeldOtherThanOrdinaryShares{_OFFICER_SUFFIX}": "other_shares",
}
_FOOTNOTES = "FootnotesDirectorsAndCorporateAuditors"
_EXECUTIVE_FOOTNOTES = "FootnotesExecutiveOfficers"
_COUNTS = {
    "NumberOfMaleDirectorsAndOtherOfficers": "men",
    "NumberOfFemaleDirectorsAndOtherOfficers": "women",
}
# The report's breakdown of ownership by kind of holder (所有者別状況): (key, holder-count suffix, percentage suffix).
HOLDER_KINDS = (
    ("financial_institutions", "FinancialInstitutions", "FinancialInstitutions"),
    ("securities_firms", "FinancialServiceProviders", "FinancialServiceProviders"),
    ("other_corporations", "OtherCorporations", "OtherCorporations"),
    ("foreign_investors", "ForeignInvestorsOtherThanIndividuals", "ForeignersOtherThanIndividuals"),
    ("foreign_individuals", "ForeignIndividualInvestors", "ForeignIndividuals"),
    ("individuals", "IndividualsAndOthers", "IndividualsAndOthers"),
    ("government", "NationalAndLocalGovernments", "NationalAndLocalGovernments"),
)
_STATISTICS = {
    "NumberOfShareholdersTotal": "shareholder_count",
    "NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc": "issued_shares",
    "TotalNumberOfSharesHeldTreasurySharesEtc": "treasury_shares",
    "ShareholdingRatioTreasurySharesEtc": "treasury_ratio",
    **{f"NumberOfShareholders{count}": f"holders:{key}" for key, count, _ in HOLDER_KINDS},
    **{f"PercentageOfShareholdings{ratio}": f"ratio:{key}" for key, _, ratio in HOLDER_KINDS},
}
_OFFICER_BLOCK = "InformationAboutOfficersTextBlock"
# Text that marks a page worth scanning: most of a report's pages hold neither section.
_PAGE_MARKERS = ("MajorShareholders", "DirectorsAndCorporateAuditors", _OFFICER_BLOCK, "PercentageOfShareholdings")

_ERA_OFFSETS = {"令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911}
_CAREER_DATE = re.compile(r"(令和|平成|昭和|大正)?\s*(\d{1,4}|元)\s*年\s*(?:(\d{1,2})\s*月)?(?:\s*(\d{1,2})\s*日)?")
_SAME_YEAR = re.compile(r"同\s*年\s*(?:(\d{1,2})\s*月)?(?:\s*(\d{1,2})\s*日)?")
_FISCAL_PERIOD = re.compile(r"(令和|平成)?\s*(\d{1,4}|元)\s*年\s*(\d{1,2})\s*月\s*期")
_NOTE_NUMBER = re.compile(r"^\s*(?:\(?\s*注\s*\)?|※)?\s*(\d{1,2})(?!\d)")
_NOTE_REFERENCE = re.compile(r"(?:注|※)\s*\)?\s*(\d{1,2})")
_OUTSIDE = re.compile(r"社外(?:取締役|監査役|役員)")
_TOTAL_WORDS = {"計", "合計", "小計"}
_DITTO = {"同上", "〃", "同左", "々", "″"}
_SHARE_UNITS = (("千株", 1_000.0), ("百株", 100.0), ("万株", 10_000.0), ("株", 1.0))


@dataclass
class Shareholder:
    seq: int
    rank: int
    basis: str = "shares"
    name: str = ""
    address: str = ""
    shares: float | None = None
    ratio: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Officer:
    seq: int
    name: str
    proposed: bool = False
    # The filer's English label for the person ("Toyoda, Akio"), and the element it names ("ToyodaAkioMember").
    name_en: str = ""
    member: str = ""
    title: str = ""
    birth_date: str | None = None
    career: list[tuple[str, str]] = field(default_factory=list)
    # When the term began and the fiscal period whose shareholders' meeting ends it; the filed wording is
    # kept only where it states no such period.
    term: str = ""
    term_start: str | None = None
    term_end: str | None = None
    shares: float | None = None
    other_shares: float | None = None
    outside: bool = False
    source: str = "tagged"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ParsedReport:
    shareholders: list[Shareholder] = field(default_factory=list)
    officers: list[Officer] = field(default_factory=list)
    shareholders_as_of: str | None = None
    officers_as_of: str | None = None
    # The table's own total row: what the listed shareholders hold together.
    top_shares: float | None = None
    top_ratio: float | None = None
    men: int | None = None
    women: int | None = None
    # Share statistics from the same section: holders of record, shares issued at the year end, the company's
    # own shares, and ownership by kind of holder as {key, ratio, holders}.
    shareholder_count: int | None = None
    issued_shares: float | None = None
    treasury_shares: float | None = None
    treasury_ratio: float | None = None
    distribution: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class _Fact:
    start: int
    name: str
    context: str
    attributes: dict[str, str]
    inner: str


# --- text ---------------------------------------------------------------

def plain(html: str) -> str:
    """The text of an HTML fragment on one line."""
    return clean(unescape(_TAGS.sub(" ", html)))


def lines(html: str) -> list[str]:
    """The text of an HTML fragment, one entry per table row, paragraph, or line break."""
    text = _TAGS.sub(" ", _LINE_BREAKS.sub("\n", html))
    return [line for line in (clean(unescape(part)) for part in text.split("\n")) if line]


def is_japanese(text: Any) -> bool:
    return bool(_JAPANESE.search(str(text or "")))


def person_name(text: Any) -> str:
    """A person's name as one or two words: ``豊 田 章 男`` → ``豊田章男``, ``渡 邉 光一郎`` → ``渡邉 光一郎``.

    Filings pad names to a fixed width with spaces between every character,
    and add what is not the name in brackets: a footnote mark, a reading, a
    registered or maiden name. A name written in Latin letters keeps its spaces.
    """
    value = clean(_NAME_NOTE.sub(" ", _NAME_BIRTH.sub(" ", clean(text))))
    for _ in range(3):
        trimmed = clean(_NAME_BRACKET.sub(" ", value))
        if trimmed == value or not trimmed:
            break
        value = trimmed
    value = clean(_NAME_NOTE.sub(" ", value)).strip(" ()")
    if _LATIN.search(value):
        return value
    tokens = value.split(" ")
    # A reading in hiragana written before the name: いしだ しょうじ 石田 昭二.
    reading = 0
    while reading < len(tokens) and _HIRAGANA_ONLY.match(tokens[reading]):
        reading += 1
    if 0 < reading < len(tokens) and any(_KANJI.search(token) for token in tokens[reading:]):
        tokens = tokens[reading:]
    merged: list[str] = []
    run = ""
    for token in tokens:
        if len(token) == 1:
            run += token
            continue
        if run:
            merged.append(run)
            run = ""
        merged.append(token)
    if run:
        merged.append(run)
    return " ".join(merged)


def _number(fact: _Fact) -> float | None:
    if fact.attributes.get("xsi:nil") == "true":
        return None
    match = _NUMBER.search(numeric(plain(fact.inner)))
    if not match:
        return None
    try:
        scale = int(fact.attributes.get("scale") or 0)
    except ValueError:
        scale = 0
    value = float(match.group(0)) * 10.0 ** scale
    if fact.attributes.get("sign") == "-":
        value = -value
    return round(value, 10)


def _year(era: str | None, text: str) -> int | None:
    year = 1 if text == "元" else int(text)
    if era:
        return year + _ERA_OFFSETS[era]
    return year if 1900 <= year <= 2100 else None


def _career_date(text: str, previous_year: int | None) -> tuple[str, int | None, int] | None:
    """A date at the start of a career line as ``YYYY-MM``, its year, and where it ends."""
    same = _SAME_YEAR.match(text)
    if same and previous_year:
        month = same.group(1)
        return (f"{previous_year:04d}-{int(month):02d}" if month else f"{previous_year:04d}"), previous_year, same.end()
    match = _CAREER_DATE.match(text)
    if not match:
        return None
    year = _year(match.group(1), match.group(2))
    if year is None:
        return None
    month = match.group(3)
    if month and not 1 <= int(month) <= 12:
        return None
    return (f"{year:04d}-{int(month):02d}" if month else f"{year:04d}"), year, match.end()


def career_entries(text_lines: list[str]) -> list[tuple[str, str]]:
    """Career lines as (``YYYY-MM``, what happened) pairs.

    A line is one entry when it starts with a date. Layouts that put the date
    and the text on separate lines, or all dates in one column and all texts
    in the next, are paired in order; text without a date continues the entry
    before it.
    """
    entries: list[list[str]] = []
    pending: list[str] = []
    year: int | None = None
    for line in (clean(text) for text in text_lines):
        if not line:
            continue
        found = _career_date(line, year)
        if found is None:
            if pending:
                entries.append([pending.pop(0), line])
            elif entries:
                entries[-1][1] = f"{entries[-1][1]} {line}".strip()
            else:
                entries.append(["", line])
            continue
        date, year, end = found
        rest = line[end:].strip(" 　:：")
        if rest.startswith("生"):
            # A date of birth repeated inside the career cell is not a career event.
            continue
        if rest:
            entries.append([date, rest])
        else:
            pending.append(date)
    entries.extend([date, ""] for date in pending)
    return [(date, text) for date, text in entries if date or text]


def term_end(text: str) -> str | None:
    """The fiscal period whose shareholders' meeting ends a term: ``2026年３月期に係る定時株主総会…まで`` → ``2026-03``."""
    periods = list(_FISCAL_PERIOD.finditer(clean(text)))
    if not periods:
        return None
    match = periods[-1]
    year = _year(match.group(1), match.group(2))
    month = int(match.group(3))
    if year is None or not 1 <= month <= 12:
        return None
    return f"{year:04d}-{month:02d}"


def _set_term(officer: Officer, text: str) -> None:
    officer.term_end = term_end(text)
    officer.term_start = parse_date(text) if officer.term_end else None
    # A cell that only points at a footnote nobody could resolve says nothing.
    pointer = _NOTE_REFERENCE.search(text) is not None and len(compact(text)) <= 12
    officer.term = "" if officer.term_end or pointer or _is_blank(text) else text[:160]


# --- inline XBRL ----------------------------------------------------------

def _local(name: str) -> str:
    return name.rsplit(":", 1)[-1]


def _facts(html: str, wanted: set[str]) -> list[_Fact]:
    """The ``ix`` elements named in ``wanted``, in document order; elements may nest."""
    found: list[_Fact] = []
    stack: list[tuple[int, str, str, dict[str, str]] | None] = []
    for tag in _IX_TAG.finditer(html):
        closing, _, raw, self_closing = tag.groups()
        if closing:
            opened = stack.pop() if stack else None
            if opened is not None:
                start, name, context, attributes = opened
                found.append(_Fact(start, name, context, attributes, html[start:tag.start()]))
            continue
        attributes = dict(_ATTRIBUTE.findall(raw))
        name = _local(attributes.get("name", ""))
        if name not in wanted and not any(name.startswith(prefix) for prefix in (_FOOTNOTES, _EXECUTIVE_FOOTNOTES)):
            if not self_closing:
                stack.append(None)
            continue
        context = attributes.get("contextRef", "")
        if self_closing:
            found.append(_Fact(tag.end(), name, context, attributes, ""))
        else:
            stack.append((tag.end(), name, context, attributes))
    found.sort(key=lambda fact: fact.start)
    return found


def _contexts(html: str) -> dict[str, tuple[str | None, str]]:
    """Each context's instant and the local name of its (last) dimension member."""
    result: dict[str, tuple[str | None, str]] = {}
    for context_id, body in _CONTEXT.findall(html):
        instant = _INSTANT.search(body)
        members = _MEMBER.findall(body)
        result[context_id] = (instant.group(1) if instant else None, _local(members[-1]) if members else "")
    return result


def _labels(linkbase: str) -> dict[str, str]:
    """Standard labels by element name, from a label linkbase."""
    locators = {label: element for element, label in _LOC.findall(linkbase)}
    locators.update({label: element for label, element in _LOC_REVERSED.findall(linkbase)})
    targets: dict[str, list[str]] = {}
    for raw in _LABEL_ARC.findall(linkbase):
        attributes = dict(_ATTRIBUTE.findall(raw))
        source, target = attributes.get("xlink:from"), attributes.get("xlink:to")
        if source in locators and target:
            targets.setdefault(target, []).append(locators[source])
    labels: dict[str, str] = {}
    for raw, text in _LABEL.findall(linkbase):
        attributes = dict(_ATTRIBUTE.findall(raw))
        if attributes.get("xlink:role", _STANDARD_LABEL) != _STANDARD_LABEL:
            continue
        for element in targets.get(attributes.get("xlink:label", ""), ()):
            # Element ids are "<prefix>_<LocalName>"; contexts name the member by its local name.
            labels[element.rsplit("_", 1)[-1]] = clean(unescape(text))
    return labels


def _text_block(html: str, suffix: str) -> str | None:
    """The HTML inside the ``ix:nonNumeric`` element whose name ends with ``suffix``."""
    match = re.search(r'<ix:nonNumeric\b[^>]*\bname="[^"]*' + re.escape(suffix) + r'"[^>]*>', html, re.IGNORECASE)
    if not match:
        return None
    depth = 1
    for tag in _IX_TAG.finditer(html, match.end()):
        if tag.group(4):
            continue
        depth += -1 if tag.group(1) else 1
        if depth == 0:
            return html[match.end():tag.start()]
    return html[match.end():]


# --- shareholders ---------------------------------------------------------

def _shareholders(facts: list[_Fact], contexts: dict[str, tuple[str | None, str]]) -> tuple[list[Shareholder], dict[str, Any]]:
    rows: dict[tuple[str, str], Shareholder] = {}
    totals: dict[str, Any] = {}
    for fact in facts:
        target = _SHAREHOLDER_FIELDS.get(fact.name)
        if target is None:
            continue
        basis, column = target
        instant, member = contexts.get(fact.context, (None, ""))
        rank = _RANK.search(fact.context) or _RANK.search(member)
        if rank is None and "_" not in fact.context:
            # The table's total row carries no dimension member.
            if basis == "shares" and column in ("shares", "ratio"):
                totals.setdefault(column, _number(fact))
            continue
        key = (basis, fact.context)
        row = rows.get(key)
        if row is None:
            row = rows[key] = Shareholder(seq=len(rows) + 1, rank=int(rank.group(1)) if rank else 0, basis=basis)
            if basis == "shares" and instant:
                totals.setdefault("as_of", instant)
        if column in ("name", "address"):
            setattr(row, column, plain(fact.inner))
        elif getattr(row, column) is None:
            setattr(row, column, _number(fact))
    result = [row for row in rows.values() if row.name and not _is_blank(row.name)]
    for basis in ("shares", "voting"):
        listed = [row for row in result if row.basis == basis]
        if any(row.rank == 0 for row in listed):
            # A filer's own row members carry no rank: the table's order is the ranking.
            for position, row in enumerate(listed, start=1):
                row.rank = position
    result.sort(key=lambda row: (row.basis != "shares", row.rank, row.seq))
    for position, row in enumerate(result, start=1):
        row.seq = position
    return result, totals


def _is_blank(text: str) -> bool:
    value = compact(text)
    return not value or all(char in "-‐‑‒–—―ー−" for char in value)


def _statistics(report: ParsedReport, facts: list[_Fact]) -> None:
    """Share statistics, taken for ordinary shares where a company reports several classes."""
    found: dict[str, tuple[int, float]] = {}
    for fact in facts:
        target = _STATISTICS.get(fact.name)
        if target is None or "Row" in fact.context:
            continue
        value = _number(fact)
        if value is None:
            continue
        # Ordinary shares first, then a figure for all classes together, then whatever class is stated.
        preference = 0 if fact.context.endswith("OrdinaryShareMember") else 1 if "_" not in fact.context else 2
        if target not in found or preference < found[target][0]:
            found[target] = (preference, value)
    values = {target: value for target, (_, value) in found.items()}
    report.shareholder_count = int(values["shareholder_count"]) if "shareholder_count" in values else None
    report.issued_shares = values.get("issued_shares")
    report.treasury_shares = values.get("treasury_shares")
    report.treasury_ratio = values.get("treasury_ratio")
    report.distribution = [
        {"key": key, "ratio": values.get(f"ratio:{key}"), "holders": int(values[f"holders:{key}"]) if f"holders:{key}" in values else None}
        for key, _, _ in HOLDER_KINDS
        if f"ratio:{key}" in values or f"holders:{key}" in values
    ]


# --- officers ---------------------------------------------------------------

def _footnote_texts(facts: list[_Fact], proposed: bool) -> list[str]:
    texts = []
    for fact in facts:
        base = fact.name.removesuffix("TextBlock")
        is_proposal = base.endswith("Proposal")
        if base.removesuffix("Proposal") in (_FOOTNOTES, _EXECUTIVE_FOOTNOTES) and is_proposal == proposed:
            texts.append(plain(fact.inner))
    return [text for text in texts if text]


def _resolve_term(term: str, footnotes: list[str]) -> str:
    """A term cell that only points at a footnote ("(注)３") is replaced by that footnote's text."""
    reference = _NOTE_REFERENCE.search(term)
    if reference is None or len(compact(term)) > 12:
        return term
    number = reference.group(1)
    for text in footnotes:
        leading = _NOTE_NUMBER.match(text)
        if leading and leading.group(1) == number:
            return text
    return term


def _mark_outside(officers: list[Officer], footnotes: list[str]) -> None:
    """Officers a footnote names as outside directors or auditors, and those whose title says so."""
    statements = [compact(sentence) for text in footnotes for sentence in re.split(r"[。\n]", text) if _OUTSIDE.search(sentence)]
    for officer in officers:
        if "社外" in officer.title:
            officer.outside = True
            continue
        name = compact(officer.name)
        if len(name) >= 2 and any(name in statement for statement in statements):
            officer.outside = True


def _tagged_officers(facts: list[_Fact], contexts: dict[str, tuple[str | None, str]], proposed: bool) -> tuple[list[Officer], str | None]:
    rows: dict[str, dict[str, Any]] = {}
    as_of: str | None = None
    for fact in facts:
        base = fact.name.removesuffix("TextBlock")
        if base.endswith("Proposal") != proposed:
            continue
        column = _OFFICER_FIELDS.get(base.removesuffix("Proposal") + ("TextBlock" if fact.name.endswith("TextBlock") else ""))
        if column is None or "_" not in fact.context:
            # Without a member the fact is the table's total, not a person.
            continue
        row = rows.setdefault(fact.context, {"start": fact.start})
        if column in ("shares", "other_shares"):
            row.setdefault(column, _number(fact))
        elif column in ("career", "career_more"):
            row.setdefault("career_lines", []).extend(lines(fact.inner))
        else:
            row.setdefault(column, fact.inner)
    officers: list[Officer] = []
    for context, row in sorted(rows.items(), key=lambda item: item[1]["start"]):
        name = person_name(plain(row.get("name", "")))
        if not name or _is_blank(name):
            continue
        instant, member = contexts.get(context, (None, ""))
        as_of = as_of or instant
        term = plain(row.get("term", ""))
        officers.append(Officer(
            seq=len(officers) + 1,
            name=name,
            proposed=proposed,
            member=member or context.rsplit("_", 1)[-1],
            title=plain(row.get("title", "")),
            birth_date=parse_date(plain(row.get("birth", ""))),
            career=career_entries(row.get("career_lines", [])),
            term=term,
            shares=row.get("shares"),
            other_shares=row.get("other_shares"),
        ))
    footnotes = _footnote_texts(facts, proposed)
    for officer in officers:
        _set_term(officer, _resolve_term(officer.term, footnotes))
    _mark_outside(officers, footnotes)
    return officers, as_of


def _apply_labels(officers: list[Officer], english: dict[str, str], japanese: dict[str, str]) -> None:
    for officer in officers:
        label = english.get(officer.member, "")
        if label and not is_japanese(label):
            officer.name_en = label
        filed = japanese.get(officer.member, "")
        # The label separates family and given name with a single space where the table pads every character.
        if filed and compact(filed) == compact(officer.name):
            officer.name = clean(filed)


def _column_roles(header: list[str]) -> dict[str, list[int]]:
    roles: dict[str, list[int]] = {}
    for index, cell in enumerate(header):
        text = compact(cell)
        if "氏名" in text:
            role = "name"
        elif "生年月日" in text:
            role = "birth"
        elif "略歴" in text:
            role = "career"
        elif "任期" in text:
            role = "term"
        elif "所有株式数" in text or "所有する当社株式" in text:
            role = "shares"
        elif "役名" in text or "職名" in text or "役職" in text or "地位" in text:
            role = "title"
        else:
            continue
        roles.setdefault(role, []).append(index)
    return roles


def _share_unit(header: str) -> float:
    text = compact(header)
    return next((factor for word, factor in _SHARE_UNITS if word in text), 1.0)


def _distinct(cells: list[str]) -> list[str]:
    seen: list[str] = []
    for cell in cells:
        if cell and cell not in seen:
            seen.append(cell)
    return seen


def _split_career_text(text: str) -> list[str]:
    """Break a career cell flattened to one line back into lines, each starting at a date."""
    marks = [match.start() for match in re.finditer(r"(?:令和|平成|昭和)?\s*(?:\d{1,4}|元)\s*年\s*\d{1,2}\s*月|同\s*年\s*\d{1,2}\s*月", text)]
    if not marks:
        return [text] if text else []
    pieces = [text[:marks[0]].strip()] if marks[0] > 0 else []
    for start, end in zip(marks, [*marks[1:], len(text)]):
        pieces.append(text[start:end].strip())
    return [piece for piece in pieces if piece]


def _table_officers(block: str) -> list[Officer]:
    """Officers from the section's HTML table, for reports that do not tag each person."""
    soup = BeautifulSoup(block, "html.parser")
    officers: dict[tuple[str, str | None], Officer] = {}
    careers: dict[tuple[str, str | None], list[str]] = {}
    terms: dict[tuple[str, str | None], str] = {}
    for table in soup.find_all("table"):
        if table.find_parent("table") is not None:
            continue
        grid = table_grid(table)
        header_index = next((index for index, row in enumerate(grid) if any("氏名" in compact(cell) for cell in row) and any("生年月日" in compact(cell) for cell in row)), None)
        if header_index is None:
            continue
        roles = _column_roles(grid[header_index])
        if "name" not in roles:
            continue
        unit = _share_unit(grid[header_index][roles["shares"][0]]) if "shares" in roles else 1.0

        def cell(row: list[str], role: str) -> str:
            return " ".join(_distinct([row[index] for index in roles.get(role, []) if index < len(row)]))

        for row in grid[header_index + 1:]:
            filed_name = cell(row, "name")
            name = person_name(filed_name)
            title = cell(row, "title")
            if not name or _is_blank(name) or compact(name) in _TOTAL_WORDS or "氏名" in name or compact(title) in _TOTAL_WORDS:
                continue
            birth = parse_date(cell(row, "birth")) or parse_date(filed_name)
            key = (compact(name), birth)
            officer = officers.get(key)
            if officer is None:
                shares_text = numeric(cell(row, "shares"))
                numbers = _NUMBER.findall(shares_text)
                officer = officers[key] = Officer(
                    seq=len(officers) + 1,
                    name=name,
                    title="" if _CAREER_DATE.fullmatch(title) else title,
                    birth_date=birth,
                    shares=round(float(numbers[-1]) * unit, 6) if numbers else None,
                    source="table",
                )
                terms[key] = cell(row, "term")
            careers.setdefault(key, []).extend(
                line for part in _distinct([row[index] for index in roles.get("career", []) if index < len(row)]) for line in _split_career_text(part)
            )
    # Footnotes follow the table as numbered paragraphs: they say who is an outside officer and how long terms run.
    footnotes = [line for line in lines(block) if _NOTE_NUMBER.match(line) and len(line) > 12]
    previous: Officer | None = None
    for key, officer in officers.items():
        officer.career = career_entries(careers.get(key, []))
        term = terms.get(key, "")
        if compact(term) in _DITTO and previous is not None:
            officer.term, officer.term_start, officer.term_end = previous.term, previous.term_start, previous.term_end
        else:
            _set_term(officer, _resolve_term(term, footnotes))
        previous = officer
    result = list(officers.values())
    _mark_outside(result, footnotes)
    return result


# --- reports ----------------------------------------------------------------

def _pages(bundle: zipfile.ZipFile) -> tuple[list[str], str | None, list[str]]:
    names = [name for name in bundle.namelist() if "publicdoc" in name.lower()]
    pages = sorted(name for name in names if name.lower().endswith((".htm", ".html")))
    header = next((name for name in pages if "header" in name.lower()), None)
    # 第4 提出会社の状況 holds both sections and is page 0104 of a standard report: read it first.
    body = sorted((name for name in pages if name != header), key=lambda name: (not name.rsplit("/", 1)[-1].startswith("0104"), name))
    labels = [name for name in names if name.lower().endswith(("_lab.xml", "_lab-en.xml"))]
    return body, header, labels


def parse_annual_report(archive: bytes) -> ParsedReport:
    """The major shareholders and officers an annual report's archive states."""
    wanted = set(_SHAREHOLDER_FIELDS) | set(_COUNTS) | set(_STATISTICS)
    for name in _OFFICER_FIELDS:
        wanted.add(name)
        wanted.add(name.removesuffix("TextBlock") + "Proposal" + ("TextBlock" if name.endswith("TextBlock") else ""))
    wanted.update(f"{name}Proposal" for name in _COUNTS)
    report = ParsedReport()
    with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
        body, header, label_files = _pages(bundle)
        facts: list[_Fact] = []
        contexts: dict[str, tuple[str | None, str]] = {}
        officer_block: str | None = None
        seen_shareholders = seen_officers = False
        for page, name in enumerate(body):
            html = bundle.read(name).decode("utf-8", "replace")
            if not any(marker in html for marker in _PAGE_MARKERS):
                continue
            # Facts keep the order they are read in across pages.
            offset = page * 1_000_000_000
            facts.extend(_Fact(offset + fact.start, fact.name, fact.context, fact.attributes, fact.inner) for fact in _facts(html, wanted))
            if officer_block is None:
                officer_block = _text_block(html, _OFFICER_BLOCK)
            seen_shareholders = seen_shareholders or "NameMajorShareholders" in html
            seen_officers = seen_officers or _OFFICER_BLOCK in html
            if seen_shareholders and seen_officers:
                break
        if header is not None and facts:
            contexts.update(_contexts(bundle.read(header).decode("utf-8", "replace")))

        _statistics(report, facts)
        report.shareholders, totals = _shareholders(facts, contexts)
        report.shareholders_as_of = totals.get("as_of")
        report.top_shares = totals.get("shares")
        report.top_ratio = totals.get("ratio")

        current, as_of = _tagged_officers(facts, contexts, proposed=False)
        proposed, proposed_as_of = _tagged_officers(facts, contexts, proposed=True)
        report.officers = [*current, *proposed]
        report.officers_as_of = as_of or proposed_as_of
        if report.officers:
            english: dict[str, str] = {}
            japanese: dict[str, str] = {}
            for name in label_files:
                target = english if name.lower().endswith("_lab-en.xml") else japanese
                target.update(_labels(bundle.read(name).decode("utf-8", "replace")))
            _apply_labels(report.officers, english, japanese)
        elif officer_block:
            report.officers = _table_officers(officer_block)
            counted = _GENDER_COUNT.search(plain(officer_block[:4000]))
            if counted:
                report.men = int(counted.group(1))
                report.women = int(counted.group(2)) if counted.group(2).isdigit() else 0
        for position, officer in enumerate(report.officers, start=1):
            officer.seq = position

        counts: dict[str, int] = {}
        for fact in facts:
            column = _COUNTS.get(fact.name.removesuffix("Proposal"))
            value = _number(fact) if column else None
            if column and value is not None:
                # The slate proposed to the meeting is the board in office once it is held.
                if fact.name.endswith("Proposal") or column not in counts:
                    counts[column] = int(value)
        if counts:
            report.men, report.women = counts.get("men"), counts.get("women")
    return report
