"""English names for the bond views.

Filings name everything in Japanese. A bond's issuer is shown exactly as
Company Analysis shows that company: by the name, ticker, and industry in
``CompanyInfo``, read on every request. (Where EDINET's code list has no
English name, ``orchestrator.common.company_names`` fills in the one the
company's own annual report states, for every view at once.) The bond's title
is rewritten from the terms parsed out of it. A subsidiary that files nothing with EDINET has no English name in any
of those; it is shown as "Subsidiary 1", "Subsidiary 2" within its group rather
than by a translated name, because translating a company's name gets it wrong
too often. What the filing says stays beside each as ``*_ja``.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, field
from typing import Any

from .parsing import clean, compact, series_number

# Kana, kanji, and the ditto marks a schedule uses for "same as above".
_JAPANESE = re.compile(r"[\u3000-\u30ff\u3400-\u9fff]")
# Footnote marks beside a name: "(注)1", "(注3)", "注2.3", "※1".
_NOTE_MARKS = re.compile(r"\(\s*注\s*\d*\s*\)\s*(?:\d+(?:\s*[、,.]\s*\d+)*)?|注\s*\d+(?:[、,.]\d+)*|[※*]\s*\d*")
_PARI_PASSU = ("(特定社債間限定同順位特約付)", "(社債間限定同順位特約付)", "(社債間限定同順位特約付き)")

_KINDS = {
    "convertible": "Convertible bond",
    "hybrid": "Hybrid bond",
    "subordinated": "Subordinated bond",
    "secured": "Secured bond",
}
# What an ordinary senior bond's title says it is, beyond its ranking.
_TITLE_KINDS = (
    ("短期社債", "Short-term bond"),
    ("政府保証", "Government-guaranteed bond"),
    ("投資法人債", "Investment corporation bond"),
)
# An annual report's schedule names the issuer of each bond; these stand for the filer itself…
_PARENT_WORDS = {"当社", "提出会社", "連結財務諸表提出会社", "親会社", "当行", "当金庫"}
_COMPANY_WORDS = re.compile(r"株式会社|\(株\)|㈱|有限会社|合同会社|holdings|ホールディングス|グループ|[\s・.,]", re.IGNORECASE)
# …and these for subsidiaries it does not name.
_UNNAMED_ISSUERS = {
    "子会社": "Subsidiaries",
    "連結子会社": "Consolidated subsidiaries",
    "その他の連結子会社": "Other consolidated subsidiaries",
    "その他連結子会社": "Other consolidated subsidiaries",
    "国内連結子会社": "Domestic consolidated subsidiaries",
    "在外連結子会社": "Overseas consolidated subsidiaries",
    "海外連結子会社": "Overseas consolidated subsidiaries",
    "その他の社債": "Other bonds",
    # A column heading read as a row: the schedule names no issuer here.
    "会社名": "Not named",
}
_DITTO_MARKS = {"〃", "同上", "同左", "々", "″", "''", '"'}
# A company's legal form, as it is abbreviated beside a name in Latin script.
_LEGAL_FORMS = (
    ("株式会社", "K.K."), ("(株)", "K.K."), ("有限会社", "Y.K."), ("(有)", "Y.K."), ("合同会社", "G.K."), ("(同)", "G.K."),
    ("特定目的会社", "TMK"), ("投資法人", "Investment Corporation"),
)
_NOTE_NUMBER = re.compile(r"注\)?(\d+)")


def is_japanese(text: Any) -> bool:
    return bool(_JAPANESE.search(str(text or "")))


def _lower_first(text: str) -> str:
    return text[:1].lower() + text[1:]


def filed_label(name: str | None, filer_name: str | None) -> str:
    """A bond's title as filed, without the issuer a supplement names first ("ダイキン工業株式会社第36回…")."""
    if not is_japanese(name):
        # Japanese titles lose the spaces markup leaves inside them; a title in Latin script needs its own.
        return clean(name)
    label = compact(name or "")
    company = compact(filer_name or "")
    if company and label.startswith(company) and len(label) > len(company):
        label = label[len(company):]
    # Nearly every senior bond ranks pari passu with the issuer's other bonds; saying so adds nothing.
    for boilerplate in _PARI_PASSU:
        label = label.replace(boilerplate, "")
    return label or compact(name or "")


def bond_label(bond: dict[str, Any], filed: str) -> str:
    """A bond's title in English, from its parsed terms: "Bond No. 70", "Subordinated bond No. 3", "USD bond due 2031".

    The ranking is named only where it is not an ordinary senior bond, and a
    title that is not Japanese is kept as filed.
    """
    if filed and not is_japanese(filed):
        return filed
    features = bond.get("features") or []
    kind = "General-mortgage bond" if "general-mortgage" in features else _KINDS.get(str(bond.get("seniority") or ""), "")
    if not kind:
        kind = next((label for word, label in _TITLE_KINDS if word in filed), "Bond")
    currency = bond.get("currency") or "JPY"
    if currency != "JPY":
        kind = f"{currency} {_lower_first(kind)}"
    perpetual = bool(bond.get("perpetual"))
    if perpetual:
        # Banks number their perpetual and their dated subordinated bonds separately, so the series alone is not enough.
        kind = f"Perpetual {kind if currency != 'JPY' else _lower_first(kind)}"
    series = bond.get("series") or series_number(filed)
    if series:
        return f"{kind} No. {series}"
    year = "" if perpetual else str(bond.get("maturity") or "")[:4]
    return f"{kind} due {year}" if year else kind


def _name_key(name: Any) -> str:
    """A company's Japanese name as either a filer or an annual report writes it: no spaces, ``(株)`` spelled out."""
    return _NOTE_MARKS.sub("", compact(name)).replace("(株)", "株式会社")


def company_key(name: str | None) -> str:
    """A company's name without its legal form, spacing, or case."""
    return _COMPANY_WORDS.sub("", compact(name or "")).casefold()


def _short_name(name: str | None) -> str:
    # Gas utilities register 瓦斯 and write ガス: 大阪瓦斯株式会社 is 大阪ガス(株) in its own report.
    return company_key(name).replace("瓦斯", "ガス")


def is_parent_issuer(issuer: str, company_names: tuple[str, ...]) -> bool:
    """Whether a schedule's issuer cell means the filer itself: blank, "当社", or its own name in short."""
    key = compact(issuer)
    if not key or key in _PARENT_WORDS or _name_key(issuer) in _PARENT_WORDS:
        return True
    short = _short_name(issuer)
    if not short:
        return True
    return any(name and (short == name or (len(short) >= 2 and (short in name or name in short))) for name in map(_short_name, company_names))


@dataclass(frozen=True)
class CompanyDirectory:
    """Companies as Company Analysis names them, by EDINET code and by the filer's Japanese name."""

    by_code: dict[str, dict[str, Any]] = field(default_factory=dict)
    # A name shared by several filers identifies none of them.
    codes_by_name: dict[str, str | None] = field(default_factory=dict)

    @classmethod
    def load(cls, conn: sqlite3.Connection) -> CompanyDirectory:
        try:
            rows = conn.execute(
                'SELECT Company_Code, "Submitter Name", Company_Name, Company_Ticker, Company_Industry, Listed FROM CompanyInfo'
            ).fetchall()
        except sqlite3.Error:
            # Without the company table a bond keeps the names stored with it.
            return cls()
        by_code: dict[str, dict[str, Any]] = {}
        codes_by_name: dict[str, str | None] = {}
        for code, filer_name, english, ticker, industry, listed in rows:
            code = str(code or "")
            filer_name = str(filer_name or "").strip()
            name = str(english or "").strip() or filer_name
            if not code or not name:
                continue
            by_code[code] = {
                "name": name,
                "ticker": str(ticker or ""),
                "industry": str(industry or ""),
                "listed": 1 if str(listed or "").lower().startswith("listed") else 0,
            }
            key = _name_key(filer_name)
            if key:
                codes_by_name[key] = None if key in codes_by_name else code
        return cls(by_code, codes_by_name)

    def named(self, filed_name: Any) -> str | None:
        """The Company Analysis name of the filer an annual report names in Japanese, when exactly one has that name."""
        code = self.codes_by_name.get(_name_key(filed_name))
        return self.by_code[code]["name"] if code else None

    def _company_name(self, code: str, stored_english: Any, filer_name: str) -> str:
        return str(self.by_code.get(code, {}).get("name") or stored_english or filer_name or code)

    @staticmethod
    def _is_filer(filed_issuer: str, company_name: str, filer_name: str) -> bool:
        return bool(_name_key(filed_issuer)) and is_parent_issuer(filed_issuer, (filer_name, company_name))

    def subsidiary_labels(self, code: str, filer_name: str, stored_english: Any, filed_issuers: list[str]) -> dict[str, str]:
        """An English label for each issuer in a group's schedule that has a name only in Japanese.

        They are numbered in the order of their Japanese names, so the bonds of
        one subsidiary share a label; a group with a single such issuer needs no number.
        """
        company_name = self._company_name(code, stored_english, filer_name)
        keys = sorted({
            _name_key(filed) for filed in filed_issuers
            if filed and not self._is_filer(filed, company_name, filer_name) and is_japanese(self.issuer(filed, company_name, filer_name))
        })
        if len(keys) == 1:
            return {keys[0]: "Subsidiary"}
        return {key: f"Subsidiary {number}" for number, key in enumerate(keys, start=1)}

    def issuer(self, filed_issuer: str, company_name: str, filer_name: str = "") -> str:
        """Who issued a bond in a group's schedule, in English where the schedule's words or a filer's name allow it."""
        key = _name_key(filed_issuer)
        if self._is_filer(filed_issuer, company_name, filer_name):
            return company_name
        if key in _UNNAMED_ISSUERS:
            return _UNNAMED_ISSUERS[key]
        if key in _DITTO_MARKS:
            return "As above in the report"
        if not key and is_japanese(filed_issuer):
            # Only a footnote mark: the issuer is named in the schedule's notes.
            number = _NOTE_NUMBER.search(compact(filed_issuer))
            return f"See note {number.group(1)}" if number else "See the notes"
        named = self.named(filed_issuer)
        if named:
            return named
        # A name already in Latin script needs only its Japanese footnote mark removed,
        # and its legal form put in Latin letters: "TAKASUGI(株)" is TAKASUGI K.K.
        plain = clean(_NOTE_MARKS.sub(" ", clean(filed_issuer)))
        if plain and not is_japanese(plain):
            return plain
        for word, abbreviation in _LEGAL_FORMS:
            core = clean(plain.replace(word, " ")) if word in plain else ""
            if core and not is_japanese(core):
                return f"{core} {abbreviation}"
        return filed_issuer

    def present(self, bond: dict[str, Any], subsidiaries: dict[str, str] | None = None) -> dict[str, Any]:
        """Put ``bond``'s names in English in place, keeping the filed ones as ``*_ja`` where they differ.

        ``subsidiaries`` is its group's ``subsidiary_labels``; without it an
        issuer named only in Japanese is plainly "Subsidiary".
        """
        filer_name = bond.get("company_name") or ""
        if "name" in bond:
            filed = filed_label(bond.get("name"), filer_name)
            bond["label"] = bond_label(bond, filed)
            bond["label_ja"] = filed if filed != bond["label"] else None
        code = str(bond.get("edinet_code") or "")
        company = self.by_code.get(code)
        name = self._company_name(code, bond.pop("company_name_en", None), filer_name)
        bond["company_name"] = name
        bond["company_name_ja"] = filer_name if filer_name and filer_name != name else None
        if company:
            bond.update(ticker=company["ticker"], industry=company["industry"], listed=company["listed"])
        if "issuer" in bond:
            filed_issuer = bond.get("issuer") or ""
            issuer = name if bond.get("is_parent") else self.issuer(filed_issuer, name, filer_name)
            if is_japanese(issuer) and not bond.get("is_parent") and not self._is_filer(filed_issuer, name, filer_name):
                issuer = (subsidiaries or {}).get(_name_key(filed_issuer), "Subsidiary")
            bond["issuer"] = issuer
            bond["issuer_ja"] = filed_issuer if filed_issuer and filed_issuer != issuer else None
        return bond
