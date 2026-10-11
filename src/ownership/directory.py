"""Companies as Company Analysis names them, found by the Japanese name a shareholder list gives.

A shareholder that files with EDINET itself (a listed parent, a bank, an
insurer) is shown exactly as on its own Analysis page: by the name, ticker,
and industry in ``CompanyInfo``. The lookup is by legal name, and Japanese
legal names differ by where the legal form stands: 株式会社平和 and
平和株式会社 are two companies, so the form's position is part of the key.
A filer is also found under the names it used before a rename
(``DocumentList`` keeps the name on its filings since 2016), and when two
filers have used one name the one known to have carried it at the report's
date wins.
"""

from __future__ import annotations

import re
import sqlite3
import unicodedata
from dataclasses import dataclass, field
from datetime import date
from typing import Any

_FORMS = (
    ("株式会社", "K"), ("(株)", "K"), ("有限会社", "Y"), ("(有)", "Y"), ("合同会社", "G"), ("(同)", "G"),
    ("合資会社", "S"), ("(資)", "S"), ("合名会社", "M"), ("(名)", "M"), ("相互会社", "SO"), ("(相)", "SO"),
)
_NOISE = re.compile(r"[\s・･.,'’&\-‐－/()]+")
_LATIN_FORMS = re.compile(
    r"\b(?:co\.?,?\s*ltd\.?|company,?\s*limited|corporation|incorporated|inc\.?|limited|ltd\.?|llc|l\.l\.c\.|plc|k\.k\.|kabushiki\s+kaisha)(?![a-z])",
    re.IGNORECASE,
)
_MUNICIPALITY = re.compile(r"^(.+?[市区町村郡])")
_OPEN = ("0000-00-00", "9999-99-99")


def legal_key(name: Any) -> str:
    """A legal name's key: its words without spacing or punctuation, and its legal form by position.

    ``株式会社平和`` → ``K<平和``, ``平和㈱`` → ``平和>K``, ``農林中央金庫`` → ``農林中央金庫``.
    """
    text = re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(name or "")))
    prefix = suffix = ""
    for word, code in _FORMS:
        if text.startswith(word) and len(text) > len(word):
            prefix, text = code, text[len(word):]
            break
    for word, code in _FORMS:
        if text.endswith(word) and len(text) > len(word):
            suffix, text = code, text[:-len(word)]
            break
    core = _NOISE.sub("", _LATIN_FORMS.sub(" ", text)).casefold()
    if not core:
        return ""
    return f"{prefix}<{core}" if prefix else f"{core}>{suffix}" if suffix else core


def core_key(key: str) -> str:
    """A legal key without its legal form, for names filed without one."""
    return key.split("<", 1)[-1].split(">", 1)[0]


def _municipality(location: Any) -> str:
    match = _MUNICIPALITY.match(unicodedata.normalize("NFKC", str(location or "")).strip())
    return match.group(1) if match else ""


def _day(text: str) -> date | None:
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


@dataclass(frozen=True)
class _Span:
    """When a filer is known to have carried a name. ``dated`` is false for a name with no rename on record."""

    first: str = _OPEN[0]
    last: str = _OPEN[1]
    dated: bool = False

    def gap(self, on: date) -> int:
        """Days between ``on`` and the span; 0 inside it."""
        first, last = _day(self.first), _day(self.last)
        if first is not None and on < first:
            return (first - on).days
        if last is not None and on > last:
            return (on - last).days
        return 0


@dataclass
class FilerDirectory:
    """EDINET filers by code and by every legal name they have filed under."""

    by_code: dict[str, dict[str, Any]] = field(default_factory=dict)
    by_key: dict[str, dict[str, _Span]] = field(default_factory=dict)
    by_core: dict[str, set[str]] = field(default_factory=dict)

    @classmethod
    def load(cls, conn: sqlite3.Connection, *, history: bool = True) -> FilerDirectory:
        """Read the company table; ``history`` adds the names each filer used on earlier filings."""
        directory = cls()
        try:
            rows = conn.execute(
                'SELECT Company_Code, "Submitter Name", Company_Name, Company_Ticker, Company_Industry, Listed, "Type of Submitter", Province FROM CompanyInfo'
            ).fetchall()
        except sqlite3.Error:
            return directory
        for code, filer_name, english, ticker, industry, listed, kind, location in rows:
            code = str(code or "")
            filer_name = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(filer_name or "")).strip())
            if not code or not filer_name:
                continue
            directory.by_code[code] = {
                "edinet_code": code,
                "name": re.sub(r"\s+", " ", str(english or "").strip()) or filer_name,
                "name_ja": filer_name,
                "ticker": str(ticker or ""),
                "industry": str(industry or ""),
                "listed": str(listed or "").lower().startswith("listed"),
                "person": str(kind or "").startswith("個人"),
                "location": str(location or ""),
            }
        earlier = directory._earlier_names(conn) if history else {}
        for code, company in directory.by_code.items():
            if company["person"]:
                continue
            current = legal_key(company["name_ja"])
            names = {key: span for key, span in earlier.get(code, {}).items() if key != current}
            if not names:
                directory._add(current, code, _Span())
                continue
            # Renamed: the current name follows the last filing under an earlier one, and the oldest
            # earlier name reaches back before the record begins.
            renamed = max(last for _, last in names.values())
            directory._add(current, code, _Span(renamed, _OPEN[1], True))
            oldest = min(names, key=lambda key: names[key][0])
            for key, (first, last) in names.items():
                directory._add(key, code, _Span(_OPEN[0] if key == oldest else first, last, True))
        return directory

    def _earlier_names(self, conn: sqlite3.Connection) -> dict[str, dict[str, tuple[str, str]]]:
        try:
            rows = conn.execute(
                "SELECT edinetCode, filerName, MIN(submitDateTime), MAX(submitDateTime) FROM DocumentList "
                "WHERE edinetCode IS NOT NULL AND filerName IS NOT NULL GROUP BY edinetCode, filerName"
            ).fetchall()
        except sqlite3.Error:
            return {}
        names: dict[str, dict[str, tuple[str, str]]] = {}
        for code, filer_name, first, last in rows:
            key = legal_key(filer_name)
            if not key or str(code) not in self.by_code:
                continue
            spans = names.setdefault(str(code), {})
            span = (str(first or "")[:10], str(last or "")[:10])
            known = spans.get(key)
            spans[key] = (min(span[0], known[0]), max(span[1], known[1])) if known else span
        return names

    def _add(self, key: str, code: str, span: _Span) -> None:
        if key:
            self.by_key.setdefault(key, {})[code] = span
            self.by_core.setdefault(core_key(key), set()).add(code)

    def match(self, name: Any, on: str = "", address: Any = "") -> str | None:
        """The EDINET code of the filer a shareholder list names, when the name identifies exactly one.

        ``on`` is the report's date, which decides between two filers that have
        used the name at different times; ``address`` decides between two that
        use it at once.
        """
        key = legal_key(name)
        if not key:
            return None
        spans = self.by_key.get(key)
        if spans is None:
            if key != core_key(key):
                return None
            # Filed without a legal form: the name must belong to one filer whatever its form.
            codes = self.by_core.get(key, set())
            return next(iter(codes)) if len(codes) == 1 else None
        candidates = list(spans)
        day = _day(str(on or ""))
        if len(candidates) > 1 and day is not None:
            # Nearest in time first; a name on record at the date beats one merely assumed to be current then.
            ranked = sorted(candidates, key=lambda code: (spans[code].gap(day), not spans[code].dated))
            best = (spans[ranked[0]].gap(day), not spans[ranked[0]].dated)
            candidates = [code for code in ranked if (spans[code].gap(day), not spans[code].dated) == best]
        if len(candidates) == 1:
            return candidates[0]
        place = unicodedata.normalize("NFKC", str(address or ""))
        located = [code for code in candidates if _municipality(self.by_code[code]["location"]) and _municipality(self.by_code[code]["location"]) in place]
        return located[0] if len(located) == 1 else None

    def company(self, code: str | None) -> dict[str, Any] | None:
        return self.by_code.get(str(code or ""))
