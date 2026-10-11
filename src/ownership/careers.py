"""Career summaries in English.

An officer's career in an annual report is a list of dated lines, nearly all
of two shapes: joining an organisation (``トヨタ自動車㈱入社``) or holding a
title at one (``同社取締役副社長``, ``当社代表取締役社長（現任）``). Those are
put into English exactly: the organisation by the name Company Analysis or the
list of well-known institutions gives it, the title from the roles it states.
Any other line is left to the local translation engine, and is returned as
filed when that is unavailable.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from src.bonds.parsing import clean, compact

from .directory import FilerDirectory
from .names import is_japanese, known_family, known_name, title_en

Translate = Callable[[list[str]], dict[str, str]]

_CURRENT = re.compile(r"\s*[\((]?\s*(?:現任|現在に至る|現職|現在)\s*[\))]?\s*$")
_APPOINTED = re.compile(r"(?:に)?就任$")
_JOINED = ("入社", "入行", "入省", "入庁", "入所", "入局", "入会", "入職", "入団", "入学")
_ENDED = (("退任", "until then"), ("退職", "left"), ("退社", "left"), ("退官", "retired"), ("辞任", "resigned"))
_OWN = ("当社", "当行", "当金庫", "当法人")
_SAME = ("同社", "同行", "同所", "同省", "同庁", "同法人", "同金庫")
_REGISTERED = {
    "弁護士登録": "Registered as an attorney-at-law",
    "公認会計士登録": "Registered as a certified public accountant",
    "税理士登録": "Registered as a certified tax accountant",
    "弁理士登録": "Registered as a patent attorney",
    "司法書士登録": "Registered as a judicial scrivener",
    "社会保険労務士登録": "Registered as a labour and social security attorney",
    "中小企業診断士登録": "Registered as a small and medium enterprise management consultant",
    "医師免許取得": "Licensed as a physician",
}
# A title is rewritten only when every word of it is a role this knows; anything else goes to the translator whole.
_ROLE_WORDS = re.compile(
    r"^(?:代表|取締役|執行役員|執行役|専務役員|常務役員|監査役|監査等委員|監査委員|社長|副社長|会長|副会長|頭取|副頭取|専務|常務|上席|上級|常勤|非常勤|常任|"
    r"社外|独立|相談役|顧問|兼|[・()\s]|C[A-Z]O|CHRO|CISO)+$"
)
_FORMS = ("株式会社", "(株)", "有限会社", "(有)", "合同会社", "(同)", "相互会社", "(相)")
_LATIN_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 .,&'()\-/]*[A-Za-z0-9.)]")
_MAX_NAME = 30


def _named(name: str, directory: FilerDirectory, on: str) -> str | None:
    """An organisation's English name, where a reliable source gives one."""
    code = directory.match(name, on)
    company = directory.company(code)
    if company is not None and not is_japanese(company["name"]):
        return str(company["name"])
    family = known_family(name)
    if family and not family[2]:
        return family[0]
    return known_name(name)


def _organisation(line: str, directory: FilerDirectory, on: str, own: str, previous: str | None) -> tuple[str, str] | None:
    """The organisation a career line opens with, in English, and the rest of the line."""
    for word in _OWN:
        if line.startswith(word):
            return own, line[len(word):]
    for word in _SAME:
        if line.startswith(word):
            return (previous or "the same organisation"), line[len(word):]
    latin = _LATIN_NAME.match(line)
    if latin and latin.end() < len(line) and len(latin.group(0)) >= 3:
        return latin.group(0).strip(), line[latin.end():]
    for form in _FORMS:
        if line.startswith(form):
            # The legal form leads: the name is the longest run after it that some source knows.
            for length in range(min(len(line), len(form) + _MAX_NAME), len(form) + 1, -1):
                english = _named(line[:length], directory, on)
                if english:
                    return english, line[length:]
            return None
        position = line.find(form)
        if 1 < position <= _MAX_NAME:
            english = _named(line[:position + len(form)], directory, on)
            if english:
                return english, line[position + len(form):]
            return None
    return None


def line_en(text: Any, directory: FilerDirectory, *, on: str = "", own: str = "the Company", previous: str | None = None) -> tuple[str, str | None] | None:
    """A career line in English and the organisation it names, when the line is one of the standard shapes.

    ``own`` is the reporting company's name (当社), ``previous`` the
    organisation of the line before (同社).
    """
    line = clean(text)
    current = bool(_CURRENT.search(line))
    line = _CURRENT.sub("", line).strip()
    squeezed = compact(line)
    suffix = " (current)" if current else ""
    if squeezed in _REGISTERED:
        return _REGISTERED[squeezed] + suffix, previous
    found = _organisation(line, directory, on, own, previous)
    if found is None:
        return None
    organisation, rest = found
    rest = _APPOINTED.sub("", compact(rest))
    if rest in _JOINED:
        return f"Joined {organisation}", organisation
    ended = next((label for word, label in _ENDED if rest.endswith(word)), None)
    if ended:
        rest = rest[:-2]
    if not rest:
        return (f"Left {organisation}", organisation) if ended in ("left", "retired", "resigned") else None
    if not _ROLE_WORDS.match(rest):
        return None
    title = title_en(rest)
    if not title:
        return None
    return f"{title}, {organisation}{f' ({ended})' if ended else ''}{suffix}", organisation


def translate_lines(lines: list[str], directory: FilerDirectory, *, own: str = "the Company", on: str = "", translate: Translate | None = None) -> dict[str, str]:
    """English for each career line, by the line as filed; lines nothing could translate are left out.

    ``lines`` are in the order filed, so "the same company" follows the line before.
    """
    english: dict[str, str] = {}
    remaining: list[str] = []
    previous: str | None = None
    for line in lines:
        found = line_en(line, directory, on=on, own=own, previous=previous)
        if found is not None:
            english[line], previous = found
        elif not is_japanese(line):
            english[line] = clean(line)
        else:
            remaining.append(line)
            # A line this cannot put into English may still open with the organisation the next line calls 同社.
            opening = _organisation(clean(line), directory, on, own, previous)
            previous = opening[0] if opening else None
    if remaining and translate is not None:
        for source, text in translate(list(dict.fromkeys(remaining))).items():
            if text and text != source:
                english[source] = text
    return english
