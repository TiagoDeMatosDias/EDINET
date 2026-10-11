"""Decide who each filed shareholder and officer is, and write one row per person or organisation.

Reports only give names. This puts the same holder's rows together across
companies and years, in this order of confidence:

* an officer is a person, known by name and date of birth, so a director who
  sits on two boards is one person on both;
* a shareholder with the name of one of the company's own officers is that person;
* a shareholder whose legal name is an EDINET filer's is that company, shown
  as Company Analysis shows it;
* a custody bank, insurer, or other institution this module knows by name is
  that institution, whatever account the row is for;
* anything else is the organisation or individual of that name. A shareholder
  list gives no date of birth, so two individuals with one name cannot be told
  apart; their rows carry the addresses filed, which usually can.

The result is ``Ownership_Entities`` and the ``entity_id`` on every row.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from src.bonds.parsing import clean, compact

from . import store
from .directory import FilerDirectory, legal_key
from .names import (
    association,
    beneficiary,
    english_person_name,
    fold_name,
    is_individual,
    is_japanese,
    known_family,
    known_name,
    person_key,
    split_holder,
    title_en,
)

# A company's latest report still describes it for two years; after that its holders and officers are history.
CURRENT_YEARS = 2
_TREASURY = {"自己株式", "自己株口", "当社", "自社", "自己名義株式"}


def entity_id(prefix: str, key: str) -> str:
    """A stable identifier from what identifies the entity, so a rebuild keeps every link."""
    return prefix + hashlib.sha1(key.encode("utf-8")).hexdigest()[:11]


@dataclass
class _Entity:
    entity_id: str
    kind: str
    name: str = ""
    name_ja: str = ""
    edinet_code: str | None = None
    birth_date: str | None = None
    address: str = ""
    name_key: str = ""
    headline: str = ""
    # Filed spellings and how often each occurs; the commonest is the one shown.
    spellings: Counter[str] = field(default_factory=Counter)
    held: set[str] = field(default_factory=set)
    held_before: set[str] = field(default_factory=set)
    seats: set[str] = field(default_factory=set)
    seats_before: set[str] = field(default_factory=set)
    # The newest row seen (what the entity is called and where it is now), and each English label with how
    # often companies used it: two boards may romanise one name differently.
    newest: tuple[str, int] = ("", -1)
    labels: Counter[tuple[bool, str]] = field(default_factory=Counter)
    top: tuple[float, str] = (-1.0, "")


def _cutoff(latest_period: str) -> str:
    try:
        return f"{int(latest_period[:4]) - CURRENT_YEARS:04d}{latest_period[4:]}"
    except ValueError:
        return ""


def _latest_reports(conn: sqlite3.Connection) -> tuple[dict[str, tuple[str, int]], str]:
    """Each company's latest report that states shareholders or officers, whether it proposes a slate, and the cutoff for "current"."""
    rows = conn.execute(
        "SELECT doc_id, edinet_code, period_end, submitted_at, proposed FROM Ownership_Documents WHERE status = 'parsed' "
        "ORDER BY edinet_code, period_end, submitted_at"
    ).fetchall()
    latest = {row[1]: (row[0], int(row[4] or 0), row[2] or "") for row in rows}
    newest = max((period for _, _, period in latest.values()), default="")
    cutoff = _cutoff(newest)
    return {code: (doc_id, proposed) for code, (doc_id, proposed, period) in latest.items() if period >= cutoff}, cutoff


class _Builder:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.directory = FilerDirectory.load(conn)
        self.latest, self.cutoff = _latest_reports(conn)
        self.entities: dict[str, _Entity] = {}
        # (company, folded name) -> the people of that name who have been its officers, newest first.
        self.officers_of: dict[tuple[str, str], list[str]] = {}
        self._holders: dict[tuple[str, str, str, str], tuple[str, str]] = {}

    def entity(self, identifier: str, kind: str) -> _Entity:
        found = self.entities.get(identifier)
        if found is None:
            found = self.entities[identifier] = _Entity(identifier, kind)
        return found

    # --- officers ---------------------------------------------------------

    def read_officers(self) -> list[tuple[str, int, str, int]]:
        rows = self.conn.execute(
            "SELECT doc_id, seq, edinet_code, period_end, proposed, name, name_en, member, birth_date, title FROM Ownership_Officers "
            "ORDER BY period_end, doc_id, seq"
        ).fetchall()
        assigned: list[tuple[str, int, str, int]] = []
        for doc_id, seq, code, period_end, proposed, name, label, member, birth_date, title in rows:
            folded, birth = person_key(name, birth_date)
            if not folded:
                continue
            # Without a date of birth a person is only known within the company that names them.
            person = self.entity(entity_id("P", f"{folded}|{birth or '@' + code}"), "person")
            current = self.latest.get(code) == (doc_id, int(proposed or 0))
            order = (period_end or "", int(proposed or 0))
            english = english_person_name(label, member)
            if english:
                # A label with a comma states which name is the family name, so it outranks one without.
                person.labels[("," in str(label or ""), english)] += 1
            if order >= person.newest:
                person.newest = order
                person.name_ja = clean(name)
                person.birth_date = birth_date or person.birth_date
                company = self.directory.company(code)
                person.headline = " · ".join(part for part in (title_en(title) or clean(title), company["name"] if company else code) if part)
            person.name_key = folded
            (person.seats if current else person.seats_before).add(code)
            holders = self.officers_of.setdefault((code, folded), [])
            if person.entity_id in holders:
                holders.remove(person.entity_id)
            holders.insert(0, person.entity_id)
            assigned.append((doc_id, seq, person.entity_id, int(current)))
        return assigned

    # --- shareholders -----------------------------------------------------

    def _company(self, code: str, fallback_name: str = "") -> _Entity:
        entity = self.entity(code, "company")
        if not entity.name:
            company = self.directory.company(code) or {}
            name = str(company.get("name") or code)
            if is_japanese(name):
                # A filer EDINET's list names only in Japanese takes the institution's published English name.
                family = known_family(name)
                name = fallback_name or (family[0] if family and not family[2] else None) or known_name(name) or name
            entity.name = name
            entity.name_ja = str(company.get("name_ja") or "")
            entity.edinet_code = code
            entity.headline = str(company.get("industry") or "")
        return entity

    def _named(self, prefix: str, key: str, kind: str, name: str, filed: str) -> _Entity:
        entity = self.entity(entity_id(prefix, key), kind)
        if name:
            entity.name = name
        entity.spellings[filed] += 1
        return entity

    def _resolve(self, code: str, name: str, on: str, address: str) -> tuple[_Entity, str]:
        """Who a filed shareholder name is, and the account or proxy note left over."""
        holder, account = split_holder(name)
        folded = fold_name(holder)
        people = self.officers_of.get((code, folded)) if folded else None
        if people:
            return self.entities[people[0]], account
        if compact(holder) in _TREASURY:
            return self._company(code), "; ".join(part for part in ("自己株式", account) if part)
        filer = self.directory.match(holder, on, address)
        if filer:
            return self._company(filer), account
        family = known_family(holder)
        if family:
            english, legal, rest = family
            client = beneficiary(rest) if rest and not is_japanese(rest) else None
            if client:
                # A custodian holding for a named fund: the fund is the shareholder.
                detail = "; ".join(part for part in (f"via {english}", account) if part)
                return self._named("O", legal_key(client) or compact(client), "organization", client, client), detail
            detail = "; ".join(part for part in (rest, account) if part)
            filer = self.directory.match(legal, on, address) if legal else None
            if filer:
                return self._company(filer, english), detail
            return self._named("O", f"known:{english}", "organization", english, holder if is_japanese(holder) else ""), detail
        english = known_name(holder)
        if english:
            return self._named("O", legal_key(holder) or compact(holder), "organization", english, holder), account
        owned = association(holder)
        if owned:
            return self._association(code, holder, *owned), account
        if is_individual(holder):
            entity = self._named("N", folded, "individual", "", holder)
            entity.name_key = folded
            return entity, account
        return self._named("O", legal_key(holder) or compact(holder), "organization", "", holder), account

    def _association(self, code: str, holder: str, owner: str, label: str) -> _Entity:
        company = self.directory.company(code) or {}
        filer_name = compact(company.get("name_ja") or "")
        own = not owner or (len(owner) >= 2 and owner.replace("グループ", "") in filer_name.replace("株式会社", "").replace("ホールディングス", ""))
        if own:
            # "従業員持株会" alone, or under the company's own name: there is one per company.
            english = str(company.get("name") or "")
            name = f"{english} {label}" if english and not is_japanese(english) else ""
            return self._named("O", f"association:{code}:{label}", "organization", name, holder)
        return self._named("O", legal_key(holder) or compact(holder), "organization", "", holder)

    def read_shareholders(self) -> list[tuple[str, int, str, str, int]]:
        rows = self.conn.execute(
            "SELECT doc_id, seq, edinet_code, period_end, as_of, basis, name, address, ratio FROM Ownership_Shareholders "
            "ORDER BY period_end, doc_id, seq"
        ).fetchall()
        assigned: list[tuple[str, int, str, str, int]] = []
        for doc_id, seq, code, period_end, as_of, basis, name, address, ratio in rows:
            on = as_of or period_end or ""
            cache = (code, name, on[:4], address or "")
            found = self._holders.get(cache)
            if found is None:
                entity, account = self._resolve(code, name, on, address or "")
                found = self._holders[cache] = (entity.entity_id, account)
            entity = self.entities[found[0]]
            current = basis == "shares" and self.latest.get(code, ("", 0))[0] == doc_id
            if basis == "shares":
                (entity.held if current else entity.held_before).add(code)
            if entity.kind != "person" and address and (period_end or "") >= entity.newest[0]:
                entity.address = clean(address)
                entity.newest = (period_end or "", 0)
            if current and entity.kind != "person" and (ratio or 0.0) > entity.top[0]:
                entity.top = (float(ratio or 0.0), code)
            assigned.append((doc_id, seq, found[0], found[1], int(current)))
        return assigned

    # --- output -------------------------------------------------------------

    def rows(self) -> list[tuple[Any, ...]]:
        result = []
        for entity in self.entities.values():
            filed = entity.spellings.most_common(1)[0][0] if entity.spellings else ""
            if entity.labels:
                entity.name = max(entity.labels.items(), key=lambda item: (item[0][0], item[1], item[0][1]))[0][1]
            if entity.kind in ("organization", "individual"):
                entity.name_ja = entity.name_ja or (filed if is_japanese(filed) else "")
                entity.name = entity.name or filed or entity.name_ja
            if entity.kind != "person" and entity.kind != "company" and entity.top[1]:
                company = self.directory.company(entity.top[1]) or {}
                entity.headline = f"{entity.top[0] * 100:.1f}% of {company.get('name') or entity.top[1]}"
            if not entity.name:
                entity.name = entity.name_ja or entity.entity_id
            company = self.directory.company(entity.edinet_code) or {}
            words = [entity.name, entity.name_ja, compact(entity.name_ja), entity.edinet_code or "", str(company.get("ticker") or "")]
            words.extend(spelling for spelling, _ in entity.spellings.most_common(6))
            search = " ".join(dict.fromkeys(word.casefold() for word in words if word))
            result.append((
                entity.entity_id, entity.kind, entity.name, entity.name_ja if entity.name_ja != entity.name else None,
                entity.edinet_code, entity.birth_date, entity.address or None, entity.headline or None, entity.name_key or None,
                len(entity.held), len(entity.held_before - entity.held), len(entity.seats), len(entity.seats_before - entity.seats), search,
            ))
        return result


def build_entities(conn: sqlite3.Connection) -> dict[str, Any]:
    """Assign every stored row to a person or organisation and rewrite ``Ownership_Entities``."""
    builder = _Builder(conn)
    officers = builder.read_officers()
    shareholders = builder.read_shareholders()
    conn.execute("UPDATE Ownership_Officers SET entity_id = NULL, latest = 0")
    conn.execute("UPDATE Ownership_Shareholders SET entity_id = NULL, account = NULL, latest = 0")
    conn.executemany("UPDATE Ownership_Officers SET entity_id = ?, latest = ? WHERE doc_id = ? AND seq = ?", [(entity, current, doc_id, seq) for doc_id, seq, entity, current in officers])
    conn.executemany(
        "UPDATE Ownership_Shareholders SET entity_id = ?, account = ?, latest = ? WHERE doc_id = ? AND seq = ?",
        [(entity, account or None, current, doc_id, seq) for doc_id, seq, entity, account, current in shareholders],
    )
    rows = builder.rows()
    conn.execute("DELETE FROM Ownership_Entities")
    conn.executemany(
        f"INSERT INTO Ownership_Entities ({', '.join(store.ENTITY_COLUMNS)}) VALUES ({', '.join('?' for _ in store.ENTITY_COLUMNS)})",
        rows,
    )
    kinds = Counter(row[1] for row in rows)
    return {
        "entities": len(rows),
        "people": kinds["person"],
        "individuals": kinds["individual"],
        "companies": kinds["company"],
        "organizations": kinds["organization"],
        "shareholder_rows": len(shareholders),
        "officer_rows": len(officers),
        "current_reports": len(builder.latest),
        "current_since": builder.cutoff,
    }
