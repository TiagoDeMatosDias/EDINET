"""JSDA OTC reference prices (公社債店頭売買参考統計値).

The Japan Securities Dealers Association publishes, each business day, the
average of quotes reported by dealers for several thousand bonds: one CSV
per day (``S{yymmdd}.csv``, Shift-JIS, no header). The columns this module
reads are, by position: 0 date, 1 issue type, 2 issue code, 3 name
(``ﾀﾞｲｷﾝ工業 36``: a short issuer name and the series), 4 maturity,
5 coupon (%), 6 average compound yield (%), 7 average price, 8 change in
price, 14 average simple yield (%), and 20 the number of reporting dealers.
Types below 20 are government, municipal, and local bonds, which no EDINET
company issues, so they are skipped.

The site limits request rates, so files are fetched a few seconds apart and
a refusal (HTTP 429) ends the run with what has been read.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from collections.abc import Callable
from typing import Any

import requests

from .parsing import compact, parse_date

logger = logging.getLogger(__name__)

JSDA_BASE = "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/"
JSDA_INDEX_URL = JSDA_BASE + "index.html"
_FILE = re.compile(r"files/(\d{4})/S(\d{6})\.csv")
_TIMEOUT = (10, 60)
_PAUSE_SECONDS = 10.0
_SERIES = re.compile(r"(\d+)\D*$")
MISSING = {"", "99.999", "999.999", "-----", "--"}


class JsdaRateLimited(RuntimeError):
    """The JSDA site refused a request for making too many."""


def _number(text: str) -> float | None:
    value = text.strip()
    if value in MISSING:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def series_from_name(name: str) -> int | None:
    """The series number at the end of a JSDA name: ``ﾀﾞｲｷﾝ工業 36`` → 36, ``商工中金永劣2`` → 2."""
    match = _SERIES.search(compact(name))
    return int(match.group(1)) if match else None


def parse_reference_csv(content: bytes) -> list[dict[str, Any]]:
    """Rows for non-government bonds from one day's reference-price file."""
    text = content.decode("cp932", errors="replace")
    rows = []
    for cells in csv.reader(io.StringIO(text)):
        if len(cells) < 21 or not cells[1].strip().isdigit() or int(cells[1]) < 20:
            continue
        day = parse_date(f"{cells[0][:4]}.{cells[0][4:6]}.{cells[0][6:8]}")
        maturity = parse_date(f"{cells[4][:4]}.{cells[4][4:6]}.{cells[4][6:8]}") if len(cells[4].strip()) == 8 else None
        price = _number(cells[7])
        if not day or price is None:
            continue
        reporters = _number(cells[20])
        rows.append({
            "price_date": day,
            "kind": cells[1].strip(),
            "code": cells[2].strip(),
            "name": compact(cells[3]),
            "maturity": maturity,
            "coupon": _number(cells[5]),
            "yield": _number(cells[6]),
            "price": price,
            "change": _number(cells[8]),
            "simple_yield": _number(cells[14]),
            "reporters": int(reporters) if reporters is not None else None,
        })
    return rows


def available_files(session: requests.Session) -> list[tuple[str, str]]:
    """``(date, url)`` for every daily file the index page links, newest first."""
    response = session.get(JSDA_INDEX_URL, timeout=_TIMEOUT)
    if response.status_code == 429:
        raise JsdaRateLimited("JSDA refused the request (too many requests); try again later")
    response.raise_for_status()
    found = {}
    for year, stamp in _FILE.findall(response.text):
        day = f"{year}-{stamp[2:4]}-{stamp[4:6]}"
        found[day] = f"{JSDA_BASE}files/{year}/S{stamp}.csv"
    return sorted(found.items(), reverse=True)


def fetch_reference_prices(
    *,
    days: int,
    skip: set[str],
    session: requests.Session | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """The newest ``days`` daily files not in ``skip``, read a few seconds apart."""
    client = session or requests.Session()
    rows: list[dict[str, Any]] = []
    detail: dict[str, Any] = {"dates": []}
    wanted = [(day, url) for day, url in available_files(client) if day not in skip][:max(days, 0)]
    for index, (day, url) in enumerate(wanted):
        if index:
            sleep(_PAUSE_SECONDS)
        response = client.get(url, timeout=_TIMEOUT)
        if response.status_code == 429:
            detail["stopped"] = "JSDA refused further requests (too many requests); the rest are read on the next run"
            break
        response.raise_for_status()
        rows.extend(parse_reference_csv(response.content))
        detail["dates"].append(day)
    return rows, detail


def store_reference_prices(conn, rows: list[dict[str, Any]]) -> int:
    columns = ("price_date", "kind", "code", "name", "maturity", "coupon", "yield", "price", "change", "simple_yield", "reporters")
    conn.executemany(
        f"INSERT OR REPLACE INTO Bond_Market_Prices ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
        [tuple(row.get(column) for column in columns) for row in rows],
    )
    return len(rows)


def latest_quotes(conn) -> list[dict[str, Any]]:
    """Each bond's most recent reference price."""
    try:
        cursor = conn.execute(
            "SELECT p.* FROM Bond_Market_Prices p JOIN (SELECT code, MAX(price_date) AS price_date FROM Bond_Market_Prices GROUP BY code) latest "
            "ON latest.code = p.code AND latest.price_date = p.price_date"
        )
    except Exception:  # noqa: BLE001 - no prices stored yet
        return []
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _issuer_key(text: str | None) -> str:
    return re.sub(r"株式会社|\(株\)|[\s・.,\d]", "", compact(text or ""))


def match_quotes(bonds: list[dict[str, Any]], quotes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """The reference quote for each bond, keyed by ``bond_id``.

    A quote matches a bond with the same maturity and coupon; the series in
    its name must agree when both are known, and when several issuers share
    those terms the one whose short name the company name starts with wins.
    """
    by_terms: dict[tuple[str, float], list[dict[str, Any]]] = {}
    for quote in quotes:
        if quote.get("maturity") and quote.get("coupon") is not None:
            by_terms.setdefault((quote["maturity"], round(quote["coupon"], 3)), []).append(quote)
    matches: dict[str, dict[str, Any]] = {}
    for bond in bonds:
        if not bond.get("maturity") or bond.get("coupon") is None:
            continue
        candidates = by_terms.get((bond["maturity"], round(bond["coupon"] * 100, 3)), [])
        if bond.get("series") is not None:
            candidates = [quote for quote in candidates if series_from_name(quote["name"]) in (None, bond["series"])]
        if len(candidates) > 1 or (candidates and bond.get("series") is None):
            company = _issuer_key(bond.get("company_name") if bond.get("is_parent", 1) else bond.get("issuer"))
            named = [quote for quote in candidates if (key := _issuer_key(quote["name"])) and (company.startswith(key[:4]) or key[:4] in company)]
            candidates = named
        if len(candidates) == 1:
            matches[bond["bond_id"]] = candidates[0]
    return matches
