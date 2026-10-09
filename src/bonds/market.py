"""Japanese government bond yields from the Ministry of Finance.

The MOF publishes the JGB par-yield curve (国債金利情報) as two CSV files:
the full history since 1974 and the current month. Dates use Japanese eras
(``R8.10.1`` is 1 October 2026); yields are percentages, ``-`` where a tenor
was not quoted.
"""

from __future__ import annotations

import csv
import io
import logging
import re

import requests

from .parsing import parse_date

logger = logging.getLogger(__name__)

MOF_HISTORY_URL = "https://www.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv"
MOF_CURRENT_URL = "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv"
_TIMEOUT = (10, 60)


def parse_jgb_csv(content: bytes) -> list[tuple[str, float, float]]:
    """``(date, tenor in years, yield as a decimal)`` rows from a MOF curve file."""
    text = content.decode("cp932", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))
    header_index = next((i for i, row in enumerate(rows) if row and row[0].strip() == "基準日"), None)
    if header_index is None:
        raise ValueError("The MOF curve file has no 基準日 header")
    tenors: list[float | None] = []
    for cell in rows[header_index][1:]:
        match = re.match(r"\s*(\d+)\s*年", cell)
        tenors.append(float(match.group(1)) if match else None)
    points: list[tuple[str, float, float]] = []
    for row in rows[header_index + 1:]:
        if not row:
            continue
        day = parse_date(row[0])
        if not day:
            continue
        for tenor, cell in zip(tenors, row[1:], strict=False):
            if tenor is None:
                continue
            try:
                value = float(cell)
            except ValueError:
                continue
            points.append((day, tenor, round(value / 100, 8)))
    return points


def fetch_jgb_curve(*, history: bool, session: requests.Session | None = None) -> list[tuple[str, float, float]]:
    """Download the current month's curve, and the full history when ``history`` is set."""
    client = session or requests.Session()
    points: list[tuple[str, float, float]] = []
    for url in ((MOF_HISTORY_URL, MOF_CURRENT_URL) if history else (MOF_CURRENT_URL,)):
        response = client.get(url, timeout=_TIMEOUT)
        response.raise_for_status()
        points.extend(parse_jgb_csv(response.content))
    return points


def store_jgb_curve(conn, points: list[tuple[str, float, float]]) -> int:
    conn.executemany(
        "INSERT INTO JGB_Yields (curve_date, tenor, yield) VALUES (?, ?, ?) "
        "ON CONFLICT(curve_date, tenor) DO UPDATE SET yield = excluded.yield",
        points,
    )
    return len({day for day, _, _ in points})
