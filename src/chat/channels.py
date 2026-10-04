"""Public channel catalog: fixed topic channels plus one channel per company."""

from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from src.orchestrator.common.db_config import get_db2
from src.orchestrator.common.sqlite import connect_read
from src.utilities.stock_prices import tse_code

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Topic:
    slug: str
    name: str
    description: str


# Order is the sidebar order.
TOPICS: tuple[Topic, ...] = (
    Topic("general", "General", "Anything markets, and the workstation itself"),
    Topic("stocks", "Stocks", "Equities: single names, sectors, and valuation"),
    Topic("bonds", "Bonds", "Government and corporate bonds, credit, and rates"),
    Topic("options", "Options", "Options, volatility, and derivative strategies"),
    Topic("etfs", "ETFs", "ETFs, index funds, and fund flows"),
    Topic("futures", "Futures", "Index, rate, and commodity futures"),
    Topic("fx", "FX", "Currencies and the yen"),
    Topic("commodities", "Commodities", "Energy, metals, and agriculture"),
    Topic("macro", "Macro", "Central banks, inflation, and the economy"),
    Topic("ideas", "Ideas", "Investment ideas and theses for discussion"),
    Topic("earnings", "Earnings", "Results season: reports, guidance, and surprises"),
    Topic("filings", "Filings", "EDINET filings, disclosures, and corporate actions"),
)
_TOPICS_BY_SLUG = {topic.slug: topic for topic in TOPICS}

_EDINET_CODE = re.compile(r"^E\d{5}$")
_CHANNEL_ID = re.compile(r"^(topic|company):([A-Za-z0-9_-]{1,32})$")


def topic_channel_id(slug: str) -> str:
    return f"topic:{slug}"


def company_channel_id(edinet_code: str) -> str:
    return f"company:{edinet_code}"


def parse_channel_id(channel_id: str) -> tuple[str, str] | None:
    """``("topic", slug)`` or ``("company", edinet_code)`` for a well-formed id, else ``None``."""
    match = _CHANNEL_ID.match(channel_id or "")
    if not match:
        return None
    kind, value = match.group(1), match.group(2)
    if kind == "topic" and value not in _TOPICS_BY_SLUG:
        return None
    if kind == "company" and not _EDINET_CODE.match(value):
        return None
    return kind, value


def topic(slug: str) -> Topic | None:
    return _TOPICS_BY_SLUG.get(slug)


@dataclass(frozen=True)
class Company:
    company_code: str
    company_name: str
    ticker: str | None


def _market_db() -> Path | None:
    try:
        path = Path(get_db2())
    except Exception:  # noqa: BLE001 - chat works without market data
        return None
    return path if path.is_file() else None


def _row_company(row: sqlite3.Row) -> Company:
    return Company(
        company_code=str(row["Company_Code"]),
        company_name=str(row["Company_Name"] or row["Company_Code"]),
        ticker=tse_code(row["Company_Ticker"]) or (str(row["Company_Ticker"]) if row["Company_Ticker"] else None),
    )


def resolve_companies(tokens: list[str], db_path: Path | None = None) -> dict[str, Company]:
    """Match ``$`` reference tokens (EDINET codes or tickers such as ``7203``) to companies.

    Returns the tokens that matched, keyed as given. A missing or partial
    market database simply yields fewer matches.
    """
    path = db_path or _market_db()
    wanted = [token.strip() for token in tokens if token and token.strip()][:50]
    if path is None or not wanted:
        return {}
    found: dict[str, Company] = {}
    try:
        conn = connect_read(path)
    except (OSError, sqlite3.Error):
        return {}
    try:
        for token in wanted:
            upper = token.upper()
            if _EDINET_CODE.match(upper):
                row = conn.execute(
                    "SELECT Company_Code, Company_Name, Company_Ticker FROM CompanyInfo WHERE Company_Code = ?",
                    (upper,),
                ).fetchone()
            else:
                code = tse_code(upper)
                variants = [upper, f"{code}0", code] if code else [upper]
                placeholders = ",".join("?" * len(variants))
                row = conn.execute(
                    "SELECT Company_Code, Company_Name, Company_Ticker FROM CompanyInfo "
                    f"WHERE UPPER(Company_Ticker) IN ({placeholders}) LIMIT 1",
                    variants,
                ).fetchone()
            if row is not None:
                found[token] = _row_company(row)
    except sqlite3.Error as exc:
        logger.warning("Chat company lookup failed: %s", exc)
    finally:
        conn.close()
    return found


def company(edinet_code: str, db_path: Path | None = None) -> Company | None:
    return resolve_companies([edinet_code], db_path).get(edinet_code)
