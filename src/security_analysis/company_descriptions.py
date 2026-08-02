"""Shared Yahoo Finance company-description cache.

The standardized market database is rebuildable, so descriptions live in a
small side table in that same database. A blank response is cached briefly so
an unavailable provider is not queried on every page load, then retried.
"""

from __future__ import annotations

import html
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from src.orchestrator.common.sqlite import connect_read, transaction
from src.utilities.stock_prices import _provider_symbol_for_ticker

logger = logging.getLogger(__name__)

_TABLE = "Company_Descriptions"
_YAHOO_PROFILE_PAGE_URL = "https://finance.yahoo.com/quote/{symbol}/profile/"
_YAHOO_CRUMB_URL = "https://query1.finance.yahoo.com/v1/test/getcrumb"
_YAHOO_PROFILE_URLS = (
    "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}",
    "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}",
)
_YAHOO_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
    ),
}
_CACHE_PROVIDER = "Yahoo Finance profile data v2"
_EMPTY_CACHE_TTL = timedelta(minutes=15)


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _key(company_code: str | None, ticker: str | None) -> str:
    code = str(company_code or "").strip()
    if code:
        return f"company:{code}"
    return f"ticker:{str(ticker or '').strip().casefold()}"


def _ensure_table(db_path: str) -> None:
    with transaction(db_path) as conn:
        conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {_TABLE} (
                cache_key TEXT PRIMARY KEY,
                company_code TEXT,
                ticker TEXT,
                description TEXT NOT NULL DEFAULT '',
                provider TEXT NOT NULL DEFAULT 'Yahoo Finance',
                fetched_at TEXT NOT NULL
            )"""
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_company_descriptions_code "
            f"ON {_TABLE}(company_code)"
        )


def _cached(db_path: str, cache_key: str) -> dict[str, Any] | None:
    try:
        conn = connect_read(db_path)
    except (FileNotFoundError, OSError):
        return None
    try:
        row = conn.execute(
            f"SELECT cache_key, company_code, ticker, description, provider, fetched_at "
            f"FROM {_TABLE} WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
    except Exception:
        return None
    finally:
        conn.close()
    return dict(row) if row else None


def _profile_description(payload: dict[str, Any]) -> str:
    profile = (((payload.get("quoteSummary") or {}).get("result") or [{}])[0] or {}).get("assetProfile") or {}
    return str(profile.get("longBusinessSummary") or "").strip()


def _summary_from_payload(value: Any) -> str:
    """Find Yahoo's long business summary in a nested page payload."""
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).casefold() == "longbusinesssummary":
                if isinstance(nested, dict):
                    nested = nested.get("raw") or nested.get("fmt") or nested.get("value")
                if nested:
                    return str(nested).strip()
            found = _summary_from_payload(nested)
            if found:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _summary_from_payload(nested)
            if found:
                return found
    return ""


def _summary_from_script(script_text: str) -> str:
    """Extract a summary from a JSON or JavaScript payload embedded in HTML."""
    text = html.unescape(script_text or "")
    if "longBusinessSummary" not in text:
        return ""
    try:
        found = _summary_from_payload(json.loads(text))
    except (TypeError, ValueError, json.JSONDecodeError):
        found = ""
    if found:
        return found

    match = re.search(
        r'(?P<key>["\']?longBusinessSummary["\']?)\s*:\s*'
        r'(?P<value>"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\')',
        text,
    )
    if not match:
        return ""
    raw_value = match.group("value")
    try:
        if raw_value.startswith('"'):
            return str(json.loads(raw_value)).strip()
        return bytes(raw_value[1:-1], "utf-8").decode("unicode_escape").strip()
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return raw_value[1:-1].strip()


def _new_yahoo_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(_YAHOO_HEADERS)
    return session


def _fetch_profile_page_html(session: requests.Session, symbol: str) -> str:
    """Open the profile page so Yahoo sets the cookies needed for its API."""
    try:
        response = session.get(
            _YAHOO_PROFILE_PAGE_URL.format(symbol=requests.utils.quote(symbol, safe="")),
            params={"guccounter": "1"},
            timeout=8,
        )
        response.raise_for_status()
        return response.text
    except Exception as exc:  # noqa: BLE001 - optional provider fallback
        logger.info("Yahoo profile page unavailable for %s: %s", symbol, exc)
        return ""


def _fetch_yahoo_crumb(session: requests.Session) -> str:
    """Get the short-lived crumb paired with the profile-page cookies."""
    try:
        response = session.get(_YAHOO_CRUMB_URL, timeout=8)
        response.raise_for_status()
        return response.text.strip()
    except Exception as exc:  # noqa: BLE001 - optional provider fallback
        logger.info("Yahoo crumb unavailable: %s", exc)
        return ""


def _extract_profile_page_description(page_html: str) -> str:
    """Read the Description paragraph from Yahoo's public profile page."""
    if not page_html:
        return ""

    soup = BeautifulSoup(page_html, "html.parser")
    section = soup.select_one('[data-testid="description"]')
    if section:
        paragraph = section.find("p")
        if paragraph:
            description = " ".join(paragraph.get_text(" ", strip=True).split())
            if description:
                return description

    for heading in soup.find_all(re.compile(r"^h[1-6]$")):
        if " ".join(heading.get_text(" ", strip=True).split()).casefold() != "description":
            continue
        paragraph = heading.find_next("p")
        if paragraph:
            description = " ".join(paragraph.get_text(" ", strip=True).split())
            if description:
                return description

    for script in soup.find_all("script"):
        description = _summary_from_script(script.string or script.get_text())
        if description:
            return description
    return ""


def _fetch_yahoo_profile_page(symbol: str) -> str:
    """Fetch the profile page and its authenticated assetProfile payload."""
    session = _new_yahoo_session()
    page_description = _extract_profile_page_description(
        _fetch_profile_page_html(session, symbol)
    )
    return page_description or _fetch_yahoo_http(symbol, session=session)


def _fetch_yahoo_http(symbol: str, session: requests.Session | None = None) -> str:
    """Try Yahoo's structured profile endpoints with a profile-page crumb."""
    session = session or _new_yahoo_session()
    if not session.cookies:
        _fetch_profile_page_html(session, symbol)
    crumb = _fetch_yahoo_crumb(session)
    if not crumb:
        return ""
    for endpoint in _YAHOO_PROFILE_URLS:
        try:
            response = session.get(
                endpoint.format(symbol=requests.utils.quote(symbol, safe="")),
                params={
                    "modules": "assetProfile",
                    "crumb": crumb,
                    "lang": "en-US",
                    "region": "US",
                },
                timeout=8,
            )
            response.raise_for_status()
            description = _profile_description(response.json())
            if description:
                return description
        except Exception as exc:  # noqa: BLE001 - optional provider fallback
            logger.info("Yahoo profile endpoint unavailable for %s: %s", symbol, exc)
    return ""


def _fetch_yahoo_yfinance(symbol: str) -> str:
    """Use yfinance's crumb/session handling when direct profile calls fail."""
    try:
        import yfinance as yf

        info = yf.Ticker(symbol).get_info()
        return str(info.get("longBusinessSummary") or "").strip()
    except Exception as exc:  # noqa: BLE001 - Yahoo is an optional enrichment source
        logger.info("yfinance company description unavailable for %s: %s", symbol, exc)
        return ""


def fetch_yahoo_description(ticker: str | None) -> str:
    """Fetch Yahoo's long business summary for a ticker, if available."""
    symbol = _provider_symbol_for_ticker(str(ticker or "").strip())
    if not symbol:
        return ""
    description = _fetch_yahoo_profile_page(symbol)
    description = description or _fetch_yahoo_http(symbol)
    return description or _fetch_yahoo_yfinance(symbol)


def _empty_cache_is_fresh(cached: dict[str, Any]) -> bool:
    if str(cached.get("description") or "").strip():
        return True
    if cached.get("provider") != _CACHE_PROVIDER:
        return False
    try:
        fetched_at = datetime.fromisoformat(str(cached.get("fetched_at")))
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - fetched_at < _EMPTY_CACHE_TTL
    except (TypeError, ValueError):
        return False


def get_or_fetch_description(
    db_path: str,
    company_code: str | None,
    ticker: str | None,
) -> str:
    """Return a cached description or fetch/store it once for this company."""
    cache_key = _key(company_code, ticker)
    if not cache_key.split(":", 1)[1]:
        return ""
    _ensure_table(db_path)
    cached = _cached(db_path, cache_key)
    if cached is not None and _empty_cache_is_fresh(cached):
        return str(cached.get("description") or "")

    description = fetch_yahoo_description(ticker)
    with transaction(db_path) as conn:
        conn.execute(
            f"""INSERT INTO {_TABLE}
               (cache_key, company_code, ticker, description, provider, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(cache_key) DO UPDATE SET
                 description = excluded.description,
                 ticker = excluded.ticker,
                 provider = excluded.provider,
                 fetched_at = excluded.fetched_at""",
            (
                cache_key,
                str(company_code or "").strip() or None,
                str(ticker or "").strip() or None,
                description,
                _CACHE_PROVIDER,
                _timestamp(),
            ),
        )
    return description
