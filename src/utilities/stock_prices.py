import logging
import random
import sqlite3
import time
from collections.abc import Callable
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utilities.price_provenance import (
    ensure_price_provenance_columns,
    refresh_split_adjusted_prices,
    utc_now,
)
from src.utilities.price_provenance import (
    source_id as build_source_id,
)

logger = logging.getLogger(__name__)

_STOOQ_DOWNLOAD_ENDPOINT = "https://stooq.com/q/d/l/"

_JPX_QUOTE_ENDPOINT = "https://quote.jpx.co.jp/jpxhp/main/index.aspx"
_JPX_PROVIDER_NAME = "JPX quote"
_JPX_SOURCE_REVISION = "quote-jpx-historical-v1"
_JPX_MAX_HISTORY_ROWS = 50
_STOOQ_PROVIDER_NAME = "Stooq"
_YAHOO_PROVIDER_NAME = "Yahoo Finance chart"

_JPX_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.8,en;q=0.6",
    "Referer": "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=stock_search",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36",
}

_STOOQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0 Safari/537.36",
    "Referer": "https://stooq.com/",
}

_YAHOO_CHART_ENDPOINTS = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
    "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
)

_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0 Safari/537.36",
}

_PROVIDER_MAX_ATTEMPTS = 3
_PROVIDER_BACKOFF_BASE_SECONDS = 1.0
_PROVIDER_BACKOFF_MAX_SECONDS = 30.0
_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS = 300.0
_PROVIDER_FAILURE_COOLDOWN_SECONDS = 30.0
_PROVIDER_RETRY_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 524})
_PROVIDER_RATE_LIMIT_STATUS_CODES = frozenset({403, 429})
_PROVIDER_COOLDOWNS: dict[str, float] = {}

_PROVIDER_ERRORS = (
    requests.RequestException,
    RuntimeError,
    ValueError,
    KeyError,
    TypeError,
)


class _ProviderCoverageError(RuntimeError):
    """Raised when a provider responds but cannot cover the requested range."""


class _ProviderRateLimitError(RuntimeError):
    """Raised when a provider asks us to slow down or blocks the request."""


RequestCallable = Callable[..., requests.Response]
ResponseValidator = Callable[[requests.Response], None]

ProviderFetcher = Callable[
    [str, str | None],
    pd.DataFrame | tuple[pd.DataFrame, list[dict]],
]


def _provider_cooldown_remaining(provider: str) -> float:
    """Return the active cooldown in seconds, removing expired entries."""
    deadline = _PROVIDER_COOLDOWNS.get(provider)
    if deadline is None:
        return 0.0
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        _PROVIDER_COOLDOWNS.pop(provider, None)
        return 0.0
    return remaining


def _mark_provider_cooldown(provider: str, seconds: float) -> None:
    """Pause a provider after rate limiting or repeated transient failures."""
    deadline = time.monotonic() + max(float(seconds), 0.0)
    _PROVIDER_COOLDOWNS[provider] = max(
        deadline,
        _PROVIDER_COOLDOWNS.get(provider, 0.0),
    )


def _clear_provider_cooldown(provider: str) -> None:
    _PROVIDER_COOLDOWNS.pop(provider, None)


def _reset_provider_cooldowns() -> None:
    """Clear process-local cooldowns; intended for tests and controlled jobs."""
    _PROVIDER_COOLDOWNS.clear()


def _retry_after_seconds(response: requests.Response) -> float | None:
    """Parse a numeric or HTTP-date ``Retry-After`` response header."""
    headers = getattr(response, "headers", None)
    raw_value = headers.get("Retry-After") if headers else None
    if raw_value is None:
        return None
    try:
        return max(float(raw_value), 0.0)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(str(raw_value))
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max((retry_at - datetime.now(timezone.utc)).total_seconds(), 0.0)


def _retry_delay(attempt: int, response: requests.Response | None = None) -> float:
    """Calculate bounded exponential backoff with a small jitter component."""
    retry_after = _retry_after_seconds(response) if response is not None else None
    if retry_after is not None:
        return min(retry_after, _PROVIDER_BACKOFF_MAX_SECONDS)
    exponential = min(
        _PROVIDER_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)),
        _PROVIDER_BACKOFF_MAX_SECONDS,
    )
    return exponential + random.uniform(0.0, min(0.25, _PROVIDER_BACKOFF_MAX_SECONDS - exponential))


def _sleep_for_retry(provider: str, message: str, attempt: int, delay: float) -> None:
    """Log and sleep before the next provider request attempt."""
    logger.info(
        "%s %s on attempt %s/%s; retrying in %.2fs",
        provider, message, attempt, _PROVIDER_MAX_ATTEMPTS, delay,
    )
    time.sleep(delay)


def _retry_response_if_needed(
    provider: str,
    response: requests.Response,
    attempt: int,
    *,
    cooldown_on_failure: bool,
) -> bool:
    """Handle retryable HTTP responses and return whether to retry."""
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code in _PROVIDER_RATE_LIMIT_STATUS_CODES:
        if attempt >= _PROVIDER_MAX_ATTEMPTS:
            if cooldown_on_failure:
                retry_after = _retry_after_seconds(response) or 0.0
                _mark_provider_cooldown(
                    provider,
                    max(_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS, retry_after),
                )
            raise _ProviderRateLimitError(
                f"{provider} returned HTTP {status_code}"
            )
        delay = _retry_delay(attempt, response)
        _sleep_for_retry(provider, f"rate limit (HTTP {status_code})", attempt, delay)
        return True

    if status_code not in _PROVIDER_RETRY_STATUS_CODES:
        return False
    last_error = requests.HTTPError(
        f"{provider} returned retryable HTTP {status_code}"
    )
    if attempt >= _PROVIDER_MAX_ATTEMPTS:
        if cooldown_on_failure:
            _mark_provider_cooldown(provider, _PROVIDER_FAILURE_COOLDOWN_SECONDS)
        response.raise_for_status()
        raise last_error
    delay = _retry_delay(attempt, response)
    _sleep_for_retry(provider, f"HTTP {status_code}", attempt, delay)
    return True


def _retry_response_validator(
    provider: str,
    response: requests.Response,
    attempt: int,
    response_validator: ResponseValidator | None,
    *,
    cooldown_on_failure: bool,
) -> bool:
    """Validate a successful response, retrying provider rate-limit payloads."""
    if response_validator is None:
        return True
    try:
        response_validator(response)
    except _ProviderRateLimitError:
        if attempt >= _PROVIDER_MAX_ATTEMPTS:
            if cooldown_on_failure:
                _mark_provider_cooldown(provider, _PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS)
            raise
        delay = _retry_delay(attempt, response)
        _sleep_for_retry(provider, "rate-limit payload", attempt, delay)
        return False
    return True


def _request_with_retries(
    provider: str,
    request_fn: RequestCallable,
    url: str,
    *,
    response_validator: ResponseValidator | None = None,
    cooldown_on_failure: bool = True,
    **kwargs,
) -> requests.Response:
    """Perform one bounded, rate-limit-aware HTTP request.

    Retryable HTTP statuses and transport failures use exponential backoff.
    A rate-limited provider enters a process-local cooldown so a bulk update
    does not repeat the same blocked request for every ticker.
    """
    remaining = _provider_cooldown_remaining(provider)
    if remaining > 0:
        raise _ProviderRateLimitError(
            f"{provider} is cooling down for {remaining:.1f}s"
        )

    for attempt in range(1, _PROVIDER_MAX_ATTEMPTS + 1):
        try:
            response = request_fn(url, **kwargs)
        except requests.RequestException as exc:
            if attempt >= _PROVIDER_MAX_ATTEMPTS:
                if cooldown_on_failure:
                    _mark_provider_cooldown(provider, _PROVIDER_FAILURE_COOLDOWN_SECONDS)
                raise
            delay = _retry_delay(attempt)
            _sleep_for_retry(provider, type(exc).__name__, attempt, delay)
            continue

        if _retry_response_if_needed(
            provider,
            response,
            attempt,
            cooldown_on_failure=cooldown_on_failure,
        ):
            continue

        response.raise_for_status()
        if not _retry_response_validator(
            provider,
            response,
            attempt,
            response_validator,
            cooldown_on_failure=cooldown_on_failure,
        ):
            continue

        _clear_provider_cooldown(provider)
        return response

    raise RuntimeError(f"No response received from {provider}")


def _provider_symbol_for_ticker(ticker: str) -> str:
    """Map stored ticker values to the symbol expected by Yahoo Finance."""
    clean_ticker = str(ticker).strip()
    if not clean_ticker:
        return clean_ticker
    if clean_ticker.lower().endswith(".jp"):
        return clean_ticker[:-3] + ".T"
    if "." in clean_ticker:
        return clean_ticker
    if len(clean_ticker) >= 4 and clean_ticker[:4].isdigit():
        return clean_ticker[:4] + ".T"
    return clean_ticker


def _stooq_symbol_for_ticker(ticker: str) -> str:
    """Map stored ticker values to the symbol expected by Stooq."""
    clean_ticker = str(ticker).strip().lower()
    if not clean_ticker:
        return clean_ticker
    if clean_ticker.endswith(".jp"):
        return clean_ticker
    if "." in clean_ticker:
        return clean_ticker
    if len(clean_ticker) >= 4 and clean_ticker[:4].isdigit():
        return clean_ticker[:4] + ".jp"
    return clean_ticker


def _jpx_symbol_for_ticker(ticker: str) -> str | None:
    """Map a stored Japanese ticker to the JPX four-character code.

    Standardized.db stores Japanese codes as five digits (for example,
    ``31100``), while the JPX quote site expects ``3110``.  Tickers that do
    not look like Japanese exchange codes return ``None`` so the JPX request
    is not attempted for currencies, foreign ETFs, or other symbols.
    """
    clean_ticker = str(ticker).strip()
    lowered = clean_ticker.lower()
    if lowered.endswith(".jp"):
        clean_ticker = clean_ticker[:-3]
    elif lowered.endswith(".t"):
        clean_ticker = clean_ticker[:-2]
    if len(clean_ticker) == 4 and clean_ticker.isdigit():
        return clean_ticker
    if len(clean_ticker) == 5 and clean_ticker.isdigit() and clean_ticker.endswith("0"):
        return clean_ticker[:4]
    return None


def _parse_jpx_number(value: str) -> float | None:
    """Parse a JPX table number, returning ``None`` for unavailable values."""
    text = str(value or "").strip().replace(",", "").replace(" ", "")
    text = text.replace("−", "-").replace("－", "-")
    if not text or text in {"-", "--", "—", "―"}:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_jpx_history(html: str, start_date: str | None = None) -> pd.DataFrame:
    """Parse the JPX ``#historical`` HTML table into ``Date``/``Close``."""
    soup = BeautifulSoup(html or "", "html.parser")
    table = soup.find("table", id="historical")
    if table is None:
        raise RuntimeError("JPX historical table was not present in the response")

    rows: list[dict[str, object]] = []
    for table_row in table.find_all("tr"):
        cells = table_row.find_all("td")
        if len(cells) < 5:
            continue
        date_value = table_row.get("data-value") or cells[0].get_text(" ", strip=True)
        if len(date_value) == 8 and date_value.isdigit():
            date = pd.to_datetime(date_value, format="%Y%m%d", errors="coerce")
        else:
            date = pd.to_datetime(date_value, errors="coerce")
        close = _parse_jpx_number(cells[4].get_text(" ", strip=True))
        if pd.isna(date) or close is None:
            continue
        rows.append({"Date": date.strftime("%Y-%m-%d"), "Close": close})

    history = pd.DataFrame(rows, columns=["Date", "Close"])
    if history.empty:
        return history
    history = history.drop_duplicates(subset=["Date"], keep="last")
    history = history.sort_values("Date").reset_index(drop=True)
    if start_date:
        lower_bound = pd.Timestamp(start_date).strftime("%Y-%m-%d")
        history = history[history["Date"] >= lower_bound].reset_index(drop=True)
    return history


def _fetch_jpx_history(provider_ticker: str, start_date: str | None = None) -> pd.DataFrame:
    """Fetch JPX's split-adjusted historical closing-price table.

    The public quote page exposes the most recent 50 trading sessions.  It is
    therefore used for current/incremental updates; older backfills continue
    through the existing Stooq/Yahoo fallback chain.  JPX rejects a direct
    historical-page request unless it follows the quote site's normal
    search/detail navigation, so keep one session and establish that
    referrer chain before reading the table.
    """
    session = requests.Session()
    search_url = f"{_JPX_QUOTE_ENDPOINT}?F=stock_search"
    session.get(
        search_url,
        headers=_JPX_HEADERS,
        timeout=30,
    ).raise_for_status()

    detail_url = (
        f"{_JPX_QUOTE_ENDPOINT}?f=stock_detail&"
        f"qcode={provider_ticker}"
    )
    session.get(
        _JPX_QUOTE_ENDPOINT,
        params={"f": "stock_detail", "qcode": provider_ticker},
        headers={**_JPX_HEADERS, "Referer": search_url},
        timeout=30,
    ).raise_for_status()

    response = session.get(
        _JPX_QUOTE_ENDPOINT,
        params={
            "f": "stock_detail",
            "disptype": "historical",
            "qcode": provider_ticker,
        },
        headers={**_JPX_HEADERS, "Referer": detail_url},
        timeout=30,
    )
    response.raise_for_status()
    return _parse_jpx_history(response.text, start_date=start_date)


def _flatten_history_column_name(column_name) -> str:
    """Flatten provider column names so they can be matched consistently."""
    if isinstance(column_name, tuple):
        parts = [str(part).strip() for part in column_name if str(part).strip()]
        return " ".join(parts)
    return str(column_name).strip()


def _column_tokens(column_name: str) -> set[str]:
    """Return lowercase tokens from a history column name."""
    return set(str(column_name).lower().replace("_", " ").split())


def _find_history_column(columns, required_tokens, excluded_tokens=None):
    """Find the first history column whose tokens match the requested set."""
    excluded_tokens = set(excluded_tokens or [])
    for column_name in columns:
        tokens = _column_tokens(column_name)
        if required_tokens.issubset(tokens) and not excluded_tokens.intersection(tokens):
            return column_name
    return None


def _validate_stooq_response(response: requests.Response) -> None:
    """Treat Stooq's successful rate-limit text response as retryable."""
    lowered_text = response.text.lower()
    if "write to www@stooq.com" in lowered_text or "exceeded the daily hits limit" in lowered_text:
        raise _ProviderRateLimitError("Stooq returned a daily-hit limit response")


def _fetch_stooq_history(provider_ticker: str, start_date: str | None = None) -> pd.DataFrame:
    """Fetch daily price history for a ticker from Stooq's CSV endpoint."""
    params = {
        "s": provider_ticker,
        "i": "d",
    }
    if start_date:
        params["d1"] = pd.Timestamp(start_date).strftime("%Y%m%d")
        params["d2"] = pd.Timestamp.today().strftime("%Y%m%d")

    response = _request_with_retries(
        _STOOQ_PROVIDER_NAME,
        requests.get,
        _STOOQ_DOWNLOAD_ENDPOINT,
        response_validator=_validate_stooq_response,
        params=params,
        headers=_STOOQ_HEADERS,
        timeout=30,
    )

    text = response.text.strip()
    if not text:
        return pd.DataFrame(columns=["Date", "Close"])

    history = pd.read_csv(StringIO(text))
    if history.empty:
        return pd.DataFrame(columns=["Date", "Close"])
    return history


def _extract_split_events(result: dict) -> list[dict]:
    """Extract split events from a Yahoo chart API result.

    Returns a list of ``{"split_date": "YYYY-MM-DD", "ratio_from": N,
    "ratio_to": M}`` dicts.  Yahoo reports the split as ``numerator /
    denominator`` (e.g. 5 / 1 for a 5:1 split), so ``ratio_from`` is the
    denominator and ``ratio_to`` is the numerator.

    These are authoritative for the true split date — the price heuristic
    can misattribute a split to a data-update boundary, whereas the
    provider's event calendar gives the exact date.
    """
    events = result.get("events") or {}
    splits = events.get("splits") or {}
    out: list[dict] = []
    for _ts, event in splits.items():
        if not isinstance(event, dict):
            continue
        try:
            numerator = float(event.get("numerator") or 0)
            denominator = float(event.get("denominator") or 0)
            date_ts = int(event.get("date") or 0)
        except (TypeError, ValueError):
            continue
        if numerator <= 0 or denominator <= 0 or not date_ts:
            continue
        out.append({
            "split_date": pd.to_datetime(date_ts, unit="s", utc=True)
            .tz_convert("Asia/Tokyo").tz_localize(None).strftime("%Y-%m-%d"),
            "ratio_from": denominator,
            "ratio_to": numerator,
        })
    return out


def _validate_yahoo_response(response: requests.Response) -> None:
    """Detect Yahoo's rate-limit payloads even when HTTP status is 200."""
    try:
        payload = response.json()
    except ValueError:
        return
    error = (payload.get("chart") or {}).get("error") or {}
    if not error:
        return
    code = str(error.get("code") or "").lower()
    description = str(error.get("description") or "").lower()
    if code in {"429", "999", "rate_limit"} or "rate limit" in description:
        raise _ProviderRateLimitError("Yahoo Finance returned a rate-limit payload")


def _parse_yahoo_chart_payload(payload: dict) -> tuple[pd.DataFrame, list[dict]]:
    """Convert a Yahoo chart payload into prices and authoritative splits."""
    chart = payload.get("chart", {})
    if chart.get("error"):
        raise RuntimeError(chart["error"])

    results = chart.get("result") or []
    if not results:
        return pd.DataFrame(columns=["Date", "Close"]), []

    result = results[0]
    timestamps = result.get("timestamp") or []
    quotes = result.get("indicators", {}).get("quote") or []
    if not timestamps or not quotes:
        return pd.DataFrame(columns=["Date", "Close"]), []

    close_values = quotes[0].get("close") or []
    row_count = min(len(timestamps), len(close_values))
    if row_count == 0:
        return pd.DataFrame(columns=["Date", "Close"]), []

    price_df = pd.DataFrame(
        {
            "Date": pd.to_datetime(timestamps[:row_count], unit="s", utc=True)
            .tz_convert("Asia/Tokyo")
            .tz_localize(None),
            "Close": close_values[:row_count],
        }
    )
    return price_df, _extract_split_events(result)


def _fetch_yahoo_history(
    provider_ticker: str,
    start_date: str | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    """Fetch daily price history for a ticker from the Yahoo Finance chart API.

    Returns ``(price_df, split_events)``.  *split_events* is a list of dicts
    with ``split_date`` / ``ratio_from`` / ``ratio_to`` for each split in the
    requested range — the authoritative source for true split dates.
    """
    params = {
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    }
    if start_date:
        start_ts = int(pd.Timestamp(start_date).timestamp())
        end_ts = int((pd.Timestamp.utcnow().normalize() + pd.Timedelta(days=1)).timestamp())
        params["period1"] = start_ts
        params["period2"] = end_ts
    else:
        params["range"] = "max"

    last_error = None
    for endpoint in _YAHOO_CHART_ENDPOINTS:
        try:
            response = _request_with_retries(
                _YAHOO_PROVIDER_NAME,
                requests.get,
                endpoint.format(symbol=provider_ticker),
                response_validator=_validate_yahoo_response,
                cooldown_on_failure=False,
                params=params,
                headers=_YAHOO_HEADERS,
                timeout=30,
            )
            return _parse_yahoo_chart_payload(response.json())
        except _ProviderRateLimitError as exc:
            last_error = exc
        except _PROVIDER_ERRORS as exc:
            last_error = exc

    if isinstance(last_error, _ProviderRateLimitError):
        _mark_provider_cooldown(_YAHOO_PROVIDER_NAME, _PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS)
        raise last_error
    _mark_provider_cooldown(_YAHOO_PROVIDER_NAME, _PROVIDER_FAILURE_COOLDOWN_SECONDS)
    raise RuntimeError(
        f"Failed to fetch Yahoo Finance history for {provider_ticker}: {last_error}"
    ) from last_error


def _normalise_price_history(raw_history: pd.DataFrame) -> pd.DataFrame:
    """Convert provider price history into a Date/Close frame."""
    if raw_history is None or raw_history.empty:
        return pd.DataFrame(columns=["Date", "Close"])

    history = raw_history.copy().reset_index()
    history.columns = [_flatten_history_column_name(column) for column in history.columns]

    date_column = _find_history_column(history.columns, {"date"})
    if date_column is None:
        date_column = _find_history_column(history.columns, {"datetime"})

    close_column = _find_history_column(history.columns, {"close"}, excluded_tokens={"adj"})
    if close_column is None:
        close_column = _find_history_column(history.columns, {"adj", "close"})

    if date_column is None or close_column is None:
        raise ValueError(
            "Price provider response missing expected date/close columns. "
            f"Available columns: {list(history.columns)}"
        )

    out_data = pd.DataFrame(
        {
            "Date": pd.to_datetime(history[date_column], errors="coerce").dt.strftime("%Y-%m-%d"),
            "Close": pd.to_numeric(history[close_column], errors="coerce"),
        }
    )
    out_data = out_data.dropna(subset=["Date", "Close"]).drop_duplicates(subset=["Date"], keep="last")
    return out_data.sort_values("Date").reset_index(drop=True)


def _annotate_provider_history(
    normalized: pd.DataFrame,
    provider_name: str,
    provider_ticker: str,
    price_basis: str,
    source_revision: str,
) -> pd.DataFrame:
    """Attach provider metadata used by the provenance append path."""
    normalized.attrs.update(
        {
            "provider": provider_name,
            "provider_symbol": provider_ticker,
            "price_basis": price_basis,
            "source_revision": source_revision,
        }
    )
    return normalized


def _jpx_history_has_requested_coverage(
    history: pd.DataFrame,
    start_date: str | None,
) -> bool:
    """Return whether the bounded JPX page can satisfy the requested range."""
    if history.empty:
        return False
    if start_date is None:
        # The detail page intentionally exposes only the latest 50 sessions.
        # Do not make a first-time ticker update silently lose older history.
        return len(history) < _JPX_MAX_HISTORY_ROWS
    requested = pd.Timestamp(start_date)
    earliest = pd.Timestamp(str(history["Date"].min()))
    if earliest <= requested:
        return True
    # ``start_date`` is the calendar day after the last cached row and may be
    # a weekend or a market holiday.  Allow the first returned trading day to
    # be a short non-trading gap, but reject an older request that fell outside
    # the page's 50-session window.
    return (earliest - requested).days <= 14


def _load_provider_history(
    ticker: str,
    start_date: str | None = None,
) -> tuple[str, pd.DataFrame, list[dict]]:
    """Load normalized history using JPX first and existing fallbacks.

    JPX is used for Japanese incremental updates.  Its public detail page is
    capped at 50 trading sessions, so an initial/full request or a request
    with an older start date falls through to Stooq/Yahoo rather than silently
    truncating the stored history.  European UCITS ETFs still try common
    exchange suffixes (.DE, .L, .MI, .AS, .PA) when the bare ticker fails.

    Returns ``(provider_name, price_df, split_events)``.  *split_events* is a
    list of authoritative split dicts (currently supplied by Yahoo only).
    """
    # Check if ticker looks like a european ETF needing suffix
    _looks_eu = (
        ticker.isalpha() and len(ticker) <= 6 and "." not in ticker
        and not ticker[:4].isdigit() and not ticker.lower().endswith(".jp")
    )

    providers: list[tuple[str, ProviderFetcher, str, str, str]] = []
    jpx_symbol = _jpx_symbol_for_ticker(ticker)
    if jpx_symbol:
        providers.append(
            (
                _JPX_PROVIDER_NAME,
                _fetch_jpx_history,
                jpx_symbol,
                "adjusted",
                _JPX_SOURCE_REVISION,
            )
        )
    providers.extend(
        [
            (
                _STOOQ_PROVIDER_NAME,
                _fetch_stooq_history,
                _stooq_symbol_for_ticker(ticker),
                "unknown",
                "stooq-csv-v1",
            ),
            (
                _YAHOO_PROVIDER_NAME,
                _fetch_yahoo_history,
                _provider_symbol_for_ticker(ticker),
                "adjusted",
                "chart-events-v1",
            ),
        ]
    )
    last_error = None

    for provider_name, fetcher, provider_ticker, price_basis, source_revision in providers:
        try:
            raw_history = fetcher(provider_ticker, start_date=start_date)
            if isinstance(raw_history, tuple):
                raw_history, split_events = raw_history
            else:
                split_events = []
            normalized = _normalise_price_history(raw_history)
            if provider_name == _JPX_PROVIDER_NAME and not _jpx_history_has_requested_coverage(
                normalized, start_date
            ):
                raise _ProviderCoverageError(
                    "JPX historical page is limited to the latest 50 sessions"
                )
            normalized = _annotate_provider_history(
                normalized,
                provider_name,
                provider_ticker,
                price_basis,
                source_revision,
            )
            if normalized.empty:
                raise RuntimeError("provider returned no usable price rows")
            return provider_name, normalized, split_events
        except _ProviderCoverageError as exc:
            last_error = exc
            logger.info(
                "%s did not cover the requested range for %s (%s); trying fallback",
                provider_name, ticker, provider_ticker,
            )
        except _ProviderRateLimitError as exc:
            last_error = exc
            logger.info(
                "%s is temporarily unavailable for %s (%s); trying fallback",
                provider_name, ticker, exc,
            )
        except _PROVIDER_ERRORS as exc:
            last_error = exc
            logger.warning(
                "%s failed for ticker %s (%s): %s",
                provider_name, ticker, provider_ticker, exc,
            )

    # European ETF suffix fallback
    if _looks_eu:
        _EU_SUFFIXES = [".DE", ".L", ".MI", ".AS", ".PA", ".SW"]
        for suffix in _EU_SUFFIXES:
            suffixed = ticker + suffix
            for provider_name, fetcher, symbol_fn in [
                (_STOOQ_PROVIDER_NAME, _fetch_stooq_history, _stooq_symbol_for_ticker),
                (_YAHOO_PROVIDER_NAME, _fetch_yahoo_history, _provider_symbol_for_ticker),
            ]:
                try:
                    p_tkr = symbol_fn(suffixed)
                    raw_history = fetcher(p_tkr, start_date=start_date)
                    if isinstance(raw_history, tuple):
                        raw_history, split_events = raw_history
                    else:
                        split_events = []
                    logger.info(
                        "Fetched %s as %s via %s (%s)",
                        ticker, suffixed, provider_name, p_tkr,
                    )
                    normalized = _normalise_price_history(raw_history)
                    normalized = _annotate_provider_history(
                        normalized,
                        provider_name + f" (as {suffixed})",
                        p_tkr,
                        "adjusted" if provider_name.startswith("Yahoo") else "unknown",
                        "chart-events-v1" if provider_name.startswith("Yahoo")
                        else "stooq-csv-v1",
                    )
                    if normalized.empty:
                        raise RuntimeError("provider returned no usable price rows")
                    return provider_name + f" (as {suffixed})", normalized, split_events
                except _PROVIDER_ERRORS:
                    continue

    raise RuntimeError(
        f"All price providers failed for ticker {ticker}: {last_error}"
    ) from last_error


def _create_prices_table(conn, table_name):
    """Create the stock prices table if it doesn't exist.

    Also ensures a composite index on (Date, Ticker) exists for fast lookups.

    Args:
        conn (sqlite3.Connection): Database connection
        table_name (str): Name of the table to create
    """
    # Do not use pandas ``to_sql`` here.  Its sqlite3 adapter commits when it
    # finishes, which would implicitly close a caller-owned SAVEPOINT during
    # an overwrite update.  Explicit DDL keeps transaction ownership with the
    # caller and is equivalent to the four-column table pandas created here.
    quoted_table = '"' + str(table_name).replace('"', '""') + '"'
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {quoted_table} ("
        '"Date" TEXT, "Ticker" TEXT, "Currency" TEXT, "Price" REAL)'
    )
    # Migrate existing installations as well as newly-created tables.  Rows
    # already present are marked ``unknown`` by the migration; callers can
    # promote them only after their source convention has been verified.
    ensure_price_provenance_columns(conn, table_name)
    logger.debug(f"Stock prices table '{table_name}' is ready")

    # Ensure a composite index on (Date, Ticker) for query performance
    idx_name = f"ix_{table_name}_Date_Ticker"
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS [{idx_name}] ON [{table_name}] (Date, Ticker)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS [ix_{table_name}_Ticker_Date_Currency] "
        f"ON [{table_name}] (Ticker, Date, Currency)"
    )
    logger.debug(f"Index '{idx_name}' on ({table_name}.Date, {table_name}.Ticker) is ready")


# If the last cached price and the first newly-fetched price differ by more
# than this band, the provider retro-adjusts prices (e.g. after a split).
# A 2:1 split produces a 0.5 ratio, a 5:1 split 0.2; a normal overnight move
# is within ±10%.  0.6 / 1.67 (±40%) cleanly separates the two.
_BASIS_CHANGE_LOW = 0.6
_BASIS_CHANGE_HIGH = 1.67


def _append_price_rows(
    conn,
    prices_table: str,
    ticker: str,
    df: pd.DataFrame,
    currency: str,
    *,
    provider: str | None = None,
    price_basis: str | None = None,
    provider_symbol: str | None = None,
    source_revision: str | None = None,
    retrieved_at: str | None = None,
    split_events: list[dict] | None = None,
) -> None:
    """Normalise and append price rows with explicit source provenance."""
    columns = ensure_price_provenance_columns(conn, prices_table)
    if not columns:
        _create_prices_table(conn, prices_table)
        columns = ensure_price_provenance_columns(conn, prices_table)
    out_data = df.copy()
    out_data["Ticker"] = ticker
    out_data["Currency"] = currency
    out_data["Price"] = out_data["Close"]
    provider = provider or df.attrs.get("provider") or "unknown"
    price_basis = (price_basis or df.attrs.get("price_basis") or "unknown").lower()
    if price_basis not in {"raw", "adjusted", "unknown"}:
        raise ValueError(f"Unsupported price basis: {price_basis!r}")
    provider_symbol = provider_symbol or df.attrs.get("provider_symbol") or ticker
    retrieved_at = retrieved_at or utc_now()
    source_revision = source_revision or "chart-csv-v1"
    if "Price_Basis" in columns:
        out_data["Price_Basis"] = price_basis
    if "Provider" in columns:
        out_data["Provider"] = provider
    if "Source_Id" in columns:
        out_data["Source_Id"] = [
            build_source_id(provider, provider_symbol, value)
            for value in out_data["Date"]
        ]
    if "Source_Revision" in columns:
        out_data["Source_Revision"] = source_revision
    if "Adjustment_Factor" in columns:
        # Record the provider's split multiplier when the provider supplies
        # authoritative events.  This is audit metadata; the source Price is
        # not changed and the derived read-model factor is maintained below.
        if split_events and price_basis == "adjusted":
            out_data["Adjustment_Factor"] = [
                _cumulative_split_factor(split_events, str(value)[:10])
                for value in out_data["Date"]
            ]
        else:
            out_data["Adjustment_Factor"] = df.attrs.get("adjustment_factor")
    if "Split_Adjustment_Factor" in columns:
        out_data["Split_Adjustment_Factor"] = None
    if "Adjusted_Price" in columns:
        out_data["Adjusted_Price"] = None
    if "Retrieved_At" in columns:
        out_data["Retrieved_At"] = retrieved_at
    ordered = ["Date", "Ticker", "Currency", "Price"]
    ordered.extend(name for name in (
        "Price_Basis", "Provider", "Source_Id", "Source_Revision",
        "Adjustment_Factor", "Split_Adjustment_Factor", "Adjusted_Price",
        "Retrieved_At",
    ) if name in columns)
    out_data = out_data[ordered]

    # Insert through the connection directly instead of pandas ``to_sql``.
    # ``to_sql`` commits its sqlite3 transaction at the end, which releases
    # SAVEPOINTs used by the overwrite pipeline before it can validate and
    # commit the replacement.  Normalising scalar values also avoids numpy
    # integer values being stored as SQLite blobs.
    def _sqlite_value(value):
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime().isoformat(sep=" ")
        try:
            if bool(pd.isna(value)):
                return None
        except (TypeError, ValueError):
            pass
        if hasattr(value, "item"):
            value = value.item()
        return value

    quoted_table = '"' + str(prices_table).replace('"', '""') + '"'
    quoted_columns = ", ".join(
        '"' + str(column).replace('"', '""') + '"' for column in ordered
    )
    placeholders = ", ".join("?" for _ in ordered)
    rows = [
        tuple(_sqlite_value(value) for value in row)
        for row in out_data.itertuples(index=False, name=None)
    ]
    if rows:
        conn.executemany(
            f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})",
            rows,
        )


def _replace_ticker_rows(
    conn,
    prices_table: str,
    ticker: str,
    full_df: pd.DataFrame,
    currency: str,
) -> int:
    """Delete all cached rows for *ticker* and store the provider's full series.

    **Caution:** prefer :func:`_adjust_cached_rows_to_provider_basis`, which
    reconciles a mixed basis in place and preserves granularity.  This
    wholesale replace should only be used when a consistent full series is
    available at full granularity.

    Returns the number of rows stored.
    """
    conn.execute(f"DELETE FROM {prices_table} WHERE Ticker = ?", (ticker,))
    _append_price_rows(conn, prices_table, ticker, full_df, currency)
    return len(full_df)


def _cumulative_split_factor(split_events: list[dict], after_date: str) -> float:
    """Product of ``ratio_from / ratio_to`` for splits strictly after *after_date*.

    This is the multiplier the provider applied to bring prices cached as of
    *after_date* onto its current (split-adjusted) basis.  Returns 1.0 when
    there are no qualifying splits.
    """
    factor = 1.0
    for event in split_events:
        if event["split_date"] > after_date:
            factor *= event["ratio_from"] / event["ratio_to"]
    return factor


def _split_restore_factor(splits: list[dict], date_str: str) -> float:
    """Multiplier to restore a price at *date_str* to the raw (as-traded) basis.

    A provider that split-adjusts history multiplies every pre-split price by
    ``ratio_to / ratio_from`` for each split that happened after it (for a 1:N
    forward split that's ``N`` — the price was divided by N; for an N:1 reverse
    split it's ``1/N`` — the price was multiplied by N).  Multiplying by the
    same ratios restores the raw (as-traded) price.
    """
    factor = 1.0
    for event in splits:
        if event["split_date"] > date_str:
            factor *= event["ratio_to"] / event["ratio_from"]
    return factor


def _restore_fetched_frame(df: pd.DataFrame, splits: list[dict]) -> pd.DataFrame:
    """Return a copy of *df* with each row restored to the raw basis.

    The provider returns split-adjusted prices; this undoes that adjustment so
    the stored series stays on the raw (as-traded) basis, on which the
    recorded splits are applied at read time.
    """
    if not splits:
        return df
    out = df.copy()
    factors = []
    for date_val in out["Date"]:
        date_str = str(date_val)[:10]
        factors.append(_split_restore_factor(splits, date_str))
    out["Close"] = out["Close"] * factors
    return out


def _restore_stored_rows_to_raw(
    conn,
    prices_table: str,
    ticker: str,
    from_date: str,
    splits: list[dict],
) -> int:
    """Restore stored rows for *ticker* from *from_date* onward to the raw basis.

    Rows before a forward split were divided by the provider's ``ratio_to``;
    multiply them back to the raw (as-traded) value so the whole series is on
    the raw basis.  Rows after every split are untouched (factor 1).

    Returns the number of rows updated.
    """
    if not splits:
        return 0
    rows = conn.execute(
        f"SELECT Date, Price FROM {prices_table} "
        "WHERE Ticker = ? AND Date >= ?",
        (ticker, from_date),
    ).fetchall()
    updated = 0
    for date_val, _price in rows:
        factor = _split_restore_factor(splits, str(date_val)[:10])
        if abs(factor - 1.0) > 1e-9:
            conn.execute(
                f"UPDATE {prices_table} SET Price = Price * ? "
                "WHERE Ticker = ? AND Date = ?",
                (factor, ticker, date_val),
            )
            updated += 1
    return updated


def _record_provider_splits(conn, ticker: str, split_events: list[dict]) -> None:
    """Record authoritative Yahoo split events.

    The event remains usable for raw price rows (``price_basis='raw'`` on the
    event), while Yahoo quote rows retain their provider split-adjusted basis.
    (as-traded) basis — read-time adjustment applies them.
    """
    if not split_events:
        return
    from src.portfolio.split_schema import ensure_split_tables

    ensure_split_tables(None, conn=conn)
    retrieved_at = utc_now()
    for event in split_events:
        split_date = event["split_date"]
        source_symbol = event.get("provider_symbol") or ticker
        source = build_source_id("Yahoo Finance chart", source_symbol, split_date)
        existing = conn.execute(
            "SELECT id FROM Stock_Splits WHERE ticker = ? AND split_date = ? "
            "ORDER BY id LIMIT 1",
            (ticker, split_date),
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE Stock_Splits
                   SET ratio_from = ?, ratio_to = ?, detection_method = 'provider',
                       confirmation = 'confirmed', confirmed_by = 'provider',
                       price_basis = 'raw', provider = 'Yahoo Finance chart',
                       source_id = ?, source_revision = 'chart-events-v1',
                       retrieved_at = ?, source_detail =
                       'Authoritative split event from provider',
                       updated_at = datetime('now')
                   WHERE ticker = ? AND split_date = ?""",
                (event["ratio_from"], event["ratio_to"], source,
                 retrieved_at, ticker, split_date),
            )
        else:
            conn.execute(
                """INSERT INTO Stock_Splits
                   (ticker, split_date, ratio_from, ratio_to,
                    detection_method, confirmation, confirmed_by, price_basis,
                    provider, source_id, source_revision, retrieved_at,
                    source_detail)
                   VALUES (?, ?, ?, ?, 'provider', 'confirmed', 'provider',
                           'raw', 'Yahoo Finance chart', ?, 'chart-events-v1',
                           ?, 'Authoritative split event from provider')""",
                (ticker, split_date, event["ratio_from"], event["ratio_to"],
                 source, retrieved_at),
            )
    logger.info("Recorded %d provider split event(s) for %s", len(split_events), ticker)
    refresh_split_adjusted_prices(conn, ticker=ticker)
    _invalidate_split_cache_for(ticker)


def _reconcile_splits(conn, ticker: str, split_events: list[dict]) -> None:
    """Reconcile split records after restoring prices to the raw basis.

    - Stale heuristic-detected splits whose ratio duplicates a provider split
      are removed — they were mis-dated boundary artifacts, and the provider's
      authoritative date wins.
    - Provider split events are recorded (``price_basis='raw'``, so read-time
      adjustment applies them).
    - Genuine older splits (e.g. manual or confirmed-on-raw-basis) are left
      untouched.
    """
    if split_events:
        from src.portfolio.split_schema import ensure_split_tables

        ensure_split_tables(None, conn=conn)
        _record_provider_splits(conn, ticker, split_events)
        # Only remove an unconfirmed heuristic candidate that is an earlier
        # boundary artifact for the same provider event.  Confirmed/manual
        # actions and candidates after the event are retained for review.
        for event in split_events:
            conn.execute(
                "DELETE FROM Stock_Splits WHERE ticker = ? "
                "AND detection_method = 'price_heuristic' "
                "AND confirmation IN ('pending', 'rejected') "
                "AND ratio_from = ? AND ratio_to = ? AND split_date < ?",
                (ticker, event["ratio_from"], event["ratio_to"], event["split_date"]),
            )


def reconcile_ticker_price_basis(
    conn,
    prices_table: str,
    ticker: str,
) -> dict:
    """Fix interior price-basis discontinuities (boundary artifacts) for a ticker.

    When price updates are infrequent, the provider retro-adjusts the newly
    fetched range after a split while cached rows stay on the old (raw) basis.
    That creates a spurious >40% jump at the fetch boundary *inside* the stored
    series, and the price heuristic mis-records a split at the wrong (boundary)
    date.

    This function is deliberately conservative — it only touches tickers where
    there is a **mis-dated heuristic split**: a heuristic-detected split whose
    ratio matches a provider event at a *different* (true) date.  For those it:
      1. Restores the fetched (provider-adjusted) rows to the raw basis,
         so the whole series is consistent on the raw basis.
      2. Records the split at the provider's true date (``price_basis='raw'``,
         so read-time adjustment applies it).
      3. Removes the mis-dated heuristic split.

    Genuine splits that were correctly dated are left untouched.  A continuity
    check guards against mis-classifying raw data: if restoring the boundary
    row would not approximately line up with the price before the boundary,
    the ticker is skipped.

    This is the backfill for existing databases; :func:`load_ticker_data`
    prevents new artifacts from forming.

    Args:
        conn: Connection to the prices database.
        prices_table: Name of the prices table (e.g. ``Stock_Prices``).
        ticker: The ticker to reconcile.

    Returns:
        A status dict describing what was fixed.
    """
    # 1. Fetch the provider's authoritative split events
    try:
        provider, _df, events = _load_provider_history(ticker)
    except _PROVIDER_ERRORS as exc:
        return {
            "ticker": ticker, "status": "provider_fetch_failed",
            "error": str(exc),
        }
    if not events:
        return {"ticker": ticker, "status": "no_provider_events"}

    # 2. Collect the ticker's heuristic-detected split records
    try:
        heuristic_rows = conn.execute(
            "SELECT split_date, ratio_from, ratio_to FROM Stock_Splits "
            "WHERE ticker = ? AND detection_method = 'price_heuristic'",
            (ticker,),
        ).fetchall()
    except sqlite3.OperationalError:
        heuristic_rows = []

    if not heuristic_rows:
        return {"ticker": ticker, "status": "no_heuristic_splits"}

    # 3. For each mis-dated heuristic split, reconcile against the provider's
    #    true split events that happened after the boundary.
    total_restored = 0
    for h in heuristic_rows:
        h_date = h[0]

        # If a provider event is already dated at this exact date, the split was
        # correctly detected — leave it (and its read-time adjustment) alone.
        if any(e["split_date"] == h_date for e in events):
            continue

        # Mis-dated: the true split(s) happened after the boundary date and were
        # retro-applied by the provider.  Use them (any ratio) to restore.
        true_events = [e for e in events if e["split_date"] > h_date]
        if not true_events:
            continue  # no provider confirmation after the boundary → leave

        # Locate the price boundary at the mis-dated split date
        price_rows = conn.execute(
            f"SELECT Date, Price FROM {prices_table} "
            "WHERE Ticker = ? AND Date <= ? ORDER BY Date DESC LIMIT 2",
            (ticker, h_date),
        ).fetchall()
        if len(price_rows) < 2:
            continue
        boundary_price = price_rows[0][1]
        prev_price = price_rows[1][1]
        if not boundary_price or not prev_price or prev_price <= 0:
            continue

        # Discontinuity guard: only reconcile a heuristic split that actually
        # sits on a >40% price move.  Spurious heuristic records (e.g. from an
        # earlier scan of already-continuous data) must not trigger a restore —
        # doing so would wrongly modify genuine historical prices.
        raw_ratio = boundary_price / prev_price
        if _BASIS_CHANGE_LOW <= raw_ratio <= _BASIS_CHANGE_HIGH:
            continue

        # Continuity guard: restoring the boundary row to raw should line up
        # with the (raw) price before the boundary.  If it doesn't, the data
        # isn't a simple mixed-basis artifact — skip it.
        restore_at_boundary = _split_restore_factor(true_events, h_date)
        if abs(prev_price - boundary_price * restore_at_boundary) / prev_price > 0.35:
            logger.warning(
                "Skipping reconciliation for %s %s: continuity check failed "
                "(%.4f vs expected %.4f).",
                ticker, h_date, boundary_price * restore_at_boundary, prev_price,
            )
            continue

        # Restore the fetched (provider-adjusted) rows to raw, from the
        # boundary onward, using the true splits.
        n = _restore_stored_rows_to_raw(
            conn, prices_table, ticker, h_date, true_events,
        )
        total_restored += n
        _reconcile_splits(conn, ticker, true_events)
        refresh_split_adjusted_prices(conn, ticker=ticker, prices_table=prices_table)
        # Remove the mis-dated heuristic split itself (its ratio may not match
        # the provider's, so the ratio-based cleanup in _reconcile_splits can
        # not remove it).
        conn.execute(
            "DELETE FROM Stock_Splits WHERE ticker = ? "
            "AND split_date = ? AND detection_method = 'price_heuristic'",
            (ticker, h_date),
        )
        conn.commit()

    if total_restored:
        return {
            "ticker": ticker,
            "status": "reconciled",
            "restored_rows": total_restored,
            "provider_splits": len(events),
            "provider": provider,
        }
    return {
        "ticker": ticker,
        "status": "already_correct",
        "provider": provider,
    }


def _invalidate_split_cache_for(ticker: str) -> None:
    """Invalidate the in-process split-factor cache for one ticker."""
    try:
        from src.portfolio.portfolio_state import _invalidate_split_cache
        _invalidate_split_cache(ticker)
    except Exception:  # noqa: BLE001 - cache invalidation must not break writes
        logger.debug("Could not invalidate split cache", exc_info=True)


def load_ticker_data(ticker, prices_table, conn, currency: str = "JPY") -> bool:
    """Download and store historical price data for a single ticker.

    Fetches price data for the given ticker, starting from the last date
    already stored in ``prices_table``. Japanese tickers try JPX first, then
    Stooq and Yahoo Finance chart fallbacks; provider requests use bounded
    retries and cooldowns for transient failures and rate limits. If the data
    is already up to date (within 5 days), the function returns early.

    **Provider basis handling:** A boundary discontinuity is retained as a
    diagnostic with the source basis instead of being silently rewritten. A
    provider event records the authoritative date; a separate reconciliation
    job can repair legacy mixed-basis rows after review.

    Args:
        ticker (str): The company ticker symbol (e.g. ``'7203'``).
        prices_table (str): Name of the SQLite table where prices are stored.
        conn (sqlite3.Connection): Active database connection.

    Returns:
        bool: ``True`` if data was fetched successfully or was already
        up to date, ``False`` if the upstream provider request failed.
    """
    try:
        ensure_price_provenance_columns(conn, prices_table)
        last_date_query = f"SELECT MAX(Date) AS Last_Date FROM {prices_table} WHERE Ticker = ?"
        df_last_date = pd.read_sql_query(last_date_query, conn, params=(ticker,))
        start_date = None
        has_prior_data = False
        last_cached_price = None

        if df_last_date["Last_Date"][0] is not None:
            has_prior_data = True
            last_date = df_last_date["Last_Date"][0]
            today = pd.Timestamp.today().strftime("%Y-%m-%d")
            days_diff = (pd.to_datetime(today) - pd.to_datetime(last_date)).days
            if days_diff <= 5:
                logger.debug(f"Data for ticker {ticker} is already up to date.")
                return True
            start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            row = conn.execute(
                f"SELECT Price FROM {prices_table} "
                "WHERE Ticker = ? ORDER BY Date DESC LIMIT 1",
                (ticker,),
            ).fetchone()
            if row:
                last_cached_price = row[0]

        provider_name, out_data, split_events = _load_provider_history(
            ticker, start_date=start_date,
        )
        if out_data.empty:
            logger.warning("No data found for ticker %s after querying %s.", ticker, provider_name)
            return True

        # --- Detect a provider retro-adjustment at the fetch boundary ---
        # Detect a provider retro-adjustment at the fetch boundary (diagnostic
        # only — the restore below is what actually fixes it).
        if has_prior_data and last_cached_price and not out_data.empty:
            first_new = float(out_data.iloc[0]["Close"])
            if last_cached_price and last_cached_price > 0 and first_new > 0:
                ratio = first_new / last_cached_price
                if ratio < _BASIS_CHANGE_LOW or ratio > _BASIS_CHANGE_HIGH:
                    if split_events:
                        logger.info(
                            "Boundary discontinuity for %s (last cached %.4f vs "
                            "first fetched %.4f); preserving source basis and "
                            "recording %d provider split event(s).",
                            ticker, last_cached_price, first_new,
                            len(split_events),
                        )
                    else:
                        logger.warning(
                            "Boundary discontinuity for %s without provider "
                            "split events — appending as-fetched. Reconcile "
                            "manually if this is a real split.",
                            ticker,
                        )

        # JPX's historical table and Yahoo chart quote.close are already
        # split-adjusted.  Keep those values unchanged and mark their rows as
        # adjusted; the read model and consumers skip split factors for them.
        # Stooq rows remain ``unknown`` because its adjustment convention is
        # not guaranteed by the CSV API.
        if split_events:
            # Compatibility for callers/tests that inject the old three-tuple
            # provider contract without the new ``price_basis`` metadata.  A
            # real provider frame is always annotated by
            # ``_load_provider_history`` and never takes this branch.
            if "price_basis" not in out_data.attrs:
                out_data = _restore_fetched_frame(out_data, split_events)
                out_data.attrs["price_basis"] = "raw"
                logger.warning(
                    "Provider frame for %s lacked price-basis metadata; "
                    "restored it conservatively to raw basis", ticker,
                )
            provider_events = [
                {**event, "provider_symbol": out_data.attrs.get("provider_symbol", ticker)}
                for event in split_events
            ]
            _record_provider_splits(conn, ticker, provider_events)

        _append_price_rows(
            conn, prices_table, ticker, out_data, currency,
            provider=provider_name,
            price_basis=out_data.attrs.get("price_basis", "unknown"),
            provider_symbol=out_data.attrs.get("provider_symbol", ticker),
            source_revision=out_data.attrs.get("source_revision"),
            split_events=split_events,
        )
        refresh_split_adjusted_prices(conn, ticker=ticker, prices_table=prices_table)
        logger.info(
            "Successfully stored %s price records for ticker %s using %s",
            len(out_data),
            ticker,
            provider_name,
        )

        return True

    except Exception as e:
        logger.error(f"Failed to fetch data for ticker {ticker}: {e}", exc_info=True)
        return False
