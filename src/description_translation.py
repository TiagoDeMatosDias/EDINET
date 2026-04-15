"""HTTP translation helpers for FinancialStatements business descriptions."""

from __future__ import annotations

import html
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)

_DEFAULT_CHUNK_CHAR_LIMIT = 700


class TranslationError(RuntimeError):
    """Raised when no translation provider can successfully translate text."""


class TranslationProviderUnavailableError(TranslationError):
    """Raised when a provider should be disabled for the rest of the run."""


class TranslationRateLimitError(TranslationProviderUnavailableError):
    """Raised when a provider signals quota exhaustion or rate limiting."""


@dataclass(frozen=True)
class TranslationProviderConfig:
    """Runtime configuration for a translation provider instance."""

    name: str
    provider_type: str
    settings: dict[str, Any]


def _clean_text_block(value: Any) -> str:
    """Return normalized plain text suitable for HTTP translation requests."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</(?:p|div|li|tr|td|th|section|article|h[1-6])\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = text.replace("\xa0", " ").replace("\u3000", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _join_units(units: list[str]) -> str:
    """Join text units while preserving compact Japanese punctuation layout."""
    combined = ""
    for unit in units:
        clean_unit = _clean_text_block(unit)
        if not clean_unit:
            continue
        if not combined:
            combined = clean_unit
            continue
        if re.match(r"[A-Za-z0-9]", clean_unit):
            combined += " "
        combined += clean_unit
    return combined.strip()


def _split_long_unit(unit: str, chunk_char_limit: int) -> list[str]:
    """Split a single overlong unit into smaller hard chunks."""
    clean_unit = _clean_text_block(unit)
    if not clean_unit:
        return []
    if len(clean_unit) <= chunk_char_limit:
        return [clean_unit]
    return [clean_unit[index:index + chunk_char_limit] for index in range(0, len(clean_unit), chunk_char_limit)]


def split_text_chunks(text: Any, chunk_char_limit: int = _DEFAULT_CHUNK_CHAR_LIMIT) -> list[list[str]]:
    """Split text into paragraph-aware translation chunks."""
    cleaned = _clean_text_block(text)
    if not cleaned:
        return []

    paragraph_chunks: list[list[str]] = []
    limit = max(int(chunk_char_limit or _DEFAULT_CHUNK_CHAR_LIMIT), 1)
    for paragraph in [block.strip() for block in cleaned.split("\n\n") if block.strip()]:
        fragments = [
            fragment.strip()
            for fragment in re.split(r"(?<=[。！？!?])|(?<=[.!?])\s+", paragraph)
            if fragment.strip()
        ]
        units = fragments or [paragraph]

        chunks: list[str] = []
        current_units: list[str] = []
        current_length = 0
        for unit in units:
            for normalized_unit in _split_long_unit(unit, limit):
                unit_length = len(normalized_unit)
                if current_units and current_length + unit_length > limit:
                    chunks.append(_join_units(current_units))
                    current_units = [normalized_unit]
                    current_length = unit_length
                else:
                    current_units.append(normalized_unit)
                    current_length += unit_length
        if current_units:
            chunks.append(_join_units(current_units))
        if chunks:
            paragraph_chunks.append(chunks)
    return paragraph_chunks


def _payload_message(payload: Any) -> str:
    """Extract the most useful human-readable message from a JSON payload."""
    if not isinstance(payload, dict):
        return ""
    for key in ("error", "message", "detail", "responseDetails"):
        value = payload.get(key)
        if value:
            return str(value)
    return ""


def _is_rate_limit_message(message: str) -> bool:
    """Return ``True`` for quota/rate-limit style provider messages."""
    lowered = str(message or "").lower()
    return any(token in lowered for token in ("rate limit", "too many requests", "quota", "usage limit"))


class TranslationProvider:
    """Base class for ordered-fallback HTTP translation providers."""

    def __init__(self, config: TranslationProviderConfig):
        self.config = config
        self.name = config.name
        timeout_value = config.settings.get("timeout_seconds", 30)
        try:
            self.timeout_seconds = max(float(timeout_value), 1.0)
        except (TypeError, ValueError):
            self.timeout_seconds = 30.0

    def _setting(self, key: str, default: str = "") -> str:
        value = self.config.settings.get(key, default)
        return str(value or "").strip()

    def _secret(self, key: str) -> str:
        direct = self._setting(key)
        if direct:
            return direct
        env_name = self._setting(f"{key}_env")
        return os.getenv(env_name, "").strip() if env_name else ""

    def _response_json(self, response: requests.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise TranslationProviderUnavailableError(f"{self.name} returned invalid JSON.") from exc

        if response.status_code == 429:
            raise TranslationRateLimitError(f"{self.name} returned HTTP 429.")
        if response.status_code >= 400:
            message = _payload_message(payload) or f"HTTP {response.status_code}"
            if _is_rate_limit_message(message):
                raise TranslationRateLimitError(message)
            raise TranslationProviderUnavailableError(message)
        return payload

    def translate(
        self,
        session: requests.Session,
        text: str,
        source_language: str,
        target_language: str,
    ) -> str:
        raise NotImplementedError


def _coerce_positive_int(value: Any, default: int) -> int:
    """Return a positive integer or the provided default."""
    try:
        return max(int(value or default), 1)
    except (TypeError, ValueError):
        return default


def _coerce_non_negative_float(value: Any, default: float) -> float:
    """Return a non-negative float or the provided default."""
    try:
        return max(float(value or default), 0.0)
    except (TypeError, ValueError):
        return default


class LibreTranslateProvider(TranslationProvider):
    """Provider for LibreTranslate-compatible endpoints."""

    def translate(
        self,
        session: requests.Session,
        text: str,
        source_language: str,
        target_language: str,
    ) -> str:
        base_url = self._setting("base_url").rstrip("/")
        endpoint = self._setting("endpoint") or (f"{base_url}/translate" if base_url else "")
        if not endpoint:
            raise TranslationProviderUnavailableError(f"{self.name} is missing endpoint/base_url.")

        payload = {
            "q": text,
            "source": source_language,
            "target": target_language,
            "format": "text",
        }
        api_key = self._secret("api_key")
        if api_key:
            payload["api_key"] = api_key

        headers = {"Accept": "application/json"}
        extra_headers = self.config.settings.get("headers")
        if isinstance(extra_headers, dict):
            headers.update({str(key): str(value) for key, value in extra_headers.items()})

        try:
            response = session.post(endpoint, data=payload, headers=headers, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            raise TranslationProviderUnavailableError(f"{self.name} request failed: {exc}") from exc

        payload_json = self._response_json(response)
        translated_text = _clean_text_block(payload_json.get("translatedText"))
        if translated_text:
            return translated_text
        message = _payload_message(payload_json) or f"{self.name} returned an empty translation."
        if _is_rate_limit_message(message):
            raise TranslationRateLimitError(message)
        raise TranslationError(message)


class MyMemoryProvider(TranslationProvider):
    """Provider for the free MyMemory translation API."""

    def translate(
        self,
        session: requests.Session,
        text: str,
        source_language: str,
        target_language: str,
    ) -> str:
        base_url = self._setting("base_url").rstrip("/")
        endpoint = self._setting("endpoint") or (f"{base_url}/get" if base_url else "")
        if not endpoint:
            raise TranslationProviderUnavailableError(f"{self.name} is missing endpoint/base_url.")

        params = {
            "q": text,
            "langpair": f"{source_language}|{target_language}",
        }
        email = self._secret("email")
        if email:
            params["de"] = email

        try:
            response = session.get(endpoint, params=params, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            raise TranslationProviderUnavailableError(f"{self.name} request failed: {exc}") from exc

        payload_json = self._response_json(response)
        status = payload_json.get("responseStatus")
        details = _payload_message(payload_json)
        if status not in (None, 200):
            if _is_rate_limit_message(details):
                raise TranslationRateLimitError(details)
            raise TranslationProviderUnavailableError(details or f"{self.name} returned status {status}.")

        translated_text = _clean_text_block(
            ((payload_json.get("responseData") or {}).get("translatedText"))
        )
        if translated_text:
            return translated_text
        if _is_rate_limit_message(details):
            raise TranslationRateLimitError(details)
        raise TranslationError(details or f"{self.name} returned an empty translation.")


PROVIDER_TYPES: dict[str, type[TranslationProvider]] = {
    "libretranslate": LibreTranslateProvider,
    "mymemory": MyMemoryProvider,
}


def load_translation_providers(config_path: str) -> tuple[list[TranslationProvider], dict[str, Any]]:
    """Load ordered translation providers and shared settings from JSON config."""
    with open(config_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    if not isinstance(raw, dict):
        raise ValueError("Translation provider config must be a JSON object.")

    raw_providers = raw.get("providers", []) or []
    if not isinstance(raw_providers, list):
        raise ValueError("Translation provider config 'providers' must be a list.")

    providers: list[TranslationProvider] = []
    for index, entry in enumerate(raw_providers):
        if not isinstance(entry, dict):
            continue
        if not entry.get("enabled", True):
            continue
        provider_type = str(entry.get("type") or "").strip().lower()
        provider_cls = PROVIDER_TYPES.get(provider_type)
        if provider_cls is None:
            raise ValueError(f"Unsupported translation provider type: {provider_type or '<missing>'}")
        provider_name = str(entry.get("name") or f"provider_{index + 1}").strip()
        providers.append(
            provider_cls(
                TranslationProviderConfig(
                    name=provider_name,
                    provider_type=provider_type,
                    settings=dict(entry),
                )
            )
        )

    if not providers:
        raise ValueError("No enabled translation providers configured.")

    settings = {
        "chunk_char_limit": _coerce_positive_int(raw.get("chunk_char_limit"), _DEFAULT_CHUNK_CHAR_LIMIT),
        "row_delay_seconds": _coerce_non_negative_float(raw.get("row_delay_seconds"), 0.0),
    }
    return providers, settings


def _retire_provider(providers: list[TranslationProvider], failed_provider: TranslationProvider) -> None:
    """Remove a failed provider from the active provider list when present."""
    try:
        providers.remove(failed_provider)
    except ValueError:
        pass


def translate_text_with_providers(
    text: Any,
    providers: list[TranslationProvider],
    *,
    source_language: str = "ja",
    target_language: str = "en",
    chunk_char_limit: int = _DEFAULT_CHUNK_CHAR_LIMIT,
    session: requests.Session | None = None,
    retire_failed_providers: bool = False,
    log_context: str | None = None,
    log_provider_activity: bool = False,
    slow_request_warning_seconds: float | None = 10.0,
) -> tuple[str, str]:
    """Translate text using ordered provider fallback and return text plus provider name.

    When ``retire_failed_providers`` is true, providers that fail with run-scoped
    availability errors are removed from *providers* in place so later calls do
    not retry the same dead endpoint again during the same run.
    """
    cleaned = _clean_text_block(text)
    if not cleaned:
        return "", ""
    if not providers:
        raise TranslationError("No translation providers are available.")

    owns_session = session is None
    active_session = session or requests.Session()
    errors: list[str] = []
    try:
        paragraph_chunks = split_text_chunks(cleaned, chunk_char_limit=chunk_char_limit)
        total_chunks = sum(len(paragraph) for paragraph in paragraph_chunks)
        context_suffix = f" for {log_context}" if log_context else ""
        for provider in list(providers):
            provider_started_at = time.perf_counter()
            if log_provider_activity:
                logger.info(
                    "Translation provider %s started%s with %d chunk(s) (timeout=%.1fs).",
                    provider.name,
                    context_suffix,
                    total_chunks,
                    provider.timeout_seconds,
                )
            try:
                translated_paragraphs: list[str] = []
                for paragraph in paragraph_chunks:
                    translated_chunks: list[str] = []
                    for chunk in paragraph:
                        translated_chunk = _clean_text_block(
                            provider.translate(
                                active_session,
                                chunk,
                                source_language,
                                target_language,
                            )
                        )
                        if not translated_chunk:
                            raise TranslationError(f"{provider.name} returned an empty chunk translation.")
                        translated_chunks.append(translated_chunk)
                    if translated_chunks:
                        translated_paragraphs.append(" ".join(translated_chunks).strip())

                elapsed_seconds = time.perf_counter() - provider_started_at
                translated_text = "\n\n".join(paragraph.strip() for paragraph in translated_paragraphs if paragraph.strip()).strip()
                if translated_text:
                    if slow_request_warning_seconds is not None and elapsed_seconds >= slow_request_warning_seconds:
                        logger.warning(
                            "Translation provider %s was slow%s: %.1fs for %d chunk(s).",
                            provider.name,
                            context_suffix,
                            elapsed_seconds,
                            total_chunks,
                        )
                    elif log_provider_activity:
                        logger.info(
                            "Translation provider %s completed%s in %.1fs across %d chunk(s).",
                            provider.name,
                            context_suffix,
                            elapsed_seconds,
                            total_chunks,
                        )
                    return translated_text, provider.name
                errors.append(f"{provider.name}: empty translation")
            except TranslationRateLimitError as exc:
                elapsed_seconds = time.perf_counter() - provider_started_at
                logger.warning(
                    "Translation provider %s rate-limited%s after %.1fs and disabled for the remainder of this run: %s",
                    provider.name,
                    context_suffix,
                    elapsed_seconds,
                    exc,
                )
                errors.append(f"{provider.name}: {exc}")
                if retire_failed_providers:
                    _retire_provider(providers, provider)
            except TranslationProviderUnavailableError as exc:
                elapsed_seconds = time.perf_counter() - provider_started_at
                logger.warning(
                    "Translation provider %s unavailable%s after %.1fs and disabled for the remainder of this run: %s",
                    provider.name,
                    context_suffix,
                    elapsed_seconds,
                    exc,
                )
                errors.append(f"{provider.name}: {exc}")
                if retire_failed_providers:
                    _retire_provider(providers, provider)
            except TranslationError as exc:
                elapsed_seconds = time.perf_counter() - provider_started_at
                logger.warning(
                    "Translation provider %s failed%s after %.1fs: %s",
                    provider.name,
                    context_suffix,
                    elapsed_seconds,
                    exc,
                )
                errors.append(f"{provider.name}: {exc}")
        if retire_failed_providers and not providers:
            raise TranslationError(
                "All translation providers are unavailable for the remainder of this run. "
                + "; ".join(errors)
            )
        raise TranslationError("All translation providers failed. " + "; ".join(errors))
    finally:
        if owns_session:
            active_session.close()