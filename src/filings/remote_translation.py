"""Server-side translation: read the cache, send only what is missing to a worker.

The web server never loads the translation engine. ``server_translator`` answers
from the filings.db translation cache (read-only) and from the glossary, and runs
a ``translation`` job for the remaining texts; the worker translates them and
stores them in the cache. When the job does not finish in time the job service
raises ``JobPending`` (HTTP 202) and the client asks again once it is done.
"""

from __future__ import annotations

from typing import Any

from src.jobs.errors import JobFailed
from src.jobs.service import get_job_service

from .translate import TextTranslator, cached_translations, local_translation


def owner_of(request: Any) -> str | None:
    """The signed-in user's id for a request, if any."""
    user = getattr(getattr(request, "state", None), "user", None)
    return getattr(user, "user_id", None)


def server_translator(
    catalog: Any,
    *,
    force: bool = False,
    owner_user_id: str | None = None,
    title: str = "Translating filing text",
) -> TextTranslator:
    """A ``translate_texts`` callable that never runs the engine in this process."""

    def translate(texts: list[str]) -> dict[str, str]:
        unique = list(dict.fromkeys(text for text in texts if text and text.strip()))
        result: dict[str, str] = {}
        pending: list[str] = []
        for text in unique:
            english = local_translation(text)
            if english is None:
                pending.append(text)
            else:
                result[text] = english
        if pending and not force:
            cached = cached_translations(pending, catalog)
            result.update(cached)
            pending = [text for text in pending if text not in cached]
        if pending:
            record = get_job_service().run_sync(
                "translation",
                {"texts": pending, "force": force},
                owner_user_id=owner_user_id,
                title=title,
            )
            translations = (record.result or {}).get("translations")
            if not isinstance(translations, dict):
                raise JobFailed("The translation worker returned no translations.", record.job_id)
            result.update({str(source): str(english) for source, english in translations.items()})
        return result

    return translate


__all__ = ["owner_of", "server_translator"]
