"""Translation job: Japanese filing text to English on a worker.

The server reads the translation cache itself and sends only the texts it is
missing; this handler translates them with the local engine and stores them in
the filings.db cache, so later requests are answered without a job.
"""

from __future__ import annotations

from src.worker.context import JobContext, JobError

_CHUNK_SIZE = 25


def run(context: JobContext) -> dict:
    """Translate ``payload["texts"]`` and return ``{"translations": {source: english}}``."""
    from src.filings.catalog import FilingCatalog
    from src.filings.translate import TranslationError, translate_batch
    from src.orchestrator.common.db_config import get_filings_db

    payload = context.payload
    texts = [str(text) for text in payload.get("texts") or [] if str(text).strip()]
    force = bool(payload.get("force"))
    catalog = FilingCatalog(get_filings_db())

    translations: dict[str, str] = {}
    total = len(texts)
    try:
        for start in range(0, total, _CHUNK_SIZE):
            context.checkpoint()
            chunk = texts[start:start + _CHUNK_SIZE]
            translations.update(translate_batch(chunk, catalog, force=force))
            done = min(start + _CHUNK_SIZE, total)
            context.report_progress(100.0 * done / total, f"Translated {done} of {total} texts")
    except TranslationError as exc:
        raise JobError(str(exc), retryable=False) from exc
    return {"translations": translations}
