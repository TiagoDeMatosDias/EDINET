"""Pipeline step for retaining and indexing EDINET type-1 XBRL packages."""

from __future__ import annotations

import logging

from src.filings.acquisition import EdinetDownloadClient
from src.filings.runtime import ARCHIVE_ROOT, catalog
from src.orchestrator.common import StepDefinition, StepFieldDefinition

logger = logging.getLogger(__name__)
_MAX_DOCUMENTS_PER_RUN = 100


def _document_ids(step_cfg: dict) -> list[str]:
    raw = step_cfg.get("document_ids", step_cfg.get("docIDs", ""))
    if isinstance(raw, str):
        values = raw.split(",")
    elif isinstance(raw, list):
        values = raw
    else:
        values = []
    return [str(value).strip() for value in values if str(value).strip()][: _MAX_DOCUMENTS_PER_RUN]


def run_download_xbrl(config, overwrite=False, context=None):
    """Download configured document IDs with bounded progress and storage."""
    del overwrite
    step_cfg = config.get("download_xbrl_config", {})
    document_ids = _document_ids(step_cfg)
    if not document_ids:
        raise ValueError("download_xbrl requires one or more document_ids")
    provider_token = step_cfg.get("provider_token")
    client = (
        EdinetDownloadClient(provider_token)
        if provider_token
        else EdinetDownloadClient.from_environment()
    )
    for index, doc_id in enumerate(document_ids):
        if context is not None:
            context.report_progress(index, len(document_ids), f"Downloading XBRL filing {doc_id}")
        client.acquire_type1(
            doc_id,
            ARCHIVE_ROOT,
            catalog,
            {"doc_type_code": "1", "xbrl_flag": "1"},
        )
    if context is not None:
        context.report_progress(len(document_ids), len(document_ids), "XBRL filing archive complete")
    logger.info("Indexed %d EDINET type-1 packages", len(document_ids))
    return {"document_count": len(document_ids)}


STEP_DEFINITION = StepDefinition(
    name="download_xbrl",
    handler=run_download_xbrl,
    input_fields=(
        StepFieldDefinition(
            "document_ids",
            "str",
            default="",
            label="EDINET document IDs (comma-separated)",
            required=True,
        ),
        StepFieldDefinition(
            "provider_token",
            "str",
            default="",
            label="EDINET provider token (optional; uses EDINET_API_TOKEN when omitted)",
        ),
    ),
)
