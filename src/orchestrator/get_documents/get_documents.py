import logging
import sqlite3
from datetime import date

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_market_db
from src.orchestrator.common.edinet import EDINET_BASE_URL, Edinet
from src.settings import edinet_api_key

logger = logging.getLogger(__name__)


def _latest_document_date(db_path: str) -> str | None:
    """Return the latest submitted-document calendar date from DocumentList."""
    connection = sqlite3.connect(db_path)
    try:
        row = connection.execute(
            "SELECT MAX(substr(submitDateTime, 1, 10)) FROM DocumentList"
        ).fetchone()
    except sqlite3.Error:
        logger.info("No existing DocumentList date available at %s", db_path)
        return None
    finally:
        connection.close()

    value = row[0] if row else None
    if not value:
        return None
    candidate = str(value)
    try:
        date.fromisoformat(candidate)
    except ValueError:
        logger.warning("Ignoring invalid latest DocumentList date %r", candidate)
        return None
    return candidate


def run_get_documents(config, overwrite=False, context=None):
    logger.info("Getting all documents with metadata...")
    step_cfg = config.get("get_documents_config", {})
    db_path = get_market_db()
    start_date = step_cfg.get("startDate") or _latest_document_date(db_path) or "2015-01-01"
    end_date = step_cfg.get("endDate") or date.today().isoformat()

    edinet = Edinet(
        base_url=EDINET_BASE_URL,
        api_key=edinet_api_key(),
        db_path=db_path,
        doc_list_table="DocumentList",
    )
    args = (start_date, end_date)
    if context is None:
        edinet.get_All_documents_withMetadata(*args)
    else:
        edinet.get_All_documents_withMetadata(*args, context=context)


STEP_DEFINITION = StepDefinition(
    name="get_documents",
    handler=run_get_documents,
    required_settings=("edinet.api_key",),
    input_fields=(
        StepFieldDefinition("startDate", "str"),
        StepFieldDefinition("endDate", "str"),
    ),
)
