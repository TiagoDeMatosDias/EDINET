import logging
import os

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_db1, _find_project_root
from src.orchestrator.common.edinet import Edinet, EDINET_BASE_URL

logger = logging.getLogger(__name__)

# Hardcoded raw documents path (was in .env)
_RAW_DOCUMENTS_PATH = os.path.join(_find_project_root(), "data", "raw_documents")


def run_download_documents(config, overwrite=False, context=None):
    logger.info("Downloading documents...")
    step_cfg = config.get("download_documents_config", {})
    # Hardcoded table names (moved out of .env)
    doc_list_table = "DocumentList"
    financial_data_table = "financialData_full"

    edinet = Edinet(
        base_url=EDINET_BASE_URL,
        api_key=config.get("API_KEY"),
        db_path=get_db1(),
        raw_docs_path=_RAW_DOCUMENTS_PATH,
        doc_list_table=doc_list_table,
    )

    filters = edinet.generate_filter("docTypeCode", "=", step_cfg.get("docTypeCode"))
    filters = edinet.generate_filter("csvFlag", "=", step_cfg.get("csvFlag"), filters)
    filters = edinet.generate_filter("Downloaded", "=", step_cfg.get("Downloaded"), filters)

    args = (doc_list_table, financial_data_table, filters)
    if context is None:
        edinet.downloadDocs(*args)
    else:
        edinet.downloadDocs(*args, context=context)


STEP_DEFINITION = StepDefinition(
    name="download_documents",
    handler=run_download_documents,
    required_keys=("API_KEY",),
    input_fields=(
        StepFieldDefinition("docTypeCode", "str", default="120"),
        StepFieldDefinition("csvFlag", "str", default="1"),
        StepFieldDefinition("Downloaded", "str", default="False"),
    ),
)
