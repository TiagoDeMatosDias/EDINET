import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.edinet import Edinet

logger = logging.getLogger(__name__)


def run_download_documents(config, overwrite=False):
    logger.info("Downloading documents...")
    step_cfg = config.get("download_documents_config", {})
    target_database = step_cfg.get("Target_Database")
    # Hardcoded table names (moved out of .env)
    doc_list_table = "DocumentList"
    financial_data_table = "financialData_full"

    edinet = Edinet(
        base_url=config.get("baseURL"),
        api_key=config.get("API_KEY"),
        db_path=target_database,
        raw_docs_path=config.get("RAW_DOCUMENTS_PATH"),
        doc_list_table=doc_list_table,
    )

    filters = edinet.generate_filter("docTypeCode", "=", step_cfg.get("docTypeCode"))
    filters = edinet.generate_filter("csvFlag", "=", step_cfg.get("csvFlag"), filters)
    filters = edinet.generate_filter("Downloaded", "=", step_cfg.get("Downloaded"), filters)

    edinet.downloadDocs(doc_list_table, financial_data_table, filters)


STEP_DEFINITION = StepDefinition(
    name="download_documents",
    handler=run_download_documents,
    required_keys=(
        "RAW_DOCUMENTS_PATH",
        "baseURL",
        "API_KEY",
    ),
    input_fields=(
        StepFieldDefinition("docTypeCode", "str", default="120"),
        StepFieldDefinition("csvFlag", "str", default="1"),
        StepFieldDefinition("Downloaded", "str", default="False"),
        StepFieldDefinition("Target_Database", "database", required=True),
    ),
)