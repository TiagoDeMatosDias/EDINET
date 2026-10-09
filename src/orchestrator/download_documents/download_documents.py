import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_market_db
from src.orchestrator.common.edinet import EDINET_BASE_URL, Edinet
from src.paths import downloads_dir
from src.settings import edinet_api_key

logger = logging.getLogger(__name__)

_RAW_DOCUMENTS_PATH = str(downloads_dir())


def run_download_documents(config, overwrite=False, context=None):
    logger.info("Downloading documents...")
    step_cfg = config.get("download_documents_config", {})
    # Hardcoded table names
    doc_list_table = "DocumentList"
    financial_data_table = "financialData_full"

    edinet = Edinet(
        base_url=EDINET_BASE_URL,
        api_key=edinet_api_key(),
        db_path=get_market_db(),
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
    display_name="Download EDINET documents",
    required_settings=("edinet.api_key",),
    input_fields=(
        StepFieldDefinition(
            "docTypeCode",
            "str",
            default="120",
            label="Document type code",
            description="EDINET document type: 120=annual, 130=semi-annual, 140=quarterly, 170=extraordinary",
        ),
        StepFieldDefinition(
            "csvFlag",
            "str",
            default="1",
            label="CSV flag",
            description="1 = only documents with CSV data, 0 = all documents",
            choices=("1", "0"),
        ),
        StepFieldDefinition(
            "Downloaded",
            "str",
            default="False",
            label="Download status filter",
            description="False = only un-downloaded, True = re-download all",
            choices=("False", "True"),
        ),
    ),
)
