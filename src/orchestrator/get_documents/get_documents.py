import logging

from src.orchestrator.common import StepDefinition
from src.orchestrator.common.edinet import Edinet

logger = logging.getLogger(__name__)


def run_get_documents(config, overwrite=False):
    logger.info("Getting all documents with metadata...")
    step_cfg = config.get("get_documents_config", {})
    target_database = step_cfg.get("Target_Database")

    edinet = Edinet(
        base_url=config.get("baseURL"),
        api_key=config.get("API_KEY"),
        db_path=target_database,
        doc_list_table=config.get("DB_DOC_LIST_TABLE"),
    )
    edinet.get_All_documents_withMetadata(
        step_cfg.get("startDate"),
        step_cfg.get("endDate"),
    )


STEP_DEFINITION = StepDefinition(
    name="get_documents",
    handler=run_get_documents,
    required_keys=("baseURL", "API_KEY"),
    required_config_fields=(("get_documents_config", "Target_Database"),),
)