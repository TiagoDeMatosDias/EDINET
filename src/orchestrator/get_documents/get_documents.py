import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.edinet import Edinet

logger = logging.getLogger(__name__)


def run_get_documents(config, overwrite=False):
    logger.info("Getting all documents with metadata...")
    step_cfg = config.get("get_documents_config", {})
    raw_target = step_cfg.get("Target_Database")
    target_database = config.resolve_db_path(raw_target) if hasattr(config, 'resolve_db_path') else raw_target

    edinet = Edinet(
        base_url=config.get("baseURL"),
        api_key=config.get("API_KEY"),
        db_path=target_database,
        doc_list_table="DocumentList",
    )
    edinet.get_All_documents_withMetadata(
        step_cfg.get("startDate"),
        step_cfg.get("endDate"),
    )


STEP_DEFINITION = StepDefinition(
    name="get_documents",
    handler=run_get_documents,
    required_keys=("baseURL", "API_KEY"),
    input_fields=(
        StepFieldDefinition("startDate", "str"),
        StepFieldDefinition("endDate", "str"),
        StepFieldDefinition("Target_Database", "database", required=True),
    ),
)