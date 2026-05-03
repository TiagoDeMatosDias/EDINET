import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_db1
from src.orchestrator.common.edinet import Edinet, EDINET_BASE_URL

logger = logging.getLogger(__name__)


def run_get_documents(config, overwrite=False):
    logger.info("Getting all documents with metadata...")
    step_cfg = config.get("get_documents_config", {})

    edinet = Edinet(
        base_url=EDINET_BASE_URL,
        api_key=config.get("API_KEY"),
        db_path=get_db1(),
        doc_list_table="DocumentList",
    )
    edinet.get_All_documents_withMetadata(
        step_cfg.get("startDate"),
        step_cfg.get("endDate"),
    )


STEP_DEFINITION = StepDefinition(
    name="get_documents",
    handler=run_get_documents,
    required_keys=("API_KEY",),
    input_fields=(
        StepFieldDefinition("startDate", "str"),
        StepFieldDefinition("endDate", "str"),
    ),
)