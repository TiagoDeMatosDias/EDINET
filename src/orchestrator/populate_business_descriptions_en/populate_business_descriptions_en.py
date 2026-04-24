import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition

from . import service as description_services

logger = logging.getLogger(__name__)


def run_populate_business_descriptions_en(config, overwrite=False):
    logger.info("Populating English business descriptions...")
    step_cfg = config.get("populate_business_descriptions_en_config", {})

    return description_services.populate_business_descriptions_en(
        target_database=step_cfg.get("Target_Database"),
        providers_config=step_cfg.get(
            "Providers_Config",
            "config/reference/business_description_translation_providers.example.json",
        ),
        table_name=step_cfg.get("Table_Name", "FinancialStatements"),
        docid_column=step_cfg.get("DocID_Column", "docID"),
        source_column=step_cfg.get("Source_Column", "DescriptionOfBusiness"),
        target_column=step_cfg.get("Target_Column", "DescriptionOfBusiness_EN"),
        source_language=step_cfg.get("Source_Language", "ja"),
        target_language=step_cfg.get("Target_Language", "en"),
        overwrite=overwrite,
        batch_size=step_cfg.get("batch_size", 25),
    )


STEP_DEFINITION = StepDefinition(
    name="populate_business_descriptions_en",
    handler=run_populate_business_descriptions_en,
    aliases=("Populate Business Descriptions (EN)",),
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition("Target_Database", "database", required=True),
        StepFieldDefinition("Table_Name", "str", default="FinancialStatements"),
        StepFieldDefinition("DocID_Column", "str", default="docID"),
        StepFieldDefinition("Source_Column", "str", default="DescriptionOfBusiness"),
        StepFieldDefinition("Target_Column", "str", default="DescriptionOfBusiness_EN"),
        StepFieldDefinition(
            "Providers_Config",
            "file",
            default="config/reference/business_description_translation_providers.example.json",
        ),
        StepFieldDefinition("Source_Language", "str", default="ja"),
        StepFieldDefinition("Target_Language", "str", default="en"),
        StepFieldDefinition("batch_size", "num", default=25),
    ),
)