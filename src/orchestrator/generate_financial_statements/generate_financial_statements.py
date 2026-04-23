import logging

from src.orchestrator.common import StepDefinition

from . import service as financial_statement_services

logger = logging.getLogger(__name__)


def run_generate_financial_statements(config, overwrite=False):
    logger.info("Generating financial statements...")
    step_cfg = config.get("generate_financial_statements_config", {})

    return financial_statement_services.generate_financial_statements(
        source_database=step_cfg.get("Source_Database"),
        source_table=step_cfg.get("Source_Table") or config.get("DB_FINANCIAL_DATA_TABLE"),
        target_database=step_cfg.get("Target_Database"),
        mappings_config=step_cfg.get(
            "Mappings_Config",
            "config/reference/canonical_metrics_config.json",
        ),
        company_table=step_cfg.get("Company_Info_Table") or config.get("DB_COMPANY_INFO_TABLE"),
        prices_table=step_cfg.get("Stock_Prices_Table") or config.get("DB_STOCK_PRICES_TABLE"),
        overwrite=overwrite,
        batch_size=step_cfg.get("batch_size", 2500),
        max_line_depth=step_cfg.get("max_line_depth", 3),
    )


STEP_DEFINITION = StepDefinition(
    name="generate_financial_statements",
    handler=run_generate_financial_statements,
    aliases=("Generate Financial Statements",),
    required_config_fields=(
        ("generate_financial_statements_config", "Source_Database"),
        ("generate_financial_statements_config", "Target_Database"),
    ),
)