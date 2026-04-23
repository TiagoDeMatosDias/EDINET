import logging

from src.orchestrator.common import StepDefinition
from src.orchestrator.common import backtesting

logger = logging.getLogger(__name__)


def run_backtest_set(config, overwrite=False):
    logger.info("Running backtest set...")
    step_cfg = config.get("backtest_set_config", {})

    return backtesting.run_backtest_set(
        step_cfg,
        db_path=step_cfg.get("Source_Database"),
        prices_table=config.get("DB_STOCK_PRICES_TABLE"),
        ratios_table=step_cfg.get("PerShare_Table") or "PerShare",
        company_table=config.get("DB_COMPANY_INFO_TABLE"),
        financial_statements_table=step_cfg.get("Financial_Statements_Table") or "FinancialStatements",
    )


STEP_DEFINITION = StepDefinition(
    name="backtest_set",
    handler=run_backtest_set,
    required_keys=("DB_STOCK_PRICES_TABLE", "DB_COMPANY_INFO_TABLE"),
    required_config_fields=(("backtest_set_config", "Source_Database"),),
)