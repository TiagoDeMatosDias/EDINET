import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common import backtesting

logger = logging.getLogger(__name__)


def run_backtest(config, overwrite=False):
    logger.info("Running backtesting...")
    step_cfg = config.get("backtesting_config", {})

    return backtesting.run_backtest(
        step_cfg,
        db_path=step_cfg.get("Source_Database"),
        prices_table=config.get("DB_STOCK_PRICES_TABLE"),
        ratios_table=step_cfg.get("PerShare_Table") or "ShareMetrics",
        company_table=config.get("DB_COMPANY_INFO_TABLE"),
        financial_statements_table=step_cfg.get("Financial_Statements_Table") or "FinancialStatements",
    )


STEP_DEFINITION = StepDefinition(
    name="backtest",
    handler=run_backtest,
    config_key="backtesting_config",
    required_keys=("DB_STOCK_PRICES_TABLE", "DB_COMPANY_INFO_TABLE"),
    input_fields=(
        StepFieldDefinition("Source_Database", "database", required=True),
        StepFieldDefinition("PerShare_Table", "str", default="ShareMetrics"),
        StepFieldDefinition(
            "Financial_Statements_Table",
            "str",
            default="FinancialStatements",
        ),
        StepFieldDefinition("start_date", "str", default="2023-01-01"),
        StepFieldDefinition("end_date", "str", default="2025-12-31"),
        StepFieldDefinition("benchmark_ticker", "str"),
        StepFieldDefinition(
            "output_file",
            "str",
            default="data/backtest_results/backtest_report.txt",
        ),
        StepFieldDefinition("risk_free_rate", "num", default=0.0),
        StepFieldDefinition("portfolio", "portfolio", default={}, required=True),
    ),
)