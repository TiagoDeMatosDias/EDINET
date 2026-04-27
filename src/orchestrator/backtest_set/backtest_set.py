import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common import backtesting

logger = logging.getLogger(__name__)


def run_backtest_set(config, overwrite=False):
    logger.info("Running backtest set...")
    step_cfg = config.get("backtest_set_config", {})

    return backtesting.run_backtest_set(
        step_cfg,
        db_path=step_cfg.get("Source_Database"),
        prices_table="Stock_Prices",
        ratios_table=step_cfg.get("PerShare_Table") or "ShareMetrics",
        company_table="CompanyInfo",
        financial_statements_table=step_cfg.get("Financial_Statements_Table") or "FinancialStatements",
    )


STEP_DEFINITION = StepDefinition(
    name="backtest_set",
    handler=run_backtest_set,
    display_name="Backtest Set (CSV)",
    required_keys=(),
    input_fields=(
        StepFieldDefinition("Source_Database", "database", required=True),
        StepFieldDefinition("PerShare_Table", "str", default="ShareMetrics"),
        StepFieldDefinition(
            "Financial_Statements_Table",
            "str",
            default="FinancialStatements",
        ),
        StepFieldDefinition(
            "csv_file",
            "file",
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*")),
            required=True,
        ),
        StepFieldDefinition("benchmark_ticker", "str"),
        StepFieldDefinition("output_dir", "str", default="data/backtest_set_results"),
        StepFieldDefinition("risk_free_rate", "num", default=0.0),
        StepFieldDefinition("initial_capital", "num", default=0.0),
    ),
)