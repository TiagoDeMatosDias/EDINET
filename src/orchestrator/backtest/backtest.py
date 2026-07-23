import logging

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common import backtesting
from src.orchestrator.common.db_config import get_db2

logger = logging.getLogger(__name__)


def run_backtest(config, overwrite=False, context=None):
    logger.info("Running backtesting...")
    step_cfg = config.get("backtesting_config", {})

    kwargs = dict(
        db_path=get_db2(),
        prices_table="Stock_Prices",
        ratios_table=step_cfg.get("PerShare_Table") or "ShareMetrics",
        company_table="CompanyInfo",
        financial_statements_table=step_cfg.get("Financial_Statements_Table") or "FinancialStatements",
    )
    if context is not None:
        kwargs["context"] = context
    return backtesting.run_backtest(step_cfg, **kwargs)


STEP_DEFINITION = StepDefinition(
    name="backtest",
    handler=run_backtest,
    config_key="backtesting_config",
    required_keys=(),
    input_fields=(
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
