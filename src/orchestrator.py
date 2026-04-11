import logging
import sqlite3
import threading
from typing import Callable

import src.edinet_api as edinet_api
from config import Config
import src.data_processing as d
import src.stockprice_api as stockprice_api
import src.regression_analysis as regression
import src.backtesting as backtesting

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step handlers — one per orchestration step.
#
# Each handler receives the full config and an overwrite flag, extracts
# the parameters it needs, and calls the appropriate module function(s)
# with explicit arguments.  No shared mutable state is carried between
# steps; every resource (DB connection, Edinet instance, data instance)
# is created locally.
# ---------------------------------------------------------------------------

def _step_get_documents(config, overwrite=False):
    logger.info("Getting all documents with metadata...")
    step_cfg = config.get("get_documents_config", {})
    target_database = step_cfg.get("Target_Database")

    edinet = edinet_api.Edinet(
        base_url=config.get("baseURL"),
        api_key=config.get("API_KEY"),
        db_path=target_database,
        doc_list_table=config.get("DB_DOC_LIST_TABLE"),
    )
    edinet.get_All_documents_withMetadata(
        step_cfg.get("startDate"),
        step_cfg.get("endDate"),
    )


def _step_download_documents(config, overwrite=False):
    logger.info("Downloading documents...")
    step_cfg = config.get("download_documents_config", {})
    target_database = step_cfg.get("Target_Database")
    doc_list_table = config.get("DB_DOC_LIST_TABLE")
    financial_data_table = config.get("DB_FINANCIAL_DATA_TABLE")

    edinet = edinet_api.Edinet(
        base_url=config.get("baseURL"),
        api_key=config.get("API_KEY"),
        db_path=target_database,
        raw_docs_path=config.get("RAW_DOCUMENTS_PATH"),
        doc_list_table=doc_list_table,
    )

    filters = edinet.generate_filter("docTypeCode", "=", step_cfg.get("docTypeCode"))
    filters = edinet.generate_filter("csvFlag", "=", step_cfg.get("csvFlag"), filters)
    filters = edinet.generate_filter("Downloaded", "=", step_cfg.get("Downloaded"), filters)

    edinet.downloadDocs(doc_list_table, financial_data_table, filters)


def _step_populate_company_info(config, overwrite=False):
    logger.info("Populating company info table...")
    step_cfg = config.get("populate_company_info_config", {})
    target_database = step_cfg.get("Target_Database")

    edinet = edinet_api.Edinet(
        base_url=config.get("baseURL", ""),
        api_key=config.get("API_KEY", ""),
        db_path=target_database,
        company_info_table=config.get("DB_COMPANY_INFO_TABLE"),
    )
    edinet.store_edinetCodes(step_cfg.get("csv_file"), target_database=target_database)


def _step_generate_financial_statements(config, overwrite=False):
    logger.info("Generating financial statements...")
    step_cfg = config.get("generate_financial_statements_config", {})

    processor = d.data()
    processor.generate_financial_statements(
        source_database=step_cfg.get("Source_Database"),
        source_table=step_cfg.get("Source_Table") or config.get("DB_FINANCIAL_DATA_TABLE"),
        target_database=step_cfg.get("Target_Database"),
        mappings_config=step_cfg.get(
            "Mappings_Config",
            "config/reference/financial_statements_mappings_config.json",
        ),
        company_table=step_cfg.get("Company_Info_Table") or config.get("DB_COMPANY_INFO_TABLE"),
        prices_table=step_cfg.get("Stock_Prices_Table") or config.get("DB_STOCK_PRICES_TABLE"),
        overwrite=overwrite,
        batch_size=step_cfg.get("batch_size", 2500),
    )


def _step_populate_business_descriptions_en(config, overwrite=False):
    logger.info("Populating English business descriptions...")
    step_cfg = config.get("populate_business_descriptions_en_config", {})

    processor = d.data()
    processor.populate_business_descriptions_en(
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


def _step_generate_ratios(config, overwrite=False):
    logger.info("Generating ratios tables (PerShare / Valuation / Quality)...")
    step_cfg = config.get("generate_ratios_config", {})

    processor = d.data()
    processor.generate_ratios(
        source_database=step_cfg.get("Source_Database"),
        target_database=step_cfg.get("Target_Database"),
        formulas_config=step_cfg.get(
            "Formulas_Config",
            "config/reference/generate_ratios_formulas_config.json",
        ),
        overwrite=overwrite,
        batch_size=step_cfg.get("batch_size", 5000),
    )


def _step_generate_historical_ratios(config, overwrite=False):
    logger.info("Generating historical ratios tables (Pershare_Historical / Quality_Historical / Valuation_Historical)...")
    step_cfg = config.get("generate_historical_ratios_config", {})

    processor = d.data()
    processor.generate_historical_ratios(
        source_database=step_cfg.get("Source_Database"),
        target_database=step_cfg.get("Target_Database"),
        overwrite=overwrite,
        company_batch_size=step_cfg.get("company_batch_size", 200),
    )


def _step_import_stock_prices_csv(config, overwrite=False):
    logger.info("Importing stock prices from CSV...")
    step_cfg = config.get("import_stock_prices_csv_config", {})

    stockprice_api.import_stock_prices_csv(
        db_name=step_cfg.get("Target_Database"),
        prices_table=config.get("DB_STOCK_PRICES_TABLE"),
        csv_path=step_cfg.get("csv_file", ""),
        default_ticker=step_cfg.get("default_ticker", step_cfg.get("ticker", "")),
        default_currency=step_cfg.get("default_currency", step_cfg.get("currency", "JPY")),
        date_column=step_cfg.get("date_column", "Date"),
        price_column=step_cfg.get("price_column", "Close"),
        ticker_column=step_cfg.get("ticker_column", ""),
        currency_column=step_cfg.get("currency_column", ""),
    )


def _step_update_stock_prices(config, overwrite=False):
    logger.info("Updating stock prices...")
    step_cfg = config.get("update_stock_prices_config", {})

    stockprice_api.update_all_stock_prices(
        db_name=step_cfg.get("Target_Database"),
        Company_Table=config.get("DB_COMPANY_INFO_TABLE"),
        prices_table=config.get("DB_STOCK_PRICES_TABLE"),
        standardized_table=config.get("DB_FINANCIAL_DATA_TABLE"),
    )


def _step_parse_taxonomy(config, overwrite=False):
    logger.info("Parsing EDINET taxonomy...")
    step_cfg = config.get("parse_taxonomy_config", {})
    target_database = step_cfg.get("Target_Database")
    taxonomy_table = config.get("DB_TAXONOMY_TABLE")

    conn = sqlite3.connect(target_database)
    try:
        processor = d.data()
        processor.parse_edinet_taxonomy(
            step_cfg.get("xsd_file"),
            taxonomy_table,
            connection=conn,
        )
    finally:
        conn.close()


def _step_multivariate_regression(config, overwrite=False):
    logger.info("Running multivariate regression...")
    step_cfg = config.get("Multivariate_Regression_config", {})

    regression.multivariate_regression(
        step_cfg,
        step_cfg.get("Source_Database"),
        company_table=config.get("DB_COMPANY_INFO_TABLE"),
    )


def _step_backtest(config, overwrite=False):
    logger.info("Running backtesting...")
    step_cfg = config.get("backtesting_config", {})

    backtesting.run_backtest(
        step_cfg,
        db_path=step_cfg.get("Source_Database"),
        prices_table=config.get("DB_STOCK_PRICES_TABLE"),
        ratios_table=step_cfg.get("PerShare_Table") or "PerShare",
        company_table=config.get("DB_COMPANY_INFO_TABLE"),
        financial_statements_table=step_cfg.get("Financial_Statements_Table") or "FinancialStatements",
    )


def _step_backtest_set(config, overwrite=False):
    logger.info("Running backtest set...")
    step_cfg = config.get("backtest_set_config", {})

    backtesting.run_backtest_set(
        step_cfg,
        db_path=step_cfg.get("Source_Database"),
        prices_table=config.get("DB_STOCK_PRICES_TABLE"),
        ratios_table=step_cfg.get("PerShare_Table") or "PerShare",
        company_table=config.get("DB_COMPANY_INFO_TABLE"),
        financial_statements_table=step_cfg.get("Financial_Statements_Table") or "FinancialStatements",
    )


# ---------------------------------------------------------------------------
# Step registry — maps step names to handler functions.
# ---------------------------------------------------------------------------

STEP_HANDLERS: dict[str, Callable] = {
    "get_documents": _step_get_documents,
    "download_documents": _step_download_documents,
    "populate_company_info": _step_populate_company_info,
    "generate_financial_statements": _step_generate_financial_statements,
    "Generate Financial Statements": _step_generate_financial_statements,
    "populate_business_descriptions_en": _step_populate_business_descriptions_en,
    "Populate Business Descriptions (EN)": _step_populate_business_descriptions_en,
    "generate_ratios": _step_generate_ratios,
    "Generate Ratios": _step_generate_ratios,
    "generate_historical_ratios": _step_generate_historical_ratios,
    "Generate Historical Ratios": _step_generate_historical_ratios,
    "import_stock_prices_csv": _step_import_stock_prices_csv,
    "update_stock_prices": _step_update_stock_prices,
    "parse_taxonomy": _step_parse_taxonomy,
    "Multivariate_Regression": _step_multivariate_regression,
    "backtest": _step_backtest,
    "backtest_set": _step_backtest_set,
}


# ---------------------------------------------------------------------------
# Pre-flight validation
# ---------------------------------------------------------------------------

# Map each step to the top-level config / .env keys it requires.
STEP_REQUIRED_KEYS: dict[str, list[str]] = {
    "get_documents":              ["baseURL", "API_KEY"],
    "download_documents":         ["DB_DOC_LIST_TABLE", "DB_FINANCIAL_DATA_TABLE",
                                   "RAW_DOCUMENTS_PATH", "baseURL", "API_KEY"],
    "populate_company_info":      ["DB_COMPANY_INFO_TABLE"],
    "import_stock_prices_csv":    ["DB_STOCK_PRICES_TABLE"],
    "update_stock_prices":        ["DB_COMPANY_INFO_TABLE",
                                   "DB_STOCK_PRICES_TABLE", "DB_FINANCIAL_DATA_TABLE"],
    "parse_taxonomy":             ["DB_TAXONOMY_TABLE"],
    "generate_financial_statements": [],
    "populate_business_descriptions_en": [],
    "generate_ratios":            [],
    "generate_historical_ratios": [],
    "Multivariate_Regression":    [],
    "backtest":                   ["DB_STOCK_PRICES_TABLE",
                                   "DB_COMPANY_INFO_TABLE"],
    "backtest_set":               ["DB_STOCK_PRICES_TABLE",
                                   "DB_COMPANY_INFO_TABLE"],
}

# Map each step to (config_section, field) pairs that must be non-empty.
STEP_REQUIRED_CONFIG_FIELDS: dict[str, list[tuple[str, str]]] = {
    "get_documents": [("get_documents_config", "Target_Database")],
    "download_documents": [("download_documents_config", "Target_Database")],
    "populate_company_info": [("populate_company_info_config", "Target_Database")],
    "import_stock_prices_csv": [("import_stock_prices_csv_config", "Target_Database")],
    "update_stock_prices": [("update_stock_prices_config", "Target_Database")],
    "parse_taxonomy": [("parse_taxonomy_config", "Target_Database")],
    "generate_financial_statements": [
        ("generate_financial_statements_config", "Source_Database"),
        ("generate_financial_statements_config", "Target_Database"),
    ],
    "populate_business_descriptions_en": [
        ("populate_business_descriptions_en_config", "Target_Database"),
        ("populate_business_descriptions_en_config", "Providers_Config"),
    ],
    "generate_ratios": [
        ("generate_ratios_config", "Source_Database"),
        ("generate_ratios_config", "Target_Database"),
    ],
    "generate_historical_ratios": [
        ("generate_historical_ratios_config", "Source_Database"),
        ("generate_historical_ratios_config", "Target_Database"),
    ],
    "Multivariate_Regression": [("Multivariate_Regression_config", "Source_Database")],
    "backtest": [("backtesting_config", "Source_Database")],
    "backtest_set": [("backtest_set_config", "Source_Database")],
}


def validate_config(config, enabled_steps: list[str]) -> None:
    """Validate that all required settings exist for the enabled steps.

    Raises ``RuntimeError`` with a detailed message when settings are missing.
    """
    missing_map: dict[str, list[str]] = {}

    for step_name in enabled_steps:
        for key in STEP_REQUIRED_KEYS.get(step_name, []):
            if not config.get(key):
                missing_map.setdefault(key, []).append(step_name)

        for cfg_name, field_name in STEP_REQUIRED_CONFIG_FIELDS.get(step_name, []):
            cfg = config.get(cfg_name, {}) or {}
            if not cfg.get(field_name):
                missing_key = f"{cfg_name}.{field_name}"
                missing_map.setdefault(missing_key, []).append(step_name)

    if missing_map:
        lines = ["The following required settings are missing from .env / config:"]
        for key, steps_needing in sorted(missing_map.items()):
            lines.append(f"  • {key}  (needed by: {', '.join(steps_needing)})")
        lines.append("")
        lines.append("Set them in the step configuration dialogs or add them to the config / .env files.")
        msg = "\n".join(lines)
        logger.error(msg)
        raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def execute_step(step_name: str, config, overwrite: bool = False) -> None:
    """Execute a single orchestration step by name.

    Args:
        step_name: Name of the step to execute (must be in STEP_HANDLERS).
        config: Configuration object (Config or dict-like with ``.get()``).
        overwrite: Whether to overwrite existing data for this step.
    """
    handler = STEP_HANDLERS.get(step_name)
    if handler is None:
        logger.warning(f"Unknown step: {step_name}")
        return
    handler(config, overwrite=overwrite)


def run() -> None:
    """Orchestrate execution based on the run config file.

    Reads the ``run_steps`` config, validates required settings for
    enabled steps, then executes them in order.
    """
    logger.info("Starting Program")
    logger.info("Loading Config")

    config = Config()
    run_steps = config.get("run_steps", {})

    # Determine which steps are enabled.
    enabled_steps = []
    for step_name, step_val in run_steps.items():
        if isinstance(step_val, dict):
            if step_val.get("enabled", False):
                enabled_steps.append(step_name)
        elif bool(step_val):
            enabled_steps.append(step_name)

    validate_config(config, enabled_steps)

    # Execute steps in order as defined in run_steps.
    logger.info(f"Steps to execute (in order): {list(run_steps.keys())}")
    for step_name, step_val in run_steps.items():
        if isinstance(step_val, dict):
            is_enabled = step_val.get("enabled", False)
            overwrite = step_val.get("overwrite", False)
        else:
            is_enabled = bool(step_val)
            overwrite = False

        if is_enabled:
            try:
                execute_step(step_name, config, overwrite=overwrite)
            except Exception as e:
                logger.error(f"Error executing step '{step_name}': {e}", exc_info=True)
        else:
            logger.debug(f"Step '{step_name}' is disabled, skipping.")

    logger.info("Program Ended")


def run_pipeline(
    steps: list[dict],
    config: Config,
    on_step_start: Callable[[str], None] | None = None,
    on_step_done: Callable[[str], None] | None = None,
    on_step_error: Callable[[str, Exception], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> None:
    """Execute a list of steps in order with per-step callbacks and cancellation.

    Args:
        steps: List of dicts with keys ``name`` and optionally ``overwrite``.
        config: Configuration object.
        on_step_start: Called with the step name before execution begins.
        on_step_done: Called with the step name after successful execution.
        on_step_error: Called with the step name and exception on failure.
        cancel_event: If set, the pipeline stops before the next step.
    """
    enabled_steps = [step.get("name") for step in steps if step.get("name")]
    validate_config(config, enabled_steps)

    for step in steps:
        if cancel_event and cancel_event.is_set():
            logger.info("Pipeline cancelled by user.")
            return

        name = step["name"]
        overwrite = step.get("overwrite", False)

        if on_step_start:
            on_step_start(name)
        try:
            execute_step(name, config, overwrite=overwrite)
            if on_step_done:
                on_step_done(name)
        except Exception as e:
            logger.error(f"Error executing step '{name}': {e}", exc_info=True)
            if on_step_error:
                on_step_error(name, e)
            raise