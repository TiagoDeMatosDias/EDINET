import logging
import json
import src.edinet_api as edinet_api
from config import Config
import src.data_processing as d
import src.stockprice_api as y
import src.regression_analysis as r
import src.backtesting as bt

logger = logging.getLogger(__name__)


def _execute_step(step_name, config, edinet, data, overwrite=False):
    """
    Execute a single orchestration step.
    
    Args:
        step_name: Name of the step to execute
        config: Configuration object
        edinet: Edinet API instance
        data: Data processing instance
        overwrite: Whether to overwrite existing data for this step
    """
    # Database table names
    DB_DOC_LIST_TABLE = config.get("DB_DOC_LIST_TABLE")
    DB_FINANCIAL_DATA_TABLE = config.get("DB_FINANCIAL_DATA_TABLE")
    DB_STANDARDIZED_TABLE = config.get("DB_STANDARDIZED_TABLE")
    DB_STANDARDIZED_RATIOS_TABLE = config.get("DB_STANDARDIZED_RATIOS_TABLE")
    DB_PATH = config.get("DB_PATH")
    DB_COMPANY_INFO_TABLE = config.get("DB_COMPANY_INFO_TABLE")
    DB_STOCK_PRICES_TABLE = config.get("DB_STOCK_PRICES_TABLE")
    DB_SIGNIFICANT_PREDICTORS_TABLE = config.get("DB_SIGNIFICANT_PREDICTORS_TABLE")
    DB_TAXONOMY_TABLE = config.get("DB_TAXONOMY_TABLE")

    if step_name == "get_documents":
        logger.info("Getting all documents with metadata...")
        get_documents_config = config.get("get_documents_config", {})
        startDate = get_documents_config.get("startDate")
        endDate = get_documents_config.get("endDate")
        edinet.get_All_documents_withMetadata(startDate, endDate)

    elif step_name == "download_documents":
        logger.info("Downloading documents...")
        download_documents_config = config.get("download_documents_config", {})
        docTypeCode = download_documents_config.get("docTypeCode")
        csvFlag = download_documents_config.get("csvFlag")
        secCode = download_documents_config.get("secCode")
        downloaded_flag = download_documents_config.get("Downloaded")

        filters = edinet.generate_filter("docTypeCode", "=", docTypeCode)
        filters = edinet.generate_filter("csvFlag", "=", csvFlag, filters)
        filters = edinet.generate_filter("secCode", "!=", secCode, filters)
        filters = edinet.generate_filter("Downloaded", "=", downloaded_flag, filters)
        edinet.downloadDocs(DB_DOC_LIST_TABLE, DB_FINANCIAL_DATA_TABLE, filters)

    elif step_name == "standardize_data":
        logger.info("Standardizing data...")
        data.copy_table_to_Standard(DB_FINANCIAL_DATA_TABLE, DB_STANDARDIZED_TABLE, overwrite=overwrite)

    elif step_name == "populate_company_info":
        logger.info("Populating company info table...")
        populate_config = config.get("populate_company_info_config", {})
        csv_file = populate_config.get("csv_file")
        target_database = populate_config.get("Target_Database") or DB_PATH
        edinet.store_edinetCodes(csv_file, target_database=target_database)

    elif step_name == "generate_financial_ratios":
        logger.info("Generating financial ratios...")
        data.Generate_Financial_Ratios(DB_STANDARDIZED_TABLE, DB_STANDARDIZED_RATIOS_TABLE, overwrite=overwrite)

    elif step_name in ("generate_financial_statements", "Generate Financial Statements"):
        logger.info("Generating financial statements...")
        fs_config = config.get("generate_financial_statements_config", {})
        source_database = fs_config.get("Source_Database") or DB_PATH
        source_table = fs_config.get("Source_Table") or DB_STANDARDIZED_TABLE
        target_database = fs_config.get("Target_Database") or DB_PATH
        company_table = fs_config.get("Company_Info_Table") or DB_COMPANY_INFO_TABLE
        prices_table = fs_config.get("Stock_Prices_Table") or DB_STOCK_PRICES_TABLE
        mappings_config = fs_config.get(
            "Mappings_Config",
            "config/reference/financial_statements_mappings_config.json",
        )
        batch_size = fs_config.get("batch_size", 2500)

        data.generate_financial_statements(
            source_database=source_database,
            source_table=source_table,
            target_database=target_database,
            mappings_config=mappings_config,
            company_table=company_table,
            prices_table=prices_table,
            overwrite=overwrite,
            batch_size=batch_size,
        )

    elif step_name == "import_stock_prices_csv":
        logger.info("Importing stock prices from CSV...")
        csv_config = config.get("import_stock_prices_csv_config", {})
        csv_path = csv_config.get("csv_file", "")
        ticker = csv_config.get("ticker", "")
        currency = csv_config.get("currency", "JPY")
        date_column = csv_config.get("date_column", "Date")
        price_column = csv_config.get("price_column", "Close")
        y.import_stock_prices_csv(
            DB_PATH, DB_STOCK_PRICES_TABLE, csv_path,
            ticker, currency, date_column, price_column,
        )

    elif step_name == "update_stock_prices":
        logger.info("Updating stock prices...")
        y.update_all_stock_prices(DB_PATH, DB_COMPANY_INFO_TABLE, DB_STOCK_PRICES_TABLE, DB_STANDARDIZED_TABLE)

    elif step_name == "parse_taxonomy":
        logger.info("Parsing EDINET taxonomy...")
        taxonomy_config = config.get("parse_taxonomy_config", {})
        xsd_file = taxonomy_config.get("xsd_file")
        data.parse_edinet_taxonomy(xsd_file, DB_TAXONOMY_TABLE)

    elif step_name == "find_significant_predictors":
        logger.info("Finding significant predictors...")
        predictor_config = config.get("find_significant_predictors_config", {})
        output_file = predictor_config.get(
            "output_file",
            "data/ols_results/predictor_search_results.txt",
        )
        winsorize_thresholds = predictor_config.get(
            "winsorize_thresholds", {"lower": 0.05, "upper": 0.95}
        )
        winsorize_limits = (
            winsorize_thresholds["lower"],
            winsorize_thresholds["upper"],
        )
        alpha = predictor_config.get("alpha", 0.05)
        dependent_variables = predictor_config.get("dependent_variables") or []

        r.find_significant_predictors(
            db_path=DB_PATH,
            table_name=DB_STANDARDIZED_RATIOS_TABLE,
            results_table_name=DB_SIGNIFICANT_PREDICTORS_TABLE,
            output_file=output_file,
            winsorize_limits=winsorize_limits,
            alpha=alpha,
            dependent_variables=dependent_variables,
            overwrite=overwrite,
        )

    elif step_name == "Multivariate_Regression":
        logger.info("Running multivariate regression...")
        mv_config = config.get("Multivariate_Regression_config", {})
        r.multivariate_regression(
            mv_config, DB_PATH,
            ratios_table=DB_STANDARDIZED_RATIOS_TABLE,
            company_table=DB_COMPANY_INFO_TABLE,
        )

    elif step_name == "backtest":
        logger.info("Running backtesting...")
        backtesting_config = config.get("backtesting_config", {})
        bt.run_backtest(
            backtesting_config,
            db_path=DB_PATH,
            prices_table=DB_STOCK_PRICES_TABLE,
            ratios_table=DB_STANDARDIZED_RATIOS_TABLE,
            company_table=DB_COMPANY_INFO_TABLE,
        )

    elif step_name == "backtest_set":
        logger.info("Running backtest set...")
        bs_config = config.get("backtest_set_config", {})
        bt.run_backtest_set(
            bs_config,
            db_path=DB_PATH,
            prices_table=DB_STOCK_PRICES_TABLE,
            ratios_table=DB_STANDARDIZED_RATIOS_TABLE,
            company_table=DB_COMPANY_INFO_TABLE,
        )

    else:
        logger.warning(f"Unknown step: {step_name}")


def run(edinet=None, data=None):
    """
    Orchestrates the execution of the application based on the run config file.
    Executes steps in the order they are defined in the run_steps configuration.
    """
    logger.info('Starting Program')

    logger.info('Loading Config')
    config = Config()
    run_steps = config.get("run_steps", {})

    # ── Pre-flight checks ────────────────────────────────────────────────
    # Map each step to the config / .env keys it requires at runtime.
    STEP_REQUIRED_KEYS: dict[str, list[str]] = {
        "get_documents":              ["baseURL", "API_KEY"],
        "download_documents":         ["DB_DOC_LIST_TABLE", "DB_FINANCIAL_DATA_TABLE",
                                       "RAW_DOCUMENTS_PATH", "baseURL", "API_KEY"],
        "standardize_data":           ["DB_FINANCIAL_DATA_TABLE", "DB_STANDARDIZED_TABLE"],
        "populate_company_info":      ["DB_COMPANY_INFO_TABLE"],
        "generate_financial_ratios":  ["DB_STANDARDIZED_TABLE", "DB_STANDARDIZED_RATIOS_TABLE"],
        "import_stock_prices_csv":    ["DB_PATH", "DB_STOCK_PRICES_TABLE"],
        "update_stock_prices":        ["DB_PATH", "DB_COMPANY_INFO_TABLE",
                                       "DB_STOCK_PRICES_TABLE", "DB_STANDARDIZED_TABLE"],
        "parse_taxonomy":             ["DB_TAXONOMY_TABLE"],
        "generate_financial_statements": [],
        "find_significant_predictors": ["DB_PATH", "DB_STANDARDIZED_RATIOS_TABLE",
                                        "DB_SIGNIFICANT_PREDICTORS_TABLE"],
        "Multivariate_Regression":    ["DB_PATH"],
        "backtest":                   ["DB_PATH", "DB_STOCK_PRICES_TABLE",
                                       "DB_STANDARDIZED_RATIOS_TABLE",
                                       "DB_COMPANY_INFO_TABLE"],
        "backtest_set":               ["DB_PATH", "DB_STOCK_PRICES_TABLE",
                                       "DB_STANDARDIZED_RATIOS_TABLE",
                                       "DB_COMPANY_INFO_TABLE"],
    }

    enabled_steps = []
    for step_name, step_val in run_steps.items():
        if isinstance(step_val, dict):
            is_enabled = step_val.get("enabled", False)
        else:
            is_enabled = bool(step_val)
        if is_enabled:
            enabled_steps.append(step_name)

    missing_map: dict[str, list[str]] = {}
    for step_name in enabled_steps:
        required = STEP_REQUIRED_KEYS.get(step_name, [])
        for key in required:
            val = config.get(key)
            if not val:
                missing_map.setdefault(key, []).append(step_name)

    if missing_map:
        lines = ["The following required settings are missing from .env / config:"]
        for key, steps_needing in sorted(missing_map.items()):
            lines.append(f"  • {key}  (needed by: {', '.join(steps_needing)})")
        lines.append("")
        lines.append("Set them in the UI (top-bar database selector / API Key) ")
        lines.append("or add them to the .env file in the project root.")
        msg = "\n".join(lines)
        logger.error(msg)
        raise RuntimeError(msg)

    if not edinet:
        edinet = edinet_api.Edinet()
    if not data:
        data = d.data()

    # Execute steps in order as defined in run_steps
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
                _execute_step(step_name, config, edinet, data, overwrite=overwrite)
            except Exception as e:
                logger.error(f"Error executing step '{step_name}': {e}", exc_info=True)
        else:
            logger.debug(f"Step '{step_name}' is disabled, skipping.")

    logger.info('Program Ended')