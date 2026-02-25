import logging
import json
import src.edinet_api as edinet_api
from config import Config
import src.data_processing as d
import src.stockprice_api as y
import src.regression_analysis as r

logger = logging.getLogger(__name__)


def _execute_step(step_name, config, edinet, data):
    """
    Execute a single orchestration step.
    
    Args:
        step_name: Name of the step to execute
        config: Configuration object
        edinet: Edinet API instance
        data: Data processing instance
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
        data.copy_table_to_Standard(DB_FINANCIAL_DATA_TABLE, DB_STANDARDIZED_TABLE)

    elif step_name == "populate_company_info":
        logger.info("Populating company info table...")
        populate_config = config.get("populate_company_info_config", {})
        csv_file = populate_config.get("csv_file")
        edinet.store_edinetCodes(csv_file)

    elif step_name == "generate_financial_ratios":
        logger.info("Generating financial ratios...")
        data.Generate_Financial_Ratios(DB_STANDARDIZED_TABLE, DB_STANDARDIZED_RATIOS_TABLE)

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
        )

    elif step_name == "Multivariate_Regression":
        logger.info("Running multivariate regression...")
        mv_config = config.get("Multivariate_Regression_config", {})
        r.multivariate_regression(mv_config, DB_PATH)

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

    if not edinet:
        edinet = edinet_api.Edinet()
    if not data:
        data = d.data()

    # Execute steps in order as defined in run_steps
    logger.info(f"Steps to execute (in order): {list(run_steps.keys())}")
    for step_name, is_enabled in run_steps.items():
        if is_enabled:
            try:
                _execute_step(step_name, config, edinet, data)
            except Exception as e:
                logger.error(f"Error executing step '{step_name}': {e}", exc_info=True)
        else:
            logger.debug(f"Step '{step_name}' is disabled, skipping.")

    logger.info('Program Ended')